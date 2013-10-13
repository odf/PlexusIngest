#!/usr/bin/env python

"""
Produces slice images for Mango volume data stored in NetCDF files.

Currently supported data types are tomo, segmented and labels. If the
data is split into multiple files, those must sit in a common directory
and that directory given as the path argument.

Slices are taken through the center of the volume. If either of the
dimensions for a slice image would be less than 10, that slice is
suppressed. Image output is in .png format.

Typical usage:
    for (data, name, action) in Slicer(path).slices:
        fp = file(name, 'wb')
        fp.write(data)
        fp.close()
        

(Requires Python 2.6 or higher.)
"""

import os, os.path, re, struct, sys
import numpy
import bz2

from logger import Logger, LOGGER_INFO, LOGGER_WARNING
import make_image
from nc3header import NC3File


class VolumeVariable:
    """
    Encapsulates information pertaining to the volume variable <var>
    within the open NC3File <file>. Currently, the name, shape, origin
    and data type - mapped to the corresponding numpy type - are
    stored.
    """
    
    def __init__(self, file, var):
        # -- remember the name
        self.name = var.name
        
        # -- extract the variable's shape
        dims = var.dimensions
        self.size = (dims[2].value, dims[1].value, dims[0].value)
        zdim_total = get_attribute(file, var, 'zdim_total')
        if zdim_total is not None:
            self.size = (self.size[0], self.size[1], zdim_total[0])

        # -- determine the origin
        origin = get_attribute(file, var, 'coordinate_origin_xyz')
        self.origin = origin or (0, 0, 0)

        # -- determine the data type
        (self.dtype, self.big_endian_type) = {
          'b': (numpy.uint8,  numpy.dtype(">u1")),
          'h': (numpy.uint16, numpy.dtype(">u2")),
          'i': (numpy.int32,  numpy.dtype(">i4")),
          'f': (numpy.float32, numpy.dtype(">f4")) }[var.python_type_code]

    def __eq__(self, other):
        return (self.name == other.name and self.size == other.size and
                self.origin == other.origin and self.dtype == other.dtype)
    
    def __ne__(self, other):
        return not self.__eq__(other)


class Histogram:
    """
    Maintains a frequency count for a series of numpy arrays. Entries
    equal to <mask_value> are counted separately. Negative values are
    ignored.
    """
    
    def __init__(self, mask_value = 0, minval = None, maxval = None):
        # -- save the mask value
        self.mask_value = mask_value
        # -- initialize the frequency count
        self.counts = numpy.array([], dtype = 'uint64')
        # -- initialize the count of masked and total non-negative entries
        self.total = 0
        self.masked = 0

        if maxval is not None:
            self.offset = minval
            self.binsize = (maxval - minval) * (1.0 - 1.0e-12) / 0x10000;
        else:
            self.offset = 0
            self.binsize = 1

    def update(self, slice):
        """
        Updates the frequency count with the data from the numpy array
        <slice>.
        """
        
        # -- process the new data
        tmp = (slice.flatten() - self.offset) / self.binsize
        flat = numpy.array(tmp, dtype = 'uint16')
        mask = (flat == self.mask_value) | (flat < 0) | (flat > 0xffff)
        new_masked = flat[mask].size
        new_counts = numpy.bincount(numpy.where(mask, 0, flat))
        
        # -- update the frequency count, resizing if necessary
        s = max(self.counts.size, new_counts.size)
        if s > self.counts.size:
            self.counts.resize(s)
        if s > new_counts.size:
            new_counts.resize(s)
        self.counts += new_counts

        # -- update the count of masked and total non-negative entries
        self.masked += new_masked
        self.total  += new_masked + int(new_counts.sum())


class Slice:
    def __init__(self, size, type, axis, pos):
        self.axis = axis.lower()
        self.pos  = pos
        self.slice_dims = {'x': (size[2], size[1]),
                           'y': (size[2], size[0]),
                           'z': (size[1], size[0]) }[self.axis]
        self.content = numpy.zeros(self.slice_dims, type)

    def update(self, z_slice, z_pos):
        """
        Updates this slices with data from the array <z_slice>, which
        is taken to be at z = <z_pos>.
        """

        if self.axis == 'x':
            self.content[z_pos, :] = z_slice[:, self.pos]
        elif self.axis == 'y':
            self.content[z_pos, :] = z_slice[self.pos, :]
        elif self.axis == 'z':
            if z_pos == self.pos:
                self.content[:, :] = z_slice[:, :]


def get_attribute(file, var, name):
    """
    Looks in the open NC3File <file> for the attribute <name>,
    first in the given variable <var> and then in the global
    attributes. Return the attribute's value if found, or None
    otherwise.
    """
    
    for attr in var.attributes:
        if attr.name == name:
            return attr.value
    for attr in file.attributes:
        if attr.name == name:
            return attr.value
    return None


def z_slices(variable, path):
    """
    A generator method that yields constant z slices corresponding
    to the variable <var> from the NetCDF file located at <path>.
        
    Each value produced is a pair containing the z coordinate of
    the slice and a two-dimensional numpy array containing the
    extracted data.

    Usage example:
        
        for (z, data) in z_slices(var, path):
            ... # do something with data
    """
        
    file = NC3File(path)
        
    for var in file.variables:
        if var.name == variable.name:
            break
    else:
        return
        
    if VolumeVariable(file, var) != variable:
        raise RuntimeError("variable mismatch between files")
    z_range = get_attribute(file, var, 'zdim_range')
    if z_range is None:
        z_range = range(0, variable.size[2])
    else:
        z_range = range(z_range[0], z_range[1] + 1)
            
    bytes_per_slice = variable.size[0] * variable.size[1] * var.element_size
    offset = var.data_start
        
    file.close()

    if path.endswith('.bz2'):
        fp = bz2.BZ2File(path, 'r', 1024 * 1024)
    else:
        fp = open(path, "rb")
    fp.seek(var.data_start)
    for z in z_range:
        buffer = fp.read(bytes_per_slice)
        if len(buffer) < bytes_per_slice:
            yield (z, None, "insufficient data")
            break
        data = numpy.fromstring(buffer, variable.big_endian_type)
        data.shape = (variable.size[1], variable.size[0])
        yield (z, data)
    fp.close()


def find_variable(path):
    """
    Looks for a volume variable to extract slice images from in the
    NetCDF file at location <path>. Returns an descriptor object of
    class VolumeVariable if something appropriate is found, or None
    otherwise.
    """
    
    # -- the default return value
    var = None

    # -- open the NetCDF file
    file = NC3File(path)
    
    # -- loop through all the variables in the file
    for v in file.variables:
        if (len(v.dimensions) == 3 and v.dimensions[0].value > 1 and
            v.python_type_code in 'bhif'
            ):
            var = VolumeVariable(file, v)
            break
    
    # -- close the file and return the result
    file.close()
    return var
    

def bottom_percentile(histogram, p):
    """
    Returns the smallest number i such that at least <p> percent of
    the non-masked entries counted so far have value i or less.
    """
    threshold = p * (histogram.total - histogram.masked) / 100.0
    count = 0
    for i in xrange(histogram.counts.size):
        count += int(histogram.counts[i])
        if count >= threshold:
            return histogram.offset + i * histogram.binsize

def top_percentile(histogram, p):
    """
    Returns the largest number i such that at least <p> percent of
    the non-masked entries counted so far have value i or more.
    """
    threshold = p * (histogram.total - histogram.masked) / 100.0
    count = 0
    for i in xrange(histogram.counts.size-1, -1, -1):
        count += int(histogram.counts[i])
        if count >= threshold:
            return histogram.offset + i * histogram.binsize


def sizeprefix(thumb_size):
    return ("__%sx%s__" % thumb_size) if thumb_size else ""


def default_slice_set(var, delta, basename):
    """
    Creates a default list of empty slice instances, paired with
    associated file names, based on the shape of the volume variable
    <var>. For each axis, a slice centered at that axis is created,
    provided that the extend of the slice in both directions would be at
    least <delta>.
    """
    pos = list((x - 1) / 2 for x in var.size)
    size = var.size
    dtype = var.dtype
    origin = var.origin

    def slice_and_name(axis, pos, offset):
        s = Slice(size, dtype, axis, pos)
        n = "slice%c%d_%s.png" % (axis.upper(), pos + offset, basename)
        return (s, n)

    slices = []
    if size[1] > delta and size[2] > delta:
        slices.append(slice_and_name('x', pos[0], origin[0]))
    if size[0] > delta and size[2] > delta:
        slices.append(slice_and_name('y', pos[1], origin[1]))
    if size[0] > delta and size[1] > delta:
        slices.append(slice_and_name('z', pos[2], origin[2]))

    return slices


def image_data(slice, lo, hi, mask_val, info, thumb_size = None):
    """
    Converts a slice into an image and returns it as a PNG encoded
    string. The values <lo> and <hi> specify the range of relevant
    data. Entries equal to <mask_val> are treated as masked.

    An appropriate encoding mode is determined from the data type
    and value range. Details of the actual encoding are deferred to
    the function 'make_image' in the package of the same name.
    """

    # -- determine the encoding mode
    content = slice.content
    if content.dtype == numpy.uint8:
        if hi <= 1:
            img_mode = make_image.BLACK_AND_WHITE
        else:
            img_mode = make_image.COLOR_CODED_FIXED
    elif content.dtype == numpy.uint16:
        img_mode = make_image.GRAYSCALE
    elif content.dtype == numpy.int32:
        img_mode = make_image.COLOR_CODED
    elif content.dtype == numpy.float32:
        img_mode = make_image.GRAYSCALE
    else:
        raise Exception("unexpected array type: %s" % content.dtype)

    # -- generate slice-specific info
    myinfo = info.copy()
    myinfo.update({ 'slice-axis': slice.axis, 'slice-pos': slice.pos })

    # -- generate and return the data
    return make_image.make_image(content, lo, hi, mask_val, img_mode,
                                 thumb_size, myinfo)


def data_range(var, entries):
    minval = maxval = None

    for filename in entries:
        for tmp in z_slices(var, filename):
            z, data = tmp[:2]
            if data is not None:
                lo = numpy.min(data)
                hi = numpy.max(data)
                if minval is None or lo < minval:
                    minval = lo
                if maxval is None or hi > maxval:
                    maxval = hi

    return (minval, maxval)


class Slicer:
    """
    Encapsulates the data and methods used in extracting slice images
    from a Mango volume data set stored in a collection of NetCDF
    files.
    
    Currently, there is a single constructor parameter <path>,
    specifying a single NetCDF file or a directory containing a single
    volume split into several NetCDF files.
    
    The lazy property 'slices' holds the result of the slice extraction
    as an immutable list of (data, name) pairs.
    
    All other methods and properties are currently only for internal
    use.

    Typical usage:
        for (data, name, action) in Slicer(path).slices:
            fp = file(name, 'wb')
            fp.write(data)
            fp.close()
    """

    def __init__(self, path,
                 existing = [],
                 replace = False,
                 dry_run = False,
                 sizes = (None,),
                 info = {}):
        self.path      = re.sub("/$", "", path)
        self.name      = re.sub("[._]nc$", "", os.path.basename(self.path))
        self.basename  = re.sub("^tomo", "tom",
                                re.sub("^segmented", "seg", self.name))
        self.existing  = existing
        self.replace   = replace
        self.dry_run   = dry_run
        self.sizes     = sizes
        self.info      = info
        
        self.img_mode  = None
        self._slices   = None
        self.log       = Logger()
    
    def process(self):
        """
        Internal! Processes all the volume data and generates the
        slices.
        """
        
        # -- set empty result as fallback (so this method is not called twice)
        self._slices = ()
        
        # -- collect the list of files to be processed
        if os.path.isdir(self.path):
            entries = list(os.path.join(root, f)
                           for (root, dirs, files) in os.walk(self.path)
                           for f in files)
            entries.sort()
        else:
            entries = [ self.path ]
        if not entries:
            return
        
        # -- find a useable volume variable and analyse it
        filename = entries[0]
        self.log.writeln("Slicer: looking for a volume variable in %s..." %
                         os.path.basename(filename))
        var = find_variable(filename)
        if var is None :
            self.log.writeln("No appropriate volume data found.")
            return
        
        # -- determine the appropriate mask value
        mask_value = { numpy.uint8:  0xff,
                       numpy.uint16: 0xffff,
                       numpy.int32:  0x7fffffff,
                       numpy.float32: 1.0e30 }[var.dtype]

        # -- initialize the slice set to be created
        slices = default_slice_set(var, 10, self.basename)
        r_or_s = 'REPLACE' if self.replace else 'SKIP'
        actions = list((r_or_s if n in self.existing else 'ADD')
                       for (s, n) in slices)
        slices = list(slices[i] + (actions[i],)
                      for i in range(len(slices))
                      if actions[i] != 'SKIP')

        if len(slices) == 0:
            self.log.writeln("No slices are to be made.")
            return
        elif self.dry_run:
            out = tuple((make_image.make_dummy(n, 256, 256, sz),
                         sizeprefix(sz) + n,
                         a)
                        for (s, n, a) in slices for sz in self.sizes)
            self._slices = out
            return

        # -- initialize the histogram
        if var.dtype == numpy.float32:
            self.log.writeln("Determining the data range...")
            (minval, maxval) = data_range(var, entries)
            hist = Histogram(mask_value, minval, maxval)
        else:
            hist = Histogram(mask_value)

        # -- loop through files and copy data into slice arrays
        for filename in entries:
            self.log.writeln("Processing %s..." % os.path.basename(filename))
            for tmp in z_slices(var, filename):
                z, data = tmp[:2]
                if data is None:
                    self.log.writeln(tmp[2] + " at z = %d" % z, LOGGER_WARNING)
                else:
                    hist.update(data)
                    for (s, n, a) in slices:
                        s.update(data, z)
            
        # -- analyse histogram to determine 'lo' and 'hi' values
        self.log.writeln("Analysing the histogram...")
        if self.name.startswith("tom"):
            # -- determine 0.1 and 99.9 percentile for contrast stretching
            lo = bottom_percentile(hist, 0.1)
            hi = top_percentile(hist, 0.1)
        else:
            lo = 0
            hi = hist.counts.size - 1
        
        # -- encode slices as PNG images
        self.log.writeln("Making the images...")
        self._slices = tuple((image_data(s, lo, hi, mask_value, self.info, sz),
                              sizeprefix(sz) + n,
                              a)
                             for (s, n, a) in slices for sz in self.sizes)
        
        # -- report success
        self.log.writeln("Slice image generation finished.")

    @property
    def slices(self):
        """
        Implements the 'slices' property. Calls process() if no slices
        have yet been computed.
        """
        
        if self._slices is None:
            self.process()
        return self._slices


if __name__ == "__main__":
    Logger().priority = LOGGER_INFO
    source = sys.argv[1]
    slicer = Slicer(source,
                    info = { 'source': source },
                    sizes = (None, (80,80)))

    for (data, name, action) in slicer.slices:
        fp = file(name, 'wb')
        fp.write(data)
        fp.close()
