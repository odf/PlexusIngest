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
        

(Requires Python 2.4 or higher.)
"""

import os, os.path, re, struct, sys
import numpy

from logger import Logger, LOGGER_INFO, LOGGER_WARNING
import make_image
from nc3header import NC3File


class Vec3:
    """
    Tiny ad-hoc class for three-dimensional vectors. Supports the four
    basic arithmetic operators and propagation of scalars.
    """
    
    def __init__(self, a):
        if isinstance(a, Vec3):
            (x, y, z) = (a.x, a.y, a.z)
        elif type(a) in (int, float):
            (x, y, z) = (a, a, a)
        else:
            (x, y, z) = a
        self.x = x
        self.y = y
        self.z = z

    def __add__(self, other):
        other = Vec3(other)
        return Vec3((self.x + other.x, self.y + other.y, self.z + other.z))
    __radd__ = __add__

    def __sub__(self, other):
        other = Vec3(other)
        return Vec3((self.x - other.x, self.y - other.y, self.z - other.z))

    def __rsub__(self, other):
        other = Vec3(other)
        return Vec3((other.x - self.x, other.y - self.y, other.z - self.z))

    def __mul__(self, a):
        return Vec3((self.x * a, self.y * a, self.z * a))
    __rmul__ = __mul__

    def __div__(self, a):
        return Vec3((self.x / a, self.y / a, self.z / a))
    
    def __eq__(self, a):
        return self.x == a.x and self.y == a.y and self.z == a.z
    
    def __ne__(self, a):
        return not self.__eq__(a)


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
        self.size = Vec3((dims[2].value, dims[1].value, dims[0].value))
        zdim_total = get_attribute(file, var, 'zdim_total')
        if zdim_total is not None: self.size.z = zdim_total[0]

        # -- determine the origin
        origin = get_attribute(file, var, 'coordinate_origin_xyz')
        if origin is None: origin = 0
        self.origin = Vec3(origin)

        # -- determine the data type
        (self.dtype, self.big_endian_type) = {
          'b': (numpy.uint8,  numpy.dtype(">u1")),
          'h': (numpy.uint16, numpy.dtype(">u2")),
          'i': (numpy.int32,  numpy.dtype(">i4")) }[var.python_type_code]

    def __eq__(self, other):
        return (self.name == other.name and self.size == other.size and
                self.origin == other.origin and self.dtype == other.dtype)
    
    def __ne__(self, other):
        return not self.__eq__(other)

    def z_slices(self, path):
        """
        A generator method that yields constant z slices corresponding
        to this variable from the NetCDF file located at <path>.
        
        Each value produced is a pair containing the z coordinate of
        the slice and a two-dimensional numpy array containing the
        extracted data.

        Usage example:
        
            for (z, data) in var.z_slices(path):
                ... # do something with data
        """
        
        file = NC3File(path)
        
        for var in file.variables:
            if var.name == self.name:
                break
        else:
            return
        
        if VolumeVariable(file, var) != self:
            raise RuntimeError("variable mismatch between files")
        z_range = get_attribute(file, var, 'zdim_range')
        if z_range is None:
            z_range = range(0, self.size.z)
        else:
            z_range = range(z_range[0], z_range[1] + 1)
            
        bytes_per_slice = self.size.x * self.size.y * var.element_size
        offset = var.data_start
        
        file.close()
        
        fp = open(path, "rb")
        fp.seek(var.data_start)
        for z in z_range:
            buffer = fp.read(bytes_per_slice)
            if len(buffer) < bytes_per_slice:
                yield (z, None, "insufficient data")
                break
            data = numpy.fromstring(buffer, self.big_endian_type)
            data.shape = (self.size.y, self.size.x)
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
            v.python_type_code in 'bhi'
            ):
            var = VolumeVariable(file, v)
            break
    
    # -- close the file and return the result
    file.close()
    return var
    

class Histogram:
    """
    Maintains a frequency count for a series of numpy arrays. Entries
    equal to <mask_value> are counted separately. Negative values are
    ignored.
    """
    
    def __init__(self, mask_value = 0):
        # -- save the mask value
        self.mask_value = mask_value
        # -- initialize the frequency count
        self.counts = numpy.array([], dtype = 'uint64')
        # -- initialize the count of masked and total non-negative entries
        self.total = 0
        self.masked = 0

    def update(self, slice):
        """
        Updates the frequency count with the data from the numpy array
        <slice>.
        """
        
        # -- process the new data
        flat = slice.flatten()
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
        
    def bottom_percentile(self, p):
        """
        Returns the smallest number i such that at least <p> percent of
        the non-masked entries counted so far have value i or less.
        """
        threshold = p * (self.total - self.masked) / 100.0
        count = 0
        for i in xrange(self.counts.size):
            count += int(self.counts[i])
            if count >= threshold:
                return i
        
    def top_percentile(self, p):
        """
        Returns the largest number i such that at least <p> percent of
        the non-masked entries counted so far have value i or more.
        """
        threshold = p * (self.total - self.masked) / 100.0
        count = 0
        for i in xrange(self.counts.size-1, -1, -1):
            count += int(self.counts[i])
            if count >= threshold:
                return i


class Slice:
    def __init__(self, size, type, axis, pos, offset):
        self.axis = axis.lower()
        self.pos = pos
        self.offset = offset
        self.slice_dims = {'x': (size.z, size.y),
                           'y': (size.z, size.x),
                           'z': (size.y, size.x) }[self.axis]         
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

    def make_name(self, basename):
        """
        Creates a name for this slice from its axis and location and
        the string <basename>.
        """
        
        return "slice%c%d_%s.png" % (self.axis.upper(),
                                     self.pos + self.offset, basename)

    def image_data(self, lo, hi, mask_val):
        """
        Converts this slice into an image and returns it as a PNG
        encoded string. The values <lo> and <hi> specify the range of
        relevant data. Entries equal to <mask_val> are treated as
        masked.
        
        An appropriate encoding mode is determined from the data type
        and value range. Details of the actual encoding are deferred to
        the function 'make_image' in the package of the same name.
        """
        
        # -- determine the encoding mode
        content = self.content
        if content.dtype == numpy.uint8:
            if hi <= 1:
                img_mode = make_image.BLACK_AND_WHITE
            else:
                img_mode = make_image.COLOR_CODED_FIXED
        elif content.dtype == numpy.uint16:
            img_mode = make_image.GRAYSCALE
        elif content.dtype == numpy.int32:
            img_mode = make_image.COLOR_CODED
        else:
            raise "unexpected array type: %s" % content.dtype
    
        # -- generate and return the data
        return make_image.make_image(content, lo, hi, mask_val, img_mode)


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

    def __init__(self, path, existing = [], replace = False, dry_run = False):
        self.path      = re.sub("/$", "", path)
        self.name      = re.sub("[._]nc$", "", os.path.basename(self.path))
        self.slicename = re.sub("^tomo", "tom",
                                re.sub("^segmented", "seg", self.name))
        self.existing  = existing
        self.replace   = replace
        self.dry_run   = dry_run
        
        self.img_mode  = None
        self._slices   = None
        self.log       = Logger()
    
    def add_slice(self, slices, size, dtype, axis, pos, origin):
        slice = Slice(size, dtype, axis, pos, origin)
        name = slice.make_name(self.slicename)
        if name in self.existing:
            if self.replace:
                action = "REPLACE"
            else:
                action = "SKIP"
        else:
            action= "ADD"
        
        if action != "SKIP":
            slice.action = action
            slices.append(slice)
        
    def default_slice_set(self, var, delta):
        """
        Creates a default list of empty slice instances based on the
        shape of the volume variable <var>. For each axis, a slice
        centered at that axis is created, provided that the extend of
        the slice in both directions would be at least <delta>.
        """
        
        pos = (var.size - 1) / 2
        size = var.size
        dtype = var.dtype
        origin = var.origin
        
        slices = []
        if size.y > delta and size.z > delta:
            self.add_slice(slices, size, dtype, 'x', pos.x, origin.x)
        if size.x > delta and size.z > delta:
            self.add_slice(slices, size, dtype, 'y', pos.y, origin.y)
        if size.x > delta and size.y > delta:
            self.add_slice(slices, size, dtype, 'z', pos.z, origin.z)

        return slices
        
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
        self.log.writeln("Looking for a volume variable in %s..." %
                         os.path.basename(filename))
        var = find_variable(filename)
        if var is None :
            self.log.writeln("No appropriate volume data found.")
            return
        
        # -- determine the appropriate mask value
        mask_value = { numpy.uint8:  0xff,
                       numpy.uint16: 0xffff,
                       numpy.int32:  0x7fffffff }[var.dtype]

        # -- initialize the slice set to be created
        slices = self.default_slice_set(var, 10)
        if len(slices) == 0:
            self.log.writeln("No slices are to be made.")
            return
        elif self.dry_run:
            name = self.slicename
            out = tuple((make_image.make_dummy(s.make_name(name)),
                         s.make_name(name), s.action) 
                        for s in slices)
            self._slices = out
            return

        # -- set up an array to hold the current z slice
        buffer = numpy.zeros((var.size.y, var.size.x), var.dtype)

        # -- initialize the histogram
        hist = Histogram(mask_value)

        # -- loop through files and copy data into slice arrays
        for filename in entries:
            self.log.writeln("Processing %s..." % os.path.basename(filename))
            for tmp in var.z_slices(filename):
                z, data = tmp[:2]
                if data is None:
                    self.log.writeln(tmp[2] + " at z = %d" % z, LOGGER_WARNING)
                else:
                    hist.update(data)
                    for s in slices:
                        s.update(data, z)
            
        # -- analyse histogram to determine 'lo' and 'hi' values
        self.log.writeln("Analysing the histogram...")
        if self.name.startswith("tom"):
            # -- determine 0.1 and 99.9 percentile for contrast stretching
            lo = hist.bottom_percentile(0.1)
            hi = hist.top_percentile(0.1)
        else:
            lo = 0
            hi = hist.counts.size - 1
        
        # -- encode slices as PNG images
        self.log.writeln("Making the images...")
        self._slices = tuple((s.image_data(lo, hi, mask_value),
                              s.make_name(self.slicename), s.action)
                             for s in slices)
        
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
    for (data, name, action) in Slicer(sys.argv[1]).slices:
        fp = file(name, 'wb')
        fp.write(data)
        fp.close()