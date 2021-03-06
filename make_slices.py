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
    for (data, name, action) in slices(path):
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
from nc3files import datafiles, nc3info


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


def get_attribute(info, var, name):
    """
    Looks in the open NC3Info <info> for the attribute <name>,
    first in the given variable <var> and then in the global
    attributes. Return the attribute's value if found, or None
    otherwise.
    """
    
    for attr in var.attributes:
        if attr.name == name:
            return attr.value
    for attr in info.attributes:
        if attr.name == name:
            return attr.value
    return None


def volume_variable(info, var):
    """
    Extracts information pertaining to the volume variable <var> within the
    NC3Info <info>. The name, shape, origin and data type - mapped to the
    corresponding numpy type - are stored.
    """
    
    # -- remember the name
    name = var.name

    # -- extract the variable's shape
    dims = var.dimensions
    size = (dims[2].value, dims[1].value, dims[0].value)
    zdim_total = get_attribute(info, var, 'zdim_total')
    if zdim_total is not None:
        size = (size[0], size[1], zdim_total[0])

    # -- determine the origin
    origin = get_attribute(info, var, 'coordinate_origin_xyz') or (0, 0, 0)

    # -- determine the data type
    (dtype, big_endian_type) = {
      'b': (numpy.uint8,  numpy.dtype(">u1")),
      'h': (numpy.uint16, numpy.dtype(">u2")),
      'i': (numpy.int32,  numpy.dtype(">i4")),
      'f': (numpy.float32, numpy.dtype(">f4")) }[var.python_type_code]

    return { 'name'  : name,
             'size'  : size,
             'origin': origin,
             'dtype' : dtype,
             'big_endian_type': big_endian_type }


def find_variable(path):
    """
    Looks for a volume variable to extract slice images from in the
    NetCDF info at location <path>. Returns an descriptor object of
    class VolumeVariable if something appropriate is found, or None
    otherwise.
    """

    # -- the default return value
    var = None

    # -- open the NetCDF file
    info = nc3info(path)

    # -- loop through all the variables in the file
    for v in info.variables:
        if (len(v.dimensions) == 3 and v.dimensions[0].value > 1 and
            v.python_type_code in 'bhif'
            ):
            return volume_variable(info, v)


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

    info = nc3info(path)

    for var in info.variables:
        if var.name == variable['name']:
            break
    else:
        return

    if volume_variable(info, var) != variable:
        raise RuntimeError("variable mismatch between files")

    (x, y, z) = variable['size']

    z_range = get_attribute(info, var, 'zdim_range')
    if z_range is None:
        z_range = range(0, z)
    else:
        z_range = range(z_range[0], z_range[1] + 1)

    bytes_per_slice = x * y * var.element_size
    offset = var.data_start

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
        data = numpy.fromstring(buffer, variable['big_endian_type'])
        data.shape = (y, x)
        yield (z, data)
    fp.close()


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


def default_slice_set(var, delta, basename):
    """
    Creates a default list of empty slice instances, paired with
    associated file names, based on the shape of the volume variable
    <var>. For each axis, a slice centered at that axis is created,
    provided that the extend of the slice in both directions would be at
    least <delta>.
    """
    pos = list((x - 1) / 2 for x in var['size'])
    size = var['size']
    dtype = var['dtype']
    origin = var['origin']

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


def slices(path,
           existing = [],
           replace = False,
           dry_run = False,
           sizes = (None,),
           info = {}):
    """
    A generator which extracts slice images from a Mango volume data set
    stored in a collection of NetCDF files.

    The parameter <path> specifies a single NetCDF file or a directory
    containing a single volume split into several NetCDF files.

    Basic usage:
        for (data, name, action) in slices(path):
            fp = file(name, 'wb')
            fp.write(data)
            fp.close()
    """

    path      = re.sub("/$", "", path)
    name      = re.sub("[._]nc$", "", os.path.basename(path))
    basename  = re.sub("^tomo", "tom", re.sub("^segmented", "seg", name))
    img_mode  = None
    log       = Logger()

    # -- collect the list of files to be processed
    entries = datafiles(path)
    if not entries:
        return

    # -- find a useable volume variable and analyse it
    filename = entries[0]
    log.writeln("slices(): looking for a volume variable in %s..." %
                os.path.basename(filename))
    var = find_variable(filename)
    if var is None :
        log.writeln("No appropriate volume data found.")
        return

    # -- determine the appropriate mask value
    mask_value = { numpy.uint8:  0xff,
                   numpy.uint16: 0xffff,
                   numpy.int32:  0x7fffffff,
                   numpy.float32: 1.0e30 }[var['dtype']]

    # -- initialize the slice set to be created
    slices = default_slice_set(var, 10, basename)
    r_or_s = 'REPLACE' if replace else 'SKIP'
    actions = list((r_or_s if n in existing else 'ADD')
                   for (s, n) in slices)
    slices = list(slices[i] + (actions[i],)
                  for i in range(len(slices))
                  if actions[i] != 'SKIP')

    if len(slices) == 0:
        log.writeln("No slices are to be made.")
        return

    if not dry_run:
        # -- initialize the histogram
        if var['dtype'] == numpy.float32:
            log.writeln("Determining the data range...")
            (minval, maxval) = data_range(var, entries)
            hist = Histogram(mask_value, minval, maxval)
        else:
            hist = Histogram(mask_value)

        # -- loop through files and copy data into slice arrays
        for filename in entries:
            log.writeln("Processing %s..." % os.path.basename(filename))
            for tmp in z_slices(var, filename):
                z, data = tmp[:2]
                if data is None:
                    log.writeln(tmp[2] + " at z = %d" % z, LOGGER_WARNING)
                else:
                    hist.update(data)
                    for (s, n, a) in slices:
                        s.update(data, z)

        # -- analyse histogram to determine 'lo' and 'hi' values
        log.writeln("Analysing the histogram...")
        if name.startswith("tom"):
            # -- determine 0.1 and 99.9 percentile for contrast stretching
            lo = bottom_percentile(hist, 0.1)
            hi = top_percentile(hist, 0.1)
        else:
            lo = 0
            hi = hist.counts.size - 1

    # -- encode slices as PNG images
    log.writeln("Making the images...")
    for (s, n, a) in slices:
        for sz in sizes:
            if dry_run:
                data = make_image.make_dummy(n, 256, 256, sz)
            else:
                data = image_data(s, lo, hi, mask_value, info, sz)
            prefix = ("__%sx%s__" % sz) if sz else ""
            yield (data, prefix + n, a)

    # -- report success
    log.writeln("Slice image generation finished.")


if __name__ == "__main__":
    Logger().priority = LOGGER_INFO
    source = sys.argv[1]
    s = slices(source, info = { 'source': source }, sizes = (None, (80,80)))

    for (data, name, action) in s:
        fp = file(name, 'wb')
        fp.write(data)
        fp.close()
