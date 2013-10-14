#!/usr/bin/env python

"""
This package provides support for parsingNetCDF file headers of format
version 1, also known as the 'classic' format. This format was used
exclusively up to NetCDF library version 3.5

The class NC3File is used to parse and represent header information.
There is no direct support as yet for reading the actual data.
"""

import os, os.path, re, struct

from file_cache import FileCache, SEEK_CUR
from logger import Logger, LOGGER_TRACE, LOGGER_INFO, LOGGER_WARNING

try:
    import hashlib
    def hexdigest(s): return hashlib.md5(s).hexdigest()
except ImportError:
    import md5
    def hexdigest(s): return md5.new(s).hexdigest()


class NC3Error(RuntimeError):
    """
    Class for errors thrown by the NetCDF parser.
    """
    def __init__(self, where, text):
        if isinstance(where, NC3File):
            RuntimeError.__init__(self, "%s: %s" % (text, where.path))
        elif isinstance(where, str):
            RuntimeError.__init__(self, "%s: %s" % (text, where))
        else:
            RuntimeError.__init__(self, text)


# -- tags found in NetCDF files --
NC_BYTE      =  1
NC_CHAR      =  2
NC_SHORT     =  3
NC_LONG      =  4
NC_FLOAT     =  5
NC_DOUBLE    =  6

NC_DIMENSION = 10
NC_VARIABLE  = 11
NC_ATTRIBUTE = 12


class NC3Type:
    """
    Represents a NetCDF data type. Accessible fields:
    
    nc_code - the integral tag specifying the type in NetCDF headers
    name    - the human readable type name
    size    - the size in bytes for an element of this type
    py_code - the corresponding python type code for the struct package
    """
    def __init__(self, nc_code, name, size, py_code):
        self.nc_code = nc_code
        self.name = name
        self.size = size
        self.py_code = py_code

# -- fill the global list of available data types --
NC_TYPE = {}
for (nc_code, name, size, py_code) in (
  (NC_BYTE,   "byte",   1, "b"),
  (NC_CHAR,   "char",   1, "c"),
  (NC_SHORT,  "short",  2, "h"),
  (NC_LONG,   "int",    4, "i"),
  (NC_FLOAT,  "float",  4, "f"),
  (NC_DOUBLE, "double", 8, "d")
  ):
    NC_TYPE[nc_code] = NC3Type(nc_code, name, size, py_code)


def fmt(val):
    "Recursively formats an attribute value."
    if type(val) is str:
        # -- escape all double quotes that aren't already escaped
        result = []
        escaped = False
        for piece in re.split('(")', val):
            if piece == '"' and not escaped:
                result.append('\\')
            result.append(piece)
            escaped = piece.endswith('\\')
        tmp = ''.join(result)
        # -- split along line breaks and quote each line separately
        return ('"%s"' % tmp).replace('\n', '\\n",\n\t\t\t"')
    elif type(val) in ( tuple, list ):
        # -- format aggregates as comma-separated lists
        return ", ".join(map(fmt, val))
    else:
        # -- everything else should be simply a number
        return str(val)


class NC3Dimension:
    """
    Represents a dimension. Accessible fields are 'name' and 'value'.
    """
    def __init__(self, name, value):
        self.name = name
        self.value = value
        
    def __str__(self):
        return "%s = %d" % (self.name, self.value)


class NC3Attribute:
    """
    Represents an attribute. Accessible fields are 'name' and 'value'.
    """
    def __init__(self, name, value):
        self.name = name
        self.value = value
    
    def __str__(self):
        return "%s = %s" % (self.name, fmt(self.value))
    

class NC3Variable:
    """
    Represents a variable. Useful properties:
    
    name             - the name of the variable
    dimensions       - the list of dimensions (class NC3Dimension)
    attributes       - the list of attributes (class NC3Attribute)
    data_size        - the size of the associated data in bytes
    data_start       - the data offset in bytes within the file
    type_name        - the human readable name of the data type
    python_type_code - the type code to use with the struct package
    element_size     - the size of a data element
    """
    def __init__(self, name, dimensions, attributes,
                 nc_type, data_size, data_start):
        self.name = name
        self.dimensions = dimensions
        self.attributes = attributes
        self.nc_type = nc_type
        self.data_size = data_size
        self.data_start = data_start
    
    @property
    def type_name(self):
        return NC_TYPE[self.nc_type].name
    
    @property
    def python_type_code(self):
        return NC_TYPE[self.nc_type].py_code
    
    @property
    def element_size(self):
        return NC_TYPE[self.nc_type].size
    
    def __str__(self):
        return "%s %s(%s)" % (self.type_name, self.name,
                              ", ".join((d.name for d in self.dimensions)))


def read_values(fp, type_code, number):
    tp = NC_TYPE[type_code]
    size = tp.size * number
    value = fp.read(size)
    if len(value) < size:
        raise Error("Premature end of file.")
    fp.seek(3 - (size + 3) % 4, SEEK_CUR)
    if type_code == NC_CHAR:
        return value
    else:
        return struct.unpack(">%d%s" % (number, tp.py_code), value)

def read_integer(fp):
    return read_values(fp, NC_LONG, 1)[0]

def read_non_negative(fp):
    n = read_integer(fp)
    if n < 0:
        raise Error("Non-negative number expected")
    return n

def read_string(fp):
    size = read_non_negative(fp)
    return read_values(fp, NC_CHAR, size)

def read_dimensions(fp):
    dimensions = []
    tag = read_integer(fp)
    ndims = read_non_negative(fp)
    if tag == NC_DIMENSION:
        for i in range(ndims):
            name = read_string(fp)
            size = read_non_negative(fp)
            dimensions.append(NC3Dimension(name, size))
    elif tag != 0 or ndims != 0:
        raise Error("Expected dimension array.")

    return dimensions

def read_attributes(fp):
    attributes = []
    tag = read_integer(fp)
    nattr = read_non_negative(fp)
    if tag == NC_ATTRIBUTE:
        for i in range(nattr):
            name = read_string(fp)
            type = read_integer(fp)
            size = read_non_negative(fp)
            values = read_values(fp, type, size)
            attributes.append(NC3Attribute(name, values))
    elif tag != 0 or nattr != 0:
        raise Error("Expected attribute array.")

    return attributes

def read_variables(fp, dimensions):
    variables = []
    tag = read_integer(fp)
    nvars = read_non_negative(fp)
    if tag == NC_VARIABLE:
        for i in range(nvars):
            name = read_string(fp)
            ndims = read_non_negative(fp)
            dims = tuple(dimensions[read_non_negative(fp)]
                         for x in range(ndims))
            attr = read_attributes(fp)
            nc_type = read_integer(fp)
            size = read_non_negative(fp)
            start = read_non_negative(fp)
            variables.append(NC3Variable(name, dims, attr, nc_type, size, start))
    elif tag != 0 or nvars != 0:
        raise Error("Expected variable descriptions.")

    return variables


class NC3File:
    """
    Represents the complete header data from a NetCDF file. The
    constructor accepts a file system path and an optional name for the
    data set. All methods are for internal use only.
    
    Useful properties:
    
    path          - the file's location on the file system
    dimensions    - the list of dimensions defined (type NC3Dimension)
    attributes    - the list of attributes defined (type NC3Attribute)
    variables     - the list of variables (type NC3Variable)
    header_size   - the header size on file in bytes
    """
    def __init__(self, path):
        # -- remember the file system path
        self.path = path
        
        # -- some logging
        self.log = Logger()
        self.log.writeln("Parsing NetCDF header from %s..." % path)
        self.log.enter()
        
        # -- open the physical file, parse and close
        self.file = FileCache(path)
        try:
            self.parse_header()
        finally:
            self.file.close()
        
        # -- more logging
        self.log.trace("Header size is %d." % self.header_size)
        self.log.leave()
        
    def parse_header(self):
        magic = read_values(self.file, NC_CHAR, 4)
        if magic != "CDF\001":
            raise NC3Error(self, "Not a NetCDF version 1 file.")
        self.numrecords = read_non_negative(self.file)
        self.dimensions = read_dimensions(self.file)
        self.attributes = read_attributes(self.file)
        self.variables  = read_variables(self.file, self.dimensions)
        self.header_size = self.file.tell()
        self.file.seek(0)
        self.fingerprint = hexdigest(self.file.read(self.header_size))


def looksLikeNetCDF(name):
    if name.endswith(".bz2"):
        name = name[:-4]
    return name.endswith('_nc') or name.endswith('.nc')

def basenameNetCDF(name):
    if name.endswith(".bz2"):
        name = name[:-4]
    if name.endswith(".nc") or name.endswith("_nc"):
        name = name[:-3]
    return name

def nc3file_from_directory(path):
    # --- normalize the path name
    if path.endswith('/'):
        path = path[:-1]
    
    # -- determine the base name for the header file
    name = basenameNetCDF(os.path.basename(path))
        
    # -- collect the files under the given path
    if os.path.isdir(path):
        entries = list(os.path.join(root, f)
                       for (root, dirs, files) in os.walk(path)
                       for f in files
                       if looksLikeNetCDF(f))
        entries.sort()
    else:
        entries = [ path ]
    if not entries:
        raise NC3Error(path, "No NetCDF files found.")
        
    # -- open the first file and return the object
    return NC3File(entries[0])


if __name__ == "__main__":
    import sys
    
    i = 1
    if sys.argv[i].startswith("-v"):
        if sys.argv[i].startswith("-vv"):
            Logger().priority = LOGGER_TRACE
        else:
            Logger().priority = LOGGER_INFO
        i += 1

    nc3file = nc3file_from_directory(sys.argv[i])

    buffer = [ "netcdf %s {" % nc3file.name ]

    buffer.append("dimensions:")
    for dim in nc3file.dimensions:
        buffer.append("\t%s = %s ;" % (dim.name, dim.value))

    buffer.append("variables:")
    for var in nc3file.variables:
        buffer.append("\t%s ;" % var)
        for attr in var.attributes:
            buffer.append("\t\t%s:%s ;" % (var.name, attr))

    buffer.append("")
    buffer.append("// global attributes:")
    for attr in nc3file.attributes:
        buffer.append("\t\t:%s ;" % attr)

    buffer.append("}")

    fp = file(("%s.cdf" % nc3file.name), 'wb')
    fp.write('\n'.join(buffer) + '\n')
    fp.close()
