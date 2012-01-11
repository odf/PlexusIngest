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
    
    @property
    def value_as_string(self):
        """
        Produces a printable representation of the attribute's value.
        """
        
        def fmt(val):
            "Recursive inner method! Formats a value."
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
        
        return fmt(self.value)
                
    def __str__(self):
        return "%s = %s" % (self.name, self.value_as_string)
    

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


class NC3File:
    """
    Represents the complete header data from a NetCDF file. The
    constructor accepts a file system path and an optional name for the
    data set. All methods are for internal use only.
    
    Useful properties:
    
    path          - the file's location on the file system
    name          - the name to be used for the associated data set
    dimensions    - the list of dimensions defined (type NC3Dimension)
    attributes    - the list of attributes defined (type NC3Attribute)
    variables     - the list of variables (type NC3Variable)
    header_size   - the header size on file in bytes
    header_as_cdl - the header in readable form using the CDL format
    """
    def __init__(self, path, name = None):
        # -- remember the file system path
        self.path = path
        
        # -- use the file name as the data set name if none was given
        self.name = basenameNetCDF(name or os.path.basename(path))
            
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
        self.log.writeln("Header size is %d." % self.header_size)
        self.log.leave()
        
    def parse_header(self):
        magic = self.get_values(NC_CHAR, 4)
        if magic != "CDF\001":
            raise NC3Error(self, "Not a NetCDF version 1 file.")
        self.numrecords = self.read_non_negative()
        self.dimensions = self.read_dimensions()
        self.attributes = self.read_attributes()
        self.variables  = self.read_variables()
        self.header_size = self.file.tell()
        self.file.seek(0)
        self.fingerprint = hexdigest(self.file.read(self.header_size))
        
    def get_values(self, type_code, number):
        type = NC_TYPE[type_code]
        size = type.size * number
        value = self.file.read(size)
        if len(value) < size:
            raise NC3Error(self, "Premature end of file.")
        self.file.seek(3 - (size + 3) % 4, SEEK_CUR)
        if type_code == NC_CHAR:
            return value
        else:
            return struct.unpack(">%d%s" % (number, type.py_code), value)
    
    def read_integer(self):
        return self.get_values(NC_LONG, 1)[0]
    
    def read_non_negative(self):
        n = self.read_integer()
        if n < 0:
            raise NC3Error(self, "Non-negative number expected")
        return n
    
    def read_string(self):
        size = self.read_non_negative()
        return self.get_values(NC_CHAR, size)
    
    def read_dimensions(self):
        self.log.writeln("Dimensions:", LOGGER_TRACE)
        self.log.enter()
        
        dimensions = []
        tag = self.read_integer()
        ndims = self.read_non_negative()
        if tag == NC_DIMENSION:
            for i in range(ndims):
                name = self.read_string()
                size = self.read_non_negative()
                dimensions.append(NC3Dimension(name, size))
                self.log.writeln("%s" % dimensions[-1], LOGGER_TRACE)
        elif tag != 0 or ndims != 0:
            raise NC3Error(self, "Expected dimension array.")
        
        self.log.leave()
        return dimensions
    
    def read_attributes(self):
        self.log.writeln("Attributes:", LOGGER_TRACE)
        self.log.enter()
        
        attributes = []
        tag = self.read_integer()
        nattr = self.read_non_negative()
        if tag == NC_ATTRIBUTE:
            for i in range(nattr):
                name = self.read_string()
                type = self.read_integer()
                size = self.read_non_negative()
                values = self.get_values(type, size)
                attributes.append(NC3Attribute(name, values))
                self.log.writeln("%s" % attributes[-1], LOGGER_TRACE)
        elif tag != 0 or nattr != 0:
            raise NC3Error(self, "Expected attribute array.")
        
        self.log.leave()
        return attributes
    
    def read_variables(self):
        self.log.writeln("Variables:", LOGGER_TRACE)
        self.log.enter()
        
        variables = []
        tag = self.read_integer()
        nvars = self.read_non_negative()
        if tag == NC_VARIABLE:
            for i in range(nvars):
                name = self.read_string()
                ndims = self.read_non_negative()
                dims = tuple( self.dimensions[self.read_non_negative()]
                              for x in range(ndims))
                self.log.enter()
                attr = self.read_attributes()
                self.log.leave()
                nc_type = self.read_integer()
                size = self.read_non_negative()
                start = self.read_non_negative()
                variables.append(NC3Variable(name, dims, attr,
                                             nc_type, size, start))
                self.log.writeln("%s" % variables[-1], LOGGER_TRACE)
        elif tag != 0 or nvars != 0:
            raise NC3Error(self, "Expected variable descriptions.")
        
        self.log.leave()
        return variables
    
    @property
    def merged_attributes(self):
        result = {}
        for attr in self.attributes:
            result.setdefault(attr.name, attr.value)
        for var in self.variables:
            for attr in var.attributes:
                result.setdefault(attr.name, attr.value)
        return result
    
    @property
    def header_as_cdl(self):
        # -- write the first line
        buffer = [ "netcdf %s {" % self.name ]
        
        # -- write the dimensions
        buffer.append("dimensions:")
        for dim in self.dimensions:
            buffer.append("\t%s = %s ;" % (dim.name, dim.value))
        
        # -- write the variables
        buffer.append("variables:")
        for var in self.variables:
            buffer.append("\t%s ;" % var)
            for attr in var.attributes:
                buffer.append("\t\t%s:%s ;" % (var.name, attr))
                
        # -- write the global attributes
        buffer.append("")
        buffer.append("// global attributes:")
        for attr in self.attributes:
            buffer.append("\t\t:%s ;" % attr)
        
        # -- write the final line
        buffer.append("}")
        
        # -- return the joined result
        return '\n'.join(buffer) + '\n'

    def close(self):
        pass


class NC3HeaderInfo:
    """
    Given a file system location <path>, extracts header data for the
    NetCDF file at that location.
    
    If the given location is a directory, its last path component is
    used to determine the output file name and its alphabetically first
    entry opened to extract header data.
    
    The following fields are available:
      text     - the header as a printable text in CDL format
      size     - the original header size in the NetCDF source file
      filename - the suggested name of the file to write CDL output to
    """
    
    def __init__(self, path):
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
        
        # -- open the first file and extract the header
        nc3file = NC3File(entries[0], name)
        self.text = nc3file.header_as_cdl
        self.size = nc3file.header_size
        self.fingerprint = nc3file.fingerprint
        self.header_path = nc3file.path
        self.filename = "%s.cdf" % nc3file.name
        nc3file.close()


if __name__ == "__main__":
    import sys
    
    i = 1
    if sys.argv[i].startswith("-v"):
        if sys.argv[i].startswith("-vv"):
            Logger().priority = LOGGER_TRACE
        else:
            Logger().priority = LOGGER_INFO
        i += 1
    info = NC3HeaderInfo(sys.argv[i])
    fp = file(info.filename, 'wb')
    fp.write(info.text)
    fp.close()
