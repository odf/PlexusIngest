#!/usr/bin/env python

import os.path, re

from file_cache import FileCache
from nc3header import NC3Info


def datafiles(path):
    if os.path.isdir(path):
        return sorted(os.path.join(root, f)
                      for (root, dirs, files) in os.walk(path)
                      for f in files
                      if re.search(r'[._]nc(\.bz2)?$', f))
    else:
        return [ path ]


def nc3info(path):
    files = datafiles(path)
    if not files:
        raise RuntimeError("%s: no NetCDF files in directory" % path)
        
    fp = FileCache(files[0])

    try:
        info = NC3Info(fp)
    finally:
        fp.close()

    return info


if __name__ == "__main__":
    import sys
    
    path = sys.argv[1]
    name = os.path.splitext(os.path.basename(re.sub('/$', '', path)))[0]
    info = nc3info(path)

    buffer = [ "netcdf %s {" % name ]

    buffer.append("dimensions:")
    for dim in info.dimensions:
        buffer.append("\t%s = %s ;" % (dim.name, dim.value))

    buffer.append("variables:")
    for var in info.variables:
        buffer.append("\t%s ;" % var)
        for attr in var.attributes:
            buffer.append("\t\t%s:%s ;" % (var.name, attr))

    buffer.append("")
    buffer.append("// global attributes:")
    for attr in info.attributes:
        buffer.append("\t\t:%s ;" % attr)

    buffer.append("}")

    fp = file(("%s.cdf" % name), 'wb')
    fp.write('\n'.join(buffer) + '\n')
    fp.close()
