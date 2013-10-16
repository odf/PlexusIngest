#!/usr/bin/env python


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


def nc3info_from_directory(path):
    import os.path

    from file_cache import FileCache
    from nc3header import NC3Info

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
        raise RuntimeError("%s: no NetCDF files found." % path)
        
    # -- open the first file and return the object
    fp = FileCache(entries[0])

    try:
        info = NC3Info(fp)
    finally:
        fp.close()

    return info


if __name__ == "__main__":
    import sys, os.path
    
    path = sys.argv[1]
    name = os.path.splitext(os.path.basename(path))[0]
    nc3info = nc3info_from_directory(sys.argv[1])

    buffer = [ "netcdf %s {" % name ]

    buffer.append("dimensions:")
    for dim in nc3info.dimensions:
        buffer.append("\t%s = %s ;" % (dim.name, dim.value))

    buffer.append("variables:")
    for var in nc3info.variables:
        buffer.append("\t%s ;" % var)
        for attr in var.attributes:
            buffer.append("\t\t%s:%s ;" % (var.name, attr))

    buffer.append("")
    buffer.append("// global attributes:")
    for attr in nc3info.attributes:
        buffer.append("\t\t:%s ;" % attr)

    buffer.append("}")

    fp = file(("%s.cdf" % name), 'wb')
    fp.write('\n'.join(buffer) + '\n')
    fp.close()
