# Convenience script - extracts Plexus import file and slice images from NetCDF
#
# (c)2010 ANUSF

if __name__ == "__main__":
    import sys, os, os.path, time

    from logger import Logger, LOGGER_INFO
    from file_cache import FileCache
    from history import History
    from make_slices import Slicer
    
    Logger().priority = LOGGER_INFO

    i = 1
    if sys.argv[i] == '-c':
        FileCache.cache_location = "/local/projects/d59/assets/nc3cache.db"
        i += 1
    fname = sys.argv[i]
    h = History(fname, fname, time.gmtime(os.stat(fname).st_mtime))
    fp = file(os.path.basename(fname) + ".json", "w")
    fp.write(h.as_json)
    fp.close()

    for (data, name, action) in Slicer(fname).slices:
        fp = file(name, 'wb')
        fp.write(data)
        fp.close()
