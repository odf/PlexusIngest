#!/usr/bin/env python
# Convenience script - extracts Plexus import file and slice images from NetCDF
#
# (c)2013 ANUSF

if __name__ == "__main__":
    import sys, os, os.path, time
    import optparse

    from logger import *
    from file_cache import FileCache
    from history import History
    from make_slices import slices
    from nc3header import nc3info_from_directory
    
    Logger().priority = LOGGER_INFO

    parser = optparse.OptionParser("usage: %prog [options] path ...")
    parser.add_option("-n", "", dest = "mock_slices",
                      default = False, action = "store_true",
                      help = "use test images instead of real slices")
    parser.add_option("-c", "--cache-location", dest = "cache_location",
                      metavar = "PATH", help = "where to cache NetCDF headers")
    (options, args) = parser.parse_args()

    if options.cache_location:
        FileCache.cache_location = options.cache_location
    mock = options.mock_slices

    fname = args[0]
    info = nc3info_from_directory(fname)
    h = History(info, fname, time.gmtime(os.stat(fname).st_mtime))
    fp = file(os.path.basename(fname) + ".json", "w")
    fp.write(h.as_json)
    fp.close()

    for (data, name, action) in slices(fname, dry_run = mock):
        fp = file(name, 'wb')
        fp.write(data)
        fp.close()
