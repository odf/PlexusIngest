#!/usr/bin/env python
# Convenience script - extracts Plexus import file and slice images from NetCDF
#
# (c)2013 ANUSF

if __name__ == "__main__":
    import sys, os, os.path, time, re
    import optparse

    from logger import *
    from file_cache import FileCache
    from history import History
    from make_slices import slices
    from nc3files import nc3info
    
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
    info = nc3info(fname)
    h = History(info, fname, time.gmtime(os.stat(fname).st_mtime))
    fp = open(os.path.basename(re.sub('/$', '', fname)) + ".json", "w")
    fp.write(as_json(h))
    fp.close()

    for (data, name, action) in slices(fname, dry_run = mock):
        fp = file(name, 'wb')
        fp.write(data)
        fp.close()
