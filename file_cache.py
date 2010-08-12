import os, os.path, shelve

from logger import Logger

# -- provide symbolic names for seek() modes
if hasattr(os, "SEEK_SET"):
    SEEK_SET = os.SEEK_SET
    SEEK_CUR = os.SEEK_CUR
    SEEK_END = os.SEEK_END
else:
    SEEK_SET = 0
    SEEK_CUR = 1
    SEEK_END = 2


class FileCache:
    """
    This class provides cached read-only access to files on the local
    file system. The constructor accepts a path argument. Instances
    provide the methods read(), size(), tell(), seek() and close().
    File contents are buffered within an open instance at least up to
    the highest position of any previous read.
    
    Caching is enabled by setting the class property 'cache_location'
    to a valid file system path. If the cache file does not exist, it
    is only created if the class property 'force_cache' is true. If
    caching is disabled, files are read directly from disk.
    
    With caching enabled, the current buffer contents are stored upon
    execution of the close() method. Cached data is only used in
    subsequent reads if the path, modification date and file size all
    match.
    
    The class property 'cache_limit' imposes a hard limit on the size
    of the initial segment of a file that can be accessed.

    The class property 'file_count' is incremented for each uncached
    file access.
    """
    
    cache_location = None
    cache_root = None
    force_cache = False
    cache_limit = 512 * 1024
    file_count = 0

    def __init__(self, path):
        self.log    = Logger() # logging information is sent here
        self.path   = path     # the file path
        self.buffer = ""       # the current buffer contents
        self.offset = 0        # offset for the next read

        if self.cache_root and self.path.startswith(self.cache_root):
            self.cache_path = self.path[len(self.cache_root):]
        else:
            self.cache_path = self.path

        # -- get information about the actual file
        self.stat = os.stat(self.path)
        
        # -- get the cached data for the file
        data = self.__get(self.cache_path)
        
        # -- check if the cached data can be used
        if data is not None:
            if (data["mtime"] == self.stat.st_mtime and
                data["size"] == self.stat.st_size
                ):
                self.log.writeln("Using cached data.")
                self.buffer = data["buffer"]
            else:
                self.log.writeln("Cached data is stale.")
        
    @classmethod
    def __cache(cls):
        """
        Internal class method! A generator iterator that yields the
        current cache, if any.
        """
        if cls.cache_location and (cls.force_cache or
                                   os.path.exists(cls.cache_location)):
            yield shelve.open(cls.cache_location)
    
    @classmethod
    def __get(cls, key):
        """
        Internal class method! Retrives the value associated to <key>
        from the current cache.
        """
        for cache in cls.__cache():
            try:
                return cache.get(key)
            finally:
                cache.close()
        return None
    
    @classmethod
    def __put(cls, key, data):
        """
        Internal class method! Associates the value <data> to the key
        <key> within current cache.
        """
        for cache in cls.__cache():
            try:
                if data != cache.get(key):
                    cache[key] = data
            finally:
                cache.close()

    def grow_buffer(self, size):
        """
        Used by read() to ensure that sufficient data from the file is
        copied into the instance's buffer.
        """
        
        # -- see if anything needs to be read
        if len(self.buffer) >= size:
            return
        
        # -- never read beyond a certain point
        if size > self.cache_limit:
            raise RuntimeError("Cache limit exceeded.")
        
        # -- keep the file system access counter up to date
        if len(self.buffer) == 0:
            self.__class__.file_count += 1
        
        # -- adjust the buffer size in order to reduce os.read() calls
        n = max(len(self.buffer), 4096)
        while n < size:
            n *= 2
        if hasattr(self.stat, "st_blksize"):
            k = self.stat.st_blksize
            n = (n + k - 1) / k * k
        
        # -- fill the buffer, but don't keep the file open
        fd = os.open(self.path, os.O_RDONLY)
        try:
            self.log.writeln("Reading first %d bytes from file..." % n)
            self.buffer = os.read(fd, n)
        finally:
            os.close(fd)
        
        # -- if the file has changed on disk, we are in trouble
        new_stat = os.stat(self.path)
        if (new_stat.st_mtime != self.stat.st_mtime or
            new_stat.st_size != self.stat.st_size
            ):
            raise RuntimeError("File changed on disk while reading.")
        
    # -- the following methods work as in standard file objects:
    def close(self):
        self.__put(self.cache_path, { "mtime":  self.stat.st_mtime,
                                      "size":   self.stat.st_size,
                                      "buffer": self.buffer[:self.offset] })

    def size(self):
        return self.stat.st_size
    
    def tell(self):
        return self.offset
    
    def seek(self, offset, mode = SEEK_SET):
        self.offset = { SEEK_SET: 0,
                        SEEK_CUR: self.offset,
                        SEEK_END: self.size() }[mode] + offset
        
    def read(self, size):
        start = self.offset
        self.offset += size
        self.grow_buffer(self.offset)
        return self.buffer[start: self.offset]
