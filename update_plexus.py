#!/usr/bin/env python

"""
Uploads NetCDF data to Plexus.

Recursively uploads data from a given file or directory, including
header information and slice images of volume data. File system paths
are used to determine the project and sample entries to upload data to.

This can be called either from the commandline or via the pattern

    updater = Updater(login, password)
    updater.go(path)
    ...

(Requires Python 2.4 or higher.)
"""

import math, os, os.path, sys, time, traceback, yaml

import json

from file_cache import FileCache
from logger import *
from nc3header import NC3HeaderInfo, NC3File, NC3Error
from history import History
from make_slices import Slicer
from simple_upload import Connection


class Updater(Connection):
    """
    Holds connection and authentication data for the Plexus server and
    provides a method for uploading files. Inherits from the Connection
    class defined in simple_upload.
    
    Mandatory constructor arguments are <user> and <password> for
    authentication with Plexus. Optional argument are
    
        <plexus>      - the base URL for the Plexus web application.
        <manager>     - the manager for newly created projects,
        <interactive> - if True, asks for missing login and/or password
        <make_slices> - if True, creates slice images from volume data
        <replace>     - if True, replaces existing files
        <dry_run>     - if True, only lists actions that would have been taken
        <mock_slices> - if True, produces placeholder images for slices
        <output>      - object to send output to (default: sys.stdout)
    """
    
    MAX_ERRORS = 1000
    
    def __init__(self,
                 user,
                 password,
                 plexus      = "https://plexus.anu.edu.au",
                 manager     = None,          
                 interactive = False,
                 make_slices = True,
                 replace     = False,
                 dry_run     = False,
                 mock_slices = False,
                 output      = sys.stdout):
        Connection.__init__(self,
                            user,
                            password,
                            server      = plexus,
                            manager     = manager,
                            interactive = interactive)
        self.log         = Logger()
        self.make_slices = make_slices
        self.replace     = replace
        self.set_output(output)
        self.dry_run     = dry_run
        self.mock_slices = mock_slices

        self.min_age     = 0
        self.max_age     = 0
        
        self.error_count = 0
        self.last_project = self.last_sample = self.last_path = None
        self.header_sizes = []

    def log_error(self, text):
        """
        Formats the error message <text> and prints onto the standard
        error channel. Counts all errors logged for this instance and
        terminates the process when that count reaches 'MAX_ERROR'.
        """
        self.log.writeln("\n".join("> %s <" % s for s in text.split("\n")),
                         LOGGER_ERROR)
        self.error_count += 1
        if self.error_count > self.MAX_ERRORS > 0:
            self.log.writeln(">>> Too many errors - exiting. <<<", LOGGER_PANIC)
            sys.exit(1)

    def log_exception(self, message = None):
        """
        Produces a short stack trace for the last exception encountered
        and passes the result to 'log_error()'.
        """
        
        if message:
            msg = [message]
        else:
            msg = []
        (type, value, _) = sys.exc_info()
        if type in (NC3Error, OSError):
            tb = [ "| %s: %s" % (type.__name__, value) ]
        else:
            tb = list("| " + line
                      for line in traceback.format_exc(10).split("\n"))
        self.log_error("\n".join(msg + tb))

    def print_action(self, project, sample, path, name, action):
        """
        Prints out an upload action. To make the output more readable,
        the last project, sample and path given are remembered and only
        printed upon change.
        """
        
        if project != self.last_project:
            self.last_sample = self.last_path = None
            self.last_project = project
            self.output.write("PROJECT %s\n" % project)
        if sample != self.last_sample:
            self.last_path = None
            self.last_sample = sample
            self.output.write("  SAMPLE %s\n" % sample)
        if path != self.last_path:
            self.last_path = path
            self.output.write("    PATH %s\n" % path)
        if name.endswith(".png") or name.endswith(".jpg"):
            self.output.write("  ")
        self.output.write("      %-7s %s\n" % (action, name))
        self.output.flush()

    def known_files(self, project, sample):
        """
        Queries Plexus for the names of all files uploaded in the given
        project and sample. Returns a list of strings.
        """
        
        try:
            # -- post request to Plexus
            (status, reason, response) = self.post_info_request(project, sample)
            if status != 200:
                self.log_error("Request failed: %d - %s" % (status, reason))
                return None
            # -- parse the response as JSON
            response = json.loads(response)
        except KeyboardInterrupt, ex:
            raise ex
        except Exception:
            self.log_exception()
            return None

        # -- extract the list of node names
        seen = {}
        for n in response.get('Nodes') or []:
            name = n['Name']
            old = seen.get(name)
            if old is None or not old['External'] or old['Date'] < n['Date']:
                seen[name] = n
            
        # -- print the result for debugging purposes
        self.log.writeln("Found in database: (%s)" %
                         ', '.join(k for k in seen if seen[k]['External']))
        
        # -- return the list of names
        return seen

    def upload_files(self, project, sample, time, files, attach_to = None):
        """
        Uploads the files specified by the sequence <files> of
        (data, name) pairs to the sample '<sample>' in the project
        '<project>'. Files were produced from primary data at time
        <time>. If 'self.replace' is true, Plexus is asked to replace
        existing data where necessary.
        
        Prints the response text to 'self.output' and returns a boolean
        indicating whether the upload was successful.
        """
        
        if len(files) != 1:
            raise "Exactly one attachment expected!"
        data = files[0][0]
        name = files[0][1]

        if attach_to:
            (status, reason, response) = self.post_image(
                project, sample, attach_to, time, data, name,
                name[:name.find("_")], self.replace)
        else:
            description = "Import generated by %s\n* File: %s\n* Date: %s" % (
                sys.argv[0], os.path.basename(name), time)
            (status, reason, response) = self.post_import(
                project, sample, time, data, name, description, self.replace)

        bad = False
        if status == 200:
            output = json.loads(response)
            self.output.write(yaml.dump(output, default_flow_style = False,
                                        explicit_start = True))
            status = output['Status']
            if status in ["Error", "Failure"]:
                bad = True
            elif status == "Partial success":
                created = output.get("Files") or []
                if len(created) == 1 and created[0].get("MainNodeID"):
                    status = "Success"
            elif status == "Mixed" and output.get("MainNodeID"):
                status = "Success"
        else:
            output = []
            status = "%d - %s" % (status, reason.replace("\n", ""))
            bad = True
            
        if bad:
            self.log_error("Upload failed: " + status)
        else:
            self.log.writeln("Upload okay: " + status)
        
        return status, output

    def update_slices(self, path, project = None, sample = None,
                      info = None, timestring = None):
        """
        Creates and uploads slice images for a single NetCDF data set at
        location <path>. If <project> and <sample> are not specified,
        they are extracted from the absolute path. Additional information
        is passed in <info> and <timestring>.
        
        The response received from Plexus is written to self.output.
        """

        if not (info.get('IdExt') or info.get('IdInt')):
            return

        seen = info['Images']

        if self.dry_run:
            slicer = Slicer(path, seen, self.replace, true)
            for (data, name, action) in slicer.slices:
                self.print_action(project, sample, os.path.dirname(path),
                                  name, action)
        else:
            history = History(path, path, time.gmtime(os.path.getmtime(path)))
            main = history.main_process().record
            meta = dict((k, main[k]) for k in ["data_file",
                                               "data_type",
                                               "date",
                                               "domain",
                                               "identifier",
                                               "name",
                                               "predecessors",
                                               "process",
                                               "run_by"])

            slicer = Slicer(path, seen, self.replace, self.mock_slices,
                            sizes = (None, (80,80), (120,120)), info = meta)
            for (data, name, action) in slicer.slices:
                self.upload_files(project, sample, timestring,
                                  ((data, name),), info)

    def update_item(self, path, project = None, sample = None, seen = None):
        """
        Uploads the data for a single NetCDF data set at location
        <path>. If <project> and <sample> are not specified, they are
        extracted from the absolute path. If <seen> is present, it is
        assumed to contain the names of nodes already uploaded to
        Plexus; otherwise Plexus is queried for the list.
        
        The response received from Plexus is written to self.output.
        """
        
        # -- extract project and sample names if not given
        path = os.path.abspath(path)
        dir = os.path.dirname(path)
        mtime = os.path.getmtime(path)
        project = project or os.path.basename(os.path.dirname(dir))
        sample = sample or os.path.basename(dir)

        # -- extract the associated data node name
        name = os.path.basename(path)

        # -- handle container directories
        if name.startswith("cntr_"):
            #TODO treat containers correctly (when Mango implements them)
            #self.update_container(path, project, sample, seen)
            return

        # -- extract other relevant information
        location = os.path.dirname(path)
        if name.endswith("_nc") or name.endswith(".nc"):
            name = name[:-3]
        self.log.writeln("Processing item '%s'..." % name)
        self.log.enter()

        try:
            # -- determine list of nodes known to Plexus, if not given
            if seen is None:
                seen = self.known_files(project, sample)
                if seen is None:
                    self.log.writeln("Unable to contact Plexus - giving up.",
                                     LOGGER_ERROR)
                    self.log.leave()
                    return
    
            # -- determine which action to take
            if name in seen.keys():
                old_time = seen[name]["Date"]
                new_time = time.strftime("%Y/%m/%d %H:%M:%S UTC",
                                         time.gmtime(mtime))
                if old_time < new_time:
                    seen[name]['Images'] = []
                if not seen[name]['External']:
                    self.log.writeln("Updating metadata...")
                    action = "UPDATE"
                elif self.replace or old_time < new_time:
                    self.log.writeln("Replacing metadata...")
                    action = "REPLACE"
                else:
                    self.log.writeln("Skipping metadata.")
                    action = "SKIP"
            else:
                self.log.writeln("Adding metadata...")
                action = "ADD"
            
            # -- extract and upload the header data if appropriate
            t = time.strftime("%Y/%m/%d %H:%M:%S UTC", time.gmtime(mtime))
            seen.setdefault(name, { 'Images': [] })
            if action != "SKIP":
                info = NC3HeaderInfo(path)
                self.header_sizes.append(info.size)
                if self.dry_run:
                    self.print_action(project, sample, location, name, action)
                else:
                    data = History(path, path, time.gmtime(mtime)).as_json
                    _, res = self.upload_files(project, sample,
                                               t, ((data, path),))
                    seen[name]['IdExt'] = res.get('MainNodeExternalID')
                    seen[name]['IdInt'] = res.get('MainNodeID')
    
            self.log.writeln(str(seen[name]))

            # -- extract and upload the slices if appropriate
            if self.make_slices:
                self.update_slices(path, project, sample, seen[name], t)

        except KeyboardInterrupt, ex:
            raise ex
        except:
            self.log_exception("Skipping item because of errors.")

        self.log.leave()

    def age_okay(self, path):
        age = time.time() - os.path.getmtime(path)
        return (self.max_age == 0
                or age <= self.max_age) and age >= self.min_age

    def update_container(self, path, project = None, sample = None,
                         seen = None):
        self.update_collection(path, project, sample, seen, "container")

    def update_sample(self, path, project = None, sample = None, seen = None):
        self.update_collection(path, project, sample, seen, "sample")

    def update_collection(self, path, project = None, sample = None,
                          seen = None, kind = "sample"):
        """
        Finds all NetCDF data sets in the sample or container
        directory at <path> and uploads those not already known to
        Plexus. If <project> and <sample> are not specified, they are
        extracted from the absolute path.
        """

        # -- extract project and sample names if not given
        path = os.path.abspath(path)
        project = project or os.path.basename(os.path.dirname(path))
        sample = sample or os.path.basename(path)
        
        try:
            if os.access(path, os.R_OK):
                # -- compose list of potential data sets under this directory
                entries =  list(os.path.join(path, f)
                                for f in os.listdir(path)
                                if self.has_volume_data(f)
                                if not f.startswith('analysis_')
                                if not f.startswith('fiducial')
                                if not f.startswith('experiment')
                                if not f.startswith('block0')
                                if self.age_okay(os.path.join(path, f)))

                if entries:
                    if kind == "sample":
                        self.log.writeln("Processing sample '%s' in '%s'..."
                                         % (sample, project))
                    else:
                        self.log.writeln("Processing %s '%s'..."
                                         % (kind, os.path.basename(path)))
                    self.log.enter()
                    # -- get the list of known data sets from Plexus
                    if seen is None:
                        seen = self.known_files(project, sample)
            
                    # -- call update_item to handle each data set
                    for file in entries:
                        self.update_item(file, project, sample, seen)
            
                    self.log.leave()
            else:
                # -- directory has no read access
                self.log_error("cannot access " + path)
        except KeyboardInterrupt, ex:
            raise ex
        except:
            self.log_exception("Skipping %s because of errors." % kind)

    def update_project(self, path, project = None):
        path = os.path.abspath(path)
        project = project or os.path.basename(path)
        
        try:
            if os.access(path, os.R_OK):
                self.log.writeln("Processing project '%s'..." % project)
                self.log.enter()
                for name in os.listdir(path):
                    self.update_sample(os.path.join(path, name), project)
                self.log.leave()
            else:
                self.log_error("cannot access " + path)
        except KeyboardInterrupt, ex:
            raise ex
        except:
            self.log_exception("Skipping project because of errors.")
        
    def update_repository(self, path):
        path = os.path.abspath(path)
        repo = os.path.basename(path)
        
        try:
            if os.access(path, os.R_OK):
                self.log.writeln("Processing repository '%s'..." % repo)
                self.log.enter()
                for name in os.listdir(path):
                    self.update_project(os.path.join(path, name))
                self.log.leave()
            else:
                self.log_error("cannot access " + path)
        except KeyboardInterrupt, ex:
            raise ex
        except:
            self.log_exception("Skipping repository because of errors.")
        
    def has_volume_data(self, path):
        """
        Checks the file name <path> to see if it indicates a file or
        directory with NetCDF volume data.
        """
        return (path.endswith(".nc") or path.endswith("_nc") or
                path.endswith(".nc.bz2") or path.endswith("_nc.bz2"))

    def is_sample_dir(self, path):
        """
        Checks the directory name <path> to see if the specified
        directory contains any files or subdirectories that seem to
        contain NetCDF volume data.
        """
        if os.access(path, os.R_OK):
            for f in os.listdir(path):
                if self.has_volume_data(f):
                    return True
        else:
            self.log_error("Can't access: " + path)
        return False

    def go(self, path, max_files = 100, start_level = None):
        """
        Inspects the given <path> and, if it seems to contain volume
        data, uploads that data to Plexus, or else searches for sample
        directories under that location, then updates each sample found
        in turn. Subdirectories of potential sample directories are
        ignored.
        """
        
        path = os.path.abspath(path)
        if start_level == "dataset":
            self.update_item(path)
        elif start_level == "sample":
            self.update_sample(path)
        elif start_level == "project":
            self.update_project(path)
        elif start_level == "repository":
            self.update_repository(path)
        elif self.has_volume_data(path):
            # -- upload volume data directly
            self.update_item(path)
        elif not os.path.isdir(path):
            # -- neither a NetCDF file nor a directory
            self.log_error("nothing to upload in " + path)
        elif not os.access(path, os.R_OK):
            # -- directory with no read access
            self.log_error("cannot access " + path)
        else:
            # -- recursively look for sample directories
            for (root, dirs, files) in os.walk(path):
                if self.is_sample_dir(root):
                    # -- upload sample data
                    self.update_sample(root)
                    # -- ignore subdirectories further down
                    dirs[:] = []
                    # -- terminate if too many files were opened
                    if FileCache.file_count > max_files > 0:
                        self.log.writeln("Too many files opened - terminating.",
                                         LOGGER_WARNING)
                        break

    def close(self):
        """
        Flushes and - if appropriate - closes the output channel for
        this instance.
        """
        self.output.flush()
        if not self.output in (sys.stdout, sys.stderr):
            self.output.close()

    @property
    def output(self):
        return self._output
    
    def set_output(self, stream):
        self._output = stream
        self.log.stream = stream


def parse_options():
    """
    Parses commandline arguments and options as passed via sys.argv.
    Returns a pair (options, args) where options is a dictionary and
    args the list of non-option arguments.
    """
    
    import optparse
    
    parser = optparse.OptionParser("usage: %prog [options] path ...")
    parser.add_option("-u", "--user", dest = "user", metavar = "NAME",
                      help = "login name for Plexus")
    parser.add_option("-p", "--password", dest = "password", metavar = "TEXT",
                      help = "password for Plexus")
    parser.add_option("-f", "--force", dest = "force", default = False,
                      action = "store_true",
                      help = "re-upload entries already in database")
    parser.add_option("-x", "--exclude-pictures", dest = "make_slices",
                      default = True, action = "store_false",
                      help = "skip slice picture creation")
    parser.add_option("-P", "--plexus", dest = "plexus", metavar = "URL",
                      default = "https://plexus.anu.edu.au",
                      help = "the base URL for the Plexus web application")
    parser.add_option("-m", "--manager", dest = "manager", metavar = "NAME",
                      help = "the manager of any new project entries")
    parser.add_option("-n", "--dry-run", dest = "dry_run", default = False,
                      action = "store_true",
                      help = "do nothing, only print actions")
    parser.add_option("", "--mock-slices", dest = "mock_slices",
                      default = False, action = "store_true",
                      help = "skip slice generation and upload test images")
    parser.add_option("-o", "--output", dest = "output", metavar = "FILE",
                      default = "-", help = "output file")
    parser.add_option("-q", "--quiet", dest = "verbose",
                      default = True, action = "store_false",
                      help = "suppress progress messages")
    parser.add_option("", "--retry-limit", dest = "retry_limit", metavar = "NR",
                      default = 0, type = "int",
                      help = "how often to retry connecting to Plexus")
    parser.add_option("", "--retry-wait", dest = "retry_wait", metavar = "NR",
                      default = 0, type = "int",
                      help = "waiting period (sec) before retrying a connection")
    parser.add_option("", "--max-files", dest = "max_files", metavar = "NR",
                      default = 100, type = "int",
                      help = "limits the number of uncached NetCDF file reads")
    parser.add_option("", "--force-cache", dest = "force_cache",
                      default = False, action = "store_true",
                      help = "create a cache for NetCDF headers if none exists")
    parser.add_option("", "--cache-location", dest = "cache_location",
                      metavar = "PATH", help = "where to cache NetCDF headers")
    parser.add_option("", "--cache-root", dest = "cache_root", metavar = "PATH",
                      help = "ignored initial path segment for cache lookup")
    parser.add_option("", "--max-age", dest = "max_age", metavar = "AGE",
                      help = "maximal file age in seconds or specified unit")
    parser.add_option("", "--min-age", dest = "min_age", metavar = "AGE",
                      help = "minimal file age in seconds or specified unit")
    parser.add_option("", "--repository", dest = "start_level",
                      action = "store_const", const = "repository")
    parser.add_option("", "--project", dest = "start_level",
                      action = "store_const", const = "project")
    parser.add_option("", "--sample", dest = "start_level",
                      action = "store_const", const = "sample")
    parser.add_option("", "--dataset", dest = "start_level",
                      action = "store_const", const = "dataset")
    
    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.error("expecting at least one argument")
    
    return options, args


def parse_age(s):
    import re
    
    if s is None:
        return 0
    
    (amount, unit) = re.sub(r"(\d+)(\D+)", r"\1 \2", s + " s").split()[:2]
    amount = int(amount)
    if 'seconds'.startswith(unit):
        return amount
    elif 'minutes'.startswith(unit):
        return amount * 60
    elif 'hours'.startswith(unit):
        return amount * 3600
    elif 'days'.startswith(unit):
        return amount * 3600 * 24
    elif 'weeks'.startswith(unit):
        return amount * 3600 * 24 * 7
    elif 'years'.startswith(unit): # approximate average year
        return amount * 3600 * (24 * 365 + 6)
    elif 'months'.startswith(unit): # one twelfth of a year
        return amount * 300 * (24 * 365 + 6)


def run():
    """
    Implements the command line interface. Program arguments and
    options are passed via sys.argv. For usage details, call with '-h'
    or refer to the parse_options() methods.
    """
    
    # -- parse options
    (options, args) = parse_options()
    
    # -- create an appropriate connection object
    updater = Updater(user        = options.user,
                      password    = options.password,
                      plexus      = options.plexus,
                      manager     = options.manager,
                      interactive = True,
                      make_slices = options.make_slices,
                      replace     = options.force,
                      dry_run     = options.dry_run,
                      mock_slices = options.mock_slices)
    
    if options.output != "-":
        updater.set_output(open(options.output, "a"))
    if options.retry_limit > 0:
        updater.retry_limit = options.retry_limit
    if options.retry_wait > 0:
        updater.retry_wait = options.retry_wait
    
    # -- process cache options
    FileCache.cache_location = options.cache_location
    FileCache.cache_root     = options.cache_root
    FileCache.force_cache    = options.force_cache
    
    # -- process age limits
    updater.min_age = parse_age(options.min_age)
    updater.max_age = parse_age(options.max_age)
    
    # -- log start time
    updater.log.writeln("Scan started at %s" % time.ctime())
    
    # -- upload data from the given paths
    for path in args:
        updater.go(path, options.max_files, options.start_level)
    
    # -- log end time and print some statistics
    updater.log.writeln("Scan finished at %s" % time.ctime())
    updater.log.writeln("Read new headers from %d files." % FileCache.file_count)
 
    # -- flush any output from the updater object
    updater.close()


if __name__ == "__main__":
    Logger().priority = LOGGER_INFO
    run()
