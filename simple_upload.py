#!/usr/bin/env python

"""
Simple script to upload data to Plexus.

-- TODO: method names and synopsis has changed

Uploads a collection of files to a specified sample. This can be called
either from the commandline or via the pattern

    connection = Connection(login, password)
    connection.post_update(project_name, sample_name, time, attachments)
    ...

where 'attachments' is a sequence of (content, name) pairs.

(Requires Python 2.4 or higher.)
"""

import httplib, mimetypes, os, os.path, re, time

from logger import Logger, LOGGER_ERROR

def encode_formdata(fields, files):
    """
    Composes an multipart/form-data message and returns its content
    type and body as a pair. Normal fields are specified as a sequence
    <fields> of (name, value) pairs. Attached file data is specified as
    a sequence <files> of (name, filename, value) elements.
    """
    
    BOUNDARY = '----------ThIs_Is_tHe_bouNdaRY_$'
    L = []
    
    for (key, value) in fields:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"' % key)
        L.append('')
        L.append(str(value))
        
    for (key, filename, value) in files:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"; filename="%s"'
                 % (key, filename))
        L.append('Content-Type: %s' % (mimetypes.guess_type(filename)[0]
                                       or 'application/octet-stream'))
        L.append('')
        L.append(str(value))
        
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = '\r\n'.join(L)
    
    content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
    
    return content_type, body


class Connection:
    """
    Holds connection and authentication data for the Plexus server and
    provides a method for uploading files.
    
    Mandatory constructor arguments are <user> and <password> for
    authentication with Plexus. Optional argument are
    
        <server>      - the base URL for the Plexus web application
        <manager>     - the manager for newly created projects,
        <interactive> - if True, asks for missing login and/or password
    """
    
    def __init__(self,
                 user,
                 password,
                 server      = "https://plexus.anu.edu.au",
                 manager     = None,
                 interactive = False):
        # -- if interactive, ask for authentication data with the service
        if interactive:
            import getpass
            while user in (None, ""):
                user = raw_input("Server login: ")
            while password in (None, ""):
                password = getpass.getpass("Server password: ")

        # -- copy data to instance attributes
        self.user     = user
        self.password = password
        self.server   = server
        self.manager  = manager or ""
            
        # -- set default retry parameters
        self.retry_limit = 10  # number of upload attempts
        self.retry_wait  = 300 # waiting period between upload attempts (sec)
        
        self.log = Logger()

    def post_form(self, selector, fields, files):
        """
        Composes an multipart/form-data message, posts it to the URL on
        the server given by the relative path <selector> and returns a
        tuple consisting of the HTTP status, reason and response text
        as received from the server.
        
        Normal fields are specified as a sequence <fields> of
        (name, value) pairs. Attached file data is specified as a
        sequence <files> of (name, filename, value) elements.
        """
        
        content_type, body = encode_formdata(fields, files)
        headers = {
            'User-Agent': 'python',
            'Content-Type': content_type,
            'Accept': 'application/json'
            }

        count = 0
        while True:
            try:
                if self.server.startswith("https://"):
                    host = re.sub('^https:\/\/', '', self.server)
                    self.log.writeln("Connecting to %s using https" % host)
                    h = httplib.HTTPSConnection(host)
                else:
                    host = re.sub('^http:\/\/', '', self.server)
                    self.log.writeln("Connecting to %s using http" % host)
                    h = httplib.HTTPConnection(host)
                        
                h.request('POST', selector, body, headers)
                res = h.getresponse()
                return res.status, res.reason, res.read()
            except KeyboardInterrupt, ex:
                raise ex
            except Exception, ex:
                self.log.writeln("> %s: %s <" % (ex.__class__.__name__, ex),
                                 LOGGER_ERROR)
                count += 1
                if count < self.retry_limit:
                    self.log.writeln("(will retry in %d seconds)"
                                     % self.retry_wait)
                    time.sleep(self.retry_wait)
                    self.log.writeln("retrying...")
                else:
                    self.log.writeln("too many retries - giving up",
                                     LOGGER_ERROR)
                    raise ex
                
    
    def post_info_request(self, project, sample):
        """
        Request information on data stored for a specified sample.

        Returns a tuple consisting of the HTTP status, reason and
        response text as received from the server.
        """
        
        # -- provide the mandatory fields for this request
        fields = (("user[name]", self.user),
                  ("user[password]", self.password),
                  ("project", project),
                  ("sample", sample))
        
        # -- post the request to the "/update" service and return the results
        return self.post_form("/samples/stored_data", fields, [])


    def post_import(self, project, sample, mtime, data, name, description,
                    replace = False):
        """
        Uploads a file '<name>' with content <data> and explanatory
        text '<description>' to the sample '<sample>' in the project
        '<project>'. Files were produced from primary data at time
        <mtime>. If <replace> is true, Plexus is asked to replace
        existing data where necessary.

        Returns a tuple consisting of the HTTP status, reason and
        response text as received from the server.
        """

        # -- provide the mandatory fields for this request
        fields = (("user[name]", self.user),
                  ("user[password]", self.password),
                  ("project", project),
                  ("sample", sample),
                  ("time", mtime),
                  ("manager", self.manager),
                  ("replace", str(replace)),
                  ("description", description))
        
        # -- convert attachment data into the form expected by Plexus
        uploads = (("data", name, data),)
        
        # -- post the request to the "/update" service and return the results
        return self.post_form("/imports", fields, uploads)


    def post_image(self, project, sample, attach_to, mtime, data, name,
                   caption, replace = False):
        """
        Uploads an image to Plexus. The parameters are as in
        post_import, except that <caption> takes on the role of
        <description> and the image is associated to the dataset with
        global identifier '<attach_to>'.
        """

        # -- provide the mandatory fields for this request
        fields = (("user[name]", self.user),
                  ("user[password]", self.password),
                  ("data_spec[project]", project),
                  ("data_spec[sample]", sample),
                  ("data_spec[identifier]", attach_to['IdExt']),
                  ("data_id", attach_to['IdInt']),
                  ("picture[caption]", caption),
                  ("time", mtime),
                  ("manager", self.manager),
                  ("replace", str(replace)))
        
        # -- convert attachment data into the form expected by Plexus
        uploads = (("picture[uploaded_data]", name, data),)
        
        # -- post the request to the "/update" service and return the results
        return self.post_form("/pictures", fields, uploads)


def parse_options():
    """
    Parses commandline arguments and options as passed via sys.argv.
    Returns a pair (options, args) where options is a dictionary and
    args the list of non-option arguments.
    """
    
    import optparse
    
    parser = optparse.OptionParser(
                      "usage: %prog [options] project sample file ...")
    parser.add_option("-u", "--user", dest = "user", metavar = "NAME",
                      help = "login name for Plexus")
    parser.add_option("-p", "--password", dest = "password", metavar = "TEXT",
                      help = "password for Plexus")
    parser.add_option("-P", "--plexus", dest = "plexus", metavar = "URL",
                      default = "https://plexus.anu.edu.au",
                      help = "the base URL for the Plexus web application")
    parser.add_option("-m", "--manager", dest = "manager", metavar = "NAME",
                      help = "the manager of any new project entries")
    parser.add_option("-q", "--quiet", dest = "verbose",
                      default = True, action = "store_false",
                      help = "suppress progress messages")
    
    (options, args) = parser.parse_args()
    if len(args) < 2:
        parser.error("expecting at least two arguments")
    
    return options, args


def contents(path):
    """
    Returns the contents of the file at location <path> as a string.
    """
    fp = file(path)
    data = fp.read()
    fp.close()
    return data


# -- TODO rewrite this using post_import etc

# def run():
#     """
#     Implements the command line interface. Program arguments and
#     options are passed via sys.argv. For usage details, call with '-h'
#     or refer to the parse_options() methods.
#     """

#     # -- parse options
#     (options, args) = parse_options()
    
#     # -- create an appropriate connection object
#     connection = Connection(user             = options.user,
#                             password         = options.password,
#                             options.manager  = options.manager,
#                             interactive      = True)

#     # -- collect and convert arguments for the post_update() call
#     project = args[0]
#     sample = args[1]
#     files = ((contents(name), name) for name in args[2:])
#     mtime = ''
#     if len(args) > 2:
#         mtime = time.asctime(time.localtime(os.path.getmtime(args[2])))

#     # -- post the request and print the result page
#     print connection.post_update(project, sample, mtime, files, False)[2]


# if __name__ == "__main__":
#     run()
