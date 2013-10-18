#!/usr/bin/env python

import os.path, re, time

import json

from logger import *


TYPE2PREFIX = {
  "Projection_Set":                  "proj",
  "Tomographic_Data":                "tomo",
  "Tomographic_Data_Floating_Point": "tomo_float",
  "Tomographic_Data_Container":      "cntr_tomo",
  "Segmented_Data":                  "segmented",
  "Distance_Map_Data":               "distance_map",
  "Medial_Axis_Data":                "medial_axis",
  "Label_Data":                      "labels"
}

PREFIX2TYPE = dict((value, key) for (key, value) in TYPE2PREFIX.items())

# Flattens a hierarchy of dictionaries.
# Keys and leaf values are presumed to be strings.
def flatten(input, prefix = None):
    output = {}
    for k, val in input.items():
        if prefix:
            key = prefix + '.'
        else:
            key = ''
        key += re.sub(r'\.', "_", k)
        if isinstance(val, dict):
            output.update(flatten(val, key))
        else:
            output[key] = val
    return output


class Patterns:
    sectionStart= re.compile(r'^\s*BeginSection\b')
    sectionEnd  = re.compile(r'^\s*EndSection\b')
    moduleStart = re.compile(r'^\s*#+\s+Results\s+from\s+(module\s+)?<(.*)>\s*:')
    namePrefix  = re.compile(r'^[a-z_]*[a-z]')
    timeForID   = '%Y%m%d_%H%M%S'
    timeFormat  = "%Y/%m/%d %H:%M:%S UTC"


def type_for_name(name):
    match = Patterns.namePrefix.match(name or '')
    if match:
        prefix = match.group(0)
        return PREFIX2TYPE.get(prefix) or prefix
    else:
        return None

def stripped_name(name):
    return re.sub('_header$', '',
                  re.sub(r'[_.?]nc$', '', os.path.basename(name)))

def parse_time(stime, sdate):
    return time.strptime(stime + '_' + sdate, Patterns.timeForID)

def time_for_id(t):
    return time.strftime(Patterns.timeForID, t) 

def format_time(t):
    return time.strftime(Patterns.timeFormat, t) 


class OrderedDict(dict):
    def __init__(self, source = None):
        self._keys = []
        if source: dict.__init__(self, source)

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        self._keys.remove(key)

    def __setitem__(self, key, item):
        dict.__setitem__(self, key, item)
        if key not in self._keys: self._keys.append(key)

    def clear(self):
        dict.clear(self)
        self._keys = []

    def copy(self):
        return OrderedDict(self)

    def items(self):
        return list((key, self[key]) for key in self._keys)

    def keys(self):
        return self._keys[:]

    def popitem(self):
        try:
            key = self._keys[-1]
        except IndexError:
            raise KeyError('dictionary is empty')

        val = self[key]
        del self[key]

        return (key, val)

    def setdefault(self, key, defval = None):
        if key in self:
            return self[key]
        else:
            self[key] = defval
            return defval

    def update(self, source):
        for (key, val) in source.items():
            self.__setitem__(key, val)

    def values(self):
        return list(self[key] for key in self._keys)


class Parser:
    def __init__(self, text):
        self._is_mango = False
        self._is_acquisition = False
        self.name = None
        self.time = None
        self.user = None
        self._errors = []
        self.parse(iter(text.splitlines()))
    
        # -- find the block that describes the relevant process
        if self.format == "mango":
            self.analyse_mango()
        else:
            self.analyse_other()
        self.post_process()

    def analyse_mango(self):
        data = self.raw_data
        candidates = list((key, val) for (key, val) in data.items()
                          if re.match(r'Run(_\d+)?$', key)
                          and val.get('enabled', '').lower() != 'false'
                          and val.get('Grid_Of_Images') is None)
        if not candidates:
            pattern = re.compile(r'^(MPI|Input_Data_File|Output_Data_File)$')
            candidates = list((key, val) for (key, val) in data.items()
                              if isinstance(val, dict)
                              and not pattern.match(key))
        
        if candidates:
            run_section_name, run_section = candidates[-1]
        else:
            run_section = None

        if not run_section:
            self.log_error("No Mango run section found.")
            self.inputs = []
            self.data = {}
            self.process = None
            return

        data_type = run_section.get('input_data_type')
        if data_type:
            prefix = TYPE2PREFIX.get(data_type)
            if not prefix:
                self.log_error("Unknown Mango type %s." % data_type)
        else:
            prefix = (list(val for (_, mid) in data.items()
                           if isinstance(mid, dict)
                           for (name, inner) in mid.items()
                           if isinstance(inner, dict)
                           and name == 'Input_Data_File'
                           for (key, val) in inner.items()
                           if key == 'file_name_base') or [None])[0]
            if not prefix:
                if re.match(r'Run(_\d+)?$', run_section_name):
                    self.log_error("No input prefix found in Mango section.")
                else:
                    prefix = TYPE2PREFIX.get(run_section_name)
        inputs = []
        if prefix:
            inputs.append(prefix + (run_section.get('suffix')
                                    or data.get('file_name_suffix') or ''))

        pattern = re.compile(r'^(Input_Data_File|Output_Data_File)$')
        processes = list(key for key, val in run_section.items()
                         if isinstance(val, dict) and not pattern.match(key))
        if len(processes) < 1:
            self.log_error("No Mango module name.")
        elif len(processes) > 1:
            self.log_error("Multiple Mango module names.")
        self.process = (processes or [None])[0]
        
        result = {}
        for key, val in flatten(run_section.get(self.process, {})).items():
            if key.endswith('_file_name'):
                inputs.append(os.path.basename(val))
            elif key not in [ 'Input_Data_File.format',
                              'Output_Data_File.format' ]:
                result[key] = val
        self.inputs = list(re.sub(r'[_.?]nc$', '', name) for name in inputs)
        self.data = result

    def analyse_other(self):
        self.inputs = []
        result = {}

        args = (self.raw_data.get('COMMAND') or '').split()
        self.process = None
        if args: self.process = os.path.basename(args[0])
        files = list(os.path.basename(re.sub('[._]nc/*$', '', name))
                     for name in args[1:] if re.search('[._]nc/*$', name))
        self.inputs = files[:-1]
        if files: self.name = files[-1]

        try:
            date = self.raw_data.get('DATE')
            if isinstance(date, list):
                date = date[0]
            self.time = time.strptime(date)
        except (ValueError, TypeError):
            pass
        
        self.user = self.raw_data.get('USER')

        progdot = None
        if self.process: progdot = self.process + '.'
        for k, v in self.raw_data.items():
            if re.match('(DATE|TIME|COMMAND|VERSION|USER|FUNCTION|RELEASE)$', k):
                continue
            elif re.search('\.(DATE|TIME|VERSION|FAST_LOOPS)$', k):
                continue
            if isinstance(v, list):
                v = list(re.sub('<([^<>]+)>', "'\\1'", x) for x in v)
            elif isinstance(v, dict):
                result.update(flatten({k: v}))
                continue
            else:
                v = re.sub('<([^<>]+)>', "'\\1'", v)
            if progdot and k.startswith(progdot):
                result[k[len(progdot):]] = v
            else:
                result[k] = v

        self.data = result

    def parse(self, lines):
        attr = {}
        current = None
        in_messages = False
        
        while True:
            try:
                line = lines.next()
            except StopIteration:
                break
            fields = line.split()
            key = val = None
            
            if not fields:
                # -- empty line
                pass
            elif fields[0].startswith('#'):
                # -- comment, possibly special module start comment
                if in_messages:
                    m = re.match(Patterns.moduleStart, line)
                    if m:
                        self._is_acquisition = True
                        current = re.sub(r'[\W_]+', "_", m.group(2))
            elif fields[0].startswith('-'):
                # -- alternate comment form
                self._is_acquisition = True
            elif line[0] in ' \t':
                # -- indented lines pertain to current section, if any
                val = line.strip()
                if current:
                    if in_messages:
                        k, v = self.parse_assignment(line)
                        if k:
                            key = current + '.' + k
                            val = v
                        else:
                            key = current + '.COMMENTS'
                    else:
                        key = current
                else:
                    key = '__COMMENTS__'
            elif Patterns.sectionStart.match(line):
                # -- start of a Mango section
                key = fields[1]
                val = self.parse_section(lines)
            else:
                key, val = self.parse_assignment(line)
                if key:
                  key = re.sub(r'\s+', '_', key)
                  current = key
                  in_messages = (key == "MESSAGES")
            
            if key:
                attr.setdefault(key, [])
                if val:
                    attr[key].append(val)

        self.raw_data = self.cleanup(attr)

    def parse_section(self, lines):
        self._is_mango = True
        attr = OrderedDict()
        while True:
            try:
                line = lines.next()
            except StopIteration:
                self.log_error("Matching 'EndSection' is missing.")
                break
            fields = line.split()
            if not fields:
                continue
            if Patterns.sectionStart.match(line):
                key = fields[1]
                val = self.parse_section(lines)
            elif Patterns.sectionEnd.match(line):
                break
            else:
                key, val = self.parse_mango_assignment(line)
            attr.setdefault(key, []).append(val)
        return self.cleanup(attr)

    def parse_assignment(self, line):
        m = re.match(r'\s*(\w+)\s*[:=](.*)', line)
        if m:
            self._is_acquisition = True
            return m.group(1).upper(), m.group(2).strip()
        m = re.match(r'([-_.A-Z\s]*)([\(\[].+[\)\]])?\s*[:=](.+)', line)
        if m:
            self._is_acquisition = True
            return (re.sub(r'[\W_]+', '_', m.group(1).strip()),
                    ' '.join([m.group(3), (m.group(2) or '').strip()]).strip())
        m = re.match(r'\s*exprem\[\d+\]\s*:(.*)', line)
        if m:
            self._is_acquisition = True
            return 'COMMENTS', m.group(1).strip()
        
        if not self._is_acquisition:
            return self.parse_mango_assignment(line)
        
        return None, None

    def parse_mango_assignment(self, line):
        fields = line.split()
        return (fields[0], " ".join(fields[1:]) or "")

    def cleanup(self, attr):
        result = OrderedDict()
        for key, val in attr.items():
            if len(val) == 1:
                result[key] = val[0] or ""
            else:
                result[key] = val or ""
        return result

    def post_process(self):
        def int8(s):
            if not s.startswith('0'):
                raise ValueError('not octal')
            return int(s, 8)
        
        def int16(s):
            if not s.startswith('0x'):
                raise ValueError('not sedecimal')
            return int(s, 16)
        
        def unquote(s):
            if not re.match("'[^']*'$", s):
                raise ValueError('not a single-quoted string')
            return s[1:-1].strip()
        
        result = {}
        for key, val in self.data.items():
            if isinstance(val, list): val = "\n".join(val)
            tmp = re.sub(r'\.$', '', val)
            for f in (int16, int8, int, float, unquote):
                try:
                    val = f(tmp)
                    break
                except ValueError:
                    pass
            if not val in ['', None]:
                result[key] = val
        self.data = result

    def attribute(self, key):
        return self.raw_data.get(key)

    @property
    def format(self):
        if self._is_mango:
            if self._is_acquisition:
                return "mixed"
            else:
                return "mango"
        else:
            return "acquisition"

    def log_error(self, text):
        self._errors.append(text)
        
    @property
    def errors(self):
        return self._errors


class Process:
    def __init__(self, timestamp, name, identifier, text, output):
        self._time = timestamp
        self._name = name
        self.identifier = identifier
        self.text = text
        self.output = output
        self._parser = Parser(text)
        self._errors = self._parser.errors[:]
        self.inputs = self.collect_inputs()
        self.domain = None
        self.data_file = None

    def __cmp__(self, other):
        for key in [ 'time', 'identifier', 'name', 'result_type', 'text',
                     'output' ]:
            d = cmp(getattr(self, key), getattr(other, key))
            if d: return d
        return 0

    def __hash__(self):
        return hash(self.identifier)

    def collect_inputs(self):
        res = []
        for line in (self.output or "").splitlines():
            if line.strip().startswith("input dataset ID: "):
                identifier = line.split()[3]
                if not identifier.startswith("UTC_"):
                    res.append({ 'identifier': identifier })
        for name in self._parser.inputs:
            res.append({ 'name': name })
        return list(res)

    @property
    def process(self):
        return self._parser.process
    
    @property
    def data(self):
        return self._parser.data
    
    @property
    def format(self):
        return self._parser.format
    
    @property
    def name(self):
        return self._name or self._parser.name
    
    @property
    def time(self):
        t = self._time or self._parser.time
        if t: return format_time(t)

    @property
    def user(self):
        return self._parser.user

    @property
    def result_type(self):
        return type_for_name(self.name)

    def log_error(self, text):
        self._errors.append(text)
        
    @property
    def errors(self):
        return self._errors

    @property
    def record(self):
        return {
            'process'     : self.process,
            'data_type'   : self.result_type,
            'name'        : self.name,
            'date'        : self.time,
            'identifier'  : self.identifier,
            'run_by'      : self.user,
            'parameters'  : self.data,
            'predecessors': self.inputs,
            'source_text' : self.text,
            'output_log'  : self.output,
            'parse_errors': self.errors,
            'domain'      : self.domain,
            'data_file'   : self.data_file
            }


class History:
    def __init__(self, source, name = None, creation_time = None):
        attributes = self.extract_attributes(source)
        fingerprint = source.fingerprint

        self.logger = Logger()
        self.name = name
        self.creation_time = creation_time
        self.processes = self.extract_processes(attributes)
        self.resolve_inputs()
        main = self.main_process()
        if main:
            main.domain = self.extract_domain(attributes)
            main.data_file = {
                'name': stripped_name(self.name),
                'date': format_time(self.creation_time),
                'fingerprint': fingerprint
                }
            if main.name is None:
                main.name = main.data_file['name']
    
    def extract_attributes(self, source):
        result = {}
        for attr in source.attributes:
            result.setdefault(attr.name, attr.value)
        for var in source.variables:
            if len(var.dimensions) == 3 and var.dimensions[0].value > 1:
                for attr in var.attributes:
                    result.setdefault(attr.name, attr.value)
        return result
        
    def extract_processes(self, attributes):
        result = []

        for key in attributes.keys():
            if not re.match('history_', key): continue
            
            fields = re.split('_+', key.strip())
            if fields[-1] == "output": continue
        
            identifier = re.sub('history_+(UTC_+)?', '', key)

            if fields[1] == "UTC":
              fields = fields[2:]
            else:
              fields = fields[1:]

            if re.match('\d+$', fields[0]):
                timestamp = parse_time(fields[0], fields[1])
                fields = fields[2:]
            else:
                timestamp = None
            
            if len(fields) > 0:
                name = re.sub('[_.?]nc$', "", "_".join(fields))
            else:
                name = None
        
            text = attributes[key]
            output = attributes.get(key + "_output")
        
            result.append(Process(timestamp, name, identifier, text, output))
      
        result.sort()
        return result

    def process_by_name(self, name):
        if not hasattr(self, '_name2process'):
            name2process = {}
            for p in self.processes:
                if name2process.get(p.name):
                    p.log_error("Duplicate name within history.")
                else:
                    name2process[p.name] = p
            self._name2process = name2process
        return self._name2process.get(name)

    def process_by_id(self, identifier):
        if not hasattr(self, '_id2process'):
            id2process = {}
            for p in self.processes:
                if id2process.get(p.identifier):
                    p.log_error("Duplicate identifier within history.")
                else:
                    id2process[p.identifier] = p
            self._id2process =id2process
        return self._id2process.get(identifier)

    def find_process(self, entry):
        if entry.get('identifier'):
            return self.process_by_id(entry['identifier'])
        elif entry.get('name'):
            return self.process_by_name(entry['name'])
        else:
            return None

    def resolve_inputs(self):
        for p in self.processes:
            names = {}
            idents = {}
            for entry in p.inputs:
                if entry.get('identifier'):
                    idents[entry['identifier']] = True
                elif entry.get('name'):
                    pred = self.find_process(entry)
                    if pred is None:
                        names[entry['name']] = True
                    elif p.identifier != pred.identifier:
                        idents[pred.identifier] = True
            names = list({'name': name, 'message': "History entry missing" }
                         for name in names.keys())
            idents = list({'identifier': ident} for ident in idents.keys())
            p.inputs = names + idents

    def main_process(self):
        # -- try to find a process with the same name as the source file
        if self.name:
            name = stripped_name(self.name)
            self.logger.trace("Stripped name: " + name)
            main = self.process_by_name(name)
            if main: return main
        else:
            name = None

        # -- collect all entries that appear as inputs
        used = {}
        for p in self.processes:
            for q in p.inputs:
                r = self.find_process(q)
                if r and r != p:
                    used[r] = True

        # -- find potential main processes
        eligible = list(p for p in self.processes if p and not used.get(p))
        eligible.sort()
        eligible.reverse()
        
        # -- get the most recent one of the correct type
        target_type = type_for_name(name)
        candidates = list(p for p in eligible if p.result_type == target_type)
        if candidates:
            return candidates[0]
        elif eligible:
            return eligible[0]

    def set_xyz(self, target, name, vec):
        if len(vec) >= 3:
            target.update(dict((name + '_' + 'xyz'[i], vec[i]) for i in (0,1,2)))

    def extract_domain(self, attr):
        result = {}
        t = attr.get("total_grid_size") or attr.get("total_grid_size_xyz")
        if t: self.set_xyz(result, "domain_size", t)
        t = attr.get("coordinate_origin") or attr.get("coordinate_origin_xyz")
        if t: self.set_xyz(result, "domain_origin", t)
        
        voxel_size = attr.get("voxel_size") or attr.get("voxel_size_xyz")
        voxel_unit = attr.get("voxel_unit") or ''

        if re.match('mm$|millimet(re|er)', voxel_unit):
            voxel_unit = "micron"
            voxel_size = list(x * 1000.0 for x in voxel_size or ())
        elif re.match('micro(metre|meter|n)', voxel_unit):
            voxel_unit = "micron"
        
        if voxel_size: self.set_xyz(result, "voxel_size", voxel_size)
        if voxel_unit: result["voxel_unit"] = voxel_unit

        return result

    @property
    def as_json(self):
        return json.dumps(list(p.record for p in self.processes),
                          sort_keys = True, indent = 4)


if __name__ == "__main__":
    import sys
    from nc3files import nc3info
    
    Logger().priority = LOGGER_INFO

    i = 1
    if sys.argv[i] == '-c':
        from file_cache import FileCache
        FileCache.cache_location = "/local/projects/d59/assets/nc3cache.db"
        i += 1
    fname = sys.argv[i]
    info = nc3info(fname)
    print History(info, fname, time.gmtime(os.stat(fname).st_mtime)).as_json
