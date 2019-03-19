import json
import os
import sys
import types
import importlib
import uuid
import traceback
import inspect
import lighthouse

from datetime import datetime
from time import sleep
from collections import namedtuple
from multiprocessing import Process
from importlib.machinery import SourceFileLoader
from lighthouse import *


class _OntologyConfiguration:
    def __init__(self):
        self.schemas = []
        self.localization = None
        self.name = None
        self.id = None


def load_module(python_file_path):
    module_name = os.path.basename(python_file_path).split('.')[0]
    loader = importlib.machinery.SourceFileLoader(module_name, python_file_path)
    mdl = types.ModuleType(loader.name)
    loader.exec_module(mdl)
    return module_name, mdl


def try_get_user_task_class(python_file_path):
    _, mdl = load_module(python_file_path)
    for element_name in dir(mdl):
        cls = getattr(mdl, element_name)
        if inspect.isclass(cls):
            try:
                if issubclass(cls, Task) and cls is not Task:
                    return cls
            except TypeError as e:
                print(e)
    return None


def get_ontology_configuration(python_file_path) -> _OntologyConfiguration:
    configuration = _OntologyConfiguration()

    module_name, mdl = load_module(python_file_path)
    configuration.name = module_name
    for element_name in dir(mdl):
        element = getattr(mdl, element_name)

        if element_name == 'NAME':
            configuration.name = element
            continue

        if element_name == 'ONTOLOGY_ID':
            configuration.id = str(element) if isinstance(element, uuid.UUID) else element
            continue

        if type(element) == Object:
            object = SchemaObject(element, mapping={})
            objects, links = Schema.process_graph([object], [])
        elif type(element) == Link:
            begin = SchemaObject(element.Begin, mapping={})
            end = SchemaObject(element.End, mapping={})
            link = SchemaLink(element, mapping={}, begin=begin, end=end)
            objects, links = Schema.process_graph([begin, end], [link])
        elif type(element) == Schema:
            configuration.schemas.append(element.to_json())
            continue
        elif type(element) == Localization:
            configuration.localization = element
            continue
        else:
            continue

        name = str(uuid.uuid4())
        schema = Schema.get_json(name, objects, links)
        configuration.schemas.append(schema)
    # return name_of_ontology, schemas, localization
    return configuration


def json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, RelativeDate):
        return obj.to_json()
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj))


def get_description(directories_file, file_with_paths_to_py, file_with_paths_to_descr):
    def set_ambiguous_key(key, val, necessary=False):
        default_value = []
        if val is None:
            if not necessary:
                task_json[key] = default_value
            else:
                raise Exception("%s can't be None" % key)
        elif type(val) in [SchemaCollection, MacroCollection, HeaderCollection, EnterParamCollection]:
            task_json[key] = val.to_json()
        elif type(val) in [Schema, Macro, Header, EnterParamField]:
            task_json[key] = [val.to_json()]
        else:
            task_json[key] = default_value

    with open(directories_file, encoding='utf-8') as dirs_reader, \
        open(file_with_paths_to_py, encoding='utf-8') as scripts_reader, \
        open(file_with_paths_to_descr, encoding='utf-8') as descriptions_reader:

        root_directories = dirs_reader.read().splitlines()
        files_with_py = scripts_reader.read().splitlines()
        files_with_descr = descriptions_reader.read().splitlines()

    if len(files_with_py) != len(files_with_descr):
        raise Exception('The number of lines in the files is different')

    for index, description_pair in enumerate(zip(files_with_py, files_with_descr)):
        lighthouse.PATHS_ROOT = root_directories[index]
        python_file_path, output_file_path = description_pair
        try:
            cls = try_get_user_task_class(python_file_path)
            task_json = {}
            if not cls:
                onto_config = get_ontology_configuration(python_file_path)
                if not onto_config.id:
                    raise Exception('Ontology must have unique id string. Please put it in "ONTOLOGY_ID" variable in script')

                name = onto_config.name

                task_json['can_execute'] = False
                task_json['enter_params'] = []
                task_json['headers'] = []
                task_json['category'] = ''
                task_json['description'] = ''
                task_json['graph_macros'] = []
                task_json['gis_macros'] = []
                task_json['schemas'] = onto_config.schemas
                task_json['id'] = onto_config.id
                task_json['calculate_weight_function'] = '0'
                task_json['localization'] = onto_config.localization.to_json() if onto_config.localization else {}
            else:
                user_task = cls()
                task_id = user_task.get_id()
                if not task_id:
                    raise Exception('Task must have unique id string. Please override get_id method')

                name = user_task.get_display_name()
                category = user_task.get_category()
                description = user_task.get_description()
                headers = user_task.get_headers()
                enter_params = user_task.get_enter_params()
                schemas = user_task.get_schemas()
                graph_macros = user_task.get_graph_macros()
                gis_macros = user_task.get_gis_macros()
                weight_function = user_task.get_weight_function()
                localization = user_task.get_localization()

                task_json['id'] = task_id
                task_json['can_execute'] = True
                task_json['category'] = category or ''
                task_json['description'] = description or ''
                task_json['calculate_weight_function'] = weight_function or ''
                set_ambiguous_key('enter_params', enter_params, necessary=True)
                set_ambiguous_key('headers', headers, necessary=True)
                set_ambiguous_key('graph_macros', graph_macros)
                set_ambiguous_key('gis_macros', gis_macros)
                set_ambiguous_key('schemas', schemas)
                task_json['localization'] = localization.to_json() if localization else {}

            task_json['name'] = name

            with open(output_file_path, 'w', encoding='utf8') as file:
                json_str = json.dumps(task_json, indent=4, separators=(',', ': '), ensure_ascii=False, sort_keys=True,
                                      default=json_default)
                file.write(json_str)
        except:
            with open(output_file_path + '.errors', 'w', encoding='utf8') as errors_file:
                errors_file.write(traceback.format_exc())
            continue


def parse_datetime(datetimestring):
    from re import compile
    from datetime import timezone, timedelta, datetime

    iso8601 = compile(r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T' \
                      r'(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(?:\.(?P<microsecond>\d+))?' \
                      r'(?P<offset>Z|(?:(?P<offsetsign>[+\-])(?P<offsethour>\d{2}):(?P<offsetminute>\d{2})))?$')
    match = iso8601.match(datetimestring.upper()).groupdict()

    year = int(match['year'])
    month = int(match['month'])
    day = int(match['day'])
    hour = int(match['hour'])
    minute = int(match['minute'])
    second = int(match['second'])
    microsecond = 0
    if match['microsecond'] is not None:
        microsecond = int(float('0.' + match['microsecond']) * 1000000)
    offset = None
    if match['offset'] is not None:
        if match['offset'] is 'Z':
            offset = timezone.utc
        else:
            offset = timezone(timedelta(minutes=(1 if match['offsetsign'] is '+' else -1) *
                                        ((int(match['offsethour']) * 60) + int(match['offsetminute']))))
    return datetime(year, month, day, hour, minute, second, microsecond, offset)


def execute(python_file_path, task_json_file_path, headers_file_path, temp_dir):
    def ensure_datetime(value, value_type):
        if value_type == ValueType.Datetime:
            return parse_datetime(value)
        return value
    instance = try_get_user_task_class(python_file_path)()
    if not instance:
        raise Exception('Subclass of UserTaskBase was not found')

    enter_param_descriptions = instance.get_enter_params()
    if enter_param_descriptions is None:
        raise Exception("Enter parameters can't be None")
    elif isinstance(enter_param_descriptions, EnterParamField):
        enter_param_descriptions = [enter_param_descriptions]
    elif isinstance(enter_param_descriptions, EnterParamCollection):
        pass
    else:
        raise TypeError("Enter parameters must be EnterParamValue or EnterParamCollection")

    with open(task_json_file_path, 'r', encoding='utf-8') as task_json:
        enter_params_json = json.load(task_json)
    names = []
    values = []
    for enter_param in enter_param_descriptions:
        names.append(enter_param.system_name)
        value = None
        if enter_param.system_name in enter_params_json:
            enter_param_value = enter_params_json[enter_param.system_name]
            if enter_param.is_array:
                value = [ensure_datetime(item, enter_param.type) for item in enter_param_value]
            else:
                value = ensure_datetime(enter_param_value, enter_param.type)
        values.append(value)
    enter_param_values = namedtuple('EnterParams', names)(*values)
    result_writer = None
    try:
        args = [enter_param_values, ResultWriter(headers_file_path, instance.get_headers()), LogWriter()]

        if temp_dir:
            args.append(temp_dir)
        instance.execute(*args)
    finally:
        if result_writer:
            result_writer.close()


def watch_dir(path):
    if not os.path.exists(path):
        raise Exception('Directory not exists: %s' % path)
    known_dirs = {}
    timeout_sec = 1
    dir_entry_ttl = 60 * 5  # iterations

    while True:
        try:
            current_names = []
            dir_entries = os.scandir(path)  # subdirectories in pool
            for entry in dir_entries:
                current_names.append(entry.name)
                if entry.name in known_dirs:
                    continue
                if entry.is_dir():
                    subentries = os.scandir(entry.path)  # files in subdirectory
                    args = {'dirs.txt': None, 'input.txt': None, 'output.txt': None}  # arguments for get_description
                    for subentry in subentries:
                        if subentry.is_file() and subentry.name in args:
                            args[subentry.name] = subentry.path
                    if all(args.values()):  # all 3 files presented
                        known_dirs[entry.name] = 0
                        describing = Process(target=get_description,
                                             args=[args['dirs.txt'], args['input.txt'], args['output.txt']])
                        describing.start()
                    else:
                        print("Insufficient enter parameters for %s" % entry.name, file=sys.stderr)
                        known_dirs[entry.name] = 0
                    subentries.close()
            dir_entries.close()

            for key in list(known_dirs.keys()):  # if ttl is over or known directory no longer exists in pool
                if known_dirs[key] >= dir_entry_ttl or key not in current_names:
                    del known_dirs[key]
                    continue
                known_dirs[key] += 1

        except Exception as e:
            print(e, file=sys.stderr)
        sleep(timeout_sec)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='mode help', dest='mode')

    description_parser = subparsers.add_parser('description', help='description mode help')
    description_parser.add_argument('directories_file', metavar='directories_file', type=str,
                                    help='path to file with root directories definitions', nargs='?')
    description_parser.add_argument('file_with_paths_to_py', metavar='file_with_paths_to_py', type=str,
                                    help='file with paths to python files')
    description_parser.add_argument('file_with_paths_to_descr', metavar='file_with_paths_to_descr', type=str,
                                    help='file with paths to description files')

    execute_parser = subparsers.add_parser('execute', help='execute mode help')
    execute_parser.add_argument('python_file_path', metavar='python_file_path', type=str, help='file to execute')
    execute_parser.add_argument('enter_params_file_path', metavar='enter_params_file_path', type=str,
                                help='enter params file path')
    execute_parser.add_argument('file_with_paths_to_results', metavar='file_with_paths_to_results', type=str,
                                help='file with paths to result files')
    execute_parser.add_argument('temp_directory_path', metavar='temp_directory_path', type=str,
                                help='path to temporary directory', nargs='?')

    monitoring_parser = subparsers.add_parser('monitoring', help='monitoring mode help')
    monitoring_parser.add_argument('directory', metavar='directory', type=str, help='directory to watch for')

    args = parser.parse_args()
    if args.mode == 'description':
        get_description(args.directories_file, args.file_with_paths_to_py, args.file_with_paths_to_descr)
    elif args.mode == 'execute':
        execute(args.python_file_path, args.enter_params_file_path, args.file_with_paths_to_results,
                args.temp_directory_path)
    elif args.mode == 'monitoring':
        watch_dir(args.directory)
    else:
        pass


if __name__ == '__main__':
    main()
