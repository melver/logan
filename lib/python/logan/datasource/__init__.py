"""
Common datasource functions.
"""

import os
import types
import re
import collections
import logging
import gzip
import pickle
import glob
import functools

import logan.compat
from logan.datasource import analysis

COMPRESS_MODULES = { "gz" : gzip }

try:
    import bz2
    import lzma
    COMPRESS_MODULES.update({
            "bz2" : bz2,
            "xz" : lzma
        })

    COMPRESS_HIGHEST = "xz"
except:
    COMPRESS_HIGHEST = "gz"

CACHE_PICKLE_PREFIX = "logan-cache.pickle"
PICKLE_VERSION = 2

class DataPoint(object):
    """
    Can be used to encapsulate data.
    """
    def __init__(self, x=0, y=0, z=0,
                 x_err=(0,0), y_err=(0,0), z_err=(0,0),
                 stack=None, cluster=None, dataset=None):
        self.x = x
        self.y = y
        self.z = z
        self.x_err = x_err
        self.y_err = y_err
        self.z_err = z_err
        self.stack = stack
        self.cluster = cluster
        self.dataset = dataset

def simpletreedict():
    return collections.defaultdict(simpletreedict)

def cfopen(fileprefix, compress=None):
    """
    Tries to open a file with prefix fileprefix. First, we check if a
    compressed file exists, and if not, open an uncompressed file. If the
    compress argument is any of the supported compression schemes, an
    uncompressed file is compressed to the selected scheme and then deleted.
    """

    result = {}
    result['name'] = fileprefix

    for filesuffix in COMPRESS_MODULES:
        c_filename = "{}.{}".format(fileprefix, filesuffix)
        if os.path.exists(c_filename):
            result['f'] = COMPRESS_MODULES[filesuffix].open(c_filename, 'rb')
            return result

    # Assume non-compressed file exists and let user handle exception in case
    # it does not
    result['f'] = open(fileprefix, 'rb')

    if compress is not None:
        if compress not in COMPRESS_MODULES:
            raise Exception("Invalid compression scheme: {} [Valid options: {}]".format(
                compress, ", ".join(COMPRESS_MODULES)))

        # Compress fileprefix
        logging.debug("Compressing {} -> .{}".format(fileprefix, compress))
        c_module = COMPRESS_MODULES[compress]
        c_filename = "{}.{}".format(fileprefix, compress)
        c_out = c_module.open(c_filename, 'wb')
        c_out.writelines(result['f'])
        c_out.close()
        result['f'].close()

        # Delete uncompressed file
        os.remove(fileprefix)

        # Open compressed file
        result['f'] = c_module.open(c_filename, 'rb')

    return result

RegexInfo = collections.namedtuple("RegexInfo", ["src", "re", "gid", "empty_def"])

def regexinfo(src, re, gid=1, empty_def=None):
    return RegexInfo(src, re, gid, empty_def)

def extract_data_from_files(datafiles, datasets, filter_by=None,
                            error_set=None, regex_fn_args=[]):
    """
    Extract data from a file-like object.

    @datafiles: List of dicts as returned by cfopen
    @datasets: mapping of datasets to information about the data.
               The minimum data about each dataset is as follows:
               { 'dataset_key' :
                    { 'regexes' : [ regexinfo("filesuffix", "regex1"), regexinfo(None, "regex2"), ..],
                      'compute' : compute_function } }
                Where compute_function is passed a list of lists mapping to
                'regex1', 'regex2', etc.
    @return: dict mapping dataset keys to data-values
    """
    def check_filter(dataset):
        """Return False if this dataset should not be included in results"""
        if 'filter' not in dataset:
            return True
        return (filter_by is None or dataset['filter'](filter_by))

    def regex_generator(regexes):
        """
        Regex-entries can be functions, which return the actual regex info --
        this is to dynamically switch, if not all datasets are accessed equally
        for different results.
        """
        for regex in regexes:
            if callable(regex):
                regex = regex(*regex_fn_args)

            if not isinstance(regex, RegexInfo):
                raise Exception("Regex not of type RegexInfo!")

            yield regex

    result = {}

    # Container for all data, which is then later used to compute final data
    # as defined in datasets.
    regex_data = {}
    compiled_regexes = {}
    markers = collections.defaultdict(set)
    for dataset_key in datasets:
        if 'regexes' not in datasets[dataset_key] or \
           not check_filter(datasets[dataset_key]): continue

        for regex in regex_generator(datasets[dataset_key]['regexes']):
            if regex not in regex_data:
                regex_data[regex] = []
                compiled_regexes[regex.re] = re.compile(regex.re)
                if regex.src is not None and not isinstance(regex.src, str):
                    markers[regex.src[0]].add(regex.src[1])

    # Read all requested data into memory
    for datafile in datafiles:
        # Set up markers
        marker_count = collections.defaultdict(lambda: 0)
        file_markers = None
        for file_suffix in markers:
            if datafile['name'].endswith(file_suffix):
                file_markers = markers[file_suffix]
                break

        for line in (logan.compat.decode_to_string(line) for line in datafile['f']):
            # Process markers
            if file_markers is not None:
                for marker in file_markers:
                    if line.startswith(marker):
                        marker_count[marker] += 1

            for regex in regex_data:
                # Check if we are allowed to process this regex
                if regex.src is not None:
                    if not isinstance(regex.src, str):
                        if not datafile['name'].endswith(regex.src[0]) \
                                or marker_count[regex.src[1]] != regex.src[2]:
                            continue
                    elif not datafile['name'].endswith(regex.src):
                        continue

                # process regex
                match_obj = compiled_regexes[regex.re].search(line)
                if match_obj is not None:
                    if callable(regex.gid):
                        regex_data[regex].append(regex.gid(match_obj.group))
                    else:
                        regex_data[regex].append(match_obj.group(regex.gid))

    # Compute final result
    for dataset_key in datasets:
        if 'compute' not in datasets[dataset_key] or \
           datasets[dataset_key]['compute'] is None or \
           not check_filter(datasets[dataset_key]): continue

        data = []
        if 'regexes' in datasets[dataset_key]:
            # Ensure data entries are ordered correctly!
            for regex in regex_generator(datasets[dataset_key]['regexes']):
                if regex.empty_def is not None and len(regex_data[regex]) == 0:
                    data.append(regex.empty_def)
                else:
                    data.append(regex_data[regex])

        try:
            compute = datasets[dataset_key]['compute']
            # Check if compute-function is dict, if so, result will be dict
            # with compute-keys assigning the results of each respectively
            if isinstance(compute, dict):
                for compute_key in compute:
                    if compute[compute_key] is not None:
                        if dataset_key not in result:
                            result[dataset_key] = type(compute)()
                        result[dataset_key][compute_key] = compute[compute_key](D=data, K=dataset_key, R=result)
            else:
                compute_key = None
                result[dataset_key] = compute(D=data, K=dataset_key, R=result)
        except analysis.AnalysisError as e:
            if error_set is not None:
                error_set.add(dataset_key)
            else:
                raise type(e)(str(e), ('datafiles'   , [df['name'] for df in datafiles]),
                                      ('dataset_key' , dataset_key), ('compute_key' , compute_key),
                                      ('data'        , data))

        except Exception as e:
            raise     type(e)(str(e), ('datafiles'   , [df['name'] for df in datafiles]),
                                      ('dataset_key' , dataset_key), ('compute_key' , compute_key),
                                      ('data'        , data))

    return result

class ReduceFunction(object):
    def __init__(self, f):
        self.f = f

    def __getitem__(self, index):
        return self

    def __call__(self, *args, **kwargs):
        return self.f(*args, **kwargs)

def reduce_deep(function, iterable, ignore_keys=[], first_keys=[]):
    """Reduce iterable of dictionary trees, applying the reduce-function to each leaf-node"""

    if isinstance(function, types.FunctionType):
        function = ReduceFunction(function)

    # Uses first_keys to establish some order when iterating over the hash-map.
    # Anything in first_keys is iterated over first, but after that, the order
    # is according to accum's iterator again.
    def _accum_iter(accum):
        for key in first_keys:
            if key in accum:
                yield key
        for key in accum:
            if key not in first_keys:
                yield key

    def _function(accum, x):
        if isinstance(accum, dict):
            for key in _accum_iter(accum):
                if key in ignore_keys: continue

                if isinstance(accum[key], dict):
                    accum[key] = reduce_deep(function[key], [accum[key], x[key]],
                                             ignore_keys=ignore_keys, first_keys=first_keys)
                else:
                    accum[key] = function[key](accum[key], x[key])

            return accum

        return function(accum, x)

    return functools.reduce(_function, iterable)

def encode_json_to_filename(jsonstring):
    """This only works for simple datasets encoding in json (no string support,
    keys without spaces only, dictionaries only)"""

    jsonstring = jsonstring.replace(" ", "") \
                     .replace('":', "=") \
                     .replace('"', "") \
                     .replace("{", "") \
                     .replace("}", "") \
                     .replace(":", "+", 1) # so we can prepend a key separated by ':'

    components = jsonstring.split("+")
    if len(components) != 1:
        jsonstring = components[0] + "+" + ",".join(sorted(components[1].split(",")))
    elif len(components) == 1:
        jsonstring = ",".join(sorted(components[0].split(",")))
    else:
        raise Exception("Unexpected input")

    return jsonstring

def decode_filename_to_json(filename):
    """The reverse of encode_json_to_filename"""

    if "+" not in filename: return filename

    if "=" in filename:
        # Assume that it is json
        result = filename.replace("+", ':{"', 1) \
                         .replace("=", '":') \
                         .replace(",", ',"') \
                         + "}"

        if ":{" not in result:
            # there was no prepended key
            result = "{" + result
    else:
        # Not json, just a value that was passed through encode_json_to_filename
        result = filename.replace("+", ":", 1)

    return result

def save_pickle(filename, **kwargs):
    """Pickle kwargs to file"""
    filename += "." + COMPRESS_HIGHEST
    f = COMPRESS_MODULES[COMPRESS_HIGHEST].open(filename, "wb")
    logging.info("Saving pickle to {} ...".format(filename))
    pickle.dump(kwargs, f, PICKLE_VERSION)
    f.close()

def cache_save_pickle(output_dir, **kwargs):
    """Pickle kwargs to cache file"""
    filename = os.path.join(output_dir, CACHE_PICKLE_PREFIX)
    save_pickle(filename, **kwargs)

def load_pickle(filename, container, **kwargs):
    """Loads pickled file and optionally assigns each element from the loaded
    file to the corresponding attribute in container."""

    compress = filename.split(".")[-1]
    f = COMPRESS_MODULES[compress].open(filename, "rb")
    logging.info("Loading pickle from {} ...".format(filename))
    result = pickle.load(f)
    f.close()

    if container is not None:
        for key in kwargs:
            setattr(container, kwargs[key], result[key])

    return result

def cache_load_pickle(output_dir, container, **kwargs):
    """Loads pickled file and optionally assigns each element from the loaded
    file to the corresponding attribute in container."""

    glob_files = glob.glob(os.path.join(output_dir, CACHE_PICKLE_PREFIX+".*"))

    if glob_files:
        if len(glob_files) > 1:
            logging.warning("Multiple cache files found!")
        filename = glob_files[0]
    else:
        filename = None

    if not filename or not os.path.exists(filename):
        logging.info("Cache does not exist!")
        return None

    return load_pickle(filename, container, **kwargs)

