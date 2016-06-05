"""
Null datasource. Also the _interface specification_ provided to all
dataoutputs. The null DataSource class should be used as a base class for all
other DataSources.
"""

from logan.datasource import DataPoint

def register_arguments(logan_config):
    """
    Interface function.
    When module is loaded, this function is called by the main module,
    allowing this module to register its own command-line arguments.

    @type logan_config: LoganConfig
    """
    logan_config.add_argument("-S", "--dsrc-path", metavar="SRCPATH", type=str,
            dest="dsrc_paths", default=["rawdata"], nargs="+",
            help="Paths to source data, passed to datasource; multiple paths will only be used if supported by datasource used. [Default:rawdata]")
    logan_config.add_argument("-g", "--dsrc-gen-data",
            action="count", dest="dsrc_gen_data", default=0,
            help="Generate the source data first; specify multiple times to generate multiple times.")
    logan_config.add_argument("-G", "--dsrc-gen-data-only",
            action="count", dest="dsrc_gen_data_only", default=0,
            help="Exit after generation of source data (implies --dsrc-gen-data).")
    logan_config.add_argument("-c", "--dsrc-cache-load",
            action="store_true", dest="dsrc_cache_load", default=False,
            help="Load processed data from cache, if available and supported by datasource.")
    logan_config.add_argument("-C", "--dsrc-cache-save",
            action="store_true", dest="dsrc_cache_save", default=False,
            help="Save processed data to cache, if supported by datasource.")
    logan_config.add_argument("-R", "--dsrc-raw-save",
            action="store_true", dest="dsrc_raw_save", default=False,
            help="Save intermediate raw data, if supported by datasource.")
    logan_config.add_argument("--dsrc-compress", metavar="COMPRESS", type=str,
            dest="dsrc_compress", default=None,
            help="Specify compression scheme to compress uncompressed source data files, if supported by datasource.")
    logan_config.add_argument("--dsrc-min-results", metavar="COUNT", type=int,
            dest="dsrc_min_results", default=3,
            help="Require a minimum of COUNT results, if supported by datasource. [Default: 3]")
    logan_config.add_argument("--dsrc-max-results", metavar="COUNT", type=int,
            dest="dsrc_max_results", default=0,
            help="Require a maximum of COUNT results, if supported by datasource.")
    logan_config.add_argument("--dsrc-ranges",
            action="store_true", dest="dsrc_ranges", default=False,
            help="Use data error/ranges, and pass to dataoutput.")

def get_description():
    """
    Interface function. Used to query description.
    """
    return ("Base/null datasource (does nothing).", None)

class DataSourceGenerator(object):
    """
    Called to generate the data, usually by an external program, before
    processing.
    """

    def __init__(self, logan_config):
        """
        @type logan_config: LoganConfig
        """
        self.logan_config = logan_config

    def __call__(self):
        raise NotImplementedError("Base module does not support source data generation!")

class DataSource(object):
    def __init__(self, logan_config):
        """
        @type logan_config: LoganConfig
        """
        self.logan_config = logan_config

    def process(self):
        """
        Interface function called to allow datasource to prepare data, before
        being accessed by a dataoutput.
        @rtype: boolean
        @return: Success or not.
        """
        return True

    def map_to_name(self, key):
        """
        Interface function. Map any key to a given name.

        @type key: str
        @key: Key which should be used to look up a mapping to a full name.
        @rtype: str
        @return: Mapped to string.
        """
        return "null"

    def get_dataset_keys(self):
        """
        Interface function.

        @return: List of keys to provide more than 1 dataset. None to ignore.
        """
        return None

    def get_presentation_hints(self, dataset):
        """
        Interface function.

        @return: dict containing hints to dataoutput how to interpret the data.
                 The understood strings by the dataoutput may vary.
        """
        return {}

    def get_description(self, dataset=None):
        """
        Interface function.

        @return: Short description of data as string. None to ignore.
        """
        return None

    def get_ylabel(self, dataset=None):
        """
        Interface function.

        @return: y-Axis label as string.
        """
        return "null"

    def get_xlabel(self, dataset=None):
        """
        Interface function.

        @return: x-Axis label as string.
        """
        return "null"

    def get_zlabel(self, dataset=None):
        """
        Interface function.

        @return: z-Axis label as string.
        """
        return "null"

    def get_yrange(self, dataset=None):
        """
        Interface function.
        Return displayed range on y-Axis. Return None to ignore.

        @return: Range on the y-Axis, as (Min,Max) tuple.
        """
        return None

    def get_xtick_keys(self, dataset=None):
        """
        Interface function.

        @return: List of keys making up the x-Axis.
        """
        return None

    def get_ztick_keys(self, dataset=None):
        """
        Interface function.
        Return the keys for the z-Axis ticks. Return None if the data
        source does not produce 3D data.

        @return: List of keys making up the z-Axis (for 3D graphs).
        """
        return None

    def get_stack_keys(self, dataset=None):
        """
        Interface function.
        Returns the keys for stacking data. Return None if the data source
        does not provide stacked data.

        @return: List of keys used for data stacking
        """
        return None

    def get_cluster_keys(self, dataset=None):
        """
        Interface function.
        Returns the keys for displaying clustered data. Return None if the
        data source does not provide clustered data.

        @return: List of keys for clustered data.
        """
        return None

    def query_data(self, x, z=0, stack=None, cluster=None, dataset=None):
        """
        Interface function.
        @x: The x value/key used for the lookup.
        @z: The z value/key used for the lookup.
        @stack: The stack key used for the lookup.
        @cluster: The cluster key used for the lookup.
        @return: DataPoint
        """
        return DataPoint(y=0)

    def data(self):
        """
        Interface function. Acts as a python-generator for all available data.
        Yields a DataPoint until no more data is available. Contrary to
        the query_data function, this can be used to generate smooth
        curves. As the dataoutput would only know about the {x,z}-ticks from
        get_{x,z}tick_keys, those would be the points queried by the
        dataoutput. Using this generator, it is possible to define the ticks, but
        generate data inbetween.
        """
        yield DataPoint()

