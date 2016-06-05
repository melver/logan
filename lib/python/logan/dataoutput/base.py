"""
Base/null dataoutput (does nothing).
"""

def register_arguments(logan_config):
    """
    Interface function.
    When module is loaded, this function is called by the main module,
    allowing this module to register its own command-line arguments.

    @type logan_config: LoganConfig
    """
    logan_config.add_argument("-O", "--dout-path", metavar="OUTPATH", type=str,
            dest="dout_path", default="output",
            help="Path to store digested data; passed to dataoutputs. [Default:output]")
    logan_config.add_argument("-f", "--dout-format", metavar="FMT", type=str,
            dest="dout_formats", default=[None], nargs="+",
            help="Output formats (see --list); passed to dataoutputs.")
    logan_config.add_argument("--dout-theme", metavar="THEME", type=str,
            dest="dout_theme", default="color",
            help="Set output theme (if available). [Default:color]")
    logan_config.add_argument("--dout-size", metavar="SIZE", type=str,
            dest="dout_size", default=None,
            help="Output size (format depends on dataoutput); passed to dataoutputs.")

def get_description():
    """
    Interface function. Used to query description.
    """
    return ("Base/null dataoutput (does nothing). [Formats:]", None)

class DataOutput(object):
    def __init__(self, logan_config):
        """
        @type logan_config: LoganConfig
        """
        self.logan_config = logan_config

    def generate(self, data_source):
        """
        Interface function to be called by the main program. This should
        trigger the generation of the output.

        @type data_source: DataSource
        @data_source: Any datasource DataSource which defines the basic interface.
        @rtype: boolean
        @return: Success or not.
        """
        return True

