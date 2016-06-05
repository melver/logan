#!/usr/bin/env python

"""
Log Analyser main file.
"""

import sys
import argparse
import time
import imp
import os
import logging
import pipes
import datetime

import logan
import logan.datasource.base
import logan.dataoutput.base
import logan.compat

class LoganConfig(object):
    """
    Configuration class which maintains the global configuration of the program.
    """
    def __init__(self):
        self._parser = argparse.ArgumentParser(prog="logan",
                description="Logan: The Universal LOG and Data ANalyser framework.")
        self._parser.add_argument("-s", "--datasource", metavar="DS", type=str,
                dest="datasource", default="base",
                help="Datasource module to be used. [Default:base]")
        self._parser.add_argument("-o", "--dataoutput", metavar="DO", type=str,
                dest="dataoutputs", default=["base"], nargs="+",
                help="Dataoutput modules to be used. [Default:base]")
        self._parser.add_argument("--list", action="store_true",
                dest="list_datamodules", default=False,
                help="List available datasources and dataoutputs and quit.")
        self._parser.add_argument("--show", action="store_true",
                dest="show_datamodules", default=False,
                help="Show detailed description/help of selected datamodules.")
        self._parser.add_argument("--loglevel", metavar="LEVEL", type=str,
                dest="loglevel", default="INFO",
                help="Loglevel (DEBUG, INFO, WARNING, ERROR, CRITICAL). [Default:INFO]")
        self._parser.add_argument("-j", "--jobs", metavar="JOBS", type=int,
                dest="jobs", default=1,
                help="Number of jobs to run simultaneously, if supported by datasource/dataoutput. [Default:1]")
        self._parser.add_argument("-b", "--batch", action="store_true",
                dest="batch", default=False,
                help="Batch mode: ask no questions.")
        self._parser.add_argument("-t", "--tag", metavar="TAG", type=str,
                dest="tag", default=None,
                help="Provide tag, if supported by datasource/dataoutput.")
        self._parser.add_argument("-m", "--message", metavar="MSG", type=str,
                dest="message", default=None,
                help="Provide message to describe data, if supported by datasource/dataoutput.")
        self._parser.add_argument("-M", "--gen-script", action="store_true",
                dest="gen_script", default=False,
                help="Generate script to re-run logan with the same parameters.")

        args_nohelp = sys.argv[:]
        for help_str in ["--help", "-h"]:
            if help_str in args_nohelp:
                args_nohelp.remove(help_str)

        (self.args, _) = self._parser.parse_known_args(args_nohelp)

        self._setup_logging()

    def _setup_logging(self):
        """
        Sets up the logging interface.
        All modules wishing to log messages, should import the logging module
        and use the given interface. logging.getLogger() returns the default logger,
        which is the one being set up in this function.
        """
        try:
            loglevel = getattr(logging, self.args.loglevel.upper())
        except AttributeError:
            loglevel = logging.INFO

        logging.basicConfig(level=loglevel,format='[Logan:%(levelname)s] %(message)s')

    def parse_args(self):
        self.args = self._parser.parse_args()

    def add_argument(self, *args, **kwargs):
        """
        Wrapper for self._parser.add_argument.
        """
        self._parser.add_argument(*args, **kwargs)

    def get_datasource_module(self):
        """
        @return: A module instance with the specified datasource module.
        """
        return __import__("logan.datasource.{}".format(self.args.datasource),
                fromlist=["*"])

    def get_dataoutput_modules(self):
        """
        @return: Set of module instances with the specified dataoutput modules.
        """
        return frozenset(__import__("logan.dataoutput.{}".format(dataoutput),
                fromlist=["*"]) for dataoutput in self.args.dataoutputs)

def list_datamodules():
    """
    Lists available datasources and dataoutputs with description.
    """
    for xend in ["datasource", "dataoutput"]:
        print("------------------------------------------------------")
        print("{}:".format(xend))
        filename, pathname, description = imp.find_module("logan/{}".format(xend))
        if filename is None: # is package
            modules = (os.path.splitext(module)[0] for module in os.listdir(pathname) \
                    if not module.startswith("__init__") and not module == "__pycache__" and \
                        (module.endswith((".py", ".pyc", ".pyo")) or \
                            os.path.isdir(os.path.join(pathname, module))) )

            # using set, so that it returns only one element per module,
            # as there could be .py, .pyc and/or .pyo files of same module.
            for name in sorted(frozenset(modules)):
                try:
                    temp_module = __import__("logan.{}.{}".format(xend, name),
                            fromlist=["get_description"])
                    print("    {}  {}".format(name.ljust(15),
                        temp_module.get_description()[0]))
                except:
                    pass
    print("------------------------------------------------------")

def gen_make_script(logan_config):
    import lancet
    vcs_info = lancet.vcs_metadata([logan.BASEPATH])

    make_scripts = (os.path.join(path, "make.sh") for path in
                    [logan_config.args.dout_path, logan_config.args.dsrc_paths[0]] if
                    os.path.exists(path))

    make_script_code = None
    for make_script in make_scripts:
        if os.path.exists(make_script):
            # Only ever keep overwriting the make.sh file in the first path
            # (dataoutput-path).
            if make_script_code is not None:
                logging.warning("File exists: {}".format(make_script))
                continue

            # Possible issue when overwriting make.sh while it is running? As a
            # precaution, just remove/unlink old make.sh.
            os.remove(make_script)

        make_script_code = """#!/usr/bin/env bash
# Generated: {}

# Logan version: {}
# Logan modifications:
# |{}

cd {}

{}

{}
""".format     (datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "\n# |".join(vcs_info['vcs_messages'][logan.BASEPATH].split('\n')),
                "\n# |".join(vcs_info['vcs_diffs'][logan.BASEPATH].split('\n')),
                pipes.quote(os.getcwd()),
                "\n".join("export {}={}".format(k, pipes.quote(os.environ[k])) \
                    for k in os.environ if \
                        k.startswith("LOGAN") or \
                        k == "PYTHONPATH" or \
                        k == "PYTHON"),
                (os.environ["PYTHON"]+" " if "PYTHON" in os.environ else "") + \
                        " ".join(pipes.quote(a) for a in sys.argv))

        with open(make_script, "w") as script_file:
            script_file.write(make_script_code)
            logging.info("Written {}".format(make_script))

def main(argv):
    the_time = time.time()
    def show_elapsed_time():
        logging.info("Elapsed time: {:.2f} sec".format(time.time() - the_time))

    logan_config = LoganConfig()

    logging.debug("Python {}".format(sys.version.replace("\n", "\n              ")))

    if logan_config.args.list_datamodules:
        list_datamodules()
        return 0

    # Load modules
    datasource_module = logan_config.get_datasource_module()
    dataoutput_modules = logan_config.get_dataoutput_modules()

    # Always register base arguments (but only once)
    logan.datasource.base.register_arguments(logan_config)
    logan.dataoutput.base.register_arguments(logan_config)

    # Allow modules to register special options
    if datasource_module is not logan.datasource.base:
        datasource_module.register_arguments(logan_config)
    for dataoutput_module in dataoutput_modules:
        if dataoutput_module is not logan.dataoutput.base:
            dataoutput_module.register_arguments(logan_config)

    # Get all args
    logan_config.parse_args()

    # Show selected datamodules information
    if logan_config.args.show_datamodules:
        print("-" * 79)
        logan.compat.print_blank()
        for summary, description in [datasource_module.get_description(),
                dataoutput_module.get_description()]:

            if None in [summary, description]:
                continue

            print("=" * len(summary))
            print(summary)
            print("=" * len(summary))
            print(description)
            logan.compat.print_blank()
        print("-" * 79)
        logan.compat.print_blank()

    # Generate outputs from source
    if logan_config.args.dsrc_gen_data != 0 or logan_config.args.dsrc_gen_data_only:
        # gen_data_only implies gen_data
        if logan_config.args.dsrc_gen_data == 0:
            logan_config.args.dsrc_gen_data = logan_config.args.dsrc_gen_data_only

        data_source_generator = datasource_module.DataSourceGenerator(logan_config)
        if not isinstance(data_source_generator, logan.datasource.base.DataSourceGenerator):
            raise Exception("not based on datasource.base.DataSourceGenerator")

        logging.info("Initiating datasource data generation ...")
        try:
            run_info_list = data_source_generator()
            logging.info("Datasource data generation done.")
        except KeyboardInterrupt:
            run_info_list = None
            logan.compat.print_blank()
            logging.warn("Datasource data generation aborted!")

        if run_info_list:
            paths_count = min(len(logan_config.args.dsrc_paths), len(run_info_list))
            # Modify AFTER runs, as they use the original dsrc_paths!
            if any(logan_config.args.dsrc_paths[i] != \
                    run_info_list[i]['root_directory_full'] for i in range(paths_count)):
                logan_config.args.dsrc_paths = [run_info['root_directory_full'] for run_info in run_info_list]
                logging.info("Modified source paths: {}".format(logan_config.args.dsrc_paths))

                # Modify output path accordingly
                commonprefix = os.path.commonprefix(logan_config.args.dsrc_paths)

                # Assume that if we have multiple runs, each run's results are
                # stored in their separate subfolder.
                if len(run_info_list) > 1:
                    commonprefix = os.path.dirname(commonprefix)

                logan_config.args.dout_path = os.path.join(
                        logan_config.args.dout_path,
                        os.path.basename(commonprefix))
                logging.info("Modified output path: {}".format(
                    logan_config.args.dout_path))

        show_elapsed_time()

    if not logan_config.args.dsrc_gen_data_only:
        # Setup source/output processors
        data_source = datasource_module.DataSource(logan_config)
        if not isinstance(data_source, logan.datasource.base.DataSource):
            raise Exception("{} not based on datasource.base.DataSource".format(
                datasource_module.DataSource))

        data_outputs = []
        for dataoutput_module in dataoutput_modules:
            data_output = dataoutput_module.DataOutput(logan_config)
            if not isinstance(data_output, logan.dataoutput.base.DataOutput):
                raise Exception("{} not based on dataoutput.base.DataOutput".format(
                    dataoutput_module.DataOutput))
            data_outputs.append(data_output)

        logging.info("Initiating datasource data processing with {} ...".format(
            data_source.__class__))
        if not data_source.process():
            logging.critical("Datasource data processing with {} failed! Aborting ...".format(
                data_source.__class__))
            return 1
        logging.info("Datasource data processing done.")

        for data_output in data_outputs:
            logging.info("Initiating dataoutput analysis generation with {} ...".format(
                data_output.__class__))
            if not data_output.generate(data_source):
                logging.critical("Dataoutput analysis generation with {} failed! Aborting ...".format(
                    data_output.__class__))
                return 1

        logging.info("Output generation done.")

        show_elapsed_time()

    # Reproducability!
    if logan_config.args.gen_script:
        gen_make_script(logan_config)

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))

