"""
Dataoutput for CSV.
"""

import os
import logging
import logan.dataoutput.base

def register_arguments(logan_config):
    """
    Interface function.
    When module is loaded, this function is called by the main module,
    allowing this module to register its own command-line arguments.

    @type logan_config: LoganConfig
    """

def get_description():
    """
    Interface function. Used to query description.
    """
    return ("Dataoutput for CSV", None)

class DataOutput(logan.dataoutput.base.DataOutput):
    def __init__(self, logan_config):
        """
        @type logan_config: LoganConfig
        """
        super(DataOutput, self).__init__(logan_config)

    def _make_table(self, data_source, dataset, f):
        # Store result to ensure ordering is preserved -- don't sort, sorted by data_source
        cluster_keys = data_source.get_cluster_keys(dataset=dataset)
        xtick_keys = data_source.get_xtick_keys(dataset=dataset)
        stack_keys = data_source.get_stack_keys(dataset=dataset)

        if data_source.get_xlabel(dataset=dataset) is not None:
            f.write(data_source.get_xlabel(dataset=dataset))
        f.write(";" + ";".join(data_source.map_to_name(c) for c in cluster_keys) + "\n")

        for x in xtick_keys:
            f.write("{}".format(data_source.map_to_name(x)))
            for i, cluster in enumerate(cluster_keys):
                y_summed = 0.0
                f.write(";")
                for j, stack in enumerate(stack_keys if stack_keys is not None else [None]):
                    y = data_source.query_data(x=x, cluster=cluster, dataset=dataset, stack=stack).y
                    y_summed += y
                    if stack is None:
                        f.write("{:.3f}".format(y))
                    else:
                        if j != 0: f.write("+")
                        f.write("{:.3f}[{}]".format(y, data_source.map_to_name(stack)))

                if stack is not None:
                    f.write("={:.3f}".format(y_summed))
            f.write("\n")

    def generate(self, data_source):
        """
        Interface function to be called by the main program. This should
        trigger the generation of the output.

        @type data_source: DataSource
        @data_source: Any datasource DataSource which defines the basic interface.
        @rtype: boolean
        @return: Success or not.
        """
        if not os.path.exists(self.logan_config.args.dout_path):
            os.makedirs(os.path.abspath(self.logan_config.args.dout_path))

        for dataset in data_source.get_dataset_keys():
            output_file_name = os.path.join(self.logan_config.args.dout_path, dataset) + ".csv"
            logging.info("(DOUT/csv) Writing to {} ...".format(output_file_name))
            with open(output_file_name, "w") as output_file:
                self._make_table(data_source, dataset, output_file)

        return True

