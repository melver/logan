"""
Dataoutput for matplotlib.
"""

import os
import logging
import logan.dataoutput.base
from logan.compat import *

import math
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

AVAILABLE_FORMATS = {
    None  : None,
    "pdf" : ".pdf",
    "pgf" : ".pgf",
    "png" : ".png",
    "eps" : ".eps",
    "svg" : ".svg"
}

COLOR_THEMES = {
    "color" : [["#2a2a2a", "#dadada", "#3f5b73", "#5c5fff", "#735c3f", "#f2c30c", "#3f7356", "#0cf262",
                "#6a3f73", "#f20c69", "#73433f", "#f20c29"],
               None],
    "bw"    : [["#202020", "#fefefe", "#5a5a5a", "#fefefe", "#afafaf", "#fefefe", "#efefef"],
               [None,      None,      None,      "/",       None,      "\\",      "."]],
    "colbw" : [["#303030", "#fafafa", "#6f6fa9", "#fafafa", "#c8c8a0", "#fafafa", "#efefef"],
               [None,      None,      None,      "/",       None,      "\\",      "."]]
    }

LINE_STYLES = [ "ko-", "ks--", "k^:", "ro-", "rs--", "r^:", "bo-", "bs--", "b^:"]

LEGEND_MAX_ROWS = int(os.environ.get("LOGAN_LEGEND_MAX_ROWS", 4))
HATCH_DENSITY = int(os.environ.get("LOGAN_HATCH_DENSITY", 20))

def _patch_matplotlib(args):
    global HATCH_DENSITY

    import matplotlib.hatch
    import matplotlib.patches

    if HATCH_DENSITY is not None:
        # HACK: There does not seem to be any other way to globally set
        # matplotlib's hatch-density
        HATCH_DENSITY = int(HATCH_DENSITY)

        matplotlib.hatch._orig_get_path = matplotlib.hatch.get_path
        def override_hatch_get_path(hatchpattern, density=None):
            return matplotlib.hatch._orig_get_path(hatchpattern, density=HATCH_DENSITY)
        matplotlib.hatch.get_path = override_hatch_get_path

    if args.dout_theme == "colbw":
        matplotlib.patches.Patch._orig_set_hatch = matplotlib.patches.Patch.set_hatch
        def override_Patch_set_hatch(self, hatch):
            if hatch is not None:
                ec = self.get_edgecolor()
                if hatch == "/":
                    self.set_edgecolor((ec[0], 0.40, ec[2], ec[3]))
                elif hatch == "\\":
                    self.set_edgecolor((0.45, ec[1], ec[2], ec[3]))
            matplotlib.patches.Patch._orig_set_hatch(self, hatch)
        matplotlib.patches.Patch.set_hatch = override_Patch_set_hatch

def register_arguments(logan_config):
    """
    Interface function.
    When module is loaded, this function is called by the main module,
    allowing this module to register its own command-line arguments.

    @type logan_config: LoganConfig
    """
    logan_config.add_argument("--mpl-interactive", dest='mpl_interactive',
            action="store_true", default=False,
            help="Display interactive selection, to select from available datasets.")

def get_description():
    """
    Interface function. Used to query description.
    """
    return ("Dataoutput for matplotlib. [Formats: GUI (default), {}]".format(
                ", ".join(k for k in AVAILABLE_FORMATS.keys() if k is not None)),
            None)

class DataOutput(logan.dataoutput.base.DataOutput):
    def __init__(self, logan_config):
        """
        @type logan_config: LoganConfig
        """
        super(DataOutput, self).__init__(logan_config)
        _patch_matplotlib(self.logan_config.args)

    def _get_bar_color_args(self, i):
        colors, hatches = COLOR_THEMES[self.logan_config.args.dout_theme]
        if hatches is not None:
            return {'hatch' : hatches[i % len(hatches)],
                    'color' : colors[i % len(colors)],
                    'ecolor' : 'gray'}
        else:
            return {'color' : colors[i % len(colors)],
                    'ecolor' : 'gray'}

    def _plot_bar(self, data_source, dataset):
        # Store result to ensure ordering is preserved -- don't sort, sorted by data_source
        cluster_keys = data_source.get_cluster_keys(dataset=dataset)
        xtick_keys = data_source.get_xtick_keys(dataset=dataset)
        stack_keys = data_source.get_stack_keys(dataset=dataset)
        fontsize = data_source.get_presentation_hints(dataset).get('fontsize', "large")
        color_offset = data_source.get_presentation_hints(dataset).get('color_offset', 0)

        matplotlib.rc("xtick", labelsize=fontsize)
        matplotlib.rc("ytick", labelsize=fontsize)

        ind = np.arange(len(xtick_keys))
        width_bar_factor = data_source.get_presentation_hints(dataset).get('bar_width', 0.75)
        if len(ind) > 1:
            width_bar = (ind[-1] / (len(cluster_keys) * len(xtick_keys)))*width_bar_factor
        else:
            width_bar = (len(cluster_keys) * len(xtick_keys))*width_bar_factor

        override_figsize = data_source.get_presentation_hints(dataset).get('size') or \
                           self.logan_config.args.dout_size
        auto_figsize = [max(12, ind[-1]*1.2), 6]
        figsize = auto_figsize if not override_figsize \
                  else [float(k) if k else auto_figsize[i] for i,k in \
                        enumerate(override_figsize.split("x"))]

        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(111)

        # Setup x-Axis
        ax.yaxis.grid(True)
        xtick_labels = [data_source.map_to_name(x) for x in xtick_keys]
        xtick_offset = width_bar*len(cluster_keys)/2.0
        ax.set_xticks(ind+xtick_offset)
        ax.set_xlim([ind[0]-width_bar, ind[-1]+xtick_offset*2.0+width_bar])

        # When to rotate x-labels
        if max(len(s) for s in xtick_labels) > 5:
            ax.set_xticklabels(xtick_labels,
                               rotation=-18,
                               horizontalalignment='left',
                               fontsize=fontsize)
            box = ax.get_position()
            ax.set_position([box.x0, box.y0 + box.height * 0.07,
                             box.width, box.height * 0.93])
        else:
            ax.set_xticklabels(xtick_labels, fontsize=fontsize)

        if data_source.get_xlabel(dataset=dataset) is not None:
            ax.set_xlabel(data_source.get_xlabel(dataset=dataset), fontsize=fontsize)

        # Setup y-Axis
        ax.set_ylabel(data_source.get_ylabel(dataset=dataset), fontsize=fontsize)

        bars_clustered = []
        all_y_values = []
        for i, cluster in enumerate(cluster_keys):
            bars_stacked = []
            y_values_summed = None
            for j, stack in enumerate(stack_keys if stack_keys is not None else [None]):
                data_gen = (data_source.query_data(x=x, cluster=cluster, dataset=dataset, stack=stack) for \
                            x in xtick_keys)

                y_values = np.zeros(len(xtick_keys))
                y_errs = np.zeros((2,len(xtick_keys)))
                y_err_used = False

                for data_idx, data_point in enumerate(data_gen):
                    if not isinstance(data_point.y, dict) and not math.isnan(data_point.y):
                        y_values[data_idx] = data_point.y
                        if data_point.y_err != (0,0):
                            y_err_used = True
                            y_errs[0][data_idx] = data_point.y_err[0]
                            y_errs[1][data_idx] = data_point.y_err[1]

                if not y_err_used:
                    y_errs = None

                if stack_keys is None:
                    bar_colors = self._get_bar_color_args(i+color_offset)
                else:
                    bar_colors = self._get_bar_color_args(j+color_offset)

                if j == 0:
                    bars_stacked.append(ax.bar(ind+width_bar*i,
                                                 y_values,
                                                 width_bar,
                                                 yerr=y_errs,
                                                 **bar_colors))
                else:
                    bars_stacked.append(ax.bar(ind+width_bar*i,
                                                 y_values,
                                                 width_bar,
                                                 yerr=y_errs,
                                                 bottom=y_values_summed,
                                                 **bar_colors))

                if y_values_summed is None:
                    y_values_summed = y_values
                else:
                    y_values_summed += y_values

            bars_clustered.append(bars_stacked)
            all_y_values += list(y_values_summed)

        # Setup y-limits
        y_range = data_source.get_yrange(dataset=dataset)
        if y_range is not None:
            if y_range in ['auto', 'auto_nozoom']:
                # Auto
                if y_range == 'auto':
                    y_min = min(all_y_values) * 0.5
                else:
                    y_min = min([0.0] + all_y_values)

                y_max = max(all_y_values)
                y_max_lim = np.mean(all_y_values) * 1.45
                if y_max < y_max_lim:
                    y_max_lim = y_max * 1.025
                ax.set_ylim([y_min, y_max_lim])
            else:
                y_max_lim = y_range[1]
                ax.set_ylim(y_range)

            # Show actual value above those that are larger than y_max_lim
            overtext_shift = data_source.get_presentation_hints(dataset).get('overtext_shift', 0.0)
            overtext_format = data_source.get_presentation_hints(dataset).get('overtext_format', "{:.2f}")
            for stacks in bars_clustered:
                for bar in stacks[-1]:
                    if bar.get_height() >= y_max_lim:
                        ax.text(bar.get_x()-bar.get_width()*0.5,
                                y_max_lim*1.05+(0.022*(auto_figsize[1]-figsize[1]))+overtext_shift,
                                overtext_format.format(bar.get_height()),
                                rotation=57,
                                horizontalalignment='left',
                                fontsize={'small':'x-small', 'medium':'small', 'large':'medium',
                                          'x-large':'large', 'xx-large':'x-large'}[fontsize])

        if data_source.get_presentation_hints(dataset).get('show_legend', True):
            # Setup legend
            if stack_keys is None:
                legend_params = (
                            [b[0][0] for b in bars_clustered],
                            [data_source.map_to_name(val) for val in cluster_keys]
                        )
            else:
                legend_params = (
                            [b[0] for b in bars_clustered[0]],
                            [data_source.map_to_name(val) for val in stack_keys],
                        )

                max_len = max(len(data_source.map_to_name(val)) for val in cluster_keys)
                cluster_annotation = \
                   "".join("({}) {}{}".format(
                            i+1,
                            data_source.map_to_name(val),
                            "\n" if (i+1)%(max((figsize[0]*10)//max_len,1))==0 else "  ") \
                                    for i, val in enumerate(cluster_keys)
                            )

                ax.annotate(
                        "L-R: "+cluster_annotation,
                        (0.5, -1.32-(0.01*(auto_figsize[1]-figsize[1]))),
                        xycoords="axes fraction", va="center", ha="center",
                        size=fontsize)

                box = ax.get_position()
                ax.set_position([box.x0, box.y0 + box.height * 0.12,
                                 box.width, box.height * 0.88])

            legend = ax.legend(*legend_params,
                      bbox_to_anchor=(0.0, 1.05+(0.032*(auto_figsize[1]-figsize[1])), 1.0, 0.102),
                      loc=3, mode='expand',
                      borderaxespad=0.0,
                      ncol=int(math.ceil(len(bars_clustered if stack_keys is None else stack_keys) / LEGEND_MAX_ROWS)),
                      fontsize=fontsize)

            box = ax.get_position()
            ax.set_position([box.x0, box.y0,
                box.width, box.height - 0.025*min(len(legend_params[0]), LEGEND_MAX_ROWS)])

    def generate(self, data_source):
        """
        Interface function to be called by the main program. This should
        trigger the generation of the output.

        @type data_source: DataSource
        @data_source: Any datasource DataSource which defines the basic interface.
        @rtype: boolean
        @return: Success or not.
        """
        out_formats = frozenset(self.logan_config.args.dout_formats) & \
                      frozenset(AVAILABLE_FORMATS.keys())

        if not os.path.exists(self.logan_config.args.dout_path):
            os.makedirs(os.path.abspath(self.logan_config.args.dout_path))

        if not out_formats:
            logging.warn("(DOUT/matplotlib) no valid output formats specified, skipping.")
            return True

        #matplotlib.rc('font', size=10)
        #matplotlib.rc('font', **{'sans-serif' : ["FreeSans", "DejaVu Sans"],
        #                         'family' : 'sans-serif'})

        def plot_and_output(dataset):
            presentation_hint_type = data_source.get_presentation_hints(dataset).get('type')
            if presentation_hint_type in [None, 'bar']:
                self._plot_bar(data_source, dataset)
            else:
                raise Exception("Can't understand presentation hint type: {}".format(presentation_hint_type))

            # Output
            for out_format in out_formats:
                if out_format is None:
                    logging.info("(DOUT/matplotlib) Displaying {} ...".format(dataset))
                    plt.show()
                else:
                    output_file_prefix = os.path.join(self.logan_config.args.dout_path,
                                                      dataset)
                    output_file_name = output_file_prefix + AVAILABLE_FORMATS[out_format]
                    logging.info("(DOUT/matplotlib) Saving {} ...".format(output_file_name))

                    plt.savefig(output_file_name, bbox_inches='tight')
            plt.close()

        if self.logan_config.args.mpl_interactive:
            options = dict(enumerate(data_source.get_dataset_keys()))

            while True:
                print("Choose from available options (space-separated):")
                for key in options:
                    print("    {}) {}".format(str(key).rjust(2), options[key]))
                print("     q) Exit")

                choice = input("Selection: ").strip().lower().split()
                if "all" in choice:
                    choice = options
                for c in choice:
                    if c == "q":
                        return True

                    try:
                        options[int(c)]
                    except:
                        print("Invalid selection!")
                        continue

                    plot_and_output(options[int(c)])

                print_blank()
        else:
            for dataset in data_source.get_dataset_keys():
                plot_and_output(dataset)

        return True

