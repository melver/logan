#!/usr/bin/env python

"""
Task-farm submission script.
"""

import sys
import os
import subprocess
import json
import math

# Defaults
PROCESSES_PER_NODE = 24
WALLTIME = "47:58:59"
WORKER_BACKEND = "mpi_tf"

BATCH_SCRIPT="""#!/bin/bash --login
#PBS -N {batch_name}
#PBS -l select={num_nodes}
#PBS -q long
#PBS -l walltime={walltime}
#PBS -o {stdout_path}
#PBS -e {stderr_path}
#PBS -A {budget}

export PBS_O_WORKDIR="$(readlink -f "$PBS_O_WORKDIR")"
cd "$PBS_O_WORKDIR"

module load python
module load ptf
{custom_setup}

aprun -n {num_pes} {extra_args} {worker} "{json_path}" {parallel_workers}
"""

# If the backend supports resuming, it can create a .running file in the
# root-dir, in which case, if that file still exists, the resume script can
# resubmit the job, and the backend implementation should continue from where
# it was interrupted.
RESUME_SCRIPT = """#!/bin/bash
cd "{cwd}"
[[ -f "{root_directory}/.running" ]] || exit 14
qsub {root_directory}/qsub.sh
"""

WORKER = {
    'ptf' : lambda argv: "ptf {}".format(
        os.path.join(os.path.dirname(argv[0]), "ptf_worker.py")),
    'mpi_tf' : lambda argv: os.path.join(os.path.dirname(argv[0]), "mpi_tf_worker.py")
}

def main(argv):
    json_path = argv[1]
    batch_name = argv[2][:15]
    job_count = int(argv[3])

    # Just fail if the file does not exist or the required fields aren't set
    with open(os.path.join(os.environ["HOME"], ".logan", "tf_submit.rc"), "r") as rcfile:
        config = json.load(rcfile)

        # Use defaults if not set
        processes_per_node = config.get('processes_per_node', PROCESSES_PER_NODE)
        extra_args = config.get('extra_args', "")
        custom_setup = config.get('custom_setup', "")

        # No defaults, fail if not set
        budget = config['budget']

    # Get the number of nodes we want to request
    max_concurrency = min(int(argv[4]), math.floor(job_count / processes_per_node))

    worker = WORKER[WORKER_BACKEND](sys.argv)
    parallel_workers = max_concurrency * processes_per_node
    root_directory = os.path.dirname(json_path)
    stdout_path = os.path.join(root_directory, "qsub.o")
    stderr_path = os.path.join(root_directory, "qsub.e")

    batch_script = BATCH_SCRIPT.format(
            batch_name=batch_name,
            num_nodes=max_concurrency,
            walltime=WALLTIME,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            budget=budget,
            custom_setup=custom_setup,
            num_pes=parallel_workers,
            extra_args=extra_args,
            worker=worker,
            json_path=json_path,
            parallel_workers=parallel_workers)

    with open(os.path.join(root_directory, "qsub.sh"), 'w') as f:
        f.write(batch_script)

    resume_script = RESUME_SCRIPT.format(
            cwd=os.getcwd(),
            root_directory=root_directory)

    with open(os.path.join(root_directory, "resume.sh"), 'w') as f:
        f.write(resume_script)

    print(batch_script)
    qsub = subprocess.Popen(["qsub"], stdin=subprocess.PIPE)
    qsub.communicate(batch_script.encode())
    return qsub.wait()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
