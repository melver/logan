#!/usr/bin/env python2
# Needs to run with ARCHER's Python version.

"""
Task-farm worker script.
"""

import sys
import json
import subprocess

def main(argv):
    json_path = argv[1]
    parallel_workers = int(argv[2])
    mpi_rank = int(argv[3]) - 1

    if mpi_rank < 0 or mpi_rank >= parallel_workers:
        raise Exception("Unexpected rank! {}".format(mpi_rank))

    with open(json_path, 'r') as json_file:
        processes = json.load(json_file)

    for i in range(mpi_rank, len(processes), parallel_workers):
        cmd = processes[i]['cmd']
        tid = processes[i]['tid']
        stdout_path = processes[i]['stdout']
        stderr_path = processes[i]['stderr']

        stdout_handle = open(stdout_path, 'wb')
        stderr_handle = open(stderr_path, 'wb')

        proc = subprocess.Popen(cmd, stdout=stdout_handle, stderr=stderr_handle)
        if proc.wait() != 0:
            print("WARNING: Process with tid {} exit with code {}!".format(
                tid, proc.poll()))

        stdout_handle.close()
        stderr_handle.close()

# Does ptf run it so that __name__ == "__main__" ??
sys.exit(main(sys.argv))
