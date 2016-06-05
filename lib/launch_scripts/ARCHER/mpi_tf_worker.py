#!/usr/bin/env python2
# Needs to run with ARCHER's Python version.

"""
MPI Task-farm worker script.

Inspired by
https://raw.githubusercontent.com/jbornschein/mpi4py-examples/master/09-task-pull.py
"""

import sys
import os
import json
import subprocess
import collections
import time
import pickle

from mpi4py import MPI

# Have the master execute a task as well or not
MASTER_TASK = True

class Tags(object):
    READY = 0x1
    DONE  = 0x2
    EXIT  = 0x4
    START = 0x8

MPI_Context = collections.namedtuple('MPI_Context',
                                     ['comm', 'size', 'rank', 'status'])

def exec_process(process):
    cmd = process['cmd']
    tid = process['tid']
    stdout_path = process['stdout']
    stderr_path = process['stderr']

    stdout_handle = open(stdout_path, 'wb')
    stderr_handle = open(stderr_path, 'wb')

    proc = subprocess.Popen(cmd, stdout=stdout_handle, stderr=stderr_handle)

    def _check_status(wait=True):
        if wait: proc.wait()
        result = proc.poll()
        if result is not None:
            stdout_handle.close()
            stderr_handle.close()
        return proc.poll()

    return _check_status

def master(mpi_ctx, json_path):
    with open(json_path, 'r') as json_file:
        processes = json.load(json_file)

    root_directory = os.path.dirname(json_path)
    runningfile_name = os.path.join(root_directory, ".running")

    processes_iter = iter(processes)
    processes_running = {}
    if os.path.exists(runningfile_name):
        # Resume from snapshot
        print("!! Resuming from snapshot !!")
        with open(runningfile_name, 'rb') as runningfile:
            processes_complete = pickle.load(runningfile)
    else:
        processes_complete = set()

    def next_process():
        while True:
            p = next(processes_iter)
            if p['tid'] not in processes_complete:
                return p

    def snapshot_progress():
        with open(runningfile_name, 'wb') as runningfile:
            pickle.dump(processes_complete, runningfile)

    worker_count = mpi_ctx.size - 1
    worker_exit_count = 0

    print("---[ Master starting with {} workers @ {} ]---".format(
        worker_count, time.strftime("%Y-%m-%dT%H:%M:%S%z")))

    check_status = None
    while worker_exit_count < worker_count:
        if MASTER_TASK:
            # Master should also execute a task.
            #
            # The master will sit idle after finishing its task until an MPI
            # message is received again.
            if check_status is not None and check_status(False) is not None:
                print("* Master finished task with result: {}".format(check_status()))
                processes_complete[0].add(processes_running[0])
                del processes_running[0]
                check_status = None

            if check_status is None:
                try:
                    process = next_process()
                except StopIteration:
                    process = None

                if process is not None:
                    print("* Executing tid {} on master".format(process['tid']))
                    try:
                        check_status = exec_process(process)
                        processes_running[0] = process['tid']
                    except Exception as e:
                        print("* Executing task on master failed: {}".format(e))
                        check_status = None
                        continue

        # Wait for messages from workers
        data = mpi_ctx.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG,
                                 status=mpi_ctx.status)
        source = mpi_ctx.status.Get_source()
        tag = mpi_ctx.status.Get_tag()

        if tag == Tags.READY:
            try:
                process = next_process()
                mpi_ctx.comm.send(process, dest=source, tag=Tags.START)
                print("+ Sending tid {} to worker {}".format(process['tid'], source))
                processes_running[source] = process['tid']
            except StopIteration:
                mpi_ctx.comm.send(None, dest=source, tag=Tags.EXIT)

        elif tag == Tags.DONE:
            print("| Worker {} finished task with result: {}".format(
                source, data))
            processes_complete.add(processes_running[source])
            del processes_running[source]
        elif tag == Tags.EXIT:
            print("= Worker {} exited with reason: {}".format(
                source, data))
            worker_exit_count += 1

        snapshot_progress()

    if check_status is not None:
        print("* Master finished task with result: {}".format(check_status()))

    print("---[ Master finishing @ {} ]---".format(
        time.strftime("%Y-%m-%dT%H:%M:%S%z")))

    os.remove(runningfile_name)
    return 0

def worker(mpi_ctx):
    while True:
        try:
            mpi_ctx.comm.send(None, dest=0, tag=Tags.READY)
            process = mpi_ctx.comm.recv(source=0, tag=MPI.ANY_TAG, status=mpi_ctx.status)
            tag = mpi_ctx.status.Get_tag()
        except Exception as e:
            reason = str(e)
            break

        if tag == Tags.START:
            try:
                check_status = exec_process(process)
                result = check_status()
            except Exception as e:
                result = str(e)

            mpi_ctx.comm.send(result, dest=0, tag=Tags.DONE)
        elif tag == Tags.EXIT:
            reason = "normal"
            break

    mpi_ctx.comm.send(reason, dest=0, tag=Tags.EXIT)
    return 0

def main(argv):
    json_path = argv[1]

    comm = MPI.COMM_WORLD
    mpi_ctx = MPI_Context(
            comm = comm,
            size = comm.Get_size(),
            rank = comm.Get_rank(),
            status = MPI.Status())

    if mpi_ctx.rank == 0:
        return master(mpi_ctx, json_path)
    else:
        return worker(mpi_ctx)

if __name__ == "__main__":
    sys.exit(main(sys.argv))
