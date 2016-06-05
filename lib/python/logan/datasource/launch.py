"""
Common launch code
"""

import os
import collections
import subprocess
import lancet

def _supported_QLauncher():
    which_qsub = subprocess.Popen(["which", "qsub"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    which_qsub.communicate()
    return which_qsub.poll() == 0

def _configure_Launcher(backend_class, logan_config):
    backend_class.max_concurrency = logan_config.args.jobs

def _supported_ARCHER():
    which_aprun = subprocess.Popen(["which", "aprun"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    which_aprun.communicate()
    return which_aprun.poll() == 0

def _configure_ARCHER(backend_class, logan_config):
    backend_class.max_concurrency = logan_config.args.jobs
    backend_class.script_path = os.path.abspath(os.path.join(os.path.dirname(
        os.path.abspath(__file__)), os.path.pardir, os.path.pardir,
        os.pardir, "launch_scripts", "ARCHER", "tf_submit.py"))

BACKENDS = collections.OrderedDict([
    ('ARCHER', {
        'class' : lancet.ScriptLauncher,
        'supported' : _supported_ARCHER,
        'configure' : _configure_ARCHER
        }),

    ('QLauncher', {
        'class' : lancet.QLauncher,
        'supported' : _supported_QLauncher
        }),

    ('Launcher', {
        'class' : lancet.Launcher,
        'supported' : lambda: True,
        'configure' : _configure_Launcher
        }),
])

def get_launcher_backend(logan_config):
    backend = os.environ.get("LOGAN_LAUNCHER")
    if backend is None:
        for backend in BACKENDS:
            if BACKENDS[backend]['supported'](): break

    backend_entry = BACKENDS[backend]
    backend_class = BACKENDS[backend]['class']
    if 'configure' in backend_entry:
        backend_entry['configure'](backend_class, logan_config)

    return backend_class
