#pylint: skip-file

"""
Helper functions to create Python 2 and 3 compatible code.
"""

import sys

if sys.version_info >= (3, 0):
    def decode_to_string(b, *args, **kwargs):
        return b.decode(*args, **kwargs)

    def print_blank():
        print()
else:
    def decode_to_string(b, *args, **kwargs):
        return b

    def print_blank():
        print

    input = raw_input
