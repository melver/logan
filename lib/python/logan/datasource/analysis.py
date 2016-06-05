"""
Common analysis code.
"""

try:
    import numpypy
except:
    pass

import numpy as np

# Compute some generic results, adheres to function interface as required by
# logan.datasource.extract_data_from_files (__init__.py)

class AnalysisError(Exception):
    pass

def count(idx=0):
    def _count(D=None,**kwargs):
        return len(D[idx])
    return _count

def scalar_int(idx=0, empty_def=None):
    def _scalar_int(D=None,**kwargs):
        if len(D[idx]) == 0:
            if empty_def is None: raise AnalysisError("0 elements")
            else:                 return empty_def
        return int(D[idx][0])
    return _scalar_int

def make_operator_type(_operator, _type):
    def operator_type(idx=0, empty_def=None):
        def _operator_type(D=None,**kwargs):
            if len(D[idx]) == 0:
                if empty_def is None: raise AnalysisError("0 elements")
                else:                 return empty_def
            return _type(_operator(map(_type, D[idx])))
        return _operator_type
    return operator_type

sum_int = make_operator_type(sum, int)
max_int = make_operator_type(max, int)

def scalar_float(idx=0, empty_def=None):
    def _scalar_float(D=None,**kwargs):
        if len(D[idx]) == 0:
            if empty_def is None: raise AnalysisError("0 elements")
            else:                 return empty_def
        return float(D[idx][0])
    return _scalar_float

def ratio_float(i1=0, i2=1, empty_def=None):
    def _ratio_float(D=None,**kwargs):
        if len(D[i1]) == 0 or len(D[i2]) == 0:
            if empty_def is None: raise AnalysisError("0 elements")
            else:                 return empty_def
        return float(D[i1][0])/float(D[i2][0])
    return _ratio_float

def sum_ratio_float(i1=0, i2=1, empty_def=None):
    def _sum_ratio_float(D=None,**kwargs):
        if len(D[i1]) == 0 or len(D[i2]) == 0:
            if empty_def is None: raise AnalysisError("0 elements")
            else:                 return empty_def
        return sum(map(float, D[i1]))/sum(map(float, D[i2]))
    return _sum_ratio_float

amean_float = make_operator_type(np.mean, float)
sum_float   = make_operator_type(sum, float)
max_float   = make_operator_type(max, float)
