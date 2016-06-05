"""
Common statistics code.
"""

import math

class NoVariability(Exception):
    pass

# --------------------
# MEAN IMPLEMENTATIONS
#
# Can be used in a reduce fashion (keep calling add), with the final result
# being obtained through get.

class amean(object):
    @classmethod
    def init(cls, v, **kwargs):
        return v

    @classmethod
    def add(cls, a, b, **kwargs):
        return a + b

    @classmethod
    def get(cls, a, cnt):
        return a / float(cnt)

class gmean(object):
    @classmethod
    def init(cls, v, **kwargs):
        return v

    @classmethod
    def add(cls, a, b, **kwargs):
        return a * b

    @classmethod
    def get(cls, a, cnt):
        return math.pow(a, 1.0/float(cnt))

class wamean(object):
    @classmethod
    def init(cls, v, weight_baseline):
        return v * weight_baseline

    @classmethod
    def add(cls, aggregate, add, weight_baseline):
        return aggregate + (add * weight_baseline)

    @classmethod
    def get(cls, a, cnt):
        return a

# ----------------------------
# OUTLIER TEST IMPLEMENTATIONS

class dixon(object):
    """
    Implementation of Dixon's Q test
    """

    Q_TABLE = {
            90 : [0.941, 0.765, 0.642, 0.560, 0.507, 0.468, 0.437, 0.412, 0.392,
                  0.376, 0.361, 0.349, 0.338, 0.329],
            95 : [0.970, 0.829, 0.710, 0.625, 0.568, 0.526, 0.493, 0.466, 0.444,
                  0.426, 0.410, 0.396, 0.384, 0.374],
            99 : [0.994, 0.926, 0.821, 0.740, 0.680, 0.634, 0.598, 0.568, 0.542,
                  0.522, 0.503, 0.488, 0.475, 0.463]
    }

    Q_TABLE_START = 3

    @classmethod
    def outliers(cls, data, confidence=99, test_order=[-1, 0]):
        """
        Returns tuple (data, outliers).
        """
        q_limits = cls.Q_TABLE[confidence]
        q_limit_idx = len(data) - cls.Q_TABLE_START

        if q_limit_idx < 0 or q_limit_idx >= len(q_limits):
            raise ValueError("Dataset size not supported: {}".format(len(data)))

        q_limit = q_limits[q_limit_idx]

        data = sorted(data)
        q_range = float(data[-1] - data[0])

        if q_range == 0.0:
            raise NoVariability("No variability in data!")

        for i in test_order:
            assert i in [0, -1]

            neighbor_offset = -1 if i == -1 else 1
            q = abs(data[i] - data[i + neighbor_offset]) / q_range
            if q > q_limit:
                outliers = [data[i]]
                rest = data[:neighbor_offset] if i == -1 else data[neighbor_offset:]
                return (rest, outliers)

        return (data, [])
