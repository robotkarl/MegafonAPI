from enum import Enum

class BaseConsts(Enum):
    """Base constants for other modules to work stable.

    requestTimeout
    Timeout for backend calls.
    Sometimes, Megafon backends are really slow and in many cases it's really necessary to wait for almost this time to get some response

    parallelRequests
    Maximum concurent calls to backend.
    I tried many values for this variable and have found that this number is well balanced for performance/stability

    QPS
    Maximum queries to backend per second.
    No more than this number of queries per second (at list within one session) or you'll get garbage instead of data
    """

    requestTimeout = 30
    parallelRequests = 50
    QPS = 40

