
"""Megafon API"""
requestTimeout = 30
parallelRequests = 50
QPS = 40

from MegafonAPI.State import State
from MegafonAPI.HttpAdapter import HttpAdapter
from MegafonAPI.LK import LK
from MegafonAPI.VATS import VATS