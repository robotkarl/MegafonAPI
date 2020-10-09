"""Provides functionality to work with Megafon VPBX and Business portal using API methods.

    This module was developed for a specified project. However, you can use it for your own needs and help me to improve it
    https://github.com/robotkarl/MegafonAPI

    Typical usage example:

    import logging
    from MegafonAPI import LK

    logging.basicConfig(level=loglevel,
                        format='%(asctime)s:%(name)s:%(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S %Z')

    lk = LK("address", "user", "password", "description")
    if lk.getSimCards():
        lk.getSimcardServices()
        simcard = next(filter(lambda x: x["msisdn"] == "7927XXXXXXX", lk.simcards), None)
        if simcard:
            if simcard["status"] == "Активен":
                pass
            else:
                raise Exception("Simcard is blocked")
        else:
            raise Exception("Simcard not found")
    else:
        raise Exception("Failed to retrieve simcards from the server")
"""

from MegafonAPI.BaseConsts import BaseConsts
from MegafonAPI.State import State
from MegafonAPI.HttpAdapter import HttpAdapter
from MegafonAPI.LK import LK
from MegafonAPI.VATS import VATS