from urllib3.poolmanager import PoolManager
from requests.adapters import HTTPAdapter

class HttpAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=10, maxsize=200, block=block)

