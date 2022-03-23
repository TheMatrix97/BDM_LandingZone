from abc import abstractmethod

from hdfs import InsecureClient

from controllers.database_controller import Database
from properties_parser import parse_properties


class Process:

    def __init__(self):
        self._host = parse_properties('hdfs')['hdfs.host']
        self._user = parse_properties('hdfs')['hdfs.user']
        self._client = InsecureClient(f'http://{self._host}', user=self._user)  # Connect to HDFS
        self._database = Database('p1')

    @abstractmethod
    def run_process(self):
        pass

    @staticmethod
    def _linux_normalize_path(path: str):  # required for windows compatibility
        return path.replace('\\', '/')
