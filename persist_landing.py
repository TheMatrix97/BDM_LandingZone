from models.datasource import Datasource
from models.tmp_landing_log_entry import TempLandingLogEntry
from process import Process
from temporal_load import TemporalLoadProcess


class LandingLoadProcess(Process):
    _log_collection_name = 'landingLog'

    def __init__(self):
        super().__init__()

    def run_process(self):
        # Enable database
        res = self._database.find('datasources', {})
        for datasource_str in res:
            datasource = Datasource(datasource_str)
            print('processing -> ' + datasource.name)
            self._batch_load_landing_inner(datasource)

    def _batch_load_landing_inner(self, datasource):
        # Get max timestamp log temporal
        max_time = self._get_max_timestamp_tmp_log(datasource.name)
        print(f'[{datasource.name}] max timestamp -> {str(max_time)}')
        files_to_process = self._get_files_to_process(max_time, datasource.name)
        print(f'[{datasource.name}] Num files to process -> {str(len(files_to_process))}')
        # TODO Process files and store in Parquet

    def _get_files_to_process(self, max_time, source_name):
        # Search in landing tmp files to pending to process
        res = self._database.find(TemporalLoadProcess.tmp_log_collection_name,
                                  {"timestamp": {"$gt": max_time}, "source_name": source_name})
        return list(res)

    def _get_max_timestamp_tmp_log(self, source_name):
        res = self._database.aggregate(self._log_collection_name, [
            {"$match": {"source_name": source_name}},
            {"$sort": {"timestamp": -1}},
            {"$limit": 1}])
        max_timestamp = 0 if not res.alive else res.next()['timestamp']
        return max_timestamp
