# Load files to Temporal zone
import glob
import os.path
import time

from models.datasource import Datasource
from models.tmp_landing_log_entry import TempLandingLogEntry
from process import Process


class TemporalLoadProcess(Process):
    tmp_log_collection_name = 'tempLandingLog'

    def __init__(self, datasource_base_folder):
        super().__init__()
        self.datasource_base_folder = datasource_base_folder

    def run_process(self):
        # Enable database
        res = self._database.find('datasources', {})
        for datasource_str in res:
            datasource = Datasource(datasource_str)
            print('processing -> ' + datasource.name)
            self._batch_load_temporal_inner(datasource)

    def _batch_load_temporal_inner(self, datasource: Datasource):
        # Get files
        files_list = glob.glob(os.path.normpath(self.datasource_base_folder + datasource.source_path))
        processed_files = self._get_tmp_loaded_files()
        for file in files_list:
            file_name = os.path.basename(file)
            dest_path = self._linux_normalize_path(
                os.path.normpath(os.path.join(datasource.dest_path_landing_temp, file_name)))
            try:
                if file_name in processed_files:  # if exists skip processing
                    continue
                self._hdfs_client.upload(hdfs_path=dest_path, local_path=file)
                print(f'[{datasource.name}] file loaded -> {dest_path}')
                processed_files.add(file_name)  # Add to current processed files
            except Exception as e:
                print(e)
                print(f'[{datasource.name}] Error processsing file -> {dest_path}')
                continue
            self._store_log(file_name, datasource.name)

    def _get_tmp_loaded_files(self):
        loaded_files = set()
        for item in self._database.find(self.tmp_log_collection_name, {}):
            loaded_files.add(item['file_name'])
        return loaded_files

    def _store_log(self, filename, source_name):
        self._database.insert_one(
            self.tmp_log_collection_name,
            TempLandingLogEntry(filename, time.time(), source_name).to_dict())
