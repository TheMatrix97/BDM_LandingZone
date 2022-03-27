import itertools
import json
import os
import re
import time

import pandas
import pyarrow
import pyarrow.parquet as pq

from models.datasource import Datasource
from models.landing_log_entry import LandingLogEntry
from process import Process
from temporal_load import TemporalLoadProcess


class LandingLoadProcess(Process):
    _log_collection_name = 'landingLog'

    def __init__(self):
        super().__init__()

    def run_process(self):
        self.remove_file('tmp/*')
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
        if len(files_to_process) <= 0: # skip if there are no files to process
            print('no files to process - Skip')
            return
        file_names_to_process = list(map(lambda x: x['file_name'], files_to_process))
        print(f'[{datasource.name}] Num files to process -> {str(len(files_to_process))}')
        loaded_files = self._get_loaded_files()
        # TODO Process files and store in Parquet
        if datasource.group_regex:
            def extract_key_fun(filename):
                aux_match = re.search(datasource.group_regex, filename)
                return aux_match.group(1)
            for key, group in itertools.groupby(file_names_to_process, extract_key_fun):
                file_names = list(group)
                print(key + " :", file_names)
                parquet_file_name = key + '__' + datasource.name + '.parquet'
                self._process_files(file_names, datasource, loaded_files, parquet_file_name)
        else:
            parquet_file_name = datasource.name + '.parquet'
            self._process_files(file_names_to_process, datasource, loaded_files, parquet_file_name)

    def _process_files(self, file_names, datasource, loaded_files, parquet_file_name):
        dest_hdfs_path = self._linux_normalize_path(
            os.path.join(datasource.dest_path_landing, parquet_file_name))
        local_parquet_file_path = self._files_to_parquet(file_names, datasource, parquet_file_name)
        if parquet_file_name in loaded_files:
            # Merge loaded file with the current one
            self._merge_with_hdfs_file(local_parquet_file_path, dest_hdfs_path)
        # write parquet to hdfs
        self._hdfs_client.upload(dest_hdfs_path, local_parquet_file_path, overwrite=True)
        self.remove_file(local_parquet_file_path)  # clean local tmp file
        # Store to log
        self.save_to_log(os.path.basename(local_parquet_file_path), datasource.name, file_names)

    def _merge_with_hdfs_file(self, local_parquet_file_path, dest_hdfs_path):
        aux_path = 'tmp/downloaded.parquet'
        self._hdfs_client.download(dest_hdfs_path, aux_path, overwrite=True)
        with open(aux_path, 'rb') as reader_hdfs:
            hdfs_table = pq.read_table(reader_hdfs)
            with open(local_parquet_file_path, 'rb') as reader_local:
                local_table = pq.read_table(reader_local)
        self.remove_file(aux_path)

        #res_table = pyarrow.concat_tables([hdfs_table, local_table], True) THAT has a problem casting Structs see https://issues.apache.org/jira/browse/ARROW-1888
        #TODO Check if this work properly
        aux = pandas.concat([hdfs_table.to_pandas(), local_table.to_pandas()])
        res_table = pyarrow.Table.from_pandas(aux, preserve_index=False)
        with pq.ParquetWriter(local_parquet_file_path, schema=res_table.schema) as writer:
            writer.write_table(res_table)
            writer.close()


    def save_to_log(self, file_name, datasource_name, file_names):
        entry = LandingLogEntry(file_name, time.time(), datasource_name, file_names)
        value_to_set = entry.to_dict().copy()
        value_to_set.pop('include_files')
        self._database.collection('landingLog').find_one_and_update(
            {"file_name": file_name, "source_name": datasource_name},
            {"$set": value_to_set,
                "$addToSet": {"include_files": {"$each": entry.include_files}}}, upsert=True)

    # Creates a local parquet file to later upload
    def _files_to_parquet(self, files_names, datasource, parquet_file_name):
        parquet_path = 'tmp/' + parquet_file_name
        final_content = []
        for index, file_name in enumerate(files_names):
            path = self._linux_normalize_path(os.path.join(datasource.dest_path_landing_temp, file_name))
            with self._hdfs_client.read(path, encoding='utf-8') as reader:
                # TODO enable support to CSV and other formats
                content = json.load(reader)
                final_content.extend(content)
        table = pyarrow.Table.from_pylist(final_content)
        with pq.ParquetWriter(parquet_path, schema=table.schema) as writer:
            # Create parquet file
            writer.write_table(table)
            writer.close()
            return parquet_path

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

    def _get_loaded_files(self):
        loaded_files = set()
        for item in self._database.find(self._log_collection_name, {}):
            loaded_files.add(item['file_name'])
        return loaded_files
