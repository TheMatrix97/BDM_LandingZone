# Load files to Temporal zone
import glob
import os.path
import time

from hdfs import InsecureClient

from controllers.database_controller import Database
from models.datasource import Datasource
from models.tmp_landing_log_entry import TempLandingLogEntry
from properties_parser import parse_properties

host = parse_properties('hdfs')['hdfs.host']
user = parse_properties('hdfs')['hdfs.user']
client = InsecureClient(f'http://{host}', user=user)  # Connect to HDFS
database = Database('p1')


def batch_load_temporal(datasource_base_folder: str):
    # Enable database
    res = database.find('datasources', {})
    for datasource_str in res:
        datasource = Datasource(datasource_str)
        print('processing -> ' + datasource.name)
        batch_load_temporal_inner(datasource, datasource_base_folder)


def get_tmp_loaded_files():
    loaded_files = set()
    for item in database.find('tempLandingLog', {}):
        loaded_files.add(item['file_name'])
    return loaded_files


def batch_load_temporal_inner(datasource: Datasource, base_folder):
    # Get files
    files_list = glob.glob(os.path.normpath(base_folder + datasource.source_path))
    processed_files = get_tmp_loaded_files()
    for file in files_list:
        file_name = os.path.basename(file)
        dest_path = linux_normalize_path(os.path.normpath(os.path.join(datasource.dest_path_landing_temp, file_name)))
        try:
            if file_name in processed_files:  # if exists skip processing
                continue
            client.upload(hdfs_path=dest_path, local_path=file)
            print(f'[{datasource.name}] file loaded -> {dest_path}')
            processed_files.add(file_name)  # Add to current processed files
        except Exception as e:
            print(e)
            print(f'[{datasource.name}] Error processsing file -> {dest_path}')
            continue
        store_log(file_name, datasource.name)


def linux_normalize_path(path: str):  # required for windows compatibility
    return path.replace('\\', '/')


def store_log(filename, source_name):
    database.insert_one('tempLandingLog', TempLandingLogEntry(filename, time.time(), source_name).to_dict())
