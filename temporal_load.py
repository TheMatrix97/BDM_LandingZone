# Load files to Temporal zone
from hdfs import InsecureClient

client = InsecureClient('http://localhost:50070') # Connect to HDFS

def batch_load_temporal():

