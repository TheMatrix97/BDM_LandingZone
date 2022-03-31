# Landing Zone Parser

This repository contains a Python script used during the realization of P1 BDM project.
Providing the following batch job functionalities:

- **Copy data from local to temporal landing zone**: Uploads your datasets folder content to 
  a remote HDFS, using the information available under a MongoDB database in charge of 
  storing metadata used for control the overall system.
- **Move data from temporal to persisted landing zone**: Once data is loaded to the 
temporal landing zone, it will be joined in batch .parquet files, providing 
  a fast and reliable source, for following processes.
  
## Requirements

- MongoDB
- HDFS Server
- Python 3

## Install

### MongoDB

You can follow the installation instructions described in the official [MongoDB documentation](https://www.mongodb.com/docs/manual/installation/)

- Once MongoDB is running, create a collection named `datasources` and populate it 
  with the contents of [objects.json](/datasets/objects.json) file.

- Create a `project.properties` file following the template indicating the connection credentials, we are 
providing our .properties used in our private environment
    ```properties
    [mongodb]
    database.user=admin
    database.host=10.4.41.74:27017
    database.password=bdmproject
    
    [hdfs]
    hdfs.host=10.4.41.74:9870
    hdfs.user=bdm
    ```
### HDFS

This project also requires a HDFS, that will be used as the persistance layer of our landing zone
You can follow [this documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) 
for installing a single Hadoop node. **Important**, don't forget to double check the `project.properties`, to make sure
the HDFS server host and user are correct.

### Python environment

It's recommended to create a virtualenv for running the script, this ensures dependencies are only installed
in a testing environment and not in the global python installation.

```bash
$ virtualenv .venv
$ source .venv/bin/activate
```
Now, you can install the project dependencies
```bash
$ pip3 install -r requirements.txt
```

## Run the script

### Load data to the temporal Landing Zone

Just, run the following command indicating the datasets source folder ([./datasets](./datasets))
````bash
$ python3 main.py --temporal-batch --datasource-folder ./datasets
````

### Process data from temporal to persisted Landing Zone

Run the following command

````bash
$ python3 main.py --persist-batch
````