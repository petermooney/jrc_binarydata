# Binary serialisation for static and dynamic data

Setting: European Joint Research Center: Technical Report: **ELISE - European Location Interoperability Solutions for e-Government: Emerging Approaches for Data Driven Innovation in Europe.**

## This is the readme file for Experiment 1 and Experiment 2
The Python code contained here was originally written in Python 3.8.10 on Ubuntu 20.04.3 LTS (focal) x86_64 (64 bit). The laptop computer was a DELL Inspiron 5567 with 16Gb memory and Intel Core(TM) i7-7500U CPU @ 2.70G processor.

### Required libraries and code.
To reproduce and replicate the experiments outlined here in this repository you will need to install a number of libraries within Python.

* The easiest method of installation of the libraries below is using pip.
* geojson https://pypi.org/project/geojson/
* FastAvro https://fastavro.readthedocs.io/en/latest/ https://pypi.org/project/fastavro/
* Avro https://avro.apache.org/docs/current/gettingstartedpython.html
* ProtoBuf https://pypi.org/project/protobuf/
* The Protoc compiler must be downloaded and built for your system. You will only need to take this step if you are creating new Protocol Buffers and need to generate the new Python code. https://developers.google.com/protocol-buffers/docs/downloads
* Requests - this is only used where the JSON data is being downloaded from an API. The source code provided uses a sample JSON file to replace the need for a URL in the source code. https://docs.python-requests.org/en/master/index.html
* GeoPandas is an open source project to make working with geospatial data in python easier. GeoPandas extends the datatypes used by pandas to allow spatial operations on geometric types. Geometric operations are performed by shapely. GeoPandas is used in Experiment 1 to handle the GPKG format directly within the Python code. https://geopandas.org/
* Numpy - The numpy library is required to calculate some simple statistics. You may already have this installed in your Python setup. https://pypi.org/project/numpy/

### Original Datasets
Within the `experiment1` and `experiment2/response-data` folder you will find the original datasets used in the testing and analysis using the Python code provided. In the experiment1 folder you will find a file called `original-dataset-experiment1.gpkg` which is a GeoPackage file containing 20,000 randomly generated point locations. In the experiment2 folder you will find a file called `original-dataset-experiment2.json` which also contains 20,000 point locations but the data is from an API call. Both files are provided for reproducibilty assistance. To use these two datasets in the experiments you will need to change the filenames where indicated in the source code for both experiments.
