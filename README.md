## This is the readme file for Experiment 1 and Experiment 2

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
