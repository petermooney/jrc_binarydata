import requests
import json
from geojson import Point, Feature, FeatureCollection, dump
import time
import os.path
import os, glob
import numpy as np

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from fastavro import writer, reader, parse_schema
from fastavro.schema import load_schema

import experiment2_pb2

numFeatures = 20000
file_name = "experiment2_{}".format(numFeatures)
avro_schema_file_name = "experiment2"
data_CRS = "epsg:4326"

CLEAN_FOLDERS = 0
## When CLEAN_FOLDERS is set to 1 all of the folders below are cleaned
## The binary data files, geojson and response json files are deleted.

if (CLEAN_FOLDERS == 1):
    dir = './response-data'
    for file in os.scandir(dir):
        os.remove(file.path)

    dir = './binary-output'
    for file in os.scandir(dir):
        os.remove(file.path)

    dir = './geojson-output'
    for file in os.scandir(dir):
        os.remove(file.path)

"""
print ("Started downloading from API ")
tic = time.perf_counter()
response = json.loads(requests.get("https://jrc.dev.52north.org/v1.1/Locations?$skip={}&$top={}".format(numFeatures,numFeatures)).text)

print("Finished downloading from API ...")
print("Started dumping JSON to file ...")
with open('./response-data/{}.json'.format(file_name), 'w') as f:
    json.dump(response, f)

toc = time.perf_counter()
print("Finished dumping JSON to file ...")
print(f"\tTiming Information: Total:\n\tDownloaded response from API\n\tDump JSON to file\n\tTotal: {toc - tic:0.4f} seconds")
"""

## Here we work directly with the JSON response file.
file_size = os.path.getsize('./response-data/{}.json'.format(file_name))
print ("\nUsing [./response-data/{}.json] as the INPUT DATA".format(file_name))
print("JSON Response File size is {} Kb".format(round(file_size/1024),2))


pbf_geojson_timing = []
json_pbf_timing = []
json_avro_timing = []
avro_geojson_timing = []
json_geojson_timing = []


for i in range(0,10):

    print ("========= GeoJSON ===========")
    print ("Searialize JSON to GeoJSON ")
    tic = time.perf_counter()

    with open('./response-data/{}.json'.format(file_name)) as json_file:
        response = json.load(json_file)
        geoJSON_Features = []

        for r in response["value"]:

            iot_selfLink = r["@iot.selfLink"]
            name = r["name"]
            localID = r["@iot.id"]
            longitude = r["location"]["coordinates"][0]
            latitude = r["location"]["coordinates"][1]
            description = r["description"]
            iotSelfLink = r["@iot.selfLink"]
            historicalLocations = r["HistoricalLocations@iot.navigationLink"]
            thingsNavigation = r["Things@iot.navigationLink"]
            response_properties = {}
            response_properties["name"] = name
            response_properties["@iot.id"] = localID
            response_properties["description"] = description
            response_properties["@iot.selfLink"] = iot_selfLink
            response_properties["Things@iot.navigationLink"] = thingsNavigation
            response_properties["HistoricalLocations@iot.navigationLink"] = historicalLocations
            geocoord = Point((float(longitude), float(latitude)))
            geoJSON_Features.append(Feature(geometry=geocoord, properties=response_properties))

    geoJSON_feature_collection = FeatureCollection(geoJSON_Features)

    with open('./geojson-output/{}.geojson'.format(file_name), 'w') as f:
       dump(geoJSON_feature_collection, f)

    print ("Finished serializing JSON to GeoJSON")
    toc = time.perf_counter()
    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./geojson-output/{}.geojson'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))
    timing_json_geojson = toc - tic

    json_geojson_timing.append(timing_json_geojson)
    ######################## fast avro ###########################

    fast_avro_experiment2_schema = load_schema("{}.avsc".format(avro_schema_file_name))

    print ("========= Apache Avro ===========")
    print ("Serialize JSON to Apache Avro (fastavro)")
    tic = time.perf_counter()

    experiment2_fast_avro = []

    with open('./response-data/{}.json'.format(file_name)) as json_file:
        response = json.load(json_file)


        for row in response["value"]:

            iot_selfLink = row["@iot.selfLink"]
            name = row["name"]
            localID = row["@iot.id"]
            longitude = row["location"]["coordinates"][0]
            latitude = row["location"]["coordinates"][1]
            description = row["description"]
            historicalLocations = row["HistoricalLocations@iot.navigationLink"]
            thingsNavigation = row["Things@iot.navigationLink"]

            tempExp2FastAvro = {}
            tempExp2FastAvro["name"] = name
            tempExp2FastAvro["iotid"] = localID
            tempExp2FastAvro["description"] = description
            tempExp2FastAvro["iotselfLink"] =  iot_selfLink
            tempExp2FastAvro["things_iot_navigationLink"] = thingsNavigation
            tempExp2FastAvro["historical_locations_iot_navigationLink"] = historicalLocations
            tempExp2FastAvro["longitude"] = float(longitude)
            tempExp2FastAvro["latitude"] = float(latitude)

            experiment2_fast_avro.append(tempExp2FastAvro)


    with open("./binary-output/{}_fast.avro".format(file_name), "wb") as out:
        writer(out,fast_avro_experiment2_schema,experiment2_fast_avro)


    print ("Finished creating Apache Avro file (fastavro)")
    toc = time.perf_counter()

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./binary-output/{}_fast.avro'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))
    timing_json_avro = toc - tic
    json_avro_timing.append(timing_json_avro)


    ######################## Apache Avro ###########################

    print ("========= Apache Avro ===========")
    print ("Deserialize Apache Avro to GeoJSON")

    tic = time.perf_counter()
    geoJSON_Features_read = []
    with open('./binary-output/{}_fast.avro'.format(file_name), "rb") as fastavro_fo:
        for fastAvroObj in reader(fastavro_fo):
            fastAvro_properties = {}
            fastAvro_properties["name"] = fastAvroObj["name"]
            fastAvro_properties["@iot.id"] = fastAvroObj["iotid"]
            fastAvro_properties["description"] = fastAvroObj["description"]
            fastAvro_properties["@iot.selfLink"] = fastAvroObj["iotselfLink"]
            fastAvro_properties["Things@iot.navigationLink"] = fastAvroObj["things_iot_navigationLink"]
            fastAvro_properties["HistoricalLocations@iot.navigationLink"] = fastAvroObj["historical_locations_iot_navigationLink"]

            geocoord = Point((float(fastAvroObj["longitude"]), float(fastAvroObj["latitude"])))

            geoJSON_Features_read.append(Feature(geometry=geocoord, properties=fastAvro_properties))

    ## we need to add the information about the CRS to the geojson file.
    ## if the data is not WSG:84/EPSG:4326 then this needs to be specified.

    data_crs = {"type": "name","properties": {"name": "{}".format(data_CRS)}}

    ## create the FeatureCollection now.
    geoJSON_feature_collection = FeatureCollection(geoJSON_Features_read,crs=data_crs)

    geoJSON_feature_collection = FeatureCollection(geoJSON_Features_read)

    with open('./geojson-output/{}-avro_fast.geojson'.format(file_name), 'w') as f:
       dump(geoJSON_feature_collection, f)

    toc = time.perf_counter()
    print ("Finished deserialize Apache Avro to GeoJSON")


    file_size = os.path.getsize('./geojson-output/{}-avro_fast.geojson'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    print("File size is {} Kb".format(round(file_size/1024),2))
    timing_avro_geojson = toc - tic

    avro_geojson_timing.append(timing_avro_geojson)
    ######################## protocol buffer ###########################
    print ("========= Protocol Buffer ===========")
    print ("Serialize JSON to Protocol Buffer (protobuf)")
    tic = time.perf_counter()
    experiment2_locations_list = experiment2_pb2.Experiment2Locations()

    with open('./response-data/{}.json'.format(file_name)) as json_file:
        response = json.load(json_file)

        for row in response["value"]:
            #print("{}".format(row["@iot.selfLink"]))
            iot_selfLink = row["@iot.selfLink"]
            name = row["name"]
            localID = row["@iot.id"]
            longitude = row["location"]["coordinates"][0]
            latitude = row["location"]["coordinates"][1]
            description = row["description"]
            historicalLocations = row["HistoricalLocations@iot.navigationLink"]
            thingsNavigation = row["Things@iot.navigationLink"]


            temp_location = experiment2_locations_list.experiment2.add()
            temp_location.name = name;
            temp_location.description = name;
            temp_location.longitude = float(longitude)
            temp_location.latitude = float(latitude)
            temp_location.iotid = localID
            temp_location.iotselfLink = iot_selfLink
            temp_location.historicalLink = historicalLocations
            temp_location.thingsLink = thingsNavigation

    f = open("./binary-output/{}.pbf".format(file_name), "wb")
    f.write(experiment2_locations_list.SerializeToString())
    f.close()
    print ("Finished creating Protocol Buffer (protobuf)")
    toc = time.perf_counter()

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./binary-output/{}.pbf'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))
    timing_json_pbf = toc - tic
    json_pbf_timing.append(timing_json_pbf)
    ######################## protocol buffer ###########################
    print ("========= Protocol Buffer ===========")
    print ("Deserialize Protocol Buffer to GeoJSON")
    tic = time.perf_counter()

    # Reading data from serialized_file
    serialized_file= open("./binary-output/{}.pbf".format(file_name), "rb")
    experiment2_locations_read = experiment2_pb2.Experiment2Locations()
    experiment2_locations_read.ParseFromString(serialized_file.read())

    geoJSON_Features_read = []

    for protoBufObj in experiment2_locations_read.experiment2:
        protoBufObj_properties = {}
        protoBufObj_properties["name"] = protoBufObj.name
        protoBufObj_properties["@iot.id"] = protoBufObj.iotid
        protoBufObj_properties["description"] = protoBufObj.description
        protoBufObj_properties["@iot.selfLink"] = protoBufObj.iotselfLink
        protoBufObj_properties["Things@iot.navigationLink"] = protoBufObj.thingsLink
        protoBufObj_properties["HistoricalLocations@iot.navigationLink"] = protoBufObj.historicalLink

        geocoord = Point((float(protoBufObj.longitude), float(protoBufObj.latitude)))

        geoJSON_Features_read.append(Feature(geometry=geocoord, properties=protoBufObj_properties))

    ## we need to add the information about the CRS to the geojson file.
    ## if the data is not WSG:84/EPSG:4326 then this needs to be specified.

    data_crs = {"type": "name","properties": {"name": "{}".format(data_CRS)}}

    ## create the FeatureCollection now.
    geoJSON_feature_collection = FeatureCollection(geoJSON_Features_read,crs=data_crs)

    with open('./geojson-output/{}-pbf.geojson'.format(file_name), 'w') as f:
       dump(geoJSON_feature_collection, f)

    print ("Finished deserialize to Protocol Buffer to GeoJSON")
    toc = time.perf_counter()

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    timing_pbf_geojson = toc - tic
    pbf_geojson_timing.append(timing_pbf_geojson)

    file_size = os.path.getsize('./geojson-output/{}-pbf.geojson'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))


print ("\n\n\n==== Statistical Report ====")
print ("=====File Sizes=====")

file_size = os.path.getsize('./response-data/{}.json'.format(file_name))
print("./response-data/{}.json size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./geojson-output/{}.geojson'.format(file_name))
print("./geojson-output/{}.geojson size is {} Kb".format(file_name,round(file_size/1024),2))


file_size = os.path.getsize('./geojson-output/{}-pbf.geojson'.format(file_name))
print("./geojson-output/{}-pbf.geojson size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./geojson-output/{}-avro_fast.geojson'.format(file_name))
print("./geojson-output/{}-avro_fast.geojson size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./binary-output/{}.pbf'.format(file_name))
print("./geojson-output/{}.pbf size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./binary-output/{}_fast.avro'.format(file_name))
print("./geojson-output/{}_fast.avro size is {} Kb".format(file_name,round(file_size/1024),2))


print ("=====Run Times=====")

pbf_geojson_timing_np = np.array(pbf_geojson_timing)
json_pbf_timing_np = np.array(json_pbf_timing)
json_avro_timing_np = np.array(json_avro_timing)
avro_geojson_timing_np = np.array(avro_geojson_timing)
json_geojson_timing_np  = np.array(json_geojson_timing)

print("JSON->Avro mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(json_avro_timing_np, dtype=np.float64),np.std(json_avro_timing_np, dtype=np.float64)))
print("JSON->PBF mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(json_pbf_timing_np, dtype=np.float64),np.std(json_pbf_timing_np, dtype=np.float64)))
print("JSON->GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(json_geojson_timing_np, dtype=np.float64),np.std(json_geojson_timing_np, dtype=np.float64)))
print("Avro->GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(avro_geojson_timing_np, dtype=np.float64),np.std(avro_geojson_timing_np, dtype=np.float64)))
print("PBF->GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(pbf_geojson_timing_np,dtype=np.float64),np.std(pbf_geojson_timing_np,dtype=np.float64)))
