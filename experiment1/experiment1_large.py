import geopandas as gpd
from geojson import Point, Feature, FeatureCollection, dump
import time
import os.path
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from fastavro import writer, reader, parse_schema
from fastavro.schema import load_schema

import address_pb2
from decimal import *
import numpy as np

getcontext().prec = 16
file_name = "experiment1"
avro_schema_file_name = "address"
INPUT_GPKG_FILE = 'test-geopackage.gpkg'

INPUT_GPKG_FILE_LAYER = 'test-geopackage'

geopkg_geojson_timing = []
geojson_pbf_timing = []
geojson_avro_timing = []
avro_geojson_timing = []
pbf_geojson_timing = []
load_geojson_timing = []

for test in range (0,5):

    ## Read the GeoPackage using GeoPandas and convert to GeoJSON file.

    tic = time.perf_counter()
    print ("===========GeoPackage to GeoJSON==============")
    print ("Begin: Converting GeoPackage to GeoJSON...")

    file_size = os.path.getsize(INPUT_GPKG_FILE)
    print("GeoPackge File size is {} Kb".format(round(file_size/1024),2))

    finland_gdf = gpd.read_file(INPUT_GPKG_FILE, layer=INPUT_GPKG_FILE_LAYER)
    finland_gdf.to_file("./geojson-output/{}.geojson".format(file_name), driver='GeoJSON')
    print ("End: Converting GeoPackage to GeoJSON...")
    toc = time.perf_counter()

    geopkg_geojson_timing.append(toc - tic)
    print(f"Timing: Converting GeoPackage to GeoJSON : {toc - tic:0.4f} seconds")
    file_size = os.path.getsize("./geojson-output/{}.geojson".format(file_name))
    print("GeoJSON File size is {} Kb".format(round(file_size/1024),2))

    tic = time.perf_counter()
    print ("\nBegin: Loading GeoJSON file for processing...")
    geojson_data = gpd.read_file("./geojson-output/{}.geojson".format(file_name))
    ## obtain the CRS of the data from GeoPandas.
    ## This is important if the CRS is not WGS 84 (EPSG:4326).
    ## Without the CRS specified, a GIS such as QGIS cannot render the GeoJSON file correctly.
    data_CRS = geojson_data.crs
    print ("Data CRS {}".format(data_CRS))

    toc = time.perf_counter()

    load_geojson_timing.append(toc - tic)
    print(f"Timing: Load GeoJSON file (using GeoPandas) : {toc - tic:0.4f} seconds")
    print ("GeoJSON file: CRS {}".format(geojson_data.crs))
    print ("GeoJSON file: Total Geometry Objects: {}".format(len(geojson_data['geometry'])))
    print ("GeoJSON file: Dataset Properties: {}, List: {}".format(len(list(geojson_data)),list(geojson_data)))
    print ("End: Loading GeoJSON file for processing...")

    ## fast avro
    print ("===========GeoJSON to Avro ==============")
    print ("Serialize GeoJSON to Apache Avro")
    fast_avro_address_schema = load_schema("{}.avsc".format(avro_schema_file_name))

    tic = time.perf_counter()
    addresses_fast_avro = []

    for index, row in geojson_data.iterrows():
        #fid =  row["fid"]
        addrHousenumber = row["addr:housenumber"]
        addrStreet = row["addr:street"]
        addrCity = row["addr:city"]
        source = row["source"]
        addrUnit = row["addr:unit"]
        fullAddress = row["fullAddress"]
        geometry = row["geometry"]

        tempAddressAvro = {}
        tempAddressAvro["addrHousenumber"] = addrHousenumber
        tempAddressAvro["addrStreet"] = addrStreet
        tempAddressAvro["addrCity"] = addrCity
        tempAddressAvro["source"] = source
        tempAddressAvro["addrUnit"] = addrUnit
        tempAddressAvro["fullAddress"] = fullAddress


        tempAddressAvro["geometry"] = geometry.to_wkt()
        tempAddressAvro["fid"] = 1

        addresses_fast_avro.append(tempAddressAvro)


    with open("./binary-output/{}_fast.avro".format(file_name), "wb") as out:
        writer(out,fast_avro_address_schema,addresses_fast_avro)


    print ("Finished serializing JSON to GeoJSON")
    toc = time.perf_counter()
    geojson_avro_timing.append(toc - tic)
    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize("./binary-output/{}_fast.avro".format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))



    #### PBF
    ## https://github.com/protocolbuffers/protobuf/issues/5450
    ## There are many good reasons why protobuf should steer clear of any validation of data inputs,
    ## even including making a field mandatory or required. Validation of values is a business logic decision,
    ## and one that cannot by solved by a data storage/interchange format.
    ## https://github.com/protocolbuffers/protobuf/issues/1606
    ## From the protobuf wire format we can tell whether a specific field exists or not.
    ## And in protobuf 2 generated code, all fields have a "HasXXX" method to tell whether the field exists or not in code.
    ## However in proto 3, we lost that ability.
    tic = time.perf_counter()
    print ("===========GeoJSON to PBF ==============")
    print ("Serialize GeoJSON to Protocol Buffer")

    address_list = address_pb2.Address()
    for index, row in geojson_data.iterrows():
        #fid =  row["fid"]
        addrHousenumber = row["addr:housenumber"]
        addrStreet = row["addr:street"]
        addrCity = row["addr:city"]
        source = row["source"]
        addrUnit = row["addr:unit"]
        fullAddress = row["fullAddress"]
        geometry = row["geometry"]


        temp_address = address_list.address.add()

        temp_address.addrHousenumber = ""
        if (addrHousenumber):
            temp_address.addrHousenumber = addrHousenumber

        temp_address.addrStreet = ""
        if (addrStreet):
            temp_address.addrStreet = addrStreet

        temp_address.addrCity = ""
        if (addrCity):
            temp_address.addrCity = addrCity

        temp_address.source = ""
        if (source):
            temp_address.source = source

        temp_address.addrUnit = ""
        if (addrUnit):
            temp_address.addrUnit = addrUnit

        temp_address.fullAddress = ""
        if (fullAddress):
            temp_address.fullAddress = fullAddress

        ## these are usually not null.

        temp_address.geometry = geometry.to_wkt()
        temp_address.fid = int(1)




    f = open("./binary-output/{}.pbf".format(file_name), "wb")
    f.write(address_list.SerializeToString())
    f.close()
    print ("Finished creating Protocol Buffer file")
    toc = time.perf_counter()
    geojson_pbf_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./binary-output/{}.pbf'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))


    ######################## protocol buffer ###########################
    print ("========= Protocol Buffer ===========")
    print ("Deserialize Protocol Buffer to GeoJSON")
    tic = time.perf_counter()
    # Reading data from serialized_file
    serialized_file= open('./binary-output/{}.pbf'.format(file_name), "rb")
    addresses_read = address_pb2.Address()
    addresses_read.ParseFromString(serialized_file.read())

    geoJSON_Features_read = []

    for i in addresses_read.address:
        response_properties = {}
        response_properties["addr:housenumber"] = i.addrHousenumber
        response_properties["addr:street"] = i.addrStreet
        response_properties["addr:city"] = i.addrCity
        response_properties["source"] = i.source
        response_properties["addr:unit"] = i.addrUnit
        response_properties["fullAddress"] = i.fullAddress
        #response_properties["fid"] = i.fid
        response_properties_geometry = i.geometry #stored as WKT

        s = gpd.GeoSeries.from_wkt([response_properties_geometry])
        # Geopandas GeoSeries converts an array or list of WKT to a GeoSeries list.
        # There is only one element in the list so we index at 0.
        #print (">> {},{}".format(Decimal(s[0].x),Decimal(s[0].y)))
        # rounded to 6 decimal places by default (GeoJSON package documentation)
        ## precision 10 seems to be the maximum allowed.
        geocoord_read = Point((float(s[0].x),float(s[0].y)),precision=10)
        geoJSON_Features_read.append(Feature(geometry=geocoord_read, properties=response_properties))

    ## we need to add the information about the CRS to the geojson file.
    ## if the data is not WSG:84/EPSG:4326 then this needs to be specified.

    data_crs = {"type": "name","properties": {"name": "{}".format(data_CRS)}}

    ## create the FeatureCollection now.
    geoJSON_feature_collection = FeatureCollection(geoJSON_Features_read,crs=data_crs)

    with open('./geojson-output/{}-pbf.geojson'.format(file_name), 'w') as f:
       dump(geoJSON_feature_collection, f)

    toc = time.perf_counter()
    pbf_geojson_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./geojson-output/{}-pbf.geojson'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))



    ######################## Apache Avro ###########################
    print ("========= Apache Avro ===========")
    print ("Deserialize Fast Avro to GeoJSON")

    tic = time.perf_counter()
    geoJSON_Features_read = []
    with open('./binary-output/{}_fast.avro'.format(file_name), "rb") as fastavro_fo:
        for t_address in reader(fastavro_fo):
            response_properties = {}
            response_properties["addr:housenumber"] = t_address["addrHousenumber"]
            response_properties["addr:street"] = t_address["addrStreet"]
            response_properties["addr:city"] = t_address["addrCity"]
            response_properties["source"] = t_address["source"]
            response_properties["addr:unit"] = t_address["addrUnit"]
            response_properties["fullAddress"] = t_address["fullAddress"]
            #response_properties["fid"] = t_address["fid"]
            response_properties_geometry = t_address["geometry"] #stored as WKT
            s = gpd.GeoSeries.from_wkt([response_properties_geometry])
            # Geopandas GeoSeries converts an array or list of WKT to a GeoSeries list.
            # There is only one element in the list so we index at 0.
            #print (">> {},{}".format(Decimal(s[0].x),Decimal(s[0].y)))
            # rounded to 6 decimal places by default (GeoJSON package documentation)
            ## precision 10 seems to be the maximum allowed.
            geocoord_read = Point((float(s[0].x),float(s[0].y)),precision=10)
            geoJSON_Features_read.append(Feature(geometry=geocoord_read, properties=response_properties))

    ## we need to add the information about the CRS to the geojson file.
    ## if the data is not WSG:84/EPSG:4326 then this needs to be specified.

    data_crs = {"type": "name","properties": {"name": "{}".format(data_CRS)}}

    ## create the FeatureCollection now.
    geoJSON_feature_collection = FeatureCollection(geoJSON_Features_read,crs=data_crs)

    with open('./geojson-output/{}-avro_fast.geojson'.format(file_name), 'w') as f:
       dump(geoJSON_feature_collection, f)


    toc = time.perf_counter()
    avro_geojson_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./geojson-output/{}-avro_fast.geojson'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))

print ("\n\n\n==== Statistical Report ====")
pbf_geojson_timing_np = np.array(pbf_geojson_timing)
geojson_pbf_timing_np = np.array(geojson_pbf_timing)
geojson_avro_timing_np = np.array(geojson_avro_timing)
avro_geojson_timing_np = np.array(avro_geojson_timing)
geopkg_geojson_timing_np  = np.array(geopkg_geojson_timing)
load_geojson_timing_np  = np.array(load_geojson_timing)

print ("=====File Sizes=====")

file_size = os.path.getsize(INPUT_GPKG_FILE)
print("Input GPKG file size is {} Kb".format(round(file_size/1024),2))

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
print("Convert GPKG -> GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geopkg_geojson_timing_np, dtype=np.float64),np.std(geopkg_geojson_timing_np, dtype=np.float64)))

print("Load GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(load_geojson_timing_np, dtype=np.float64),np.std(load_geojson_timing_np, dtype=np.float64)))

print("Serialize: GeoJSON->Avro mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geojson_avro_timing_np, dtype=np.float64),np.std(geojson_avro_timing_np, dtype=np.float64)))
print("Serialize: GeoJSON->PBF mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geojson_pbf_timing_np, dtype=np.float64),np.std(geojson_pbf_timing_np, dtype=np.float64)))
print("Deserialize: Avro->GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(avro_geojson_timing_np, dtype=np.float64),np.std(avro_geojson_timing_np, dtype=np.float64)))
print("Deserialize: PBF->GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(pbf_geojson_timing_np,dtype=np.float64),np.std(pbf_geojson_timing_np,dtype=np.float64)))
