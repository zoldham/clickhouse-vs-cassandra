# Code shared between cassandra and clickhouse
from datetime import date, datetime
import json
import pandas as pd
import numpy as np
import csv 
from pandas.io.json import json_normalize
import time

# Global vars
num_rows_list = [5000, 10000, 100000, 500000, 1000000]
num_cols = 42
output_file = "output.txt"
file = open(output_file, "w")

# Global functions
def do_logging(line):
    print(line)
    file.write(line + "\n")



# Clickhouse specific code
do_logging('Beginning Clickhouse Test')
from clickhouse_driver import Client

# Variables/constants
clkhs_select_query_prefix = ("SELECT visitParamExtractInt(Message, 'partitionhash') AS partitionhash, " +
    "visitParamExtractString(Message, 'hashcode') AS hashcode, " +
    "visitParamExtractString(Message, 'accountnumber') AS accountnumber, " +
    "visitParamExtractInt(Message, 'airtimeclass') AS airtimeclass, " +
    "visitParamExtractFloat(Message, 'airtimeunits') AS airtimeunits, " +
    "visitParamExtractString(Message, 'allocationcompletedate') AS allocationcompletedate, " +
    "visitParamExtractString(Message, 'apn') AS apn, " +
    "visitParamExtractString(Message, 'callednumber') AS callednumber, " +
    "visitParamExtractString(Message, 'callingnumber') AS callingnumber, " +
    "visitParamExtractInt(Message, 'carrierid') AS carrierid, " +
    "visitParamExtractString(Message, 'cellid') AS cellid, " +
    "visitParamExtractString(Message, 'chargingid') AS chargingid, " +
    "visitParamExtractInt(Message, 'costcenterid') AS costcenterid, " +
    "visitParamExtractInt(Message, 'downlinkvol') AS downlinkvol, " +
    "visitParamExtractFloat(Message, 'duration') AS duration, " +
    "visitParamExtractString(Message, 'exactusagedateonly') AS exactusagedateonly, " +
    "visitParamExtractString(Message, 'exactusagetime') AS exactusagetime, " +
    "visitParamExtractInt(Message, 'fileid') AS fileid, " +
    "visitParamExtractString(Message, 'iccid') AS iccid, " +
    "visitParamExtractString(Message, 'imei') AS imei, " +
    "visitParamExtractString(Message, 'imsi') AS imsi, " +
    "visitParamExtractInt(Message, 'lineid') AS lineid, " +
    "visitParamExtractInt(Message, 'linenumber') AS linenumber, " +
    "visitParamExtractString(Message, 'mobilecountrycode') AS mobilecountrycode, " +
    "visitParamExtractString(Message, 'mobilenetworkcode') AS mobilenetworkcode, " +
    "visitParamExtractBool(Message, 'mobileoriginated') AS mobileoriginated, " +
    "visitParamExtractString(Message, 'msisdn') AS msisdn, " +
    "visitParamExtractString(Message, 'network') AS network, " +
    "visitParamExtractInt(Message, 'orgid') AS orgid, " +
    "visitParamExtractString(Message, 'orgurn') AS orgurn, " +
    "visitParamExtractString(Message, 'plmn') AS plmn, " +
    "visitParamExtractRaw(Message, 'propertybag') AS propertybag, " +
    "visitParamExtractString(Message, 'recordtype') AS recordtype, " +
    "visitParamExtractString(Message, 'roamingindicator') AS roamingindicator, " +
    "visitParamExtractString(Message, 'roundingdate') AS roundingdate, " +
    "visitParamExtractString(Message, 'sender') AS sender, " +
    "visitParamExtractInt(Message, 'subscriptionid') AS subscriptionid, " +
    "visitParamExtractString(Message, 'subscriptionurn') AS subscriptionurn, " +
    "visitParamExtractInt(Message, 'surrecordtypeid') AS surrecordtypeid, " +
    "visitParamExtractString(Message, 'tapcode') AS tapcode, " +
    "visitParamExtractInt(Message, 'uplinkvol') AS uplinkvol, " +
    "visitParamExtractInt(Message, 'usagetypeid') AS usagetypeid " +
    "FROM radius.urn " + 
    "LIMIT "
    )
clkhs_UDR_df = pd.DataFrame()

# clkhs setup
clk_settings = {'max_threads': 8, 'max_block_size': 5000}
client = Client(host='192.168.5.60', port='8123', settings=clk_settings, connect_timeout=60, send_receive_timeout=900, sync_request_timeout=120)

# Time the retrieval of various number of records
for i in num_rows_list:
    query_time = 0.0
    parse_time = 0.0
    settings = {'max_block_size': 5000}

    start_time = time.perf_counter()
    result = client.execute_iter(clkhs_select_query_prefix + str(i) + ';', settings)
    clkhs_UDR_list_temp = result.next()
    mid_time = time.perf_counter()
    clkhs_UDR_list = []
    clkhs_UDR_list.extend(clkhs_UDR_list_temp)
    end_time = time.perf_counter()

    query_time = query_time + mid_time - start_time
    parse_time = parse_time + end_time - mid_time

    while (len(clkhs_UDR_list_temp) > 0):
        start_time = time.perf_counter()
        clkhs_UDR_list_temp = result.next()
        mid_time = time.perf_counter()
        clkhs_UDR_list.extend(clkhs_UDR_list_temp)
        end_time = time.perf_counter()

        query_time = query_time + mid_time - start_time
        parse_time = parse_time + end_time - mid_time

    mid_time = time.perf_counter()
    clkhs_UDR_df = pd.DataFrame(clkhs_UDR_list)
    end_time = time.perf_counter()

    parse_time = parse_time + end_time - mid_time

    do_logging("")
    do_logging("Results for " + str(i) + " records:")
    do_logging("Actual rows fetched: " + str(clkhs_UDR_df.size / num_cols))
    do_logging("Time to execute select: " + str(query_time) + " seconds")
    do_logging("Time to put into dataframe: " + str(parse_time) + " seconds")
    do_logging("Total time: " + str(query_time + parse_time) + " seconds")



# Cassandra specific code
do_logging('\n\nBeginning Cassandra Test')
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Variables/constants
cass_select_query_prefix = 'SELECT * FROM "udr" LIMIT '
cass_UDR_df = pd.DataFrame()
cass_page_size = 5000
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

# cassandra setup
authentication = PlainTextAuthProvider(username='devadmin', password='Keys2TheK1ngd0m')
cluster = Cluster(['dev-cassandra.ksg.int'], port=9042, auth_provider=authentication)
session = cluster.connect('CassandraPractice')
session.row_factory = pandas_factory
session.default_fetch_size = cass_page_size

# Time the retrieval of various number of records
for i in num_rows_list:
    query_time = 0.0
    parse_time = 0.0

    start_time = time.perf_counter()
    rows = session.execute(cass_select_query_prefix + str(i) + ';', timeout=None)
    mid_time = time.perf_counter()
    cass_UDR_list = []
    cass_UDR_list.extend(rows._current_rows.values.tolist())
    end_time = time.perf_counter()

    query_time = query_time + mid_time - start_time
    parse_time = parse_time + end_time - mid_time

    while (rows.has_more_pages):
        start_time = time.perf_counter()
        rows.fetch_next_page()
        mid_time = time.perf_counter()
        cass_UDR_list.extend(rows._current_rows.values.tolist())
        end_time = time.perf_counter()

        query_time = query_time + mid_time - start_time
        parse_time = parse_time + end_time - mid_time

    mid_time = time.perf_counter()
    cass_UDR_df = pd.DataFrame(cass_UDR_list)
    end_time = time.perf_counter() 

    parse_time = parse_time + end_time - mid_time

    do_logging("")
    do_logging("Results for " + str(i) + " records:")
    do_logging("Actual rows fetched: " + str(cass_UDR_df.size / num_cols))
    do_logging("Time to execute select: " + str(query_time) + " seconds")
    do_logging("Time to put into dataframe: " + str(parse_time) + " seconds")
    do_logging("Total time: " + str(query_time + parse_time) + " seconds")



# Cleanup
file.close()