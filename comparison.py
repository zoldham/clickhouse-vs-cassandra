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
    file.write(str(line) + "\n")




# Clickhouse specific code
do_logging('Beginning Clickhouse Test')
from clickhouse_driver import Client

# Variables/constants
clkhs_select_query_prefixes = [("SELECT visitParamExtractInt(Message, 'partitionhash') AS partitionhash, \n" +
    "visitParamExtractString(Message, 'hashcode') AS hashcode, \n" +
    "visitParamExtractString(Message, 'accountnumber') AS accountnumber, \n" +
    "visitParamExtractInt(Message, 'airtimeclass') AS airtimeclass, \n" +
    "visitParamExtractFloat(Message, 'airtimeunits') AS airtimeunits, \n" +
    "visitParamExtractString(Message, 'allocationcompletedate') AS allocationcompletedate, \n" +
    "visitParamExtractString(Message, 'apn') AS apn, \n" +
    "visitParamExtractString(Message, 'callednumber') AS callednumber, \n" +
    "visitParamExtractString(Message, 'callingnumber') AS callingnumber, \n" +
    "visitParamExtractInt(Message, 'carrierid') AS carrierid, \n" +
    "visitParamExtractString(Message, 'cellid') AS cellid, \n" +
    "visitParamExtractString(Message, 'chargingid') AS chargingid, \n" +
    "visitParamExtractInt(Message, 'costcenterid') AS costcenterid, \n" +
    "visitParamExtractInt(Message, 'downlinkvol') AS downlinkvol, \n" +
    "visitParamExtractFloat(Message, 'duration') AS duration, \n" +
    "visitParamExtractString(Message, 'exactusagedateonly') AS exactusagedateonly, \n" +
    "visitParamExtractString(Message, 'exactusagetime') AS exactusagetime, \n" +
    "visitParamExtractInt(Message, 'fileid') AS fileid, \n" +
    "visitParamExtractString(Message, 'iccid') AS iccid, \n" +
    "visitParamExtractString(Message, 'imei') AS imei, \n" +
    "visitParamExtractString(Message, 'imsi') AS imsi, \n" +
    "visitParamExtractInt(Message, 'lineid') AS lineid, \n" +
    "visitParamExtractInt(Message, 'linenumber') AS linenumber, \n" +
    "visitParamExtractString(Message, 'mobilecountrycode') AS mobilecountrycode, \n" +
    "visitParamExtractString(Message, 'mobilenetworkcode') AS mobilenetworkcode, \n" +
    "visitParamExtractBool(Message, 'mobileoriginated') AS mobileoriginated, \n" +
    "visitParamExtractString(Message, 'msisdn') AS msisdn, \n" +
    "visitParamExtractString(Message, 'network') AS network, \n" +
    "visitParamExtractInt(Message, 'orgid') AS orgid, \n" +
    "visitParamExtractString(Message, 'orgurn') AS orgurn, \n" +
    "visitParamExtractString(Message, 'plmn') AS plmn, \n" +
    "visitParamExtractRaw(Message, 'propertybag') AS propertybag, \n" +
    "visitParamExtractString(Message, 'recordtype') AS recordtype, \n" +
    "visitParamExtractString(Message, 'roamingindicator') AS roamingindicator, \n" +
    "visitParamExtractString(Message, 'roundingdate') AS roundingdate, \n" +
    "visitParamExtractString(Message, 'sender') AS sender, \n" +
    "visitParamExtractInt(Message, 'subscriptionid') AS subscriptionid, \n" +
    "visitParamExtractString(Message, 'subscriptionurn') AS subscriptionurn, \n" +
    "visitParamExtractInt(Message, 'surrecordtypeid') AS surrecordtypeid, \n" +
    "visitParamExtractString(Message, 'tapcode') AS tapcode, \n" +
    "visitParamExtractInt(Message, 'uplinkvol') AS uplinkvol, \n" +
    "visitParamExtractInt(Message, 'usagetypeid') AS usagetypeid \n" +
    "FROM radius.udr \n" + 
    "LIMIT "),
    ("SELECT visitParamExtractInt(Message, 'partitionhash') AS partitionhash, \n" +
    "visitParamExtractString(Message, 'hashcode') AS hashcode, \n" +
    "visitParamExtractString(Message, 'accountnumber') AS accountnumber, \n" +
    "visitParamExtractInt(Message, 'airtimeclass') AS airtimeclass, \n" +
    "visitParamExtractFloat(Message, 'airtimeunits') AS airtimeunits, \n" +
    "visitParamExtractString(Message, 'allocationcompletedate') AS allocationcompletedate, \n" +
    "visitParamExtractString(Message, 'apn') AS apn, \n" +
    "visitParamExtractString(Message, 'callednumber') AS callednumber, \n" +
    "visitParamExtractString(Message, 'callingnumber') AS callingnumber, \n" +
    "visitParamExtractInt(Message, 'carrierid') AS carrierid, \n" +
    "visitParamExtractString(Message, 'cellid') AS cellid, \n" +
    "visitParamExtractString(Message, 'chargingid') AS chargingid, \n" +
    "visitParamExtractInt(Message, 'costcenterid') AS costcenterid, \n" +
    "visitParamExtractInt(Message, 'downlinkvol') AS downlinkvol, \n" +
    "visitParamExtractFloat(Message, 'duration') AS duration, \n" +
    "visitParamExtractString(Message, 'exactusagedateonly') AS exactusagedateonly, \n" +
    "visitParamExtractString(Message, 'exactusagetime') AS exactusagetime, \n" +
    "visitParamExtractInt(Message, 'fileid') AS fileid, \n" +
    "visitParamExtractString(Message, 'iccid') AS iccid, \n" +
    "visitParamExtractString(Message, 'imei') AS imei, \n" +
    "visitParamExtractString(Message, 'imsi') AS imsi, \n" +
    "visitParamExtractInt(Message, 'lineid') AS lineid, \n" +
    "visitParamExtractInt(Message, 'linenumber') AS linenumber, \n" +
    "visitParamExtractString(Message, 'mobilecountrycode') AS mobilecountrycode, \n" +
    "visitParamExtractString(Message, 'mobilenetworkcode') AS mobilenetworkcode, \n" +
    "visitParamExtractBool(Message, 'mobileoriginated') AS mobileoriginated, \n" +
    "visitParamExtractString(Message, 'msisdn') AS msisdn, \n" +
    "visitParamExtractString(Message, 'network') AS network, \n" +
    "visitParamExtractInt(Message, 'orgid') AS orgid, \n" +
    "visitParamExtractString(Message, 'orgurn') AS orgurn, \n" +
    "visitParamExtractString(Message, 'plmn') AS plmn, \n" +
    "visitParamExtractRaw(Message, 'propertybag') AS propertybag, \n" +
    "visitParamExtractString(Message, 'recordtype') AS recordtype, \n" +
    "visitParamExtractString(Message, 'roamingindicator') AS roamingindicator, \n" +
    "visitParamExtractString(Message, 'roundingdate') AS roundingdate, \n" +
    "visitParamExtractString(Message, 'sender') AS sender, \n" +
    "visitParamExtractInt(Message, 'subscriptionid') AS subscriptionid, \n" +
    "visitParamExtractString(Message, 'subscriptionurn') AS subscriptionurn, \n" +
    "visitParamExtractInt(Message, 'surrecordtypeid') AS surrecordtypeid, \n" +
    "visitParamExtractString(Message, 'tapcode') AS tapcode, \n" +
    "visitParamExtractInt(Message, 'uplinkvol') AS uplinkvol, \n" +
    "visitParamExtractInt(Message, 'usagetypeid') AS usagetypeid \n" +
    "FROM radius.udr \n" + 
    "WHERE carrierid = 18000 \n" +
    "LIMIT "),
    ("SELECT visitParamExtractInt(Message, 'partitionhash') AS partitionhash, \n" +
    "visitParamExtractString(Message, 'hashcode') AS hashcode, \n" +
    "visitParamExtractString(Message, 'accountnumber') AS accountnumber, \n" +
    "visitParamExtractInt(Message, 'airtimeclass') AS airtimeclass, \n" +
    "visitParamExtractFloat(Message, 'airtimeunits') AS airtimeunits, \n" +
    "visitParamExtractString(Message, 'allocationcompletedate') AS allocationcompletedate, \n" +
    "visitParamExtractString(Message, 'apn') AS apn, \n" +
    "visitParamExtractString(Message, 'callednumber') AS callednumber, \n" +
    "visitParamExtractString(Message, 'callingnumber') AS callingnumber, \n" +
    "visitParamExtractInt(Message, 'carrierid') AS carrierid, \n" +
    "visitParamExtractString(Message, 'cellid') AS cellid, \n" +
    "visitParamExtractString(Message, 'chargingid') AS chargingid, \n" +
    "visitParamExtractInt(Message, 'costcenterid') AS costcenterid, \n" +
    "visitParamExtractInt(Message, 'downlinkvol') AS downlinkvol, \n" +
    "visitParamExtractFloat(Message, 'duration') AS duration, \n" +
    "visitParamExtractString(Message, 'exactusagedateonly') AS exactusagedateonly, \n" +
    "visitParamExtractString(Message, 'exactusagetime') AS exactusagetime, \n" +
    "visitParamExtractInt(Message, 'fileid') AS fileid, \n" +
    "visitParamExtractString(Message, 'iccid') AS iccid, \n" +
    "visitParamExtractString(Message, 'imei') AS imei, \n" +
    "visitParamExtractString(Message, 'imsi') AS imsi, \n" +
    "visitParamExtractInt(Message, 'lineid') AS lineid, \n" +
    "visitParamExtractInt(Message, 'linenumber') AS linenumber, \n" +
    "visitParamExtractString(Message, 'mobilecountrycode') AS mobilecountrycode, \n" +
    "visitParamExtractString(Message, 'mobilenetworkcode') AS mobilenetworkcode, \n" +
    "visitParamExtractBool(Message, 'mobileoriginated') AS mobileoriginated, \n" +
    "visitParamExtractString(Message, 'msisdn') AS msisdn, \n" +
    "visitParamExtractString(Message, 'network') AS network, \n" +
    "visitParamExtractInt(Message, 'orgid') AS orgid, \n" +
    "visitParamExtractString(Message, 'orgurn') AS orgurn, \n" +
    "visitParamExtractString(Message, 'plmn') AS plmn, \n" +
    "visitParamExtractRaw(Message, 'propertybag') AS propertybag, \n" +
    "visitParamExtractString(Message, 'recordtype') AS recordtype, \n" +
    "visitParamExtractString(Message, 'roamingindicator') AS roamingindicator, \n" +
    "visitParamExtractString(Message, 'roundingdate') AS roundingdate, \n" +
    "visitParamExtractString(Message, 'sender') AS sender, \n" +
    "visitParamExtractInt(Message, 'subscriptionid') AS subscriptionid, \n" +
    "visitParamExtractString(Message, 'subscriptionurn') AS subscriptionurn, \n" +
    "visitParamExtractInt(Message, 'surrecordtypeid') AS surrecordtypeid, \n" +
    "visitParamExtractString(Message, 'tapcode') AS tapcode, \n" +
    "visitParamExtractInt(Message, 'uplinkvol') AS uplinkvol, \n" +
    "visitParamExtractInt(Message, 'usagetypeid') AS usagetypeid \n" +
    "FROM radius.udr \n" + 
    "WHERE fileid = 278 \n" +
    "LIMIT "),
    ("SELECT visitParamExtractInt(Message, 'partitionhash') AS partitionhash, \n" +
    "visitParamExtractString(Message, 'hashcode') AS hashcode, \n" +
    "visitParamExtractString(Message, 'accountnumber') AS accountnumber, \n" +
    "visitParamExtractInt(Message, 'airtimeclass') AS airtimeclass, \n" +
    "visitParamExtractFloat(Message, 'airtimeunits') AS airtimeunits, \n" +
    "visitParamExtractString(Message, 'allocationcompletedate') AS allocationcompletedate, \n" +
    "visitParamExtractString(Message, 'apn') AS apn, \n" +
    "visitParamExtractString(Message, 'callednumber') AS callednumber, \n" +
    "visitParamExtractString(Message, 'callingnumber') AS callingnumber, \n" +
    "visitParamExtractInt(Message, 'carrierid') AS carrierid, \n" +
    "visitParamExtractString(Message, 'cellid') AS cellid, \n" +
    "visitParamExtractString(Message, 'chargingid') AS chargingid, \n" +
    "visitParamExtractInt(Message, 'costcenterid') AS costcenterid, \n" +
    "visitParamExtractInt(Message, 'downlinkvol') AS downlinkvol, \n" +
    "visitParamExtractFloat(Message, 'duration') AS duration, \n" +
    "visitParamExtractString(Message, 'exactusagedateonly') AS exactusagedateonly, \n" +
    "visitParamExtractString(Message, 'exactusagetime') AS exactusagetime, \n" +
    "visitParamExtractInt(Message, 'fileid') AS fileid, \n" +
    "visitParamExtractString(Message, 'iccid') AS iccid, \n" +
    "visitParamExtractString(Message, 'imei') AS imei, \n" +
    "visitParamExtractString(Message, 'imsi') AS imsi, \n" +
    "visitParamExtractInt(Message, 'lineid') AS lineid, \n" +
    "visitParamExtractInt(Message, 'linenumber') AS linenumber, \n" +
    "visitParamExtractString(Message, 'mobilecountrycode') AS mobilecountrycode, \n" +
    "visitParamExtractString(Message, 'mobilenetworkcode') AS mobilenetworkcode, \n" +
    "visitParamExtractBool(Message, 'mobileoriginated') AS mobileoriginated, \n" +
    "visitParamExtractString(Message, 'msisdn') AS msisdn, \n" +
    "visitParamExtractString(Message, 'network') AS network, \n" +
    "visitParamExtractInt(Message, 'orgid') AS orgid, \n" +
    "visitParamExtractString(Message, 'orgurn') AS orgurn, \n" +
    "visitParamExtractString(Message, 'plmn') AS plmn, \n" +
    "visitParamExtractRaw(Message, 'propertybag') AS propertybag, \n" +
    "visitParamExtractString(Message, 'recordtype') AS recordtype, \n" +
    "visitParamExtractString(Message, 'roamingindicator') AS roamingindicator, \n" +
    "visitParamExtractString(Message, 'roundingdate') AS roundingdate, \n" +
    "visitParamExtractString(Message, 'sender') AS sender, \n" +
    "visitParamExtractInt(Message, 'subscriptionid') AS subscriptionid, \n" +
    "visitParamExtractString(Message, 'subscriptionurn') AS subscriptionurn, \n" +
    "visitParamExtractInt(Message, 'surrecordtypeid') AS surrecordtypeid, \n" +
    "visitParamExtractString(Message, 'tapcode') AS tapcode, \n" +
    "visitParamExtractInt(Message, 'uplinkvol') AS uplinkvol, \n" +
    "visitParamExtractInt(Message, 'usagetypeid') AS usagetypeid \n" +
    "FROM radius.udr \n" + 
    "WHERE usagetypeid = 0 \n" + 
    "LIMIT ")]
clkhs_UDR_df = pd.DataFrame()

# clkhs setup
clk_settings = {'max_threads': 8, 'max_block_size': 5000}
client = Client(host='192.168.5.60', port='', settings=clk_settings, connect_timeout=60, send_receive_timeout=900, sync_request_timeout=120)

# Time the retrieval of various number of records
for clkhs_select_query_prefix in clkhs_select_query_prefixes:
    do_logging("\nCurrent Query: " + clkhs_select_query_prefix)

    for i in num_rows_list:
        query_time = 0.0
        parse_time = 0.0
        settings = {'max_block_size': 5000}

        start_time = time.perf_counter()
        result = client.execute_iter(clkhs_select_query_prefix + str(i) + ';', settings)
        clkhs_UDR_list = []
        for x in result:
            clkhs_UDR_list.append(x)
        mid_time = time.perf_counter()
        clkhs_UDR_df = pd.DataFrame(clkhs_UDR_list)
        end_time = time.perf_counter()

        query_time = query_time + mid_time - start_time
        parse_time = parse_time + end_time - mid_time

        do_logging("\nResults for %d records:" % (i))
        do_logging("Actual rows fetched: %d" % (int(clkhs_UDR_df.size / num_cols)))
        do_logging("Time to execute select: %.2f seconds" % (query_time))
        do_logging("Time to put into dataframe: %.2f seconds" % (parse_time))
        do_logging("Total time: %.2f seconds" % (parse_time + query_time))

        if (i > int(clkhs_UDR_df.size / num_cols)):
            break




# Cassandra specific code
do_logging('\n\nBeginning Cassandra Test')
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Variables/constants
cass_select_query_prefixes = ['SELECT * FROM "udr" LIMIT ', 
    'SELECT * FROM "udr" WHERE carrierid = 18000 LIMIT ', 
    'SELECT * FROM "udr" WHERE fileid = 278 LIMIT ', 
    'SELECT * FROM "udr" WHERE usagetypeid = 0 LIMIT ']
cass_UDR_df = pd.DataFrame()
cass_page_size = 5000
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

# cassandra setup
authentication = PlainTextAuthProvider(username='devadmin', password='Keys2TheK1ngd0m')
cluster = Cluster(['dev-cassandra.ksg.int'], port=9042, auth_provider=authentication)

# Time the retrieval of various number of records for various queries
for cass_select_query_prefix in cass_select_query_prefixes:

    # Per-query setup
    do_logging("\nCurrent Query: " + cass_select_query_prefix)
    session = cluster.connect('CassandraPractice')
    session.row_factory = pandas_factory
    session.default_fetch_size = cass_page_size

    try:
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

            do_logging("\nResults for %d records:" % (i))
            do_logging("Actual rows fetched: %d" % (int(cass_UDR_df.size / num_cols)))
            do_logging("Time to execute select: %.2f seconds" % (query_time))
            do_logging("Time to put into dataframe: %.2f seconds" % (parse_time))
            do_logging("Total time: %.2f seconds" % (parse_time + query_time))

            if (i > int(cass_UDR_df.size / num_cols)):
                break
            
    except cassandra.ReadTimeout:
        do_logging('Coordinator Node Timeout')
    except cassandra.Timeout:
        do_logging('Timeout')
    except cassandra.ReadFailure:
        do_logging('Invalid Query: At least one replica failed')

    session.shutdown()



# Cleanup
file.close()