# Code shared between cassandra and clickhouse
from datetime import date, datetime
import json
import pandas as pd
import numpy as np
import csv 
from pandas.io.json import json_normalize
import time
import random

# Global vars
num_rows_list = [5000, 10000, 100000, 500000, 1000000]
num_cols = 42
num_repetitions = 11
output_file = "output.txt"
csv_filename = "output.csv"
file = open(output_file, "w")
csv_file = open(csv_filename, "w")
clkhs_instance = '192.168.5.60'
cass_instance = 'dev-cassandra.ksg.int'
clkhs_port = 9000
cass_port = 9042
clkhs_table_definition = ("CREATE TABLE radius.udr ( \n" +
    "   CreateDate DateTime default now(), \n" +
    "   Message String \n" +
    ") ENGINE = MergeTree() \n" + 
    "PARTITION BY toYYYYMM(CreateDate) \n" + 
    "ORDER BY tuple()")
# TODO: Update this definition if I add indicies
cass_table_definition = ("CREATE TABLE \"CassandraPractice\".udr_copy1 (\n" +
    "	partitionhash int,\n" +
    "	hashcode text,\n" +
    "	accountnumber text,\n" +
    "	airtimeclass int,\n" +
    "	airtimeunits double,\n" +
    "	allocationcompletedate text,\n" +
    "	apn text,\n" +
    "	callednumber text,\n" +
    "	callingnumber text,\n" +
    "	carrierid int,\n" +
    "	cellid text,\n" +
    "	chargingid text,\n" +
    "	costcenterid int,\n" +
    "	downlinkvol bigint,\n" +
    "	duration double,\n" +
    "	exactusagedateonly date,\n" +
    "	exactusagetime text,\n" +
    "	fileid int,\n" +
    "	iccid text,\n" +
    "	imei text,\n" +
    "	imsi text,\n" +
    "	lineid bigint,\n" +
    "	linenumber int,\n" +
    "	mobilecountrycode text,\n" +
    "	mobilenetworkcode text,\n" +
    "	mobileoriginated boolean,\n" +
    "	msisdn text,\n" +
    "	network text,\n" +
    "	orgid int,\n" +
    "	orgurn text,\n" +
    "	plmn text,\n" +
    "	propertybag MAP<text, text>,\n" +
    "	recordtype text,\n" +
    "	roamingindicator text,\n" +
    "	roundingdate text,\n" +
    "	sender text,\n" +
    "	subscriptionid int,\n" +
    "	subscriptionurn text,\n" +
    "	surrecordtypeid int,\n" +
    "	tapcode text,\n" +
    "	uplinkvol bigint,\n" +
    "	usagetypeid int,\n" +
    "	PRIMARY KEY (partitionhash, hashcode)\n" +
    ") WITH bloom_filter_fp_chance = 0.01\n" +
    "AND comment = ''\n" +
    "AND crc_check_chance = 1.0\n" +
    "AND dclocal_read_repair_chance = 0.1\n" +
    "AND default_time_to_live = 0\n" +
    "AND gc_grace_seconds = 864000\n" +
    "AND max_index_interval = 2048\n" +
    "AND memtable_flush_period_in_ms = 0\n" +
    "AND min_index_interval = 128\n" +
    "AND read_repair_chance = 0.0\n" +
    "AND speculative_retry = '99.0PERCENTILE'\n" +
    "AND caching = {\n" +
    "	'keys' : 'ALL',\n" +
    "	'rows_per_partition' : 'NONE'\n" +
    "}\n" +
    "AND compression = {\n" +
    "	'chunk_length_in_kb' : 64,\n" +
    "	'class' : 'LZ4Compressor',\n" +
    "	'enabled' : true\n" +
    "}\n" +
    "AND compaction = {\n" +
    "	'class' : 'SizeTieredCompactionStrategy',\n" +
    "	'max_threshold' : 32,\n" +
    "	'min_threshold' : 4\n" +
    "};")
query_descriptions = ["Buld Retrieval",
    "Bulk Retreival: partitionhash = -1",
    "Bulk Retreival: carrierid = 18000",
    "Bulk Retreival: fileid = 278",
    "Bulk Retreival: usagetypeid = 0"]

# Global functions
def do_logging(line):
    print(line)
    file.write(str(line) + "\n")

def print_system_information():
    do_logging('Systems Information:')
    do_logging('\nClickhouse Instance: ' + str(clkhs_instance) + ':' + str(clkhs_port))
    do_logging('\nCassandra Instance: ' + str(cass_instance) + ':' + str(cass_port))
    do_logging('\nClickhouse Table Definition:\n' + str(clkhs_table_definition))
    do_logging('\nCassandra Table Definition:\n' + str(cass_table_definition))

def format_output(label, query_descriptors, num_rows_list, query_times_matrix, parse_times_matrix, rows_returned_matrix):
    query =             ",Query Description,"
    rows_selected =     label + ",Rows Selected,"
    rows_returned =     ",Rows Returned,"
    select_time =       ",Select Time,"
    dataframe_time =    ",DataFrame Time,"
    total =             ",Total,"

    for i in range(0, len(query_descriptions)):
        
        # Loop vars
        descriptor = query_descriptions[i]
        select_time_list = query_times_matrix[i]
        dataframe_time_list = parse_times_matrix[i]
        rows_returned_list = rows_returned_matrix[i]

        # Line 1
        query = query + descriptor + ("," * (len(num_rows_list) + 1))

        # Line 2
        for num in num_rows_list:
            rows_selected = rows_selected + str(num) + ","
        rows_selected = rows_selected + ","

        # Line 3
        for num in rows_returned_list:
            rows_returned = rows_returned + str(num) + ","
        rows_returned = rows_returned + ","

        # Line 4
        for time in select_time_list:
            select_time = select_time + ("%.2f" % (time)) + ","
        select_time = select_time + ","

        # Line 5
        for time in dataframe_time_list:
            dataframe_time = dataframe_time + ("%.2f" % (time)) + ","
        dataframe_time = dataframe_time + ","

        # Line 6
        for time1, time2 in zip(select_time_list, dataframe_time_list):
            if (time1 + time2 < 0):
                total = total + "-1,"
            else:
                total = total + ("%.2f" % (time1 + time2)) + ","
        total = total + ","

    final = query + "\n" + rows_selected + "\n" + rows_returned + "\n" + select_time + "\n" + dataframe_time + "\n" + total + "\n"
    return final

# Global Execution
print_system_information()
do_logging('\n')




# Clickhouse specific code
do_logging('Beginning Clickhouse Test')
from clickhouse_driver import Client

# Variables/constants
clkhs_select_query_prefix = ("SELECT visitParamExtractInt(Message, 'partitionhash') AS partitionhash, \n" +
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
    "FROM radius.udr \n")
clkhs_select_query_midfixes = ["LIMIT ",
    "WHERE partitionhash = -1 \n" + "LIMIT ",
    "WHERE carrierid = 18000 \n" + "LIMIT ",
    "WHERE fileid = 278 \n" + "LIMIT ",
    "WHERE usagetypeid = 0 \n" + "LIMIT "]
clkhs_UDR_df = pd.DataFrame()

# Functions
def clkhs_start_artificial_load():
    i = 0

# clkhs setup
clk_settings = {'max_threads': 8, 'max_block_size': 5000}
client = Client(host=clkhs_instance, port='', settings=clk_settings, connect_timeout=60, send_receive_timeout=900, sync_request_timeout=120)

clkhs_query_timing_matrix = []
clkhs_parse_timing_matrix = []
clkhs_rows_returned_matrix = []

# Time the retrieval of various number of records
# Iterate over the queries
for clkhs_select_query_midfix in clkhs_select_query_midfixes:
    do_logging("\nCurrent Query: " + clkhs_select_query_prefix + clkhs_select_query_midfix)
    clkhs_avg_query_times = []
    clkhs_avg_parse_times = []
    clkhs_rows_returned_list = []

    # Iterate over different limits
    for num_rows in num_rows_list:
        total_query_time = 0.0
        total_parse_time = 0.0
        rows_returned = 0
        do_break = False

        # Iterate set number of times, for repetitions sake
        for i in range(0, num_repetitions):
            query_time = 0.0
            parse_time = 0.0
            settings = {'max_block_size': 5000}

            # Wait
            time.sleep(random.randint(50, 61))

            # Do the query and parsing
            clkhs_start_artificial_load()
            start_time = time.perf_counter()
            result = client.execute_iter(clkhs_select_query_prefix + clkhs_select_query_midfix + str(num_rows) + ';', settings)
            clkhs_UDR_list = []
            for x in result:
                clkhs_UDR_list.append(x)
            mid_time = time.perf_counter()
            clkhs_UDR_df = pd.DataFrame(clkhs_UDR_list)
            end_time = time.perf_counter()

            # Get times
            query_time = query_time + mid_time - start_time
            parse_time = parse_time + end_time - mid_time

            # Aggregate
            total_query_time = total_query_time + query_time
            total_parse_time = total_parse_time + parse_time
            rows_returned = int(clkhs_UDR_df.size / num_cols)

            do_logging('Iteration %d: %.2f' % (i, query_time + parse_time))

            # Break condition
            if (num_rows > int(clkhs_UDR_df.size / num_cols)):
                do_break = True

        # Aggregate and add to list
        avg_query_time = total_query_time / num_repetitions
        avg_parse_time = total_parse_time / num_repetitions
        clkhs_avg_query_times.append(avg_query_time)
        clkhs_avg_parse_times.append(avg_parse_time)
        clkhs_rows_returned_list.append(rows_returned)

        # Logging
        do_logging("\nResults for %d records averaged over %d repetitions:" % (num_rows, num_repetitions))
        do_logging("Actual rows fetched: %d" % (int(clkhs_UDR_df.size / num_cols)))
        do_logging("Time to execute select: %.2f seconds" % (avg_query_time))
        do_logging("Time to put into dataframe: %.2f seconds" % (avg_parse_time))
        do_logging("Total time: %.2f seconds" % (avg_parse_time + avg_query_time))

        if (do_break):
            for j in range(0, len(num_rows_list) - len(clkhs_avg_query_times)):
                clkhs_avg_query_times.append(-1.0)
                clkhs_avg_parse_times.append(-1.0)
                clkhs_rows_returned_list.append(-1)
            break
    
    # Add to lists
    clkhs_query_timing_matrix.append(clkhs_avg_query_times)
    clkhs_parse_timing_matrix.append(clkhs_avg_parse_times)
    clkhs_rows_returned_matrix.append(clkhs_rows_returned_list)
        
csv_file.write(format_output("CH", query_descriptions, num_rows_list, clkhs_query_timing_matrix, clkhs_parse_timing_matrix, clkhs_rows_returned_matrix))




# Cassandra specific code
do_logging('\n\nBeginning Cassandra Test')
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Variables/constants
cass_select_query_prefixes = ['SELECT * FROM "udr" LIMIT ', 
    'SELECT * FROM "udr" WHERE partitionhash = -1',
    'SELECT * FROM "udr" WHERE carrierid = 18000 LIMIT '
    'SELECT * FROM "udr" WHERE fileid = 278 LIMIT ', 
    'SELECT * FROM "udr" WHERE usagetypeid = 0 LIMIT ']
cass_UDR_df = pd.DataFrame()
cass_page_size = 5000

# Functions
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

def cass_start_artificial_load():
    pi = 0

# cassandra setup
authentication = PlainTextAuthProvider(username='devadmin', password='Keys2TheK1ngd0m')
cluster = Cluster([cass_instance], port=cass_port, auth_provider=authentication)

cass_query_timing_matrix = []
cass_parse_timing_matrix = []
cass_rows_returned_matrix = []

# Time the retrieval of various number of records for various queries
# Iterate over the queries
for cass_select_query_prefix in cass_select_query_prefixes:

    # Per-query setup
    do_logging("\nCurrent Query: " + cass_select_query_prefix)
    cass_avg_query_times = []
    cass_avg_parse_times = []
    cass_rows_returned_list = []

    try:

        # Iterate over different limits
        for num_rows in num_rows_list:
            total_query_time = 0.0
            total_parse_time = 0.0
            rows_returned = 0
            do_break = False
            session = cluster.connect('CassandraPractice')
            session.row_factory = pandas_factory
            session.default_fetch_size = cass_page_size

            # Iterate set number of times, for repetitions sake
            for i in range(0, num_repetitions):
                query_time = 0.0
                parse_time = 0.0

                # Wait
                time.sleep(random.randint(50, 61))

                # Do the initial query and parsing
                cass_start_artificial_load()
                start_time = time.perf_counter()
                rows = session.execute(cass_select_query_prefix + str(num_rows) + ' ALLOW FILTERING;', timeout=None)
                mid_time = time.perf_counter()
                cass_UDR_list = []
                cass_UDR_list.extend(rows._current_rows.values.tolist())
                end_time = time.perf_counter()

                # Get times
                query_time = query_time + mid_time - start_time
                parse_time = parse_time + end_time - mid_time

                # Do the query and parsing repeatedly
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

                # Aggregate
                total_query_time = total_query_time + query_time
                total_parse_time = total_parse_time + parse_time
                rows_returned = int(cass_UDR_df.size / num_cols)

                do_logging('Iteration %d: %.2f' % (i, query_time + parse_time))

                if (num_rows > int(cass_UDR_df.size / num_cols)):
                    do_break = True
            
            # Aggregate and add to list
            avg_query_time = total_query_time / num_repetitions
            avg_parse_time = total_parse_time / num_repetitions
            cass_avg_query_times.append(avg_query_time)
            cass_avg_parse_times.append(avg_parse_time)
            cass_rows_returned_list.append(rows_returned)

            # Logging
            do_logging("\nResults for %d records averaged over %d repetitions:" % (num_rows, num_repetitions))
            do_logging("Actual rows fetched: %d" % (int(cass_UDR_df.size / num_cols)))
            do_logging("Time to execute select: %.2f seconds" % (avg_query_time))
            do_logging("Time to put into dataframe: %.2f seconds" % (avg_parse_time))
            do_logging("Total time: %.2f seconds" % (avg_parse_time + avg_query_time))

            # Cleanup
            session.shutdown()
            if (do_break):
                for j in range(0, len(num_rows_list) - len(cass_avg_query_times)):
                    cass_avg_query_times.append(-1.0)
                    cass_avg_parse_times.append(-1.0)
                    cass_rows_returned_list.append(-1)
                break

        # Add to lists
        cass_query_timing_matrix.append(cass_avg_query_times)
        cass_parse_timing_matrix.append(cass_avg_parse_times)
        cass_rows_returned_matrix.append(cass_rows_returned_list)
            
    except cassandra.ReadTimeout:
        do_logging('Coordinator Node Timeout')
        cass_query_timing_matrix.append([-1.0] * num_repetitions)
        cass_parse_timing_matrix.append([-1.0] * num_repetitions)
        cass_rows_returned_matrix.append([-1] * num_repetitions)
    except cassandra.Timeout:
        do_logging('Timeout')
        cass_query_timing_matrix.append([-1.0] * num_repetitions)
        cass_parse_timing_matrix.append([-1.0] * num_repetitions)
        cass_rows_returned_matrix.append([-1] * num_repetitions)
    except cassandra.ReadFailure:
        do_logging('Invalid Query: At least one replica failed')
        cass_query_timing_matrix.append([-1.0] * num_repetitions)
        cass_parse_timing_matrix.append([-1.0] * num_repetitions)
        cass_rows_returned_matrix.append([-1] * num_repetitions)

format_output("C*", query_descriptions, num_rows_list, cass_query_timing_matrix, cass_parse_timing_matrix, cass_rows_returned_matrix)

# Cleanup
file.close()