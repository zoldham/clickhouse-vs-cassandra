# Code shared between cassandra and clickhouse
from datetime import date, datetime
import json
import pandas as pd
import numpy as np
import csv 
from pandas.io.json import json_normalize
import time
import random
import threading

# Global vars
num_rows_list = [1, 5, 10, 100, 500, 1000, 5000, 10000, 100000, 500000, 1000000]
num_cols = 42
num_repetitions = 11
output_file = "output.txt"
csv_filename = "output.csv"
file = open(output_file, "w", buffering=1)
csv_file = open(csv_filename, "w", buffering=1)
clkhs_instance = '192.168.5.60'
cass_instance = 'dev-cassandra.ksg.int'
clkhs_port = 9000
cass_port = 9042
query_descriptions = ['WHERE provisioning_request_id = "a1e849ee-2d74-488a-92c1-3119778e21ac" AND pid = "8901263804713070001" LIMIT ',
    'WHERE provisioning_request_id = "d2c24b0a-1495-4cc1-9263-72a4aa56a6c8"']

# Global functions
def do_logging(line):
    log_line = (str(datetime.utcnow()) + ': ' + str(line)).replace('\n', '\n' + str(datetime.utcnow()) + ': ')

    print(log_line)
    file.write(log_line + '\n')

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





# Clickhouse specific code
do_logging('Beginning Clickhouse Test')
from clickhouse_driver import Client

# Variables/constants
clkhs_artificial_queries = ["select direction,forwarding_statusstatus,icmp_type,count(*) as connections,sum(in_pkts),avg(in_pkts),quantiles(.25,.5,.75)(in_pkts),median(in_pkts) from netflow.netflow_raw group by direction,forwarding_statusstatus,icmp_type order by connections desc limit 50",
    "select out_src_mac,tcp_flags,out_src_mac,count(*) as connections,sum(in_pkts),avg(in_pkts),quantiles(.25,.5,.75)(in_pkts),median(in_pkts) from netflow.netflow_raw group by out_src_mac,tcp_flags,out_src_mac order by connections desc limit 50",
    "select tcp_flags,icmp_type,count(*) as connections,sum(in_pkts),avg(in_pkts),quantiles(.25,.5,.75)(in_pkts),median(in_pkts) from netflow.netflow_raw group by tcp_flags,icmp_type order by connections desc limit 50",
    "select out_src_mac,count(*) as connections,sum(in_pkts),avg(in_pkts),quantiles(.25,.5,.75)(in_pkts),median(in_pkts) from netflow.netflow_raw group by out_src_mac order by connections desc limit 50",
    "select flowset_id,count(*) as connections,sum(in_pkts),avg(in_pkts),quantiles(.25,.5,.75)(in_pkts),median(in_pkts) from netflow.netflow_raw group by flowset_id order by connections desc limit 50",
    "select protocolname,ipv4_src_addr,count(*) as connections,sum(in_pkts),avg(in_pkts),quantiles(.25,.5,.75)(in_pkts),median(in_pkts) from netflow.netflow_raw group by protocolname,ipv4_src_addr order by connections desc limit 50",
    "select dst_mask,ipv4_next_hop,in_dst_mac,count(*) as connections,sum(in_pkts),avg(in_pkts),quantiles(.25,.5,.75)(in_pkts),median(in_pkts) from netflow.netflow_raw group by dst_mask,ipv4_next_hop,in_dst_mac order by connections desc limit 50",
    "select in_dst_mac,count(*) as connections,sum(in_pkts),avg(in_pkts),quantiles(.25,.5,.75)(in_pkts),median(in_pkts) from netflow.netflow_raw group by in_dst_mac order by connections desc limit 50",
    "select in_bytes,ipv4_next_hop,last_switched,count(*) as connections,sum(in_pkts),avg(in_pkts),quantiles(.25,.5,.75)(in_pkts),median(in_pkts) from netflow.netflow_raw group by in_bytes,ipv4_next_hop,last_switched order by connections desc limit 50",
    "select tcp_flags,count(*) as connections,sum(in_pkts),avg(in_pkts),quantiles(.25,.5,.75)(in_pkts),median(in_pkts) from netflow.netflow_raw group by tcp_flags order by connections desc limit 50"] # For use in the threads
clkhs_stop_threads = False

clkhs_select_query_prefix = ("SELECT visitParamExtractInt(Message, 'provisioning_request_id') AS provisioning_request_id, \n" +
    "visitParamExtractString(Message, 'pid') AS pid, \n" +
    "visitParamExtractString(Message, 'async_response') AS async_response, \n" +
    "visitParamExtractString(Message, 'completed_on') AS completed_on, \n" +
    "visitParamExtractString(Message, 'created_on') AS created_on, \n" +
    "visitParamExtractString(Message, 'finalized_response') AS finalized_response, \n" +
    "visitParamExtractString(Message, 'payload') AS payload, \n" +
    "visitParamExtractString(Message, 'request_details') AS request_details, \n" +
    "visitParamExtractString(Message, 'response_message') AS response_message, \n" +
    "visitParamExtractBool(Message, 'response_successful') AS response_successful, \n" +
    "visitParamExtractString(Message, 'sync_request') AS sync_request, \n" +
    "visitParamExtractString(Message, 'sync_response') AS sync_response \n" +
    "FROM radius.provisioning_request \n")

clkhs_select_query_midfixes = ['WHERE provisioning_request_id = "a1e849ee-2d74-488a-92c1-3119778e21ac" AND pid = "8901263804713070001" LIMIT ',
   'WHERE provisioning_request_id = "d2c24b0a-1495-4cc1-9263-72a4aa56a6c8" LIMIT ']

clkhs_UDR_df = pd.DataFrame()

# Functions
def clkhs_artificial_load():
    do_logging('Thread ' + str(threading.get_ident()) + ' starting')
    clk_settings = {'max_threads': 8, 'max_block_size': 5000}
    client = Client(host=clkhs_instance, port='', settings=clk_settings, connect_timeout=60, send_receive_timeout=900, sync_request_timeout=120)
    settings = {'max_block_size': 5000}
    while (not clkhs_stop_threads):
        try:
            # Select a query to issue
            query = clkhs_artificial_queries[random.randint(0, 9)]
            do_logging('Thread ' + str(threading.get_ident()) + ' starting background query: ' + query)

            # Issue query and iterate through result set
            result = client.execute_iter(query, settings)
            for x in result:
                if (clkhs_stop_threads):
                    return
        except Exception as e: 

            # This is generally a serverside memory exception
            do_logging('Thread ' + str(threading.get_ident()) + ' encountered an exception:')
            do_logging(e)
            time.sleep(5)
            do_logging('Continuing after exception')
            continue

# clkhs setup
clk_settings = {'max_threads': 8, 'max_block_size': 5000}
client = Client(host=clkhs_instance, port='', settings=clk_settings, connect_timeout=60, send_receive_timeout=900, sync_request_timeout=120)

clkhs_query_timing_matrix = []
clkhs_parse_timing_matrix = []
clkhs_rows_returned_matrix = []

# Start artifical load
threads = []
for i in range(0, 4):
    threads.append(threading.Thread(target=clkhs_artificial_load))
    threads[i].start()
    time.sleep(1)

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
        i = 0
        while (i < num_repetitions):
            try:
                query_time = 0.0
                parse_time = 0.0
                settings = {'max_block_size': 5000}

                # Wait
                time.sleep(random.randint(50, 61))

                # Do the query and parsing
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
                
                i = i + 1

            except Exception as e:  # In case of an error, try try again
                do_logging('Main thread encountered an exception:')
                do_logging(e)
                time.sleep(5)
                do_logging('Main thread continuing')
                continue

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
                clkhs_avg_query_times.append(0.0)
                clkhs_avg_parse_times.append(0.0)
                clkhs_rows_returned_list.append(0)
            break
    
    # Add to lists
    clkhs_query_timing_matrix.append(clkhs_avg_query_times)
    clkhs_parse_timing_matrix.append(clkhs_avg_parse_times)
    clkhs_rows_returned_matrix.append(clkhs_rows_returned_list)
        
# Write to file
csv_file.write(format_output("CH", query_descriptions, num_rows_list, clkhs_query_timing_matrix, clkhs_parse_timing_matrix, clkhs_rows_returned_matrix) + '\n')

# Clean up artifical load
clkhs_stop_threads = True
for thread in threads:
    thread.join()




# Cassandra specific code
do_logging('\n\nBeginning Cassandra Test')
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Variables/constants
cass_select_query_prefixes = ['SELECT * FROM "provisioning_request" WHERE provisioning_request_id = "a1e849ee-2d74-488a-92c1-3119778e21ac" AND pid = "8901263804713070001" LIMIT ',
    'SELECT * FROM "provisioning_request" WHERE provisioning_request_id = "d2c24b0a-1495-4cc1-9263-72a4aa56a6c8"']
cass_filtering_postfix = " ALLOW FILTERING"
cass_UDR_df = pd.DataFrame()
cass_page_size = 5000

# Functions
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

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

    # Iterate over different limits
    for num_rows in num_rows_list:
        total_query_time = 0.0
        total_parse_time = 0.0
        rows_returned = 0
        do_break = False
        timeout = False

        # Setup connection (or try to)
        while (True):
            try:
                session = cluster.connect('CassandraPractice')
                break
            except Exception as e:
                do_logging('Exception while connecting to cassandra: ')
                do_logging(e)
                time.sleep(5)
                do_logging('retrying connection:')
                continue
        session.row_factory = pandas_factory
        session.default_fetch_size = cass_page_size

        # Iterate set number of times, for repetitions sake
        i = 0
        while (i < num_repetitions):
            try:
                query_time = 0.0
                parse_time = 0.0

                # Wait
                time.sleep(random.randint(50, 61))

                # Do the initial query and parsing
                start_time = time.perf_counter()
                rows = session.execute(cass_select_query_prefix + str(num_rows) + cass_filtering_postfix + ';', timeout=None)
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

                i = i + 1

            except cassandra.ReadTimeout:   # Timeout: will skip this query
                do_logging('Coordinator Node Timeout')
                do_break = True
                timeout = True
                total_query_time = -1 * num_repetitions
                total_parse_time = -1 * num_repetitions
                rows_returned = -1
                break
            except cassandra.Timeout:   # Timeout: Will skip this query
                do_logging('Timeout')
                do_break = True
                timeout = True
                total_query_time = -1 * num_repetitions
                total_parse_time = -1 * num_repetitions
                rows_returned = -1
                break
            except cassandra.ReadFailure:   # Cassandra error: Skip this query
                do_logging('Error: At least one replica failed')
                do_break = True
                timeout = True
                total_query_time = -1 * num_repetitions
                total_parse_time = -1 * num_repetitions
                rows_returned = -1
                break
            except Exception as e:  # Error: Try again
                do_logging('Unexpected exception')
                time.sleep(5)
                continue
        
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
        if (do_break):  # Fill in the rest of the times/rows for consistent formatting
            if (timeout):
                for j in range(0, len(num_rows_list) - len(cass_avg_query_times)):
                    cass_avg_query_times.append(-1.0)
                    cass_avg_parse_times.append(-1.0)
                    cass_rows_returned_list.append(-1)
            else:
                for j in range(0, len(num_rows_list) - len(cass_avg_query_times)):
                    cass_avg_query_times.append(0.0)
                    cass_avg_parse_times.append(0.0)
                    cass_rows_returned_list.append(0)
            
            break

    # Add to lists
    cass_query_timing_matrix.append(cass_avg_query_times)
    cass_parse_timing_matrix.append(cass_avg_parse_times)
    cass_rows_returned_matrix.append(cass_rows_returned_list)

csv_file.write(format_output("C*", query_descriptions, num_rows_list, cass_query_timing_matrix, cass_parse_timing_matrix, cass_rows_returned_matrix))

# Cleanup
csv_file.close()
file.close()