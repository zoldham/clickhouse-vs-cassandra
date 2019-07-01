# Code shared between cassandra and clickhouse
from datetime import date, datetime
import json
import pandas as pd
import numpy as np
import csv 
from pandas.io.json import json_normalize
import time

num_rows_list = [5000, 10000, 100000, 500000, 1000000]


# Clickhouse specific code
print('Beginning Clickhouse Test')
from clickhouse_driver import Client

client = Client(host='192.168.5.60', port=8123)

UDR_clkhs_df = pd.DataFrame()
UDR_clkhs_df = UDR_clkhs_df.append(pd.DataFrame({'Message': "asdf"}, index=np.arange(1)))

print(UDR_clkhs_df)



# Cassandra specific code
print('\n\nBeginning Cassandra Test')
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

# cassandra setup
authentication = PlainTextAuthProvider(username='devadmin', password='Keys2TheK1ngd0m')
cluster = Cluster(['dev-cassandra.ksg.int'], port=9042, auth_provider=authentication)
session = cluster.connect('CassandraPractice')
session.row_factory = pandas_factory
session.default_fetch_size = 5000

# Constants
cass_select_query_prefix = 'SELECT * FROM "udr" LIMIT '
UDR_cass_df = pd.DataFrame()

# Time the retrieval of various number of records
for i in num_rows_list:
    query_time = 0.0
    parse_time = 0.0

    start_time = time.perf_counter()
    rows = session.execute(cass_select_query_prefix + str(i) + ';', timeout=None)
    mid_time = time.perf_counter()
    UDR_cass_df = rows._current_rows
    end_time = time.perf_counter()

    query_time = query_time + mid_time - start_time
    parse_time = parse_time + end_time - mid_time

    while (rows.has_more_pages):
        start_time = time.perf_counter()
        rows.fetch_next_page()
        mid_time = time.perf_counter()
        UDR_cass_df = UDR_cass_df.append(rows._current_rows)
        end_time = time.perf_counter()

        query_time = query_time + mid_time - start_time
        parse_time = parse_time + end_time - mid_time

    print( )
    print("Results for " + str(i) + " records:")
    print("Time to execute select: " + str(query_time) + " seconds")
    print("Time to put into dataframe: " + str(parse_time) + " seconds")
    print("Total time: " + str(query_time + parse_time) + " seconds")

