# Code shared between cassandra and clickhouse
from datetime import date, datetime
import json
import pandas as pd
import numpy as np
import csv 
from pandas.io.json import json_normalize



# Clickhouse specific code
print('Beginning Clickhouse Test')
from clickhouse_driver import Client

client = Client(host='192.168.5.60', port=8123)

UDR_clkhs_df = pd.DataFrame()
UDR_clkhs_df = UDR_clkhs_df.append(pd.DataFrame({'Message': "asdf"}, index=np.arange(1)))

print(UDR_clkhs_df)



# Cassandra specific code
print('Beginning Cassandra Test')
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

# cassandra setup
authentication = PlainTextAuthProvider(username='devadmin', password='Keys2TheK1ngd0m')
cluster = Cluster(['dev-cassandra.ksg.int'], port=9042, auth_provider=authentication)
session = cluster.connect('cmp_dev_ripple')
session.row_factory = pandas_factory
session.default_fetch_size = None

# queries
cass_select_query = 'COPY "udr_loadtest" TO \'test.csv\''
cass_insert_query = 'COPY "udr" FROM \'test.csv\''

# execution
rows = session.execute(cass_select_query)
session.set_keyspace('CassandraPractice')
session.execute(cass_insert_query)

UDR_cass_df = rows._current_rows
print(UDR_cass_df)

