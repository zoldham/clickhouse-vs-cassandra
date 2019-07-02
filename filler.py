# Code shared between cassandra and clickhouse
from datetime import date, datetime
import json
import pandas as pd
import numpy as np
import csv 
import sys
from pandas.io.json import json_normalize



# Clickhouse specific code
print('Beginning Clickhouse Test')

# Setup
from clickhouse_driver import Client
clk_settings = {'max_threads': 8, 'max_block_size': 5000}
client = Client(host='192.168.5.60', port='', settings=clk_settings, connect_timeout=60, send_receive_timeout=900, sync_request_timeout=120)
create_table_query = ("CREATE TABLE radius.udr ( " +
    "CreateDate DateTime default now(), " +
    "Message String " +
    ") ENGINE = MergeTree() PARTITION BY toYYYYMM(CreateDate) order by tuple()")
insert_query = "INSERT INTO radius.udr(Message) Format VALUES "

# Create table
print('start')
client.execute_with_progress('SHOW TABLES;')
client.execute(create_table_query)
print('created')
sys.exit()

def insert_rows(df):
    client.execute(insert_query, df.to_dict('r'))

UDR_data_df = pd.read_csv('backup.csv')
UDR_clkhs_df = pd.DataFrame()
index = 0
for row in UDR_data_df.itertuples(name=None):
    rowdict = {
        'partitionhash': row[1],
        'hashcode': row[2],
        'accountnumber': row[3],
        'airtimeclass': row[4],
        'airtimeunits': row[5],
        'allocationcompletedate': row[6],
        'apn': row[7],
        'callednumber': row[8],
        'callingnumber': row[9],
        'carrierid': row[10],
        'cellid': row[11],
        'chargingid': row[12],
        'costcenterid': row[13],
        'downlinkvol': row[14],
        'duration': row[15],
        'exactusagedateonly': row[16],
        'exactusagetime': row[17],
        'fileid': row[18],
        'iccid': row[19],
        'imei': row[20],
        'imsi': row[21],
        'lineid': row[22],
        'linenumber': row[23],
        'mobilecountrycode': row[24],
        'mobilenetworkcode': row[25],
        'mobileoriginated': row[26],
        'msisdn': row[27],
        'network': row[28],
        'orgid': row[29],
        'orgurn': row[30],
        'plmn': row[31],
        'propertybag': row[32],
        'recordtype': row[33],
        'roamingindicator': row[34],
        'roundingdate': row[35],
        'sender': row[36],
        'subscriptionid': row[37],
        'subscriptionurn': row[38],
        'surrecordtypeid': row[39],
        'tapcode': row[40],
        'uplinkvol': row[41],
        'usagetypeid': row[42]
    }
    row_json = json.dumps(rowdict)

    UDR_clkhs_df = UDR_clkhs_df.append(pd.DataFrame({'Message' : row_json}, index=np.arange(1)))
    index = index + 1

    if (index % 1000 == 0):
        index = 0
        insert_rows(UDR_clkhs_df)
        UDR_clkhs_df = pd.DataFrame()

insert_rows(UDR_clkhs_df)



# # Cassandra specific code
# print('Beginning Cassandra Test')
# from cassandra.cluster import Cluster
# from cassandra.auth import PlainTextAuthProvider

# def pandas_factory(colnames, rows):
#     return pd.DataFrame(rows, columns=colnames)

# # cassandra setup
# authentication = PlainTextAuthProvider(username='devadmin', password='Keys2TheK1ngd0m')
# cluster = Cluster(['dev-cassandra.ksg.int'], port=9042, auth_provider=authentication)
# session = cluster.connect('cmp_dev_ripple')
# session.row_factory = pandas_factory
# session.default_fetch_size = None

# # queries
# cass_select_query = 'COPY "udr_loadtest" TO \'backup.csv\''
# cass_insert_query = 'COPY "udr" FROM \'backup.csv\''

# # execution
# rows = session.execute(cass_select_query)
# session.set_keyspace('CassandraPractice')
# session.execute(cass_insert_query)

# UDR_cass_df = rows._current_rows
# print(UDR_cass_df)

