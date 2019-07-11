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
def insert_rows(clkhs_list):
    print('Adding messages')
    UDR_clkhs_df = pd.DataFrame(clkhs_list, columns = ['partitionhash', 'hashcode', 'carrierid', 'subscriptionid', 'Message'])
    client.execute(insert_query, UDR_clkhs_df.to_dict('r'))

# Setup
UDR_data_df = pd.read_csv('backup.csv')
from clickhouse_driver import Client
for i in range(0, 10):
    clk_settings = {'max_threads': 8, 'max_block_size': 5000}
    client = Client(host='192.168.5.60', port='', settings=clk_settings, connect_timeout=60, send_receive_timeout=900, sync_request_timeout=120)
    create_table_query = ("CREATE TABLE radius.udr_other ( " +
        "CreateDate DateTime default now(), " +
        "partitionhash UInt64," +
        "hashcode String," +
        "carrierid UInt64," +
        "subscriptionid UInt64," +
        "Message String " +
        ") ENGINE = MergeTree() PARTITION BY toYYYYMM(CreateDate) order by partitionhash")
    insert_query = "INSERT INTO radius.udr_other(partitionhash, hashcode, carrierid, subscriptionid, Message) Format VALUES "

    # Create table
    client.execute(create_table_query)
    UDR_clkhs_list = []
    index = 0
    for row in UDR_data_df.itertuples(name=None):
        partitionhash = int(row[1])
        hashcode = str(row[2])
        carrierid = int(row[10])
        subscriptionid = int(row[37])

        rowdict = {
            "accountnumber":str(row[3]),
            "airtimeclass":int(row[4]),
            "airtimeunits":float(row[5]),
            "allocationcompletedate":str(row[6]),
            "apn":str(row[7]),
            "callednumber":str(row[8]),
            "callingnumber":str(row[9]),
            "cellid":str(row[11]),
            "chargingid":str(row[12]),
            "costcenterid":int(row[13]),
            "downlinkvol":int(row[14]),
            "duration":float(row[15]),
            "exactusagedateonly":str(row[16]),
            "exactusagetime":str(row[17]),
            "fileid":int(row[18]),
            "iccid":str(row[19]),
            "imei":str(row[20]),
            "imsi":str(row[21]),
            "lineid":int(row[22]),
            "linenumber":int(row[23]),
            "mobilecountrycode":str(row[24]),
            "mobilenetworkcode":str(row[25]),
            "mobileoriginated":bool(row[26]),
            "msisdn":str(row[27]),
            "network":str(row[28]),
            "orgid":int(row[29]),
            "orgurn":str(row[30]),
            "plmn":str(row[31]),
            "propertybag":str(row[32]),
            "recordtype":str(row[33]),
            "roamingindicator":str(row[34]),
            "roundingdate":str(row[35]),
            "sender":str(row[36]),
            "subscriptionurn":str(row[38]),
            "surrecordtypeid":int(row[39]),
            "tapcode":str(row[40]),
            "uplinkvol":int(row[41]),
            "usagetypeid":int(row[42])
        }
        row_json = json.dumps(rowdict).replace(' ', '').replace('":NaN,"', '":null,"').replace('":"nan","', '":null,"')
        UDR_clkhs_list.append([partitionhash, hashcode, carrierid, subscriptionid, row_json])
        index = index + 1

        if (index % 100000 == 0):
            index = 0
            insert_rows(UDR_clkhs_list)
            UDR_clkhs_list = []

    insert_rows(UDR_clkhs_list)




    # Clickhouse specific code
    print('Beginning Clickhouse Test')
    clk_settings = {'max_threads': 8, 'max_block_size': 5000}
    client = Client(host='192.168.5.60', port='', settings=clk_settings, connect_timeout=60, send_receive_timeout=900, sync_request_timeout=120)
    create_table_query = ("CREATE TABLE radius.udr ( " +
        "CreateDate DateTime default now(), " +
        "Message String " +
        ") ENGINE = MergeTree() PARTITION BY toYYYYMM(CreateDate) order by tuple()")
    insert_query = "INSERT INTO radius.udr(Message) Format VALUES "

    # Create table
    client.execute(create_table_query)

    UDR_clkhs_list = []
    index = 0
    for row in UDR_data_df.itertuples(name=None):
        rowdict = {
            "partitionhash":int(row[1]),
            "hashcode":str(row[2]),
            "accountnumber":str(row[3]),
            "airtimeclass":int(row[4]),
            "airtimeunits":float(row[5]),
            "allocationcompletedate":str(row[6]),
            "apn":str(row[7]),
            "callednumber":str(row[8]),
            "callingnumber":str(row[9]),
            "carrierid":int(row[10]),
            "cellid":str(row[11]),
            "chargingid":str(row[12]),
            "costcenterid":int(row[13]),
            "downlinkvol":int(row[14]),
            "duration":float(row[15]),
            "exactusagedateonly":str(row[16]),
            "exactusagetime":str(row[17]),
            "fileid":int(row[18]),
            "iccid":str(row[19]),
            "imei":str(row[20]),
            "imsi":str(row[21]),
            "lineid":int(row[22]),
            "linenumber":int(row[23]),
            "mobilecountrycode":str(row[24]),
            "mobilenetworkcode":str(row[25]),
            "mobileoriginated":bool(row[26]),
            "msisdn":str(row[27]),
            "network":str(row[28]),
            "orgid":int(row[29]),
            "orgurn":str(row[30]),
            "plmn":str(row[31]),
            "propertybag":str(row[32]),
            "recordtype":str(row[33]),
            "roamingindicator":str(row[34]),
            "roundingdate":str(row[35]),
            "sender":str(row[36]),
            "subscriptionid":int(row[37]),
            "subscriptionurn":str(row[38]),
            "surrecordtypeid":int(row[39]),
            "tapcode":str(row[40]),
            "uplinkvol":int(row[41]),
            "usagetypeid":int(row[42])
        }
        row_json = json.dumps(rowdict).replace(' ', '').replace('":NaN,"', '":null,"').replace('":"nan","', '":null,"')
        UDR_clkhs_list.append(row_json)
        index = index + 1

        if (index % 100000 == 0):
            print(row_json)
            index = 0
            insert_rows(UDR_clkhs_list)
            UDR_clkhs_list = []

    insert_rows(UDR_clkhs_list)



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
# # NOTE: This driver does not support COPY. USE cqlsh -u devadmin -p Keys2TheK1ngd0m 172.30.100.206
# cass_select_query = 'COPY "udr_loadtest" TO \'backup.csv\''
# cass_insert_query = 'COPY "udr" FROM \'backup.csv\''

# # execution
# rows = session.execute(cass_select_query)
# session.set_keyspace('CassandraPractice')
# session.execute(cass_insert_query)

# UDR_cass_df = rows._current_rows
# print(UDR_cass_df)

