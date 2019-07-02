#!/usr/bin/python3.6
import argparse
import json
import sys # used to exit
from kafka import KafkaConsumer
import pandas as pd
import numpy as np
import csv
from pandas.io.json import json_normalize
from clickhouse_driver import Client
from datetime import datetime
from subprocess import Popen, PIPE
import hashlib

clk_settings = {'max_threads':8,'max_block_size':200000}

class arg:
        pass

insert_clause = "INSERT INTO radius.radius_raw(Message) Format VALUES "
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def coerce_df_columns_to_numeric(df, column_list):
    df[column_list] = df[column_list].apply(pd.to_numeric, errors='coerce')

KAFKA_TOPIC = 'RadiusAcctLogs'
#KAFKA_BROKERS = 'k1d-kafka-02.ksg.int:9092'
KAFKA_BROKERS = 'k1-kclst-01.prod.kore.com:9092'
i = 0
row_counter = 1
parser = argparse.ArgumentParser()
#parser.add_argument('--topic', help='The topic to listen for ')
##parser.add_argument('--output_file', help='The name of the output file')
#args = parser.parse_args(namespace=arg)
#KAFKA_TOPIC = arg.topic
#output_file = open(arg.output_file,'w+')

consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKERS,
                         auto_offset_reset='earliest')
kafka_array = []
counter = 1
loop_counter = 1
write_to_db = 0
kafka_df = pd.DataFrame()
mod_factor = 10
m = hashlib.md5()
try:
        for message in consumer:
                kafka_array.append(message.value.decode('UTF-8'))
                jsonrow = message.value.decode('UTF-8')
                if write_to_db == 0:
                        print(jsonrow)
                if counter == 1:
                        kafka_df = pd.DataFrame({'Message' : jsonrow },index=np.arange(1))
                else:
                        kafka_df.append(pd.DataFrame({'Message' : jsonrow },index=np.arange(1)))
                if (counter % mod_factor) == 0:
                        print('Prnting Dataframe')
                        print(kafka_df)
                        if write_to_db == 0:
                                sys.exit()
                        if write_to_db == 1:
                                try:
                                        clk_haus_client = Client('localhost',settings=clk_settings,connect_timeout=60,send_receive_timeout=900,sync_request_timeout=120)
                                        clk_haus_client.execute(insert_clause,kafka_df.to_dict('r'))
                                        clk_haus_client.disconnect()
                                except Exception as e:
                                        print(str(e))
                                        pass
                        kafka_array = []
                        kafka_df = pd.DataFrame()
                        counter = 0
                counter = counter + 1
except Exception as e:
        print("Outer Exception")
        print(str(e))
        sys.exit()
        pass
except KeyboardInterrupt:
    sys.exit()
