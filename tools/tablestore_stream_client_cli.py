#!/bin/usr/env python
#-*- coding: utf-8 -*-

from optparse import OptionParser
import sys
import json
import tabulate
import zlib
import time
from ots2 import *

class ConsoleConfig:
    def __init__(self, config_file):
        f = open(config_file, 'r')
        config = json.loads(f.read())
        self.endpoint = str(config['endpoint'])
        self.accessid = str(config['accessId'])
        self.accesskey = str(config['accessKey'])
        self.instance_name = str(config['instanceName'])
        self.status_table = str(config['statusTable'])

        self.ots = OTSClient(self.endpoint, self.accessid, self.accesskey, self.instance_name)

def _list_checkpoints(config, stream_id, timestamp):
    start_pk = [('StreamId', stream_id), ('StatusType', 'CheckpointForDataxReader'), ('StatusValue', '%16d' % timestamp)]
    end_pk = [('StreamId', stream_id), ('StatusType', 'CheckpointForDataxReader'), ('StatusValue', '%16d' % (timestamp + 1))]

    consumed_counter = CapacityUnit(0, 0)
    columns_to_get = []
    checkpoints = []
    range_iter = config.ots.xget_range(
                config.status_table, Direction.FORWARD,
                start_pk, end_pk,
                consumed_counter, columns_to_get, 100,
                column_filter=None, max_version=1
    )

    rows = []
    for (primary_key, attrs) in range_iter:
        checkpoint = {}
        for attr in attrs:
            checkpoint[attr[0]] = attr[1]

        if not checkpoint.has_key('SendRecordCount'):
            checkpoint['SendRecordCount'] = 0
        checkpoint['ShardId'] = primary_key[2][1].split('\t')[1]
        checkpoints.append(checkpoint)

    return checkpoints

def list_stream(config, options):
    consumed_counter = CapacityUnit(0, 0)

    start_pk = [('StreamId', INF_MIN), ('StatusType', INF_MIN), ('StatusValue', INF_MIN)]
    end_pk = [('StreamId', INF_MAX), ('StatusType', INF_MAX), ('StatusValue', INF_MAX)]

    columns_to_get = []
    range_iter = config.ots.xget_range(
                config.status_table, Direction.FORWARD,
                start_pk, end_pk,
                consumed_counter, columns_to_get, 100,
                column_filter=None, max_version=1
    )

    stream_ids = set()

    for (primary_key, attrs) in range_iter:
        stream_ids.add(primary_key[0][1])

    rows = [[stream_id] for stream_id in stream_ids]

    headers = ['StreamId']
    print tabulate.tabulate(rows, headers=headers)

def list_leases(config, options):
    consumed_counter = CapacityUnit(0, 0)

    if not options.stream_id:
         print "Error: Should set the stream id using '-s' or '--streamid'."
         sys.exit(-1)

    start_pk = [('StreamId', options.stream_id), ('StatusType', 'LeaseKey'), ('StatusValue', INF_MIN)]
    end_pk = [('StreamId', options.stream_id), ('StatusType', 'LeaseKey'), ('StatusValue', INF_MAX)]

    columns_to_get = []
    range_iter = config.ots.xget_range(
                config.status_table, Direction.FORWARD,
                start_pk, end_pk,
                consumed_counter, columns_to_get, 100,
                column_filter=None, max_version=1
    )

    rows = []
    lease_details = []
    for (primary_key, attrs) in range_iter:
        lease_detail = parse_lease_detail(attrs)
        lease_details.append(lease_detail)
        rows.append([primary_key[2][1], lease_detail['LeaseOwner'], lease_detail['LastAck'], lease_detail['LeaseCounter'], lease_detail['LeaseStealer'], lease_detail['Checkpoint']])

    headers = ['ShardId', 'LeaseOwner', 'LastAck', 'LeaseCounter', 'LeaseStealer', 'Checkpoint']
    print tabulate.tabulate(rows, headers=headers)

    expire_lease_count = 0
    alive_lease_count = 0
    alive_workers = set()
    now = time.time()

    for lease_detail in lease_details:
        if now - lease_detail['LastAckInInt'] > 30:
            expire_lease_count += 1
        else:
            alive_lease_count += 1
        alive_workers.add(lease_detail['LeaseOwner'])

    print '-------------- Summary --------------'
    print 'AliveLeaseCount:', alive_lease_count
    print 'ExpireLeaseCount:', expire_lease_count
    print 'AliveWorkers:', len(alive_workers)

def parse_lease_detail(attrs):
    lease_details = {}
    for attr in attrs:
        lease_details[attr[0]] = attr[1]
        if attr[0] == 'LeaseCounter':
            lease_details['LastAck'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(attr[2] / 1000))
            lease_details['LastAckInInt'] = attr[2] / 1000

    return lease_details

def parse_time(value):
    try:
        return int(value)
    except Exception,e:
        return int(time.mktime(time.strptime(value, '%Y-%m-%d %H:%M:%S')))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-c', '--config', dest='config_file', help='path of config file', metavar='tablestore_streamreader_config.json')
    parser.add_option('-a', '--action', dest='action', help='the action to do', choices = ['list_stream', 'list_leases'], metavar='')
    parser.add_option('-t', '--timestamp', dest='timestamp', help='the timestamp', metavar='')
    parser.add_option('-s', '--streamid', dest='stream_id', help='the id of stream', metavar='')
    parser.add_option('-d', '--shardid', dest='shard_id', help='the id of shard', metavar='')

    options, args = parser.parse_args()

    if not options.config_file:
        print "Error: Should set the path of config file using '-c' or '--config'."
        sys.exit(-1)

    if not options.action:
        print "Error: Should set the action using '-a' or '--action'."
        sys.exit(-1)

    console_config = ConsoleConfig(options.config_file)
    if options.action == 'list_stream':
        list_stream(console_config, options)
    elif options.action == 'list_leases':
        list_leases(console_config, options)

