#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This file contains various utility functions that are useful
from within IPython notebooks trying to analyze YCSB results.

This should be included using '%run utils.py'
"""
import glob
import json
import matplotlib
from matplotlib import pyplot as plt
import numpy
import os
import sys
import time
from parse_ycsb_log import YcsbLogParser

# Add the scripts directory to the patch so we can use the parse_metrics_log script.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../scripts"))
from parse_metrics_log import MetricsLogParser

def parse_ycsb_start_ts(path):
    for line in open(path):
        if "0 sec: " in line:
            ts = line[0:len("2016-04-14 16:16:47")]
            return time.mktime(time.strptime(ts, "%Y-%m-%d %H:%M:%S"))
    raise "Could not find start time in " + path


def load_experiments(paths):
    data = {}
    common_prefix = os.path.commonprefix(paths)
    for path in paths:
        key = os.path.relpath(path, common_prefix)
        data[key] = load_experiment(path)
    return data

def load_experiment(experiment):
    exp = {}
    # YCSB load data.
    ycsb_load_log  = "%s/ycsb-load.log" % experiment
    exp['name'] = experiment
    exp['ycsb_load_start_ts'] = parse_ycsb_start_ts(ycsb_load_log)
    exp['ycsb_load_ts'] = YcsbLogParser(ycsb_load_log).as_numpy_array()
    summary = json.load(open("%s/ycsb-load.json" % experiment))
    exp['ycsb_load_stats'] = dict(((p['metric'], p['measurement']), p['value']) for p in summary)
    # YCSB run data.
    ycsb_run_log  = "%s/ycsb-run.log" % experiment
    exp['ycsb_run_start_ts'] = parse_ycsb_start_ts(ycsb_run_log)
    exp['ycsb_run_ts'] = YcsbLogParser(ycsb_run_log).as_numpy_array()
    summary = json.load(open("%s/ycsb-run.json" % experiment))
    exp['ycsb_run_stats'] = dict(((p['metric'], p['measurement']), p['value']) for p in summary)
    # TServer metric data.
    parser = MetricsLogParser(
        glob.glob("%s/logs/*tserver*diagnostics*" % experiment),
        simple_metrics=[
            ("server.generic_current_allocated_bytes", "heap_allocated")],
        rate_metrics=[
            ("tablet.leader_memory_pressure_rejections", "mem_rejections"),
            ("server.block_manager_total_bytes_written", "bytes_written")
        ],
        histogram_metrics=[
            ("tablet.bloom_lookups_per_op", "bloom_lookups")])
    types = [(colname, float) for colname in parser.column_names()]
    exp['ts_metrics'] = numpy.array(
        list(parser),
        dtype=types)
    return exp

def plot_ycsb_stats(experiments, metric, measurement, phase='run'):
    labels = experiments.keys()
    values = list()
    for key in experiments.keys():
        experiment = experiments[key]
        stats = experiment['ycsb_%s_stats' % phase]
        stat = stats.get((metric, measurement), 0)
        values.append(stat)

    y_pos = numpy.arange(len(labels))

    plt.bar(y_pos, values, align='center', alpha=0.5)
    plt.xticks(y_pos, labels, rotation=90)
    plt.ylabel(measurement)
    plt.show()

def print_ycsb_stats(experiments, metric, measurement, phase='run'):
    print("%s %s:" % (metric, measurement))
    for key in experiments.keys():
        experiment = experiments[key]
        print_ycsb_stat(experiment, metric, measurement, phase, "%s - " % key)
    print()

# Metrics include:
#   OVERALL, READ, INSERT
# Measurements include:
#   Throughput(ops/sec)
# Phases include:
#   load, run
def print_ycsb_stat(experiment, metric, measurement, phase='run', prefix=""):
    stats = experiment['ycsb_%s_stats' % phase]
    stat = stats.get((metric, measurement), 0)
    print("%s%s %s: %d" % (prefix, metric, measurement, stat))

# Operations include:
#   READ, INSERT,
# Metrics include:
#   {operation}_90, {operation}_99, {operation}_99.9, {operation}_99.99,
#   {operation}_Count, {operation}_Avg, {operation}_Max, {operation}_Min
#   OVERALL_Count
# Phases include:
#   load, run
# TODO: Add ylabel
def plot_ycsb_metric(experiment, metric, divisor=1, phase='run'):
    # Plot the YCSB time series
    data = experiment['ycsb_%s_ts' % phase]

    fig = plt.figure(figsize=(20, 5))
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(data['time'], data[metric]/divisor)
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel(metric)
    plt.show()

# See kudu_metrics_log.py for available metrics.
def plot_ts_metric(experiment, metric, ylabel, divisor=1, phase='run'):
    # Restrict the metrics log to the time range while the load was happening
    tsm = experiment['ts_metrics']
    min_time = experiment['ycsb_%s_start_ts' % phase]
    max_time = min_time + max(experiment['ycsb_%s_ts' % phase]['time'])
    # TODO: Fix time filtering.
    # tsm = tsm[tsm['time'] <= max_time]
    # tsm = tsm[tsm['time'] >= min_time]
    # tsm['time'] -= min_time

    # Plot various interesting tablet server memory usage
    fig = plt.figure(figsize=(20, 5))
    ax = fig.add_subplot(1, 1, 1)
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel(ylabel)
    ax.plot(tsm['time'], tsm[metric]/divisor)
    plt.show()

matplotlib.rcParams.update({'font.size': 18})