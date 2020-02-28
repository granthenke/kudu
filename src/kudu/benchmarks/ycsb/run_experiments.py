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

import argparse
import copy
import logging
import os
import shutil
import subprocess
import sys
import time
import yaml
try:
    import urllib.request as urllib  # For Python 3
except ImportError:
    import urllib  # For Python 2

try:
    xrange  # For Python 2
except NameError:
    xrange = range  # For Python 3

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


YCSB_EXPORTER_FLAGS= ["-p", "exporter=site.ycsb.measurements.exporter.JSONArrayMeasurementsExporter"]

class Experiment:
    def __init__(self, dimensions, config):
        self.dimensions = dimensions
        self.config = config
        self.ycsb_bin = os.path.abspath(self.config['ycsb_bin'])

    @property
    def results_dir(self):
        path = os.path.join(BASE_DIR, "results")
        for dim_name, dim_val in sorted(self.dimensions.items()):
            path = os.path.join(path, "%s=%s" % (dim_name, dim_val))
        return path

    @property
    def log_dir(self):
        return os.path.join(self.results_dir, "logs")

    def flags(self, config_key):
        ret = []
        for flag, value in self.config[config_key].items():
            if type(value) == list:
                value = ",".join(value)
            ret.append("--%s=%s" % (flag, value))
        return ret

    def workload_path(self, workload):
        return os.path.join(self.workload_dir, workload)

def start_servers(exp):
    bin_dir = os.path.join(exp.config['kudu_sbin_dir'])
    logging.info("Starting servers using kudu_sbin_dir: %s" % bin_dir)

    ts_bin = os.path.join(bin_dir, "kudu-tserver")
    master_bin = os.path.join(bin_dir, "kudu-master")
    if not os.path.exists(exp.log_dir):
        os.makedirs(exp.log_dir)
    try:
        ts_proc = subprocess.Popen(
            [ts_bin] +
            exp.flags("ts_flags") +
            ["--log_dir", exp.log_dir],
            stderr=subprocess.STDOUT,
            stdout=open(os.path.join(exp.log_dir, "ts.stderr"), "w"))
        master_proc = subprocess.Popen(
            [master_bin] +
            exp.flags("master_flags") +
            [ "--log_dir", exp.log_dir],
            stderr=subprocess.STDOUT,
            stdout=open(os.path.join(exp.log_dir, "master.stderr"), "w"))
    except OSError as e:
        logging.fatal("Failed to start kudu servers: %s", e)
        if '/' not in ts_bin:
            logging.fatal("Make sure they are on your $PATH, or configure kudu_sbin_dir")
        else:
            logging.fatal("Tried path: %s", ts_bin)
        sys.exit(1)
    # Wait for servers to start.
    for x in xrange(60):
        try:
            logging.info("Waiting for servers to come up...")
            urllib.urlopen("http://localhost:8050/").read()
            urllib.urlopen("http://localhost:8051/").read()
            break
        except:
            if x == 59:
                raise
            time.sleep(1)
            pass
    return (master_proc, ts_proc)

def stop_servers():
    subprocess.call(["pkill", "kudu-tserver"])
    subprocess.call(["pkill", "kudu-master"])


def remove_data():
    for d in DATA_DIRS:
        rmr(d)
        os.makedirs(d)

def rmr(dir):
    if os.path.exists(dir):
        logging.info("Removing data from %s" % dir)
        shutil.rmtree(dir)

def run_ycsb(exp, phase):
    sync_ops = False
    if phase == 'load':
        sync_ops = exp.config['ycsb_opts'].get('load_sync', 'false')

    logging.info("Running YCSB %s for config %s" % (phase, exp.dimensions))
    results_json = os.path.join(exp.results_dir, "ycsb-%s.json" % phase)
    ycsb_opts = exp.config['ycsb_opts']

    # If there is a recordsize option set, override the fieldlength to
    # generate that record size based on the fieldcount.
    if 'recordsize' in ycsb_opts:
        recordsize = ycsb_opts.get('recordsize')
        fieldcount = ycsb_opts.get('fieldcount', 10)
        if recordsize % fieldcount != 0:
            logging.warning("Records may be under sized. "
                            "'recordsize' does not evenly divide into 'fieldcount' for experiment %s" % exp.dimensions)
        fieldlength = int(recordsize / fieldcount)
        ycsb_opts['fieldlength'] = fieldlength

    argv = [exp.ycsb_bin, phase, "kudu",
            # Kudu & table configurations.
            "-p", "kudu_sync_ops=%s" % (sync_ops and "true" or "false"),
            "-p", "table=%s" % ycsb_opts.get('table', 'userable'),
            "-p", "kudu_table_num_replicas=%d" % ycsb_opts.get('replication_factor', 1),
            "-p", "kudu_pre_split_num_tablets=%d" % ycsb_opts.get('num_tablets', 4),
            # Load & record configurations.
            "-p", "insertorder=%s" % ycsb_opts.get('insertorder', 'ordered'),
            "-p", "recordcount=%d" % ycsb_opts.get('recordcount', 1000000),
            "-p", "fieldcount=%d" % ycsb_opts.get('fieldcount', 10),
            "-p", "fieldlength=%d" % ycsb_opts.get('fieldlength', 100),
            "-p", "fieldnameprefix=%s" % ycsb_opts.get('fieldnameprefix', 'field'),
            "-p", "operationcount=%d" % ycsb_opts.get('operationcount', 1000000),
            # Workload and operation configurations.
            "-p", "workload=%s" % ycsb_opts.get('workload', 'site.ycsb.workloads.CoreWorkload'),
            "-p", "readallfields=%s" % ycsb_opts.get('readallfields', 'true') , # TODO(ghenke): Add readfieldcount support to YCSB.
            "-p", "writeallfields=%s" % ycsb_opts.get('writeallfields', 'false'), # TODO(ghenke): Add writefieldcount support to YCSB.
            "-p", "readproportion=%d" % ycsb_opts.get('readproportion', 0.95),
            "-p", "updateproportion=%d" % ycsb_opts.get('updateproportion', 0.05),
            "-p", "insertproportion=%d" % ycsb_opts.get('insertproportion', 0.00),
            "-p", "scanproportion=%d" % ycsb_opts.get('scanproportion', 0.00),
            "-p", "requestdistribution=%s" % ycsb_opts.get('requestdistribution', 'uniform'),
            "-s",
            "-threads", str(ycsb_opts['threads'])] + \
           YCSB_EXPORTER_FLAGS + \
           ["-p", "exportfile=%s" % results_json]
    if phase != "load":
        argv += ["-p", "maxexecutiontime=%s" % ycsb_opts['max_execution_time']]

    stdout_log = os.path.join(exp.results_dir, "ycsb-%s.log" % phase)
    try:
        subprocess.check_call(argv,
                              stdout=open(stdout_log, "w"),
                              stderr=subprocess.STDOUT,
                              cwd=os.path.join(os.path.dirname(exp.ycsb_bin), ".."))
    except subprocess.CalledProcessError as e:
        logging.fatal("ycsb failed. Check log in %s", stdout_log)
        sys.exit(1)


def dump_ts_info(exp, suffix):
    for page, fname in [("rpcz", "rpcz"),
                        ("metrics?include_raw_histograms=1", "metrics"),
                        ("mem-trackers?raw", "mem-trackers")]:
        fname = "%s-%s.txt" % (fname, suffix)
        dst = open(os.path.join(exp.results_dir, fname), "w")
        try:
            shutil.copyfileobj(urllib.urlopen("http://localhost:8050/" + page), dst)
        except:
            logging.fatal("Failed to fetch tablet server info. TS may have crashed.")
            logging.fatal("Check for FATAL log files in %s", exp.log_dir)
            sys.exit(1)


def run_experiment(exp):
    if os.path.exists(exp.results_dir):
        logging.info("Skipping experiment %s (results dir already exists)" % exp.dimensions)
        return
    logging.info("Running experiment %s" % exp.dimensions)
    stop_servers()
    remove_data()
    # Sync file system so that there isn't any dirty data left over from prior runs
    # sitting in buffer caches.
    subprocess.check_call(["sync"])
    start_servers(exp)

    # Load the YCSB workload.
    run_ycsb(exp, "load")
    dump_ts_info(exp, "after-load")

    # Run the YCSB workload.
    run_ycsb(exp, "run")
    dump_ts_info(exp, "after-run")

    stop_servers()
    remove_data()

def generate_dimension_combinations(setup_yaml):
    combos = [{}]
    for dim_name, dim_values in setup_yaml['dimensions'].items():
        new_combos = []
        for c in combos:
            for dim_val in dim_values:
                new_combo = c.copy()
                new_combo[dim_name] = dim_val
                new_combos.append(new_combo)
        combos = new_combos
    return combos


def load_experiments(setup_yaml):
    combos = generate_dimension_combinations(setup_yaml)
    exps = []
    for c in combos:
        # 'c' is a dictionary like {"dim1": "dim_val1", "dim2": "dim_val2"}.
        # We need to convert it into the actual set of options and flags.
        setup = copy.deepcopy(setup_yaml['base_flags'])
        setup['dimensions'] = c
        for dim_name, dim_val in c.items():
            # Look up the options for the given dimension value.
            # e.g.: {"ts_flags": {"foo", "bar"}}
            dim_val_dict = setup_yaml['dimensions'][dim_name][dim_val]
            for k, v in dim_val_dict.items():
                setup[k].update(v)
        exps.append(Experiment(c, setup))
    return exps


def run_all(exps):
    for exp in exps:
        run_experiment(exp)


def main():
    p = argparse.ArgumentParser("Run a set of YCSB experiments")
    p.add_argument("--setup-yaml",
                   dest="setup_yaml_path",
                   type=str,
                   help="YAML file describing experiments to run",
                   default=os.path.join(BASE_DIR, "setup.yaml"))
    args = p.parse_args()
    setup_yaml = yaml.load(open(args.setup_yaml_path), Loader=yaml.SafeLoader)
    global DATA_DIRS
    DATA_DIRS = setup_yaml['all_data_dirs']
    exps = load_experiments(setup_yaml)
    run_all(exps)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()