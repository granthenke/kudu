#!/usr/bin/env python2
#
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
#
# This script runs on the distributed-test slave and acts
# as a wrapper around run-test.sh.
#
# The distributed testing system can't pass in environment variables
# to commands, so this takes some parameters, turns them into environment
# variables, and then executes the test wrapper.
#
# We also 'cat' the test log upon completion so that the test logs are
# uploaded by the test slave back.

import glob
import optparse
import os
import re
import shutil
import subprocess
import sys

ME = os.path.abspath(__file__)
ROOT = os.path.abspath(os.path.join(os.path.dirname(ME), ".."))

JAVA_CANDIDATES = """
      # Oracle JDK 8 Candidates
    /usr/java/jdk1.8
    /usr/java/jre1.8
    /usr/lib/jvm/j2sdk1.8-oracle
    /usr/lib/jvm/j2sdk1.8-oracle/jre
    /usr/lib/jvm/java-8-oracle
    /usr/lib/jdk8-latest

    # OpenJDK 8 Candidates
    /usr/lib/jvm/java-1.8.0-openjdk-amd64
    /usr/lib/jvm/java-1.8.0-openjdk-ppc64el
    /usr/lib/jvm/java-1.8.0-openjdk
    /usr/lib64/jvm/java-1.8.0-openjdk-1.8.0

    # Oracle JDK 7 Candidates
    /usr/java/jdk1.7
    /usr/java/jre1.7
    /usr/lib/jvm/j2sdk1.7-oracle
    /usr/lib/jvm/j2sdk1.7-oracle/jre
    /usr/lib/jvm/java-7-oracle
    /usr/lib/jdk7-latest

    # OpenJDK 7 Candidates
    /usr/lib/jvm/java-1.7.0-openjdk
    /usr/lib/jvm/java-7-openjdk

    # Misc. Candidates
    /Library/Java/Home
    /usr/java/default
    /usr/lib/jvm/java
    /usr/lib/jvm/jre
    /usr/lib/jvm/default-java
    /usr/lib/jvm/java-openjdk
    /usr/lib/jvm/jre-openjdk"""
JAVA_CANDIDATES = [x.strip() for x in JAVA_CANDIDATES.split("\n")]
JAVA_CANDIDATES = [x for x in JAVA_CANDIDATES if not x.startswith("#")]

def is_elf_binary(path):
  """ Determine if the given path is an ELF binary (executable or shared library) """
  if not os.path.isfile(path) or os.path.islink(path):
    return False
  try:
    with file(path, "rb") as f:
      magic = f.read(4)
      return magic == "\x7fELF"
  except:
    # Ignore unreadable files
    return False

def fix_rpath_component(bin_path, path):
  """
  Given an RPATH component 'path' of the binary located at 'bin_path',
  fix the thirdparty dir to be relative to the binary rather than absolute.
  """
  rel_tp = os.path.relpath(os.path.join(ROOT, "thirdparty/"),
                           os.path.dirname(bin_path))
  path = re.sub(r".*thirdparty/", "$ORIGIN/"+rel_tp + "/", path)
  return path

def fix_rpath(path):
  """
  Fix the RPATH/RUNPATH of the binary located at 'path' so that
  the thirdparty/ directory is properly found, even though we will
  run the binary at a different path than it was originally built.
  """
  # Fetch the original rpath.
  p = subprocess.Popen(["chrpath", path],
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
  stdout, stderr = p.communicate()
  if p.returncode != 0:
    return
  rpath = re.search("R(?:UN)?PATH=(.+)", stdout.strip()).group(1)
  # Fix it to be relative.
  new_path = ":".join(fix_rpath_component(path, c) for c in rpath.split(":"))
  # Write the new rpath back into the binary.
  subprocess.check_call(["chrpath", "-r", new_path, path])

def fixup_rpaths(root):
  """
  Recursively walk the directory tree 'root' and fix the RPATH for any
  ELF files (binaries/libraries) that are found.
  """
  for dirpath, dirnames, filenames in os.walk(root):
    for f in filenames:
      p = os.path.join(dirpath, f)
      if is_elf_binary(p):
        fix_rpath(p)

def find_java():
  for x in JAVA_CANDIDATES:
    if os.path.exists(x):
      print >>sys.stderr, "found JAVA_HOME: " + x
      return os.path.join(x, "bin", "java")

def main():
  p = optparse.OptionParser(usage="usage: %prog [options] <test-name>")
  p.add_option("-e", "--env", dest="env", type="string", action="append",
               help="key=value pairs for environment variables",
               default=[])
  p.add_option("--collect-tmpdir", dest="collect_tmpdir", action="store_true",
               help="whether to collect the test tmpdir as an artifact if the test fails",
               default=False)
  p.add_option("--test-type", dest="test_type", action="store",
               help="java or cpp",
               default="cpp")
  options, args = p.parse_args()
  if len(args) < 1:
    p.print_help(sys.stderr)
    sys.exit(1)

  env = os.environ.copy()
  for env_pair in options.env:
    (k, v) = env_pair.split("=", 1)
    env[k] = v

  # Fix the RPATHs of any binaries. During the build, we end up with
  # absolute paths from the build machine. This fixes the paths to be
  # binary-relative so that we can run it on the new location.
  #
  # It's important to do this rather than just putting all of the thirdparty
  # lib directories into $LD_LIBRARY_PATH below because we need to make sure
  # that non-TSAN-instrumented runtime tools (like 'llvm-symbolizer') do _NOT_
  # pick up the TSAN-instrumented libraries, whereas TSAN-instrumented test
  # binaries (like 'foo_test' or 'kudu-tserver') _DO_ pick them up.
  fixup_rpaths(os.path.join(ROOT, "build"))
  fixup_rpaths(os.path.join(ROOT, "thirdparty"))

  # Add environment variables for Java dependencies. These environment variables
  # are used in mini_hms.cc.
  env['HIVE_HOME'] = glob.glob(os.path.join(ROOT, "thirdparty/src/apache-hive-*-bin"))[0]
  env['HADOOP_HOME'] = glob.glob(os.path.join(ROOT, "thirdparty/src/hadoop-*"))[0]
  env['JAVA_HOME'] = glob.glob("/usr/lib/jvm/java-1.8.0-*")[0]

  env['LD_LIBRARY_PATH'] = ":".join(
    [os.path.join(ROOT, "build/dist-test-system-libs/"),
     os.path.join(ROOT, "build/lib/")] +
    glob.glob(os.path.abspath((os.path.join(ROOT, "build/*/lib")))))

  # GTEST_OUTPUT must be canonicalized and have a trailing slash for gtest to
  # properly interpret it as a directory.
  test_logdir = os.path.abspath(os.path.join(ROOT, "build/test-logs"))
  env['GTEST_OUTPUT'] = 'xml:' + test_logdir + '/'
  env['TEST_LOGDIR'] = test_logdir

  # Don't pollute /tmp in dist-test setting. If a test crashes, the dist-test slave
  # will clear up our working directory but won't be able to find and clean up things
  # left in /tmp.
  test_tmpdir = os.path.abspath(os.path.join(ROOT, "test-tmp"))
  env['TEST_TMPDIR'] = test_tmpdir

  env['ASAN_SYMBOLIZER_PATH'] = os.path.join(ROOT, "thirdparty/installed/uninstrumented/bin/llvm-symbolizer")

  stdout = None
  stderr = None
  if options.test_type == 'cpp':
    cmd = [os.path.join(ROOT, "build-support/run-test.sh")] + args
  elif options.test_type == 'java':
    # The build/test-logs/ dir needs to be at the root of the
    # of the expanded isolate tree.
    isolate_dir = os.path.join(ROOT, "java/build/dist-test")
    test_logdir = os.path.abspath(os.path.join(isolate_dir, "build/test-logs"))
    if not os.path.exists(test_logdir):
      os.makedirs(test_logdir)
    cmd = [find_java()] + args
    stdout = stderr = file(os.path.join(test_logdir, "test-output.txt"), "w")
  else:
    raise ValueError("invalid test type: " + options.test_type)
  print >>sys.stderr, "Running command: ", cmd
  print >>sys.stderr, "in dir: ", os.getcwd()
  print >>sys.stderr, "Running with env: ", repr(env)
  rc = subprocess.call(cmd, env=env, stdout=stdout, stderr=stderr)

  if rc != 0 and options.collect_tmpdir:
    os.system("tar czf %s %s" % (os.path.join(test_logdir, "test_tmpdir.tgz"), test_tmpdir))
  sys.exit(rc)


if __name__ == "__main__":
  main()
