// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.test.junit;

import java.util.ArrayList;
import java.util.List;

import junit.runner.Version;
import org.junit.internal.RealSystem;
import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

/**
 * A Wrapper around the JUnitCore test runner that attaches the
 * {@link JUnitXmlRunListener} to generate JUnit XML reports.
 */
public class JunitXMLCore {

  public static void main(String... args) {
    System.out.println("JUnit version " + Version.id());
    Result result = runTests(args);
    System.exit(result.wasSuccessful() ? 0 : 1);
  }

  public static Result runTests(String... args) {
    JUnitCore core = new JUnitCore();
    core.addListener(new TextListener(new RealSystem()));
    core.addListener(new JUnitXmlRunListener());

    Class[] classes = parseClassArgs(args);
    return core.run(classes);
  }

  private static Class[] parseClassArgs(String[] args) {
    try {
      List<Class> classes = new ArrayList<>(args.length);
      for (String arg : args) {
        classes.add(Class.forName(arg));
      }
      return classes.toArray(new Class[0]);
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }
}
