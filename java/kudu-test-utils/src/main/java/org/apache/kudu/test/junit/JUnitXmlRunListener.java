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

import static org.apache.kudu.test.TempDirUtils.DeleteOnExit.NO_DELETE_ON_EXIT;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import org.apache.kudu.test.TempDirUtils;

/**
 * This {@link RunListener} produces a JUnit XML report.
 * The format of the JUnit XML report, though not an official standard,
 * follows the format of the Ant Junit task:
 * https://ant.apache.org/manual/Tasks/junit.html
 */
public class JUnitXmlRunListener extends RunListener {

  private static final String TEST_LOGDIR = "TEST_LOGDIR";

  public final SimpleDateFormat iso8601DatetimeFormat =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  private final File reportDir;

  private Document document;
  private Element testSuite;

  private Map<Description, Long> testStarts = new HashMap<>();
  private Map<Description, Failure> testFailures = new HashMap<>();

  private ByteArrayOutputStream stdout;
  private ByteArrayOutputStream stderr;

  private PrintStream oldStdout;
  private PrintStream oldStderr;

  public JUnitXmlRunListener() {
    String logDirProp = System.getProperty(TEST_LOGDIR);
    File reportDir;
    try {
      if (logDirProp != null) {
        reportDir = new File(logDirProp);
        if (!reportDir.mkdirs()) {
          throw new IOException("Could not create test log directory: " + logDirProp);
        }
      } else {
        reportDir = TempDirUtils.makeTempDirectory("test-results", NO_DELETE_ON_EXIT);
      }
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
    this.reportDir = reportDir;
    this.document = createDocument();
  }

  private Document createDocument() {
    try {
      DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      return documentBuilder.newDocument();
    } catch (ParserConfigurationException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void testRunStarted(Description description) throws Exception {
    super.testRunStarted(description);
    testSuite = document.createElement("testsuite");
    document.appendChild(testSuite);

    String name = description.getDisplayName();
    testSuite.setAttribute("name", name == null ? "unknown" : name);

    final String timestamp = iso8601DatetimeFormat.format(new Date());
    testSuite.setAttribute("timestamp", timestamp);

    final String hostname = InetAddress.getLocalHost().getHostName();
    testSuite.setAttribute("hostname", hostname);
  }

  @Override
  public void testRunFinished(Result result) throws Exception {
    super.testRunFinished(result);
    testSuite.setAttribute("tests",  Integer.toString(result.getRunCount()));

    testSuite.setAttribute("skipped", Integer.toString(result.getIgnoreCount()));

    testSuite.setAttribute("failures", Integer.toString(result.getFailureCount()));

    // JUnit4 does not seem to discern between failures and errors.
    testSuite.setAttribute("errors", "0");

    double runtimeSeconds = result.getRunTime() / 1000.0;
    testSuite.setAttribute("time", Double.toString(runtimeSeconds));

    writeJunitXML();
  }

  private void writeJunitXML() throws Exception {
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer = transformerFactory.newTransformer();
    // Configure pretty print.
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");

    DOMSource source = new DOMSource(document);

    String suitName = testSuite.getAttribute("name");
    String fileName = "TEST-" + suitName + ".xml";
    StreamResult file = new StreamResult(new File(reportDir, fileName));

    transformer.transform(source, file);
  }

  @Override
  public void testStarted(Description description) throws Exception {
    super.testStarted(description);
    testStarts.put(description, System.currentTimeMillis());

    // Save the old stdout & stderr so we can reset after the test is done.
    // Then, set sdout and stderr to be captured in a ByteArrayOutputStream
    // for the duration of the test.
    this.oldStdout = System.out;
    this.oldStderr = System.err;
    this.stdout = new ByteArrayOutputStream();
    this.stderr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(stdout));
    System.setErr(new PrintStream(stderr));
  }

  @Override
  public void testFinished(Description description) throws Exception {
    super.testFinished(description);
    // Reset back to the original stdout and stderr.
    System.out.flush();
    System.err.flush();
    System.setOut(oldStdout);
    System.setErr(oldStderr);

    Element testCase = createTestCaseElement(description);

    if (testFailures.containsKey(description)) {
      Failure failure = testFailures.get(description);

      Element failureElement = document.createElement("failure");
      testCase.appendChild(failureElement);

      String message = failure.getMessage();
      if (message != null && message.length() > 0) {
        failureElement.setAttribute("message", message);
      }
      failureElement.setAttribute("type", description.getDisplayName());

      String stackTrace = getStackTrace(failure.getException());
      Text textNode = document.createTextNode(stackTrace);
      failureElement.appendChild(textNode);
    }

    Element stdoutElement = document.createElement("system-out");
    testCase.appendChild(stdoutElement);
    Text outData = document.createCDATASection(stdout.toString());
    stdoutElement.appendChild(outData);

    Element sterrElement = document.createElement("system-err");
    testCase.appendChild(sterrElement);
    Text errData = document.createCDATASection(stderr.toString());
    sterrElement.appendChild(errData);

    addTimeToTestCaseElement(testCase, description);

    testSuite.appendChild(testCase);
  }

  @Override
  public void testFailure(Failure failure) throws Exception {
    super.testFailure(failure);
    handleTestFailure(failure);
  }

  @Override
  public void testAssumptionFailure(Failure failure) {
    super.testAssumptionFailure(failure);
    handleTestFailure(failure);
  }

  private void handleTestFailure(Failure failure) {
    testFailures.put(failure.getDescription(), failure);
  }

  private static String getStackTrace(Throwable throwable) {
    try (StringWriter sw = new StringWriter();
         PrintWriter pw = new PrintWriter(sw, true)) {
      throwable.printStackTrace(pw);
      return sw.toString();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void testIgnored(Description description) throws Exception {
    super.testIgnored(description);
    Element testCase = createTestCaseElement(description);
    Element skipped = document.createElement("skipped");
    testCase.appendChild(skipped);
    addTimeToTestCaseElement(testCase, description);
    testSuite.appendChild(testCase);
  }

  private Element createTestCaseElement(Description description) {
    Element testcase = document.createElement("testcase");

    String testName = description.getMethodName();
    testcase.setAttribute("name", testName == null ? "unknown" : testName);

    String className = description.getClassName();
    testcase.setAttribute("classname", className == null ? "unknown" : className);

    return testcase;
  }

  // Add the time attribute to the finished tests.
  private void addTimeToTestCaseElement(Element testCase, Description description) {
    long startTime = testStarts.getOrDefault(description, 0L);
    double runTime = (System.currentTimeMillis() - startTime) / 1000.0;
    testCase.setAttribute("time", Double.toString(runTime));
  }
}