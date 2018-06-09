/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.CanonTestCase
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;

/**
 * Run a test that compares itself to a master (canon) file.
 * This is used to support cannon based tests that ran
 * under the old Derby test harness without having to convert
 * them. It is not recommended for new tests. New test should
 * use the JUnit assert mechanisms.
 *
 */
abstract class CanonTestCase extends BaseJDBCTestCase {

    final static String DEFAULT_ENCODING = "US-ASCII";
    final String outputEncoding;

    private ByteArrayOutputStream rawBytes;

    CanonTestCase(String name) {
        this(name, null);
    }

    CanonTestCase(String name, String encoding) {
        super(name);
        outputEncoding = (encoding == null) ? DEFAULT_ENCODING : encoding;
    }

    OutputStream getOutputStream() {
        return rawBytes = new ByteArrayOutputStream(20 * 1024);
    }

    /**
     * Compare the output to the canon provided.
     * 
     * @param canon
     *            Name of canon as a resource.
     */
    void compareCanon(String canon) throws Throwable {
        rawBytes.flush();
        rawBytes.close();

        byte[] testRawBytes = rawBytes.toByteArray();
        rawBytes = null;
        BufferedReader cannonReader = null;
        BufferedReader testOutput = null;

        try {
            URL canonURL = getTestResource(canon);
            assertNotNull("No master file " + canon, canonURL);

            InputStream canonStream = openTestResource(canonURL);

            cannonReader = new BufferedReader(
                    new InputStreamReader(canonStream, outputEncoding));

            testOutput = new BufferedReader(
                    new InputStreamReader(
                            new ByteArrayInputStream(testRawBytes),
                            outputEncoding));

            for (int lineNumber = 1;; lineNumber++) {
                String testLine = testOutput.readLine();

                String canonLine = cannonReader.readLine();

                if (canonLine == null && testLine == null)
                    break;

                if (canonLine == null)
                    fail("More output from test than expected");

                if (testLine == null)
                    fail("Less output from test than expected, stoped at line"
                            + lineNumber);

                assertEquals("Output at line " + lineNumber, canonLine,
                        testLine);
            }
        } catch (Throwable t) {
            dumpForFail(testRawBytes);
            throw t;
        } finally {
            if (cannonReader != null) {
                try {
                    cannonReader.close();
                } catch (IOException e) {
                }
            }
            
            if (testOutput != null) {
                try {
                    testOutput.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * Dump the output that did not compare correctly into the failure folder
     * with the name this.getName() + ".out".
     * 
     * @param rawOutput
     * @throws IOException
     * @throws PrivilegedActionException
     */
    private void dumpForFail(byte[] rawOutput) throws IOException,
            PrivilegedActionException {

        File folder = getFailureFolder();
        final File outFile = new File(folder, getName() + ".out");

        OutputStream outStream = AccessController
                .doPrivileged(new PrivilegedExceptionAction<OutputStream>() {
                    public OutputStream run() throws IOException {
                        return new FileOutputStream(outFile);
                    }
                });

        outStream.write(rawOutput);
        outStream.flush();
        outStream.close();
    }
}
