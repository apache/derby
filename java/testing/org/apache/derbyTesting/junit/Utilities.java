/*
 *
 * Derby - Class org.apache.derbyTesting.junit.Utilities
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
package org.apache.derbyTesting.junit;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.StringTokenizer;

import junit.framework.Assert;

/**
 * General non-JDBC related utilities.
 * Some of these were relocated from TestUtil.
 */
public class Utilities {

    /** The newline character(s). */
    public static final String NL;
    static {
        NL = BaseTestCase.getSystemProperty("line.separator");
    }

    private Utilities() { }

    /**
     * Converts a string to a hex literal to assist in converting test
     * cases that used to insert strings into bit data tables.
     * <p>
     * Converts using UTF-16BE just like the old casts used to.
     *
     * @param s string to convert
     * @return hex literal that can be inserted into a bit column.
     */
    public static String stringToHexLiteral(String s) {
        byte[] bytes;
        String hexLiteral = null;
        try {
            bytes = s.getBytes("UTF-16BE");
            hexLiteral = convertToHexString(bytes);
        } catch (UnsupportedEncodingException ue) {
            Assert.fail("Encoding UTF-16BE unavailable: " + ue.getMessage());
        }

        return hexLiteral;
    }

    /**
     * Convert a byte array to a hex string suitable for insert.
     *
     * @param buf  byte array to convert
     * @return     formated string representing byte array
     */
    private static String convertToHexString(byte[] buf) {
        StringBuffer str = new StringBuffer();
        str.append("X'");
        String val;
        int byteVal;
        for (int i = 0; i < buf.length; i++) {
            byteVal = buf[i] & 0xff;
            val = Integer.toHexString(byteVal);
            if (val.length() < 2) {
                str.append("0");
            }
            str.append(val);
        }
        return str.toString() + "'";
    }

    /**
     * Creates a string with the specified length.
     * <p>
     * Called from various tests to test edge cases and such.
     *
     * @param c             character to repeat
     * @param repeatCount   Number of times to repeat character
     * @return              String of repeatCount characters c
     */
    public static String repeatChar(String c, int repeatCount) {
        char[] chArray = new char[repeatCount];
        Arrays.fill(chArray, c.charAt(0));
        return String.valueOf(chArray);
    }

    /**
     * Print out resultSet in two dimensional array format, for use by
     * JDBC.assertFullResultSet(rs,expectedRows) expectedRows argument.
     * Useful while converting tests to get output in correct format.
     *
     * @param rs result set to print
     * @throws SQLException if accessing the result set fails
     */
    public static void showResultSet(ResultSet rs) throws SQLException {
        System.out.print("{");
        int row = 0;
        boolean next = rs.next();
        while (next) {
            row++;
            ResultSetMetaData rsmd = rs.getMetaData();
            int nocols = rsmd.getColumnCount();
            System.out.print("{");

            for (int i = 0; i < nocols; i++) {
                String val = rs.getString(i + 1);
                if (val == null) {
                    System.out.print("null");
                } else {
                    System.out.print("\"" + rs.getString(i + 1) + "\"");
                }
                if (i == (nocols - 1)) {
                    System.out.print("}");
                } else {
                    System.out.print(",");
                }

            }
            next = rs.next();

            if (next) {
                System.out.println(",");
            } 
        }
        System.out.println("};\n");
    }

    /**
     * Calls the public method {@code getInfo} of the sysinfo tool within
     * this JVM and returns a {@code BufferedReader} for reading its
     * output. This is useful for obtaining system information that could be 
     * used to verify, for example, values returned by Derby MBeans.
     * 
     * @return a buffering character-input stream containing the output from
     *         sysinfo
     * @see org.apache.derby.tools.sysinfo#getInfo(java.io.PrintWriter out)
     */
    public static BufferedReader getSysinfoLocally() {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(20 * 1024);
        PrintWriter pw = new PrintWriter(byteStream, true); // autoflush
        org.apache.derby.tools.sysinfo.getInfo(pw);
        pw.flush();
        pw.close();
        byte[] outBytes = byteStream.toByteArray();
        BufferedReader sysinfoOutput = new BufferedReader(
                new InputStreamReader(
                new ByteArrayInputStream(outBytes)));
        return sysinfoOutput;
    }

    /**
     * <p>Calls the public method {@code getSysInfo} of the Network
     * Server instance associated with the current test configuration and 
     * returns the result as a BufferedReader, making it easy to analyse the 
     * output line by line.</p>
     * 
     * <p>This is useful for obtaining system information that could be 
     * used to verify, for example, values returned by Derby MBeans.</p>
     * 
     * @return a buffering character-input stream containing the output from 
     *         the server's sysinfo.
     * @see org.apache.derby.drda.NetworkServerControl#getSysinfo()
     */
    public static BufferedReader getSysinfoFromServer() throws Exception {

        return new BufferedReader(new StringReader(
                NetworkServerTestSetup.getNetworkServerControl().getSysinfo()));
    }

    /**
     * Splits a string around matches of the given delimiter character.
     * Copied from org.apache.derby.iapi.util.StringUtil
     *
     * Where applicable, this method can be used as a substitute for
     * {@code String.split(String regex)}, which is not available
     * on a JSR169/Java ME platform.
     *
     * @param str the string to be split
     * @param delim the delimiter
     * @throws NullPointerException if str is null
     */
    public static String[] split(String str, char delim) {
        // Note the javadoc on StringTokenizer:
        //     StringTokenizer is a legacy class that is retained for
        //     compatibility reasons although its use is discouraged in
        //     new code.
        // In other words, if StringTokenizer is ever removed from the JDK,
        // we need to have a look at String.split() (or java.util.regex)
        // if it is supported on a JSR169/Java ME platform by then.
        StringTokenizer st = new StringTokenizer(str, String.valueOf(delim));
        int n = st.countTokens();
        String[] s = new String[n];
        for (int i = 0; i < n; i++) {
            s[i] = st.nextToken();
        }
        return s;
    }

    /**
     * Sleeps the specified number of milliseconds.
     *
     * @param millis sleep duration
     */
    public static void sleep(long millis) {
        long started = System.currentTimeMillis();
        long waited = 0;
        while (waited < millis) {
            try {
                Thread.sleep(millis - waited);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            waited = System.currentTimeMillis() - started;
        }
    }

    /**
     * Function to eliminate known package prefixes given a class full path
     * 
     * @param test
     *            class name prefixed with package
     */
    public static String formatTestClassNames(String test) {
        final String COMMON_FUNCTIONTEST_PREFIX = "org.apache.derbyTesting.functionTests.tests.";
        final String COMMON_TEST_PREFIX = "org.apache.derbyTesting.";
        if (test.startsWith(COMMON_FUNCTIONTEST_PREFIX)) {
            return test.substring(COMMON_FUNCTIONTEST_PREFIX.length());
        } else if (test.startsWith(COMMON_TEST_PREFIX)) {
            return test.substring(COMMON_TEST_PREFIX.length());
        } else {
            return test;
        }
    }
}
