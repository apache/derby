/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.Utf8CcsidManagerClientTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;

import junit.framework.Test;
import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.LogWriter;

import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.net.NetAgent;
import org.apache.derby.client.net.Utf8CcsidManager;
import org.apache.derbyTesting.functionTests.util.TestNullOutputStream;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

public class Utf8CcsidManagerClientTest extends BaseTestCase {
    private static final String CANNOT_CONVERT = "22005";

    private Utf8CcsidManager ccsidManager;
    private Agent agent;
    
    public Utf8CcsidManagerClientTest(String name) throws Exception {
        super(name);
        
        ccsidManager = new Utf8CcsidManager();

        // Set up a dummy Agent since testInvalidCharacters require one for
        // generating exceptions.
//IC see: https://issues.apache.org/jira/browse/DERBY-5771
        PrintWriter pw = new PrintWriter(new TestNullOutputStream());
        agent = new NetAgent(null, new LogWriter(pw, 0));
    }

    protected void tearDown() {
        ccsidManager = null;
        agent = null;
    }

    /**
     * Use the Utf8CcsidManager to convert strings from UCS2/UTF-16 into UTF-8
     */
    public void testConvertFromJavaString() throws Exception {
        // Get the UTF-16 representation of "Hello World" in Chinese
        String ucs2String = new String(new String("\u4f60\u597d\u4e16\u754c").getBytes("UTF-16"),"UTF-16");
        
        // Get the same as above but in UTF-8
        byte[] utf8Bytes = new String("\u4f60\u597d\u4e16\u754c").getBytes("UTF-8");

        // Use the CcsidManager to convert the UTF-16 string to UTF-8 bytes
        byte[] utf8Converted = ccsidManager.convertFromJavaString(ucs2String, null);
        
        // Compare the bytes
        assertTrue("UTF-8 conversion isn't equal to bytes",
                Arrays.equals(utf8Bytes, utf8Converted));
        
    }
    
    /**
     * Use the Utf8CcsidManager to convert strings from UCS2/UTF-16 into UTF-8
     * while offsetting the first character (3 bytes)
     */
    public void testConvertFromJavaStringWithOffset() throws Exception {
        // String with 1 more chinese char (3 bytes) in the beginning
        String ucs2String = new String(new String("\u4f60\u597d\u4e16\u754c").getBytes("UTF-16"),"UTF-16");
        
        // Create a new byte array with one additional chinese char (3 bytes) in the beginning
        byte[] additionalBytes = new String("\u53f0\u4f60\u597d\u4e16\u754c").getBytes("UTF-8");
        
        // Create a buffer to accommodate additionalBytes.length bytes
        byte[] buffer = new byte[additionalBytes.length];
        
        // Copy the first character manually
        buffer[0] = additionalBytes[0];
        buffer[1] = additionalBytes[1];
        buffer[2] = additionalBytes[2];
        
        // Offset 3 bytes and convert the 4 chars in ucs2String
//IC see: https://issues.apache.org/jira/browse/DERBY-5068
        ByteBuffer wrapper = ByteBuffer.wrap(buffer);
        wrapper.position(3);
        ccsidManager.startEncoding();
        boolean success =
            ccsidManager.encode(CharBuffer.wrap(ucs2String), wrapper, null);
        assertTrue("Overflow in encode()", success);
            
        assertTrue("UTF-8 conversion isn't equal to bytes (with buffer)",
                Arrays.equals(additionalBytes, buffer));
    }
    
    /**
     * Use the Utf8CcsidManager to convert strings from UTF-8 into UCS2/UTF-16
     */
    public void testConvertToJavaString() throws Exception {
        // Get the UTF-8 bytes for "Hello World" in Chinese
        byte[] utf8Bytes = new String("\u4f60\u597d\u4e16\u754c").getBytes("UTF-8");
        
        // Get the 2nd and 3rd Chinese characters in UTF-16
        String offsetUcs2String = new String(new String("\u597d\u4e16").getBytes("UTF-16"),"UTF-16");
        
        // Convert just the two characters as offset above and compare
        String convertedOffset = ccsidManager.convertToJavaString(utf8Bytes, 3, 6);
        assertEquals(offsetUcs2String, convertedOffset);
    }

    /**
     * Test encoding of invalid Unicode characters. Expect an exception to
     * be thrown when encountering a character that cannot be encoded.
     */
    public void testInvalidCharacters() {
        // Codepoints 0xD800 - 0xDFFF arent legal
        String invalidString = "\uD800";
//IC see: https://issues.apache.org/jira/browse/DERBY-5068

        ccsidManager.startEncoding();
        try {
            ccsidManager.encode(
                    CharBuffer.wrap(invalidString),
                    ByteBuffer.allocate(10),
                    agent);
            fail("Encoding invalid codepoint should fail");
        } catch (SqlException sqle) {
            if (!CANNOT_CONVERT.equals(sqle.getSQLState())) {
                fail("Expected SQLState " + CANNOT_CONVERT, sqle);
            }
        }

        try {
            ccsidManager.convertFromJavaString(invalidString, agent);
            fail("Encoding invalid codepoint should fail");
        } catch (SqlException sqle) {
            if (!CANNOT_CONVERT.equals(sqle.getSQLState())) {
                fail("Expected SQLState " + CANNOT_CONVERT, sqle);
            }
        }
    }

    public static Test suite() {
        return TestConfiguration.clientServerSuite(Utf8CcsidManagerClientTest.class);
    }
}
