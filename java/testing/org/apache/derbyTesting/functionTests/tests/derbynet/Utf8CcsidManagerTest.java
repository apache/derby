/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.Utf8CcsidManagerTest

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

import java.nio.ByteBuffer;
import java.util.Arrays;

import junit.framework.Test;

import org.apache.derby.impl.drda.Utf8CcsidManager;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/* This test uses internal APIs which might change over consequent releases. */
public class Utf8CcsidManagerTest extends BaseTestCase {
    private Utf8CcsidManager ccsidManager;
    
    public Utf8CcsidManagerTest(String name) {
        super(name);
        
        ccsidManager = new Utf8CcsidManager();
    }
    
    /**
     * Use the Utf8CcsidManager to convert strings from UCS2/UTF-16 into UTF-8
     */
    public void testConvertFromUCS2() throws Exception {
        // Get the UTF-16 representation of "Hello World" in Chinese
        String ucs2String = new String(new String("\u4f60\u597d\u4e16\u754c").getBytes("UTF-16"),"UTF-16");
        
        // Get the same as above but in UTF-8
        byte[] utf8Bytes = new String("\u4f60\u597d\u4e16\u754c").getBytes("UTF-8");
        
        // Use the CcsidManager to convert the UTF-16 string to UTF-8 bytes
        byte[] utf8Converted = ccsidManager.convertFromJavaString(ucs2String);
        
        // Compare the bytes
        assertTrue("UTF-8 conversion isn't equal to bytes",
                Arrays.equals(utf8Bytes, utf8Converted));
        
        // Repeat the process for the conversion using a buffer
        ByteBuffer buffer = ByteBuffer.allocate(utf8Bytes.length);
        
        ccsidManager.convertFromJavaString(ucs2String, buffer);
        if (buffer.hasArray()) {
            utf8Converted = buffer.array();
            
            assertTrue("UTF-8 conversion isn't equal to bytes (with buffer)",
                    Arrays.equals(utf8Bytes, utf8Converted));
        } else {
            fail("Could not convert from UCS2 to UTF-8 using a buffer");
        }
    }
    
    /**
     * Use the Utf8CcsidManager to convert strings from UTF-8 into UCS2/UTF-16
     */
    public void testConvertToUCS2() throws Exception {
        // Get the UTF-8 bytes for "Hello World" in Chinese
        byte[] utf8Bytes = new String("\u4f60\u597d\u4e16\u754c").getBytes("UTF-8");
        
        // Get the UTF-16 string for "Hello World" in Chinese
        String ucs2String = new String(new String("\u4f60\u597d\u4e16\u754c").getBytes("UTF-16"),"UTF-16");
        
        // Get the 2nd and 3rd Chinese characters in UTF-16
        String offsetUcs2String = new String(new String("\u597d\u4e16").getBytes("UTF-16"),"UTF-16");
        
        // Convert our UTF-8 bytes to UTF-16 using the CcsidManager and compare
        String convertedString = ccsidManager.convertToJavaString(utf8Bytes);
        assertEquals(ucs2String, convertedString);
        
        // Convert just the two characters as offset above and compare
        String convertedOffset = ccsidManager.convertToJavaString(utf8Bytes, 1, 2);
        assertEquals(offsetUcs2String, convertedOffset);
    }
    
    public static Test suite() {
        return TestConfiguration.clientServerSuite(Utf8CcsidManagerTest.class);
    }
}
