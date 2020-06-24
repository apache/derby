/*

   Derby - Class org.apache.derby.impl.jdbc.BiggerStoreStreamTest

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

 */
package org.apache.derby.impl.jdbc;

import java.io.InputStream;
import junit.framework.Test;
import org.apache.derby.iapi.jdbc.CharacterStreamDescriptor;
import org.apache.derbyTesting.junit.BaseTestSuite;

/**
 * Tests basic operations on a bigger read-only Clob from the store module.
 */
public class BiggerStoreStreamClobTest
    extends InternalClobTest {

    private static final long CLOBLENGTH = 67*1024+19; // ~97 KB
    private static final long BYTES_PER_CHAR = 1; // All modern Latin

    public BiggerStoreStreamClobTest(String name) {
        super(name);
    }

    public void setUp()
            throws Exception {
        super.initialCharLength = CLOBLENGTH;
//IC see: https://issues.apache.org/jira/browse/DERBY-3907
        super.headerLength = 2 +3;
        // The fake stream uses ascii. Add header and EOF marker.
        super.initialByteLength = CLOBLENGTH + headerLength;
        super.bytesPerChar = BYTES_PER_CHAR;
        EmbedStatement embStmt = (EmbedStatement)createStatement();
        InputStream is = new FakeStoreStream(CLOBLENGTH);
        CharacterStreamDescriptor csd =
                new CharacterStreamDescriptor.Builder().stream(is).
                    charLength(initialCharLength).byteLength(0L).
                    curCharPos(CharacterStreamDescriptor.BEFORE_FIRST).
                    dataOffset(2L).build();
        iClob = new StoreStreamClob(csd, embStmt);
        assertEquals(CLOBLENGTH, iClob.getCharLength());
    }

    public void tearDown()
            throws Exception {
        this.iClob.release();
        this.iClob = null;
        super.tearDown();
    }

    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite(
            BiggerStoreStreamClobTest.class,
            "BiggerStoreStreamClobTest suite");
        return suite;
    }
} // End class BiggerStoreStreamClobTest
