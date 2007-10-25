/*

   Derby - Class org.apache.derby.impl.jdbc.BiggerTemporaryClobTest

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

import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Test basic operations on a read-wrote Clob that is kept on disk.
 */
public class BiggerTemporaryClobTest
    extends InternalClobTest {

    private static final long CLOBLENGTH = 287*1024-115; //~287 KB
    private static final long BYTES_PER_CHAR = 3; // Only Tamil characters.

    public BiggerTemporaryClobTest(String name) {
        super(name);
    }

    /**
     * Creates a bigger read-write Clob that is being kept on disk due to its
     * size.
     */
    public void setUp()
            throws Exception {
        super.initialCharLength = CLOBLENGTH;
        super.initialByteLength = CLOBLENGTH *3; // Only Tamil characters.
        super.bytesPerChar = BYTES_PER_CHAR;
        EmbedStatement embStmt = (EmbedStatement)createStatement();
        EmbedConnection embCon =(EmbedConnection)getConnection();
        iClob = new TemporaryClob(embCon.getDBName(), embStmt);
        transferData(
            new LoopingAlphabetReader(CLOBLENGTH, CharAlphabet.cjkSubset()),
            iClob.getWriter(1L),
            CLOBLENGTH);
        assertEquals(CLOBLENGTH, iClob.getCharLength());
    }

    public void tearDown()
            throws Exception {
        this.iClob.release();
        this.iClob = null;
        super.tearDown();
    }

    public static Test suite()
            throws Exception {
        Class<? extends TestCase> theClass = BiggerTemporaryClobTest.class;
        TestSuite suite = new TestSuite(theClass, "BiggerTemporaryClobTest suite");
        suite.addTest(addModifyingTests(theClass));
        return suite;
    }
} // End class BiggerTemporaryClobTest
