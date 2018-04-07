/*

   Derby - Class org.apache.derby.impl.jdbc._Suite

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

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;

public class _Suite
    extends BaseTestCase {

    /**
     * Use suite method instead.
     */
    private _Suite(String name) {
        super(name);
    }

    public static Test suite()
            throws Exception {

        BaseTestSuite suite = new BaseTestSuite("jdbc.impl package-private");

        suite.addTest(SmallTemporaryClobTest.suite());
        suite.addTest(BiggerTemporaryClobTest.suite());
        suite.addTest(SmallStoreStreamClobTest.suite());
        suite.addTest(BiggerStoreStreamClobTest.suite());
        suite.addTest(UTF8ReaderTest.suite());

        return suite;
    }
} // End class _Suite
