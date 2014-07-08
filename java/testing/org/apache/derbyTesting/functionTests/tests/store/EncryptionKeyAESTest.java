/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.EncryptionKeyAESTest
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
package org.apache.derbyTesting.functionTests.tests.store;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.SupportFilesSetup;

/**
 * Test basic functionality on a database encrypted with the AES algorithm.
 *
 * @see EncryptionKeyTest
 */
public class EncryptionKeyAESTest
    extends EncryptionKeyTest {

    public EncryptionKeyAESTest(String name) {
        super(name,
              "AES/CBC/NoPadding",
              "616263646666768661626364666676AF",
              "919293949999798991929394999979CA",
              "616263646666768999616263646666768",
              "X1X2X3X4XXXX7X8XX1X2X3X4XXXX7X8X");
    }

    public static Test suite() {
        // This test runs only in embedded due to the use of external files.
        BaseTestSuite suite = new BaseTestSuite(EncryptionKeyAESTest.class,
                                        "EncryptionKey AES suite");
        return new SupportFilesSetup(suite);
    }
} // End class EncryptionKeyAESTest
