/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.EncryptionKeyBlowfishTest
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

import org.apache.derbyTesting.junit.*;
import junit.framework.*;

/**
 * Test basic functionality on a database encrypted with the Blowfish
 * algorithm.
 *
 * @see EncryptionKeyTest
 */
public class EncryptionKeyBlowfishTest
    extends EncryptionKeyTest {

    public EncryptionKeyBlowfishTest(String name) {
        super(name,
              "Blowfish/OFB/NoPadding",
              "6162636466667686",
              "9192939499997989",
              "6162636466667689979",
              "X1X2X3X4XXXX7X8X");
    }

    public static Test suite() {
        // This test runs only in embedded due to the use of external files.
        Test suite =
            TestConfiguration.embeddedSuite(EncryptionKeyBlowfishTest.class);
        return new SupportFilesSetup(suite);
    }
} // End class EncryptionKeyBlowfishTest
