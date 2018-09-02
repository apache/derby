/*

Derby - Class org.apache.derbyTesting.junit.DerbyConstants

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

package org.apache.derbyTesting.junit;

/**
 * DerbyConstants used by the Derby JUnit framework, for instance when assuming
 * something about default settings of a property.
 */
public interface DerbyConstants {

    /**
     * The default port used by the network server and the network tools when
     * no port has been specified.
     */
    int DEFAULT_DERBY_PORT = 1527;

    /** Default name for the DBO (database owner). */
    String TEST_DBO = "TEST_DBO";

    /** Name of the automatic JUnit module. */
    String JUNIT_MODULE_NAME = "junit";
}
