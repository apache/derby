/*

   Derby - Class
   org.apache.derbyTesting.functionTests.tests.multi.StressMulti50x59

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.multi;

import junit.framework.Test;
import junit.framework.TestCase;

/**
 * This test runs the StressMultiTest with 50 threads for 59 minutes.
 *
 */
public class StressMulti50x59 extends TestCase {

    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
        return StressMultiTest.embeddedSuite(50,59);
    }
}
