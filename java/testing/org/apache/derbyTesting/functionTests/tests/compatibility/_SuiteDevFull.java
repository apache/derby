/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility._SuiteDevFull

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
package org.apache.derbyTesting.functionTests.tests.compatibility;

import junit.framework.Test;

/**
 * Tests trunk against all available versions of old Derby releases. 
 * <p>
 * This is different from the MATS in that it also tests old releases on
 * branches and not only the latest release on each branch.
 */
public class _SuiteDevFull
        extends _Suite {

    public _SuiteDevFull(String name) {
        super(name);
        throw new IllegalStateException("invoke suite() instead");
    }

    public static Test suite() {
        configurator = VersionCombinationConfigurator.getInstanceDevFull();
        return _Suite.suite();
    }
}
