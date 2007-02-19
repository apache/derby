/*
 *
 * Derby - Class org.apache.derbyTesting.junit.ConnectorSetup
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
package org.apache.derbyTesting.junit;

import junit.framework.Assert;
import junit.framework.Test;

/**
 * Change the Connector implementation at setup time and
 * restore at tearDown time.
 *
 */
final class ConnectorSetup extends ChangeConfigurationSetup {

    private final String connectorClass;
    public ConnectorSetup(Test test, String connectorClass) {
        super(test);
        this.connectorClass = connectorClass;
    }

    TestConfiguration getNewConfiguration(TestConfiguration old) {
        // Copy the current configuration
        TestConfiguration newConfig = 
            new TestConfiguration(old);
        
        try {
            newConfig.connector = (Connector)
             Class.forName(connectorClass).newInstance();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        newConfig.connector.setConfiguration(newConfig);
        return newConfig;
    }

}
