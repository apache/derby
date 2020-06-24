/*

   Derby - Class org.apache.derby.impl.services.jmxnone.NoManagementService

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

package org.apache.derby.impl.services.jmxnone;

import org.apache.derby.iapi.services.jmx.ManagementService;

/** 
 * Dummy management service for environments that do not support
 * JMX, such as Java SE compact profile 2.
*/
public final class NoManagementService implements ManagementService {
    public NoManagementService() {
    }
    public <T> Object registerMBean(T bean,
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            Class<T> beanInterface,
            String keyProperties)
    {
        return null;
    }
    public void unregisterMBean(Object mbeanIdentifier) {
    }
    public boolean isManagementActive() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3424
//IC see: https://issues.apache.org/jira/browse/DERBY-1387
        return false;
    }
    public void startManagement() {
    }
    public void stopManagement() {
    }
    public String getSystemIdentifier() {
        return null;
    }
    public String quotePropertyValue(String value) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
        return null;
    }
}
