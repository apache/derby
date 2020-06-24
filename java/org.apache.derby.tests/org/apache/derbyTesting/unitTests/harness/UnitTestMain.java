/*

   Derby - Class org.apache.derbyTesting.unitTests.harness.UnitTestMain

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

package org.apache.derbyTesting.unitTests.harness;

import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;
import org.apache.derby.iapi.services.monitor.Monitor;

/** 
	A very simple class to boot up a system based upon a configuration file
	passed in as the first argument. The optional second argument is taken
	as a boolean. If the argument is missing or false, the configuration
	is started, otherwise the configuration is created.
	

    Usage: java org.apache.derbyTesting.unitTests.harness.UnitTestMain config-file [true]
**/


public class UnitTestMain  { 

	public static void main(String args[]) {

//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     Properties bootProperties = new Properties();

                     // request that a unit test manager service is started
                     bootProperties.put("derby.service.unitTestManager", UnitTestManager.MODULE);
                     Monitor.startMonitor(bootProperties, new PrintWriter(System.err, true));
                     
                     return null;
                 }
             }
             );


	}

    
}
