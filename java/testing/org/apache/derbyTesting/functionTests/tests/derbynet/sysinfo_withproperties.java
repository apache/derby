/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.sysinfo_withproperties

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
package org.apache.derbyTesting.functionTests.tests.derbynet;

/**
	Purpose of this class is to test the sysinfo command when 
    server is started with some drda properties. The derby properties
    in the test harness framework are added to 
    sysinfo_withproperties_derby.properties.
    
    Most of the work of calling sysinfo is done in sysinfo.
    @see sysinfo#test 
*/

public class sysinfo_withproperties
{
	public static void main (String args[]) throws Exception
	{
        // test the sysinfo calls.
        sysinfo.test(args);
        
	}

}
