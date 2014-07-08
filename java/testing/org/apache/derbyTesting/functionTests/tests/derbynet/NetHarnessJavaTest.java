/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.NetworkClientHarnessJavaTest

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

import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.HarnessJavaTest;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * NetHarnessJavaTest includes .java tests in the derbynet directory that
 * have not been converted to junit and do not have multiple masters.
 * 
 * The following tests could not be run this way, reasons for the
 * 
 * dblook_test_net - filters output
 * dblook_test_net_territory - filters output 
 * getCurrentProperties - ExceptionInInitializerError, needs investigation
 * maxthreads - forks VM
 * runtimeinfo" - filters output
 * sysinfo" - forks VM
 * sysinfo_withproperties" - forks VM
 * testij" - filters output
 * timeslice" - forks VM
 * DerbyNetAutoStart" - forks VM
 */
public class NetHarnessJavaTest extends HarnessJavaTest {
    
    /**
     * Only allow construction from our suite method.
     * 
     * @param name the name of the test to execute
     */
    private NetHarnessJavaTest(String name) {
        super(name);
     }

    /**
     * Run tests from the functionTests/tests/derbynet directory.
     */
    protected String getArea() {
        return "derbynet";
    }
    
    public static Test suite()
    {
        BaseTestSuite suite =
            new BaseTestSuite("derbynet: old harness java tests");
        
        if (!Derby.hasServer())
            return suite;

        suite.addTest(TestConfiguration.clientServerDecorator(
        		         decorate(new NetHarnessJavaTest("executeUpdate"))));

        //DERBY-2348: SECMEC 9 is available on IBM 1.4.2 and 1.5 VMs, leading
        //            to differences in output, disabling for now. While tests
        //            for security mechanism exist in NSSecurityMechanismTest,
        //            that test does not currently check the correct order of
        //            responses of secmec and secchkcd for various error cases,
        //            which is tested in ProtocolTest.
        return new SupportFilesSetup(suite,
        	           new String[] {
	                       "functionTests/tests/derbynet/excsat_accsecrd1.inc",
	                       "functionTests/tests/derbynet/excsat_accsecrd2.inc",
	                       "functionTests/tests/derbynet/excsat_secchk.inc",
	                       "functionTests/tests/derbynet/connect.inc",
	                       "functionTests/tests/derbynet/values1.inc",
	                       "functionTests/tests/derbynet/values64kblksz.inc"
	                   });
    }

}
