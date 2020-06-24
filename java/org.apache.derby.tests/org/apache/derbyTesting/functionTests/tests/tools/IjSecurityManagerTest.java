/*

   Derby - Class 
   org.apache.derbyTesting.functionTests.tests.tools.IjSecurityManager

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.io.PrintStream;
import junit.framework.Test;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestNullOutputStream;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class IjSecurityManagerTest extends BaseTestCase {

	public IjSecurityManagerTest(String name) {
		super(name);
	}

	public void testRunIJ() throws Exception {
	    /* Save the original out stream */
	    final PrintStream out = System.out;
	    
	    /* Mute the test */
        setSystemOut(new PrintStream(new TestNullOutputStream()));
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

	    try {
	        /* Run ij */
	        ij.main(new String[]{"extinout/IjSecurityManagerTest.sql"});
	    } catch (Exception e) { /* Should NEVER happen */
	        fail("Failed to run ij under security manager.",e);
	    } finally {
	        /* Restore the original out stream */
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            setSystemOut(out);
	    }
	}
	
	private static Test decorateTest() {	    
	    Test test = TestConfiguration.embeddedSuite(IjSecurityManagerTest.class);
        test = new SupportFilesSetup
         (
          test,
          null,
          new String[] { "functionTests/tests/tools/IjSecurityManagerTest.sql"  },
          null,
          new String[] { "IjSecurityManagerTest.sql"}
          );
        return test;
	}
	public static Test suite() {		
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("IjSecurityManagerTest");
		suite.addTest(decorateTest());
		return suite;
	}
}
