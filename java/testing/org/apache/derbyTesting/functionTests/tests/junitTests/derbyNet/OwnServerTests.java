/*

Derby - Class org.apache.derbyTesting.functionTests.tests.junitTests.derbyNet.OwnServerTests

Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derbyTesting.functionTests.tests.junitTests.derbyNet;

import org.apache.derbyTesting.functionTests.util.BaseTestCase;
import org.apache.derbyTesting.functionTests.tests.derbynet.SuicideOfStreaming;
import org.apache.derby.iapi.services.sanity.SanityManager;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * This class deals tests which starts its own Network Server in JUnit framework.
 */

public class OwnServerTests extends BaseTestCase {
    
    public OwnServerTests(){
	super("ownServerTests");
    }
    
    public static Test suite(){
	
	TestSuite suite = new TestSuite();
	
	if(SanityManager.DEBUG){
	    suite.addTest( new SuicideOfStreaming() );
	}
	
	return suite;
	
    }
    
}
