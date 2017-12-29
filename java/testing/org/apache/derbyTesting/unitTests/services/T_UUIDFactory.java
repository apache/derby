/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_UUIDFactory

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

package org.apache.derbyTesting.unitTests.services;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.derbyTesting.unitTests.harness.T_Generic;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import  org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.shared.common.stream.HeaderPrintWriter;

import org.apache.derby.iapi.services.uuid.UUIDFactory;

/**
	Test to ensure a implementation of the UUID module
	implements the protocol correctly. 
*/

public class T_UUIDFactory extends T_Generic {

	protected UUIDFactory factory;
	boolean resultSoFar;

	public 	T_UUIDFactory() {
		super();
	}

	protected String getModuleToTestProtocolName() {

		return "A.Dummy.Name";
	}

	/**
		Run all the tests, each test that starts with 'S' is a single user
		test, each test that starts with 'M' is a multi-user test.

		@exception T_Fail The test failed in some way.
	*/
	protected void runTests() throws T_Fail {

		factory = getMonitor().getUUIDFactory();
		if (factory == null) {
			throw T_Fail.testFailMsg(getModuleToTestProtocolName() + " module not started.");
		}

		if (!testUUID())
			throw T_Fail.testFailMsg("testUUID indicated failure");
	}


	/*
	** Tests
	*/

	protected boolean testUUID() {
		resultSoFar = true;

		UUID uuid1 = factory.createUUID();
		UUID uuid2 = factory.createUUID();

		if (uuid1.equals(uuid2)){
			// Resolve: format this with a message factory
			String message =  
				"UUID factory created matching UUIDS '%0' and '%1'";
			out.printlnWithHeader(message);
			resultSoFar =  false;
		}

		if (!uuid1.equals(uuid1)){
			// Resolve: format this with a message factory
			String message = 
				"UUID '%0' does not equal itself";
			resultSoFar =  false;
		}

		if (uuid1.hashCode() != uuid1.hashCode()){
			// Resolve: format this with a message factory
			String message = 
				"UUID '%0' does not hash to the same thing twice.";
			out.printlnWithHeader(message);
			resultSoFar =  false;
		}

		// Check that we can go from UUID to string and back.
		
		String suuid1 = uuid1.toString();
		UUID uuid3 = factory.recreateUUID(suuid1);
		if (!uuid3.equals(uuid1)){
			// Resolve: format this with a message factory
			String message = 
				"Couldn't recreate UUID: "
				+ uuid3.toString() 
				+ " != "
				+ uuid1.toString();
			out.printlnWithHeader(message);
			resultSoFar =  false;
		}

		// Check that we can transform from string to UUID and back
		// for a few "interesting" UUIDs.

		// This one came from GUIDGEN.EXE.
		testUUIDConversions(out, "7878FCD0-DA09-11d0-BAFE-0060973F0942");

		// Interesting bit patterns.
		testUUIDConversions(out, "80706050-4030-2010-8070-605040302010");
		testUUIDConversions(out, "f0e0d0c0-b0a0-9080-7060-504030201000");
		testUUIDConversions(out, "00000000-0000-0000-0000-000000000000");
		testUUIDConversions(out, "ffffffff-ffff-ffff-ffff-ffffffffffff");

		// A couple self-generated ones for good measure.
		testUUIDConversions(out, factory.createUUID().toString());
 		testUUIDConversions(out, factory.createUUID().toString());

		return resultSoFar;
	
	}

	private void testUUIDConversions(HeaderPrintWriter out, String uuidstring)
	{
		UUID uuid = factory.recreateUUID(uuidstring);
		if (!uuidstring.equalsIgnoreCase(uuid.toString())){
			// Resolve: format this with a message factory
			String message = 
				"Couldn't recreate UUID String: "
				+ uuidstring 
				+ " != " 
				+ uuid.toString();
			out.printlnWithHeader(message);
			resultSoFar =  false;
		}
	}
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

}
