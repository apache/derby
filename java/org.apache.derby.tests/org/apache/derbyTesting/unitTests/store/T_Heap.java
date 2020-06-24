/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_Heap

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

package org.apache.derbyTesting.unitTests.store;

// impl imports are the preferred way to create unit tests.
import org.apache.derbyTesting.unitTests.harness.T_Generic;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.impl.store.access.heap.*;

import java.util.Properties;

import java.io.PrintWriter;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.stream.HeaderPrintWriter;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.shared.common.reference.Property;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.AccessController;
import java.util.Properties;

public class T_Heap extends T_Generic
{
	private static final String testService = "heapTest";
	/*
	** Methods required by T_Generic
	*/

	public String getModuleToTestProtocolName() {
		return AccessFactory.MODULE;
	}

	/**
		@exception T_Fail test failed.
	*/
	protected void runTests() throws T_Fail
	{
		AccessFactory store = null;
		TransactionController tc = null;
		boolean pass = false;

        out.println("executing heap test");

		// don't automatic boot this service if it gets left around
		if (startParams == null) {
			startParams = new Properties();
		}
		startParams.put(Property.NO_AUTO_BOOT, Boolean.TRUE.toString());
		// remove the service directory to ensure a clean run
		startParams.put(Property.DELETE_ON_CREATE, Boolean.TRUE.toString());

		// see if we are testing encryption
		startParams = T_Util.setEncryptionParam(startParams);

		try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
			store = (AccessFactory) createPersistentService(getModuleToTestProtocolName(),
			testService, startParams);
		} catch (StandardException mse) {
			throw T_Fail.exceptionFail(mse);
		}

		if (store == null) {
			throw T_Fail.testFailMsg(getModuleToTestProtocolName() + " service not started.");
		}
		REPORT("(unitTestMain) Testing " + testService);

		try {

            tc = store.getTransaction(
                    getContextService().getCurrentContextManager());
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

            if (t_001(tc))
			{
				pass = true;
			}

			tc.commit();
			tc.destroy();
		}
		catch (StandardException e)
		{
            System.out.println("got an exception.");
			String  msg = e.getMessage();
			if (msg == null)
				msg = e.getClass().getName();
			REPORT(msg);
			throw T_Fail.exceptionFail(e);
		}

		if (!pass)
			throw T_Fail.testFailMsg("T_Heap test failed");
	}

    /*
     * Test Qualifiers.
     */
    protected boolean t_001(TransactionController tc)
        throws StandardException, T_Fail
    {
        REPORT("Starting t_001");

        T_QualifierTest q_test = 
            new T_QualifierTest(
                "heap",         // create a heap
                null,           // properties
                false,          // not temporary
                out,
                T_QualifierTest.ORDER_NONE);         // unordered data

        boolean test_result = q_test.t_testqual(tc);

        if (!test_result)
            throw T_Fail.testFailMsg("T_Heap.t_001 failed");

        REPORT("Ending t_001");

        return(test_result);
    }
    
    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ContextService    getContextService()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ContextService>()
             {
                 public ContextService run()
                 {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                     return ContextService.getFactory();
                 }
             }
             );
    }

    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object createPersistentService( final String factoryInterface, final String serviceName, final Properties properties ) 
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.createPersistentService( factoryInterface, serviceName, properties );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

}
