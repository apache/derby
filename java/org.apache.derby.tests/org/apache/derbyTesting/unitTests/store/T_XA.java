/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_XA

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

import org.apache.derbyTesting.unitTests.harness.T_Generic;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.store.access.xa.*;
import org.apache.derby.iapi.store.access.*;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.shared.common.reference.Property;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.shared.common.error.StandardException; 

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedAction;
import java.security.AccessController;
import java.util.Properties; 

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;


public class T_XA extends T_Generic
{
    private static final String testService = "XaTest";

    byte[] global_id = 
        { 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
         10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
         20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
         30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
         40, 41, 42, 44, 44, 45, 46, 47, 48, 49,
         50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
         60, 61, 62, 63};

    byte[] branch_id = 
        { 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
         10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
         20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
         30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
         40, 41, 42, 44, 44, 45, 46, 47, 48, 49,
         50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
         60, 61, 62, 63};

    AccessFactory store = null;

	public T_XA()
    {
		super();
	}

	/*
	** Methods of UnitTest.
	*/

	/*
	** Methods required by T_Generic
	*/

	public String getModuleToTestProtocolName()
    {
		return AccessFactory.MODULE;
	}

	/**
		@exception T_Fail Unexpected behaviour from the API
	 */

	protected void runTests() throws T_Fail
	{
		// Create a AccessFactory to test.

		// don't automatic boot this service if it gets left around
		if (startParams == null) 
        {
			startParams = new Properties();
		}
		startParams.put(Property.NO_AUTO_BOOT, Boolean.TRUE.toString());
		// remove the service directory to ensure a clean run
		startParams.put(Property.DELETE_ON_CREATE, Boolean.TRUE.toString());

		// see if we are testing encryption
		startParams = T_Util.setEncryptionParam(startParams);

		try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
			store = (AccessFactory) createPersistentService(
				getModuleToTestProtocolName(), testService, startParams);
		} catch (StandardException mse) {
			throw T_Fail.exceptionFail(mse);
		}


		if (store == null) 
        {
			throw T_Fail.testFailMsg(
                getModuleToTestProtocolName() + " service not started.");
		}

		REPORT("(unitTestMain) Testing " + testService);

		try {

            XATest_1(new commit_method(store, true));
            XATest_2(new commit_method(store, true));
            XATest_3(new commit_method(store, true));
            XATest_4(new commit_method(store, true));
            XATest_5(new commit_method(store, true));
            XATest_6(new commit_method(store, true));

            XATest_1(new commit_method(store, false));
            XATest_2(new commit_method(store, false));
            XATest_3(new commit_method(store, false));
            XATest_4(new commit_method(store, false));
            XATest_5(new commit_method(store, false));
            XATest_6(new commit_method(store, false));
		}
		catch (StandardException e)
		{
			String  msg = e.getMessage();
			if (msg == null)
				msg = e.getClass().getName();
			REPORT(msg);
            e.printStackTrace();
			throw T_Fail.exceptionFail(e);
		}
        catch (Throwable t)
        {
            t.printStackTrace();
        }
	}

    /**************************************************************************
     * Utility methods.
     **************************************************************************
     */

    /**************************************************************************
     * Test Cases.
     **************************************************************************
     */

    /**
     * one phase commit xa transaction.
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
    void XATest_1(
    commit_method   commit_method)
        throws StandardException, T_Fail
    {
        REPORT("(XATest_1) starting");

        ContextManager cm = 
                getContextService().getCurrentContextManager();

        // COMMIT AN IDLE TRANSACTION.

        // Start a global transaction
        XATransactionController xa_tc = (XATransactionController) 
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

        // commit an idle transaction - using onePhase optimization.
        commit_method.commit(true, 42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        // COMMIT AN UPDATE ONLY TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(1);
		long conglomid = 
            xa_tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null,  	//column sort order - not required for heap
				null,  	//default collation
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary


        // commit an idle transaction - using onePhase optimization.
        commit_method.commit(true, 42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        // COMMIT A READ ONLY TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Open a scan on the conglomerate.
		ScanController scan1 = xa_tc.openScan(
			conglomid,
			false, // don't hold
			0,     // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        scan1.next();
        scan1.close();

        // commit an idle transaction - using onePhase optimization.
        commit_method.commit(true, 42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        REPORT("(XATest_1) finishing");
    }

    /**
     * simple two phase commit xa transaction.
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
    void XATest_2(
    commit_method   commit_method)
        throws StandardException, T_Fail
    {
        REPORT("(XATest_2) starting");
        ContextManager cm = 
                getContextService().getCurrentContextManager();

        // COMMIT AN IDLE TRANSACTION.

        // Start a global transaction
        XATransactionController xa_tc = (XATransactionController) 
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

        if (!xa_tc.isGlobal())
        {
			throw T_Fail.testFailMsg("should be a global transaction.");
        }

        // This prepare will commit the idle transaction.
        if (xa_tc.xa_prepare() != XATransactionController.XA_RDONLY)
        {
			throw T_Fail.testFailMsg(
                "prepare of idle xact did not return XA_RDONLY.");
        }

        // commit an idle transaction - using onePhase optimization.
        try 
        {
            // this should fail as the xact has been committed, so committing
            // it in 2 phase mode should fail.  This test can't be run in 
            // offline mode, no transaction will be found.  Pass null as
            // global_id to make that test not run.

            commit_method.commit(false, 42, null, null, xa_tc);

			throw T_Fail.testFailMsg(
                "A XA_RDONLY prepare-committed xact cant be 2P xa_committed.");
        }
        catch (StandardException se)
        {
            // expected exception - drop through.
        }

        // should not be able to find this global xact, it has been committed
        if (((XAResourceManager) store.getXAResourceManager()).find(
                new XAXactId(42, global_id, branch_id)) != null)
        {
			throw T_Fail.testFailMsg(
                "A XA_RDONLY prepare-committed xact should not be findable.");
        }

        // done with this xact.
        xa_tc.destroy();

        // COMMIT AN UPDATE ONLY TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(1);
		long conglomid = 
            xa_tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
                null, 	//column sort order - not required for heap
				null,  	//default collation
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

        // prepare the update xact.
        if (xa_tc.xa_prepare() != XATransactionController.XA_OK)
        {
			throw T_Fail.testFailMsg(
                "prepare of update xact did not return XA_OK.");
        }

        // commit an idle transaction - using onePhase optimization.
        commit_method.commit(false, 42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        // COMMIT A READ ONLY TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Open a scan on the conglomerate.
		ScanController scan1 = xa_tc.openScan(
			conglomid,
			false, // don't hold
			0,     // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        scan1.next();
        scan1.close();

        // This prepare will commit the idle transaction.
        if (xa_tc.xa_prepare() != XATransactionController.XA_RDONLY)
        {
			throw T_Fail.testFailMsg(
                "prepare of idle xact did not return XA_RDONLY.");
        }

        // commit an idle transaction - using onePhase optimization.
        try 
        {
            // this should fail as the xact has been committed, so committing
            // it in 2 phase mode should fail.  This test can't be run in 
            // offline mode, no transaction will be found.  Pass null as
            // global_id to make that test not run.

            commit_method.commit(false, 42, null, null, xa_tc);

			throw T_Fail.testFailMsg(
                "A XA_RDONLY prepare-committed xact cant be 2P xa_committed.");
        }
        catch (StandardException se)
        {
            // expected exception - drop through.
        }

        // should not be able to find this global xact, it has been committed
        if (((XAResourceManager) store.getXAResourceManager()).find(
                new XAXactId(42, global_id, branch_id)) != null)
        {
			throw T_Fail.testFailMsg(
                "A XA_RDONLY prepare-committed xact should not be findable.");
        }

        // done with this xact.
        xa_tc.destroy();

        REPORT("(XATest_2) finishing");
    }

    /**
     * Test aborts of unprepared xa transaction.
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
    void XATest_3(
    commit_method   commit_method)
        throws StandardException, T_Fail
    {
        REPORT("(XATest_3) starting");

        ContextManager cm = 
                getContextService().getCurrentContextManager();

        // ABORT AN IDLE TRANSACTION.

        // Start a global transaction
        XATransactionController xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

        // commit an idle transaction - using onePhase optimization.
        commit_method.rollback(42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        // ABORT AN UPDATE ONLY TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(1);
		long conglomid = 
            xa_tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, //column sort order - not required for heap
				null,  	//default collation
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary


        // commit an idle transaction - using onePhase optimization.
        commit_method.rollback(42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        // ABORT A READ ONLY TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Create a heap conglomerate.
        template_row = new T_AccessRow(1);
		conglomid = 
            xa_tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, //column sort order - not required for heap
				null,  	//default collation
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary


        // commit an idle transaction - using onePhase optimization.
        commit_method.commit(true, 42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Open a scan on the conglomerate.
		ScanController scan1 = xa_tc.openScan(
			conglomid,
			false, // don't hold
			0,     // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        scan1.next();
        scan1.close();

        // commit an idle transaction - using onePhase optimization.
        commit_method.rollback(42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        REPORT("(XATest_3) finishing");
    }

    /**
     * Test aborts of prepared two phase commit xa transaction.
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
    void XATest_4(
    commit_method   commit_method)
        throws StandardException, T_Fail
    {
        REPORT("(XATest_4) starting");

        ContextManager cm = 
                getContextService().getCurrentContextManager();
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

        // ABORT AN IDLE TRANSACTION.

        // Start a global transaction
        XATransactionController xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

        // This prepare will commit the idle transaction.
        if (xa_tc.xa_prepare() != XATransactionController.XA_RDONLY)
        {
			throw T_Fail.testFailMsg(
                "prepare of idle xact did not return XA_RDONLY.");
        }

        // nothing to do, will just abort the next current idle xact.

       // after prepare/readonly we cna continue to use transaction   
		commit_method.commit(true, 42, null, null, xa_tc);



        // should not be able to find this global xact, it has been committed
        if (((XAResourceManager) store.getXAResourceManager()).find(
                new XAXactId(42, global_id, branch_id)) != null)
        {
			throw T_Fail.testFailMsg(
                "A XA_RDONLY prepare-committed xact should not be findable.");
        }

        // done with this xact.
        xa_tc.destroy();

        // ABORT AN UPDATE ONLY TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(1);
		long conglomid = 
            xa_tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, //column sort order - not required for heap
				null,  	//default collation
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary



        // Open a scan on the conglomerate, to verify the create happened,
        // and to show that the same openScan done after abort fails.
        ScanController scan1 = xa_tc.openScan(
            conglomid,
            false, // don't hold
            0,     // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
            (FormatableBitSet) null, // all columns, all as objects
            null, // start position - first row in conglomerate
            0,    // unused if start position is null.
            null, // qualifier - accept all rows
            null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        scan1.next();
        scan1.close();

        // prepare the update xact.
        if (xa_tc.xa_prepare() != XATransactionController.XA_OK)
        {
			throw T_Fail.testFailMsg(
                "prepare of update xact did not return XA_OK.");
        }

        try
        {
            // Open a scan on the conglomerate.
            scan1 = xa_tc.openScan(
                conglomid,
                false, // don't hold
                0,     // not for update
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null, // all columns, all as objects
                null, // start position - first row in conglomerate
                0,    // unused if start position is null.
                null, // qualifier - accept all rows
                null, // stop position - last row in conglomerate
                0);   // unused if stop position is null.

            scan1.next();
            scan1.close();

			throw T_Fail.testFailMsg(
                "Should not be able to do anything on xact after prepare.");
        }
        catch (StandardException se)
        {
            // expected exception, fall through.
        }


        // commit an idle transaction - using onePhase optimization.
        commit_method.rollback(42, global_id, branch_id, xa_tc);

        commit_method.commit(true, 42, null, null, xa_tc);

        // should not be able to find this global xact, it has been committed
        if (((XAResourceManager) store.getXAResourceManager()).find(
                new XAXactId(42, global_id, branch_id)) != null)
        {
			throw T_Fail.testFailMsg(
                "A xa_rollbacked xact should not be findable.");
        }


        // done with this xact.
        xa_tc.destroy();

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

        try
        {
            // Open a scan on the conglomerate.
            scan1 = xa_tc.openScan(
                conglomid,
                false, // don't hold
                0,     // not for update
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null, // all columns, all as objects
                null, // start position - first row in conglomerate
                0,    // unused if start position is null.
                null, // qualifier - accept all rows
                null, // stop position - last row in conglomerate
                0);   // unused if stop position is null.

            scan1.next();
            scan1.close();

			throw T_Fail.testFailMsg(
                "Should not be able to open conglom, the create was aborted.");
        }
        catch (StandardException se)
        {
            // expected exception, fall through.
        }

        xa_tc.destroy();


        // ABORT A READ ONLY TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Create a heap conglomerate.
        template_row = new T_AccessRow(1);
		conglomid = 
            xa_tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, //column sort order - not required for heap
				null,  	//default collation
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

        commit_method.commit(true, 42, global_id, branch_id, xa_tc);

        xa_tc.destroy();


        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Open a scan on the conglomerate.
		scan1 = xa_tc.openScan(
			conglomid,
			false, // don't hold
			0,     // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        scan1.next();
        scan1.close();

        // This prepare will commit the idle transaction.
        if (xa_tc.xa_prepare() != XATransactionController.XA_RDONLY)
        {
			throw T_Fail.testFailMsg(
                "prepare of idle xact did not return XA_RDONLY.");
        }

        // commit an idle transaction - using onePhase optimization.
        commit_method.commit(true, 42, null, null, xa_tc);

        // should not be able to find this global xact, it has been committed
        if (((XAResourceManager) store.getXAResourceManager()).find(
                new XAXactId(42, global_id, branch_id)) != null)
        {
			throw T_Fail.testFailMsg(
                "A XA_RDONLY prepare-committed xact should not be findable.");
        }

        // done with this xact.
        xa_tc.destroy();

        REPORT("(XATest_5) finishing");
    }

    /**
     * Very simple testing of the recover() call.
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
    void XATest_5(
    commit_method   commit_method)
        throws StandardException, T_Fail
    {
        REPORT("(XATest_5) starting");

        // Should be no prepared transactions when we first start.
        if (((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMSTARTRSCAN).length != 0)
        {
			throw T_Fail.testFailMsg(
                "recover incorrectly returned prepared xacts.");
        }

        // Should be no prepared transactions when we first start.
        if (((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMNOFLAGS).length != 0)
        {
			throw T_Fail.testFailMsg("NOFLAGS should always return 0.");
        }

        ContextManager cm = 
                getContextService().getCurrentContextManager();

        // COMMIT AN IDLE TRANSACTION.

        // Start a global transaction
        XATransactionController xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

        // Should be no prepared transactions, there is one idle global xact.
        if (((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMSTARTRSCAN).length != 0)
        {
			throw T_Fail.testFailMsg(
                "recover incorrectly returned prepared xacts.");
        }

        // commit an idle transaction - using onePhase optimization.
        commit_method.commit(true, 42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        // COMMIT AN UPDATE ONLY TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(1);
		long conglomid = 
            xa_tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, //column sort order - not required for heap
				null,  	//default collation
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

        // Should be no prepared transactions, there is one update global xact.
        if (((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMSTARTRSCAN).length != 0)
        {
			throw T_Fail.testFailMsg(
                "recover incorrectly returned prepared xacts.");
        }

        // commit an idle transaction - using onePhase optimization.
        commit_method.commit(true, 42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        // COMMIT A READ ONLY TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Open a scan on the conglomerate.
		ScanController scan1 = xa_tc.openScan(
			conglomid,
			false, // don't hold
			0,     // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        scan1.next();
        scan1.close();

        // Should be no prepared transactions, there is one update global xact.
        if (((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMSTARTRSCAN).length != 0)
        {
			throw T_Fail.testFailMsg(
                "recover incorrectly returned prepared xacts.");
        }


        // commit an idle transaction - using onePhase optimization.
        commit_method.commit(true, 42, global_id, branch_id, xa_tc);

        // done with this xact.
        xa_tc.destroy();

        // PREPARE AN UPDATE TRANSACTION.

        // Start a global transaction
        xa_tc = (XATransactionController)
            store.startXATransaction(
                cm,
                42, // fake format id
                global_id,
                branch_id);

		// Create a heap conglomerate.
        template_row = new T_AccessRow(1);
		conglomid = 
            xa_tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, //column sort order - not required for heap
				null,  	//default collation
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

        // Should be no prepared transactions, there is one update global xact.
        if (((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMSTARTRSCAN).length != 0)
        {
			throw T_Fail.testFailMsg(
                "recover incorrectly returned prepared xacts.");
        }

        // prepare the update xact.
        if (xa_tc.xa_prepare() != XATransactionController.XA_OK)
        {
			throw T_Fail.testFailMsg(
                "prepare of update xact did not return XA_OK.");
        }

        try
        {
            // Open a scan on the conglomerate.
            scan1 = xa_tc.openScan(
                conglomid,
                false, // don't hold
                0,     // not for update
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null, // all columns, all as objects
                null, // start position - first row in conglomerate
                0,    // unused if start position is null.
                null, // qualifier - accept all rows
                null, // stop position - last row in conglomerate
                0);   // unused if stop position is null.

            scan1.next();
            scan1.close();

			throw T_Fail.testFailMsg(
                "Should not be able to do anything on xact after prepare.");
        }
        catch (StandardException se)
        {
            // expected exception, fall through.
        }

        // Should be no prepared transactions, there is one update global xact.
        Xid[] prepared_xacts = 
            ((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMSTARTRSCAN);

        if (prepared_xacts.length != 1)
        {
			throw T_Fail.testFailMsg(
                "recover incorrectly returned wrong prepared xacts.");
        }

        if (prepared_xacts[0].getFormatId() != 42)
			throw T_Fail.testFailMsg(
                "bad format id = " + prepared_xacts[0].getFormatId());

        byte[] gid = prepared_xacts[0].getGlobalTransactionId();

        if (!java.util.Arrays.equals(gid, global_id))
        {
			throw T_Fail.testFailMsg(
                "bad global id = " + org.apache.derbyTesting.unitTests.util.BitUtil.hexDump(gid));
        }

        byte[] bid = prepared_xacts[0].getBranchQualifier();

        if (!java.util.Arrays.equals(bid, branch_id))
        {
			throw T_Fail.testFailMsg(
                "bad branch id = " + org.apache.derbyTesting.unitTests.util.BitUtil.hexDump(bid));
        }

        if (((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMNOFLAGS).length != 0)
        {
			throw T_Fail.testFailMsg("NOFLAGS should always return 0.");
        }

        // commit a prepared transaction - using two phase.
        commit_method.commit(false, 42, global_id, branch_id, xa_tc);

        // Should be no prepared transactions, there is one update global xact.
        if (((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMSTARTRSCAN).length != 0)
        {
			throw T_Fail.testFailMsg(
                "recover incorrectly returned prepared xacts.");
        }

        // done with this xact.
        xa_tc.destroy();

        // Should be no prepared transactions, there is one update global xact.
        if (((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMSTARTRSCAN).length != 0)
        {
			throw T_Fail.testFailMsg(
                "recover incorrectly returned prepared xacts.");
        }

        REPORT("(XATest_5) finishing");

    }

    /**
     * Very simple testing of changing a local transaction to a global.
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
    void XATest_6(
    commit_method   commit_method)
        throws StandardException, T_Fail
    {
        REPORT("(XATest_5) starting");

        ContextManager cm = 
                getContextService().getCurrentContextManager();
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

        TransactionController   tc = store.getTransaction(cm);

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(1);
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, //column sort order - not required for heap
				null,  	//default collation
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

        tc.commit();

        // COMMIT AN IDLE TRANSACTION.

        // Start a global transaction
        XATransactionController xa_tc = (XATransactionController)
            tc.createXATransactionFromLocalTransaction(
                42, // fake format id
                global_id,
                branch_id);

        if (!xa_tc.isGlobal())
        {
			throw T_Fail.testFailMsg("should be a global transaction.");
        }


		// Open a scan on the conglomerate.
		ScanController scan1 = xa_tc.openScan(
			conglomid,
			false, // don't hold
			0,     // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.


        scan1.next();
        scan1.close();


		// Create a heap conglomerate.
        template_row = new T_AccessRow(1);
		conglomid = 
            xa_tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, //column sort order - not required for heap
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
				null,  	//default collation
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

        // Should be no prepared transactions, there is one update global xact.
        if (((XAResourceManager) store.getXAResourceManager()).recover(
                XAResource.TMSTARTRSCAN).length != 0)
        {
			throw T_Fail.testFailMsg(
                "recover incorrectly returned prepared xacts.");
        }

        // prepare the update xact.
        if (xa_tc.xa_prepare() != XATransactionController.XA_OK)
        {
			throw T_Fail.testFailMsg(
                "prepare of update xact did not return XA_OK.");
        }

        try
        {
            // Open a scan on the conglomerate.
            scan1 = xa_tc.openScan(
                conglomid,
                false, // don't hold
                0,     // not for update
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null, // all columns, all as objects
                null, // start position - first row in conglomerate
                0,    // unused if start position is null.
                null, // qualifier - accept all rows
                null, // stop position - last row in conglomerate
                0);   // unused if stop position is null.

            scan1.next();
            scan1.close();

			throw T_Fail.testFailMsg(
                "Should not be able to do anything on xact after prepare.");
        }
        catch (StandardException se)
        {
            // expected exception, fall through.
        }

        // commit a prepared transaction - using two phase.
        commit_method.commit(false, 42, global_id, branch_id, xa_tc);

        xa_tc.destroy();

        REPORT("(XATest_6) finishing");
    }

    
    /**
     * Privileged lookup of the ContextService. Package protected so that user code
     * can't call this entry point.
     */
    static  ContextService    getContextService()
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

class commit_method
{
    private boolean         online_xact;
    private AccessFactory   store;

    public commit_method(
    AccessFactory   store,
    boolean         online_xact)
    {
        this.store       = store;
        this.online_xact = online_xact;
    }

    public void commit(
    boolean                 one_phase,
    int                     format_id,
    byte[]                  global_id,
    byte[]                  branch_id,
    XATransactionController xa_tc)
        throws StandardException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT((global_id != null) || (xa_tc != null));

        boolean local_online_xact = online_xact;

        if (global_id == null)
            local_online_xact = true;
        if (xa_tc == null)
            local_online_xact = false;
            
        if (local_online_xact)
        {
            xa_tc.xa_commit(one_phase);
        }
        else
        {
            Xid xid = new XAXactId(format_id, global_id, branch_id);

            ContextManager cm = 
                ((XAResourceManager) store.getXAResourceManager()).find(xid);

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(cm != null, "could not find xid = " + xid);

                SanityManager.ASSERT(
                    cm == 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                    T_XA.getContextService().getCurrentContextManager(),
                    "cm = " + cm +
                    "current = " + 
                        T_XA.getContextService().getCurrentContextManager());
            }

            ((XAResourceManager) store.getXAResourceManager()).commit(
                cm, xid, one_phase);
        }
    }

    public void rollback(
    int                     format_id,
    byte[]                  global_id,
    byte[]                  branch_id,
    XATransactionController xa_tc)
        throws StandardException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT((global_id != null) || (xa_tc != null));

        boolean local_online_xact = online_xact;

        if (global_id == null)
            local_online_xact = true;
        if (xa_tc == null)
            local_online_xact = false;
            
        if (local_online_xact)
        {
            xa_tc.xa_rollback();
        }
        else
        {
            Xid xid = new XAXactId(format_id, global_id, branch_id);

            ContextManager cm = 
                ((XAResourceManager) store.getXAResourceManager()).find(xid);

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(cm != null, "could not find xid = " + xid);

                SanityManager.ASSERT(
                    cm == 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                    T_XA.getContextService().getCurrentContextManager(),
                    "cm = " + cm +
                    "current = " + 
                        T_XA.getContextService().getCurrentContextManager());
            }

            ((XAResourceManager) store.getXAResourceManager()).rollback(
                cm, xid);
        }
    }

}
