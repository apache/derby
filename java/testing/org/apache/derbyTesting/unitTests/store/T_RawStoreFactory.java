/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_RawStoreFactory

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

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.raw.*;

// impl imports are the preferred way to create unit tests.
import org.apache.derbyTesting.unitTests.harness.T_MultiThreadedIterations;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.locks.*;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.*;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.types.DataValueDescriptor;

// impl a logInstant implemented as log counter to test truncateLWMs
import org.apache.derby.impl.store.raw.log.LogCounter;

import org.apache.derby.iapi.types.SQLChar;

import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.io.*;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.AccessController;
import java.util.Properties;

/**
	A protocol unit test for the RawStore interface.
*/

public class T_RawStoreFactory extends T_MultiThreadedIterations {


	static protected final String REC_001 = "McLaren";
	static protected final String REC_002 = "Ferrari";
	static protected final String REC_003 = "Benetton";
	static protected final String REC_004 = "Prost";
	static protected final String REC_005 = "Tyrell";
	static protected final String REC_006 = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
	static protected final String REC_007 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
	static protected final String REC_008 = "z";
	static protected final String REC_009 = "nanonano";
	static protected final String REC_010 = "fuzzbutt";
	static protected final String REC_011 = "mork";
	static protected final String REC_012 = "orson";
	static protected final String REC_013 = "mindy";
	static protected final String REC_014 = "thomas";
	static protected final String REC_015 = "henry";
	static protected final String REC_016 = "gordon";
	static protected final String REC_017 = "mavis";
	static protected final String REC_018 = "fatcontroller";
	static protected final String REC_UNDO = "Lotus";
	static protected final String REC_NULL = "NULL";

	static final FormatableBitSet BS_COL_0 = new FormatableBitSet(1);

	static protected final String SP1 = "savepoint1";
	static protected final String SP2 = "savepoint2";

	private static final String TEST_ROLLBACK_OFF = "derby.RawStore.RollbackTestOff";

	private static boolean testRollbackProperty;// initialize in start
	static protected boolean testRollback; // each thread has its own test rollback value

	static protected RawStoreFactory	factory;
	static protected LockFactory  lf;
	static protected ContextService contextService;

	static protected UUIDFactory uuidfactory;
	protected  T_Util t_util;
	protected int    openMode;	// mode flags used in all open containers.
	protected boolean logDataForPurges = true; //used to test non-logged data purges

	public T_RawStoreFactory() {
		super();
		BS_COL_0.set(0);
	}

	/**
	  @exception StandardException cannot startup the context service
	*/
	public void boot(boolean create, Properties startParams)
		 throws StandardException
	{
		super.boot(create, startParams);
		contextService = getContextService();
	}

	/*
	** Methods required by T_Generic
	*/

	protected String getModuleToTestProtocolName() {
		return RawStoreFactory.MODULE;
	}

	/**
		Set up test

		@exception T_Fail Unexpected behaviour from the API
	 */
	protected void setupTest() throws T_Fail {
		
		String rollbackOff = PropertyUtil.getSystemProperty(TEST_ROLLBACK_OFF);
		testRollback = !Boolean.valueOf(rollbackOff).booleanValue();

		testRollbackProperty = testRollback; // testRollbackProperty never changes

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

			factory = (RawStoreFactory) createPersistentService(getModuleToTestProtocolName(),
								getTestService(), startParams);

			if (factory == null) {
				throw T_Fail.testFailMsg(getModuleToTestProtocolName() + " service not started.");
			}

			lf = factory.getLockFactory();
			if (lf == null) {
				throw T_Fail.testFailMsg("LockFactory.MODULE not found");
			}

			uuidfactory = getMonitor().getUUIDFactory();

		} catch (StandardException mse) {
			throw T_Fail.exceptionFail(mse);
		}

		REPORT("testRollback=" + testRollback);

		return;
	}

	protected String getTestService()
	{
		return "rawStoreTest";
	}


	/**
	 * T_MultiThreadedIteration method
	 *
	 * @exception T_Fail Unexpected behaviour from the API
	 */
	protected void joinSetupTest() throws T_Fail {

		T_Fail.T_ASSERT(factory != null, "raw store factory not setup ");
		T_Fail.T_ASSERT(lf != null, "Lock factory not setup ");
		T_Fail.T_ASSERT(contextService != null, "Context service not setup ");

		testRollback = testRollbackProperty;

	}

	protected T_MultiThreadedIterations newTestObject() {
		try
		{
			Class<?> thisClass = this.getClass();
			return (T_MultiThreadedIterations)(thisClass.getConstructor().newInstance());
		}
		catch (InstantiationException ie)
		{
			return new T_RawStoreFactory();
		}	
		catch (IllegalAccessException iae)
		{
			return new T_RawStoreFactory();
		}
		catch (NoSuchMethodException iae)
		{
			return new T_RawStoreFactory();
		}
		catch (java.lang.reflect.InvocationTargetException iae)
		{
			return new T_RawStoreFactory();
		}
	}	


	/**
	  run the test

	  @exception T_Fail Unexpected behaviour from the API
	*/
	protected void runTestSet() throws T_Fail {

		// get a utility helper
		t_util = new T_Util(factory, lf, contextService);

		ContextManager cm1 = contextService.newContextManager();
		contextService.setCurrentContextManager(cm1);

		try {


			// Run the tests with data not logged for purges.
			REPORT("Running tests with no data logged  for purges");
			openMode = 0;		// logged by default
			runPurgeWithNoDataLoggesTests();

			// Run the tests in normal logged mode
			REPORT("Running tests with logging requested");
			openMode = 0;		// logged by default
			runEachTest();

			
			// run the tests on temp tables
			REPORT("Running tests for temp tables");
			testRollback = false;	// obviously, we can't test rollback if we are not logging
			runTempTests();

			// Run the tests in unlogged mode
			REPORT("Running tests in unlogged mode");
			openMode = ContainerHandle.MODE_UNLOGGED | ContainerHandle.MODE_CREATE_UNLOGGED;
			testRollback = false;	// obviously, we can't test rollback if we are not logging
			runEachTest();

			// if more runs are added here then you probably want to reset testRollback to
			// its initial value, or add the runs before the unlogged mode.

		} catch (StandardException se) {

            //Assume database is not active. DERBY-4856 thread dump
            cm1.cleanupOnError(se, false);
			throw T_Fail.exceptionFail(se);
		}
		finally {
			contextService.resetCurrentContextManager(cm1);
		}
	}

	protected void runEachTest() throws T_Fail, StandardException {

		t_util.setOpenMode(openMode);

		// Transaction tests
 		T000();
		T001();
		T002();
		T003();
		T004();
		T005();
		T006();
		T007();
		T008();
		T009();
		T010();
		T011();
        T012();

		// ContainerHandle tests
		C010(0);
		C011();
		C012(1);
		C014();
		C200();
		C201(0);
		C201(1);

		// Page tests
		P001(0);
		P002(0);
		P005(0);
		P006();
		P007(0);
		P008(0);
		P009(0);
		P011(0);
		P012(0);
		P013();
		P014();
		P015();
		P016();
		P017();
		P018();
		P019();		// test addPage with preallocation turned on
		P020();		// test createContainer with initialPage set to 10 pages
		P021();		// test preAllocate
		P022();
		P023(0);	// overflowThreshold test
		P024(0);	// test page latching

		// long row tests
		P030(0);
		P031(0);
		P032(0);
		P033(0);
		P034(0);

		P035(0);	// long column test



		//run  the following test because they do lot of checks 
		//on rollbacking when contyainer is unlogged nothing is rolled back
		if((openMode & ContainerHandle.MODE_UNLOGGED) ==  ContainerHandle.MODE_UNLOGGED)
		{
			openMode = 0; //run them as logged for time being
			t_util.setOpenMode(openMode);
		}

 		// reclaiming space from long column and long rows - temp container
		// row space is not reclaimed.
		P036();
		P037();
		P038();
		P039();
		P040();
		P041();
		P042();
		P043();


		P050(); // rollback tests
		P051();
		P053();
		P054();
        P055(0);
        P056(0);


		P061(); // sparse row test

        P071(); // serializable column test

		// update/update partial tests with long rows
		P701(0);
        P702(0);
		P703(0);
		P704(0);
		P705(0);

		P706(0, false);
		P706(0, true);
		P707(0);
		P708(0, false);
		P708(0, true);


		L001();	// large log record test

		// checkpoint test
		CP001();
	}

	protected void runTempTests() throws T_Fail, StandardException {

		REPORT("Thread " + threadNumber + " entering temp tests ");

		openMode = 0;			// logged by default
		t_util.setOpenMode(openMode);	// logged mode should be overriden for temp tables

		// now tests for temporary tables

		C010(ContainerHandle.TEMPORARY_SEGMENT);
		C012(ContainerHandle.TEMPORARY_SEGMENT);
		//P001(ContainerHandle.TEMPORARY_SEGMENT);
		//P002(ContainerHandle.TEMPORARY_SEGMENT);
		P005(ContainerHandle.TEMPORARY_SEGMENT);
		P011(ContainerHandle.TEMPORARY_SEGMENT);
		P012(ContainerHandle.TEMPORARY_SEGMENT);
		P030(ContainerHandle.TEMPORARY_SEGMENT);

		// update/update partial tests with long rows
		P055(ContainerHandle.TEMPORARY_SEGMENT);
		P056(ContainerHandle.TEMPORARY_SEGMENT);

		P701(ContainerHandle.TEMPORARY_SEGMENT);
		P702(ContainerHandle.TEMPORARY_SEGMENT);
		P703(ContainerHandle.TEMPORARY_SEGMENT);
		P704(ContainerHandle.TEMPORARY_SEGMENT);
		P705(ContainerHandle.TEMPORARY_SEGMENT);
		P706(ContainerHandle.TEMPORARY_SEGMENT, false);
		P706(ContainerHandle.TEMPORARY_SEGMENT, true);
		P707(ContainerHandle.TEMPORARY_SEGMENT);

		// tests specific to temp tables

		// checking truncate at commit/rollback works
		TC001();
		TC002(ContainerHandle.MODE_TRUNCATE_ON_COMMIT, true);
		TC002(ContainerHandle.MODE_TRUNCATE_ON_COMMIT, false);
		TC002(0, false);

		// checking an explict drop works ...
		TC003(ContainerHandle.MODE_TRUNCATE_ON_COMMIT, true);
		TC003(ContainerHandle.MODE_TRUNCATE_ON_COMMIT, false);
		TC003(0, false);
		TC003(0, true);
		TC003(ContainerHandle.MODE_DROP_ON_COMMIT, true);
		TC003(ContainerHandle.MODE_DROP_ON_COMMIT, false);

		// various combinations of opens ...
		TC004all();

		REPORT("Thread " + threadNumber + " exiting temp tests ");
 	}


	protected void runPurgeWithNoDataLoggesTests() throws T_Fail, StandardException {

		REPORT("Thread " + threadNumber + " entering purges with no data logged tests ");
		logDataForPurges = false;
		P005(0);
		P006();
		P014();
		P036();
		P037();
		P709();
		P710();
		P711();
		REPORT("Thread " + threadNumber + " exiting purge with no data logged tests ");
		logDataForPurges = true;
	}
	/*
	** The tests
	**		Tnnn indicates a test that is mainly testing the Transaction interface
	**		Cnnn indicates a test that is mainly testing the ContainerHandle interface
	**		Pnnn indicates a test that is mainly testing the Page interface
	**
	**	nnn < 200 tends to indicate purely API tests, ie checking methods
	**		are callable and return the right value. This includes negative tests.
	**
	**  nnn >= 200 tends to indicate more involved tests, ie ones that test the
	**			methods actually did something.

	*/

	/**
		T000 - ensure a transaction starts out idle.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void T000() throws T_Fail, StandardException {

		Transaction t1 = t_util.t_startTransaction();

		t1.close();


		t1 = t_util.t_startInternalTransaction();

		t1.close();

		t1 = t_util.t_startTransaction();
		Transaction ti = t_util.t_startInternalTransaction();

		ti.close();

		t1.close();

		PASS("T000");
	}

	/**
		T001 - start and commit an empty transaction.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void T001() throws T_Fail, StandardException {

		Transaction t1 = t_util.t_startTransaction();

		t_util.t_commit(t1);

		t1.close();

		PASS("T001");
	}

	/**
		T002 - start and abort an empty transaction.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void T002() throws T_Fail, StandardException {

		Transaction t1 = t_util.t_startTransaction();

		t_util.t_abort(t1);

		t1.close();

		PASS("T002");
	}

	/**
		T003 - start and commit an empty transaction and then ensure
		that the transaction remains open for another commit.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void T003() throws T_Fail, StandardException {

		Transaction t1 = t_util.t_startTransaction();

		t_util.t_commit(t1);

		t_util.t_commit(t1);
		t_util.t_abort(t1);

		t1.close();

		PASS("T003");
	}

	/**
		T004 - start and abort an empty transaction and then ensure
		that the transaction remains open for a commit and another abort.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void T004() throws T_Fail, StandardException {

		Transaction t1 = t_util.t_startTransaction();

		t_util.t_abort(t1);

		t_util.t_commit(t1);

		t_util.t_abort(t1);

		t1.close();

		PASS("T004");
	}


	/**
		T005 check transaction identifiers on idle transactions.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/
	protected void T005() throws T_Fail, StandardException {

		Transaction t1 = t_util.t_startTransaction();

        // local transactions do not have global id's
		GlobalTransactionId id1 = t1.getGlobalId();
		if (id1 != null)
			throw T_Fail.testFailMsg("null not returned from local Transaction.getId()");
        t1.close();

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

        t1 = t_util.t_startGlobalTransaction(42, global_id, branch_id);

        id1 = t1.getGlobalId();

		if (!id1.equals(id1))
			throw T_Fail.testFailMsg("TransactionId does not compare equal to itself");

		if (!id1.equals(t1.getGlobalId()))
			throw T_Fail.testFailMsg("TransactionId has changed without any activity on Transaction");

		if (id1.equals(this))
			throw T_Fail.testFailMsg("TransactionId compared equal to an non-transaction id object");

		t1.close();
		t1 = null;

        // change the branch_id for the second global xact.
        branch_id[63] = 82;
		Transaction t2 = 
            t_util.t_startGlobalTransaction(42, global_id, branch_id);

		GlobalTransactionId id2 = t2.getGlobalId();
		if (id2 == null)
			throw T_Fail.testFailMsg("null returned from Transaction.getId()");

		if (id1.equals(id2))
			throw T_Fail.testFailMsg("TransactionId's returned equal from different transactions");
		if (id2.equals(id1))
			throw T_Fail.testFailMsg("TransactionId's returned equal from different transactions");

		t2.close();

		PASS("T005");
	}

	/**
		T006 - savepoint basic API testing

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void T006() throws T_Fail, StandardException {	

		Transaction t1 = t_util.t_startTransaction();

		// check a random savepoint name is not accepted
		t_util.t_checkInvalidSavePoint(t1, "sdfjsdfg");


		t1.setSavePoint(SP1, null);

		t1.rollbackToSavePoint(SP1, null);	// leaves savepoint around
		t1.rollbackToSavePoint(SP1, null);	// therefore this should work

		t1.releaseSavePoint(SP1, null);

		// SP1 should no longer exist
		t_util.t_checkInvalidSavePoint(t1, SP1);

		// should be able to re-use it ...
		t1.setSavePoint(SP1, null);
		t1.rollbackToSavePoint(SP1, null);	// leaves savepoint around
		t1.rollbackToSavePoint(SP1, null);	// therefore this should work

		t1.releaseSavePoint(SP1, null);
		t_util.t_checkInvalidSavePoint(t1, SP1);

		t_util.t_commit(t1);
		t1.close();

		PASS("T006");
	}


	/**
		T007 - savepoint nesting testing

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void T007() throws T_Fail, StandardException {	

		Transaction t1 = t_util.t_startTransaction();
		int position = 0;

		/*
		** Push two save points and release the first, both should disappear
		*/
		t1.setSavePoint(SP1, null);
		t1.setSavePoint(SP2, null);

		position = t1.releaseSavePoint(SP1, null);
		if (position  != 0)
			throw T_Fail.testFailMsg("Save Point Position in the stack isincorrect:"+
									 position);


		// SP1 and SP2 should no longer exist
		t_util.t_checkInvalidSavePoint(t1, SP1);
		t_util.t_checkInvalidSavePoint(t1, SP2);

		/*
		** Push two save points and remove the second, first should remain
		*/
		t1.setSavePoint(SP1, null);
		t1.setSavePoint(SP2, null);

		t1.rollbackToSavePoint(SP2, null);	// leaves savepoint around
		position = t1.rollbackToSavePoint(SP2, null);	// therefore this should work
		
		if (position  != 2)
			throw T_Fail.testFailMsg("Save Point Position in the stack isincorrect:"+
									 position);



		position = t1.releaseSavePoint(SP2, null);
		if (position  != 1)
			throw T_Fail.testFailMsg("Save Point Position in the stack is incorrect:"+
									 position);


		t_util.t_checkInvalidSavePoint(t1, SP2);

		t1.rollbackToSavePoint(SP1, null);	// this is the main test

		t1.releaseSavePoint(SP1, null);
		t_util.t_checkInvalidSavePoint(t1, SP1);

		/*
		** Push two save points and rollback to the first, the second should disappear
		*/
		t1.setSavePoint(SP1, null);
		t1.setSavePoint(SP2, null);

		position = t1.rollbackToSavePoint(SP1, null);	// leaves SP1, removes SP2
		if (position  != 1)
			throw T_Fail.testFailMsg("Save Point Position in the stack is incorrect:"+
												 position);

		
		t_util.t_checkInvalidSavePoint(t1, SP2);
		t1.rollbackToSavePoint(SP1, null);

		t1.releaseSavePoint(SP1, null);

		t_util.t_commit(t1);
		t1.close();

		PASS("T007");
	}

	/**
		T008 - savepoint  testing, ensure save points disappear at commit or abort.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void T008() throws T_Fail, StandardException {	

		Transaction t1 = t_util.t_startTransaction();
		int position1 = 0;
		int position2 = 0;

		position1 = t1.setSavePoint(SP1, null);
		position2 = t1.setSavePoint(SP2, null);

		if (position1 != 1 && position2 != 2)
			throw T_Fail.testFailMsg("Save Point Position in the Stack seeme to wrong");

		t1.commit();

		t_util.t_checkInvalidSavePoint(t1, SP1);
		t_util.t_checkInvalidSavePoint(t1, SP2);

		position1 = t1.setSavePoint(SP1, null);
		position2 = t1.setSavePoint(SP2, null);
		
		if (position1 != 1 && position2 != 2)
			throw T_Fail.testFailMsg("Save Point Position in the Stack seeme to wrong");

		t1.abort();
		position1 = t1.setSavePoint(SP1, null);
		position2 = t1.setSavePoint(SP2, null);
		if (position1 != 1 && position2 != 2)
			throw T_Fail.testFailMsg("Save Point Position in the Stack seeme to wrong");
		t1.abort();
		t_util.t_checkInvalidSavePoint(t1, SP1);
		t_util.t_checkInvalidSavePoint(t1, SP2);

		t1.close();

		PASS("T008");
	}


	/**
		T009 - add a container and remove it within the same transaction.
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/

	protected void T009() throws StandardException, T_Fail {
		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, 0, 4096);
		t_util.t_dropContainer(t, 0, cid);

		ContainerKey id = new ContainerKey(0, cid);
		ContainerHandle ch = t.openContainer(id, ContainerHandle.MODE_READONLY);
		if (ch != null)
			throw T_Fail.testFailMsg("Dropped Container should not open");

		t_util.t_commit(t);

		t.close();

		PASS("T009");
	}

	/**
		T010 - add a container with a default size and remove it within the same transaction.
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/

	protected void T010() throws StandardException, T_Fail {
		Transaction t1 = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t1, 0);

		t_util.t_dropContainer(t1, 0, cid);

		ContainerKey id = new ContainerKey(0, cid);
		ContainerHandle ch = t1.openContainer(id, ContainerHandle.MODE_READONLY);
		if (ch != null)
			throw T_Fail.testFailMsg("Dropped Container should not open");

		t_util.t_commit(t1);

		t1.close();

		PASS("T010");
	}

	/**
		T011 - see that a container remains open over the commit of an open transaction..
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/

	protected void T011() throws StandardException, T_Fail {

		Transaction t = t_util.t_startInternalTransaction();


		long cid = t_util.t_addContainer(t, 0);

		ContainerHandle c;

		c = t_util.t_openContainer(t, 0, cid, true);

		t.commit();

		// container should still be open
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		t.commit();

		// page should still be latched
		if (!page.isLatched())
			throw T_Fail.testFailMsg("page not latched after commit of internal transaction");

		page.unlatch();
		c.close();

		t.commit();

		c = t_util.t_openContainer(t, 0, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// container and page should be closed
		t.abort();
		if (page.isLatched())
			throw T_Fail.testFailMsg("page latched after abort of internal transaction");

		try {
			page = t_util.t_getLastPage(c);
			throw T_Fail.testFailMsg("container open after abort of internal transaction");

		} catch (StandardException te) {
		}

		t_util.t_dropContainer(t, 0, cid);	// cleanup

		t_util.t_commit(t);

		t.close();

		PASS("T011");	
	}

	/**
		Test Xact.makeRecordHandle()

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void T012() throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, 0);

		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		RecordHandle r1, r2;
		RecordHandle new_r1, new_r2;

		T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
		T_RawStoreRow row2 = new T_RawStoreRow(REC_002);

		r1 = t_util.t_insertAtSlot(page, 0, row1);
        new_r1 = c.makeRecordHandle(r1.getPageNumber(), r1.getId());

		t_util.t_checkFetch(page, new_r1, REC_001);

		r2 = t_util.t_insertAtSlot(page, 1, row2);

		if (r2 != null)
        {
            new_r2 = 
                c.makeRecordHandle(r2.getPageNumber(), r2.getId());
			t_util.t_checkFetch(page, r2, REC_002);
        }

		t_util.t_commit(t);
		t.close();

		PASS("T012");

	}

	/**
		C010 - Create a container within a transaction, commit and the re-open
		the container twice.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/
	protected void C010(int segment) throws T_Fail, StandardException {

		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, segment);

		t_util.t_commit(t);
	
		ContainerHandle c1, c2;

		c1 = t_util.t_openContainer(t, segment, cid, true);
		c1 = t_util.t_openContainer(t, segment, cid, true);
		t_util.t_dropContainer(t, segment, cid);	// cleanup


		t_util.t_commit(t);
		t.close();

		PASS("C010");

	}

	/**
		C011 - Create a container withina transaction, commit and the re-open
		the container in update and non-update mode.
		
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/
	protected void C011() throws T_Fail, StandardException {

		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, 0);

		t_util.t_commit(t);
	
		ContainerHandle c1, c2;

		c1 = t_util.t_openContainer(t, 0, cid, false);
		c1 = t_util.t_openContainer(t, 0, cid, true);

		t_util.t_dropContainer(t, 0, cid);	// cleanup

		t_util.t_commit(t);
		t.close();
		PASS("C011");

	}

	/**
		C012 - Drop a container within a transaction, commit, see that it is deleted.
		Drop a container within a transaction, rollback and re-open and see
		that it is not deleted. 

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Standard Derby error policy
	*/
	protected void C012(long segment) throws T_Fail, StandardException {

		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, segment);
		t_util.t_commit(t);

		ContainerHandle c1 = t_util.t_openContainer(t, segment, cid, true);

		t_util.t_dropContainer(t, segment, cid);

		if (testRollback)
		{
			t_util.t_abort(t);			// this should rollback the drop
			c1 = t_util.t_openContainer(t, segment, cid, true);

			REPORT("rollback of drop container tested");

			t_util.t_dropContainer(t, segment, cid);
		}

		t_util.t_commit(t);

		ContainerKey id = new ContainerKey(segment, cid);
		c1 = t.openContainer(id, (ContainerHandle.MODE_FORUPDATE | openMode));	// this should fail
		if (c1 != null)
			throw T_Fail.testFailMsg("Deleted Container should fail to open");

		t_util.t_commit(t);
		t.close();
		PASS("C012");
	}

	/**
		C014 - Open a container for locking only.
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Standard Derby error policy
	*/
	protected void C014() throws T_Fail, StandardException {

		Transaction t = t_util.t_startTransaction();

		ContainerKey id = new ContainerKey(77, 45);
		ContainerHandle c = t.openContainer(id,
			ContainerHandle.MODE_OPEN_FOR_LOCK_ONLY);

		if (c == null)
			throw T_Fail.testFailMsg("open of a container for lock only failed.");

		RecordHandle rh1 = c.makeRecordHandle(23, 456);
		if (rh1 == null)
			throw T_Fail.testFailMsg("makeRecordHandle returned null");
		c.getLockingPolicy().lockRecordForRead(t, c, rh1, true, true);


		RecordHandle rh2 = c.makeRecordHandle(23, 7);
		if (rh2 == null)
			throw T_Fail.testFailMsg("makeRecordHandle returned null");
		c.getLockingPolicy().lockRecordForRead(t, c, rh2, true, false);

		RecordHandle rh3 = c.makeRecordHandle(23, 9);
		c.getLockingPolicy().lockRecordForWrite(t, rh3, false, true);
		if (rh3 == null)
			throw T_Fail.testFailMsg("makeRecordHandle returned null");

		c.getLockingPolicy().unlockRecordAfterRead(t, c, rh2, false, true);

		c.close();

		t.commit();

		t.close();

		PASS("C014");
	}

	/**
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void C200() throws T_Fail, StandardException {

		Transaction t1 = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t1, 0);

		t_util.t_commit(t1);

		ContainerHandle c1;
		Page lastPage;
		RecordHandle rh001, rh002, rh003;
		T_RawStoreRow row;
	
		REPORT("see if the container can be opened again");
		c1 = t_util.t_openContainer(t1, 0, cid, false);

		c1.close();
		t_util.t_commit(t1);

		REPORT("insert a record into the container.");

		c1 = t_util.t_openContainer(t1, 0, cid, true);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Could get container's last page");

		if (lastPage.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
			throw T_Fail.testFailMsg("Initial page must be " + ContainerHandle.FIRST_PAGE_NUMBER + ", is " + lastPage.getPageNumber());

		row = new T_RawStoreRow(REC_001);
		if (!lastPage.spaceForInsert())
			throw T_Fail.testFailMsg("No room for record on page");

		rh001 = t_util.t_insert(lastPage, row);
		if (rh001 == null)
			throw T_Fail.testFailMsg("Failed to insert record");

		// see if we can fetch that record
		t_util.t_checkFetch(lastPage, rh001, REC_001);

		lastPage.unlatch();
		lastPage = null;

		t_util.t_commit(t1);
		c1 = null;


		REPORT("read record just inserted.");

		c1 = t_util.t_openContainer(t1, 0, cid, false);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Could get container's last page");

		t_util.t_checkFetch(lastPage, rh001, REC_001);
		t_util.t_checkFetchFirst(lastPage, REC_001);
		t_util.t_checkFetchLast(lastPage, REC_001);

		t_util.t_commit(t1);
		lastPage = null;
		c1 = null;


		REPORT("insert 2 more records.");

		c1 = t_util.t_openContainer(t1, 0, cid, true);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Could get container's last page");

		if (lastPage.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
			throw T_Fail.testFailMsg("Initial page must be " + ContainerHandle.FIRST_PAGE_NUMBER + ", is " + lastPage.getPageNumber());

		row = new T_RawStoreRow(REC_002);
		if (!lastPage.spaceForInsert())
			throw T_Fail.testFailMsg("No room for record on page");

		if (!lastPage.recordExists(rh001, false))
			throw T_Fail.testFailMsg("Record 001 has vanished");

		//

		// RESOLVE: just insert them for now, order is 002,001,003
		// 001 is already on the page

		rh002 = t_util.t_insertAtSlot(lastPage, 0, row);
		row = new T_RawStoreRow(REC_003);
		rh003 = t_util.t_insert(lastPage, row);
		// Order is 002, 001, 003
	

		lastPage.unlatch();
		lastPage = null;

		t_util.t_commit(t1);
		c1 = null;
	
		REPORT("checks on all 3 records.");

		c1 = t_util.t_openContainer(t1, 0, cid, false);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Could get container's last page");

		// Order is 002, 001, 003
		t_util.t_checkFetch(lastPage, rh001, REC_001);
		t_util.t_checkFetch(lastPage, rh002, REC_002);
		t_util.t_checkFetch(lastPage, rh003, REC_003);


		t_util.t_checkFetch(lastPage, lastPage.getRecordHandle(rh001.getId()), REC_001);
		t_util.t_checkFetch(lastPage, lastPage.getRecordHandle(rh002.getId()), REC_002);

		lastPage.unlatch();
		lastPage = null;

		t_util.t_commit(t1);

		REPORT("start deleting.");

		c1 = t_util.t_openContainer(t1, 0, cid, true);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Could get container's last page");

		// Order is 002, 001, 003
		t_util.t_checkFetch(lastPage, rh001, REC_001);
		t_util.t_checkFetch(lastPage, rh002, REC_002);
		t_util.t_checkFetch(lastPage, rh003, REC_003);

		int slot1 = lastPage.getSlotNumber(rh001);
		lastPage.deleteAtSlot(slot1, true, null);
		if (lastPage.fetchFromSlot(
				rh001, slot1, new DataValueDescriptor[0], null, false) != null)
        {
			throw T_Fail.testFailMsg("deleted record is still present");
        }
		// Order is 002,  003
		t_util.t_checkFetch(lastPage, rh002, REC_002);
		t_util.t_checkFetch(lastPage, rh003, REC_003);

		t_util.t_checkFetchNext(lastPage, rh002, REC_003);
		t_util.t_checkFetchPrevious(lastPage, rh003, REC_002);

		int slot2 = lastPage.getSlotNumber(rh002);
		lastPage.deleteAtSlot(slot2, true, null);
		if (lastPage.fetchFromSlot(
				rh002, slot2, new DataValueDescriptor[0], null, false) != null)
        {
			throw T_Fail.testFailMsg("deleted record is still present");
        }

		t_util.t_checkFetch(lastPage, rh003, REC_003);
		t_util.t_checkFetchFirst(lastPage, REC_003);
		t_util.t_checkFetchLast(lastPage, REC_003);

		lastPage.unlatch();
		lastPage = null;

		t_util.t_commit(t1);

		REPORT("update the remaining record.");

		c1 = t_util.t_openContainer(t1, 0, cid, true);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Could get container's last page");

		// Order is 003
		t_util.t_checkFetch(lastPage, rh003, REC_003);

		T_RawStoreRow urow = new T_RawStoreRow(REC_004);

		int slot3 = lastPage.getSlotNumber(rh003);
		if (lastPage.updateAtSlot(slot3, urow.getRow(), null) == null)
			throw T_Fail.testFailMsg("updateAtSlot returned null");

		// Order is 003
		t_util.t_checkFetch(lastPage, rh003, REC_004);

		lastPage.unlatch();
		lastPage = null;

		t_util.t_commit(t1);

		t_util.t_dropContainer(t1, 0, cid); // cleanup

		t_util.t_commit(t1);
		t1.close();
		   
		PASS("C200");

	}

	/**
		C201 - Create container with different page size, minimum record size,
		inserting into these containers to check if the variables are set correctly.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void C201(int whatPage) throws T_Fail, StandardException {

		int pageSize = (whatPage == 0 ? 4096 : 32768);

		Transaction t1 = t_util.t_startTransaction();

		REPORT("create container with pageSize " + pageSize + ", spareSpace " + 0 + ", minimumRecordSize " + pageSize/2);
		long cid = t_util.t_addContainer(t1, 0, pageSize, 0, pageSize/2, false);
		
		t_util.t_commit(t1);

		ContainerHandle c1;
		Page lastPage;
		RecordHandle rh001, rh002, rh003;
		T_RawStoreRow row;
	
		REPORT("see if the container can be opened again");
		c1 = t_util.t_openContainer(t1, 0, cid, false);

		c1.close();
		t_util.t_commit(t1);

		REPORT("insert a record into the container.");

		c1 = t_util.t_openContainer(t1, 0, cid, true);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Couldn't get container's last page");

		if (lastPage.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
			throw T_Fail.testFailMsg("Initial page must be " + ContainerHandle.FIRST_PAGE_NUMBER + ", is " + lastPage.getPageNumber());

		row = new T_RawStoreRow(REC_001);
		if (!lastPage.spaceForInsert())
			throw T_Fail.testFailMsg("No room for record on page");

		rh001 = t_util.t_insert(lastPage, row);
		if (rh001 == null)
			throw T_Fail.testFailMsg("Failed to insert record");

		// see if we can fetch that record
		t_util.t_checkFetch(lastPage, rh001, REC_001);

		lastPage.unlatch();
		lastPage = null;

		t_util.t_commit(t1);
		c1 = null;

		REPORT("read record just inserted.");

		c1 = t_util.t_openContainer(t1, 0, cid, false);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Couldn't get container's last page");

		t_util.t_checkFetch(lastPage, rh001, REC_001);
		t_util.t_checkFetchFirst(lastPage, REC_001);
		t_util.t_checkFetchLast(lastPage, REC_001);

		t_util.t_commit(t1);
		lastPage = null;
		c1 = null;

		// negative testing
		REPORT("try inserting 1 more record, but there should be no room on page for it.");

		c1 = t_util.t_openContainer(t1, 0, cid, true);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Couldn't get container's last page");

		if (lastPage.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
			throw T_Fail.testFailMsg("Initial page must be " + ContainerHandle.FIRST_PAGE_NUMBER + ", is " + lastPage.getPageNumber());

		row = new T_RawStoreRow(REC_002);
		if (lastPage.spaceForInsert())
        {
			throw T_Fail.testFailMsg("Did not get no room for record on page error");
        }

		if (!lastPage.recordExists(rh001, false))
			throw T_Fail.testFailMsg("Record 001 has vanished");

		lastPage.unlatch();
		lastPage = null;

		t_util.t_commit(t1);
		c1 = null;

		t_util.t_dropContainer(t1, 0, cid); // cleanup
		t_util.t_commit(t1);
		//t1.close();


		//t1 = t_util.t_startTransaction();

		REPORT("create container with pageSize " + pageSize + ", spareSpace " + 0 + ", minimumRecordSize " + pageSize);
		REPORT("this should set minimumRecordSize to the default 100");
		cid = t_util.t_addContainer(t1, 0, pageSize, 0, pageSize, false);
		
		t_util.t_commit(t1);

		REPORT("see if the container can be opened again");
		c1 = t_util.t_openContainer(t1, 0, cid, false);

		c1.close();
		t_util.t_commit(t1);

		REPORT("insert a record into the container.");

		c1 = t_util.t_openContainer(t1, 0, cid, true);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Couldn't get container's last page");

		if (lastPage.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
			throw T_Fail.testFailMsg("Initial page must be " + ContainerHandle.FIRST_PAGE_NUMBER + ", is " + lastPage.getPageNumber());

		row = new T_RawStoreRow(REC_001);
		if (!lastPage.spaceForInsert())
			throw T_Fail.testFailMsg("No room for record on page");

		rh001 = t_util.t_insert(lastPage, row);
		if (rh001 == null)
			throw T_Fail.testFailMsg("Failed to insert record");

		// see if we can fetch that record
		t_util.t_checkFetch(lastPage, rh001, REC_001);

		lastPage.unlatch();
		lastPage = null;

		t_util.t_commit(t1);
		c1 = null;

		REPORT("read record just inserted.");

		c1 = t_util.t_openContainer(t1, 0, cid, false);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Couldn't get container's last page");

		t_util.t_checkFetch(lastPage, rh001, REC_001);
		t_util.t_checkFetchFirst(lastPage, REC_001);
		t_util.t_checkFetchLast(lastPage, REC_001);

		t_util.t_commit(t1);
		lastPage = null;
		c1 = null;

		REPORT("insert 2 more records.");

		c1 = t_util.t_openContainer(t1, 0, cid, true);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Could get container's last page");

		if (lastPage.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
			throw T_Fail.testFailMsg("Initial page must be " + ContainerHandle.FIRST_PAGE_NUMBER + ", is " + lastPage.getPageNumber());

		row = new T_RawStoreRow(REC_002);
		if (!lastPage.spaceForInsert())
			throw T_Fail.testFailMsg("No room for record on page");

		if (!lastPage.recordExists(rh001, false))
			throw T_Fail.testFailMsg("Record 001 has vanished");

		rh002 = t_util.t_insertAtSlot(lastPage, 0, row);
		row = new T_RawStoreRow(REC_003);
		rh003 = t_util.t_insert(lastPage, row);

		lastPage.unlatch();
		lastPage = null;

		t_util.t_commit(t1);
		c1 = null;
	
		REPORT("checks on all 3 records.");

		c1 = t_util.t_openContainer(t1, 0, cid, false);

		lastPage = t_util.t_getLastPage(c1);
		if (lastPage == null)
			throw T_Fail.testFailMsg("Could get container's last page");

		// Order is 002, 001, 003
		t_util.t_checkFetch(lastPage, rh001, REC_001);
		t_util.t_checkFetch(lastPage, rh002, REC_002);
		t_util.t_checkFetch(lastPage, rh003, REC_003);


		t_util.t_checkFetch(lastPage, lastPage.getRecordHandle(rh001.getId()), REC_001);
		t_util.t_checkFetch(lastPage, lastPage.getRecordHandle(rh002.getId()), REC_002);

		lastPage.unlatch();
		lastPage = null;

		t_util.t_commit(t1);

		c1 = null;

		// clean ip
		t_util.t_dropContainer(t1, 0, cid);
		t_util.t_commit(t1);
		t1.close();

		PASS("C201 - " + whatPage);

	}

	/**
		Page tests
	 */
	 /**
	    Create a container, ensure it has one page with no records. Then test
		all the things we can do with an empty page opened read-only in the container.
		Then add a new page, ensure it has the correct page number and is empty.
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P001(long segment) throws T_Fail, StandardException {

		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, segment);

		t_util.t_commit(t);

		// Get the first page & check the record counts are zero
		ContainerHandle c = t_util.t_openContainer(t, segment, cid, false);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		t_util.t_checkEmptyPage(page);

		if (Page.FIRST_SLOT_NUMBER != 0)
			throw T_Fail.testFailMsg("Page.FIRST_SLOT_NUMBER must be 0, is " + Page.FIRST_SLOT_NUMBER);

		page.unlatch();
		page = null;

		// get the last page and check it is the first page
		page = t_util.t_getLastPage(c);

		t_util.t_checkPageNumber(page, ContainerHandle.FIRST_PAGE_NUMBER);
	
		t_util.t_checkEmptyPage(page);

		t_util.t_commit(t);

		// t_util.t_addPage checks that the page is empty.
		c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_addPage(c);

		t_util.t_checkPageNumber(page, ContainerHandle.FIRST_PAGE_NUMBER + 1);
		page.unlatch();

		page = t_util.t_addPage(c);
		t_util.t_checkPageNumber(page, ContainerHandle.FIRST_PAGE_NUMBER + 2);
		page.unlatch();

		t_util.t_commit(t);

		c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_updateSlotOutOfRange(page, 0);
		t_util.t_updateSlotOutOfRange(page, -1);
		t_util.t_updateSlotOutOfRange(page, 1);


		t_util.t_dropContainer(t, segment, cid);	// cleanup
		t_util.t_commit(t);

		// RESOLVE drop container

		t.close();

		PASS("P001");
	}

	/**
		Insert rows on the first page until the page is full, then add a page
		and repeat the test (for a total of three pages with full rows).
		Fetch the rows back by handle and slot methods.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P002(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		RecordHandle rh;
		T_RawStoreRow row;
		int	recordCount[] = {0,0,0};

		for (int i = 0; i < 3;) {
			row = new T_RawStoreRow(REC_001 + i + "X" + recordCount[i]);

			boolean spaceThere = page.spaceForInsert();

			rh = t_util.t_insert(page, row);

			if (rh != null) {
				recordCount[i]++;
				if (!spaceThere)
					REPORT("record inserted after spaceForInsert() returned false, count is " + recordCount[i]);
			} else {
				if (spaceThere)
					REPORT("record insert failed after spaceForInsert() returned true, count is " + recordCount[i]);
			}

			t_util.t_checkRecordCount(page, recordCount[i], recordCount[i]);

			if (rh != null)
				continue;

			page.unlatch();
			page = null;

			if (++i < 3) {
				page = t_util.t_addPage(c);
				t_util.t_checkEmptyPage(page);
			}
		}
		t_util.t_commit(t);

		for (int i = 0; i < 3; i++) {
			REPORT("RecordCount on page " + i + "=" + recordCount[i]);
		}

		// now check that we read the same number of records back
		// using the handle interface
		c = t_util.t_openContainer(t, segment, cid, false);

		long pageNumber = ContainerHandle.FIRST_PAGE_NUMBER;
		for (int i = 0; i < 3; i++, pageNumber++) {
			page = t_util.t_getPage(c, pageNumber);
			t_util.t_checkRecordCount(page, recordCount[i], recordCount[i]);
			rh = t_util.t_checkFetchFirst(page, REC_001 + i + "X" + 0);
			for (int j = 1; j < recordCount[i]; j++)
				rh = t_util.t_checkFetchNext(page, rh, REC_001 + i + "X" + j);

            try
            {
                rh = page.fetchFromSlot(
                        null, 
                        page.getSlotNumber(rh) + 1, 
                        new DataValueDescriptor[0], 
                        (FetchDescriptor) null, 
                        false);

				throw T_Fail.testFailMsg(
                        "reading more rows on page than were written");
            }
            catch (StandardException se)
            {
                // expected error.
            }

			rh = t_util.t_checkFetchLast(page, REC_001 + i + "X" + (recordCount[i] - 1));
			for (int j = recordCount[i] - 2; j >= 0; j--)
				rh = t_util.t_checkFetchPrevious(page, rh, REC_001 + i + "X" + j);

			page.unlatch();
			page = null;
		}
		t_util.t_commit(t);

		// now check that we read the same number of records back
		// using the slot interface
		c = t_util.t_openContainer(t, segment, cid, false);

		pageNumber = ContainerHandle.FIRST_PAGE_NUMBER;
		for (int i = 0; i < 3; i++, pageNumber++) {
			page = t_util.t_getPage(c, pageNumber);
		
			for (int j = 0; j < recordCount[i]; j++)
				t_util.t_checkFetchBySlot(page, j, REC_001 + i + "X" + j,
										false, false);

			t_util.t_readOnlySlotOutOfRange(page, recordCount[i]);

			page.unlatch();
			page = null;
		}

		t_util.t_dropContainer(t, segment, cid);	// cleanup

		t_util.t_commit(t);

		t.close();

		PASS("P002");
	}

	/* test repeated insert */
	protected void P005(long segment) throws StandardException, T_Fail 
	{
		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page1 = t_util.t_getLastPage(c);
	
		T_RawStoreRow row0 = new T_RawStoreRow
			("long row xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx long row ");
		T_RawStoreRow row1 = new T_RawStoreRow
			("medium row yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy medium row");

		t_util.t_insertAtSlot(page1, 0, row0);

		int i = 0;
		while (page1.spaceForInsert())
		{
			if (t_util.t_insertAtSlot(page1, 1, row1) == null)
				break;
			i++;
		}

		int count1 = page1.recordCount();

		Page page2 = t_util.t_addPage(c);
		t_util.t_insertAtSlot(page2, 0, row0);

		i = 1;
		while (page2.spaceForInsert())
		{
			if (t_util.t_insertAtSlot(page2, i++, row1) == null)
				break;
		}

		int count2 = page2.recordCount();

		// now purge them all and start over
		page1.purgeAtSlot(1, page1.recordCount()-1, logDataForPurges);
		page2.purgeAtSlot(1, page2.recordCount()-1, logDataForPurges);
		if (page1.recordCount() != 1)
			throw T_Fail.testFailMsg("purge did not clean up page");

		if (page2.recordCount() != 1)
			throw T_Fail.testFailMsg("purge did not clean up page");

		i = 0;
		while(page1.spaceForInsert())
		{
			if (t_util.t_insertAtSlot(page1, 1, row1) == null)
				return;
			i++;
		}

		if (page1.recordCount() != count1)
			throw T_Fail.testFailMsg("cannot insert back same number of rows we purged");


		i = 1;
		while(page2.spaceForInsert())
		{
			if (t_util.t_insertAtSlot(page2, i++, row1) == null)
				break;
		}

		if (page2.recordCount() != count2)
			throw T_Fail.testFailMsg("cannot insert back same number of rows we purged");

		page1.unlatch();
		page2.unlatch();	

		t_util.t_dropContainer(t, segment, cid);	// cleanup

		t_util.t_commit(t);
		t.close();

		PASS("P005");
	}

	/*
		P006

		test page time stamp - make sure all operation changes page time stamp
	*/
	protected void P006() throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();
		PageTimeStamp ts;

		long cid = t_util.t_addContainer(t, 0);
		t_util.t_commit(t);

		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
		Page page1 = t_util.t_getLastPage(c);
		
		ts = page1.currentTimeStamp();
		if (ts != null && !page1.equalTimeStamp(ts))
			throw T_Fail.testFailMsg("page returns non-null time stamp which is not equal to its current time stamp");

		T_RawStoreRow row = new T_RawStoreRow(REC_001);
		RecordHandle rh = t_util.t_insert(page1, row);

		if (page1.equalTimeStamp(ts))
			throw T_Fail.testFailMsg("timestamp on page not changed after insert operation");
		page1.setTimeStamp(ts);
		if (ts != null && !page1.equalTimeStamp(ts))
			throw T_Fail.testFailMsg("page returns non-null time stamp which is not equal to its current time stamp");

		// failed update should not change time stamp
		t_util.t_updateSlotOutOfRange(page1, 3);
		if (ts != null && !page1.equalTimeStamp(ts))
			throw T_Fail.testFailMsg("failed pdate should not change time stamp");

		T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
		int slot2 = page1.getSlotNumber(rh);
		page1.updateAtSlot(slot2, row2.getRow(), null);
		if (page1.equalTimeStamp(ts))
			throw T_Fail.testFailMsg("timestamp on page not changed after update operation");

		page1.setTimeStamp(ts);

		T_RawStoreRow upd1 = new T_RawStoreRow(REC_003);
		int slot = page1.getSlotNumber(rh);
		page1.updateAtSlot(slot, upd1.getRow(), BS_COL_0);
		if (page1.equalTimeStamp(ts))
			throw T_Fail.testFailMsg("timestamp on page not changed after update field operation");

		page1.setTimeStamp(ts);

		page1.deleteAtSlot(slot, true, null);
		if (page1.equalTimeStamp(ts))
			throw T_Fail.testFailMsg("timestamp on page not changed after delete operation");

		page1.setTimeStamp(ts);
		
		page1.purgeAtSlot(0, 1, logDataForPurges);
		if (page1.equalTimeStamp(ts))
			throw T_Fail.testFailMsg("timestamp on page not changed after delete operation");

		page1.setTimeStamp(ts);
		page1.unlatch();

		if (testRollback)
		{
			t_util.t_abort(t);
			c = t_util.t_openContainer(t, 0, cid, true);
			page1 = t_util.t_getLastPage(c);

			if (page1.equalTimeStamp(ts))
				throw T_Fail.testFailMsg("timestamp on page not changed after rollback");

			page1.setTimeStamp(ts);
		}
		
		Page page2 = c.addPage();
		Page page3 = c.addPage();

		page2.setTimeStamp(ts);

		if (ts != null)
		{
			try 
			{
				if (page3.equalTimeStamp(ts))
					throw T_Fail.testFailMsg("timestamp on 2 different pages should not equate");
			}
			catch (StandardException se)
			{
				// either throw an exception or return false is OK
			}
		}


		t_util.t_dropContainer(t, 0, cid);	// cleanup
		t_util.t_commit(t);
		t.close();

		PASS("P006");

	}

	/**
		P007

		this test exercises repeated updates on a 1K page

		2 rows (with 1 column) will be inserted into the page.
		We expand the row data in slot 0 by 1 byte until the page is completely full,
		and overflows the record to an overflow page.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	  */

	  protected void P007(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		// PART 1:
		// insert two 1-column rows into the page, expand the first row, until it overflows.
		long cid = t_util.t_addContainer(t, segment, 4096, 0, 1, false);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getLastPage(c);
	
		T_RawStoreRow row = new T_RawStoreRow(REC_001); // McLaren
		T_RawStoreRow row2 = new T_RawStoreRow(new String(new char[300])); 

		RecordHandle r1 = t_util.t_insertAtSlot(page, 0, row);
		RecordHandle r2 = t_util.t_insertAtSlot(page, 1, row2);

		// update the row size 1 byte larger, until the page is full
		String rowData = REC_001;
		// 900 is an estimated number, because for 1K page,
		// if you expand your row by 1 byte 900 times, the row will overflow for sure.
		for (int i = 0; i <= 900; i++) {
			t_util.t_checkFetch(page, r1, rowData);

			rowData = rowData + REC_008;	// "z"
			row = new T_RawStoreRow(rowData);
			page.updateAtSlot(0, row.getRow(), (FormatableBitSet) null);
		}

		t_util.t_dropContainer(t, segment, cid);	// cleanup

		// PART 2:
		// insert two 2-column rows into the page,
		// expand the first row by expanding the first column by 300 bytes,
		// and shrinking the second column by 300 bytes.  Update should secceed.
		cid = t_util.t_addContainer(t, segment, 4096, 0, 1, false);
		c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_getLastPage(c);
		long pid = page.getPageNumber();

		row = new T_RawStoreRow(2);
		row.setColumn(0, REC_001);	// small column
		row.setColumn(1, new String (new char[400]));	// large column

		r1 = t_util.t_insertAtSlot(page, 0, row);
		r2 = t_util.t_insertAtSlot(page, 1, row2);

		row.setColumn(0, REC_001 + new String (new char[300]));
		row.setColumn(1, new String (new char[100]));

		page.updateAtSlot(0, row.getRow(), (FormatableBitSet) null);

		Page page2 = t_util.t_addPage(c);
		long pid2 = page2.getPageNumber();
		if (pid2 != (pid + 1))
			throw T_Fail.testFailMsg("The update should not have overflowed the record");

		// Now, shrink the first column by 300 bytes, and expand the second column by 300 bytes.
		// the update should also succeed.
		row.setColumn(0, REC_001);
		row.setColumn(1, new String (new char[400]));

		page.updateAtSlot(0, row.getRow(), (FormatableBitSet) null);

		Page page3 = t_util.t_addPage(c);
		long pid3 = page3.getPageNumber();
		if (pid3 != (pid2 + 1))
			throw T_Fail.testFailMsg("The update should not have overflowed the record");

		t_util.t_dropContainer(t, segment, cid);	// cleanup

		t_util.t_commit(t);
		t.close();

		PASS("P007");

	}

	/**
		P008

		this test exercises repeated inserts with small rows on a 1K page

		we will insert as many rows as possible into the page.  Then we reduce the row by 1 byte at a time,
		we will try to insert another smaller row.
		This test also tests spaceForInsert().

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	  */

	protected void P008(long segment) throws StandardException, T_Fail 
	{
		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096, 0, 1, false);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page1 = t_util.t_getLastPage(c);
	
		T_RawStoreRow row = new T_RawStoreRow(REC_001);		// McLaren

		t_util.t_insertAtSlot(page1, 0, row);

		int i = 0;
		while (page1.spaceForInsert(row.getRow(), (FormatableBitSet) null, 100))
		{
			// if it says there is enough room for this row, the insert should succeed.
			if (t_util.t_insertAtSlot(page1, 1, row) == null)
				throw T_Fail.testFailMsg("There is space for this insert.  It shouldn't have failed.  "
					+ "record #" + i);
			i++;
		}

		REPORT(i + " rows inserted.");

		// We got out of the while loop because there is no room for the insert.
		// So, if the insert succeed, then we have a problem.
		if (t_util.t_insertAtSlot(page1, 1, row) != null)
			throw T_Fail.testFailMsg("There is no space for this insert.  It should have failed.");

		// Now, we will try to fill the page with smaller rows.
		String[] s = new String[7];
		s[6] = "McLare";
		s[5] = "McLar";
		s[4] = "McLa";
		s[3] = "McL";
		s[2] = "Mc";
		s[1] = "M";
		s[0] = null;
		// reduce the row by 1 byte
		i = 6;
		boolean notDone = true;
		do
		{
			row = new T_RawStoreRow(s[i]);
			if (page1.spaceForInsert(row.getRow(), (FormatableBitSet) null, 100))
			{
				// If it says there is enough room for the row, then the insert should succed.
				if (t_util.t_insertAtSlot(page1, 1, row) == null)
					throw T_Fail.testFailMsg("There should be space for this insert, row is " + s[i]);
				else
					notDone = false;
			}
			else
				i--;
		} while ((notDone) && (i >= 0));


		page1.unlatch();

		t_util.t_dropContainer(t, segment, cid);	// cleanup

		t_util.t_commit(t);
		t.close();

		PASS("P008");
	}

	/**
		P009

		this test exercises repeated shrinking and expanding of fields using updateFieldBySlot

		we will insert as many rows as possible into the page. Then set some of the columns to null,
		That should not create more space on the page for inserts, because the extra space become
		reservedspace for the row.  So, the next insert should fail.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	  */

	protected void P009(long segment) 
		 throws StandardException, T_Fail 
	{
		int slot = 0;
		int i = 0;
		int j = 0;
		String field = REC_001;

		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, segment);

		// Get the first page & check the record counts are zero
		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		t_util.t_checkEmptyPage(page);

		// Create a 13-column row
		T_RawStoreRow row = new T_RawStoreRow(13);
		row.setColumn(0, (String) null);
		row.setColumn(1, REC_001);
		row.setColumn(2, REC_002);
		row.setColumn(3, REC_003);
		row.setColumn(4, REC_004);
		row.setColumn(5, REC_005);
		row.setColumn(6, REC_006);
		row.setColumn(7, REC_007);
		row.setColumn(8, (String) null);
		row.setColumn(9, (String) null);
		row.setColumn(10, REC_007);
		row.setColumn(11, (String) null);
		row.setColumn(12, REC_006);

		// insert the row into the page until the page is full
		int numRows = 0;
		slot = page.FIRST_SLOT_NUMBER;
		while (page.spaceForInsert(row.getRow(), (FormatableBitSet) null, 100)) {
			t_util.t_insert(page, row);
			numRows++;
		}
		REPORT(numRows + " rows inserted ");

		// update all the fields in the even number rows to null
		// set all the fields in the odd number rows to REC_001
		DataValueDescriptor col = new SQLChar();	// null
		for (i = page.FIRST_SLOT_NUMBER; i < (page.FIRST_SLOT_NUMBER + 2); i++) {

			for (slot = i; slot <= (numRows - 1); slot += 2) {

				for (j = 0; j <= 12; j++) {
					if (page.updateFieldAtSlot(slot, j, col, null) == null) {

						throw T_Fail.testFailMsg("Failed to update field " + j+ ", in row " + slot);
					}
				}
			}

			col = new SQLChar(REC_001);
		}

		// fetch all the fields, and see if they are correct
		DataValueDescriptor storedColumn = new SQLChar();
		field = null;
		for (i = page.FIRST_SLOT_NUMBER; i < (page.FIRST_SLOT_NUMBER + 2); i++) {

			for (slot = i; slot <= (numRows - 1); slot += 2) {

				for (j = 0; j <= 12; j++) {

					t_util.t_checkFetchColFromSlot(page, slot, j, storedColumn, false, field);

				}
			}

			field = REC_001;
		}

		// Now if we try to insert the old row again, there should still be no room
		if (page.spaceForInsert())
			throw T_Fail.testFailMsg("Did not get no room for record on page error");

		// update the first and last field of every row to REC_006
		col = new SQLChar(REC_006);
		for (slot = page.FIRST_SLOT_NUMBER; slot <= (numRows - 1); slot++) {
			if (page.updateFieldAtSlot(slot, 0, col, null) == null ||
					page.updateFieldAtSlot(slot, 12, col, null) == null) {

				throw T_Fail.testFailMsg("Failed to update fields to REC_006 in row " + slot);
			}
		}
		
		// update field 5 and 6 of every row to REC_007
		col = new SQLChar(REC_007);
		for (slot = page.FIRST_SLOT_NUMBER; slot <= (numRows - 1); slot++) {
			if (page.updateFieldAtSlot(slot, 5, col, null) == null ||
					page.updateFieldAtSlot(slot, 6, col, null) == null) {

				throw T_Fail.testFailMsg("Failed to update fields to REC_007 in row " + slot);
			}
		}

		// fetch all the fields again, and see if they are correct
		for (i = page.FIRST_SLOT_NUMBER; i < (page.FIRST_SLOT_NUMBER + 2); i++) {

			for (slot = i; slot <= (numRows - 1); slot += 2) {

				for (j = 0; j <= 12; j++) {

					switch (j) {
					case 0:
					case 12:
						field = REC_006;
						break;
					case 5:
					case 6:
						field = REC_007;
						break;
					default:
						if ((slot % 2) == 0)
							field = null;
						else
							field = REC_001;
						break;
					}

					t_util.t_checkFetchColFromSlot(page, slot, j, storedColumn, false, field);

				}
			}
		}

		// We now try to insert the old row one last time, there should still be no room
		if (page.spaceForInsert())
			throw T_Fail.testFailMsg("Did not get no room for record on page error");

		// now we want to increase row 0 and column 5 one byte at a time, until the page is full
		// but, every 5 increases we will reduce the field size by one byte
		field = REC_007;
		i = 0;
		String field_pre = null;
		while (true) {
			if ((i % 5) != 0) {
				field_pre = field;
				field += REC_008;
			} else {
				field = field_pre;
			}

			if (((i % 10) == 3) || ((i % 10) == 7)) {
				page.unlatch();
				page = null;

				factory.idle();

				page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			}

			col = new SQLChar(field);

			try {
				page.updateFieldAtSlot(0, 5, col, null);
			} catch (StandardException se) {
				// now we have filled the page
				if (i < 809) {
					throw T_Fail.testFailMsg("should be able to update Row 0 Column 5 809 times"
						+ ", but only updated " + i
						+ " times.  Note: you maybe getting this error if your page format has changed.");
				} else {
					REPORT("Row 0 Column 5 was updated " + i + " times.");
				}
				break;
			}
			i++;
		}
		
		// The page is completely full at this point.
		// update Row 1 Column 1 from REC_001 to REC_002.  They are the same length
		page.unlatch();
		page = null;
		factory.idle();
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		col = new SQLChar(REC_002);
		if (page.updateFieldAtSlot(1, 1, col, null) == null) {
			throw T_Fail.testFailMsg("update Row 1 and Column 1 to same length data failed.");
		}

		REPORT("updated col1 in row 1 to same length");

		// now expand update Row 1 Column 1 by one byte.  This should fail.
		page.unlatch();
		page = null;
		factory.idle();
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		field = REC_002 + REC_008;
		col = new SQLChar(field);
		try {
			page.updateFieldAtSlot(1, 1, col, null);
			throw T_Fail.testFailMsg("update Row 1 and Column 1 to longer length should have failed.");
		} catch (StandardException se) {
			;
		}


		// clean up
		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);
		t.close();

		PASS("P009: segment " + segment);
		
	}



	/**
		P011

		this test exercises insertAtSlot, (LogicalUndo)null

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	  */
	protected void P011(long segment)
		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, segment);
		t_util.t_commit(t);

	
		// Get the first page
		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// REPORT("insert 2 records at FIRST_SLOT_NUMBER");
		T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
		T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
		T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
		T_RawStoreRow row4 = new T_RawStoreRow(REC_004);

		// try inserting at slot -1 and slot 1
		try {
			RecordHandle r = t_util.t_insertAtSlot(page, -1, row1);
			throw T_Fail.testFailMsg("insert at slot -1 succeeded");
		} catch (StandardException se) {
			// throw if not a statement exception.
            if (se.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
                throw se;
		}

		try {
			RecordHandle r = t_util.t_insertAtSlot(page, 1, row1);
			throw T_Fail.testFailMsg("insert at slot 1 succeeded");
		} catch (StandardException se) {
			// throw if not a statement exception.
            if (se.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
                throw se;
		}
		
		RecordHandle r1, r2, r3, r4;
		// first insert to a page must suceed
		r3 = t_util.t_insertAtSlot(page, Page.FIRST_SLOT_NUMBER, row3);
		t_util.t_checkFetch(page, r3, REC_003);
		t_util.t_checkFetchBySlot(page, Page.FIRST_SLOT_NUMBER, REC_003,false, false);			

		r1 = r2 = r4 = null;
		r1 = t_util.t_insertAtSlot(page, Page.FIRST_SLOT_NUMBER, row1);

		if (r1 != null) {
			t_util.t_checkFetch(page, r1, REC_001);
			t_util.t_checkFetchBySlot(page, Page.FIRST_SLOT_NUMBER, REC_001,false, false);
		} else {
			t_util.t_abort(t);
			t.close();
			REPORT("P011 not run - could not fit 4 rows on page");
			return;
		}

		// REPORT("insert a record at 2nd slot");
		r2 = t_util.t_insertAtSlot(page, Page.FIRST_SLOT_NUMBER+1, row2);

		if (r2 != null) {
			t_util.t_checkFetch(page, r2, REC_002);
			t_util.t_checkFetchBySlot(page, Page.FIRST_SLOT_NUMBER+1, REC_002,false, false);
		} else {
			t_util.t_abort(t);
			t.close();
			REPORT("P011 not completed - could not fit 4 rows on page");
			return;
		}

		// REPORT("insert a record at the end");
		r4 = t_util.t_insertAtSlot(page, 3, row4);
		if (r4 != null) {
			t_util.t_checkFetch(page, r4, REC_004);
			t_util.t_checkFetchBySlot(page, 3, REC_004,false, false);
		} else {
			t_util.t_abort(t);
			t.close();
			REPORT("P011 not completed - could not fit 4 rows on page");
			return;
		}

		// REPORT("make sure records are in the correct order");
		// order is REC_001 REC_002 REC_003 REC_004 


		t_util.t_checkFetchFirst(page, REC_001);
		t_util.t_checkFetchNext(page, r1, REC_002);
		t_util.t_checkFetchNext(page, r2, REC_003);
		t_util.t_checkFetchNext(page, r3, REC_004);
		t_util.t_checkFetchLast(page, REC_004);

		// check the fetch by slot interface
		t_util.t_checkFetchBySlot(page, Page.FIRST_SLOT_NUMBER, REC_001,false, true);
		t_util.t_checkFetchBySlot(page, Page.FIRST_SLOT_NUMBER+1, REC_002,false, false);
		t_util.t_checkFetchBySlot(page, Page.FIRST_SLOT_NUMBER+2, REC_003,false, true);
		t_util.t_checkFetchBySlot(page, Page.FIRST_SLOT_NUMBER+3, REC_004,false, false);

		// clean up

		t_util.t_dropContainer(t, segment, cid);	// cleanup

		t_util.t_commit(t);
		t.close();

		PASS("P011");

	}

	/**
	  P012

	  this test exercises updateAtSlot

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P012(long segment)
		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, segment);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// REPORT("insert 3 records");
		T_RawStoreRow row1 = new T_RawStoreRow(REC_001);

		T_RawStoreRow row2 = new T_RawStoreRow(2);
		row2.setColumn(0, (String) null);
		row2.setColumn(1, REC_001);

		T_RawStoreRow row3 = new T_RawStoreRow(3);
		row3.setColumn(0, REC_001);
		row3.setColumn(1, REC_002);
		row3.setColumn(2, REC_003);

		RecordHandle r1, r2, r3;
		r1 = t_util.t_insertAtSlot(page, 0, row1);

		r2 = r3 = null;
		r2 = t_util.t_insertAtSlot(page, 1, row2);
	
		if (r2 == null) {
			REPORT("P012 not completed - cannot insert second row");
			return;
		}

		r3 = t_util.t_insertAtSlot(page, 2, row3);
		if (r3 == null) {
			REPORT("P012 not completed - cannot insert third row");
			return;
		}

		// check that they are inserted correctly
		t_util.t_checkFetch(page, r1, row1);
		t_util.t_checkFetch(page, r2, row2);
		t_util.t_checkFetch(page, r3, row3);

		// REPORT("update that grows the #columns in row");
		T_RawStoreRow upd1 = new T_RawStoreRow(2);
		upd1.setColumn(0, (String) null);
		upd1.setColumn(1, REC_001);

		r1 = page.updateAtSlot(0, upd1.getRow(), (FormatableBitSet) null);
		t_util.t_checkFetch(page, r1, upd1);
		t_util.t_checkFetch(page, r2, row2);
		t_util.t_checkFetch(page, r3, row3);

		// REPORT("update that shrinks the #columns in row");
		T_RawStoreRow upd2 = new T_RawStoreRow(REC_004);

		r2 = page.updateAtSlot(1, upd2.getRow(), (FormatableBitSet) null);
		t_util.t_checkFetch(page, r1, upd1);
		t_util.t_checkFetch(page, r2, upd2);
		t_util.t_checkFetch(page, r3, row3);

		// REPORT("update same #columns in row");
		T_RawStoreRow upd3 = new T_RawStoreRow(3);
		upd3.setColumn(0, REC_003);
		upd3.setColumn(1, REC_002);
		upd3.setColumn(2, REC_001);

		r3 = page.updateAtSlot(2, upd3.getRow(), (FormatableBitSet) null);
		t_util.t_checkFetch(page, r1, upd1);
		t_util.t_checkFetch(page, r2, upd2);
		t_util.t_checkFetch(page, r3, upd3);

		// clean up

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);
		t.close();

		PASS("P012");
	}

	/**
	  P013

	  this test exercises deleteAtSlot and isDeletedAtSlot
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P013()
		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, 0);

		t_util.t_commit(t);

		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// REPORT("insert 2 records");
		T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
		T_RawStoreRow row2 = new T_RawStoreRow(REC_002);

		RecordHandle r1 = t_util.t_insertAtSlot(page, 0, row1);
		RecordHandle r2;
		r2 = t_util.t_insertAtSlot(page, 1, row2);
		if (r2 == null) {
			REPORT("P013 not completed - could not fit two rows on a page");
			return;
		}

		t_util.t_checkRecordCount(page, 2, 2);

		// REPORT("delete them one by one");
		page.deleteAtSlot(0, true, (LogicalUndo)null);

		t_util.t_checkRecordCount(page, 2, 1);

		if (!page.isDeletedAtSlot(0))
			throw T_Fail.testFailMsg("Failed to delete record 0");
		if (page.isDeletedAtSlot(1))
			throw T_Fail.testFailMsg("Record mistakenly deleted");

		page.deleteAtSlot(1, true, (LogicalUndo)null);
		t_util.t_checkRecordCount(page, 2, 0);
		if (!page.isDeletedAtSlot(1))
			throw T_Fail.testFailMsg("Failed to delete record 1");

		page.deleteAtSlot(0, false, (LogicalUndo)null);
		t_util.t_checkRecordCount(page, 2, 1);
		if (page.isDeletedAtSlot(0))
			throw T_Fail.testFailMsg("Failed to undelete record 0");
		if (!page.isDeletedAtSlot(1))
			throw T_Fail.testFailMsg("Record mistakenly undeleted");

		page.deleteAtSlot(1, false, (LogicalUndo)null);
		t_util.t_checkRecordCount(page, 2, 2);
		if (page.isDeletedAtSlot(1))
			throw T_Fail.testFailMsg("Failed to undelete record 1");

		// try the negative tests
		try {
			page.deleteAtSlot(0, false, (LogicalUndo)null);
			throw T_Fail.testFailMsg("undeleted on undeleted record succeeded");
		} catch (StandardException se) {
			// throw if not a statement exception.
            if (se.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
                throw se;
		}

		page.deleteAtSlot(0, true, (LogicalUndo)null);
		try {
			page.deleteAtSlot(0, true, (LogicalUndo)null);
			throw T_Fail.testFailMsg("deleted on deleted record succeeded");
		} catch (StandardException se) {
			// throw if not a statement exception.
            if (se.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
                throw se;
		}

		t_util.t_checkRecordCount(page, 2, 1);
		// clean up
		PASS("P013");

		t_util.t_dropContainer(t, 0, cid);	// cleanup



		t_util.t_commit(t);
		t.close();

		
	}

	/**
	  P014

	  this test exercises purgeAtSlot
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P014()
		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t,0);
		t_util.t_commit(t);

	
		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// REPORT("insert 5 records");
		T_RawStoreRow row0 = new T_RawStoreRow(REC_001);
		T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
		T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
		T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
		T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
		
		RecordHandle r0, r1, r2, r3, r4;
		r0 = t_util.t_insertAtSlot(page, 0, row0);
		r1 = t_util.t_insertAtSlot(page, 1, row1);
		r2 = t_util.t_insertAtSlot(page, 2, row2);
		r3 = t_util.t_insertAtSlot(page, 3, row3);
		r4 = t_util.t_insertAtSlot(page, 4, row4);

		if (r3 != null) page.deleteAtSlot(3, true, (LogicalUndo)null);

		// REPORT("commit it");
		t_util.t_commit(t);

		c = t_util.t_openContainer(t, 0, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		try 
		{
			page.purgeAtSlot(-1, 1, logDataForPurges);
			throw T_Fail.testFailMsg("negative slot number did not cause an exception");
		}
		catch (StandardException se) {}	// expected

		try
		{
			page.purgeAtSlot(4, 4, logDataForPurges);
			throw T_Fail.testFailMsg("purging more rows than is on page did not cause an exception");
		}
		catch (StandardException se) {}	// expected

		// if not all the rows are there, do minimal test
		if (r4 == null)
		{
			int rcount = page.recordCount();
			page.purgeAtSlot(0, 1, logDataForPurges);
			if (page.recordCount() != rcount-1)
				T_Fail.testFailMsg("failed to purge a record, expect " + 
								   (rcount-1) + " got " + page.recordCount());

			if (testRollback)
			{
				t_util.t_abort(t);

				c = t_util.t_openContainer(t, 0, cid, true);
				page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
				if(logDataForPurges)
					t_util.t_checkFetchBySlot(page, 0, REC_001,false, true);
				else
					t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
				if (page.recordCount() != rcount)
					T_Fail.testFailMsg("failed to rollback purge, expect " + 
								   rcount + " got " + page.recordCount());
			}
			else
			{
				t_util.t_commit(t);
			}
			PASS("minimal P014");
			return;
		}

		// REPORT("purge 2 records from middle");
		page.purgeAtSlot(1, 2, logDataForPurges);
		t_util.t_checkFetchBySlot(page, 0, REC_001,false, true);
		t_util.t_checkFetchBySlot(page, 1, REC_003,true, true);
		t_util.t_checkFetchBySlot(page, 2, REC_004,false, true);

		if (page.recordCount() != 3)
			T_Fail.testFailMsg("page expect to have 3 records, recordCount() = " +
							   page.recordCount());

		// REPORT("purge all records from the page");
		page.purgeAtSlot(0, 3, logDataForPurges);
		if (page.recordCount() != 0)
			T_Fail.testFailMsg("page expect to have 0 records, recordCount() = " +
							   page.recordCount());

		if (testRollback)
		{

			REPORT("testing rollback");
			t_util.t_abort(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			if(logDataForPurges){
				t_util.t_checkFetchBySlot(page, 0, REC_001,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_001,false, true);
				t_util.t_checkFetchBySlot(page, 2, REC_002,false, true);
				t_util.t_checkFetchBySlot(page, 3, REC_003,true, true);
				t_util.t_checkFetchBySlot(page, 4, REC_004,false, true);
			}else
			{
				t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 2, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 3, REC_NULL,true, true);
				t_util.t_checkFetchBySlot(page, 4, REC_NULL,false, true);
			}

			if (page.recordCount() != 5)
				T_Fail.testFailMsg("page expect to have 5 records, recordCount() = " +
								   page.recordCount());

			// REPORT("purge 3 records from the end");
			page.purgeAtSlot(2, 3, logDataForPurges);
			if(logDataForPurges)
			{
				t_util.t_checkFetchBySlot(page, 0, REC_001,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_001,false, true);
			}else
			{
				t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_NULL,false, true);
			}
			if (page.recordCount() != 2)
				T_Fail.testFailMsg("page expect to have 2 records, recordCount() = " +
								   page.recordCount());

			// REPORT("rollback");
			t_util.t_abort(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			if(logDataForPurges){
				t_util.t_checkFetchBySlot(page, 0, REC_001,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_001,false, true);
				t_util.t_checkFetchBySlot(page, 2, REC_002,false, true);
				t_util.t_checkFetchBySlot(page, 3, REC_003,true, true);
				t_util.t_checkFetchBySlot(page, 4, REC_004,false, true);
			}else
			{
				t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 2, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 3, REC_NULL,true, true);
				t_util.t_checkFetchBySlot(page, 4, REC_NULL,false, true);	
			}

			if (page.recordCount() != 5)
				T_Fail.testFailMsg("page expect to have 5 records, recordCount() = " +
								   page.recordCount());

			// REPORT("make sure delete record is reconstituted as such");
			if (page.isDeletedAtSlot(1))
				T_Fail.testFailMsg("rolled back purged undeleted record cause record to be deleted");
			if (!page.isDeletedAtSlot(3))
				T_Fail.testFailMsg("rolled back purged deleted record cause record to be undeleted");
		}
		PASS("P014");

		t_util.t_dropContainer(t, 0, cid);	// cleanup
		t_util.t_commit(t);
		t.close();
	}


	/**
	  P015

	  this test exercises updateAtSlot

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P015()
		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, 0);

		t_util.t_commit(t);


		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// REPORT("insert 3 records");
		T_RawStoreRow row1 = new T_RawStoreRow(REC_001);

		T_RawStoreRow row2 = new T_RawStoreRow(2);
		row2.setColumn(0, (String) null);
		row2.setColumn(1, REC_001);

		T_RawStoreRow row3 = new T_RawStoreRow(3);
		row3.setColumn(0, REC_001);
		row3.setColumn(1, REC_002);
		row3.setColumn(2, REC_003);

		RecordHandle r1, r2, r3;
		r1 = t_util.t_insertAtSlot(page, 0, row1);

		r2 = r3 = null;
		r2 = t_util.t_insertAtSlot(page, 1, row2);

		if (r2 == null) {
			REPORT("P015 not completed - cannot insert second row");
			return;
		}

		r3 = t_util.t_insertAtSlot(page, 2, row3);

		if (r3 == null) {
			REPORT("P015 not completed - cannot insert third row");
			return;
		}

		// check that they are inserted correctly
		t_util.t_checkFetch(page, r1, row1);
		t_util.t_checkFetch(page, r2, row2);
		t_util.t_checkFetch(page, r3, row3);

		// now update the middle row with a large value
		T_RawStoreRow row2u = new T_RawStoreRow(2);
		row2u.setColumn(0, "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789");
		row2u.setColumn(1, "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789");

		page.updateAtSlot(1, row2u.getRow(), (FormatableBitSet) null);
		t_util.t_checkFetch(page, r2, row2u);


		// now update the field of the first record with a large value
		((T_RawStoreRow) row1).setColumn(0, "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz");

        FormatableBitSet validColumn = new FormatableBitSet(2);

        validColumn.clear();
        validColumn.set(0);

        page.updateAtSlot(0, row1.getRow(), validColumn);

		t_util.t_checkFetch(page, r1, row1);

		((T_RawStoreRow) row3).setColumn(1, "XXabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz");

        validColumn.clear();
        validColumn.set(1);

        page.updateAtSlot(2, row3.getRow(), validColumn);
		t_util.t_checkFetch(page, r3, row3);

		// clean up
		PASS("P015");

		t_util.t_dropContainer(t, 0, cid);	// cleanup


		t_util.t_commit(t);
		t.close();

	}

	/*
		P016

		this test exercises copyAndPurge
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P016()
		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, 0);
		long cid2 = t_util.t_addContainer(t, 0);

		t_util.t_commit(t);


		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

		Page page1 = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		Page page2 = t_util.t_addPage(c);
		long pid1 = page1.getPageNumber();
		long pid2 = page2.getPageNumber();

		t_util.t_checkEmptyPage(page2);

		// first fill up page 1

		int i = 0;
		int deleted = 0;
		RecordHandle rh;

		T_RawStoreRow row;

		for (i = 0, row = new T_RawStoreRow("row at slot " + i);
			 page1.spaceForInsert();
			 i++, row = new T_RawStoreRow("row at slot " + i))
		{
			rh = t_util.t_insertAtSlot(page1, i, row );
			if (rh == null)
				break;
			
			// delete every third row
			if ((i % 3) == 1)
			{
				deleted++;
				page1.deleteAtSlot(i, true, null);
			}
		}

		int recordCount = i;

		// negative testing
		// copy into page of different container
		ContainerHandle c2 = t_util.t_openContainer(t, 0, cid2, true);
		Page wrongPage = t_util.t_getPage(c2, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			page1.copyAndPurge(wrongPage, 0, recordCount, 0);
			throw T_Fail.testFailMsg("copying to page from a different contaier should cause and exception");
		}
		catch (StandardException se) {} // expected

		try 
		{
			page1.copyAndPurge(page2, 1, 0, 0);
			throw T_Fail.testFailMsg("copying zero rows should cause an exception");
		}
		catch (StandardException se) {} // expected

		try
		{
			page1.copyAndPurge(page2, 1, recordCount, 0);
			throw T_Fail.testFailMsg("copying more rows than page contains should cause an exception");
		}
		catch (StandardException se) {} // expected

		try
		{
			page1.copyAndPurge(page2, 0, 1, 1);
			throw T_Fail.testFailMsg("copying rows to nonexistant slot should cause an exception");
		}
		catch (StandardException se) {} // expected


		// copy the whole page to page2
		page1.copyAndPurge(page2, 0, recordCount, 0);

		// check
		t_util.t_checkEmptyPage(page1);

		for (i = 0; i < recordCount; i++)
		{
			t_util.t_checkFetchBySlot(page2, i, "row at slot " + i,
									((i%3) == 1), true);
		}
		t_util.t_checkRecordCount(page2, recordCount, recordCount-deleted);


		t_util.t_commit(t);

		if (recordCount > 2)
		{
			// now copy and purge part of the page
			c = t_util.t_openContainer(t, 0, cid, true);
			page1 = t_util.t_getPage(c, pid1);
			page2 = t_util.t_getPage(c, pid2);


			// insert 2 rows into page1
			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			t_util.t_insertAtSlot(page1, 0, row1);
			t_util.t_insertAtSlot(page1, 1, row2);

			page2.copyAndPurge(page1, 1, recordCount-2, 1);

			t_util.t_checkFetchBySlot(page2, 0, "row at slot " + 0,false, true);
			
			// need to figure out the delete status of the last row on page 2
			boolean tdeleted = ((recordCount-1)%3) == 1;
			t_util.t_checkFetchBySlot(page2, 1, "row at slot " + (recordCount-1),tdeleted, true);
			
			if (((recordCount-1) % 3) == 1)
				t_util.t_checkRecordCount(page2, 2, 1);	// last record on page2 was deleted
			else
				t_util.t_checkRecordCount(page2, 2, 2);	// last record on page2 was not deleted

			t_util.t_checkFetchBySlot(page1, 0, REC_001,false, false);
			for (i = 1; i < recordCount-1; i++)
			{
				t_util.t_checkFetchBySlot(page1, i, "row at slot " + i,
										((i%3)==1), false);
			}
			t_util.t_checkFetchBySlot(page1, recordCount-1, REC_002,false, false);
			if (((recordCount-1) % 3) == 1)
				// one (the last one) of the deleted rows did not get copied over
				t_util.t_checkRecordCount(page1, recordCount, recordCount-deleted+1);
			else
				t_util.t_checkRecordCount(page1, recordCount, recordCount-deleted);

			if (testRollback)
			{
				t_util.t_abort(t);

				c = t_util.t_openContainer(t, 0, cid, true);
				page1 = t_util.t_getPage(c, pid1);
				page2 = t_util.t_getPage(c, pid2);

				// the two inserted rows is rolled back by deletion
				t_util.t_checkFetchBySlot(page1, 0, REC_001,true, false);
				t_util.t_checkFetchBySlot(page1, 1, REC_002,true, false);
				t_util.t_checkRecordCount(page1, 2, 0);

				for (i = 0; i < recordCount; i++)
				{
					t_util.t_checkFetchBySlot(page2, i, "row at slot " + i,
											((i%3)==1), true);
				}
				t_util.t_checkRecordCount(page2, recordCount, recordCount-deleted);

				REPORT("tested roll back of copyAndPurge");
			}


			PASS("P016");
		}

		t_util.t_dropContainer(t, 0, cid);	// cleanup
		t_util.t_dropContainer(t, 0, cid2);	// cleanup
		t_util.t_commit(t);
		t.close();
	}


	/*
		P017
		this test getInvalidRecordHandle and makeRecordHandle 

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P017()		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();
		

		long cid = t_util.t_addContainer(t, 0);
		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

		Page page1 = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		T_RawStoreRow row1 = new T_RawStoreRow(REC_001);

		RecordHandle valid = t_util.t_insert(page1, row1);

		RecordHandle[] rhs = new RecordHandle[RecordHandle.FIRST_RECORD_ID];
		rhs[0] = page1.getInvalidRecordHandle();
		rhs[1] = page1.makeRecordHandle(RecordHandle.RESERVED1_RECORD_HANDLE);
		rhs[2] = page1.makeRecordHandle(RecordHandle.DEALLOCATE_PROTECTION_HANDLE);
		rhs[3] = page1.makeRecordHandle(RecordHandle.PREVIOUS_KEY_HANDLE);
		rhs[4] = page1.makeRecordHandle(RecordHandle.RESERVED4_RECORD_HANDLE);
		rhs[5] = page1.makeRecordHandle(RecordHandle.RESERVED5_RECORD_HANDLE);

		for (int i = 0; i < RecordHandle.FIRST_RECORD_ID; i++)
		{
			try 
			{
				page1.recordExists(rhs[i], true);
				throw T_Fail.testFailMsg("record exists for invalid record " + rhs[i]);
			}
			catch (StandardException se)
			{ 
                /* expected */ 
            }


			try 
			{
				int slot = page1.getSlotNumber(rhs[i]);
				page1.fetchFromSlot(
					rhs[i], slot, new DataValueDescriptor[0], null, false);
				throw T_Fail.testFailMsg("fetched an invalid record " + rhs[i]);
			}
			catch (StandardException se)
			{ 
                /* expected */ 
            }

			try 
			{
				int slot = page1.getSlotNumber(rhs[i]);
				page1.updateAtSlot(slot, row1.getRow(), null);
				throw T_Fail.testFailMsg("updated an invalid record " + rhs[i]);
			}
			catch (StandardException se)
			{ 
                /* expected */ 
            }

			try 
			{
				int slot = page1.getSlotNumber(rhs[i]);
				page1.updateAtSlot(slot, row1.getRow(), BS_COL_0);
				throw T_Fail.testFailMsg("updated an invalid record field");
			}
			catch (StandardException se)
			{ 
                /* expected */ 
            }

			try 
			{
				int slot = page1.getSlotNumber(rhs[i]);
				page1.deleteAtSlot(slot, true, null);
				throw T_Fail.testFailMsg("delete an invalid record " + rhs[i]);
			}
			catch (StandardException se)
			{ 
                /* expected */ 
            }


			try 
			{
				page1.fetchNumFields(rhs[i]);
				throw T_Fail.testFailMsg("fetch num fields on invalid record " + rhs[i]);
			}
			catch (StandardException se)
			{ 
                /* expected */ 
            }

			try 
			{
				page1.getSlotNumber(rhs[i]);
				throw T_Fail.testFailMsg("got slot number of invalid record " + rhs[i]);
			}
			catch (StandardException se)
			{ 
                /* expected */ 
            }

		}
		PASS("P017");

		t_util.t_dropContainer(t, 0, cid);
		t_util.t_commit(t);	
		t.close();
	}

	/*
		P018

		this test exercises track # 590, test that copyRows successfully
        notices that a copy can't be done.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P018()
		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();

        // create container with 0 spare space, 1 minimum record size to easily
        // force absolutely full page.  Record id's are not reusable.
		long cid = t_util.t_addContainer(t, 0, 4096, 0, 1, false);

		t_util.t_commit(t);

		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

		Page page1 = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		Page page2 = t_util.t_addPage(c);
		long pid1 = page1.getPageNumber();
		long pid2 = page2.getPageNumber();

		t_util.t_checkEmptyPage(page2);

		// first fill up page 1

		int i = 0;
		int deleted = 0;
		RecordHandle rh;

		T_RawStoreRow row;

        // first insert and purge 100 rows, setting the next recid to be 
        // greater than 64.  This will cause the next
        // recid to be allocated to be greater than 64, this means 
        // that new recid's added to the page take 2 bytes rather than 1.
		for (i = 0; i < 100; i++)
		{
            row = new T_RawStoreRow("r" + i);

			rh = t_util.t_insertAtSlot(page1, 0, row);

            page1.purgeAtSlot(0, 1, logDataForPurges);
		}

        // fill up another page starting with "small" record id's.
		for (i = 0; true; i++)
		{
            row = new T_RawStoreRow("r" + i);

			rh = t_util.t_insertAtSlot(page2, i, row);

			if (rh == null)
				break;
		}

        // an attempt to copy all the rows should get an error, because the
        // recid's are bigger so all the records will not fit.
        try 
        {
            page2.copyAndPurge(page1, 0, page2.recordCount(), 0);

			throw T_Fail.testFailMsg(
                "copying rows with expanding recids should cause an exception");
        }
        catch (StandardException se)
        {
            // expect a out of space error.

        }

        // cleanup after first part of test.
		t_util.t_dropContainer(t, 0, cid);
		t_util.t_commit(t);

        // Now test that with the reusable record id's that the copy works.

        // create container with 0 spare space, 1 minimum record size to easily
        // force absolutely full page.  This container will allow 
        // reusable record id's which will mean the copy should succeed.
		cid = t_util.t_addContainer(t, 0, 4096, 0, 1, true);

		t_util.t_commit(t);

		c = t_util.t_openContainer(t, 0, cid, true);

		page1 = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		page2 = t_util.t_addPage(c);
		pid1 = page1.getPageNumber();
		pid2 = page2.getPageNumber();

		t_util.t_checkEmptyPage(page2);

		// first fill up page 1

		i = 0;

        // first insert and purge 100 rows, setting the next recid to be 
        // greater than 64.  This will cause the next
        // recid to be allocated to be greater than 64, this means 
        // that new recid's added to the page take 2 bytes rather than 1.
		for (i = 0; i < 100; i++)
		{
            row = new T_RawStoreRow("r" + i);

			rh = t_util.t_insertAtSlot(page1, 0, row);

            page1.purgeAtSlot(0, 1, logDataForPurges);
		}

        // fill up another page starting with "small" record id's.
		for (i = 0; true; i++)
		{
            row = new T_RawStoreRow("r" + i);

			rh = t_util.t_insertAtSlot(page2, i, row);

			if (rh == null)
				break;
		}
		long pnum2 = page2.getPageNumber();
		int numrows = page2.recordCount();

        // an attempt to copy all the rows should  get an error, 
        try 
        {
            // This copy should not succeed.
            page2.copyAndPurge(page1, 0, page2.recordCount(), 0);

			throw T_Fail.testFailMsg(
                "copying rows with expanding recids should cause an exception");
        }
        catch (StandardException se)
        {
            // expect a out of space error.
        }

		// now deallocated this page and get it to go thru a reuse cycle
		t_util.t_removePage(c, page1);
		t_util.t_commit(t);
		
		c = t_util.t_openContainer(t, 0, cid, true);
		page1 = t_util.t_addPage(c);
		int tries = 0;
		while(page1.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
		{
			REPORT("getting page " + page1.getPageNumber());

			t_util.t_commit(t);

			if (tries++ > 100)
				throw T_Fail.testFailMsg("failed to get back first page after "
										 + tries + " tries");
			c = t_util.t_openContainer(t, 0, cid, true);
			page1 = t_util.t_addPage(c);
		}
		page2 = t_util.t_getPage(c, pnum2);


		t_util.t_checkRecordCount(page2, numrows, numrows);
		t_util.t_checkEmptyPage(page1);

        // now the attempt to copy all the rows should succeed
        try 
        {
            page2.copyAndPurge(page1, 0, page2.recordCount(), 0);
        }
        catch (StandardException se)
        {
			throw T_Fail.testFailMsg(
                "copying rows with non-expanding recids should not cause an exception");
        }

		PASS("P018");

		t_util.t_dropContainer(t, 0, cid);	// cleanup
		t_util.t_commit(t);
		t.close();
	}

	/**
		Test bulk load and preallocation
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P019() throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, 0, 4096, 0, 1, false);
		t_util.t_commit(t);

		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

		// add page for bulk load
		Page p1 = c.addPage(ContainerHandle.ADD_PAGE_BULK);	
		long pnum1 = p1.getPageNumber();
		p1.unlatch();

		// since the interface does not guarentee that anything special will
		// actually happen, can't really test that. Just make sure that
		// everything else works
		Page p2 = c.addPage();
		long pnum2 = p2.getPageNumber();
		p2.unlatch();

		Page p3 = c.addPage(ContainerHandle.ADD_PAGE_BULK);	
		long pnum3 = p3.getPageNumber();
		p3.unlatch();

		Page p = c.getFirstPage(); // this is the first page that came with the
								   // container when it was created

		try
		{
			long pnum0 = p.getPageNumber();
			p.unlatch();
			p = c.getNextPage(pnum0);
			if (p.getPageNumber() != pnum1)
				throw T_Fail.testFailMsg("expected pagenum " + pnum1 + " got " + p.getPageNumber());
			p.unlatch();
			p = null;

			p = c.getNextPage(pnum1);
			if (p.getPageNumber() != pnum2)
				throw T_Fail.testFailMsg("expected pagenum " + pnum2 + " got " + p.getPageNumber());
			p.unlatch();
			p = null;

			p = c.getNextPage(pnum2);
			if (p.getPageNumber() != pnum3)
				throw T_Fail.testFailMsg("expected pagenum " + pnum3 + " got " + p.getPageNumber());
			p.unlatch();
			p = null;

			p = c.getNextPage(pnum3);
			if (p != null)
				throw T_Fail.testFailMsg("expected null page after " + pnum3 +
										 " got " + p.getPageNumber());

			// make sure rollback is unaffected
			if (testRollback)
			{
				t_util.t_abort(t);
				c = t_util.t_openContainer(t, 0, cid, true);
				p = t_util.t_getPage(c, pnum0);
				t_util.t_checkEmptyPage(p);
				p.unlatch();
				p = null;
			
				p = t_util.t_getPage(c, pnum1);
				t_util.t_checkEmptyPage(p);
				p.unlatch();
				p = null;

				p = t_util.t_getPage(c, pnum2);
				t_util.t_checkEmptyPage(p);
				p.unlatch();
				p = null;

				p = t_util.t_getPage(c, pnum3);
				t_util.t_checkEmptyPage(p);
				p.unlatch();
				p = null;

				p = t_util.t_getLastPage(c);
				if (p.getPageNumber() != pnum3)
					throw T_Fail.testFailMsg("expect last page to be " + pnum3
											 + " got " + p.getPageNumber());
				p.unlatch();
				p = null;
			}

			t_util.t_dropContainer(t, 0, cid);	// cleanup

		}
		finally
		{
			if (p != null)
				p.unlatch();
			p = null;
			t_util.t_commit(t);
			t.close();
		}
		PASS("P019 - container " + cid);

	}

	/**
		Test create container with initial page set to 100 pages
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P020() throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();
		Properties tableProperties = new Properties();
		tableProperties.put(Property.PAGE_SIZE_PARAMETER, Integer.toString(4096));
		tableProperties.put(RawStoreFactory.CONTAINER_INITIAL_PAGES, Integer.toString(100));

		long cid = t_util.t_addContainer(t, 0, tableProperties);
		if (cid < 0)
			throw T_Fail.testFailMsg("addContainer");

		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

		Page p1 = c.getFirstPage();
		if (p1.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
			throw T_Fail.testFailMsg("expect first page to have FIRST_PAGE_NUMBER");
		p1.unlatch();

		if (c.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
			throw T_Fail.testFailMsg("expect to have only 1 page allocated");

		t_util.t_dropContainer(t, 0, cid);	// cleanup
		t_util.t_commit(t);
		t.close();
		PASS("P020 - container " + cid);
	}

	/**
		Test preAllocate
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P021() throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, 0, 4096);
		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

		// now preallocate 10 pages
		c.preAllocate(10);

		Page p1 = c.getFirstPage();
		if (p1.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
			throw T_Fail.testFailMsg("expect first page to have FIRST_PAGE_NUMBER");
		p1.unlatch();

		if (c.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
			throw T_Fail.testFailMsg("expect to have only 1 page allocated");

		t_util.t_dropContainer(t, 0, cid);	// cleanup
		t_util.t_commit(t);
		t.close();
		PASS("P021 - container " + cid);
	}

	/**
		Test minimumRecordSize: this is to make sure that logRow and storeRecord
		are consistent with each other when it comes to reserve space.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P022()
		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();

        // create container with 4096 page size, 0 spare space, 9 minimum record size
		long cid = t_util.t_addContainer(t, 0, 4096, 0, 9, false);

		t_util.t_commit(t);

		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

		Page page1 = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		RecordHandle rh;
		T_RawStoreRow row;

        // insert records to fill the page.
		for (int i = 0; i < 60; i++) {
            row = new T_RawStoreRow("r" + i);
			rh = t_util.t_insertAtSlot(page1, 0, row);
		}

        // cleanup after first part of test.
		t_util.t_dropContainer(t, 0, cid);	// cleanup
		t_util.t_commit(t);
		t.close();
		PASS("P022");
	}
		
	/**
		Test overflowThreshold: this is to make sure that logRow and storeRecord
		are consistent with each other when it comes to reserve space.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P023(int segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();
		// create a container with 1K page, 0 spareSpace, and 0 minimumRecordSize
		long cid = t_util.t_addContainer(t, segment, 4096, 0, 0, false);
		t_util.t_commit(t);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		int overflowThreshold = 50;

		t_util.t_checkEmptyPage(page);

		// use default insert, not allowing overflow
		int insertFlag = Page.INSERT_INITIAL | Page.INSERT_DEFAULT;

		// test 1:
		// create a row that's under the threshold
		T_RawStoreRow r1 = new T_RawStoreRow(1);
		r1.setColumn(0, 200, REC_001);
		// insert the row twice, should fit on 1 page
		RecordHandle rh1 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag, overflowThreshold);
		if (rh1 == null)
			throw T_Fail.testFailMsg("insert of first long row failed.");
		RecordHandle rh2 = t_util.t_insertAtSlot(page, 1, r1, (byte) insertFlag, overflowThreshold);	
		if (rh2 == null)
			throw T_Fail.testFailMsg("insert of second long row failed.");
		t_util.t_checkFetch(page, rh1, r1);
		t_util.t_checkFetch(page, rh2, r1);
		page.unlatch();
		page = null;
		REPORT("test 1: 2 rows under threshold inserted...");

		// test 2:
		// get a new page
		page = t_util.t_addPage(c);
		// create a row that's over the threshold
		T_RawStoreRow r2 = new T_RawStoreRow(1);
		r2.setColumn(0, 2000, REC_001);
		// insert the row twice, should fail both inserts
		rh1 = t_util.t_insertAtSlot(page, 0, r2, (byte) insertFlag, overflowThreshold);
		if (rh1 != null)
        {
			throw T_Fail.testFailMsg("insert of 1st over threshold row should failed.");
        }
		rh2 = t_util.t_insertAtSlot(page, 0, r2, (byte) insertFlag, overflowThreshold);	
		if (rh2 != null)
			throw T_Fail.testFailMsg("insert of 2nd over threshold row should failed.");
		page.unlatch();
		page = null;
		REPORT("test 2: 2 rows over threshold not inserted...");

		// test 3:
		// get a new page
		page = t_util.t_addPage(c);
		// create a row with 2 columns, each column is under the threshold,
		// and the row is also under the threshold
		T_RawStoreRow r3 = new T_RawStoreRow(2);
		// create a row that's under the threshold
		r3.setColumn(0, 400, REC_001);
		r3.setColumn(1, 400, REC_001);
		// insert the row twice, should fit on 1 page
		rh1 = t_util.t_insertAtSlot(page, 0, r3, (byte) insertFlag, overflowThreshold);
		if (rh1 == null)
			throw T_Fail.testFailMsg("insert of 1st 2-column row failed.");
		rh2 = t_util.t_insertAtSlot(page, 1, r3, (byte) insertFlag, overflowThreshold);	
		if (rh2 == null)
			throw T_Fail.testFailMsg("insert of 2nd 2-column row failed.");
		page.unlatch();
		page = null;
		REPORT("test 3: 2 rows with 2 columns under the threshold inserted...");

		// test 4:
		// get a new page
		page = t_util.t_addPage(c);
		// create a row with 2 columns, each column is under the threshold,
		// but the row is over the threshold
		T_RawStoreRow r4 = new T_RawStoreRow(2);
		// create a row that's under the threshold
		r4.setColumn(0, 800, REC_001);
		r4.setColumn(1, 800, REC_001);
		// insert the row twice, should fit on 1 page
		rh1 = t_util.t_insertAtSlot(page, 0, r4, (byte) insertFlag, overflowThreshold);
		if (rh1 != null)
        {
            SanityManager.DEBUG_PRINT("bug", "page = " + page);
			throw T_Fail.testFailMsg("insert of 1st 2-column row (OT) should failed.");
        }
		rh2 = t_util.t_insertAtSlot(page, 0, r4, (byte) insertFlag, overflowThreshold);	
		if (rh2 != null)
			throw T_Fail.testFailMsg("insert of 2nd 2-column row (OT) should failed.");
		page.unlatch();
		page = null;
		REPORT("test 4: 2 rows with 2 columns over the threshold not inserted...");

        // cleanup after first part of test.
		t_util.t_dropContainer(t, segment, cid);	// cleanup
		t_util.t_commit(t);
		t.close();
		PASS("P023");
	}

	/**
	 * Test that latches are exclusive.
	 *
	 * @exception T_Fail Unexpected behaviour from the API
	 * @exception StandardException Unexpected exception from the implementation
	 */
	protected void P024(long segment) throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, segment, 4096, 0, 1, false);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);

		Page page1 = t_util.t_getLastPage(c);
		Page page2 = t_util.t_addPage(c);

		long p1 = page1.getPageNumber();
		long p2 = page2.getPageNumber();

		// check that we cannot get any of the latched pages
		t_util.t_checkGetLatchedPage(c, p1);
		t_util.t_checkGetLatchedPage(c, p2);

		page1.unlatch();

		// check that we still cannot get page2
		t_util.t_checkGetLatchedPage(c, p2);

		// we should be able to get page1
		page1 = t_util.t_getPage(c, p1);

		page1.unlatch();
		page2.unlatch();

		// cleanup
		t_util.t_dropContainer(t, segment, cid);

		t_util.t_commit(t);
		t.close();

		PASS("P024");
	}
		
	/**
		Insert small rows and update them so that they overflow a page.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P030(long segment) throws StandardException, T_Fail {

	 	Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		T_RawStoreRow r0 = new T_RawStoreRow(0);
		T_RawStoreRow r1 = new T_RawStoreRow((String) null);
		T_RawStoreRow r2 = new T_RawStoreRow("0123456789");

		t_util.t_insertAtSlot(page, 0, r0);
		t_util.t_insertAtSlot(page, 1, r1);
		t_util.t_insertAtSlot(page, 2, r2);

		t_util.t_checkRecordCount(page, 3, 3);
		page.unlatch();
		page = null;

		// check there is only one page
		if (c.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
			throw T_Fail.testFailMsg("an extra page has appeared in the container");
		t_util.t_commit(t);

		//
		// Update the row at slot 1 so that it fills most of the page
		//
		c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		t_util.t_checkStringLengthFetch(page, 1, -1);
		t_util.t_checkStringLengthFetch(page, 2, 10);

		t_util.t_checkFieldCount(page, 0, 0);
		t_util.t_checkFieldCount(page, 1, 1);
		t_util.t_checkFieldCount(page, 2, 1);

		T_RawStoreRow r1u = new T_RawStoreRow(String.valueOf(new char[1937]));
		page.updateAtSlot(1, r1u.getRow(), (FormatableBitSet) null);
		t_util.t_checkStringLengthFetch(page, 1, 1937);  // on page 1
		t_util.t_checkStringLengthFetch(page, 2, 10);	  // on page 1
		t_util.t_checkFieldCount(page, 0, 0);
		t_util.t_checkFieldCount(page, 1, 1);
		t_util.t_checkFieldCount(page, 2, 1);
		page.unlatch();
		// check there is only one page
		if (c.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
			throw T_Fail.testFailMsg("an extra page has appeared in the container");
		t_util.t_commit(t);

		//
		// Update the row at slot 2 so that it overflows
		//
		c = t_util.t_openContainer(t, segment, cid, true);
		T_RawStoreRow r2u = new T_RawStoreRow(String.valueOf(new char[1099])); // stored length is twice string lenght + 2
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		page.updateAtSlot(2, r2u.getRow(), (FormatableBitSet) null);
		t_util.t_checkStringLengthFetch(page, 1, 1937); // on page 1
		t_util.t_checkStringLengthFetch(page, 2, 1099); // on first overflow page
		t_util.t_checkFieldCount(page, 0, 0);
		t_util.t_checkFieldCount(page, 1, 1);
		t_util.t_checkFieldCount(page, 2, 1);
		page.unlatch();
		// check there is only one page
		if (c.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
			throw T_Fail.testFailMsg("an extra page has appeared in the container");
		t_util.t_commit(t);

		c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkStringLengthFetch(page, 1, 1937); // on page 1
		t_util.t_checkStringLengthFetch(page, 2, 1099); // on first overflow page
		t_util.t_checkFieldCount(page, 0, 0);
		t_util.t_checkFieldCount(page, 1, 1);
		t_util.t_checkFieldCount(page, 2, 1);
		// check there is only one page
		if (c.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
			throw T_Fail.testFailMsg("an extra page has appeared in the container");
		t_util.t_commit(t);

		//
		// Update the row at slot 0 so that it overflows onto the same page as the first
		// overflow.
		//
		c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		T_RawStoreRow r0u = new T_RawStoreRow(String.valueOf(new char[423]));
		page.updateAtSlot(0, r0u.getRow(), (FormatableBitSet) null);
		t_util.t_checkStringLengthFetch(page, 0, 423); // on first overflow page
		t_util.t_checkStringLengthFetch(page, 1, 1937); // on page 1
		t_util.t_checkStringLengthFetch(page, 2, 1099); // on first overflow page
		t_util.t_checkFieldCount(page, 0, 1);
		t_util.t_checkFieldCount(page, 1, 1);
		t_util.t_checkFieldCount(page, 2, 1);
		page.unlatch();
		// check there is only one page
		if (c.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
			throw T_Fail.testFailMsg("an extra page has appeared in the container");

		t_util.t_commit(t);

		//
		// Update the row at slot 0 that has already been overflowed
		// but keeping it on the same page
		//
		c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		r0u = new T_RawStoreRow(String.valueOf(new char[399]));
		page.updateAtSlot(0, r0u.getRow(), (FormatableBitSet) null);
		t_util.t_checkStringLengthFetch(page, 0, 399); // on first overflow page
		t_util.t_checkStringLengthFetch(page, 1, 1937); // on page 1
		t_util.t_checkStringLengthFetch(page, 2, 1099); // on first overflow page
		t_util.t_checkFieldCount(page, 0, 1);
		t_util.t_checkFieldCount(page, 1, 1);
		t_util.t_checkFieldCount(page, 2, 1);
		page.unlatch();
		// check there is only one page
		if (c.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
			throw T_Fail.testFailMsg("an extra page has appeared in the container");

		//
		// Update the row at slot 0 that has already been overflowed
		// but moving it to a new page
		//
		c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		r0u = new T_RawStoreRow(String.valueOf(new char[1400]));
		page.updateAtSlot(0, r0u.getRow(), (FormatableBitSet) null);
		t_util.t_checkStringLengthFetch(page, 0, 1400); // on second overflow page
		t_util.t_checkStringLengthFetch(page, 1, 1937); // on page 1
		t_util.t_checkStringLengthFetch(page, 2, 1099); // on first overflow page
		t_util.t_checkFieldCount(page, 0, 1);
		t_util.t_checkFieldCount(page, 1, 1);
		t_util.t_checkFieldCount(page, 2, 1);
		page.unlatch();
		// check there is only one page
		if (c.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
			throw T_Fail.testFailMsg("an extra page has appeared in the container");

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);

		t.close();

		PASS("P030: segment = " + segment);
	}

	/**
		Insert 4-column long rows into 1K pages, each column is less than a page.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P031(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		T_RawStoreRow r0 = new T_RawStoreRow(4);
		r0.setColumn(0, 256, REC_001);
		r0.setColumn(1, 256, REC_002);
		r0.setColumn(2, 256, REC_003);
		r0.setColumn(3, 256, REC_004);

		int insertFlag = Page.INSERT_INITIAL;
		insertFlag |= Page.INSERT_OVERFLOW;

		RecordHandle rh0 = null;
		try {
			rh0 = t_util.t_insertAtSlot(page, 0, r0, (byte) insertFlag);	
		} catch (StandardException se) {
			throw T_Fail.testFailMsg("insert of long row failed.");
		}

		if (rh0 == null) 
			throw T_Fail.testFailMsg("insert of first long row failed.");
		else {
			REPORT("about to check fetch...");
			DataValueDescriptor column = new SQLChar();
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 0, column, false, REC_001, 256);
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 1, column, false, REC_002, 256);
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 2, column, false, REC_003, 256);
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 3, column, false, REC_004, 256);
		}

		page.unlatch();

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);

		t.close();

		PASS("P031: segment = " + segment);
	}

	/**
		Insert 60-column long rows into 1K pages, each column is less than a page.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P032(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		int insertFlag = Page.INSERT_INITIAL;
		insertFlag |= Page.INSERT_OVERFLOW;

		T_RawStoreRow r0 = new T_RawStoreRow(60);
		for (int i = 0; i < 60; i++) {
			r0.setColumn(i, 1200, REC_001);
		}

		RecordHandle rh0 = null;
		try {
			rh0 = t_util.t_insertAtSlot(page, 0, r0, (byte) insertFlag);	
		} catch (StandardException se) {
			throw T_Fail.testFailMsg("insert of first long row failed.");
		}

		if (rh0 == null)
			throw T_Fail.testFailMsg("insert of a 60-column (300 bytes per column) row failed.");
		else {
			REPORT("about to check fetch the first long row inserted...");
			DataValueDescriptor column = new SQLChar();
			for (int i = 0; i < 60; i++) {
				t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, i, column, false, REC_001, 1200);
			}
		}

		// create a new row with 60 columns, and each column has REC_001 = "McLaren"
		for (int i = 0; i < 60; i++) {
			r0.setColumn(i, REC_001);
		}

		RecordHandle rh1 = null;
		try {
			rh1 = t_util.t_insertAtSlot(page, 1, r0, (byte) insertFlag);
		} catch (StandardException se) {
			throw T_Fail.testFailMsg("insert of second long row failed.");
		}

		if (rh1 == null) {
			throw T_Fail.testFailMsg("insert of a 60-column (~10 bytes per column) row failed.");
		} else {
			REPORT("about to check fetch the second long row inserted ...");
			DataValueDescriptor column = new SQLChar();
			for (int i = 0; i < 60; i++) {
				t_util.t_checkFetchColFromSlot(page, 1, i, column, false, REC_001);
			}
		}

		page.unlatch();

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);

		t.close();

		PASS("P032: segment = " + segment);
	}

	/**
		Insert 100-column long rows into 1K pages, each column is less than a page.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P033(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			t_util.t_checkEmptyPage(page);

			T_RawStoreRow r0 = new T_RawStoreRow(100);
			for (int i = 0; i < 100; i++) {
				r0.setColumn(i, REC_007);
			}

			int insertFlag = Page.INSERT_INITIAL | Page.INSERT_OVERFLOW;

			RecordHandle rh0 = null;
			try {
				rh0 = t_util.t_insertAtSlot(page, 0, r0, (byte) insertFlag);	
			} catch (StandardException se) {
				throw T_Fail.testFailMsg("insert of long row failed.");
			}

			if (rh0 == null) 
				throw T_Fail.testFailMsg("insert of first long row failed.");
			else {
				REPORT("about to check fetch...");
				DataValueDescriptor column = new SQLChar();
				for (int i = 0; i < 100; i++) {
					t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, i, column, false, REC_007);
				}
			}

			page.unlatch();
			page = null;
			if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
				t_util.t_dropContainer(t, segment, cid);	// cleanup
			}

		}
		finally
		{
			if (page != null)
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}

		PASS("P033: segment = " + segment);
	}

	/**
		Insert 401 column long row with many small columns in the beginning,
		and one large column at the end into 4K pages.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P034(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			t_util.t_checkEmptyPage(page);

			int insertFlag = Page.INSERT_INITIAL | Page.INSERT_OVERFLOW;

			T_RawStoreRow r1 = new T_RawStoreRow(401);
			for (int i = 0; i < 400; i++)
				r1.setColumn(i, REC_001);
			r1.setColumn(400, 1500, REC_001);
			RecordHandle rh = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			t_util.t_checkFetch(page, rh, r1);
			page.unlatch();
			page = null;
			if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
				t_util.t_dropContainer(t, segment, cid);	// cleanup
			}

		}
		finally
		{
			if (page != null)
				page.unlatch();

			t_util.t_commit(t);
			t.close();
		}

		PASS("P034: segment = " + segment);
	}

	/**
		Insert a single long column long row into a 1K page.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P035(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			t_util.t_checkEmptyPage(page);

			int insertFlag = Page.INSERT_INITIAL | Page.INSERT_OVERFLOW;

			T_RawStoreRow r1 = new T_RawStoreRow(1);
			// insert a long column
			r1.setColumn(0, 500, REC_001);
			RecordHandle rh1 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			t_util.t_checkFetch(page, rh1, r1);
			REPORT("row 1 inserted...");

			// insert a 6 column row, every other column is long
			T_RawStoreRow r2 = new T_RawStoreRow(6);
			r2.setColumn(0, 400, REC_001); // this takes 800 bytes
			r2.setColumn(1, 500, REC_002); // this takes 1000 bytes
			r2.setColumn(2, 400, REC_001);
			r2.setColumn(3, 500, REC_002);
			r2.setColumn(4, 400, REC_001);
			r2.setColumn(5, 500, REC_002);
			RecordHandle rh2 = t_util.t_insertAtSlot(page, 0, r2, (byte) insertFlag);	
			t_util.t_checkFetch(page, rh2, r2);
			REPORT("row 2 inserted...");

			// insert a long column
			r1.setColumn(0, 1500, REC_001);
			RecordHandle rh3 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			if (rh3 != null)
				throw T_Fail.testFailMsg("expect the 3rd row to not fit on page");

			page.unlatch();
			page = null;

			if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
				t_util.t_dropContainer(t, segment, cid);	// cleanup
			}

		}
		finally
		{
			if (page != null)
				page.unlatch();

			t_util.t_commit(t);
			t.close();
		}


		PASS("P035: segment = " + segment);
	}

	/**
		Test space reclaimation - purging of a long row gets back all
		the row pieces.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P036() throws StandardException, T_Fail
	{
		long segment = 0;
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			t_util.t_checkEmptyPage(page);

			int insertFlag = Page.INSERT_INITIAL | Page.INSERT_OVERFLOW;

			// insert a row with 400 columns from 200 to 400 bytes each and make it
			// sprawl across many pages.
			T_RawStoreRow r1 = new T_RawStoreRow(400);
			for (int i = 0; i < 400; i++)
				r1.setColumn(i, 100+i, REC_001);

			RecordHandle rh1 = t_util.t_insertAtSlot(page, 0, r1, (byte)insertFlag);
			t_util.t_checkFetch(page, rh1, r1);

			Page nextPage = t_util.t_addPage(c);
			long nextPageNumber = nextPage.getPageNumber();
			// deallocate it
			t_util.t_removePage(c, nextPage);

			REPORT("P036 - Nextpage is " + nextPageNumber);
			page.unlatch();
			page = null;
			t_util.t_commit(t);

			// See what the next page is.
			c = t_util.t_openContainer(t, segment, cid, true);

			// Now purge that first row.
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, 1, 1);
			page.purgeAtSlot(0, 1, logDataForPurges);

			t_util.t_checkEmptyPage(page);
			page.unlatch();
			page = null;
			t_util.t_commit(t);

			// give some time for post commit to finish
			t_util.t_wait(10);		// wait 10 milliseconds.

			// reinsert r1, it should use no extra page than last time.
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			RecordHandle rh2 = t_util.t_insertAtSlot(page, 0, r1, (byte)insertFlag);
			t_util.t_checkFetch(page, rh2, r1);
			page.unlatch();
			page = null;

			// now verify that it used up no more page than last time
			nextPage = t_util.t_addPage(c);
			long checkNextPageNumber = nextPage.getPageNumber();
			nextPage.unlatch();

			if (nextPageNumber != checkNextPageNumber)
				throw T_Fail.testFailMsg("fail to reuse row pieces expect next page=" + 
					nextPageNumber + " but got " + checkNextPageNumber );


			t_util.t_commit(t);

			// Purge them and roll them back via savepoint.  These should not
			// be reclaimed.
			c = t_util.t_openContainer(t, segment, cid, true);
			t.setSavePoint(SP1, null);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 1, 1);
			page.purgeAtSlot(0, 1, logDataForPurges);
			page.unlatch();
			page = null;

			// make sure we cannot get our hands on a page that is freed up by
			// the purge
			Page testPage = t_util.t_addPage(c);
			T_RawStoreRow testRow = new T_RawStoreRow(REC_001);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			t.rollbackToSavePoint(SP1, null);

			testPage = t_util.t_addPage(c);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			t_util.t_commit(t);
			t_util.t_wait(10);
			
			c = t_util.t_openContainer(t, segment, cid, true);

			testPage = t_util.t_addPage(c);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			//when container is in unlogged mode, the check is untru,
			//because rollback to save point would have done nothing.
			//so purge was not rolled back. The row does not exist any more
			if((openMode & ContainerHandle.MODE_UNLOGGED) == 0)
			{
				page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
				if(logDataForPurges)
				{
					t_util.t_checkFetch(page, rh2, r1);			
				}else
				{
					//when data is not logged for purges ,
					//in this particular first four columns becomes null becuase
					//first 4 fileds data are on the 1st page and do not get 
                    //logged during pruge and
					//the rest of the columns gets the data back because they
					//are removed page by page.
					T_RawStoreRow r1_wnl = new T_RawStoreRow(400);
					for (int i = 0; i < 18; i++)
						r1_wnl.setColumn(i, 4, REC_NULL);
					for (int i = 18; i < 400; i++)
						r1_wnl.setColumn(i, 100+i, REC_001);
					t_util.t_checkFetch(page, rh2, r1_wnl);		
				}
				page.unlatch();
				page = null;
			}


			t_util.t_dropContainer(t, segment, cid);	// cleanup

		}
		finally	
		{
			if (page != null)
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}

		PASS("P036");
	}


	/**
		Test space reclaimation - purging of a row with serveral long columns
		get back all the column chains.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P037() throws StandardException, T_Fail
	{
		// Insert the 3 rows in P035, then purge them and reinsert them and
		// make sure it reuses all the pages from last time.

		long segment = 0;
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			t_util.t_checkEmptyPage(page);

			int insertFlag = Page.INSERT_INITIAL | Page.INSERT_OVERFLOW;
			T_RawStoreRow r1 = new T_RawStoreRow(1);
			// insert a long column
			r1.setColumn(0, 5500, REC_001);
			RecordHandle rh1 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			t_util.t_checkFetch(page, rh1, r1);

			// insert a 6 column row, every other column is long, sizes 
            // picked so that 2 rows fit, but 3rd row won't fit even if whole
            // row is overflowed.
			T_RawStoreRow r2 = new T_RawStoreRow(6);
			r2.setColumn(0, 660, REC_001); // this takes ~1320 bytes
			r2.setColumn(1, 5000, REC_002); // this takes ~10000 bytes
			r2.setColumn(2, 660, REC_001);
			r2.setColumn(3, 5000, REC_002);
			r2.setColumn(4, 660, REC_001);
			r2.setColumn(5, 5000, REC_002);
			RecordHandle rh2 = t_util.t_insertAtSlot(page, 0, r2, (byte) insertFlag);	
			t_util.t_checkFetch(page, rh2, r2);


			// insert a long column - this should fail
			RecordHandle rh3 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			if (rh3 != null)
            {
                throw T_Fail.testFailMsg("expect the 3rd row to not fit on page");
            }

			page.unlatch();
			page = null;

			Page nextPage = t_util.t_addPage(c);
			long nextPageNumber = nextPage.getPageNumber();
			// deallocate it
			t_util.t_removePage(c, nextPage);

			REPORT("P037 - Nextpage is " + nextPageNumber);

			t_util.t_commit(t);

			// now purge them
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, 2, 2);
			page.purgeAtSlot(0, 2, logDataForPurges);
			t_util.t_checkEmptyPage(page);
			page.unlatch();
			page = null;

			t_util.t_commit(t);

			// give some time for post commit to finish
			t_util.t_wait(10);		// wait 10 milliseconds.

			// reinsert all 3 of them again, exactly the same way.
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			rh1 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			rh2 = t_util.t_insertAtSlot(page, 0, r2, (byte) insertFlag);	
			rh3 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			t_util.t_checkFetch(page, rh1, r1);
			t_util.t_checkFetch(page, rh2, r2);
			if (rh3 != null)
				throw T_Fail.testFailMsg("expect the 3rd row to not fit on page");
			page.unlatch();
			page = null;

			nextPage = t_util.t_addPage(c);
			long checkNextPageNumber = nextPage.getPageNumber();
			nextPage.unlatch();

			if (nextPageNumber != checkNextPageNumber)
				throw T_Fail.testFailMsg("fail to reuse row pieces expect next page=" + 
					 nextPageNumber + " but got " + checkNextPageNumber );

			t_util.t_commit(t);

			// Purge them and roll them back via savepoint.  These should not
			// be reclaimed.
			c = t_util.t_openContainer(t, segment, cid, true);

			t.setSavePoint(SP1, null);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			
			t_util.t_checkRecordCount(page, 2, 2);
			page.purgeAtSlot(0, 2, logDataForPurges);
			t_util.t_checkEmptyPage(page);

			page.unlatch();
			page = null;

			// make sure we cannot get our hands on a page that is freed up by
			// the purge
			Page testPage = t_util.t_addPage(c);
			T_RawStoreRow testRow = new T_RawStoreRow(REC_001);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			t.rollbackToSavePoint(SP1, null);

			testPage = t_util.t_addPage(c);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			t_util.t_commit(t);

			// give some time for post commit to finish
			t_util.t_wait(10);

			// check to make sure post commit did not reclaim those rows.
			c = t_util.t_openContainer(t, segment, cid, true);

			testPage = t_util.t_addPage(c);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			
			t_util.t_checkRecordCount(page, 2, 2);
			t_util.t_checkFetch(page, rh1, r1);
		
			if(logDataForPurges)
				t_util.t_checkFetch(page, rh2, r2);
			else{
				
				// During purges when data is not logged when slots are purged
				// they become null on rollback and some cases like long columns
				// we remove the wholepage on rollback we get the data back.
				T_RawStoreRow r2_wnl = new T_RawStoreRow(6);
				r2_wnl.setColumn(0, 4, REC_NULL); 
				r2_wnl.setColumn(1, 5000, REC_002); 
				r2_wnl.setColumn(2, 4, REC_NULL);
				r2_wnl.setColumn(3, 5000, REC_002);
				r2_wnl.setColumn(4, 4, REC_NULL);
				r2_wnl.setColumn(5, 5000, REC_002);
				t_util.t_checkFetch(page, rh2, r2_wnl);
			}
				
			page.unlatch();
			page = null;

			t_util.t_dropContainer(t, segment, cid);	// cleanup

		}
		finally
		{
			if (page != null)
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}

		PASS("P037");

	}

	/**
		Test space reclaimation - rollback of an insert (with purge) of a row
		that overflows and with long column get back all the space in the row
		and column chain. 

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P038() throws StandardException, T_Fail
	{
		long segment = 0;

		// Insert the 3 rows in P035, then abort the insert.
		// Reinsert them and make sure it reuses all the pages from last time. 
		Transaction t = t_util.t_startTransaction();		
		long cid = t_util.t_addContainer(t, segment, 4096);
		t_util.t_commit(t);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			t_util.t_checkEmptyPage(page);

			int insertFlag = Page.INSERT_INITIAL | Page.INSERT_OVERFLOW |
				Page.INSERT_UNDO_WITH_PURGE; 

			T_RawStoreRow r1 = new T_RawStoreRow(1);
			// insert a long column
			r1.setColumn(0, 1500, REC_001);
			RecordHandle rh1 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	

			// insert a 6 column row, every other column is long
			T_RawStoreRow r2 = new T_RawStoreRow(6);
			r2.setColumn(0, 400, REC_001); // this takes ~800 bytes
			r2.setColumn(1, 500, REC_002); // this takes ~1000 bytes
			r2.setColumn(2, 400, REC_001);
			r2.setColumn(3, 500, REC_002);
			r2.setColumn(4, 400, REC_001);
			r2.setColumn(5, 500, REC_002);
			RecordHandle rh2 = t_util.t_insertAtSlot(page, 0, r2, (byte) insertFlag);	

			// insert a long column
			RecordHandle rh3 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			if (rh3 != null)
				throw T_Fail.testFailMsg("expect the 3rd row to not fit on page");
			page = null;

			Page nextPage = t_util.t_addPage(c);
			long nextPageNumber = nextPage.getPageNumber();
			// deallocate it
			t_util.t_removePage(c, nextPage);

			REPORT("P038 - Nextpage is " + nextPageNumber);

			t_util.t_abort(t);

			// the abort rolled back the removal of nextPage also, redo the removed
			c = t_util.t_openContainer(t, segment, cid, true);		
			nextPage = t_util.t_getPage(c, nextPageNumber);
			t_util.t_removePage(c, nextPage);
			t_util.t_commit(t);

			// reinsert the 3 rows, they should not take up any more space than
			// last time.
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkEmptyPage(page);

			rh1 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			rh2 = t_util.t_insertAtSlot(page, 0, r2, (byte) insertFlag);	
			rh3 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
		
			t_util.t_checkFetch(page, rh1, r1);
			t_util.t_checkFetch(page, rh2, r2);
			if (rh3 != null)
				throw T_Fail.testFailMsg("expect the 3rd row to not fit on page");

			page.unlatch();
			page = null;

			nextPage = t_util.t_addPage(c);
			long checkNextPageNumber = nextPage.getPageNumber();
			nextPage.unlatch();


			if (nextPageNumber != checkNextPageNumber)
				throw T_Fail.testFailMsg("fail to reuse row pieces expect next page=" + 
					nextPageNumber + " but got " + checkNextPageNumber );

			t_util.t_dropContainer(t, segment, cid);	// cleanup

		}
		finally	
		{
			if (page != null)
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}

		PASS("P038");
	}

	/**
		Test space reclaimation - shrink a head row piece.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P039() throws StandardException, T_Fail
	{
		// insert 3 2K rows of 3 columns,  size (100, 1500, 400), into an 8K
		// page, then fill up the page with 1K rows.  
		// 
		// 1. Update the first 2K row so that the 2nd and 3rd column gets moved
		// to another page.  See that we can insert at least 1 more 1K row into
		// the page.
		// 
		// 2. Update the second 2K row so that the 2nd column becomes a long
		// column.  See that we can insert at least 1 more 1K row into the
		// page. 
		//
		// 3. Update the third 2K row so that the column size shrinks to (200,
		// 200, 200).  See that we can insert at least 1 more 1K row into the
		// page.

		long segment = 0;
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, segment, 8*1024);

		T_RawStoreRow bigRow = new T_RawStoreRow(3);
		bigRow.setColumn(0,  50, REC_001); // remember each char takes 2 bytes
		bigRow.setColumn(1, 750, REC_002);
		bigRow.setColumn(2, 200, REC_003);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			RecordHandle rh1 = t_util.t_insert(page, bigRow);
			RecordHandle rh2 = t_util.t_insert(page, bigRow);
			RecordHandle rh3 = t_util.t_insert(page, bigRow);

			t_util.t_checkFetch(page, rh1, bigRow);
			t_util.t_checkFetch(page, rh2, bigRow);
			t_util.t_checkFetch(page, rh3, bigRow);

			// now fill up the page with smaller rows
			T_RawStoreRow smallRow = new T_RawStoreRow(1);
			smallRow.setColumn(0, 500, REC_004);

			while(page.spaceForInsert())
			{
				if (t_util.t_insert(page, smallRow) == null)
					break;
			}
			REPORT("P039: " + (page.recordCount()-3) + " small rows have been inserted");
			page.unlatch();
			page = null;

			t_util.t_commit(t);
		
			// (1) update rh1 so that column 2 and 3 are moved off page
			bigRow.setColumn(1, 2000, REC_005);
			c = t_util.t_openContainer(t, segment, cid, true);

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			int slot1 = page.getSlotNumber(rh1);
			page.updateAtSlot(slot1, bigRow.getRow(), null);
			t_util.t_checkFetch(page, rh1, bigRow);
			page.unlatch();
			page = null;
			t_util.t_commit(t);

			t_util.t_wait(10);		// wait for post commit to get processed.

			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			if (t_util.t_insert(page, smallRow) == null)
				throw T_Fail.testFailMsg("expect row to have shrunk (1)");

			// fill it up again 
			while(page.spaceForInsert())
			{
				if (t_util.t_insert(page, smallRow) == null)
					break;
			}
			REPORT("P039: " + (page.recordCount()-3) + " small rows have been inserted");
			
			page.unlatch();
			page = null;

			t_util.t_commit(t);

			// (2) update rh2 so that column 2 becomes a long column
			FormatableBitSet colList = new FormatableBitSet(2);
			colList.set(1);			// update column 1, the second column
            // use sparse rows
			T_RawStoreRow partialRow = new T_RawStoreRow(2);
			partialRow.setColumn(1, 8000, REC_006);

			c = t_util.t_openContainer(t, segment, cid, true);

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			int slot2 = page.getSlotNumber(rh2);
			page.updateAtSlot(slot2, partialRow.getRow(), colList);

			bigRow.setColumn(1, 8000, REC_006);
			t_util.t_checkFetch(page, rh2, bigRow);
			page.unlatch();
			page = null;
			t_util.t_commit(t);

			t_util.t_wait(10);		// wait for post commit to get processed.

			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			if (t_util.t_insert(page,smallRow) == null)
				throw T_Fail.testFailMsg("expect row to have shrunk (2)");

			// fill it up again 
			while(page.spaceForInsert())
			{
				if (t_util.t_insert(page, smallRow) == null)
					break;
			}
			REPORT("P039: " + (page.recordCount()-3) + " small rows have been inserted");

			page.unlatch();
			page = null;

			t_util.t_commit(t);

			// (3) - update rh3 to have (200, 400, 400) bytes columns
			bigRow.setColumn(0, 100, REC_001);
			bigRow.setColumn(1, 200, REC_002);
			bigRow.setColumn(2, 200, REC_003);

			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			int slot3 = page.getSlotNumber(rh3);
			page.updateAtSlot(slot3, bigRow.getRow(), null);
			t_util.t_checkFetch(page, rh3, bigRow);
			page.unlatch();
			page = null;
			t_util.t_commit(t);

			t_util.t_wait(10);		// wait for post commit to get processed.

			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			if (t_util.t_insert(page,smallRow) == null)
				throw T_Fail.testFailMsg("expect row to have shrunk (3)");
			page.unlatch();
			page = null;

			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}
		finally	
		{
			if (page != null)
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}

		PASS("P039");
	}

	/**
		Test space reclaimation - shrink a non head row piece.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P040() throws StandardException, T_Fail
	{
		// Manufacture a row that has a small head row piece, a large 2nd row
		// piece and a small third row piece.
		// Using the same head page, add a second row that has a small head row
		// piece and a medium sized 2nd row piece, a new overflow page should
		// be allocated.
		// Update the first row to now have a small 2nd row piece.
		// Using the same head page, add a third row that has a small head row
		// piece and a medium sized 2nd row piece, no new overflow page should
		// be allocated.

		long segment = 0;
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, 0, 4096);
		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);

		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			int insertFlag = Page.INSERT_INITIAL | Page.INSERT_OVERFLOW |
				Page.INSERT_UNDO_WITH_PURGE;

			T_RawStoreRow row1 = new T_RawStoreRow(3);
			row1.setColumn(0, 400, REC_001);
			row1.setColumn(1, 800, REC_002); // this takes ~1600 bytes
			row1.setColumn(2, 400, REC_003);
			RecordHandle rh1 = t_util.t_insertAtSlot(page, 0, row1, (byte)insertFlag);

			t_util.t_checkFetch(page, rh1, row1);

			page.unlatch();
			page = null;
			t_util.t_commit(t);

			c = t_util.t_openContainer(t, segment, cid, true);
			Page nextPage = t_util.t_addPage(c);

			// remember where next page is
			long nextPageNumber = nextPage.getPageNumber();
			t_util.t_removePage(c, nextPage);
			t_util.t_commit(t);

			T_RawStoreRow row2 = new T_RawStoreRow(3);
			row2.setColumn(0, 1200, REC_001);
			row2.setColumn(1, 1200, REC_002);
			row2.setColumn(2, 400, REC_003);

			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			RecordHandle rh2 = t_util.t_insertAtSlot(page, 1, row2, (byte)insertFlag);

			t_util.t_checkFetch(page, rh2, row2);
			page.unlatch();
			page = null;

			// this should have allocated more overflow page
			nextPage = t_util.t_addPage(c);
			long checkNextPageNumber = nextPage.getPageNumber();
			if (checkNextPageNumber == nextPageNumber)
				throw T_Fail.testFailMsg("expected to allocate more pages");
			t_util.t_removePage(c, nextPage);
			t_util.t_commit(t);

			// now this is the next free page
			nextPageNumber = checkNextPageNumber;

			// shrink first row 2nd column, and second row 1st column so we
			// have space on both the first and the second page on the row
			// chain.
            // use sparse rows
			T_RawStoreRow partialRow = new T_RawStoreRow(2);
			partialRow.setColumn(1, 400, REC_004);

			T_RawStoreRow partialRow2 = new T_RawStoreRow(2);
			partialRow2.setColumn(0, 400, REC_004);

			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			FormatableBitSet colList = new FormatableBitSet(2);
			colList.set(1);		// update first row column 1, the second column
			int slot1 = page.getSlotNumber(rh1);
			page.updateAtSlot(slot1, partialRow.getRow(), colList);

			colList.clear(1);
			colList.set(0);		// update second row column 0, the first column
			int slot2 = page.getSlotNumber(rh2);
			page.updateAtSlot(slot2, partialRow2.getRow(), colList);

			// verify the update worked.
			row1.setColumn(1, 400, REC_004);
			row2.setColumn(0, 400, REC_004);
			t_util.t_checkFetch(page, rh1, row1);
			t_util.t_checkFetch(page, rh2, row2);

			page.unlatch();
			page = null;

			t_util.t_commit(t);
			t_util.t_wait(10);	// give post commit a chance to work

			// We think the head row should have 2 200 bytes row.
			// One of the overflow row piece chain has an overflow page with a
			// 200 bytes row followed by another overflow page with a 200 bytes
			// row.
			// The other overflow row piece should have 1 600 bytes row and no
			// other overflow page.
			T_RawStoreRow row3 = new T_RawStoreRow(2);
			row3.setColumn(0, 400, REC_001);
			row3.setColumn(1, 800, REC_002);
			// We think this should select the first overflow chain.
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			if (!page.spaceForInsert())
				throw T_Fail.testFailMsg("No space for insert after shrink row");

			RecordHandle rh3 = t_util.t_insertAtSlot(page, 1, row3, (byte)insertFlag);
			t_util.t_checkFetch(page, rh3, row3);

			page.unlatch();
			page = null;

			// this should not allocate more overflow pages
			nextPage = t_util.t_addPage(c);
			checkNextPageNumber = nextPage.getPageNumber();
			if (checkNextPageNumber != nextPageNumber)
				throw T_Fail.testFailMsg("not expected to allocate more pages "
										 + nextPageNumber + "," + checkNextPageNumber);

			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}
		finally
		{
			if (page != null)
				page.unlatch();

			t_util.t_commit(t);
			t.close();
		}
		PASS("P040");
	}

	/**
		Test space reclaimation - update a long column to another long column.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P041() throws StandardException, T_Fail
	{
		// Make a row with several long columns, on different row pieces.
		// Update these long columns - to other long columns or to short
		// columns.  Remember what the next page is.
		// Update these long columns to the original long column (in length),
		// we shouldn't be adding any more pages.

		long segment = 0;
		Transaction t = t_util.t_startTransaction();		
		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			t_util.t_checkEmptyPage(page);

			int insertFlag = Page.INSERT_INITIAL | Page.INSERT_OVERFLOW;

			// insert a 6 column row, every other column is long
			T_RawStoreRow r1 = new T_RawStoreRow(6);
			r1.setColumn(0, 400, REC_001); // this takes ~800 bytes
			r1.setColumn(1, 500, REC_002); // this takes ~1000 bytes
			r1.setColumn(2, 400, REC_001);
			r1.setColumn(3, 500, REC_002);
			r1.setColumn(4, 400, REC_001);
			r1.setColumn(5, 500, REC_002);
			RecordHandle rh1 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			t_util.t_checkFetch(page, rh1, r1);

			// update column 0,1,2 to short columns, columns 3, 4, 5 to other
			// long columns.
			T_RawStoreRow r2 = new T_RawStoreRow(6);
			r2.setColumn(0, 100, REC_003);
			r2.setColumn(1, 100, REC_004);
			r2.setColumn(2, 100, REC_003);
			r2.setColumn(3, 500, REC_005);
			r2.setColumn(4, 500, REC_006);
			r2.setColumn(5, 500, REC_005);

			int slot1 = page.getSlotNumber(rh1);
			page.updateAtSlot(slot1, r2.getRow(), null);
			t_util.t_checkFetch(page, rh1, r2);

			page.unlatch();
			page = null;

			Page nextpage = t_util.t_addPage(c);
			long nextPageNumber = nextpage.getPageNumber();
			t_util.t_removePage(c, nextpage);

			t_util.t_commit(t);
			t_util.t_wait(10);	// let post commit work
			
			// now update to original long rows, should not take any more
			// space.
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkFetch(page, rh1, r2);
			
			t.setSavePoint(SP1, null);

			slot1 = page.getSlotNumber(rh1);
			page.updateAtSlot(slot1, r1.getRow(), null);
			t_util.t_checkFetch(page, rh1, r1);
			page.unlatch();
			page = null;

			nextpage = t_util.t_addPage(c);
			long checkNextPageNumber = nextpage.getPageNumber();
			nextpage.unlatch();
			if (checkNextPageNumber != nextPageNumber)
				throw T_Fail.testFailMsg("expect next page to be unchanged");

			// now roll back the update via savepoint.
			t.rollbackToSavePoint(SP1, null);

			t_util.t_commit(t);
			t_util.t_wait(10);	// make sure post commit don't 
								// reclaim things that are not garbage.

			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkFetch(page, rh1, r2);
			page.unlatch();
			page = null;

			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}
		finally
		{
			if (page != null)
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}

		PASS("P041");

	}

	/**
		Test space reclaimation - rollback of an update that create a long
		column. 

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P042() throws StandardException, T_Fail
	{
		// Make a row with a short column.  Remember the next page.
		// Update it to a long column, roll back.  See that the next page goes
		// back to before the update.
		//
		// Update the row so that it overflows to another page and have long
		// column there.  Remember the next page.  Update the long column to
		// another long column.  Rollback the update.  See that the next page
		// goes back to before the update.
		// 
		long segment = 0;
		Transaction t = t_util.t_startTransaction();		
		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		try
		{
			T_RawStoreRow smallRow = new T_RawStoreRow(REC_001);
			RecordHandle rh1 = t_util.t_insert(page, smallRow);
			t_util.t_commit(t);

			c = t_util.t_openContainer(t, segment, cid, true);
			Page nextPage = t_util.t_addPage(c);
			long nextPageNumber = nextPage.getPageNumber();
			// deallocate it
			t_util.t_removePage(c, nextPage);
			t_util.t_commit(t);			

			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			T_RawStoreRow bigRow = new T_RawStoreRow(1);
			bigRow.setColumn(0, 6400, REC_001);
			int slot1 = page.getSlotNumber(rh1);
			page.updateAtSlot(slot1, bigRow.getRow(), null);
			t_util.t_checkFetch(page,rh1, bigRow);
			page.unlatch();
			page = null;

			Page checkGrow = t_util.t_addPage(c);
			long checkGrowPageNumber = checkGrow.getPageNumber();
			if (checkGrowPageNumber == nextPageNumber)
				throw T_Fail.testFailMsg("expect to have allocated more pages");
			t_util.t_removePage(c, checkGrow);

			t_util.t_abort(t);

			t_util.t_wait(10);
			c = t_util.t_openContainer(t, segment, cid, true);

			// the abort rolled back the removePage, remove it again.
			checkGrow = t_util.t_getPage(c, checkGrowPageNumber);
			t_util.t_removePage(c, checkGrow);

			nextPage = t_util.t_addPage(c);
			if (nextPage.getPageNumber() != nextPageNumber)
				throw T_Fail.testFailMsg(
				"rollback of update to long column did not release the long column chain pages");

			t_util.t_removePage(c, nextPage);
			t_util.t_commit(t);

			T_RawStoreRow row2 = new T_RawStoreRow(6);
			row2.setColumn(0, 1600, REC_001); // this takes ~3200 bytes
			row2.setColumn(1, 2000, REC_002); // this takes ~4000 bytes
			row2.setColumn(2, 1600, REC_001);
			row2.setColumn(3, 2000, REC_002);
			row2.setColumn(4, 1600, REC_001);
			row2.setColumn(5, 2000, REC_002);

			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t.setSavePoint(SP1, null);
			slot1 = page.getSlotNumber(rh1);
			page.updateAtSlot(slot1, row2.getRow(), null);

			nextPage = t_util.t_addPage(c);
			nextPageNumber = nextPage.getPageNumber();
			t_util.t_removePage(c, nextPage);			

			t.rollbackToSavePoint(SP1, null); // this should free up some pages

			nextPage = t_util.t_getPage(c, nextPageNumber);
			t_util.t_removePage(c, nextPage);			

			t_util.t_commit(t);	
			t_util.t_wait(10);

			c = t_util.t_openContainer(t, segment, cid, true);
			Page checkNextPage = t_util.t_addPage(c);
			if (checkNextPage.getPageNumber() == nextPageNumber)
				throw T_Fail.testFailMsg(
					"expect some pages to be freed by update rollback");
			t_util.t_removePage(c, checkNextPage);
			t_util.t_commit(t);

			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkFetch(page, rh1, smallRow);

			// update row so that it has overflow rows and long columns
			slot1 = page.getSlotNumber(rh1);
			page.updateAtSlot(slot1, row2.getRow(), null);

			// remember the next page
			nextPage = t_util.t_addPage(c);
			nextPageNumber = nextPage.getPageNumber();
			t_util.t_removePage(c, nextPage);

			t_util.t_commit(t);

			// now update columns 0, 1, 4  to long columns and roll it back. 
			T_RawStoreRow row3 = new T_RawStoreRow(5);
			row3.setColumn(0, 4000, REC_003);
			row3.setColumn(1, 4000, REC_004);
			row3.setColumn(2, REC_001);
			row3.setColumn(3, REC_001);
			row3.setColumn(4, 4000, REC_003);
			
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			slot1 = page.getSlotNumber(rh1);
			page.updateAtSlot(slot1, row3.getRow(), null);
			t_util.t_checkFetch(page, rh1, row3);
			page.unlatch();
			page = null;

			t_util.t_abort(t);
			t_util.t_wait(10);

			c = t_util.t_openContainer(t, segment, cid, true);
			nextPage = t_util.t_addPage(c);
			if (nextPage.getPageNumber() != nextPageNumber)
				throw T_Fail.testFailMsg("expect pages to be freed by update rollback");
			nextPage.unlatch();

			t_util.t_dropContainer(t, segment, cid);
		}
		finally	
		{
			if (page != null && page.isLatched())
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}
		PASS("P042");

	}

	/**
		Test space reclaimation - rollback of an update that create a new row
		piece. 
	 */
	protected void P043()
	{
		// this space cannot be reclaimed.
	}


	/** 
		Test that post commit processor does not stubbify a drop table that is
		rolled back in a savepoint
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/
	protected void P050()
		 throws StandardException, T_Fail
	{
		if (!testRollback)
			return;

		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t,0);
		t_util.t_commit(t);

		t.setSavePoint(SP1, null);

		t.dropContainer(new ContainerKey(0, cid));
		t.rollbackToSavePoint(SP1, null);

		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
		t_util.t_addPage(c);
		t_util.t_commit(t);

		long cid2 = t_util.t_addContainer(t, 0);
		c = t_util.t_openContainer(t, 0, cid2, true);
		t_util.t_addPage(c);
		t_util.t_addPage(c);
		t_util.t_addPage(c);
		t_util.t_addPage(c);
		t_util.t_commit(t);

		c = t_util.t_openContainer(t, 0, cid, false); // it should not be stubbified...

		PASS("P050");

		t_util.t_dropContainer(t, 0, cid);	// cleanup - commit it for real
		t_util.t_dropContainer(t, 0, cid2);	// cleanup - commit it for real

		t_util.t_commit(t);
		t.close();

	}

	/**
		Test rollback of Page.insert
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/
	protected void P051()
		 throws StandardException, T_Fail
	{
		if (!testRollback)
			return;

		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t,0);
		t_util.t_commit(t);


		T_RawStoreRow row = new T_RawStoreRow(REC_001);
		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// first insert and check that an abort leaves the row there
		RecordHandle rh1 = t_util.t_insert(page, row);

		t_util.t_commit(t);

		c = t_util.t_openContainer(t, 0, cid, true);

		t_util.t_checkFetch(c, rh1, REC_001);

		row = new T_RawStoreRow(REC_002);

		RecordHandle rh2 = t_util.t_insert(c, row);

		t_util.t_checkFetch(c, rh1, REC_001);
		t_util.t_checkFetch(c, rh2, REC_002);

		t_util.t_abort(t);

		c = t_util.t_openContainer(t, 0, cid, true);
		t_util.t_checkFetch(c, rh1, REC_001);

		page = t_util.t_getPage(c, rh2.getPageNumber());
		if (page.recordExists(rh2, false)) {
			throw T_Fail.testFailMsg("record insert was not undone");
		}
		page.unlatch();

		PASS("P051");

		t_util.t_dropContainer(t, 0, cid);	// cleanup

		t_util.t_commit(t);
		t.close();
	}

	/**
		Test insertAtSlot that rolls back with a purge

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/
	protected void P053()
		 throws StandardException, T_Fail
	{
		if (!testRollback)
			return;
		Transaction t = t_util.t_startTransaction();

		try
		{
			long cid = t_util.t_addContainer(t,0);
			t_util.t_commit(t);

			
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_addPage(c);

			T_RawStoreRow row0 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
			T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
			
			t_util.t_insertAtSlot(page, 0, row0, Page.INSERT_UNDO_WITH_PURGE);

			if (t_util.t_insertAtSlot(page, 1, row1) == null)
				return;

			if (t_util.t_insertAtSlot(page, 2, row2, Page.INSERT_UNDO_WITH_PURGE) == null)
				return;

			if (t_util.t_insertAtSlot(page, 3, row3) == null)
				return;
	
			if (t_util.t_insertAtSlot(page, 4, row4, Page.INSERT_UNDO_WITH_PURGE) == null)
				return;
	
			int fillerRows = 0;
			while (page.spaceForInsert())
			{
				t_util.t_insertAtSlot(page, 4, row4);
				fillerRows++;
			}

			t_util.t_checkRecordCount(page, fillerRows + 5, fillerRows + 5);
			t_util.t_abort(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getLastPage(c);

			// 2 + fillerRows deleted, 3 purged, 0 nondeleted
			t_util.t_checkRecordCount(page, 2 + fillerRows, 0);

			// since I just purged them, there must be space for re-inserting them
			t_util.t_insert(page, row0);
			t_util.t_insert(page, row2);
			t_util.t_insert(page, row4);

			t_util.t_checkRecordCount(page, 5 + fillerRows, 3);

			page.unlatch();
			PASS("P053");

			t_util.t_dropContainer(t, 0, cid);	// cleanup
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/**
		Test internal transaction 

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/
	protected void P054()
		 throws StandardException, T_Fail
	{
		if (!testRollback)
			return;

		ContextManager previousCM = contextService.getCurrentContextManager();

		ContextManager cm1 = contextService.newContextManager();
		contextService.setCurrentContextManager(cm1);

		Transaction tuser = t_util.t_startTransaction();
		Transaction tinternal = null;

		try
		{

			long cid1 = t_util.t_addContainer(tuser, 0);
			ContainerHandle c1 = t_util.t_openContainer(tuser, 0, cid1, true);
			Page p1 = t_util.t_addPage(c1);
			t_util.t_commit(tuser);

			// insert a row using user transaction 
			T_RawStoreRow row = new T_RawStoreRow(REC_001);

			c1 = t_util.t_openContainer(tuser, 0, cid1, true);
			p1 = t_util.t_getLastPage(c1);
			RecordHandle r1 = t_util.t_insert(p1, row);

			REPORT("starting internal transaction");

			tinternal = t_util.t_startInternalTransaction();
			long cid2 = t_util.t_addContainer(tinternal, 0);
			ContainerHandle c2 = t_util.t_openContainer(tinternal, 0, cid2, true);
			Page p2 = t_util.t_addPage(c2);
			RecordHandle r2 = t_util.t_insert(p2, row);

			// commit internal transaction
			tinternal.commit();
			tinternal.abort();	// this will close the container and release
								// the page
			tinternal.close();
			tinternal = null;

			REPORT("commit internal transaction");

			// abort user transaction
			t_util.t_abort(tuser);

			REPORT("rollback user transaction");

			c1 = t_util.t_openContainer(tuser, 0, cid1, true);

			p1 = t_util.t_getPage(c1, r1.getPageNumber());
			if (p1.recordExists(r1, false))
				throw T_Fail.testFailMsg("user transaction failed to rollback");


			c2 = t_util.t_openContainer(tuser, 0, cid2, true);
			t_util.t_checkFetch(c2, r2, REC_001); // this should be unaffected by the
										 // user transaction rollback
			p2 = t_util.t_getLastPage(c2);

			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);

			if (!p2.spaceForInsert())
			{
				REPORT("P054 not run, page cannot accomodate 2 rows");
				return;
			}
			RecordHandle r21 = t_util.t_insert(p2, row2);

			// throw an exception, make sure everything is aborted.
			tinternal = t_util.t_startInternalTransaction();
			long cid3 = t_util.t_addContainer(tinternal, 0);
			ContainerHandle c3 = t_util.t_openContainer(tinternal, 0, cid3, true);
			Page p3 = t_util.t_addPage(c3);
			RecordHandle r3 = t_util.t_insert(p3, row);
			try
			{
				// this will throw a data statement exception
				t_util.t_insertAtSlot(p3, 100, row);
			}
			catch (StandardException se)
			{
				REPORT("cleanup on error");
                //Assume database is not active. DERBY-4856 thread dump
                cm1.cleanupOnError(se, false);
				REPORT("done cleanup on error");
			}

			tinternal = null;
			// 	tuser = t_util.t_startTransaction();
			c2 = t_util.t_openContainer(tuser, 0, cid2, true);
			t_util.t_checkFetch(c2, r2, REC_001);

			p2 = t_util.t_getPage(c2, r21.getPageNumber());
			if (p2.recordExists(r21, false))
				throw T_Fail.testFailMsg("expect user transaction to rollback");

			// this should fail
			ContainerKey id3 = new ContainerKey(0, cid3);
			c3 = tuser.openContainer(id3, ContainerHandle.MODE_READONLY);
			if (c3 != null)
				throw T_Fail.testFailMsg("expect internal transaction to rollback");

			LockingPolicy nolock =
                tuser.newLockingPolicy(LockingPolicy.MODE_NONE, 0, false);

			RawContainerHandle stub =
				((RawTransaction)tuser).openDroppedContainer(
					id3, nolock);

			if (stub == null)
				throw T_Fail.testFailMsg("expect container to be dropped");

			if (stub.getContainerStatus() != RawContainerHandle.COMMITTED_DROP)
				throw T_Fail.testFailMsg("expect container to be committed dropped");

			// this should fail
			p3 = stub.getPage(r3.getPageNumber());

			if (p3 != null)
				throw T_Fail.testFailMsg("should not getpage with committed dropped container");

			PASS("P054");


			t_util.t_dropContainer(tuser, 0, cid2); // cleanup
			t_util.t_dropContainer(tuser, 0, cid1); // cleanup

			if (tinternal != null)
			{
				t_util.t_abort(tinternal);
				tinternal.close();
			}

			if (tuser != null)
			{
				t_util.t_commit(tuser);
				tuser.close();
			}
		}
		finally
		{

			contextService.resetCurrentContextManager(cm1);
		}

	}


	/**
        Test rollback of partial row update.
        Create a long row with 10 columns on 2 pages (5 columns on each page).
        Update the 1st column on the 2nd page (the 6th column) which causes the
        last column (10th column) to move off the page. Then abort and make sure
        that all the original columns are there and correct.

        NOTE: stored length is twice string length + 2

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P055(long segment) throws StandardException, T_Fail {

		if (!testRollback)
			return;

		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

        int colSize = 90;
		T_RawStoreRow r0 = new T_RawStoreRow(10);
		r0.setColumn(0, colSize, REC_001);
		r0.setColumn(1, colSize, REC_002);
		r0.setColumn(2, colSize, REC_003);
		r0.setColumn(3, colSize, REC_004);
		r0.setColumn(4, colSize, REC_005);
		r0.setColumn(5, colSize, REC_009);
        r0.setColumn(6, colSize, REC_010);
		r0.setColumn(7, colSize, REC_011);
        r0.setColumn(8, colSize, REC_012);
		r0.setColumn(9, colSize, REC_013);

		int insertFlag = Page.INSERT_INITIAL;
		insertFlag |= Page.INSERT_OVERFLOW;

		RecordHandle rh0 = null;
		try {
			rh0 = t_util.t_insertAtSlot(page, 0, r0, (byte) insertFlag);
		} catch (StandardException se) {
			throw T_Fail.testFailMsg("insert of long row failed.");
		}

		if (rh0 == null)
			throw T_Fail.testFailMsg("insert of first long row failed.");
		else {
			REPORT("about to check fetch...");
			DataValueDescriptor column = new SQLChar();
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 0, column, false, REC_001, colSize);
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 1, column, false, REC_002, colSize);
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 2, column, false, REC_003, colSize);
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 3, column, false, REC_004, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 4, column, false, REC_005, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 5, column, false, REC_009, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 6, column, false, REC_010, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 7, column, false, REC_011, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 8, column, false, REC_012, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 9, column, false, REC_013, colSize);
		}

   		t_util.t_commit(t);


        // update col 5 (the 6th column, the first column on the 2nd overflow page), which causes
        // the last column (col 9, the 10th column) to move off the page.

        c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		T_RawStoreRow updateRow = new T_RawStoreRow(10);
		for (int i = 0; i < 10; i++)
			updateRow.setColumn(i, (String) null);
        updateRow.setColumn(5, colSize*2, REC_009);
		FormatableBitSet colList = new FormatableBitSet(10);
		colList.set(5);
		page.updateAtSlot(0, updateRow.getRow(), colList);

        REPORT("about to check fetch after update ...");
        DataValueDescriptor column = new SQLChar();
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 0, column, false, REC_001, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 1, column, false, REC_002, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 2, column, false, REC_003, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 3, column, false, REC_004, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 4, column, false, REC_005, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 5, column, false, REC_009, colSize*2);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 6, column, false, REC_010, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 7, column, false, REC_011, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 8, column, false, REC_012, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 9, column, false, REC_013, colSize);
        page.unlatch();

        t_util.t_abort(t);

        REPORT("about to check fetch after abort ...");
		c = t_util.t_openContainer(t, segment, cid, false);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 0, column, false, REC_001, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 1, column, false, REC_002, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 2, column, false, REC_003, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 3, column, false, REC_004, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 4, column, false, REC_005, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 5, column, false, REC_009, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 6, column, false, REC_010, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 7, column, false, REC_011, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 8, column, false, REC_012, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 9, column, false, REC_013, colSize);
        page.unlatch();


		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);
        t.close();

		PASS("P055: segment = " + segment);
	}


	/**
        Test rollback of partial row update.
        Create a long row with 15 columns on 3 pages (5 columns on each page).
        Update the 1st column on the 2nd page (the 6th column) which causes the
        last column of that page (10th column) to move off the page. Then abort
        and make sure that all the original columns are there and correct.

        NOTE: stored length is twice string length + 2

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P056(long segment) throws StandardException, T_Fail {

		if (!testRollback)
			return;

		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

        int colSize = 90;
		T_RawStoreRow r0 = new T_RawStoreRow(15);
		r0.setColumn(0, colSize, REC_001);
		r0.setColumn(1, colSize, REC_002);
		r0.setColumn(2, colSize, REC_003);
		r0.setColumn(3, colSize, REC_004);
		r0.setColumn(4, colSize, REC_005);
		r0.setColumn(5, colSize, REC_009);
        r0.setColumn(6, colSize, REC_010);
		r0.setColumn(7, colSize, REC_011);
        r0.setColumn(8, colSize, REC_012);
		r0.setColumn(9, colSize, REC_013);
		r0.setColumn(10, colSize, REC_014);
        r0.setColumn(11, colSize, REC_015);
		r0.setColumn(12, colSize, REC_016);
        r0.setColumn(13, colSize, REC_017);
		r0.setColumn(14, colSize, REC_018);


		int insertFlag = Page.INSERT_INITIAL;
		insertFlag |= Page.INSERT_OVERFLOW;

		RecordHandle rh0 = null;
		try {
			rh0 = t_util.t_insertAtSlot(page, 0, r0, (byte) insertFlag);
		} catch (StandardException se) {
			throw T_Fail.testFailMsg("insert of long row failed.");
		}

		if (rh0 == null)
			throw T_Fail.testFailMsg("insert of first long row failed.");
		else {
			REPORT("about to check fetch...");
			DataValueDescriptor column = new SQLChar();
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 0, column, false, REC_001, colSize);
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 1, column, false, REC_002, colSize);
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 2, column, false, REC_003, colSize);
			t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 3, column, false, REC_004, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 4, column, false, REC_005, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 5, column, false, REC_009, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 6, column, false, REC_010, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 7, column, false, REC_011, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 8, column, false, REC_012, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 9, column, false, REC_013, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 10, column, false, REC_014, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 11, column, false, REC_015, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 12, column, false, REC_016, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 13, column, false, REC_017, colSize);
            t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 14, column, false, REC_018, colSize);
		}

   		t_util.t_commit(t);


        // update col 5 (the 6th column, the first column on the 2nd overflow page), which causes
        // the last column (col 9, the 10th column) to move off the page.

        c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		T_RawStoreRow updateRow = new T_RawStoreRow(15);
		for (int i = 0; i < 15; i++)
			updateRow.setColumn(i, (String) null);
        updateRow.setColumn(5, colSize*2, REC_009);
		FormatableBitSet colList = new FormatableBitSet(15);
		colList.set(5);
		page.updateAtSlot(0, updateRow.getRow(), colList);

        REPORT("about to check fetch after update ...");
        DataValueDescriptor column = new SQLChar();
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 0, column, false, REC_001, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 1, column, false, REC_002, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 2, column, false, REC_003, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 3, column, false, REC_004, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 4, column, false, REC_005, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 5, column, false, REC_009, colSize*2);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 6, column, false, REC_010, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 7, column, false, REC_011, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 8, column, false, REC_012, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 9, column, false, REC_013, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 10, column, false, REC_014, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 11, column, false, REC_015, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 12, column, false, REC_016, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 13, column, false, REC_017, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 14, column, false, REC_018, colSize);
        page.unlatch();

        t_util.t_abort(t);


        REPORT("about to check fetch after abort ...");
		c = t_util.t_openContainer(t, segment, cid, false);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 0, column, false, REC_001, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 1, column, false, REC_002, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 2, column, false, REC_003, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 3, column, false, REC_004, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 4, column, false, REC_005, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 5, column, false, REC_009, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 6, column, false, REC_010, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 7, column, false, REC_011, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 8, column, false, REC_012, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 9, column, false, REC_013, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 10, column, false, REC_014, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 11, column, false, REC_015, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 12, column, false, REC_016, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 13, column, false, REC_017, colSize);
        t_util.t_checkFetchColFromSlot(page, page.FIRST_SLOT_NUMBER, 14, column, false, REC_018, colSize);
        page.unlatch();


		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}
		t_util.t_commit(t);
        t.close();

		PASS("P056: segment = " + segment);
	}




	/**
	   Sparse row test.
	   Test sparse representation of rows using the FormatableBitSet class.
	   Insert, fetch and update a row having gaps.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P061() throws StandardException, T_Fail
	{
		long segment = 0;
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, segment);

		int numCols = 6;
		T_RawStoreRow row1 = new T_RawStoreRow(numCols);
		row1.setColumn(0,  (String) null);
		row1.setColumn(1,  REC_001);
		row1.setColumn(2,  (String) null);
		row1.setColumn(3,  REC_002);
		row1.setColumn(4,  (String) null);
		row1.setColumn(5,  REC_003);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			RecordHandle rh1 = t_util.t_insert(page, row1);

			t_util.t_checkFetchCol(page, rh1, 1, numCols, REC_001);
			t_util.t_checkFetchCol(page, rh1, 3, numCols, REC_002);
			t_util.t_checkFetchCol(page, rh1, 5, numCols, REC_003);

			t_util.t_checkUpdateCol(page, rh1, 1, numCols, "woody");
			t_util.t_checkUpdateCol(page, rh1, 3, numCols, "buzz");
			t_util.t_checkUpdateCol(page, rh1, 5, numCols, "andy");

			t_util.t_checkUpdateCol(page, rh1, 2, numCols, "dino");

			page.unlatch();
			page = null;

			t_util.t_commit(t);
		}
		finally
		{
			if (page != null)
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}

		PASS("P061");
	}


	/**
	   Serializable column test.
       Want to make sure we hit some otherwise dead code in StoredPage, used
       for storing/reading Serializable/Externalizable data to/from a page.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P071() throws StandardException, T_Fail
	{
        /*
		long segment = 0;
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, segment);

		int numCols = 1;
		T_RawStoreRow row1 = new T_RawStoreRow(numCols);
		row1.setColumn(0,  new T_Serializable(REC_001));

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			RecordHandle rh1 = t_util.t_insert(page, row1);

			t_util.t_checkFetchSerCol(page, rh1, 0, numCols, new T_Serializable(REC_001));

			page.unlatch();
			page = null;

			t_util.t_commit(t);
		}
		finally
		{
			if (page != null)
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}
        */

		PASS("P071");
	}



	/*
	** Update and update partial tests aimed at long rows
	*/

	/**
		Insert a single row and keep updating it, adding columns
		not using partial rows.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P701(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		T_RawStoreRow row = new T_RawStoreRow(0);
		RecordHandle rh = t_util.t_insertAtSlot(page, 0, row, (byte) (Page.INSERT_INITIAL | Page.INSERT_OVERFLOW));
		t_util.t_checkFetch(page, rh, row);

		page.unlatch();
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);


		for (int i = 0; i < 10;i++) {

			REPORT("P701 - iteration " + i);
			
			row = new T_RawStoreRow(i);

			for (int j = 0; j < i; j++) {
				row.setColumn(j, 256, "XX" + j + "YY");
			}

			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, row.getRow(), null);
			page.unlatch();
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkFetch(page, rh, row);
		}

		page.unlatch();

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}


		t_util.t_commit(t);

		t.close();

		PASS("P701: segment = " + segment);
	}
	/*
	** Update and update partial tests aimed at long rows
	*/

	/**
		Insert a single row and keep updating it, adding columns
		using partial rows.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P702(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		T_RawStoreRow row = new T_RawStoreRow(0);
		RecordHandle rh = t_util.t_insertAtSlot(page, 0, row, (byte) (Page.INSERT_INITIAL | Page.INSERT_OVERFLOW));
		t_util.t_checkFetch(page, rh, row);

		page.unlatch();
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);


		for (int i = 0; i < 10;i++) {

			REPORT("P702 - iteration " + i);

			FormatableBitSet colList = new FormatableBitSet(i+1);
			colList.set(i);

			T_RawStoreRow rowU = new T_RawStoreRow(i+1);
			rowU.setColumn(i, 256, "XX" + i + "YY");

			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, rowU.getRow(), colList);
			page.unlatch();

			T_RawStoreRow rowF = new T_RawStoreRow(i+1);

			for (int j = 0; j <= i; j++) {
				rowF.setColumn(j, 256, "XX" + j + "YY");
			}

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkFetch(page, rh, rowF);
		}
		page.unlatch();

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);

		t.close();

		PASS("P702: segment = " + segment);
	}

	/**
		Simple set of partial row updates on a singel page with
		shrinking and expanding columns.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/

	protected void P703(long segment)
		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		REPORT("P703 - start ");
		T_RawStoreRow row = new T_RawStoreRow(2);
		row.setColumn(0, REC_001);
		row.setColumn(1, REC_002);
		RecordHandle rh = t_util.t_insertAtSlot(page, 0, row, (byte) (Page.INSERT_INITIAL | Page.INSERT_OVERFLOW));
		t_util.t_checkFetch(page, rh, row);
		REPORT("P703 - insert Ok ");

		page.unlatch();
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// Perform 10 tests
		// 1  update col0 to grow
		// 2  update col1 to grow
		// 3  update col0 to shrink
		// 4  update col1 to shrink
		// 5  update col0 to null
		// 6  update col1 to null
		// 7  update col0 to non-null
		// 8  update col1 to non-null
		// 9  update no columns
		// 10 update both cols

		P703Helper(page, rh, 0, REC_006, REC_002);
		REPORT("P703 - case 1 passed");

		P703Helper(page, rh, 1, REC_007, REC_006);
		REPORT("P703 - case 2 passed");

		P703Helper(page, rh, 0, REC_003, REC_007);
		REPORT("P703 - case 3 passed");

		P703Helper(page, rh, 1, REC_004, REC_003);
		REPORT("P703 - case 4 passed");

		P703Helper(page, rh, 0, null, REC_004);
		REPORT("P703 - case 5 passed");

		P703Helper(page, rh, 1, null, null);
		REPORT("P703 - case 6 passed");

		P703Helper(page, rh, 0, REC_002, null);
		REPORT("P703 - case 7 passed");

		P703Helper(page, rh, 1, REC_001, REC_002);
		REPORT("P703 - case 8 passed");



		P703Helper(page, rh, -1, REC_002, REC_001);
		REPORT("P703 - case 9 passed");

		FormatableBitSet colList = new FormatableBitSet(2);
		colList.set(0);
		colList.set(1);
		row.setColumn(0, REC_004);
		row.setColumn(1, REC_003);
		int slot = page.getSlotNumber(rh);
		page.updateAtSlot(slot, row.getRow(), colList);
		t_util.t_checkFetch(page, rh, row);


		REPORT("P703 - case 10 passed");


		page.unlatch();

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t.commit();

		t.close();

		PASS("P703: segment = " + segment);
	}

	private void P703Helper(Page page, RecordHandle rh, int colNum, String newVal, String unchangedCol)
		throws StandardException, T_Fail {

		FormatableBitSet colList = new FormatableBitSet(2);
		T_RawStoreRow rowU = new T_RawStoreRow(2);

		// -1 indicates no columns set in bit set
		if (colNum != -1) {
			colList.grow(colNum+1);
			colList.set(colNum);
			rowU.setColumn(colNum, newVal);
		} else {
			colNum = 0; // only used for read from now on
		}
		int slot = page.getSlotNumber(rh);
		page.updateAtSlot(slot, rowU.getRow(), colList);

		T_RawStoreRow row = new T_RawStoreRow(2);
		row.setColumn(colNum, newVal);
		row.setColumn(colNum == 0 ? 1 : 0, unchangedCol);

		t_util.t_checkFetch(page, rh, row);
	}


	/**
		Insert a single row with multiple portions.
		Update fields in the various portions that grow.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P704(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		// row has 15 cols, each with 200 (ish) bytes (100 null chars)
		// thus we would expect at least 3 pages
		T_RawStoreRow row = new T_RawStoreRow(15);
		for (int i = 0; i < 15; i++) {
			row.setColumn(i, 100, "XX" + i + "YY");
		}

		RecordHandle rh = t_util.t_insertAtSlot(page, 0, row, (byte) (Page.INSERT_INITIAL | Page.INSERT_OVERFLOW));
		t_util.t_checkFetch(page, rh, row);

		page.unlatch();
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);


		// update the each column to grow to be a single page (800 bytes ish)
		for (int i = 0; i < 15;i++) {

			REPORT("P704 - col " + i);

			FormatableBitSet colList = new FormatableBitSet(i+1);
			colList.set(i);

			T_RawStoreRow rowU = new T_RawStoreRow(i+1);
			rowU.setColumn(i, 400, "WW" + i + "UU");

			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, rowU.getRow(), colList);
			page.unlatch();

			row.setColumn(i, 400, "WW" + i + "UU");

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkFetch(page, rh, row);
		}
		page.unlatch();

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);

		t.close();

		PASS("P704: segment = " + segment);
	}

	/**
		Same as 704 but update fields in the reverse order.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P705(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		// row has 15 cols, each with 200 (ish) bytes (100 null chars)
		// thus we would expect at least 3 pages
		T_RawStoreRow row = new T_RawStoreRow(15);
		for (int i = 0; i < 15; i++) {
			row.setColumn(i, 100, "XX" + i + "YY");
		}

		RecordHandle rh = t_util.t_insertAtSlot(page, 0, row, (byte) (Page.INSERT_INITIAL | Page.INSERT_OVERFLOW));
		t_util.t_checkFetch(page, rh, row);

		page.unlatch();
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);


		// update the each column to grow to be a single page (800 bytes ish)
		for (int i = 14; i >=0; i--) {

			REPORT("P705 - col " + i);

			FormatableBitSet colList = new FormatableBitSet(i+1);
			colList.set(i);

			T_RawStoreRow rowU = new T_RawStoreRow(i+1);
			rowU.setColumn(i, 400, "WW" + i + "UU");

			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, rowU.getRow(), colList);
			page.unlatch();

			row.setColumn(i, 400, "WW" + i + "UU");

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkFetch(page, rh, row);
		}
		page.unlatch();

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);

		t.close();

		PASS("P705: segment = " + segment);
	}

	/**
		Insert a single row with single or multiple portions.
		Update every other field with a long col
		The update each column back to a null

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P706(long segment, boolean multiPortion) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		// row has 15 cols, each with 200 (ish) bytes (100 null chars)
		// thus we would expect at least 3 pages
		T_RawStoreRow row = new T_RawStoreRow(15);
		for (int i = 0; i < 15; i++) {
	
			row.setColumn(i, multiPortion ? 100 : 10, "XX" + i + "YY");
		}

		RecordHandle rh = t_util.t_insertAtSlot(page, 0, row, (byte) (Page.INSERT_INITIAL | Page.INSERT_OVERFLOW));
		t_util.t_checkFetch(page, rh, row);

		page.unlatch();
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// update  every other column to be a long column
		for (int i = 0; i < 15;i++) {

			if ((i % 2) == 0) {		
				continue;
			}

			REPORT("P706 : multiPortion " + multiPortion + " - col " + i);

			FormatableBitSet colList = new FormatableBitSet(i+1);
			colList.set(i);

			T_RawStoreRow rowU = new T_RawStoreRow(i+1);
			rowU.setColumn(i, 3000, "WW" + i + "UU"); // longer than 4096 page length

			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, rowU.getRow(), colList);
			page.unlatch();

			row.setColumn(i, 3000, "WW" + i + "UU");

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkFetch(page, rh, row);
		}

		t_util.t_commit(t);

		// update  every column to a null
		c = t_util.t_openContainer(t, segment, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		for (int i = 0; i < 15;i++) {

			REPORT("P706 : update to null " + multiPortion + " - col " + i);

			FormatableBitSet colList = new FormatableBitSet(i+1);
			colList.set(i);

			T_RawStoreRow rowU = new T_RawStoreRow(i+1);
			rowU.setColumn(i, (String) null);

			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, rowU.getRow(), colList);
			page.unlatch();

			row.setColumn(i, (String) null);

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkFetch(page, rh, row);
		}
		page.unlatch();

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);

		t.close();

		PASS("P706: multiPortion " + multiPortion + " segment = " + segment);
	}

	/**
		Insert a single record that has several chunks
		and every other column is a long column
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P707(long segment) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		T_RawStoreRow row = new T_RawStoreRow(20);
		for (int i = 0; i < 20; i++) {
			if ((i % 2) ==0)
				row.setColumn(i, 200, "XX" + i + "YY");	// big but first within a page
			else
				row.setColumn(i, 4000, "XX" + i + "YY"); // long column
		}

		RecordHandle rh = t_util.t_insertAtSlot(page, 0, row, (byte) (Page.INSERT_INITIAL | Page.INSERT_OVERFLOW));
		t_util.t_checkFetch(page, rh, row);

		page.unlatch();

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}


		t_util.t_commit(t);

		t.close();

		PASS("P707: segment = " + segment);
	}

	/**
		Insert a single row with single or multiple portions.
		Update every other field with a long col
		rollback.
		The update each column back to a null & rollback

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void P708(long segment, boolean multiPortion) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		// row has 15 cols, each with 200 (ish) bytes (100 null chars)
		// thus we would expect at least 3 pages
		T_RawStoreRow row = new T_RawStoreRow(15);
		for (int i = 0; i < 15; i++) {

			row.setColumn(i, multiPortion ? 100 : 10, "XX" + i + "YY");
		}

		RecordHandle rh = t_util.t_insertAtSlot(page, 0, row, (byte) (Page.INSERT_INITIAL | Page.INSERT_OVERFLOW));
		t_util.t_checkFetch(page, rh, row);

		page.unlatch();
		t_util.t_commit(t);
		c = t_util.t_openContainer(t, segment, cid, true);

		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// update  every other column to be a long column
		for (int i = 0; i < 15;i++) {

			if ((i % 2) == 0) {
				continue;
			}

			REPORT("P708 : multiPortion " + multiPortion + " - col " + i);

			FormatableBitSet colList = new FormatableBitSet(i+1);
			colList.set(i);

			T_RawStoreRow rowU = new T_RawStoreRow(i+1);
			rowU.setColumn(i, 3000, "WW" + i + "UU"); // longer than 4096 page length

			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, rowU.getRow(), colList);
		}

		t_util.t_abort(t);

		c = t_util.t_openContainer(t, segment, cid, false);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkFetch(page, rh, row);
		page.unlatch();

		if (segment != ContainerHandle.TEMPORARY_SEGMENT) {
			t_util.t_dropContainer(t, segment, cid);	// cleanup
		}

		t_util.t_commit(t);

		t.close();

		PASS("P708: multiPortion " + multiPortion + " segment = " + segment);
	}


	/**
	  P709:
	  this test exercises purgeAtSlot , rollsback and purges the slot again,
	  to make sure not logging the data does not have any impact on repurging
	  the rollbacked purges.
	  @exception T_Fail Unexpected behaviour from the API
	  @exception StandardException Unexpected exception from the implementation
	*/
	protected void P709()
		 throws StandardException, T_Fail
	{
		logDataForPurges = false;
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t,0);
		t_util.t_commit(t);

	
		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		// REPORT("insert 5 records");
		T_RawStoreRow row0 = new T_RawStoreRow(REC_001);
		T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
		T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
		T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
		T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
		
		RecordHandle r0, r1, r2, r3, r4;
		r0 = t_util.t_insertAtSlot(page, 0, row0);
		r1 = t_util.t_insertAtSlot(page, 1, row1);
		r2 = t_util.t_insertAtSlot(page, 2, row2);
		r3 = t_util.t_insertAtSlot(page, 3, row3);
		r4 = t_util.t_insertAtSlot(page, 4, row4);

		if (r3 != null) page.deleteAtSlot(3, true, (LogicalUndo)null);

		// REPORT("commit it");
		t_util.t_commit(t);

		c = t_util.t_openContainer(t, 0, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		try 
		{
			page.purgeAtSlot(-1, 1, logDataForPurges);
			throw T_Fail.testFailMsg("negative slot number did not cause an exception");
		}
		catch (StandardException se) {}	// expected

		try
		{
			page.purgeAtSlot(4, 4, logDataForPurges);
			throw T_Fail.testFailMsg("purging more rows than is on page did not cause an exception");
		}
		catch (StandardException se) {}	// expected

		// if not all the rows are there, do minimal test
		if (r4 == null)
		{
			int rcount = page.recordCount();
			page.purgeAtSlot(0, 1, logDataForPurges);
			if (page.recordCount() != rcount-1)
				T_Fail.testFailMsg("failed to purge a record, expect " + 
								   (rcount-1) + " got " + page.recordCount());

			if (testRollback)
			{
				t_util.t_abort(t);

				c = t_util.t_openContainer(t, 0, cid, true);
				page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
				if(logDataForPurges)
					t_util.t_checkFetchBySlot(page, 0, REC_001,false, true);
				else
					t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
				if (page.recordCount() != rcount)
					T_Fail.testFailMsg("failed to rollback purge, expect " + 
								   rcount + " got " + page.recordCount());
			}
			else
			{
				t_util.t_commit(t);
			}
			PASS("mimimal purging P709");
			return;
		}

		// REPORT("purge 2 records from middle");
		page.purgeAtSlot(1, 2, logDataForPurges);
		t_util.t_checkFetchBySlot(page, 0, REC_001,false, true);
		t_util.t_checkFetchBySlot(page, 1, REC_003,true, true);
		t_util.t_checkFetchBySlot(page, 2, REC_004,false, true);

		if (page.recordCount() != 3)
			T_Fail.testFailMsg("page expect to have 3 records, recordCount() = " +
							   page.recordCount());

		// REPORT("purge all records from the page");
		page.purgeAtSlot(0, 3, logDataForPurges);
		if (page.recordCount() != 0)
			T_Fail.testFailMsg("page expect to have 0 records, recordCount() = " +
							   page.recordCount());

		if (testRollback)
		{

			REPORT("testing rollback");
			t_util.t_abort(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			if(logDataForPurges){
				t_util.t_checkFetchBySlot(page, 0, REC_001,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_001,false, true);
				t_util.t_checkFetchBySlot(page, 2, REC_002,false, true);
				t_util.t_checkFetchBySlot(page, 3, REC_003,true, true);
				t_util.t_checkFetchBySlot(page, 4, REC_004,false, true);
			}else
			{
				t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 2, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 3, REC_NULL,true, true);
				t_util.t_checkFetchBySlot(page, 4, REC_NULL,false, true);
			}

			if (page.recordCount() != 5)
				T_Fail.testFailMsg("page expect to have 5 records, recordCount() = " +
								   page.recordCount());

			// REPORT("purge 3 records from the end");
			page.purgeAtSlot(2, 3, logDataForPurges);
			if(logDataForPurges)
			{
				t_util.t_checkFetchBySlot(page, 0, REC_001,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_001,false, true);
			}else
			{
				t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_NULL,false, true);
			}
			if (page.recordCount() != 2)
				T_Fail.testFailMsg("page expect to have 2 records, recordCount() = " +
								   page.recordCount());

			// REPORT("rollback");
			t_util.t_abort(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			if(logDataForPurges){
				t_util.t_checkFetchBySlot(page, 0, REC_001,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_001,false, true);
				t_util.t_checkFetchBySlot(page, 2, REC_002,false, true);
				t_util.t_checkFetchBySlot(page, 3, REC_003,true, true);
				t_util.t_checkFetchBySlot(page, 4, REC_004,false, true);
			}else
			{
				t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 1, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 2, REC_NULL,false, true);
				t_util.t_checkFetchBySlot(page, 3, REC_NULL,true, true);
				t_util.t_checkFetchBySlot(page, 4, REC_NULL,false, true);	
			}

			if (page.recordCount() != 5)
				T_Fail.testFailMsg("page expect to have 5 records, recordCount() = " +
								   page.recordCount());

			// REPORT("make sure delete record is reconstituted as such");
			if (page.isDeletedAtSlot(1))
				T_Fail.testFailMsg("rolled back purged undeleted record cause record to be deleted");
			if (!page.isDeletedAtSlot(3))
				T_Fail.testFailMsg("rolled back purged deleted record cause record to be undeleted");
		}

		REPORT("purging again the purges rolled back earlier");
		//purge again and this time do commit , instead of rollback.
		// REPORT("purge 2 records from middle");
		page.purgeAtSlot(1, 2, logDataForPurges);
		t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
		t_util.t_checkFetchBySlot(page, 1, REC_NULL,true, true);
		t_util.t_checkFetchBySlot(page, 2, REC_NULL,false, true);

		if (page.recordCount() != 3)
			T_Fail.testFailMsg("page expect to have 3 records, recordCount() = " +
							   page.recordCount());

		// REPORT("purge all records from the page");
		page.purgeAtSlot(0, 3, logDataForPurges);
		if (page.recordCount() != 0)
			T_Fail.testFailMsg("page expect to have 0 records, recordCount() = " +
							   page.recordCount());
		
		
		t_util.t_abort(t);

		c = t_util.t_openContainer(t, 0, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		if (page.recordCount() != 5)
			T_Fail.testFailMsg("page expect to have 5 records, recordCount() = " +
							   page.recordCount());

		// REPORT("purge 3 records from the end");
		page.purgeAtSlot(2, 3, logDataForPurges);
		t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
		t_util.t_checkFetchBySlot(page, 1, REC_NULL,false, true);
		if (page.recordCount() != 2)
			T_Fail.testFailMsg("page expect to have 2 records, recordCount() = " +
							   page.recordCount());

		// REPORT("commit");
		t_util.t_commit(t);

		c = t_util.t_openContainer(t, 0, cid, true);
		page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkFetchBySlot(page, 0, REC_NULL,false, true);
		t_util.t_checkFetchBySlot(page, 1, REC_NULL,false, true);

		if (page.recordCount() != 2)
				T_Fail.testFailMsg("page expect to have 2 records, recordCount() = " +
								   page.recordCount());

		
		PASS("P709");

		t_util.t_dropContainer(t, 0, cid);	// cleanup
		t_util.t_commit(t);
		t.close();
	}

	
	/**
		Test space reclaimation - purging of a long rows with a rollback and
        purging againg with no data logging for purges
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P710() throws StandardException, T_Fail
	{
		long segment = 0;
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			t_util.t_checkEmptyPage(page);

			int insertFlag = Page.INSERT_INITIAL | Page.INSERT_OVERFLOW;

			// insert a row with 100 columns from 200 to 400 bytes each and make it
			// sprawl across many pages.
			T_RawStoreRow r1 = new T_RawStoreRow(100);
			for (int i = 0; i < 100; i++)
				r1.setColumn(i, 100+i, REC_001);

			RecordHandle rh1 = t_util.t_insertAtSlot(page, 0, r1, (byte)insertFlag);
			t_util.t_checkFetch(page, rh1, r1);

			Page nextPage = t_util.t_addPage(c);
			long nextPageNumber = nextPage.getPageNumber();
			// deallocate it
			t_util.t_removePage(c, nextPage);

			REPORT("P710 - Nextpage is " + nextPageNumber);
			page.unlatch();
			page = null;
			t_util.t_commit(t);

			// See what the next page is.
			c = t_util.t_openContainer(t, segment, cid, true);

			// Now purge that first row.
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, 1, 1);
			page.purgeAtSlot(0, 1, logDataForPurges);

			t_util.t_checkEmptyPage(page);
			page.unlatch();
			page = null;
			t_util.t_commit(t);

			// give some time for post commit to finish
			t_util.t_wait(10);		// wait 10 milliseconds.

			// reinsert r1, it should use no extra page than last time.
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			RecordHandle rh2 = t_util.t_insertAtSlot(page, 0, r1, (byte)insertFlag);
			t_util.t_checkFetch(page, rh2, r1);
			page.unlatch();
			page = null;

			// now verify that it used up no more page than last time
			nextPage = t_util.t_addPage(c);
			long checkNextPageNumber = nextPage.getPageNumber();
			nextPage.unlatch();

			if (nextPageNumber != checkNextPageNumber)
				throw T_Fail.testFailMsg("fail to reuse row pieces expect next page=" + 
					nextPageNumber + " but got " + checkNextPageNumber );


			t_util.t_commit(t);

			// Purge them and roll them back via savepoint.  These should not
			// be reclaimed.
			c = t_util.t_openContainer(t, segment, cid, true);
			t.setSavePoint(SP1, null);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 1, 1);
			page.purgeAtSlot(0, 1, logDataForPurges);
			page.unlatch();
			page = null;

			// make sure we cannot get our hands on a page that is freed up by
			// the purge
			Page testPage = t_util.t_addPage(c);
			T_RawStoreRow testRow = new T_RawStoreRow(REC_001);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			t.rollbackToSavePoint(SP1, null);

			testPage = t_util.t_addPage(c);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			t_util.t_commit(t);
			t_util.t_wait(10);
			
			c = t_util.t_openContainer(t, segment, cid, true);

			testPage = t_util.t_addPage(c);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 1, 1);

			//repurge again.
			page.purgeAtSlot(0, 1, logDataForPurges);
			t_util.t_abort(t);
			//				t_util.t_checkFetch(page, rh2, r1);			
			page = null;
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 1, 1);
			//repurge again and do commit
			page.purgeAtSlot(0, 1, logDataForPurges);
			page.unlatch();
			page = null;
			t_util.t_commit(t);
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 0, 0);
			page.unlatch();
			page = null;
			t_util.t_dropContainer(t, segment, cid);	// cleanup

		}
		finally	
		{
			if (page != null)
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}

		PASS("P710");
	}

	
	/**
		Test space reclaimation - purging of a row with serveral long columns
	    rollback and repurge them again.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void P711() throws StandardException, T_Fail
	{

		long segment = 0;
		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, segment, 4096);

		ContainerHandle c = t_util.t_openContainer(t, segment, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{
			t_util.t_checkEmptyPage(page);

			int insertFlag = Page.INSERT_INITIAL | Page.INSERT_OVERFLOW;
			T_RawStoreRow r1 = new T_RawStoreRow(1);
			// insert a long column
			r1.setColumn(0, 5000, REC_001);
			RecordHandle rh1 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			t_util.t_checkFetch(page, rh1, r1);

			// insert a 6 column row, every other column is long
			T_RawStoreRow r2 = new T_RawStoreRow(6);
			r2.setColumn(0, 600, REC_001); // this takes ~1200 bytes
			r2.setColumn(1, 5000, REC_002); // this takes ~10000 bytes
			r2.setColumn(2, 600, REC_001);
			r2.setColumn(3, 5000, REC_002);
			r2.setColumn(4, 600, REC_001);
			r2.setColumn(5, 5000, REC_002);
			RecordHandle rh2 = t_util.t_insertAtSlot(page, 0, r2, (byte) insertFlag);	
			t_util.t_checkFetch(page, rh2, r2);

			// insert a long column - this should fail
			RecordHandle rh3 = t_util.t_insertAtSlot(page, 0, r1, (byte) insertFlag);	
			if (rh3 != null)
            {
				throw T_Fail.testFailMsg("expect the 3rd row to not fit on page");
            }
			page.unlatch();
			page = null;

			Page nextPage = t_util.t_addPage(c);
			long nextPageNumber = nextPage.getPageNumber();
			// deallocate it
			t_util.t_removePage(c, nextPage);

			REPORT("P711 - Nextpage is " + nextPageNumber);

			t_util.t_commit(t);

			// now purge them
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, 2, 2);
			page.purgeAtSlot(0, 2, logDataForPurges);
			t_util.t_checkEmptyPage(page);
			page.unlatch();
			page = null;

			t_util.t_abort(t);

			// give some time for post commit to finish
			t_util.t_wait(10);		// wait 10 milliseconds.


			// Purge them again and roll them back via savepoint.  These should not
			// be reclaimed.
			c = t_util.t_openContainer(t, segment, cid, true);

			t.setSavePoint(SP1, null);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			
			t_util.t_checkRecordCount(page, 2, 2);
			page.purgeAtSlot(0, 2, logDataForPurges);
			t_util.t_checkEmptyPage(page);

			page.unlatch();
			page = null;

			// make sure we cannot get our hands on a page that is freed up by
			// the purge
			Page testPage = t_util.t_addPage(c);
			T_RawStoreRow testRow = new T_RawStoreRow(REC_001);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			t.rollbackToSavePoint(SP1, null);

			testPage = t_util.t_addPage(c);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			t_util.t_commit(t);

			// give some time for post commit to finish
			t_util.t_wait(10);

			// check to make sure post commit did not reclaim those rows.
			c = t_util.t_openContainer(t, segment, cid, true);

			testPage = t_util.t_addPage(c);
			t_util.t_insert(testPage, testRow);
			testPage.unlatch();

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			

			t_util.t_checkRecordCount(page, 2, 2);
			t_util.t_checkFetch(page, rh1, r1);
		

			// During purges when data is not logged when slots are purged
			// they become null on rollback and some cases like long columns
			// we remove the wholepage on rollback we get the data back.
			T_RawStoreRow r2_wnl = new T_RawStoreRow(6);
			r2_wnl.setColumn(0, 4, REC_NULL); 
			r2_wnl.setColumn(1, 5000, REC_002); 
			r2_wnl.setColumn(2, 4, REC_NULL);
			r2_wnl.setColumn(3, 5000, REC_002);
			r2_wnl.setColumn(4, 4, REC_NULL);
			r2_wnl.setColumn(5, 5000, REC_002);
			t_util.t_checkFetch(page, rh2, r2_wnl);
				
			page.unlatch();
			page = null;

			t_util.t_dropContainer(t, segment, cid);	// cleanup

		}
		finally
		{
			if (page != null)
				page.unlatch();
			t_util.t_commit(t);
			t.close();
		}

		PASS("P711");

	}



	// test writing out large log records
	protected void L001()
		 throws StandardException, T_Fail
	{
		Transaction t = t_util.t_startTransaction();

		// if runing multi threaded, only have 1 thread log these large record
		// or we may get out of memeory error
		int loop = 10;
		int logSize = (threadNumber == 0) ? 50000 : 50;

		try
		{
			for (int i = 0; i < loop; i++)
			{
				Loggable l = new T_Undoable(t.getGlobalId(), -1, -1, 
											T_Undoable.REMOVE_NONE,
											0, //LWM ignored
											true,10,false,i*logSize,false);
				t.logAndDo(l);
			}
			t.commit();
			t.close();
			t = null;

			t = t_util.t_startTransaction();
			for (int i = 0; i < loop; i++)
			{
				Loggable l = new T_Undoable(t.getGlobalId(), -1, -1, 
											T_Undoable.REMOVE_NONE,
											0, //LWM ignored
											true,10,false,i*logSize,false);

				t.logAndDo(l);
			}

			PASS("L001");
		}
		finally
		{
			t.commit();
			t.close();
		}
	}


	/**
		Test checkpoint
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void CP001()
		 throws StandardException, T_Fail
	{
		if (!testRollback)
			return;

		ContextManager previousCM = contextService.getCurrentContextManager();
		Transaction longtran = null;

		ContextManager cm1 = null;
		Transaction t1 = null;

		ContextManager cm2 = null;
		Transaction t2 = null;

		// ContextManager cpm = null; // reserved for the checkpoint transaction
		try {

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
			T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
			T_RawStoreRow row5 = new T_RawStoreRow(REC_005);

			// start a long running transaction that spans multiple checkpoints
			// make sure it can be rolled back a the end
			longtran = t_util.t_startTransaction();

			long cid = t_util.t_addContainer(longtran, 0);
			ContainerHandle c = t_util.t_openContainer(longtran, 0, cid, true);
			RecordHandle r1 = t_util.t_insert(c, row1);
			RecordHandle r2 = t_util.t_insert(c, row2);
			t_util.t_commit(longtran);

			c = t_util.t_openContainer(longtran, 0, cid, true);
			Page p2 = t_util.t_getPage(c, r2.getPageNumber());
			int slot2 = p2.getSlotNumber(r2);
			p2.updateAtSlot(slot2, row5.getRow(), null);
			p2.unlatch();


			// a bunch of short running transactions that criss cross the
			// checkpoints 

			cm1 = contextService.newContextManager();
			contextService.setCurrentContextManager(cm1);
			t1 = t_util.t_startTransaction();

			long cid1 = t_util.t_addContainer(t1, 0);
			ContainerHandle c1 = t_util.t_openContainer(t1, 0, cid1, true);
			RecordHandle r3 = t_util.t_insert(c1, row3);
			RecordHandle r4 = t_util.t_insert(c1, row4);
			contextService.resetCurrentContextManager(cm1);

			cm2 = contextService.newContextManager();
			contextService.setCurrentContextManager(cm2);
			t2 = t_util.t_startTransaction();

			long cid2 = t_util.t_addContainer(t2, 0);
			ContainerHandle c2 = t_util.t_openContainer(t2, 0, cid2, true);
			RecordHandle r5 = t_util.t_insert(c2, row1);
			t_util.t_commit(t2);
			c2 = t_util.t_openContainer(t2, 0, cid2, true);
			Page p5 = t_util.t_getPage(c2, r5.getPageNumber());
			int slot5 = p5.getSlotNumber(r5);
			p5.updateAtSlot(slot5, row5.getRow(), null);
			p5.unlatch();

			//		cpm = contextService.newContextManager();
			//		contextService.setCurrentContextManager(cpm);
			factory.checkpoint();
			contextService.resetCurrentContextManager(cm2);


			// make sure checkpoint did not destroy any data
			contextService.setCurrentContextManager(previousCM);
			t_util.t_checkFetch(c, r1, REC_001);
			t_util.t_checkFetch(c, r2, REC_005);
			contextService.resetCurrentContextManager(previousCM);

			contextService.setCurrentContextManager(cm1);
			t_util.t_checkFetch(c1, r3, REC_003);
			t_util.t_checkFetch(c1, r4, REC_004);
			contextService.resetCurrentContextManager(cm1);

			contextService.setCurrentContextManager(cm2);
			t_util.t_checkFetch(c2, r5, REC_005);

			// two consecutive checkpoints
			//		contextService.setCurrentContextManager(cpm);
			factory.checkpoint();
			contextService.resetCurrentContextManager(cm2);

			// we can insert some more
			contextService.setCurrentContextManager(previousCM);
			Page page = t_util.t_addPage(c);
			RecordHandle r6 = t_util.t_insertAtSlot(page, 0, row1, Page.INSERT_UNDO_WITH_PURGE);
			page.unlatch();
			contextService.resetCurrentContextManager(previousCM);

			// commit/abort everything except the long running transaction
			contextService.setCurrentContextManager(cm1);
			t_util.t_commit(t1);
			contextService.resetCurrentContextManager(cm1);

			contextService.setCurrentContextManager(cm2);
			t_util.t_abort(t2);
			contextService.resetCurrentContextManager(cm2);

			contextService.setCurrentContextManager(previousCM);
			t_util.t_checkFetch(c, r1, REC_001);
			t_util.t_checkFetch(c, r2, REC_005);
			t_util.t_checkFetch(c, r6, REC_001);
			contextService.resetCurrentContextManager(previousCM);

			contextService.setCurrentContextManager(cm1);
			c1 = t_util.t_openContainer(t1, 0, cid1, true);
			t_util.t_checkFetch(c1, r3, REC_003);
			t_util.t_checkFetch(c1, r4, REC_004);
			contextService.resetCurrentContextManager(cm1);

			contextService.setCurrentContextManager(cm2);
			c2 = t_util.t_openContainer(t2, 0, cid2, true);
			t_util.t_checkFetch(c2, r5, REC_001);

			// checkpoint again
			//		contextService.setCurrentContextManager(cpm);
			factory.checkpoint();
			contextService.resetCurrentContextManager(cm2);

			contextService.setCurrentContextManager(previousCM);
			t_util.t_abort(longtran);
			c = t_util.t_openContainer(longtran, 0, cid, true);
			t_util.t_checkFetch(c, r1, REC_001);
			t_util.t_checkFetch(c, r2, REC_002);

			Page p6 = t_util.t_getPage(c, r6.getPageNumber());
			t_util.t_checkEmptyPage(p6);
			p6.unlatch();

			t_util.t_dropContainer(longtran, 0, cid); // cleanup
			contextService.resetCurrentContextManager(previousCM);

			contextService.setCurrentContextManager(cm1);
			t_util.t_checkFetch(c1, r3, REC_003);
			t_util.t_checkFetch(c1, r4, REC_004);
			t_util.t_dropContainer(t1, 0, cid1);
			contextService.resetCurrentContextManager(cm1);

			contextService.setCurrentContextManager(cm2);
			t_util.t_checkFetch(c2, r5, REC_001);
			t_util.t_dropContainer(t2, 0, cid2);

			// checkpoint again
			//		contextService.setCurrentContextManager(cpm);
		    factory.checkpoint();
			contextService.resetCurrentContextManager(cm2);

			PASS("CP001");


		}
		catch (Throwable t)
		{
			t.printStackTrace(System.err);

			if (cm1 != null)
                //Assume database is not active. DERBY-4856 thread dump
                cm1.cleanupOnError(t, false);
			if (cm2 != null)
                cm2.cleanupOnError(t, false);
			//		if (cpm != null)
            //			cpm.cleanupOnError(t, false);

		} finally {

			if (t2 != null)
			{
				contextService.setCurrentContextManager(cm2);
				t_util.t_commit(t2);
				t2.close();
				contextService.resetCurrentContextManager(cm2);
			}

			if (t1 != null)
			{
				contextService.setCurrentContextManager(cm1);
				t_util.t_commit(t1);
				t1.close();
				contextService.resetCurrentContextManager(cm1);
			}

			if (longtran != null)
			{
				contextService.setCurrentContextManager(previousCM);
				t_util.t_commit(longtran);
				longtran.close();
				contextService.resetCurrentContextManager(previousCM);
			}

			//DJD contextService.setCurrentContextManager(previousCM);
		}
	}


	/**
		TC001 - Test the drop on commit mode for temp containers.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation

	*/
	protected void TC001() throws T_Fail, StandardException {

		Transaction t = t_util.t_startTransaction();

		// open a container with drop on commit and see if it disappears
		long cid = t_util.t_addContainer(t, ContainerHandle.TEMPORARY_SEGMENT);
		t_util.setOpenMode(openMode | ContainerHandle.MODE_DROP_ON_COMMIT);
		ContainerHandle c = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);
		t_util.t_commit(t);

		ContainerKey id = new ContainerKey(ContainerHandle.TEMPORARY_SEGMENT, cid);
		c = t.openContainer(id, ContainerHandle.MODE_READONLY);
		if (c != null)
			throw T_Fail.testFailMsg("Temp Container should not exist");

		// open a container with drop on commit, close it, check it is still there and see if it disappears on a commit
		cid = t_util.t_addContainer(t, ContainerHandle.TEMPORARY_SEGMENT);
		c = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);
		c.close();
		c = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);
		c.close();
		t_util.t_commit(t);

		id = new ContainerKey(ContainerHandle.TEMPORARY_SEGMENT, cid);
		c = t.openContainer(id, ContainerHandle.MODE_READONLY);
		if (c != null)
			throw T_Fail.testFailMsg("Temp Container should not exist");

		// open a container with drop on commit, abort the transaction, and see if it disappears
		cid = t_util.t_addContainer(t, ContainerHandle.TEMPORARY_SEGMENT);
		t_util.setOpenMode(openMode | ContainerHandle.MODE_DROP_ON_COMMIT);
		c = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);
		t_util.t_abort(t);

		id = new ContainerKey(ContainerHandle.TEMPORARY_SEGMENT, cid);
		c = t.openContainer(id, ContainerHandle.MODE_READONLY);
		if (c != null)
			throw T_Fail.testFailMsg("Temp Container should not exist");

		t_util.t_commit(t);
		t.close();
		PASS("TC001");

	}

	// code to populate a temp table for some temp tests
	private int[] populateTempTable(ContainerHandle c)  throws StandardException, T_Fail  {
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		t_util.t_checkEmptyPage(page);

		RecordHandle rh;
		T_RawStoreRow row;
		int[]	recordCount = {0,0,0};

		for (int i = 0; i < 3;) {

			for (;;) {

				row = new T_RawStoreRow(REC_001 + i + "X" + recordCount[i]);
				rh = t_util.t_insert(page, row);

				if (rh == null)
					break;

				recordCount[i]++;
				t_util.t_checkRecordCount(page, recordCount[i], recordCount[i]);
			}

			page.unlatch();
			page = null;

			if (++i < 3) {
				page = t_util.t_addPage(c);
				t_util.t_checkEmptyPage(page);
			}
		}

		return recordCount;
	}

	/**
		A clone of P002 for temporary containers.
		Insert rows on the first page until the page is full, then add a page
		and repeat the test (for a total of three pages with full rows).
		Fetch the rows back by handle methods.
		Commit or abort the transaction, and see if table is empty.
		Can be used as follows:

		<PRE>
		mode                   doCommit

        TRUNCATE_ON_COMMIT     true       Ensure the table has only one empty page after commit
		TRUNCATE_ON_COMMIT     false      Ensure the table has only one empty page after abort
		0                      false      Ensure the table has only one empty page after abort

		</PRE>

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void TC002(int mode, boolean doCommit) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, ContainerHandle.TEMPORARY_SEGMENT);

		REPORT("TC002 container id = " + cid);

		t_util.setOpenMode(openMode | mode);
		
		ContainerHandle c = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);

		int[] recordCount = populateTempTable(c);

		for (int i = 0; i < recordCount.length; i++) {
			REPORT("RecordCount on page " + i + "=" + recordCount[i]);
		}



		// now check that we read the same number of records back
		// using the handle interface

		long pageNumber = ContainerHandle.FIRST_PAGE_NUMBER;
		for (int i = 0; i < recordCount.length; i++, pageNumber++) {
			Page page = t_util.t_getPage(c, pageNumber);
			t_util.t_checkRecordCount(page, recordCount[i], recordCount[i]);
			RecordHandle rh = t_util.t_checkFetchFirst(page, REC_001 + i + "X" + 0);
			for (int j = 1; j < recordCount[i]; j++)
				rh = t_util.t_checkFetchNext(page, rh, REC_001 + i + "X" + j);

            try
            {
                rh = page.fetchFromSlot(
                        null, recordCount[i], new DataValueDescriptor[0], 
                        (FetchDescriptor) null,
                        false);

                throw T_Fail.testFailMsg(
                        "reading more rows on page than were written");
            }
            catch (StandardException se)
            {
                // expected exception.
            }


			rh = t_util.t_checkFetchLast(page, REC_001 + i + "X" + (recordCount[i] - 1));
			for (int j = recordCount[i] - 2; j >= 0; j--)
				rh = t_util.t_checkFetchPrevious(page, rh, REC_001 + i + "X" + j);

			page.unlatch();
			page = null;
		}

		c.close();

		// this commit or abort will truncate the table
		if (doCommit)
			t_util.t_commit(t);
		else
			t_util.t_abort(t);

		t_util.setOpenMode(openMode);

		c = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);		
		
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

		t_util.t_checkEmptyPage(page);
		page.unlatch();
		page = null;

		page = c.getPage(ContainerHandle.FIRST_PAGE_NUMBER + 1);

		if (page != null)
			throw T_Fail.testFailMsg("truncate of temp container did not succeed");

		t_util.t_commit(t);

		t.close();

		PASS("TC002 " + mode + " " + doCommit);
	}


	/**

		Add a number of rows to a temp table opened in various modes,
		and drop it before commit/abort.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void TC003(int mode, boolean doCommit) throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, ContainerHandle.TEMPORARY_SEGMENT);

		REPORT("TC003 container id = " + cid);

		t_util.setOpenMode(openMode | mode);
		
		ContainerHandle c = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);
		populateTempTable(c);

		// table is populated, drop it ...
		t_util.t_dropContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid);
		// table should have disappeared ...
		ContainerKey id = new ContainerKey(ContainerHandle.TEMPORARY_SEGMENT, cid);
		ContainerHandle ce = t.openContainer(id, ContainerHandle.MODE_READONLY);
		if (ce != null)
			throw T_Fail.testFailMsg("Dropped Container should not open");

		if (doCommit)
			t_util.t_commit(t);
		else
			t_util.t_abort(t);

		// table should have disappeared ...
		ContainerHandle cd = t.openContainer(id, ContainerHandle.MODE_READONLY);
		if (cd != null)
			throw T_Fail.testFailMsg("Dropped Container should not open");

		t_util.t_commit(t);

		t.close();

		PASS("TC003 " + mode + " " + doCommit);
	}

	/**
		Open a temp table several times with different modes and ensure the
		correct behaviour (most severe open wins).

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void TC004all() throws StandardException, T_Fail {
		int[] modes = {
			0,
			ContainerHandle.MODE_DROP_ON_COMMIT,
			ContainerHandle.MODE_TRUNCATE_ON_COMMIT
		};

		for (int m1 = 0; m1 < modes.length; m1++) {
			for (int m2 = 0; m2 < modes.length; m2++) {
				for (int m3 = 0; m3 < modes.length; m3++) {

					TC004(m1, m2, m3, false, false);
					TC004(m1, m2, m3, false, true);
					TC004(m1, m2, m3, true, false);
					TC004(m1, m2, m3, true, false);
				}
			}

		}
	}


	/**
		Open a temp table several time swith different modes and ensure the
		correct behaviour (most severe open wins).

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	protected void TC004(int mode1, int mode2, int mode3, boolean doCommit, boolean closeThem) throws StandardException, T_Fail {

		String testInfo = "TC004 mode1 " + mode1 + " mode2 " + mode2 + " mode3 " + mode3 +
			" doCommit " + doCommit + " closeThem "  + closeThem;
		REPORT("start " + testInfo);

		Transaction t = t_util.t_startTransaction();		

		long cid = t_util.t_addContainer(t, ContainerHandle.TEMPORARY_SEGMENT);

		REPORT("TC004 container id = " + cid);

		t_util.setOpenMode(openMode | mode1);	
		ContainerHandle c1 = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);
		populateTempTable(c1);
		if (closeThem)
			c1.close();

		t_util.setOpenMode(openMode | mode2);
		ContainerHandle c2 = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);
		if (closeThem)
			c2.close();

		t_util.setOpenMode(openMode | mode3);
		ContainerHandle c3 = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);
		if (closeThem)
			c2.close();

		if (doCommit)
			t_util.t_commit(t);
		else
			t_util.t_abort(t);

		int fullMode = mode1 | mode2 | mode3;

		if ((fullMode & ContainerHandle.MODE_DROP_ON_COMMIT) == ContainerHandle.MODE_DROP_ON_COMMIT) {
			// table should have disappeared ...
			ContainerKey id = new ContainerKey(ContainerHandle.TEMPORARY_SEGMENT, cid);
			ContainerHandle cd = t.openContainer(id, ContainerHandle.MODE_READONLY);
			if (cd != null)
				throw T_Fail.testFailMsg("Dropped Container should not open");

		} else if (!doCommit 
			|| ((fullMode & ContainerHandle.MODE_TRUNCATE_ON_COMMIT) == ContainerHandle.MODE_TRUNCATE_ON_COMMIT)) {
			// table should be empty
			ContainerHandle ce = t_util.t_openContainer(t, ContainerHandle.TEMPORARY_SEGMENT, cid, true);		
			
			Page page = t_util.t_getPage(ce, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkEmptyPage(page);
			page.unlatch();
			page = null;
		}

		t_util.t_commit(t);

		t.close();

		PASS(testInfo);
	}

    
    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ContextService    getContextService()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ContextService>()
             {
                 public ContextService run()
                 {
                     return ContextService.getFactory();
                 }
             }
             );
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

