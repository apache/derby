/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_RecoverFullLog

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

import org.apache.derbyTesting.unitTests.harness.UnitTest;

import org.apache.derby.impl.store.raw.log.*;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.reference.EngineType;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.store.raw.*;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.AccessController;
import java.util.Properties;


/**
	A implementation unit test for log full condition

    To run, create a derby.properties file in a new directory with the
	contents

	derby.module.test.recoverFullLog=org.apache.derbyTesting.unitTests.store.T_RecoverFullLog

    Execute in order

	java -DTestFillLog=true org.apache.derbyTesting.unitTests.harness.UnitTestMain

	java -DTestLogSwitchFail=true org.apache.derbyTesting.unitTests.harness.UnitTestMain

	java -DTestFullRecoveryFail=true org.apache.derbyTesting.unitTests.harness.UnitTestMain 
		(run this serveral times, this simulate recovery running out of log)

	java -DTestFullRecover=true org.apache.derbyTesting.unitTests.harness.UnitTestMain 

*/

public class T_RecoverFullLog extends T_Generic {

	private static final String testService = "FullLogTest";

	static final String REC_001 = "McLaren";
	static final String REC_002 = "Ferrari";
	static final String REC_003 = "Benetton";
	static final String REC_004 = "Prost";
	static final String REC_005 = "Tyrell";
	static final String REC_006 = "Derby, Natscape, Goatscape, the popular names";
	static final String REC_UNDO = "Lotus";
	static final String SP1 = "savepoint1";
	static final String SP2 = "savepoint2";

	private RandomAccessFile infofile = null;
	private static final String infoPath = "extinout/T_RecoverFullLog.info";

	private boolean fillLog;	// test to full up the log
	private boolean recoveryFail; // recovery fill up the log
	private boolean logSwitchFail;	// log filled up during log switch
	private boolean recover;	// successfully recover

	private String TEST_FILL_LOG = "TestFillLog";	// test to full up the log
	private String TEST_FULL_RECOVER_FAIL = "TestFullRecoveryFail"; // recovery fill up the log
	private String TEST_LOG_SWITCH_FAIL = "TestLogSwitchFail"; // log filled up during log switch
	private String TEST_FULL_RECOVER = "TestFullRecover";	// successfully recover

	private static final String TEST_FULL_LOG_INFO = "TestFullLogInfo";

	RawStoreFactory	factory;
	LockFactory  lf;
	ContextService contextService;
	T_Util t_util;

	public T_RecoverFullLog() {
		super();
	}

	/*
	** Methods required by T_Generic
	*/

	public String getModuleToTestProtocolName() {
		return RawStoreFactory.MODULE;
	}

	/**
	*/
	private void getConfig()
	{
		String param;

		param = PropertyUtil.getSystemProperty(TEST_FILL_LOG);
		fillLog = Boolean.valueOf(param).booleanValue();

		param = PropertyUtil.getSystemProperty(TEST_FULL_RECOVER_FAIL);
		recoveryFail = Boolean.valueOf(param).booleanValue();

		param = PropertyUtil.getSystemProperty(TEST_FULL_RECOVER);
		recover = Boolean.valueOf(param).booleanValue();

		param = PropertyUtil.getSystemProperty(TEST_LOG_SWITCH_FAIL);
		logSwitchFail = Boolean.valueOf(param).booleanValue();
	}

	/**
	    See T_Recovery for the general testing frame work

		@exception T_Fail Unexpected behaviour from the API
	 */
	public void runTests() throws T_Fail {

		getConfig();
		int tests = 0;
		if (fillLog) tests++;
		if (recoveryFail) tests++;
		if (recover) tests++;
		if (logSwitchFail) tests++;

		if (tests != 1)
			throw T_Fail.testFailMsg("One & only one of the full log recovery test should be run, now " + tests + " set");

		if (!SanityManager.DEBUG)
		{
			REPORT("recoverBadLog cannot be run on an insane server");
			return;
		}

		try {

			contextService = getContextService();
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

			File ifile = new File(infoPath);
			
			//if external input output files dir does not exist ,create one
			File ifdir = new File("extinout");
			if(!ifdir.exists())
				ifdir.mkdirs();
			// see if we are testing encryption
			startParams = T_Util.setEncryptionParam(startParams);

			if (fillLog)				// the first test cleans up and start from fresh
			{
				// remove the service directory to ensure a clean run
				REPORT("_______________________________________________________");
				REPORT("\n\t\tcleaning up database for recovering from filled log");
				REPORT("_______________________________________________________");

				// don't automatic boot this service if it gets left around
				if (startParams == null) 
					startParams = new Properties();
				
				startParams.put(Property.NO_AUTO_BOOT, Boolean.TRUE.toString());
				// remove the service directory to ensure a clean run
				startParams.put(Property.DELETE_ON_CREATE, Boolean.TRUE.toString());

//IC see: https://issues.apache.org/jira/browse/DERBY-6648
				factory = (RawStoreFactory) createPersistentService(getModuleToTestProtocolName(),
																  testService,
																  startParams);
				// create a database with nothing

				// if exist, delete the info file
				if (ifile.exists())
					ifile.delete();


				// create a new info file
				try
				{
					infofile = new RandomAccessFile(ifile, "rw");
				}
				catch (IOException ioe)
				{
					System.out.println("Cannot write to temporary file " +
									   infoPath + 
									   ".  Please make sure it is correct, if not, please set the property " +
									   "TestFullLogInfo=<where temp files should go>");

					throw T_Fail.exceptionFail(ioe);
				}

					
			}
			else
			{
				// see if we can recover the database
				REPORT("_______________________________________________________");
				if (recoveryFail)
					REPORT("\n\t\trecovering database - recovery will fill up log");
				else
					REPORT("\n\t\trecovering database - recovery should succeed");
				REPORT("_______________________________________________________");

				try
				{
					// make sure it does exist
					infofile = new RandomAccessFile(ifile, "rw");
				}
				catch (IOException ioe)
				{
					throw T_Fail.exceptionFail(ioe);
				}

				// let recovery log 10 records then run out of space
				if (recoveryFail)
				{
					SanityManager.DEBUG_SET(LogToFile.TEST_LOG_FULL);
//IC see: https://issues.apache.org/jira/browse/DERBY-615
//IC see: https://issues.apache.org/jira/browse/DERBY-616
					System.setProperty(LogToFile.TEST_RECORD_TO_FILL_LOG, "10");
				}

//IC see: https://issues.apache.org/jira/browse/DERBY-6648
				if (!startPersistentService(testService, startParams))
					throw T_Fail.testFailMsg("Monitor didn't know how to restart service: " + testService);
				factory = (RawStoreFactory) findService(getModuleToTestProtocolName(), testService);

				if (recoveryFail)
				{
					throw T_Fail.testFailMsg("recovery should have failed but did not - did you run the test in order?");
				}
			}
		} catch (StandardException mse) {

			if (recoveryFail) {
						REPORT("_______________________________________________________");
						REPORT("\n\tRecovery failed due to log full as requested ");
						REPORT("\texception was " + mse.toString());
						REPORT("_______________________________________________________");
						return;

			}
			throw T_Fail.exceptionFail(mse);
		} catch (NullPointerException npe) {

			if (recoveryFail) {
						REPORT("_______________________________________________________");
						REPORT("\n\tRecovery failed due to log full as requested ");
						REPORT("\texception was " + npe.toString());
						REPORT("_______________________________________________________");
						return;

			}
			throw T_Fail.exceptionFail(npe);
		}


		if (factory == null) {
			throw T_Fail.testFailMsg(getModuleToTestProtocolName() + " service not started.");
		}
			
		lf = factory.getLockFactory();
		if (lf == null) {
			throw T_Fail.testFailMsg("LockFactory.MODULE not found");
		}

		// get a utility helper
		t_util = new T_Util(factory, lf, contextService);

		try
		{
			if (fillLog)
			{
				testBasic(1);
				fillUpLog();
			}
			else if (logSwitchFail)
			{
				testBasic(2);
				logSwitchFail1();
				testBasic(3);
				logSwitchFail2();
			}
			else if (!recoveryFail)
			{
				checkRecovery();
			}
		} catch (StandardException se) {

			throw T_Fail.exceptionFail(se);
		}
	}

	private long find(long inkey)
	{
		if (infofile == null)
			return -1;

		try 
		{
			infofile.seek(0);
			long key;

			while(true)
			{
				key = infofile.readLong();
				if (key == inkey)
				{
					long value = infofile.readLong();
					// System.out.println("found " + key + " " + value);
					return value;
				}
				infofile.readLong();
			}
		} 
		catch (IOException ioe)
		{
			// System.out.println("key not found " + inkey);
			return -1;
		}

	}

	private long key(int test, int param)
	{
		long i = test;
		return ((i << 32) + param);
	}

	private void register(long key, long value)
		 throws T_Fail
	{
		// System.out.println("registering " + key + " " + value);
		try 
		{
			// go to the end
			infofile.seek(infofile.length());
			infofile.writeLong(key);
			infofile.writeLong(value);
		}
		catch (IOException ioe)
		{
			T_Fail.exceptionFail(ioe);
		}
	}


	/*
	 * A basic routine to write a bunch of stuff to the log
	 * There will be some committed transactions, some uncommitted transactions,
	 * serveral checkpoints.
	 */
	protected void testBasic(int testNumber) throws T_Fail, StandardException
	{
		int numtrans = 7;
		int numpages = 7;
		int i,j;

		// this is basically T_Recovery S203 
		T_TWC[] t = new T_TWC[numtrans];
		for (i = 0; i < numtrans; i++)
			t[i] =  t_util.t_startTransactionWithContext();

		long[] cid = new long[numtrans];
		ContainerHandle[] c = new ContainerHandle[numtrans];

		for (i = 0; i < numtrans; i++)
		{
			cid[i] = t_util.t_addContainer(t[i], 0);
			t_util.t_commit(t[i]);
			c[i] = t_util.t_openContainer(t[i], 0, cid[i], true);
		}

		Page page[][] = new Page[numtrans][numpages];
		long pagenum[][] = new long[numtrans][numpages];

		for (i = 0; i < numtrans; i++)
		{
			for (j = 0; j < numpages; j++)
			{
				t[i].switchTransactionContext();
				page[i][j] = t_util.t_addPage(c[i]);
				pagenum[i][j] = page[i][j].getPageNumber();
				t[i].resetContext();
			}
		}

		// set up numtrans (at least 5) transactions, each with one
		// container and numpages pages.  Do the following test:
		//
		// 1) insert 1 row onto each page
		// set savepoint SP1 on first transaction (t0)
		//
		// 2) update every rows
		// set savepoint SP1 on all other transactions
		//
		// 3) update every rows
		// set savepoint SP2 on all transactions
		// 
		// 4) update every rows
		//
		// 5) rollback t0 to SP1
		//
		// check that only page[0][x] have been rolled back
		// past SP2
		//
		// 6) update every row
		// 7) rollback SP2 on all transaction except the first
		// 
		// 8) update every rows
		// 9) rollback t0 to SP1
		//
		// 10) leave transactions in the following state
		// t0 - incomplete
		// t1 - abort
		// t2 - commit
		// t3 - incomplete
		// t4 - commit
		// any other transactions - incomplete


		//////////////////////// step 1 ////////////////////////
		RecordHandle[][] rh = new RecordHandle[numtrans][numpages];
		T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
		for (i = 0; i < numtrans; i++)
			for (j = 0; j < numpages; j++)
			{
				t[i].switchTransactionContext();
				rh[i][j] = t_util.t_insert(page[i][j], row1); 
				t[i].resetContext();
			}

		t[0].setSavePoint(SP1, null);	// sp1

		//////////////////////// step 2 ////////////////////////
		T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
		for (i = 0; i < numtrans; i++)
			for (j = 0; j < numpages; j++)
			{
				t[i].switchTransactionContext();
				int slot = page[i][j].getSlotNumber(rh[i][j]);
				page[i][j].updateAtSlot(slot, row2.getRow(), null);
				t[i].resetContext();
			}

		for (i = 1; i < numtrans; i++) // sp1
		{
			t[i].setSavePoint(SP1, null);
		}

		//////////////////////// step 3 ////////////////////////
		T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
		for (i = 0; i < numtrans; i++)
			for (j = 0; j < numpages; j++) {
				int slot = page[i][j].getSlotNumber(rh[i][j]);
				page[i][j].updateAtSlot(slot, row3.getRow(), null);
			}

		for (i = 0; i < numtrans; i++)
			t[i].setSavePoint(SP2, null);	// sp2

		//////////////////////// step 4 ////////////////////////
		T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
		for (i = 0; i < numtrans; i++)
		{
			t[i].switchTransactionContext();

			for (j = 0; j < numpages; j++) {
				int slot = page[i][j].getSlotNumber(rh[i][j]);
				page[i][j].updateAtSlot(slot, row4.getRow(), null);
			}
			t[i].resetContext();
		}


		//////////////////////// step 5 ////////////////////////
		// unlatch relavante pages
		t[0].switchTransactionContext();

		for (j = 0; j < numpages; j++)
			page[0][j].unlatch();

		t[0].rollbackToSavePoint(SP1, null); // step 5


		// relatch relavante pages
		for (j = 0; j < numpages; j++)
			page[0][j] = t_util.t_getPage(c[0], pagenum[0][j]);

		t[0].resetContext();

		//////////////////////// check ////////////////////////
		for (i = 1; i < numtrans; i++)
		{
			t[i].switchTransactionContext();
			for (j = 0; j < numpages; j++)
				t_util.t_checkFetch(page[i][j], rh[i][j], REC_004);
			t[i].resetContext();
		}

		t[0].switchTransactionContext();
		for (j = 0; j < numpages; j++)
			t_util.t_checkFetch(page[0][j], rh[0][j], REC_001);

		t[0].resetContext();
			//////////////////////// step 6 ////////////////////////
		T_RawStoreRow row5 = new T_RawStoreRow(REC_005);
		for (i = 0; i < numtrans; i++)
		{
			t[i].switchTransactionContext();
			for (j = 0; j < numpages; j++) {
				int slot = page[i][j].getSlotNumber(rh[i][j]);
				page[i][j].updateAtSlot(slot, row5.getRow(), null);
			}
			t[i].resetContext();
		}

		//////////////////////// step 7 ////////////////////////
		for (i = 1; i < numtrans; i++)
		{
			t[i].switchTransactionContext();

			for (j = 0; j < numpages; j++)
				page[i][j].unlatch();

			t[i].rollbackToSavePoint(SP2, null);

			for (j = 0; j < numpages; j++)
				page[i][j] = t_util.t_getPage(c[i],pagenum[i][j]);
			t[i].resetContext();
		}

		//////////////////////// check ////////////////////////
		for (i = 1; i < numtrans; i++)
		{
			t[i].switchTransactionContext();
			for (j = 0; j < numpages; j++)
				t_util.t_checkFetch(page[i][j], rh[i][j], REC_003);
			t[i].resetContext();
		}

		t[0].switchTransactionContext();
		for (j = 0; j < numpages; j++)
			t_util.t_checkFetch(page[0][j], rh[0][j], REC_005);

		t[0].resetContext();


		//////////////////////// step 8 ////////////////////////
		T_RawStoreRow row6 = new T_RawStoreRow(REC_006);
		for (i = 0; i < numtrans; i++)
		{
			t[i].switchTransactionContext();
			for (j = 0; j < numpages; j++) {
				int slot = page[i][j].getSlotNumber(rh[i][j]);
				page[i][j].updateAtSlot(slot, row6.getRow(), null);
			}
			t[i].resetContext();
		}

		//////////////////////// step 9 ////////////////////////
		// unlatch relavante pages
		t[0].switchTransactionContext();
		for (j = 0; j < numpages; j++)
			page[0][j].unlatch();


		t[0].rollbackToSavePoint(SP1, null); 

		// relatch relevant pages
		for (j = 0; j < numpages; j++)
			page[0][j] = t_util.t_getPage(c[0], pagenum[0][j]);

		t[0].resetContext();
		//////////////////////// check ////////////////////////
		for (i = 1; i < numtrans; i++)
		{
			t[i].switchTransactionContext();

			for (j = 0; j < numpages; j++)
			{
				t_util.t_checkFetch(page[i][j], rh[i][j], REC_006);
				t_util.t_checkRecordCount(page[i][j], 1, 1);
			}
			t[i].resetContext();
		}

		t[0].switchTransactionContext();
		for (j = 0; j < numpages; j++)
		{
			t_util.t_checkFetch(page[0][j], rh[0][j], REC_001);
			t_util.t_checkRecordCount(page[0][j], 1, 1);
		}
		t[0].resetContext();

		//////////////////////// step 10 ////////////////////////
		// unlatch all pages
		for (i = 0; i < numtrans; i++)
		{
			t[i].switchTransactionContext();
			for (j = 0; j < numpages; j++)
				page[i][j].unlatch();
			t[i].resetContext();
		}

		// t[0] incomplete
		t_util.t_abort(t[1]);
		t_util.t_commit(t[2]);
		// t[3] incomplete
		t_util.t_commit(t[4]);

			// reopen containers 1, 2, and 4, where were closed when the
			// transaction terminated.
		c[1] = t_util.t_openContainer(t[1], 0, cid[1], false);
		c[2] = t_util.t_openContainer(t[2], 0, cid[2], false);
		c[4] = t_util.t_openContainer(t[4], 0, cid[4], false);

		//////////////////////// check ////////////////////////
		for (j = 0; j < numpages; j++)	
		{
			t[0].switchTransactionContext();
			t_util.t_checkFetch(c[0], rh[0][j], REC_001);
			t[0].resetContext();

			// t[1] has been aborted
			// rh[1][j] (REC_001) is deleted
			t[1].switchTransactionContext();
			page[1][j] = t_util.t_getPage(c[1], pagenum[1][j]);
			t_util.t_checkRecordCount(page[1][j], 1, 0);
			t_util.t_checkFetchBySlot(page[1][j], Page.FIRST_SLOT_NUMBER,
									  REC_001, true, false);
			page[1][j].unlatch();
			t[1].resetContext();

			t[2].switchTransactionContext();
			t_util.t_checkFetch(c[2], rh[2][j], REC_006);
			t[2].resetContext();

			t[3].switchTransactionContext();
			t_util.t_checkFetch(c[3], rh[3][j], REC_006);
			t[3].resetContext();

			t[4].switchTransactionContext();
			t_util.t_checkFetch(c[4], rh[4][j], REC_006);
			t[4].resetContext();
		}


		for (i = 0; i < numtrans; i++)
		{
			register(key(testNumber, i+10), cid[i]);

			String str = "container " + i + ":" + find(key(testNumber,i+10)) + " pages: ";

			for (j = 0; j < numpages; j++)
			{
				str += pagenum[i][j] + " ";
				register(key(testNumber, (i+1)*1000+j), pagenum[i][j]);
			}
			REPORT("\t" + str);
		}

		register(key(testNumber,1), numtrans); 
		register(key(testNumber,2), numpages);

		// let recovery try to roll back transactions t0, t3 
	}

	// fill up the log immediately
	protected void fillUpLog() throws T_Fail, StandardException
	{
		SanityManager.DEBUG_SET(LogToFile.TEST_LOG_FULL);
		System.setProperty(LogToFile.TEST_RECORD_TO_FILL_LOG, "1");
//IC see: https://issues.apache.org/jira/browse/DERBY-615
//IC see: https://issues.apache.org/jira/browse/DERBY-616

		Transaction t = t_util.t_startTransaction();
		try
		{
			long cid = t_util.t_addContainer(t, 0);
		}
		catch (StandardException se)
		{
			REPORT("_______________________________________________________");
			REPORT("\n\tlog filled up as requested");
			REPORT("_______________________________________________________");
			return;
		}
		catch (NullPointerException npe)
		{
			// likely to be a null pointer exception being thrown because the
			// system is forcibly shutdown due to corruption
			REPORT("_______________________________________________________");
			REPORT("\n\tlog filled up as requested");
			REPORT("_______________________________________________________");
			return;
		}

		throw T_Fail.testFailMsg("log should have filled but did not");
	}

	protected void logSwitchFail1() throws T_Fail, StandardException
	{
		SanityManager.DEBUG_SET(LogToFile.TEST_SWITCH_LOG_FAIL1);

		factory.checkpoint(); // this should succeed, switch log is
								   // optional before the end marker is written

		SanityManager.DEBUG_CLEAR(LogToFile.TEST_SWITCH_LOG_FAIL1);
	}
	
	protected void logSwitchFail2() throws T_Fail, StandardException
	{
		SanityManager.DEBUG_SET(LogToFile.TEST_SWITCH_LOG_FAIL2);

		int tries = 10;
		try
		{
			// checkpoint should fail if it is attempted, after end marker is
			// written, any error is fatal.  If another
			// checkpoint is in progress, log some things and try again

			// if we are extremely unlucky, it is possible that we will fail
			// even after 10 tries.  It is better to keep trying than to
			// disable the background checkpoint daemon because this is how the
			// system actually runs in real life.  Do not manufacture a
			// non-existant condition just to make the test pass.

			for (int i = 10; i < 110 + tries; i++)
			{
				factory.checkpoint();
				testBasic(i);
			}
		}
		catch (StandardException se) {
			REPORT("_______________________________________________________");
			REPORT("\n\tlog switch failed as requested");
			REPORT("_______________________________________________________");

			return;
		} catch (NullPointerException npe) {
			REPORT("_______________________________________________________");
			REPORT("\n\tlog switch failed as requested");
			REPORT("_______________________________________________________");

			return;
		}
		finally { SanityManager.DEBUG_CLEAR(LogToFile.TEST_SWITCH_LOG_FAIL2); }

		throw T_Fail.testFailMsg("log switch should have failed but did not even after " + tries + " tries");
	}

	protected void checkRecovery() throws T_Fail, StandardException
	{
		// check for numTest=1, 2, 3
		
		for(int numTest=1; numTest <= 3; numTest++)
		{
			int numtrans = (int)find(key(numTest, 1));
			int numpages = (int)find(key(numTest, 2));

			if (numtrans < 5 || numpages < 1)
			{
				REPORT("full log test " + numTest + " not run");
				continue;
			}
			else
			{
				REPORT("Test recovery of test " + numTest);
			}

			Transaction t = t_util.t_startTransaction();

			long[] cid = new long[numtrans];
			ContainerHandle[] c = new ContainerHandle[numtrans];

			long[][] pagenum = new long[numtrans][numpages];
			Page[][] page = new Page[numtrans][numpages];

			int i,j;

			for	(i = 0; i < numtrans; i++)
			{

				cid[i] = find(key(numTest, i+10));
				c[i] = t_util.t_openContainer(t, 0, cid[i], true);
			
				for (j = 0; j < numpages; j++)
				{
					pagenum[i][j] = find(key(numTest, (i+1)*1000+j));
					page[i][j] = t_util.t_getPage(c[i], pagenum[i][j]);
				}
			}

			// transactions were left in the following state
			// t0 - incomplete (rolled back)
			// t1 - abort
			// t2 - commit
			// t3 - incomplete (rolled back)
			// t4 - commit
			// any other transactions - incomplete
			//
			// all the rolled back transaction should have a deleted REC_001
			// all the committed transactions should have a REC_006
			//
			for (j = 0; j < numpages; j++)
			{
				t_util.t_checkRecordCount(page[0][j], 1, 0);
				t_util.t_checkFetchBySlot(page[0][j], Page.FIRST_SLOT_NUMBER,
										  REC_001, true, true);

				t_util.t_checkRecordCount(page[1][j], 1, 0);
				t_util.t_checkFetchBySlot(page[1][j], Page.FIRST_SLOT_NUMBER,
										  REC_001, true, true);

				t_util.t_checkRecordCount(page[2][j], 1, 1);
				t_util.t_checkFetchBySlot(page[2][j], Page.FIRST_SLOT_NUMBER,
										  REC_006, false, true);

				t_util.t_checkRecordCount(page[3][j], 1, 0);
				t_util.t_checkFetchBySlot(page[3][j], Page.FIRST_SLOT_NUMBER,
										  REC_001, true, true);

				t_util.t_checkRecordCount(page[4][j], 1, 1);
				t_util.t_checkFetchBySlot(page[4][j], Page.FIRST_SLOT_NUMBER,
										  REC_006, false, true);
			}

			for (i = 0; i < numtrans; i++)
			{
				String str = "container " + i + ":" + cid[i] + " pages: ";
				for (j = 0; j < numpages; j++)
					str += pagenum[i][j] + " ";
				REPORT("\t" + str);
			}
			t_util.t_commit(t);
			t.close();

		}
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
     * Privileged service lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object findService( final String factoryInterface, final String serviceName )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.findService( factoryInterface, serviceName );
                 }
             }
             );
    }
    
    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  boolean startPersistentService( final String serviceName, final Properties properties ) 
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Boolean>()
                 {
                     public Boolean run()
                         throws StandardException
                     {
                         return Monitor.startPersistentService( serviceName, properties );
                     }
                 }
                 ).booleanValue();
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
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


