/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_RecoverBadLog

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
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.EngineType;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.io.StorageRandomAccessFile;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.*;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.access.Qualifier;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.Properties;



/**
	A implementation unit test for recovering log that has been damanged but salvagable.

    To run, create a derby.properties file in a new directory with the
	contents

	derby.module.test.recoverBadLog=org.apache.derbyTesting.unitTests.store.T_RecoverBadLog

    Execute in order

	To Test Bad Log due to partial write that are identified by checking the
	length in the beginning and end of the log record. 

	java -DTestBadLogSetup=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog1=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog2=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog3=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog4=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog5=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog6=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog7=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog1=true org.apache.derbyTesting.unitTests.harness.UnitTestMain

	To Test Bad Log due to an incomplete out of order write that is identified
	by the checksum logic (simulated by	explicitly corrupting a middle of a 
	log record at  the  end of log file after it is written).
	
	java -DTestBadLogSetup=true -DTestBadChecksumLog=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog1=true -DTestBadChecksumLog=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog2=true -DTestBadChecksumLog=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog3=true -DTestBadChecksumLog=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog4=true -DTestBadChecksumLog=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog5=true -DTestBadChecksumLog=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog6=true -DTestBadChecksumLog=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog7=true -DTestBadChecksumLog=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestBadLog1=true -DTestBadChecksumLog=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	
	
*/

public class T_RecoverBadLog extends T_Generic {

	private  String testService = "BadLogTest";

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

	private boolean setup;
	private boolean test1;
	private boolean test2;
	private boolean test3;
	private boolean test4;
	private boolean test5;
	private boolean test6;
	private boolean test7;
	private boolean checksumTest; 
	
	private  String infoPath = "extinout/T_RecoverBadLog.info";

	private static final String TEST_BADLOG_SETUP = "TestBadLogSetup";
	private static final String TEST_BADLOG1 = "TestBadLog1";
	private static final String TEST_BADLOG2 = "TestBadLog2";
	private static final String TEST_BADLOG3 = "TestBadLog3";
	private static final String TEST_BADLOG4 = "TestBadLog4";
	private static final String TEST_BADLOG5 = "TestBadLog5";
	private static final String TEST_BADLOG6 = "TestBadLog6";
	private static final String TEST_BADLOG7 = "TestBadLog7";

	private static final String TEST_BAD_CHECKSUM_LOG = "TestBadChecksumLog";

	private static final String TEST_BADLOG_INFO = "TestBadLogInfo";
	private static final String TEST_BADCHECKSUMLOG_INFO = "TestBadChecksumLogInfo";

	RawStoreFactory	factory;
	LockFactory  lf;
	LogToFile   logFactory;
	ContextService contextService;
	T_Util t_util;

	public T_RecoverBadLog() {
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

		param = PropertyUtil.getSystemProperty(TEST_BADLOG_SETUP);
		setup = Boolean.valueOf(param).booleanValue();

		param = PropertyUtil.getSystemProperty(TEST_BADLOG1);
		test1 = Boolean.valueOf(param).booleanValue();

		param = PropertyUtil.getSystemProperty(TEST_BADLOG2);
		test2 = Boolean.valueOf(param).booleanValue();

		param = PropertyUtil.getSystemProperty(TEST_BADLOG3);
		test3 = Boolean.valueOf(param).booleanValue();
		
		
		param = PropertyUtil.getSystemProperty(TEST_BADLOG4);
		test4 = Boolean.valueOf(param).booleanValue();
		
		param = PropertyUtil.getSystemProperty(TEST_BADLOG5);
		test5 = Boolean.valueOf(param).booleanValue();

		param = PropertyUtil.getSystemProperty(TEST_BADLOG6);
		test6 = Boolean.valueOf(param).booleanValue();

		param = PropertyUtil.getSystemProperty(TEST_BADLOG7);
		test7 = Boolean.valueOf(param).booleanValue();
		
		param = PropertyUtil.getSystemProperty(TEST_BAD_CHECKSUM_LOG);
		checksumTest = Boolean.valueOf(param).booleanValue();
		
		if(checksumTest)
		{
			infoPath = "extinout/T_RecoverBadChecksumLog.info";
			testService = "BadChecksumLogTest";
		}
	}


	/**
	    See T_Recovery for the general testing frame work

		@exception T_Fail Unexpected behaviour from the API
	 */
	public void runTests() throws T_Fail {

		getConfig();
		int tests = 0;
		if (setup) tests++;
		if (test1) tests++;
		if (test2) tests++;
		if (test3) tests++;
		if (test4) tests++;
		if (test5) tests++;
		if (test6) tests++;
		if (test7) tests++;
		
		if (tests != 1)
			throw T_Fail.testFailMsg("One & only one of the bad log recovery test should be run");

		if (!SanityManager.DEBUG)
		{
			REPORT("recoverBadLog cannot be run on an insane server");
			return;
		}

		try {
			contextService = ContextService.getFactory();

			File ifile = new File(infoPath);

			//
			// no checkpoint log record in any of the log files - unless this value
			// is reset. LogToFile.TEST_LOG_SWITCH_LOG
			// this will cause recovery to switch log without checkpointing
			//
			SanityManager.DEBUG_SET(LogToFile.TEST_LOG_SWITCH_LOG);

			// don't want background checkpoint process to be running
			SanityManager.DEBUG_SET(DaemonService.DaemonOff);

			// see if we are testing encryption
			startParams = T_Util.setEncryptionParam(startParams);

			if (setup)				// the first test cleans up and start from fresh
			{
				// remove the service directory to ensure a clean run
				REPORT("_______________________________________________________");
				REPORT("\n\t\tcleaning up database for recovering from bad logs");
				REPORT("_______________________________________________________");

				// don't automatic boot this service if it gets left around
				if (startParams == null) 
					startParams = new Properties();
				
				startParams.put(Property.NO_AUTO_BOOT, Boolean.TRUE.toString());
				// remove the service directory to ensure a clean run
				startParams.put(Property.DELETE_ON_CREATE, Boolean.TRUE.toString());

				factory = (RawStoreFactory) Monitor.createPersistentService(getModuleToTestProtocolName(),
															  testService,
															  startParams);
				// create a database with nothing

				// delete the info file
				if (ifile.exists())
					ifile.delete();

				return;				// don't run anything now

			}
			else					// not setup, recover it
			{
				REPORT("_______________________________________________________");
				
				String message = "\n\t\tRunning bad log test ";
				if (checksumTest)
					message = "\n\t\tRunning bad checksum log test ";
				if (test1)
					REPORT(message + " 1");
				if (test2)
					REPORT(message + " 2");
				if (test3)
					REPORT(message + " 3");
				if (test4)
					REPORT(message + " 4");
				if (test5)
					REPORT(message + " 5");
				if (test6)
					REPORT(message + " 6");
				if (test7)
					REPORT(message + " 7");

				REPORT("_______________________________________________________");

				//if external input output files does not exist ,create one
				File ifdir = new File("extinout");
				if(!ifdir.exists())
					ifdir.mkdirs();

				try
				{
					// make sure it does exist
					infofile = new RandomAccessFile(ifile, "rw");
				}
				catch (IOException ioe)
				{
					System.out.println("Cannot write to temporary file " +
									   infoPath + 
									   ".  Please make sure it is correct, if not, please set the property " +
									   "TestBadLogInfo=<where temp files should go>");
					
					throw T_Fail.exceptionFail(ioe);
				}

				if (!Monitor.startPersistentService(testService, startParams))
					throw T_Fail.testFailMsg("Monitor didn't know how to restart service: " + testService);

				factory = (RawStoreFactory) Monitor.findService(getModuleToTestProtocolName(), testService);
				logFactory =(LogToFile) Monitor.findServiceModule(factory, factory.getLogFactoryModule());
				
			}
		} catch (StandardException mse) {
			throw T_Fail.exceptionFail(mse);
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

		try {
			

			// these tests can be run in any order
			RTest1();
			RTest2();
			RTest3();
			RTest4();
			RTest5();
			RTest6();
			RTest7();

			if (test1)
				STest1();

			if (test2)
				STest2();
				
			if (test3)
				STest3();
						
			if (test4)
				STest4();

			if(test5) 
				STest5();

			if(test6) 
				STest6();

			if(test7) 
				STest7();

			if (infofile != null)
				infofile.close();

		} catch (StandardException se) {

			throw T_Fail.exceptionFail(se);
		}
		catch (IOException ioe)
		{
			throw T_Fail.exceptionFail(ioe);
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
	 * test1 manufactures a log with the following recoverable 'defects':
	 *		- a log file that only have a single large 1/2 written log record
	 */
	protected void STest1() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		///////////////////////////////////////////
		//// log switch without checkpoint here ///
		///////////////////////////////////////////
		factory.checkpoint();

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// make a really big record - fill 80% of the page
			int numcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));

			T_RawStoreRow bigrow = new T_RawStoreRow(numcol);
			String string1 = "01234567890123456789"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string1);

			// if overhead is > 80%, then reduce the row size until it fits
			RecordHandle rh = null;
			while(numcol > 0)
			{
				try {
					rh = t_util.t_insert(page, bigrow);
					break;
				} catch (StandardException se) {
					bigrow.setColumn(--numcol, (String) null);
				}
			}
			if (numcol == 0)
				throw T_Fail.testFailMsg("cannot fit any column into the page");

			

			t_util.t_commit(t);

			// make a big log record - update row
			String string2 = "abcdefghijklmnopqrst"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string2);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			Page p2 = t_util.t_addPage(c);		// do something so we get the beginXact log
								// record out of the way
			t_util.t_insert(p2, new T_RawStoreRow(REC_001));


			///////////////////////////////////////////
			//// log switch without checkpoint here ///
			///////////////////////////////////////////
			factory.checkpoint();

			//////////////////////////////////////////////////////////
			// writing approx 1/2 log record to the end of the log - 
			// NO MORE LOG RECORD SHOULD BE WRITTEN,
			//////////////////////////////////////////////////////////
			if(!checksumTest)
			{
				SanityManager.DEBUG_SET(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
				System.setProperty(LogToFile.TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES, Integer.toString(numcol*20));
			}
			
			logFactory.flushAll();

			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, bigrow.getRow(), null);

			if(checksumTest)
				simulateLogFileCorruption();

			////////////////////////////////////////////////////////

			REPORT("badlog test1: cid = " + cid + " numcol " + numcol);

			register(key(1,1), cid);
			register(key(1,2), numcol);
		}
		finally
		{
			SanityManager.DEBUG_CLEAR(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
		}
	}

	/*
	 * test recovery of test 1
	 */
	void RTest1() throws T_Fail, StandardException
	{
		long cid = find(key(1, 1));
		if (cid < 0)
		{
			REPORT("bad log test1 not run");
			return;
		}
		int numcol = (int)find(key(1,2));

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			int optimisticNumcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));
			T_RawStoreRow bigrow = new T_RawStoreRow(optimisticNumcol);
			for (int i = 0; i < optimisticNumcol; i++)
				bigrow.setColumn(i, (String) null);

			page.fetchFromSlot(
                (RecordHandle) null, 0, bigrow.getRow(), 
                (FetchDescriptor) null,
                false);

			Storable column;
			String string1 = "01234567890123456789"; // the original 20 char string

			for (int i = 0; i < numcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!(column.toString().equals(string1)))
					throw T_Fail.testFailMsg("Column " + i + " value incorrect, got :" + column.toString());
			}
			for (int i = numcol; i < optimisticNumcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!column.isNull())
					throw T_Fail.testFailMsg("Column " + i + 
											 " expect Null, got : " + column.toString());
			}

			REPORT("RTest1 passed");

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/*
	 * test2 manufactures a log with the following recoverable 'defects':
	 *		- a log file that ends with a large 1/2 written log record
	 */
	protected void STest2() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// make a really big record - fill 80% of the page with 20 bytes row
			int numcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));

			T_RawStoreRow bigrow = new T_RawStoreRow(numcol);
			String string1 = "01234567890123456789"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string1);

			// if overhead is > 80%, then reduce the row size until it fits
			RecordHandle rh = null;
			while(numcol > 0)
			{
				try {
					rh = t_util.t_insert(page, bigrow);
					break;
				} catch (StandardException se) {
					bigrow.setColumn(--numcol, (String) null);
				}
			}
			if (numcol == 0)
				throw T_Fail.testFailMsg("cannot fit any column into the page");

			rh = t_util.t_insert(page, bigrow);

			t_util.t_commit(t);

			// make a big log record - update row
			String string2 = "abcdefghijklmnopqrst"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string2);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			//////////////////////////////////////////////////////////
			// writing approx 1/2 log record to the end of the log - 
			// NO MORE LOG RECORD SHOULD BE WRITTEN,
			//////////////////////////////////////////////////////////
			if(!checksumTest)
			{
				SanityManager.DEBUG_SET(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
				System.setProperty(LogToFile.TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES,Integer.toString(numcol*20));
			}

			logFactory.flushAll();
			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, bigrow.getRow(), null);
			
			if(checksumTest)
				simulateLogFileCorruption();

			////////////////////////////////////////////////////////

			REPORT("badlog test2: cid = " + cid + " numcol " + numcol);

			register(key(2,1), cid);
			register(key(2,2), numcol);
		}
		finally
		{
			SanityManager.DEBUG_CLEAR(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
		}
	}

	/*
	 * test recovery of test 2
	 */
	void RTest2() throws T_Fail, StandardException
	{
		long cid = find(key(2, 1));
		if (cid < 0)
		{
			REPORT("bad log test2 not run");
			return;
		}
		int numcol = (int)find(key(2,2));

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			int optimisticNumcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));
			T_RawStoreRow bigrow = new T_RawStoreRow(optimisticNumcol);
			for (int i = 0; i < optimisticNumcol; i++)
				bigrow.setColumn(i, (String) null);

			page.fetchFromSlot(
                (RecordHandle) null, 0, bigrow.getRow(), 
                (FetchDescriptor) null,
                false);
			Storable column;
			String string1 = "01234567890123456789"; // the original 20 char string

			for (int i = 0; i < numcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!(column.toString().equals(string1)))
					throw T_Fail.testFailMsg("Column " + i + " value incorrect, got :" + column.toString());
			}
			for (int i = numcol; i < optimisticNumcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!column.isNull())
					throw T_Fail.testFailMsg("Column " + i + 
											 " expect Null, got : " + column.toString());
			}

			REPORT("RTest2 passed");

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}



	/*
	 * test3 manufactures a log with the following recoverable 'defects':
	 *    - a log with multiple files but no checkpoint log record
	 *	  - a last log file with a paritally written log record at the end
	 */
	protected void STest3() throws T_Fail, StandardException
	{
		int numtrans = 7;
		int numpages = 7;
		int i,j;

		// this is basically T_Recovery S203 with a couple of log switches
		try
		{
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

			///////////////////////////////////////////
			//// log switch without checkpoint here ///
			///////////////////////////////////////////
			factory.checkpoint();


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

			///////////////////////////////////////////
			//// log switch without checkpoint here ///
			///////////////////////////////////////////
			factory.checkpoint();


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

			///////////////////////////////////////////
			//// log switch without checkpoint here ///
			///////////////////////////////////////////
			factory.checkpoint();


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


			///////////////////////////////////////////////////////////
			//// now write a 1/2 log record to the end of the log
			//////////////////////////////////////////////////////////
			t[3].switchTransactionContext();// this is going to be an
											// incomplete transaction

			// make a full page and then copy and purge it to another page
			Page badPage1 = t_util.t_addPage(c[3]);
			Page badPage2 = t_util.t_addPage(c[3]);
			T_RawStoreRow row;
			for (i = 0, row = new T_RawStoreRow("row at slot " + i);
				 badPage1.spaceForInsert();
				 i++, row = new T_RawStoreRow("row at slot " + i))
			{
				if (t_util.t_insertAtSlot(badPage1, i, row, Page.INSERT_UNDO_WITH_PURGE) == null)
					break;
			}

			//////////////////////////////////////////////////////////
			// writing 200 bytes of the log record to the end of the log - 
			// NO MORE LOG RECORD SHOULD BE WRITTEN,
			//////////////////////////////////////////////////////////
			if(!checksumTest)
			{
				SanityManager.DEBUG_SET(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
				System.setProperty(LogToFile.TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES, "200");
			}
			logFactory.flushAll();


			// RESOLVE:
			// copy and purge actually generates 2 log records, this is
			// actually not a good operation to use for this test.  Just make
			// sure the first log record is > 400 or else the log will be hosed
			//
			badPage1.copyAndPurge(badPage2, 0, i, 0);

			t[3].resetContext();

			if(checksumTest)
				simulateLogFileCorruption();

			////////////////////////////////////////////////////////

			REPORT("badlog test3: numtrans " + numtrans + " numpages " + numpages);

			for (i = 0; i < numtrans; i++)
			{
				register(key(3, i+10), cid[i]);

				String str = "container " + i + ":" + find(key(3,i+10)) + " pages: ";

				for (j = 0; j < numpages; j++)
				{
					str += pagenum[i][j] + " ";
					register(key(3, (i+1)*1000+j), pagenum[i][j]);
				}
				REPORT("\t" + str);
			}

			register(key(3,1), numtrans); 
			register(key(3,2), numpages);
			register(key(3,3), badPage1.getPageNumber());
			register(key(3,4), badPage2.getPageNumber());

		}
		finally
		{
			SanityManager.DEBUG_CLEAR(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
		}
	}

	/*
	 * test recovery of test3
	 */
	void RTest3() throws T_Fail, StandardException
	{
		int numtrans = (int)find(key(3,1));
		if (numtrans < 0)
		{
			REPORT("bad log test3 not run");
			return;
		}

		int numpages = (int)find(key(3,2));
		long badPagenum1 = find(key(3,3)); // these two pages are involved in
										   // the 1/2 written log record, make
										   // sure they are not corrupted
		long badPagenum2 = find(key(3,4));

		Transaction t = t_util.t_startTransaction();

		long[] cid = new long[numtrans];
		ContainerHandle[] c = new ContainerHandle[numtrans];

		long[][] pagenum = new long[numtrans][numpages];
		Page[][] page = new Page[numtrans][numpages];

		int i,j;

		for (i = 0; i < numtrans; i++)
		{
			cid[i] = find(key(3, i+10));

			c[i] = t_util.t_openContainer(t, 0, cid[i], true);
			
			for (j = 0; j < numpages; j++)
			{
				pagenum[i][j] = find(key(3, (i+1)*1000+j));

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
		try 
		{
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

			// now check the two bad pages - they are in c[3] and should be empty
			Page badPage1 = t_util.t_getPage(c[3], badPagenum1);
			Page badPage2 = t_util.t_getPage(c[3], badPagenum2);
			t_util.t_checkRecordCount(badPage1, 0, 0);
			t_util.t_checkRecordCount(badPage2, 0, 0);

			REPORT("RTest3 passed: numtrans " + numtrans + " numpages " + numpages);

			for (i = 0; i < numtrans; i++)
			{
				String str = "container " + i + ":" + cid[i] + " pages: ";
				for (j = 0; j < numpages; j++)
					str += pagenum[i][j] + " ";
				REPORT("\t" + str);
			}
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

		
	/*
	 * test4 manufactures a log with the following recoverable 'defects':
	 * - a log file that only has the partial log instance(7 bytes instead of 8
	 * bytes writtne) of a log record written 
	 */
	protected void STest4() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// make a really big record - fill 80% of the page
			int numcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));

			T_RawStoreRow bigrow = new T_RawStoreRow(numcol);
			String string1 = "01234567890123456789"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string1);

			// if overhead is > 80%, then reduce the row size until it fits
			RecordHandle rh = null;
			while(numcol > 0)
			{
				try {
					rh = t_util.t_insert(page, bigrow);
					break;
				} catch (StandardException se) {
					bigrow.setColumn(--numcol, (String) null);
				}
			}
			if (numcol == 0)
				throw T_Fail.testFailMsg("cannot fit any column into the page");

			

			t_util.t_commit(t);

			// make a big log record - update row
			String string2 = "abcdefghijklmnopqrst"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string2);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			Page p2 = t_util.t_addPage(c);		// do something so we get the beginXact log
								// record out of the way
			t_util.t_insert(p2, new T_RawStoreRow(REC_001));


			//////////////////////////////////////////////////////////
			// writing approx 1/2 of log record  instance to the end of the log - 
			// NO MORE LOG RECORD SHOULD BE WRITTEN,
			// Length  4 bytes + 7(8) bytes of log record instance
			//////////////////////////////////////////////////////////
			if(!checksumTest)
			{
				SanityManager.DEBUG_SET(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
				System.setProperty(LogToFile.TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES, Integer.toString(11));
			}

			logFactory.flushAll();
			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, bigrow.getRow(), null);

			if(checksumTest)
				simulateLogFileCorruption();

			////////////////////////////////////////////////////////

			REPORT("badlog test4: cid = " + cid + " numcol " + numcol);

			register(key(4,1), cid);
			register(key(4,2), numcol);
		}
		finally
		{
			SanityManager.DEBUG_CLEAR(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
		}
	}

	/*
	 * test recovery of test 4
	 */
	void RTest4() throws T_Fail, StandardException
	{
		long cid = find(key(4, 1));
		if (cid < 0)
		{
			REPORT("bad log test4 not run");
			return;
		}
		int numcol = (int)find(key(4,2));

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			int optimisticNumcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));
			T_RawStoreRow bigrow = new T_RawStoreRow(optimisticNumcol);
			for (int i = 0; i < optimisticNumcol; i++)
				bigrow.setColumn(i, (String) null);

			page.fetchFromSlot(
                (RecordHandle) null, 0, bigrow.getRow(), 
                (FetchDescriptor) null,
                false);

			Storable column;
			String string1 = "01234567890123456789"; // the original 20 char string

			for (int i = 0; i < numcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!(column.toString().equals(string1)))
					throw T_Fail.testFailMsg("Column " + i + " value incorrect, got :" + column.toString());
			}
			for (int i = numcol; i < optimisticNumcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!column.isNull())
					throw T_Fail.testFailMsg("Column " + i + 
											 " expect Null, got : " + column.toString());
			}

			REPORT("RTest4 passed");

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}
	
	/*
	 * test5 manufactures a log with the following recoverable 'defects':
	 * - a log file that only has the partial log record length (3 bytes instead of 4
	 * bytes writtne) of a log record written in the beginning
	 */
	protected void STest5() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// make a really big record - fill 80% of the page
			int numcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));

			T_RawStoreRow bigrow = new T_RawStoreRow(numcol);
			String string1 = "01234567890123456789"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string1);

			// if overhead is > 80%, then reduce the row size until it fits
			RecordHandle rh = null;
			while(numcol > 0)
			{
				try {
					rh = t_util.t_insert(page, bigrow);
					break;
				} catch (StandardException se) {
					bigrow.setColumn(--numcol, (String) null);
				}
			}
			if (numcol == 0)
				throw T_Fail.testFailMsg("cannot fit any column into the page");

			

			t_util.t_commit(t);

			// make a big log record - update row
			String string2 = "abcdefghijklmnopqrst"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string2);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			Page p2 = t_util.t_addPage(c);		// do something so we get the beginXact log
								// record out of the way
			t_util.t_insert(p2, new T_RawStoreRow(REC_001));


			//////////////////////////////////////////////////////////
			// writing approx 3 bytes of log record to the end of the log - 
			// NO MORE LOG RECORD SHOULD BE WRITTEN,
			// Length  3 bytes (4) of log record length
			//////////////////////////////////////////////////////////
			if(!checksumTest)
			{
				SanityManager.DEBUG_SET(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
				System.setProperty(LogToFile.TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES, Integer.toString(3));
			}
			logFactory.flushAll();

			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, bigrow.getRow(), null);

			if(checksumTest)
				simulateLogFileCorruption();

			////////////////////////////////////////////////////////

			REPORT("badlog test5: cid = " + cid + " numcol " + numcol);

			register(key(5,1), cid);
			register(key(5,2), numcol);
		}
		finally
		{
			SanityManager.DEBUG_CLEAR(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
		}
	}

	/*
	 * test recovery of test 5
	 */
	void RTest5() throws T_Fail, StandardException
	{
		long cid = find(key(5, 1));
		if (cid < 0)
		{
			REPORT("bad log test5 not run");
			return;
		}
		int numcol = (int)find(key(5,2));

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			int optimisticNumcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));
			T_RawStoreRow bigrow = new T_RawStoreRow(optimisticNumcol);
			for (int i = 0; i < optimisticNumcol; i++)
				bigrow.setColumn(i, (String) null);

			page.fetchFromSlot(
                (RecordHandle) null, 0, bigrow.getRow(), 
                (FetchDescriptor) null,
                false);

			Storable column;
			String string1 = "01234567890123456789"; // the original 20 char string

			for (int i = 0; i < numcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!(column.toString().equals(string1)))
					throw T_Fail.testFailMsg("Column " + i + " value incorrect, got :" + column.toString());
			}
			for (int i = numcol; i < optimisticNumcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!column.isNull())
					throw T_Fail.testFailMsg("Column " + i + 
											 " expect Null, got : " + column.toString());
			}

			REPORT("RTest5 passed");

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}
		
	/*
	 * test6 manufactures a log with the following recoverable 'defects':
	 * - a log file that only has the log record with partial data portion
	 * written (approximately (1997/2 (data)+ 16(log records ov)))	 */
	protected void STest6() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// make a really big record - fill 80% of the page
			int numcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));

			T_RawStoreRow bigrow = new T_RawStoreRow(numcol);
			String string1 = "01234567890123456789"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string1);

			// if overhead is > 80%, then reduce the row size until it fits
			RecordHandle rh = null;
			while(numcol > 0)
			{
				try {
					rh = t_util.t_insert(page, bigrow);
					break;
				} catch (StandardException se) {
					bigrow.setColumn(--numcol, (String) null);
				}
			}
			if (numcol == 0)
				throw T_Fail.testFailMsg("cannot fit any column into the page");

			

			t_util.t_commit(t);

			// make a big log record - update row
			String string2 = "abcdefghijklmnopqrst"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string2);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			Page p2 = t_util.t_addPage(c);		// do something so we get the beginXact log
								// record out of the way
			t_util.t_insert(p2, new T_RawStoreRow(REC_001));


			//////////////////////////////////////////////////////////
			// writing (1997/2 (data)+ 16(log records ov)) bytes of log record to the end of the log - 
			// NO MORE LOG RECORD SHOULD BE WRITTEN,
			//////////////////////////////////////////////////////////
			if(!checksumTest)
			{
				SanityManager.DEBUG_SET(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
				System.setProperty(LogToFile.TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES, Integer.toString((1997/2) + 16));
			}
			logFactory.flushAll();
			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, bigrow.getRow(), null);

			if(checksumTest)
				simulateLogFileCorruption();

			////////////////////////////////////////////////////////

			REPORT("badlog test6: cid = " + cid + " numcol " + numcol);

			register(key(6,1), cid);
			register(key(6,2), numcol);
		}
		finally
		{
			SanityManager.DEBUG_CLEAR(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
		}
	}

	/*
	 * test recovery of test 6
	 */
	void RTest6() throws T_Fail, StandardException
	{
		long cid = find(key(6, 1));
		if (cid < 0)
		{
			REPORT("bad log test6 not run");
			return;
		}
		int numcol = (int)find(key(6,2));

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			int optimisticNumcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));
			T_RawStoreRow bigrow = new T_RawStoreRow(optimisticNumcol);
			for (int i = 0; i < optimisticNumcol; i++)
				bigrow.setColumn(i, (String) null);

			page.fetchFromSlot(
                (RecordHandle) null, 0, bigrow.getRow(), 
                (FetchDescriptor) null,
                false);

			Storable column;
			String string1 = "01234567890123456789"; // the original 20 char string

			for (int i = 0; i < numcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!(column.toString().equals(string1)))
					throw T_Fail.testFailMsg("Column " + i + " value incorrect, got :" + column.toString());
			}
			for (int i = numcol; i < optimisticNumcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!column.isNull())
					throw T_Fail.testFailMsg("Column " + i + 
											 " expect Null, got : " + column.toString());
			}

			REPORT("RTest6 passed");

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}
	/*
	 * test7 manufactures a log with the following recoverable 'defects':
	 * - a log file that has the last log record with partial end length
	 * written( 3 of 4 bytes). instead of (1997(data) + 16 (log records overhead)) write (1997 + 15) 
	 */
	protected void STest7() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// make a really big record - fill 80% of the page
			int numcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));

			T_RawStoreRow bigrow = new T_RawStoreRow(numcol);
			String string1 = "01234567890123456789"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string1);

			// if overhead is > 80%, then reduce the row size until it fits
			RecordHandle rh = null;
			while(numcol > 0)
			{
				try {
					rh = t_util.t_insert(page, bigrow);
					break;
				} catch (StandardException se) {
					bigrow.setColumn(--numcol, (String) null);
				}
			}
			if (numcol == 0)
				throw T_Fail.testFailMsg("cannot fit any column into the page");

			

			t_util.t_commit(t);

			// make a big log record - update row
			String string2 = "abcdefghijklmnopqrst"; // 20 char string
			for (int i = 0; i < numcol; i++)
				bigrow.setColumn(i, string2);

			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			Page p2 = t_util.t_addPage(c);		// do something so we get the beginXact log
								// record out of the way
			t_util.t_insert(p2, new T_RawStoreRow(REC_001));


			//////////////////////////////////////////////////////////
			// writing only 3 bytes of end length of the log record to the end of the log - 
			//i.e: instead of (1997(data) + 16 (log records overhead)) write (1997 + 15) 
			// NO MORE LOG RECORD SHOULD BE WRITTEN,
			//////////////////////////////////////////////////////////
			if(!checksumTest)
			{
				SanityManager.DEBUG_SET(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
				System.setProperty(LogToFile.TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES, Integer.toString(1997+15));
			}
			logFactory.flushAll();
			int slot = page.getSlotNumber(rh);
			page.updateAtSlot(slot, bigrow.getRow(), null);

			if(checksumTest)
				simulateLogFileCorruption();


			////////////////////////////////////////////////////////

			REPORT("badlog test7: cid = " + cid + " numcol " + numcol);

			register(key(7,1), cid);
			register(key(7,2), numcol);
		}
		finally
		{
			SanityManager.DEBUG_CLEAR(LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE);
		}
	}

	/*
	 * test recovery of test 7
	 */
	void RTest7() throws T_Fail, StandardException
	{
		long cid = find(key(6, 1));
		if (cid < 0)
		{
			REPORT("bad log test7 not run");
			return;
		}
		int numcol = (int)find(key(6,2));

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			int optimisticNumcol = (int)((RawStoreFactory.PAGE_SIZE_MINIMUM*8)/(10*20));
			T_RawStoreRow bigrow = new T_RawStoreRow(optimisticNumcol);
			for (int i = 0; i < optimisticNumcol; i++)
				bigrow.setColumn(i, (String) null);

			page.fetchFromSlot(
                (RecordHandle) null, 0, bigrow.getRow(), 
                (FetchDescriptor) null,
                false);

			Storable column;
			String string1 = "01234567890123456789"; // the original 20 char string

			for (int i = 0; i < numcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!(column.toString().equals(string1)))
					throw T_Fail.testFailMsg("Column " + i + " value incorrect, got :" + column.toString());
			}
			for (int i = numcol; i < optimisticNumcol; i++)
			{
				column = bigrow.getStorableColumn(i);
				if (!column.isNull())
					throw T_Fail.testFailMsg("Column " + i + 
											 " expect Null, got : " + column.toString());
			}

			REPORT("RTest7 passed");

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}



	/*
	 * simulate log corruption to test the checksuming of log records. 
	 */
	private void simulateLogFileCorruption() throws T_Fail, StandardException
	{
		long filenum;
		long filepos;
		long amountOfLogWritten;
		LogCounter logInstant = (LogCounter)logFactory.getFirstUnflushedInstant();
		filenum = logInstant.getLogFileNumber();
		filepos = logInstant.getLogFilePosition();
		logFactory.flushAll();
		logInstant = (LogCounter)logFactory.getFirstUnflushedInstant();
		filenum = logInstant.getLogFileNumber();
		amountOfLogWritten = logInstant.getLogFilePosition() - filepos;

		// write some random  garbage into the log file , 
		// purpose of doing this is to test that recovery works correctly when 
		// log records in the end of a log file did not get wrtten completely
		// and in the correct order. 

		try{
			StorageRandomAccessFile log = logFactory.getLogFileToSimulateCorruption(filenum) ;
		
			int noWrites = (int) amountOfLogWritten / 512;
			//mess up few bytes in every block of a 512 bytes.
			filepos += 512;
			java.util.Random r = new java.util.Random();
			for(int i = 0 ; i < noWrites ; i++)
			{
				REPORT("corruptig log file : filenum " + filenum + " fileposition " + filepos);
				log.seek(filepos);
				log.writeInt(r.nextInt());
				filepos +=512;

			}
            log.sync();
			log.close();
		}catch(IOException ie)
		{
			throw T_Fail.exceptionFail(ie);
		}
		
	}

}



