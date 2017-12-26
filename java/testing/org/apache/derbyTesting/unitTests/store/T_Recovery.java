/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_Recovery

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

import org.apache.derby.iapi.store.raw.*;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.impl.store.raw.log.LogCounter;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.locks.*;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.types.SQLChar;

import org.apache.derby.iapi.types.DataValueDescriptor;


import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.io.*;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.AccessController;
import java.util.Properties;


/**
	A protocol unit test for recovery.

    To run, create a derby.properties file in a new directory with the
	contents

	derby.module.test.recovery=org.apache.derbyTesting.unitTests.store.T_Recovery

    Execute

	java -DSetupRecovery=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
	java -DTestRecovery=true org.apache.derbyTesting.unitTests.harness.UnitTestMain
*/

public class T_Recovery extends T_Generic {

	private static final String testService = "RecoveryTest";

	static final String REC_001 = "McLaren";
	static final String REC_002 = "Ferrari";
	static final String REC_003 = "Benetton";
	static final String REC_004 = "Prost";
	static final String REC_005 = "Tyrell";
	static final String REC_006 = "Derby, Natscape, Goatscape, the popular names";
	static final String REC_UNDO = "Lotus";
	static final String REC_NULL = "NULL";

	static final String SP1 = "savepoint1";
	static final String SP2 = "savepoint2";

	static final FormatableBitSet BS_COL_0 = new FormatableBitSet(1);

	private RandomAccessFile filein = null;
	private RandomAccessFile fileout = null;

	private boolean setupRecovery;
	private boolean testRecovery;
	private static final String infoPath = "extinout/T_Recovery.info";


	private static final String SETUP_RECOVERY = "SetupRecovery";
	private static final String TEST_RECOVERY = "TestRecovery";
	private static final String RECOVERY_TESTPATH = "RecoveryTestPath";


	RawStoreFactory	factory;
	LockFactory  lf;
	ContextService contextService;
	UUIDFactory uuidfactory;
	T_Util t_util;

	public T_Recovery() {
		super();
		BS_COL_0.set(0);
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
	
		String param = PropertyUtil.getSystemProperty(SETUP_RECOVERY);
		setupRecovery = Boolean.valueOf(param).booleanValue();

		param = PropertyUtil.getSystemProperty(TEST_RECOVERY);
		testRecovery = Boolean.valueOf(param).booleanValue();
	}



	/**
		Tests in here come in pairs (Snnn Rnnn), one to set it up, one to test
		it after recovery.  Information that needs to be passed from the setup
		to the recovery should, ideally, be written out to a database.  For
		now, it is written out as a pair of (key,value) long in the file
		T_Recovery.info.

		To make sure you don't accidently tramples on someone else's key, 
		encode your test number (nnn) by shifting over 32 bits and then add
		your key.  Multiple invocations which needs paramaters saved should
		be encoded futher.

		001 &lt; nnn &lt; 200 -  no recovery undo
		200 &lt; nnn &lt; 400 -  recovery undo

		@exception T_Fail Unexpected behaviour from the API
	 */
	public void runTests() throws T_Fail {

		getConfig();

		if (!(setupRecovery ^ testRecovery))
			throw T_Fail.testFailMsg("One & only one of the SetupRecovery and TestRecovery properties must be set");

		try {

			uuidfactory = getMonitor().getUUIDFactory();
			if (uuidfactory == null) {
				throw T_Fail.testFailMsg("UUIDFactory.MODULE not found");
			}

			// see if we are testing encryption
			startParams = T_Util.setEncryptionParam(startParams);

			contextService = getContextService();

			if (testRecovery)
			{
				if (!startPersistentService(testService, startParams))
					throw T_Fail.testFailMsg("Monitor didn't know how to restart service: " + testService);
				factory = (RawStoreFactory) findService(getModuleToTestProtocolName(), testService);

			}
			else					// setup
			{
				// don't automatic boot this service if it gets left around
				if (startParams == null) 
					startParams = new Properties();
				
				startParams.put(Property.NO_AUTO_BOOT, Boolean.TRUE.toString());

				// do not make T_Recovery test a source, or it won't run on a
				// syncless configuration
				//
				// remove the service directory to ensure a clean run
				startParams.put(Property.DELETE_ON_CREATE, Boolean.TRUE.toString());

				// keep all log files for diagnostics
				startParams.put(RawStoreFactory.KEEP_TRANSACTION_LOG, "true");

				factory = (RawStoreFactory) createPersistentService(getModuleToTestProtocolName(),
																  testService,
																  startParams);
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

			if (setupRecovery)
			{
				// dmls
				S001();			// insert 1 row
				S002();			// insert a bunch of rows
				S003();			// update row
				S004();			// update field
				S005();			// purge and delete
				S006();			// page allocation
				S007();			// page deallocation
				S008();			// rollback of page deallocation
				S009();			// deallocation and reuse of page
				S010();			// allocation/deallocation with overflow pages
				S011();			// allocate a lot of pages so that > 1
								// allocation pages are needed
				S012();			// test page estimation

				// ddls
				S020();			// create multiple containers
				S022();
				

				// committed transactions
				S100();			// multiple intervening transactions
				S101();			// transaction with rollback to savepoints

				// aborted transaction
				//  - these transactions are rollback during runtime - 

				// incomplete transaction 
				//  - these transactions are rollback during recovery -

				S200();			// 1 incomplete transaction
				S201();			// multiple intervening incomplete transaction
				S202();			// incomplete transaction with rollback to
								// savepoint 
				S203();			// incomplete and committed and aborted
									// transaction with intervening rollback to savepoints
				S204();			// incomplete and committed and aborted
									// internal transactions

				// incomplete transaction with ddls
				S300();			// incomplete transactions with drop containers
				S301();			// incomplete transactions with create containers
				S302();         //purging rows with no data logged
				S303();         //purging long columns with no data logged
				S304();         //pruging long rows with no data logged.
			}

			if (testRecovery || setupRecovery)
			{
				// basic recovery
				R001();
				R002();
				R003();
				R004();
				R005();
				R006();
				R007();
				R008();
				R009();
				R010();
				R011();
				R012();	

				R020();
				R022();

				R100();
				R101();
				R302();

				// the following tests depends on recovery to roll back the
				// changes. Only test them after recovery and not during setup
				if (testRecovery)
				{

					R200();
					R201();
					R202();
					R203();
					R204();

					R300();
					R301();
					R303();
					R304();

					R999();
				}
			}

			if (setupRecovery)
			{
				S999();		// always run this last, this leaves a 1/2
							// written log record at the end of the log
							// and it leaves a page latched
			}

			if (fileout != null)
			{
				fileout.close();
				fileout = null;
			}


			if (filein != null)
			{
				filein.close();
				filein = null;
			}

		} catch (Throwable t) {

			SanityManager.showTrace(t);
			System.out.println("caught exception t " + t);

			t.printStackTrace();

			// this test should exit the JVM as abruptly as possible.
			System.exit(0);
		}
	}

	private long find(long inkey) throws T_Fail
	{
		try 
		{
			if (filein == null)
			{
				// make sure it does exist
				File infoFile = new File(infoPath);
				if (infoFile.exists())
				{
					try
					{
						filein = new RandomAccessFile(infoFile, "r");
					}
					catch (IOException ioe)
					{
						System.out.println("Cannot write to temporary file " +
										   infoPath + 
										   ".  Please make sure it is correct, if not, please set the property " +
										   "RecoveryTestPath=<where temp files should go>");
						throw T_Fail.exceptionFail(ioe);
					}
				}
				else
					return -1;
			}

			filein.seek(0);

			long key;
			while(true)
			{
				key = filein.readLong();
				if (key == inkey)
				{
					long value = filein.readLong();
					// System.out.println("found " + key + " " + value);
					return value;
				}
				filein.readLong();
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
			if (fileout == null)
			{
				// make sure it does not exist
				File infofile = new File(infoPath);
				if (infofile.exists())
					infofile.delete();
				
				//if external input output files dir does not exist ,create one
				File ifdir = new File("extinout");
				if(!ifdir.exists())
					ifdir.mkdirs();
				fileout = new RandomAccessFile(infoPath, "rw");
			}

			fileout.writeLong(key);
			fileout.writeLong(value);
		}
		catch (IOException ioe)
		{
			T_Fail.exceptionFail(ioe);
		}
	}


	/* 
	 * test 1 - insert 1 row
	 */
	protected void S001() throws T_Fail, StandardException 
	{
		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, 0);

		t_util.t_commit(t);

		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
		Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
		try
		{

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
	
			RecordHandle r1 = t_util.t_insertAtSlot(page, 0, row1);

			page.unlatch();

			c.close();

			register(key(1, 1), cid);
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		REPORT("setup S001: " + cid);
	}

	/* recover test 1 */
	protected void R001() throws T_Fail, StandardException
	{
		long cid = find(key(1, 1));
		if (cid < 0)
		{
			REPORT("R001 not run");
			return;
		}

		Transaction t = t_util.t_startTransaction();

		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, 1, 1);
			t_util.t_checkFieldCount(page, 0, 1);
		
			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			page.unlatch();

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R001: containerId " + cid);

	}

	/*
	 * test 2 - insert a bunch of rows into one container
	 */
	protected void S002() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, 0);

		t_util.t_commit(t);

		try
		{	
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
			T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
			T_RawStoreRow row5 = new T_RawStoreRow(REC_005);

			RecordHandle r1 = t_util.t_insertAtSlot(page, 0, row1);

			RecordHandle r2 = t_util.t_insertAtSlot(page, 1, row2);

			RecordHandle r3 = (r2 == null) ? r2 : t_util.t_insertAtSlot(page, 2, row3);

			RecordHandle r4 = (r3 == null) ? r3 : t_util.t_insertAtSlot(page, 3, row4);

			RecordHandle r5 = (r4 == null) ? r4 : t_util.t_insertAtSlot(page, 4, row5);

			REPORT("setup S002: containerId " + cid + " recordCount " + page.recordCount());
	
			register(key(2, 1), cid);
			register(key(2, 2), page.recordCount());
			page.unlatch();
			c.close();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/* recover test 2 */
	protected void R002() throws T_Fail, StandardException
	{
		long cid = find(key(2,1));
		if (cid < 0)
		{
			REPORT("R002 not run");
			return;
		}

		int recordCount = (int)find(key(2,2));

		Transaction t = t_util.t_startTransaction();

		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);


			t_util.t_checkRecordCount(page, recordCount, recordCount);

			switch(recordCount)
			{
			case 5: t_util.t_checkFetchBySlot(page, 4, REC_005, false, false);
			case 4: t_util.t_checkFetchBySlot(page, 3, REC_004, false, false);
			case 3: t_util.t_checkFetchBySlot(page, 2, REC_003, false, false);
			case 2: t_util.t_checkFetchBySlot(page, 1, REC_002, false, false);
			case 1: t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			}
			page.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R002: containerId " + cid + " recordCount " + recordCount);

	}

	/*
	 *  test 3 -  update row
	 */
	protected void S003() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
	
			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);

			RecordHandle r1 = t_util.t_insertAtSlot(page, 0, row1);

			t_util.t_checkFetch(page, r1, row1);

		// REPORT("grows the #column in row");
			T_RawStoreRow upd1 = new T_RawStoreRow(3);
			upd1.setColumn(0, (String) null);
			upd1.setColumn(1, REC_003);
			upd1.setColumn(2, REC_004);

			r1 = page.updateAtSlot(0, upd1.getRow(), (FormatableBitSet) null);

			// REPORT("update that shrinks the #columns in row");
			T_RawStoreRow row2 = new T_RawStoreRow(3);
			row2.setColumn(0, REC_001);
			row2.setColumn(1, REC_002);
			row2.setColumn(2, REC_003);

			T_RawStoreRow upd2 = new T_RawStoreRow(REC_005);

			RecordHandle r2 = t_util.t_insertAtSlot(page, 1, row2);
			if (r2 != null) {
				r2 = page.updateAtSlot(1, upd2.getRow(), (FormatableBitSet) null);
			}

			t_util.t_checkFetch(page, r1, upd1);
			// first row should contain (null, REC_003, REC_004)
			DataValueDescriptor column = new SQLChar();

		// 				page, slot, field, column, forUpdate, data
			t_util.t_checkFetchColFromSlot(page, 0, 0, column, true, null);
			t_util.t_checkFetchColFromSlot(page, 0, 1, column, true, REC_003);
			t_util.t_checkFetchColFromSlot(page, 0, 2, column, true, REC_004);


			if (r2 != null)
			{
				t_util.t_checkFetch(page, r2, upd2);
				// second row should contain (REC_005)
				t_util.t_checkFetchColFromSlot(page, 1, 0, column, true, REC_005);
			}


			REPORT("setup S003: containerId " + cid + " recordCount " + page.recordCount());
			register(key(3, 1), cid);
			register(key(3, 2), page.recordCount());

			page.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/*
	 * recover test 3 
	 */
	protected void R003() throws T_Fail, StandardException
	{
		long cid = find(key(3,1));
		if (cid < 0)
		{
			REPORT("R003 not run");
			return;
		}

		int recordCount = (int)find(key(3,2));

		Transaction t = t_util.t_startTransaction();
		try
		{

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, recordCount, recordCount);


		// first row should contain (null, REC_003, REC_004)

			t_util.t_checkFieldCount(page, 0, 3);

			DataValueDescriptor column = new SQLChar();
			t_util.t_checkFetchColFromSlot(page, 0, 0, column, false, null);
			t_util.t_checkFetchColFromSlot(page, 0, 1, column, false, REC_003);
			t_util.t_checkFetchColFromSlot(page, 0, 2, column, false, REC_004);

			if (recordCount == 2)
			{
				// second row should contain (REC_005)
				t_util.t_checkFieldCount(page, 1, 1);
				t_util.t_checkFetchColFromSlot(page, 1, 0, column, false, REC_005);
			}

			page.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
		PASS("R003: containerId " + cid + " recordCount " + recordCount);
	}

	/*
	 * test 4 - update field
	 */
	protected void S004() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		try
		{

			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			T_RawStoreRow row = new T_RawStoreRow(5);
			row.setColumn(0, (String) null);
			row.setColumn(1, REC_004);
			row.setColumn(2, (String) null);
			row.setColumn(3, REC_005);
			row.setColumn(4, REC_005);
		

			RecordHandle rh = t_util.t_insert(page, row);

			DataValueDescriptor col0 = new SQLChar((String)null);
			DataValueDescriptor col1 = new SQLChar(REC_001);
			DataValueDescriptor col2 = new SQLChar(REC_002);
			DataValueDescriptor col3 = new SQLChar((String)null);

			if (page.updateFieldAtSlot(page.FIRST_SLOT_NUMBER, 0, col0, null) == null ||
				page.updateFieldAtSlot(page.FIRST_SLOT_NUMBER, 1, col1, null) == null ||
				page.updateFieldAtSlot(page.FIRST_SLOT_NUMBER, 2, col2, null) == null ||
				page.updateFieldAtSlot(page.FIRST_SLOT_NUMBER, 3, col3, null) == null)
			{
				throw T_Fail.testFailMsg("Failed to update field");
			}

			page.unlatch();
			REPORT("setup S004: containerId " + cid);

			register(key(4,1), cid);
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/* recover test 4 */
	protected void R004() throws T_Fail,StandardException
	{
		long cid = find(key(4,1));
		if (cid < 0)
		{
			REPORT("R004 not run");
			return;
		}

		Transaction t = t_util.t_startTransaction();

		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// first row should contain (null, REC_001, REC_002, null, REC_005)
			DataValueDescriptor column = new SQLChar();
			t_util.t_checkFetchColFromSlot(page, 0, 0, column, false, null);
			t_util.t_checkFetchColFromSlot(page, 0, 1, column, false, REC_001);
			t_util.t_checkFetchColFromSlot(page, 0, 2, column, false, REC_002);
			t_util.t_checkFetchColFromSlot(page, 0, 3, column, false, null);
			t_util.t_checkFetchColFromSlot(page, 0, 4, column, false, REC_005);

			page.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R004: containerId " + cid );

	}

	/*
	 * test 5 - purge and delete
	 */
	protected void S005() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		try
		{

			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
			T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
			T_RawStoreRow row5 = new T_RawStoreRow(REC_005);

			long numPurged = 0;

			// row 0
			RecordHandle r1 = t_util.t_insertAtSlot(page, 0, row1);

			// purge slot 0
			page.purgeAtSlot(0, 1, true);
			numPurged++;

			// slot 0
			RecordHandle r2 = t_util.t_insertAtSlot(page, 0, row2);
			if (r2 != null) {
				page.deleteAtSlot(0, true, null);
			}

			// slot 1
			RecordHandle r3 = (r2 == null) ? r2 : t_util.t_insertAtSlot(page, 1, row3);
			if (r3 != null)
			{
				page.deleteAtSlot(1, true, null);
			}

			// slot 2
			RecordHandle r4 = (r3 == null) ? r3 : t_util.t_insertAtSlot(page, 2, row4);

			// slot 3
			RecordHandle r5 = (r4 == null) ? r4 : t_util.t_insertAtSlot(page, 3, row5);
			if (r5 != null)
			{
				// purge slot 1 and 2
				page.purgeAtSlot(1, 2, true);
				numPurged += 2;
			}

			REPORT("setup S005: containerId " + cid + " recordCount " +
				   page.recordCount() + " numPurges " + numPurged);

			register(key(5,1), cid);
			register(key(5,2), page.recordCount());
			register(key(5,3), numPurged);

			page.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/* recover test 5 */
	protected void R005() throws T_Fail, StandardException
	{
		long cid = find(key(5,1));
		if (cid < 0)
		{
			REPORT("R005 not run");
			return;
		}
		int recordCount = (int)find(key(5,2));
		int numPurged = (int)find(key(5,3));

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, recordCount, 1);

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
			T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
			T_RawStoreRow row5 = new T_RawStoreRow(REC_005);

			if (numPurged == 1)
			{
				// REC_002 (deleted), REC_003 (deleted), REC_004
				switch(recordCount)
				{
				case 3:
					t_util.t_checkFetchBySlot(page, 2, REC_004, false, false);

				case 2:
					t_util.t_checkFetchBySlot(page, 1, REC_003, true, false);

				case 1:
					t_util.t_checkFetchBySlot(page, 0, REC_002, true, false);
				}
			}
			else
			{
				// REC_002 (deleted), REC_005
				switch(recordCount)
				{
				case 2:
					t_util.t_checkFetchBySlot(page, 1, REC_005, false, false);

				case 1:
					t_util.t_checkFetchBySlot(page, 0, REC_002, true, false);
					if (!page.isDeletedAtSlot(0))
						throw T_Fail.testFailMsg("record should be deleted");
				}
			}

			page.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R005: containerId " + cid + " recordCount " +
			   recordCount + " numPurges " + numPurged);
	}

	/*
	 * test 6 - page allocation
	 */
	protected void S006() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_insertAtSlot(page, 0, row1);
			long page1_Id = page.getPageNumber();
			page.unlatch();

			page = t_util.t_addPage(c);
			t_util.t_insertAtSlot(page, 0, row2);
			long page2_Id = page.getPageNumber();
			page.unlatch();

			page = t_util.t_addPage(c);
			t_util.t_insertAtSlot(page, 0, row3);
			long page3_Id = page.getPageNumber();
			page.unlatch();

			if (page1_Id == page2_Id ||
				page1_Id == page3_Id ||
				page2_Id == page3_Id)
				throw T_Fail.testFailMsg("not getting new pages");

			REPORT("setup S006: containerId " + cid);

			register(key(6,1), cid);
			register(key(6,2), page1_Id);
			register(key(6,3), page2_Id);
			register(key(6,4), page3_Id);
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/* recover test 6 */
	protected void R006() throws T_Fail, StandardException
	{
		long cid = find(key(6,1));
		if (cid < 0)
		{
			REPORT("R006 not run");
			return;
		}

		long page1_Id = find(key(6,2));
		long page2_Id = find(key(6,3));
		long page3_Id = find(key(6,4));

		Transaction t = t_util.t_startTransaction();
		try
		{

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			if (page1_Id != c.FIRST_PAGE_NUMBER)
				throw T_Fail.testFailMsg("first page != container first page");

			Page page = t_util.t_getPage(c, page1_Id);
			t_util.t_checkRecordCount(page, 1, 1);
			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			page.unlatch();

			page = t_util.t_getPage(c, page2_Id);
			t_util.t_checkRecordCount(page, 1, 1);
			t_util.t_checkFetchBySlot(page, 0, REC_002, false, false);
			page.unlatch();

			page = t_util.t_getPage(c, page3_Id);
			t_util.t_checkRecordCount(page, 1, 1);
			t_util.t_checkFetchBySlot(page, 0, REC_003, false, false);
			page.unlatch();

			page = t_util.t_getLastPage(c);
			t_util.t_checkPageNumber(page, page3_Id);
			page.unlatch();

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R006: containerId " + cid );
	}

	/*
	 * test 7 - page deallocation
	 */
	protected void S007() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();
		try
		{

			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			Page page1 = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			long p1 = page1.getPageNumber();

			Page page2 = t_util.t_addPage(c);
			long p2 = page2.getPageNumber();

			Page page3 = t_util.t_addPage(c);
			long p3 = page3.getPageNumber();

			t_util.t_removePage(c, page2);
			t_util.t_removePage(c, page3);
			t_util.t_removePage(c, page1);

			if (page1.isLatched())
				throw T_Fail.testFailMsg("page is still latched after remove");

			if (page2.isLatched())
				throw T_Fail.testFailMsg("page is still latched after remove");

			if (page3.isLatched())
				throw T_Fail.testFailMsg("page is still latched after remove");

			register(key(7,0), cid);
			register(key(7,1), p1);
			register(key(7,2), p2);
			register(key(7,3), p3);

			REPORT("setup S007: containerId " + cid);
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/* recover test 7 */
	protected void R007() throws T_Fail, StandardException
	{
		long cid = find(key(7,0));
		if (cid < 0)
		{
			REPORT("R007 not run");
			return;
		}

		long p1 = find(key(7,1));
		long p2 = find(key(7,2));
		long p3 = find(key(7,3));

		Transaction t = t_util.t_startTransaction();
		try
		{

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);

			Page p = c.getPage(p1);
			if (p != null)
				throw T_Fail.testFailMsg("got a deallcated page " + p1);

			p = c.getPage(p2);
			if (p != null)
				throw T_Fail.testFailMsg("got a deallcated page " + p2);

			p = c.getPage(p3);
			if (p != null)
				throw T_Fail.testFailMsg("got a deallcated page " + p3);

			p = c.getPage(p3+1);
			if (p != null)
				throw T_Fail.testFailMsg("got a non-existant page " + p3+100);

			p = c.getFirstPage();
			if (p != null)
				throw T_Fail.testFailMsg("got a non-existant first page ");

			p = t_util.t_getLastPage(c);
			if (p != null)
				throw T_Fail.testFailMsg("got a non-existant last page ");

			PASS("R007: containerId " + cid);
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/*
	 * test 8 - page deallocation with rollback
	 */
	protected void S008() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();
		try
		{
			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			Page page1 = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			long p1 = page1.getPageNumber();

			Page page2 = t_util.t_addPage(c);
			long p2 = page2.getPageNumber();

			Page page3 = t_util.t_addPage(c);
			long p3 = page3.getPageNumber();

			Page page4 = t_util.t_addPage(c);
			long p4 = page4.getPageNumber();

			Page page5 = t_util.t_addPage(c);
			long p5 = page5.getPageNumber();

			t_util.t_removePage(c, page1);
			t_util.t_removePage(c, page3);
			t_util.t_removePage(c, page5);
			t_util.t_commit(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			page3 = t_util.t_getPage(c, p2);
			page1 = t_util.t_getPage(c, p4);

			// page 2 and page 4 are not removed
			t_util.t_removePage(c, page2); 
			t_util.t_removePage(c, page4);

			register(key(8,0), cid);
			register(key(8,1), p1);
			register(key(8,2), p2);
			register(key(8,3), p3);
			register(key(8,4), p4);
			register(key(8,5), p5);

			REPORT("setup S008: containerId " + cid);
		}
		finally
		{
			t_util.t_abort(t);
			t.close();
		}

	}

	/* recover test 8 */
	protected void R008() throws T_Fail, StandardException
	{
		long cid = find(key(8,0));
		if (cid < 0)
		{
			REPORT("R008 not run");
			return;
		}

		long p1 = find(key(8,1));
		long p2 = find(key(8,2));
		long p3 = find(key(8,3));
		long p4 = find(key(8,4));
		long p5 = find(key(8,5));

		Transaction t = t_util.t_startTransaction();
		try
		{

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);

		/* page 1, 3, 5 has been removed, page 2, 4 has not */

			Page p = c.getPage(p1);
			if (p != null)
				throw T_Fail.testFailMsg("got a deallcated page " + p1);

			p = t_util.t_getPage(c,p2);
			p.unlatch();

			p = c.getPage(p3);
			if (p != null)
				throw T_Fail.testFailMsg("got a deallcated page " + p3);

			p = t_util.t_getPage(c,p4);
			p.unlatch();

			p = c.getPage(p5);
			if (p != null)
				throw T_Fail.testFailMsg("got a deallcated page " + p5);

			p = c.getPage(p5+1);
			if (p != null)
				throw T_Fail.testFailMsg("got a non-existant page " + p5+1);

		// make sure get first page skips over p1
			p = c.getFirstPage();
			if (p == null || p.getPageNumber() != p2)
				throw T_Fail.testFailMsg("get first page failed");
			p.unlatch();

			// make sure get next page skips over p3
			p = c.getNextPage(p2);	
			if (p == null || p.getPageNumber() != p4)
				throw T_Fail.testFailMsg("get next page failed");
			p.unlatch();

			// make sure get next page skips over p5
			p = c.getNextPage(p4);
			if (p != null)
			{
				p.unlatch();
				throw T_Fail.testFailMsg("get next page failed to terminate");
			}

			p = t_util.t_getLastPage(c);	// make sure it skips over p5
			if (p == null || p.getPageNumber() != p4)
				throw T_Fail.testFailMsg("getLastPage failed");
			p.unlatch();

			PASS("R008: containerId " + cid);

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

	}

	/*
	 * test 9 - deallocation and reuse pag
	 */
	protected void S009() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();
		try
		{
			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);


			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			int numpages = 10;

			Page[] origpage = new Page[numpages];
			int[] origrid = new int[numpages];
			long[] origpnum = new long[numpages];
			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);

			for (int i = 0; i < numpages; i++)
			{
				if (i == 0)
					origpage[i] = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
				else
					origpage[i] = t_util.t_addPage(c);

				origrid[i] = t_util.t_insert(origpage[i], row1).getId();
				origpnum[i] = origpage[i].getPageNumber();

				t_util.t_removePage(c, origpage[i]);
			}
			t_util.t_commit(t);

			// check that pages are not reused before transaction is committed
			for (int i = 0; i < numpages-1; i++)
			{
				for (int j = i+1; j < numpages; j++)
				{
					if (origpnum[i] == origpnum[j])
						throw T_Fail.testFailMsg("page reused before transaction is committed");
				}
			}

			register(key(9,0), cid);
			register(key(9,1), numpages);

			for (int i = 0; i < numpages; i++)
			{
				register(key(9,i+10), origpnum[i]);
				register(key(9,i+numpages+10), origrid[i]);
			}

			// now see if we can reuse them
			c = t_util.t_openContainer(t, 0, cid, true);

			Page[] newpage = new Page[numpages];
			int[] newrid = new int[numpages];
			long[] newpnum = new long[numpages];
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);

			for (int i = 0; i < numpages; i++)
			{
				newpage[i] = t_util.t_addPage(c);
				newpnum[i] = newpage[i].getPageNumber();
				newrid[i] = t_util.t_insert(newpage[i], row2).getId();
			}

			// if any page is reused, make sure the rid is not reused
			int reuse = 0;
			for (int i = 0; i < numpages; i++)
			{
				for (int j = 0; j < numpages; j++)
				{
					if (origpnum[i] == newpnum[j])
					{
						reuse++;
						if (origrid[i] == newrid[j])
							throw T_Fail.testFailMsg("resued page rid is not preserved");
					
						break;		// inner loop
					}
				}
			}

			for (int i = 0; i < numpages; i++)
			{
				register(key(9,i+100), newpnum[i]);
				register(key(9,i+numpages+100), newrid[i]);
			}

			REPORT("setup S009: containerId " + cid + " of " +
				   numpages + " original pages," +
				   reuse + " pages were reused.");
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/* recover test 9 */
	protected void R009() throws T_Fail, StandardException
	{
		long cid = find(key(9,0));
		if (cid < 0)
		{
			REPORT("R009 not run");
			return;
		}
		int numpages = (int)find(key(9,1));

		int[] newrid = new int[numpages];
		long[] newpnum = new long[numpages];
		Page[] newpage = new Page[numpages];

		Transaction t = t_util.t_startTransaction();
		try
		{

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);

			for (int i = 0; i < numpages; i++)
			{
				newrid[i] = (int)find(key(9, i+numpages+100));
				newpnum[i] = find(key(9,i+100));

				newpage[i] = t_util.t_getPage(c, newpnum[i]);
				t_util.t_checkRecordCount(newpage[i], 1, 1);
				RecordHandle rh = t_util.t_checkFetchFirst(newpage[i], REC_002);
				if (rh.getId() != newrid[i])
					throw T_Fail.testFailMsg("recordId not match");
			}
			REPORT("R009: containerId " + cid);
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/*
	 * test 10 - allocation/deallocation with overflow page
	 */
	protected void S010() throws T_Fail,StandardException
	{
		// maufacture a container with the first and last page being overflow
		// pages 
		Transaction t = t_util.t_startTransaction();
		try
		{
			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			int numpages = 10;
			Page[] page = new Page[numpages];
			long[] pnum = new long[numpages];
			RecordHandle[] recordHandles = new RecordHandle[numpages];

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			for (int i = 0; i < numpages; i++)
			{
				if (i == 0)
					page[i] = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
				else
					page[i] = t_util.t_addPage(c);

				pnum[i] = page[i].getPageNumber();

				// remove first two and last page as we go, pages are not reused
				// until after commit. These pages have no rows in them
				if (i < 2 || i == numpages-1)
				{
					t_util.t_checkEmptyPage(page[i]);
					t_util.t_removePage(c, page[i]); 
				}
				else
					recordHandles[i] = t_util.t_insert(page[i], row1);

			}

			t_util.t_commit(t);
			c = t_util.t_openContainer(t, 0, cid, true);

			Page p = c.getFirstPage();
			if (p.getPageNumber() != pnum[2])
				throw T_Fail.testFailMsg("first page expected to be page " +
										 pnum[2] + ", got " + p.getPageNumber() + 
										 " instead");
			p.unlatch();
			p = t_util.t_getLastPage(c);
			if (p.getPageNumber() != pnum[numpages-2])
				throw T_Fail.testFailMsg("last page expected to be page " +
										 pnum[numpages-2] + ", got " + p.getPageNumber() + 
										 " instead");
			p.unlatch();

		// now make rows on the rest of the page overflow
			RecordHandle rh;
			T_RawStoreRow big = new T_RawStoreRow(String.valueOf(new char[1500]));
			REPORT("start reusing pages hopefully");
			for (int i = 2; i < numpages-1; i++)
			{
				T_RawStoreRow row2 = new T_RawStoreRow(REC_002);

				p = t_util.t_getPage(c, pnum[i]);
				while(p.spaceForInsert(row2.getRow(), (FormatableBitSet) null, 100))
					t_util.t_insert(p, row2);

				// now page is filled
                rh = p.fetchFromSlot(
                        (RecordHandle) null, 
                        0, 
                        row2.getRow(), 
                        (FetchDescriptor) null, 
                        true);
				int slot = p.getSlotNumber(rh);
				p.updateAtSlot(slot, big.getRow(), null);
				p.unlatch();
			}

			register(key(10, 1), cid);
			register(key(10, 2), numpages);
			for (int i = 0; i < numpages; i++)
				register(key(10, 10+i), pnum[i]);
			REPORT("setup S010");
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	protected void R010() throws T_Fail, StandardException
	{
		long cid = find(key(10, 1));
		if (cid < 0)
		{
			REPORT("R010 not run");
			return;
		}
		int numpages = (int)find(key(10,2));
		long[] pnum = new long[numpages];
		for (int i = 0; i < numpages; i++)
			pnum[i] = find(key(10, 10+i));

		// now check the pages, 0, 1 and last page (...) are all overflowpages
		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page p;

			p = c.getPage(pnum[0]);
			if (p.recordCount() == 0) // it has no overflow rows in it
			{
				p.unlatch();
				throw T_Fail.testFailMsg("first page failed to get any overflow records");
			}
			p.unlatch();
			p = c.getPage(pnum[1]);
			if (p.recordCount() == 0) // it has no overflow rows in it
			{
				p.unlatch();
				throw T_Fail.testFailMsg("second page failed to get any overflow records");
			}
			p.unlatch();
			p = c.getPage(pnum[numpages-1]);
			if (p.recordCount() == 0) // it has no overflow rows in it
			{
				p.unlatch();
				throw T_Fail.testFailMsg("last page failed to get any overflow records");
			}
			p.unlatch();

		// all other pages have one huge row at the beginning
			p = c.getFirstPage();
			if (p.getPageNumber() != pnum[2])
				throw T_Fail.testFailMsg("first page expected to be page " +
										 pnum[2] + ", got " + p.getPageNumber() + 
										 " instead");
			long pageNum = p.getPageNumber();
			t_util.t_checkStringLengthFetch(p, 0, 1500);

			p.unlatch();
			int i = 3;
			while((p = c.getNextPage(pageNum)) != null)
			{
				pageNum = p.getPageNumber();
				if (pageNum != pnum[i])
					throw T_Fail.testFailMsg("expect page " + pnum[i] + 
											 " get page " + pageNum);
				t_util.t_checkStringLengthFetch(p, 0, 1500);			
				p.unlatch();
				i++;
			}
			if (i != numpages-1)
				throw T_Fail.testFailMsg("expect last head page to be " +
										 (numpages-2) +  " got " + i + " page instead"); 
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R010");
	}

	/*
	 * test 11 - allocate a lot of pages so that we need > 1 allocation pages
	 */
	protected void S011() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();
		int iterations = 10000;

		try
		{
			long cid = t_util.t_addContainer(t, 0, 4096);
			t_util.t_commit(t);
			
			T_RawStoreRow row = new T_RawStoreRow(REC_001);
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			// allocate iterations pages, this ought to bring the number of pages
			// over what 1 allocation page can handle
			Page p = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_insert(p, row);
			p.unlatch();

			long pnum = ContainerHandle.FIRST_PAGE_NUMBER;
			long lastPageNum = ContainerHandle.INVALID_PAGE_NUMBER;
			for (int i = 1; i <= iterations; i++)
			{
				p = t_util.t_addPage(c);
				if (p.getPageNumber() != pnum+1)
					REPORT("S011: skipping " + (pnum+1) + " going to " + p.getPageNumber());
				pnum = p.getPageNumber();

				t_util.t_insert(p, row);

				if (i == iterations)
				{
					lastPageNum = p.getPageNumber();
					REPORT("S011: Last page number is " + lastPageNum);
				}

				p.unlatch();
			}
			t_util.t_commit(t);

			// now scan the pages
			c = t_util.t_openContainer(t, 0, cid, true);
			p = c.getFirstPage();
			if (p == null || p.getPageNumber() !=
				ContainerHandle.FIRST_PAGE_NUMBER)
				throw T_Fail.testFailMsg("first page not where it is expected");
			p.unlatch();

			p = t_util.t_getLastPage(c);
			if (p == null || p.getPageNumber() != lastPageNum)
				throw T_Fail.testFailMsg("last page not where it is expected");
			p.unlatch();

			register(key(11, 1), cid);
			register(key(11, 2), lastPageNum);
			register(key(11, 3), iterations);

			REPORT("setup S011, container id = " + cid);
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	protected void R011() throws T_Fail, StandardException
	{
		long cid = find(key(11,1));
		if (cid < 0)
		{
			REPORT("R011 not run");
			return;
		}
		else
			REPORT("R011 container id = " + cid);

		long expectedLastPageNum = find(key(11,2));
		int iterations = (int)find(key(11,3));
			 
		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			Page p = c.getFirstPage();
			if (p == null || p.getPageNumber() !=
				ContainerHandle.FIRST_PAGE_NUMBER)
				throw T_Fail.testFailMsg("first page not where it is expected");
			p.unlatch();


			long pageNum = ContainerHandle.FIRST_PAGE_NUMBER;
			long pnum = pageNum;
			int pcount = 1;
			while((p = c.getNextPage(pageNum)) != null)
			{
				t_util.t_checkFetchFirst(p, REC_001);
				pageNum = p.getPageNumber();
				if (pageNum != pnum+1)
					REPORT("R011: skipping " + (pnum+1) + " going to " + pageNum);
				pnum = pageNum;

				pcount++;
				p.unlatch();
			}
			if (pcount != (iterations+1))
            {
				throw T_Fail.testFailMsg(
                    "expect to see " + (iterations+1) + " pages, got: " + pcount + 
					" last page number is " + pageNum);
            }

			p = t_util.t_getLastPage(c);
			if (p.getPageNumber() != expectedLastPageNum)
			{
				throw T_Fail.testFailMsg(
					"expect last page num to be " + expectedLastPageNum +
					" , instead got " + p.getPageNumber());
			}

			REPORT("Last page pagenumber is " + p.getPageNumber() + 
				   ", it is the last page of " + (iterations+1) + " user pages");
			p.unlatch();
			

			PASS("R011");
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

	}

	/*
	 * test 12 - test estimated page count
	 */
	protected void S012() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		long cid = t_util.t_addContainer(t, 0, 4096);
		t_util.t_commit(t);

		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			if (c.getEstimatedPageCount(0) != 1)
				throw T_Fail.testFailMsg("Expect 2 user page, got " + c.getEstimatedPageCount(0));

			// allocate 30 pages
			Page p = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			T_RawStoreRow row = new T_RawStoreRow(REC_001);

			t_util.t_insert(p, row);
			p.unlatch();
			for (int i = 2; i <= 30; i++)
			{
				p = t_util.t_addPage(c);
				t_util.t_insert(p, row);
				p.unlatch();
			}

			register(key(12, 1), cid);
			REPORT("Setup S012");
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	protected void R012() throws T_Fail, StandardException
	{
		long cid = find(key(12,1));
		if (cid < 0)
		{
			REPORT("R012 not run");
			return;
		}

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			if (c.getEstimatedPageCount(0) != 30)
				throw T_Fail.testFailMsg("expect 30 pages, got " + c.getEstimatedPageCount(0));

			PASS("R012");
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

	}

	/*
	 * test 20 - create multiple containers
	 */
	protected void S020() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();
		T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
		T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
		T_RawStoreRow row3 = new T_RawStoreRow(REC_003);

		try
		{

			long cid1 = t_util.t_addContainer(t, 0);
			ContainerHandle c1 = t_util.t_openContainer(t, 0, cid1, true);
			Page page = t_util.t_getPage(c1, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_insertAtSlot(page, 0, row1);
			page.unlatch();
		
			long cid2 = t_util.t_addContainer(t, 0);
			ContainerHandle c2 = t_util.t_openContainer(t, 0, cid2, true);

			long cid3 = t_util.t_addContainer(t, 0);
			ContainerHandle c3 = t_util.t_openContainer(t, 0, cid3, true);

			page = t_util.t_getPage(c2, ContainerHandle.FIRST_PAGE_NUMBER);
			// blank first page
			page.unlatch();

			page = t_util.t_addPage(c2);
			t_util.t_insertAtSlot(page, 0, row2);
			long pageId = page.getPageNumber();
			page.unlatch();

			page = t_util.t_getPage(c3, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_insertAtSlot(page, 0, row3);
			page.unlatch();

			REPORT("setup S020: container1 " + cid1 + 
				   " container2 " + cid2 + " container3 " + cid3 +
				   " page " + pageId);

			register(key(20, 1), cid1);
			register(key(20, 2), cid2);
			register(key(20, 3), cid3);
			register(key(20, 4), pageId);
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/* recover test 20 */
	protected void R020() throws T_Fail, StandardException
	{
		long cid1 = find(key(20, 1));
		if (cid1 < 0)
		{
			REPORT("R020 not run");
			return;
		}

		long cid2 = find(key(20,2));
		long cid3 = find(key(20,3));
		long pageId = find(key(20,4));

		Transaction t = t_util.t_startTransaction();
		try
		{

			ContainerHandle c = t_util.t_openContainer(t, 0, cid1, false);
			Page page = t_util.t_getPage(c, c.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 1, 1);
			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			page.unlatch();

			c = t_util.t_openContainer(t, 0, cid2, false);
			page = t_util.t_getPage(c,  c.FIRST_PAGE_NUMBER);
			t_util.t_checkEmptyPage(page);
			page.unlatch();			

			page = t_util.t_getPage(c, pageId);
			t_util.t_checkRecordCount(page, 1, 1);
			t_util.t_checkFetchBySlot(page, 0, REC_002, false, false);
			page.unlatch();

			c = t_util.t_openContainer(t, 0, cid3, false);
			page = t_util.t_getPage(c, c.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 1, 1);
			t_util.t_checkFetchBySlot(page, 0, REC_003, false, false);
			page.unlatch();

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R020 container1 " + cid1 + 
			   " container2 " + cid2 + " container3 " + cid3 +
			   " page " + pageId);
	}

	/*
	 * test 022 - drop containers
	 */
	protected void S022() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();
		try
		{

			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			t_util.t_openContainer(t, 0, cid, true);
			t_util.t_dropContainer(t, 0, cid);

			t_util.t_abort(t);			// this should rollback the drop
			t_util.t_openContainer(t, 0, cid, true);

			REPORT("rollback of drop container tested");

			t.dropContainer(new ContainerKey(0, cid));

			t.commit();

			REPORT("setup S022: containerId " + cid);

			register(key(22, 1), cid);
				 
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/* 
	 * recover test 022 - drop container
	 */
	protected void R022() throws T_Fail, StandardException
	{
		long cid = find(key(22, 1));
		if (cid < 0)
		{
			REPORT("R022 not run");
			return;
		}

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerKey id = new ContainerKey(0, cid);
			ContainerHandle c1 = t.openContainer(id, ContainerHandle.MODE_READONLY);	// this should fail
			if (c1 != null)
				throw T_Fail.testFailMsg("dropped container should fail to open");				
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
		PASS("R022 : containerId " + cid);
	}

	/*
	 * test 100 - multiple intervening committed transactions
	 */
	protected void S100() throws T_Fail, StandardException
	{
		T_TWC t1 = t_util.t_startTransactionWithContext();
		T_TWC t2 = t_util.t_startTransactionWithContext();
		try
		{

			long cid10 = t_util.t_addContainer(t1,0);
			long cid11 = t_util.t_addContainer(t1,0);
			t_util.t_commit(t1);

			long cid20 = t_util.t_addContainer(t2, 0);
			long cid21 = t_util.t_addContainer(t2, 0);
			t_util.t_commit(t2);

			ContainerHandle c10 = t_util.t_openContainer(t1, 0, cid10, true);
			ContainerHandle c11 = t_util.t_openContainer(t1, 0, cid11, true);
			ContainerHandle c20 = t_util.t_openContainer(t2, 0, cid20, true);
			ContainerHandle c21 = t_util.t_openContainer(t2, 0, cid21, true);

			t1.switchTransactionContext();
			Page p10 = t_util.t_getPage(c10, ContainerHandle.FIRST_PAGE_NUMBER);
			Page p11 = t_util.t_getPage(c11, ContainerHandle.FIRST_PAGE_NUMBER);
			t1.resetContext();

			t2.switchTransactionContext();
			Page p20 = t_util.t_getPage(c20, ContainerHandle.FIRST_PAGE_NUMBER);
			Page p21 = t_util.t_getPage(c21, ContainerHandle.FIRST_PAGE_NUMBER);

			// for each page, insert, update, updatefield, (some) delete

			T_RawStoreRow row1 = new T_RawStoreRow(3);
			row1.setColumn(0, REC_001);
			row1.setColumn(1, REC_002);
			row1.setColumn(2, (String) null);

			T_RawStoreRow row2 = new T_RawStoreRow(2);
			row2.setColumn(0, REC_003);
			row2.setColumn(1, REC_004);


			T_RawStoreRow rowP = new T_RawStoreRow(1);
			rowP.setColumn(0, REC_005);
			t2.resetContext();


			t1.switchTransactionContext();
			RecordHandle r10 = t_util.t_insertAtSlot(p10, 0, row1);
			RecordHandle r11 = t_util.t_insertAtSlot(p11, 0, row1);
			t1.resetContext();

			t2.switchTransactionContext();
			RecordHandle r20 = t_util.t_insertAtSlot(p20, 0, row1);
			RecordHandle r21 = t_util.t_insertAtSlot(p21, 0, row1);
			t2.resetContext();

			t1.switchTransactionContext();
			int slot10 = p10.getSlotNumber(r10);
			p10.updateAtSlot(slot10, row2.getRow(), null);
			int slot11 = p11.getSlotNumber(r11);
			p11.updateAtSlot(slot11, row2.getRow(), null);
			t1.resetContext();

			t2.switchTransactionContext();
			int slot20 = p20.getSlotNumber(r20);
			p20.updateAtSlot(slot20, row2.getRow(), null);
			int slot21 = p21.getSlotNumber(r21);
			p21.updateAtSlot(slot21, row2.getRow(), null);
			t2.resetContext();
		
			t1.switchTransactionContext();
			slot10 = p10.getSlotNumber(r10);
			p10.updateAtSlot(slot10, rowP.getRow(), BS_COL_0);
			slot11 = p11.getSlotNumber(r11);
			p11.updateAtSlot(slot11, rowP.getRow(), BS_COL_0);
			p10.unlatch();
			p11.unlatch();
			t1.resetContext();



			t2.switchTransactionContext();
			slot20 = p20.getSlotNumber(r20);
			p20.updateAtSlot(slot20, rowP.getRow(), BS_COL_0);
			slot21 = p21.getSlotNumber(r21);
			p21.updateAtSlot(slot21, rowP.getRow(), BS_COL_0);

			p21.deleteAtSlot(slot21, true, null);
			p20.unlatch();
			p21.unlatch();
			t2.resetContext();

			REPORT("setup S100: container1 " + cid10 + " container2 " + cid11 + 
				   " container3 " + cid20 + " container4 " + cid21);

			register(key(100, 1), cid10);
			register(key(100, 2), cid11);
			register(key(100, 3), cid20);
			register(key(100, 4), cid21);
		}
		finally
		{
			t_util.t_commit(t1);
			t_util.t_close(t1);

			t_util.t_commit(t2);
			t_util.t_close(t2);
		}
	}

	/* recover S100 */
	protected void R100 () throws T_Fail, StandardException
	{
		long[] cid = new long[4];
		cid[0] = find(key(100, 1));
		if (cid[0] < 0)
		{
			REPORT("R100 not run");
			return;
		}
			
		cid[1] = find(key(100, 2));
		cid[2] = find(key(100, 3));
		cid[3] = find(key(100, 4));

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c;
			Page page;

			for (int i = 0; i < 4; i++)
			{
				c = t_util.t_openContainer(t, 0, cid[i], false);
				page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
				if (i == 3)
					t_util.t_checkRecordCount(page, 1, 0);
				else
					t_util.t_checkRecordCount(page, 1, 1);

				t_util.t_checkFieldCount(page, 0, 2);

				// each row has REC_005, REC_004
				DataValueDescriptor column = new SQLChar();
				t_util.t_checkFetchColFromSlot(page, 0, 0, column, false, REC_005);
				t_util.t_checkFetchColFromSlot(page, 0, 1, column, false, REC_004);
				page.unlatch();
			}

			PASS("R100 passed: container1 " + cid[0] + " container2 " + cid[1] + 
				 " container3 " + cid[2] + " container4 " + cid[3]);
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/*
	 * test 101 - transaction with rollback to savepoint
	 */
	protected void S101() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();
		try
		{

			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
			T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
			T_RawStoreRow row5 = new T_RawStoreRow(REC_005);
		
			RecordHandle r0 = t_util.t_insertAtSlot(page, 0, row1);
			if (t_util.t_insertAtSlot(page, 1, row2) == null)
				return;			// test case not interesting

			t_util.t_checkRecordCount(page, 2, 2);
			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_002, false, false);
			/////////////////////////////////////////////////////
			// At SP1, has 2 records of REC_001 and REC_002    //
			/////////////////////////////////////////////////////
			t.setSavePoint(SP1, null);

			if (t_util.t_insertAtSlot(page, 2, row3) == null)
				return;			// test case not interesting

			page.purgeAtSlot(1, 1, true);

			if (t_util.t_insertAtSlot(page, 1, row4) == null)
				return;

			t_util.t_checkRecordCount(page, 3, 3);
			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_004, false, false);
			t_util.t_checkFetchBySlot(page, 2, REC_003, false, false);
			////////////////////////////////////////////////////////////////
			// At SP2, has 3 records of REC_001 and REC_004 and REC_003   //
			////////////////////////////////////////////////////////////////
			t.setSavePoint(SP2, null);

			int slot0 = page.getSlotNumber(r0);
			page.updateAtSlot(slot0, row5.getRow(), null);
			page.deleteAtSlot(1, true, (LogicalUndo)null);

			t_util.t_checkRecordCount(page, 3, 2);
			t_util.t_checkFetchBySlot(page, 0, REC_005, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_004, true, false);
			t_util.t_checkFetchBySlot(page, 2, REC_003, false, false);

			page.unlatch();
			t.rollbackToSavePoint(SP2, null);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, 3, 3);
			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_004, false, false);
			t_util.t_checkFetchBySlot(page, 2, REC_003, false, false);

			// after a rollback to sp, do some more changes

			slot0 = page.getSlotNumber(r0);
			page.updateAtSlot(slot0, row5.getRow(), null);
			page.deleteAtSlot(0, true, (LogicalUndo)null);
			page.deleteAtSlot(1, true, (LogicalUndo)null);
			page.deleteAtSlot(2, true, (LogicalUndo)null);

			t_util.t_checkRecordCount(page, 3, 0);
			t_util.t_checkFetchBySlot(page, 0, REC_005, true, false);
			t_util.t_checkFetchBySlot(page, 1, REC_004, true, false);
			t_util.t_checkFetchBySlot(page, 2, REC_003, true, false);

			page.unlatch();
			t.rollbackToSavePoint(SP1, null);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// note that an insert, when rolled back, becomes a deleted row but
			// will not disappear.  A purge row will come back at the same slot
			// and with the same record id.
			t_util.t_checkRecordCount(page, 4, 2);

			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_002, false, false);
			t_util.t_checkFetchBySlot(page, 2, REC_004, true, false);
			t_util.t_checkFetchBySlot(page, 3, REC_003, true, false);

			// add one more record to this
			if (page.spaceForInsert())
				t_util.t_insertAtSlot(page, 3, row5);

			REPORT("setup S101: containerId " + cid + " recordCount " + page.recordCount());

			register(key(101, 1), cid);
			register(key(101, 2), page.recordCount());

			page.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/* recover test 101 */
	protected void R101() throws T_Fail, StandardException
	{
		long cid = find(key(101, 1));
		if (cid < 0)
		{
			REPORT("R101 not run");
			return;
		}
		int recordCount = (int)find(key(101, 2));

		Transaction t = t_util.t_startTransaction();
		try
		{

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, recordCount, recordCount-2);
			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_002, false, false);
			t_util.t_checkFetchBySlot(page, 2, REC_004, true, false);
			if (recordCount == 5)
			{
				t_util.t_checkFetchBySlot(page, 3, REC_005, false, false);
				t_util.t_checkFetchBySlot(page, 4, REC_003, true, false);
			}
			else
				t_util.t_checkFetchBySlot(page, 3, REC_003, true, false);

			page.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R101: containerId " + cid + " recordCount " + recordCount);
	}


	/*
	 * the following tests has recovery undo work, cannot test Rnnn during
	 * setup because it hasn't been rolled back during setup yet.  Test the
	 * state in Snnn.
	 */

	/*
	 * test 200 - incomplete transaction
	 */
	protected void S200() throws T_Fail, StandardException
	{
		T_TWC ctx = t_util.t_startTransactionWithContext();
		Transaction t = ctx.tran;
		Page page = null;

		try
		{
			long cid = t_util.t_addContainer(t, 0);

			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			ctx.switchTransactionContext();

			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);

			int rowcount = 0;
			while(page.spaceForInsert())
			{
				if (t_util.t_insertAtSlot(page, 0, row1) != null)
					rowcount++;
			}

			t_util.t_checkRecordCount(page, rowcount, rowcount);
			for (int i = 0; i < rowcount; i++)
				t_util.t_checkFetchBySlot(page, i, REC_001, false, false);

			REPORT("setup S200: containerId " + cid + " recordCount " + rowcount);
			register(key(200, 1), cid);
			register(key(200, 2), rowcount);
		}
		finally
		{
			if (page != null && page.isLatched())
				page.unlatch();
			ctx.resetContext();
		}
		// do not abort it at run time, abort it at recovery time
		// t_util.t_abort(t);
		// t.close();
	}

	/* recover test 200 */
	protected void R200() throws T_Fail, StandardException
	{
		long cid = find(key(200, 1));
		if (cid < 0)
		{
			REPORT("R200 not run");
			return;
		}

		int recordCount = (int)find(key(200, 2));

		Transaction t = t_util.t_startTransaction();
		try
		{

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// rollback of an insert is a deleted record
			t_util.t_checkRecordCount(page, recordCount, 0);
			for (int i = 0; i < recordCount; i++)
				t_util.t_checkFetchBySlot(page, i, REC_001, true, false);
			page.unlatch();

			PASS("R200: containerId " + cid + " recordCount " + recordCount);

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/*
	 * test 201 - multiple intervening incomplete transaction
	 */
	protected void S201() throws T_Fail, StandardException
	{
		/* this is the same as S100 but left it at an incomplete state */
		T_TWC t1 = t_util.t_startTransactionWithContext();
		T_TWC t2 = t_util.t_startTransactionWithContext();
		Page p10, p11, p20, p21;
		p10 = p11 = p20 = p21 = null;

		try
		{
			long cid10 = t_util.t_addContainer(t1,0);
			long cid11 = t_util.t_addContainer(t1,0);

			long cid20 = t_util.t_addContainer(t2, 0);
			long cid21 = t_util.t_addContainer(t2, 0);

			t_util.t_commit(t1);
			t_util.t_commit(t2);

			ContainerHandle c10 = t_util.t_openContainer(t1, 0, cid10, true);
			ContainerHandle c11 = t_util.t_openContainer(t1, 0, cid11, true);
			ContainerHandle c20 = t_util.t_openContainer(t2, 0, cid20, true);
			ContainerHandle c21 = t_util.t_openContainer(t2, 0, cid21, true);

			t1.switchTransactionContext();
			p10 = t_util.t_getPage(c10, ContainerHandle.FIRST_PAGE_NUMBER);
			p11 = t_util.t_getPage(c11, ContainerHandle.FIRST_PAGE_NUMBER);
			t1.resetContext();

			t2.switchTransactionContext();
			p20 = t_util.t_getPage(c20, ContainerHandle.FIRST_PAGE_NUMBER);
			p21 = t_util.t_getPage(c21, ContainerHandle.FIRST_PAGE_NUMBER);

			// for each page, insert, update, updatefield, (some) delete

			T_RawStoreRow row1 = new T_RawStoreRow(3);
			row1.setColumn(0, REC_001);
			row1.setColumn(1, REC_002);
			row1.setColumn(2, (String) null);

			T_RawStoreRow row2 = new T_RawStoreRow(2);
			row2.setColumn(0, REC_003);
			row2.setColumn(1, REC_004);


			T_RawStoreRow rowP = new T_RawStoreRow(1);
			rowP.setColumn(0, REC_005);
			t2.resetContext();

			t1.switchTransactionContext();
			RecordHandle r10 = t_util.t_insertAtSlot(p10, 0, row1);
			RecordHandle r11 = t_util.t_insertAtSlot(p11, 0, row1);
			t1.resetContext();

			t2.switchTransactionContext();
			RecordHandle r20 = t_util.t_insertAtSlot(p20, 0, row1);
			RecordHandle r21 = t_util.t_insertAtSlot(p21, 0, row1);
			t2.resetContext();

			t1.switchTransactionContext();
			int slot10 = p10.getSlotNumber(r10);
			p10.updateAtSlot(slot10, row2.getRow(), null);
			int slot11 = p11.getSlotNumber(r11);
			p11.updateAtSlot(slot11, row2.getRow(), null);
			t1.resetContext();

			t2.switchTransactionContext();
			int slot20 = p20.getSlotNumber(r20);
			p20.updateAtSlot(slot20, row2.getRow(), null);
			int slot21 = p21.getSlotNumber(r21);
			p21.updateAtSlot(slot21, row2.getRow(), null);
			t2.resetContext();


			t1.switchTransactionContext();
			slot10 = p10.getSlotNumber(r10);
			p10.updateAtSlot(slot10, rowP.getRow(), BS_COL_0);
			slot11 = p11.getSlotNumber(r11);
			p11.updateAtSlot(slot11, rowP.getRow(), BS_COL_0);
			t1.resetContext();

			t2.switchTransactionContext();
			slot20 = p20.getSlotNumber(r20);
			p20.updateAtSlot(slot20, rowP.getRow(), BS_COL_0);
			slot21 = p21.getSlotNumber(r21);
			p21.updateAtSlot(slot21, rowP.getRow(), BS_COL_0);

			p21.deleteAtSlot(slot21, true, null);
			t2.resetContext();

			REPORT("setup S201: container1 " + cid10 + " container2 " + cid11 + 
				   " container3 " + cid20 + " container4 " + cid21);

			register(key(201, 1), cid10);
			register(key(201, 2), cid11);
			register(key(201, 3), cid20);
			register(key(201, 4), cid21);
		}
		finally
		{
			if (p10 != null && p10.isLatched())
				p10.unlatch();


			if (p11 != null && p11.isLatched())
				p11.unlatch();


			if (p20 != null && p20.isLatched())
				p20.unlatch();

			if (p21 != null && p21.isLatched())
				p21.unlatch();
		}

		// let recovery do the abort
	}

	/* recover test 201 */
	protected void R201() throws T_Fail, StandardException
	{
		long[] cid = new long[4];
		cid[0] = find(key(201, 1));
		if (cid[0] < 0)
		{
			REPORT("R201 not run");
			return;
		}
			
		cid[1] = find(key(201, 2));
		cid[2] = find(key(201, 3));
		cid[3] = find(key(201, 4));

		Transaction t = t_util.t_startTransaction();
		try
		{

			ContainerHandle c;
			Page page;

			for (int i = 0; i < 4; i++)
			{
				c = t_util.t_openContainer(t, 0, cid[i], false);
				page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
				t_util.t_checkRecordCount(page, 1, 0);

				// record has the following fields: REC_001, REC_002, null
				DataValueDescriptor column = new SQLChar();
				t_util.t_checkFetchColFromSlot(page, 0, 0, column, false, REC_001);
				t_util.t_checkFetchColFromSlot(page, 0, 1, column, false, REC_002);
				t_util.t_checkFetchColFromSlot(page, 0, 2, column, false, null);
			}
		}
		finally
		{
			t_util.t_commit(t);	
			t.close();
		}
		PASS("R201 passed:  container1 " + cid[0] + " container2 " + cid[1] + 
			   " container3 " + cid[2] + " container4 " + cid[3]);
	}

	/*
	 * test 202 - incomplete transaction with rollback to savepoints
	 */
	protected void S202() throws T_Fail, StandardException
	{
		/* this is S101 which is left in an incomplete state */
		T_TWC ctx = t_util.t_startTransactionWithContext();
		Transaction t = ctx.tran;
		Page page = null;
		ctx.switchTransactionContext();

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
			T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
			T_RawStoreRow row5 = new T_RawStoreRow(REC_005);

			RecordHandle r0 = t_util.t_insertAtSlot(page, 0, row1);
			if (t_util.t_insertAtSlot(page, 1, row2) == null)
			{
				page.unlatch();
				t_util.t_abort(t);
				t.close();
				return;			// test case not interesting
			}


			/////////////////////////////////////////////////////
			// At SP1, has 2 records of REC_001 and REC_002    //
			/////////////////////////////////////////////////////
			t.setSavePoint(SP1, null);

			int slot0 = page.getSlotNumber(r0);
			page.updateAtSlot(slot0, row5.getRow(), null);
			t_util.t_checkFetchBySlot(page, 0, REC_005, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_002, false, false);

			page.unlatch();
			t.rollbackToSavePoint(SP1, null);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_002, false, false);

			if (t_util.t_insertAtSlot(page, 2, row3) == null)
			{
				page.unlatch();
				t_util.t_abort(t);
				t.close();
				return;			// test case not interesting
			}

			page.purgeAtSlot(1, 1, true);

			if (t_util.t_insertAtSlot(page, 1, row4) == null)
			{
				page.unlatch();
				t_util.t_abort(t);
				t.close();
				return;			// test case not interesting
			}

			t_util.t_checkRecordCount(page, 3, 3);
			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_004, false, false);
			t_util.t_checkFetchBySlot(page, 2, REC_003, false, false);
			////////////////////////////////////////////////////////////////
			// At SP2, has 3 records of REC_001 and REC_004 and REC_003   //
			////////////////////////////////////////////////////////////////
			t.setSavePoint(SP2, null);

			slot0 = page.getSlotNumber(r0);
			page.updateAtSlot(slot0, row5.getRow(), null);
			page.deleteAtSlot(1, true, (LogicalUndo)null);

			t_util.t_checkRecordCount(page, 3, 2);
			t_util.t_checkFetchBySlot(page, 0, REC_005, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_004, true, false);
			t_util.t_checkFetchBySlot(page, 2, REC_003, false, false);

			page.unlatch();
			t.rollbackToSavePoint(SP2, null);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, 3, 3);
			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_004, false, false);
			t_util.t_checkFetchBySlot(page, 2, REC_003, false, false);

		// after a rollback to sp, do some more changes

			slot0 = page.getSlotNumber(r0);
			page.updateAtSlot(slot0, row5.getRow(), null);
			page.deleteAtSlot(0, true, (LogicalUndo)null);
			page.deleteAtSlot(1, true, (LogicalUndo)null);
			page.deleteAtSlot(2, true, (LogicalUndo)null);

			t_util.t_checkRecordCount(page, 3, 0);
			t_util.t_checkFetchBySlot(page, 0, REC_005, true, false);
			t_util.t_checkFetchBySlot(page, 1, REC_004, true, false);
			t_util.t_checkFetchBySlot(page, 2, REC_003, true, false);

			page.unlatch();
			t.rollbackToSavePoint(SP1, null);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// note that an insert, when rolled back, becomes a deleted row but
			// will not disappear.  A purge row will come back at the same slot
			// and with the same record id.
			t_util.t_checkRecordCount(page, 4, 2);

			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_002, false, false);
			t_util.t_checkFetchBySlot(page, 2, REC_004, true, false);
			t_util.t_checkFetchBySlot(page, 3, REC_003, true, false);

			// add one more record to this
			if (page.spaceForInsert())
				t_util.t_insertAtSlot(page, 3, row5);

			REPORT("setup S202: containerId " + cid + " recordCount " + page.recordCount());

			register(key(202, 1), cid);
			register(key(202, 2), page.recordCount());

		}
		finally
		{
			if (page != null && page.isLatched())
				page.unlatch();
			ctx.resetContext();
		}

		// let recovery undo rollback this transaction

	}


	/* recover test 202 */
	protected void R202() throws T_Fail, StandardException
	{
		long cid = find(key(202, 1));
		if (cid < 0)
		{
			REPORT("R202 not run");
			return;
		}
		int recordCount = (int)find(key(202, 2));

		Transaction t = t_util.t_startTransaction();
		Page page = null;

		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			// rollback will leave the page with only deleted rows
			t_util.t_checkRecordCount(page, recordCount, 0);
			t_util.t_checkFetchBySlot(page, 0, REC_001, true, true);
			t_util.t_checkFetchBySlot(page, 1, REC_002, true, true);
			t_util.t_checkFetchBySlot(page, 2, REC_004, true, true);
			if (recordCount == 5)
			{
				t_util.t_checkFetchBySlot(page, 3, REC_005, true, true);
				t_util.t_checkFetchBySlot(page, 4, REC_003, true, true);
			}
			else
				t_util.t_checkFetchBySlot(page, 3, REC_003, true, true);

		}
		finally
		{
			if (page != null && page.isLatched())
				page.unlatch();

			t_util.t_commit(t);
			t.close();
		}
		PASS("R202: containerId " + cid + " recordCount " + recordCount);
	}

	/*
	 * test 203 -  incomplete and committed and aborted
	 *				transaction with intervening rollback to savepoints
	 */
	protected void S203() throws T_Fail, StandardException
	{
		int numtrans = 5;
		int numpages = 2;
		int i,j;

		T_TWC[] t = new T_TWC[numtrans];
		for (i = 0; i < numtrans; i++)
			t[i] =  t_util.t_startTransactionWithContext();

		Page[][] page = null;

		try
		{
			long[] cid = new long[numtrans];
			ContainerHandle[] c = new ContainerHandle[numtrans];

			for (i = 0; i < numtrans; i++)
			{
				cid[i] = t_util.t_addContainer(t[i], 0);
				t_util.t_commit(t[i]);
				c[i] = t_util.t_openContainer(t[i], 0, cid[i], true);
			}

			page = new Page[numtrans][numpages];
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

			t[0].switchTransactionContext();
			t[0].setSavePoint(SP1, null);	// sp1
			t[0].resetContext();


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
				t[i].switchTransactionContext();
				t[i].setSavePoint(SP1, null);
				t[i].resetContext();
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

			// relatch relavante pages
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


			REPORT("setup S203: numtrans " + numtrans + " numpages " + numpages);

			for (i = 0; i < numtrans; i++)
			{
				String str = "container " + i + ":" + cid[i] + " pages: ";
				register(key(203, i+10), cid[i]);

				for (j = 0; j < numpages; j++)
				{
					str += pagenum[i][j] + " ";
					register(key(203, (i+1)*1000+j), pagenum[i][j]);
				}
				REPORT("\t" + str);
			}

			register(key(203,1), numtrans); 
			register(key(203,2), numpages);

		}
		finally
		{
			for (i = 0; i < numtrans; i++)
			{
				for (j =0; j < numpages; j++)
				{
					if (page != null && page[i][j] != null
						&& page[i][j].isLatched())
						page[i][j].unlatch();
				}
			}
		}

		// let recovery rollback incomplete transactions t0 and t3

	}


	/* recover test 203 */
	protected void R203() throws T_Fail, StandardException
	{
		int numtrans = (int)find(key(203, 1));
		int numpages = (int)find(key(203, 2));
		int i,j;
		if (numtrans < 5 || numpages < 1)
		{
			REPORT("R203 not run");
			return;
		}
		else
		{
			REPORT("R203 started, numtrans " + numtrans + " numpages " +
				  numpages );
		}

		Transaction t = t_util.t_startTransaction();

		try
		{

			long[] cid = new long[numtrans];
			ContainerHandle[] c = new ContainerHandle[numtrans];

			long[][] pagenum = new long[numtrans][numpages];
			Page[][] page = new Page[numtrans][numpages];


			for (i = 0; i < numtrans; i++)
			{

				cid[i] = find(key(203, i+10));
				c[i] = t_util.t_openContainer(t, 0, cid[i], true);
			
				for (j = 0; j < numpages; j++)
				{
					pagenum[i][j] = find(key(203, (i+1)*1000+j));

					if (SanityManager.DEBUG)
					{
						if (i == 0 && j == (numpages-1))
						{
							SanityManager.DEBUG_SET("TEST_BAD_CHECKSUM");
							Page p = null;
							try {
								p = c[i].getPage(pagenum[i][j]);
							} catch (StandardException se) {

								if (se.getMessageId().equals(SQLState.FILE_IO_GARBLED))
									REPORT("bad checksum tested");
								else
									throw se; // not expected
							}
							SanityManager.DEBUG_CLEAR("TEST_BAD_CHECKSUM");
							if (p != null)
								throw T_Fail.testFailMsg("failed to generate expected error with bad checksum");
						}
					}

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
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
		PASS("R203: numtrans " + numtrans + " numpages " + numpages);
	}

	/*
	 * test 204 - incomplete and committed and aborted internal transactions
	 */
	protected void S204() throws T_Fail, StandardException
	{
		// start 2 user transaction, then 2 internal transaction, then
		// another user transaction.
		// do some work on all 5 transactions
		// roll back one of the internal transaction
		// commit the other one
		// do some more work on the 2 internal transactions and let
		// recovery roll them all back
		T_TWC ut1 = t_util.t_startTransactionWithContext();
		T_TWC ut2 = t_util.t_startTransactionWithContext();
		Page p10, p11, p20, p21, p30;
		p10 = p11 = p20 = p21 = p30 = null;

		try
		{

			long cid10 = t_util.t_addContainer(ut1, 0);
			long cid11 = t_util.t_addContainer(ut1, 0);
			t_util.t_commit(ut1);

			long cid20 = t_util.t_addContainer(ut2, 0);
			long cid21 = t_util.t_addContainer(ut2, 0);
			t_util.t_commit(ut2);

			T_RawStoreRow row = new T_RawStoreRow(REC_001);

		// all three user transactions have committed.
		// 
		// container	used by		row				row
		// cid10		ut1			r10
		// cid11		it1			r11		commit	r12
		// cid20		ut2			r20	
		// cid21		it2			r21		abort	r22
		// cid30		ut3			r30		
		// (ut3 is started after everything is done)
		// after recovery, r11 is the only record 

			ut1.switchTransactionContext();

			ContainerHandle c10 = t_util.t_openContainer(ut1, 0, cid10, true);
			p10 = t_util.t_addPage(c10);
			RecordHandle r10 = t_util.t_insert(p10, row);

			Transaction it1 = t_util.t_startInternalTransaction();
			ContainerHandle c11 = t_util.t_openContainer(it1, 0, cid11, true);
			p11 = t_util.t_addPage(c11);
			RecordHandle r11 = t_util.t_insert(p11, row);
			ut1.resetContext();

			ut2.switchTransactionContext();
			ContainerHandle c20 = t_util.t_openContainer(ut2, 0, cid20, true);
			p20 = t_util.t_addPage(c20);
			RecordHandle r20 = t_util.t_insert(p20, row);

			Transaction it2 = t_util.t_startInternalTransaction();
			ContainerHandle c21 = t_util.t_openContainer(it2, 0, cid21, true);
			p21 = t_util.t_addPage(c21);
			RecordHandle r21 = t_util.t_insert(p21, row);
			ut2.resetContext();

		// r10, r1, r20, r21, r30 inserted by the corresponding transactions

		// commit it1 - it uses the same context manager as ut1
			ut1.switchTransactionContext();
			it1.commit();

		// container is left opened and page p11 is left latched
			t_util.t_checkFetch(p11, r11, REC_001);

			// use it1 to add another row
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			RecordHandle r12 = t_util.t_insert(p11, row2);
			t_util.t_checkFetch(p11, r12, REC_002);
			ut1.resetContext();

			// abort it2 - it uses the same context maanger as ut2
			ut2.switchTransactionContext();
			it2.abort();
			
		// need to reopen container
			c21 = t_util.t_openContainer(it2, 0, cid21, true);
			p21 = t_util.t_getLastPage(c21);
			RecordHandle r22 = t_util.t_insert(p21, row2);
			ut2.resetContext();

			// start ut3 at the end to test
			// internal transactions are rolled back first

			T_TWC ut3 = t_util.t_startTransactionWithContext();
			long cid30 = t_util.t_addContainer(ut3, 0);
			t_util.t_commit(ut3);

			ContainerHandle c30 = t_util.t_openContainer(ut3, 0, cid30, true);

			ut3.switchTransactionContext();
			p30 = t_util.t_addPage(c30);
			RecordHandle r30 = t_util.t_insert(p30, row);
			ut3.resetContext();

			register(key(204, 10), cid10);
			register(key(204, 11), cid11);
			register(key(204, 20), cid20);
			register(key(204, 21), cid21);
			register(key(204, 30), cid30);

			REPORT("setup S204: cid10 " + cid10 +
				   ", cid11 " + cid11 +
				   ", cid20 " + cid20 +
				   ", cid21 " + cid21 +
				   ", cid30 " + cid30);

		}
		finally
		{
			if (p10 != null && p10.isLatched())
				p10.unlatch();

			if (p11 != null && p11.isLatched())
				p11.unlatch();

			if (p20 != null && p20.isLatched())
				p20.unlatch();

			if (p21 != null && p21.isLatched())
				p21.unlatch();

			if (p30 != null && p30.isLatched())
				p30.unlatch();
		}
			// let recovery rollback incomplete transactions 

	}

	/*
	 * test recovery of 204
	 */
	protected void R204() throws T_Fail, StandardException
	{
		long cid10 = find(key(204, 10));
		if (cid10 < 0)
		{
			REPORT("R204 not run");
			return;
		}

		long cid11 = find(key(204, 11));
		long cid20 = find(key(204, 20));
		long cid21 = find(key(204, 21));
		long cid30 = find(key(204, 30));

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c;
			Page p;

			c = t_util.t_openContainer(t, 0, cid10, false);
			p = t_util.t_getLastPage(c);
			t_util.t_checkRecordCount(p, 1, 0);
			p.unlatch();

			c = t_util.t_openContainer(t, 0, cid11, false);
			p = t_util.t_getLastPage(c);
			t_util.t_checkRecordCount(p, 2, 1); // r11 is the only record that is not rolled back
			p.unlatch();

			c = t_util.t_openContainer(t, 0, cid20, false);
			p = t_util.t_getLastPage(c);
			t_util.t_checkRecordCount(p, 1, 0);
			p.unlatch();

			c = t_util.t_openContainer(t, 0, cid21, false);
			p = t_util.t_getLastPage(c);
			t_util.t_checkRecordCount(p, 2, 0);
			p.unlatch();

			c = t_util.t_openContainer(t, 0, cid30, false);
			p = t_util.t_getLastPage(c);
			t_util.t_checkRecordCount(p, 1, 0);
			p.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R204 passed: cid10 " + cid10 +
			   ", cid11 " + cid11 +
			   ", cid20 " + cid20 +
			   ", cid21 " + cid21 +
			   ", cid30 " + cid30);
	}

	/*
	 * test 300 - incomplete transaction with drop containers 
	 */
	protected void S300() throws T_Fail, StandardException
	{
		T_TWC ctx = t_util.t_startTransactionWithContext();
		Transaction t = ctx.tran;

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			t_util.t_openContainer(t, 0, cid, true);
			t_util.t_dropContainer(t, 0, cid);

			REPORT("setup S300: containerId " + cid);

			register(key(300, 1), cid);
		}
		catch (StandardException se)
		{
			t_util.t_abort(t);	
			t.close();
			throw se;
		}
		catch (T_Fail tf)
		{
			t_util.t_abort(t);	
			t.close();
			throw tf;
		}

		// let recovery rollback incomplete transaction
	}

	/*
	 * test recovery of 300
	 */
	protected void R300() throws T_Fail, StandardException
	{
		long cid = find(key(300, 1));
		if (cid < 0)
		{
			REPORT("R300 not run");
			return;
		}

		Transaction t = t_util.t_startTransaction();
		// recovery should have rolled back the dropped container
		t_util.t_openContainer(t, 0, cid, true);

		t_util.t_commit(t);
		t.close();

		PASS("R300 : containerId " + cid);

	}

	/*
	 * incomplete transactions with create container
	 */
	protected void S301() throws T_Fail, StandardException
	{
		T_TWC ctx = t_util.t_startTransactionWithContext();
		Transaction t = ctx.tran;
		Page page = null;
		ctx.switchTransactionContext();

		try
		{

			long cid1 = t_util.t_addContainer(t, 0);
			ContainerHandle c1 = t_util.t_openContainer(t, 0, cid1, true);
			page = t_util.t_addPage(c1);
			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			RecordHandle r1 = t_util.t_insert(page, row1);

			t.abort();

			ContainerKey id1 = new ContainerKey(0, cid1);
			c1 = t.openContainer(id1, ContainerHandle.MODE_READONLY);
			if (c1 != null)
				throw T_Fail.testFailMsg("expect container to be dropped");

			LockingPolicy nolock = 
				t.newLockingPolicy(LockingPolicy.MODE_NONE, 0, false);

			RawContainerHandle stub = 
				((RawTransaction)t).openDroppedContainer(
					new ContainerKey(0, cid1), nolock);
			
			/*Not true always after fix for p4#25641(fix for bug:4580)
			  Checkpoint calls cleans up the stubs that not necessary
			  for recovery.
			  if (stub == null)
			  throw T_Fail.testFailMsg("drop container should still be there");
			*/
			if(stub!=null)
				if (stub.getContainerStatus() != RawContainerHandle.COMMITTED_DROP)
					throw T_Fail.testFailMsg("expect container to be committed dropped");

			long cid2 = t_util.t_addContainer(t, 0);
			ContainerHandle c2 = t_util.t_openContainer(t, 0, cid2, true);
			page = t_util.t_addPage(c2);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			RecordHandle r2 = t_util.t_insert(page, row2);

			REPORT("setup S301: cid1 " + cid1 + " cid2 " + cid2);
			register(key(301, 1), cid1);
			register(key(301, 2), cid2);

		}
		finally	
		{
			if (page != null && page.isLatched())
				page.unlatch();
			ctx.resetContext();
		}
		// let recovery rollback incomplete transaction
	}

	/*
	 * test recovery of 301
	 */
	protected void R301() throws T_Fail, StandardException
	{
		long cid1 = find(key(301, 1));
		if (cid1 < 0)
		{
			REPORT("R301 not run");
			return;
		}

		long cid2 = find(key(301, 2));

		Transaction t = t_util.t_startTransaction();
		try
		{
			LockingPolicy nolock = 
				t.newLockingPolicy(LockingPolicy.MODE_NONE, 0, false);

			ContainerKey id1 = new ContainerKey(0, cid1);
			ContainerHandle c = t.openContainer(id1, ContainerHandle.MODE_READONLY);
			if (c != null)
				throw T_Fail.testFailMsg("expect container to be dropped");

			RawContainerHandle stub = ((RawTransaction)t).openDroppedContainer(
				id1, nolock);
			/*Not true always after fix for p4#25641(fix for bug:4580)
			  Checkpoint calls cleans up the stubs that not necessary
			  for recovery.
			  if (stub == null)
			  throw T_Fail.testFailMsg("drop container should still be there");
			*/
			if(stub!=null)
				if (stub.getContainerStatus() != RawContainerHandle.COMMITTED_DROP)
					throw T_Fail.testFailMsg("expect container to be committed dropped");

			ContainerKey id2 = new ContainerKey(0, cid2);
			c = t.openContainer(id2, ContainerHandle.MODE_READONLY);
			if (c != null)
				throw T_Fail.testFailMsg("expect container to be dropped");

			stub = ((RawTransaction)t).openDroppedContainer(
				id2, nolock);
			/*Not true always after fix for p4#25641(fix for bug:4580)
			  Checkpoint calls cleans up the stubs that not necessary
			  for recovery.
			  if (stub == null)
			  throw T_Fail.testFailMsg("drop container should still be there");
			*/
			if(stub!=null)
				if (stub.getContainerStatus() != RawContainerHandle.COMMITTED_DROP)
					throw T_Fail.testFailMsg("expect container to be committed dropped");

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R301 : cid1 " + cid1 + " cid2 " + cid2);

	}


	
	/*
	 * test 302 - purge and delete with no data logging for purges
	 */
	protected void S302() throws T_Fail, StandardException
	{
		Transaction t = t_util.t_startTransaction();

		try
		{

			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
			T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
			T_RawStoreRow row5 = new T_RawStoreRow(REC_005);

			long numPurged = 0;

			// slot 0
			RecordHandle r1 = t_util.t_insertAtSlot(page, 0, row1);
			// slot 1
			RecordHandle r2 = t_util.t_insertAtSlot(page, 1, row2);
			// slot 2
			RecordHandle r3 = (r2 == null) ? r2 : t_util.t_insertAtSlot(page, 2, row3);
			// slot 3
			RecordHandle r4 = (r3 == null) ? r3 : t_util.t_insertAtSlot(page, 3, row4);
			// slot 4
			RecordHandle r5 = (r4 == null) ? r4 : t_util.t_insertAtSlot(page, 4, row5);
			if (r5 != null)
			{
				int slot = page.getSlotNumber(r5);
				page.deleteAtSlot(slot, true, null);
			}

			t_util.t_commit(t);
			
			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			//do some purging now
			// purge slot 0
			page.purgeAtSlot(0, 1, false);
			// purge slot 1 and 2 3
			page.purgeAtSlot(0, 3, false);
			t_util.t_abort(t);
			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			//repurge the rows that got rolled back.
			page.purgeAtSlot(0, 1, false);
			numPurged++;
			if (r5 != null)
			{
				// purge slot 1 and 2
				page.purgeAtSlot(0, 2, false);
				numPurged += 2;
			}

			REPORT("setup S302: containerId " + cid + " recordCount " +
				   page.recordCount() + " numPurges " + numPurged);

			register(key(302,1), cid);
			register(key(302,2), page.recordCount());
			register(key(302,3), numPurged);

			page.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
	}

	/* recover test 302 */
	protected void R302() throws T_Fail, StandardException
	{
		long cid = find(key(302,1));
		if (cid < 0)
		{
			REPORT("R302 not run");
			return;
		}
		int recordCount = (int)find(key(302,2));
		int numPurged = (int)find(key(302,3));

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, false);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, recordCount, 1);

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
			T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
			T_RawStoreRow row5 = new T_RawStoreRow(REC_005);

			t_util.t_checkFetchBySlot(page, 1, REC_005, true, false);
			if (!page.isDeletedAtSlot(1))
				throw T_Fail.testFailMsg("record should be deleted");
			//REC_004 should have become a null value because of rollback of purge
			t_util.t_checkFetchBySlot(page, 0, "NULL", false, false);
			page.unlatch();
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R302: containerId " + cid + " recordCount " +
			   recordCount + " numPurges " + numPurged);
	}


		
	/**
		Test space reclaimation - purging of a row with serveral long columns
	    rollback and repurge them again.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void S303() throws StandardException, T_Fail
	{
		REPORT("START S303");
		long segment = 0;
		T_TWC ctx = t_util.t_startTransactionWithContext();
		Transaction t = ctx.tran;
		ctx.switchTransactionContext();

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
			r2.setColumn(0, 1600, REC_001); // this takes 3200 bytes
			r2.setColumn(1, 4000, REC_002); // this takes 8000 bytes
			r2.setColumn(2, 1600, REC_001);
			r2.setColumn(3, 4000, REC_002);
			r2.setColumn(4, 1600, REC_001);
			r2.setColumn(5, 4000, REC_002);
			RecordHandle rh2 = t_util.t_insertAtSlot(page, 0, r2, (byte)
													 insertFlag);	
			
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

			REPORT("S303 - Nextpage is " + nextPageNumber);

			t_util.t_commit(t);

			// now purge them
			c = t_util.t_openContainer(t, segment, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			t_util.t_checkRecordCount(page, 2, 2);
			page.purgeAtSlot(0, 2, false);
			t_util.t_checkEmptyPage(page);
			page.unlatch();
			page = null;

			t_util.t_abort(t);

			// give some time for post commit to finish
			t_util.t_wait(10);		// wait 10 milliseconds.


			// Purge them again and roll them back via recovery.  These should not
			// be reclaimed.
			c = t_util.t_openContainer(t, segment, cid, true);
 			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 2, 2);
			int rh1slotnumber = page.getSlotNumber(rh1);
			int rh2slotnumber = page.getSlotNumber(rh2);
			page.purgeAtSlot(0, 2, false);
			t_util.t_checkEmptyPage(page);

			page.unlatch();
			page = null;

			REPORT("S303 - Purged Slots" + rh1slotnumber + "," + rh2slotnumber);

			register(key(303,1), cid);
			register(key(303,2), rh1slotnumber);
			register(key(303,3), rh2slotnumber);
			
			// let recovery undo rollback this transaction
		}
		finally
		{
			if (page != null)
				page.unlatch();
			ctx.resetContext();
		}

		PASS("S303");
	}

	
	/* recover test 303:
	 * repurge the same rows whose purging we rolled back in s303.
	 */
	protected void R303() throws T_Fail, StandardException
	{
		long cid = find(key(303,1));
		if (cid < 0)
		{
			REPORT("R303 not run");
			return;
		}

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 2, 2);
			int r1slot = (int)find(key(303,2));
			int r2slot = (int)find(key(303,3));
			T_RawStoreRow r1 = new T_RawStoreRow(1);
			// insert a long column
			r1.setColumn(0, 5000, REC_001);
			// During purges when data is not logged when slots are purged
			// they become null on rollback and some cases like long columns
			// we remove the wholepage on rollback we get the data back.
			T_RawStoreRow r2_wnl = new T_RawStoreRow(6);
			r2_wnl.setColumn(0, 4, REC_NULL); 
			r2_wnl.setColumn(1, 4000, REC_002); 
			r2_wnl.setColumn(2, 1600, REC_001);
			r2_wnl.setColumn(3, 4000, REC_002);
			r2_wnl.setColumn(4, 1600, REC_001);
			r2_wnl.setColumn(5, 4000, REC_002);
			RecordHandle rh1 = page.getRecordHandleAtSlot(r1slot) ;

			t_util.t_checkFetch(page, rh1, r1);
			RecordHandle rh2 = page.getRecordHandleAtSlot(r2slot) ;
			t_util.t_checkFetch(page, rh2, r2_wnl);

			//purge after the recovery.
			page.purgeAtSlot(0, 2, false);
			
			page.unlatch();
			page = null;
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R303");
	}



		
	/**
		Test space reclaimation - purging of a long rows with a rollback and
        purging again after recovery in R304
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	 */
	protected void S304() throws StandardException, T_Fail
	{
		long segment = 0;
		T_TWC ctx = t_util.t_startTransactionWithContext();
		Transaction t = ctx.tran;
		ctx.switchTransactionContext();

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
			page.unlatch();
			page = null;
			t_util.t_commit(t);

			c = t_util.t_openContainer(t, segment, cid, true);
			// Now purge that long row.
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 1, 1);
			page.purgeAtSlot(0, 1, false);
			t_util.t_checkEmptyPage(page);
			page.unlatch();
			page = null;
			// let recovery undo rollback this transaction
			register(key(304,1), cid);

		}
		finally	
		{
			if (page != null)
				page.unlatch();
			ctx.resetContext();
		}

		PASS("S304");
	}

	/* recover test 304:
	 * repurge the same rows whose purging we rolled back in s304 at recovery.
	 */
	protected void R304() throws T_Fail, StandardException
	{
		long cid = find(key(304,1));
		if (cid < 0)
		{
			REPORT("R304 not run");
			return;
		}

		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			t_util.t_checkRecordCount(page, 1, 1);

			// During purges when data is not logged when slots are purged
			// they become null on rollback and some cases like long columns
			// we remove the wholepage on rollback we get the data back.
			T_RawStoreRow r1_wnl = new T_RawStoreRow(100);
			for (int i = 0; i < 18; i++)
				r1_wnl.setColumn(i, 4, REC_NULL);
			for (int i = 18; i < 100; i++)
				r1_wnl.setColumn(i, 100+i, REC_001);
			RecordHandle rh1 = page.getRecordHandleAtSlot(0) ;
			t_util.t_checkFetch(page, rh1, r1_wnl);		

			//purge after the recovery.
			page.purgeAtSlot(0, 1, false);
			page.unlatch();
			page = null;
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

		PASS("R304");
	}


	/*
	 * S999 - last test of the recovery unit test, this leave the end of the
	 * log in a fuzzy state, do NOT write any more log record after this or 
	 * you will corrupt the database
	 *
	 * DO NOT run this test if recovery test is ever run iteratively
	 */
	protected void S999() throws T_Fail, StandardException
	{
		// only runnable in debug server since trace flags are set by SanityManager
		if (!SanityManager.DEBUG) 
			return;

		T_TWC ctx = t_util.t_startTransactionWithContext();
		Transaction t = ctx.tran;
		ctx.switchTransactionContext();

		// LogToFile.TEST_LOG_SWITCH_LOG
		SanityManager.DEBUG_SET("TEST_LOG_SWITCH_LOG");

		// this will switch the log without writing out a checkpoint log record
		factory.checkpoint();
		Page page = null;

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			RecordHandle rh1 = t_util.t_insertAtSlot(page, 0, row1);
			t_util.t_commit(t);

			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			c = t_util.t_openContainer(t, 0, cid, true);
			page = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);

			RecordHandle rh2 = t_util.t_insertAtSlot(page, 1, row2);	
			if (rh2 == null)
			{
				REPORT("S999 not run, page cannot accomodate 2 rows");
				return;
			}


			t_util.t_checkRecordCount(page, 2, 2);
			t_util.t_checkFetch(page, rh1, REC_001);
			t_util.t_checkFetch(page, rh2, REC_002);

			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);

			// LogToFile.TEST_LOG_INCOMPLETE_LOG_WRITE
			SanityManager.DEBUG_SET("TEST_LOG_INCOMPLETE_LOG_WRITE");

			// let's hope that this page is not written to disk... can't until
			// the page is unlatch, and we never unlatch it
			RecordHandle rh3 = t_util.t_insert(page, row3); // this is written out incompletely
			if (rh3 == null) {
				REPORT("S999 not run, page cannot accomodate 3 rows");
				return;
			}


			t_util.t_checkRecordCount(page, 3, 3);
			t_util.t_checkFetch(page, rh3, REC_003);

			REPORT("setup S999: cid1 " + cid + " page " + page.getPageNumber());
			register(key(999, 1), cid);
			register(key(999, 2), page.getPageNumber());
		}
		finally
		{
			SanityManager.DEBUG_CLEAR("TEST_LOG_SWITCH_LOG");
			SanityManager.DEBUG_CLEAR("TEST_LOG_INCOMPLETE_LOG_WRITE");

			ctx.resetContext();
			// let recovery roll it back
		}
	}

	protected void R999() throws StandardException, T_Fail
	{
		long cid = find(key(999,1));
		if (cid < 0)
		{
			REPORT("R999 not run");
			return;
		}

		long pageid = find(key(999,2));
		Transaction t = t_util.t_startTransaction();
		try
		{
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page page = t_util.t_getPage(c, pageid);

			// there should be 2 rows on the page, 1 undelete and 1 deleted.
			// The third row is only partially written out the log and should
			// never appear
			t_util.t_checkRecordCount(page, 2, 1);
			t_util.t_checkFetchBySlot(page, 0, REC_001, false, false);
			t_util.t_checkFetchBySlot(page, 1, REC_002, true, false);
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
		PASS("R999: cid " + cid + " page " + pageid);

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



