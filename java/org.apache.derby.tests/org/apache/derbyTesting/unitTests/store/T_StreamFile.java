/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_StreamFile

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

import org.apache.derbyTesting.unitTests.harness.T_MultiThreadedIterations;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.impl.store.raw.data.*;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.locks.*;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.FormatIdOutputStream;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.raw.*;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.shared.common.reference.Property;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;

import java.io.*;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.AccessController;
import java.util.Properties;

/**
	An Impl unittest for rawstore data that is based on the stream file
*/

public class T_StreamFile extends T_MultiThreadedIterations {

	private static final String testService = "streamFileTest";

	static final String REC_001 = "McLaren";
	static final String REC_002 = "Ferrari";
	static final String REC_003 = "Benetton";
	static final String REC_004 = "Prost";
	static final String REC_005 = "Tyrell";
	static final String REC_006 = "Derby, Natscape, Goatscape, the popular names";
	static final String REC_007 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";

	static final String SP1 = "savepoint1";
	static final String SP2 = "savepoint2";


	static RawStoreFactory	factory;
	static LockFactory lf;
	static long commonContainer = -1;

	static boolean testRollback; // initialize in start
	static final String TEST_ROLLBACK_OFF = "derby.RawStore.RollbackTestOff";

	private static ContextService contextService;
	private T_Util t_util;

	public T_StreamFile() {
		super();
	}

	/**
	  @exception StandardException cannot startup the context service
	 */
	public void boot(boolean create, Properties startParams)
		 throws StandardException {
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
		Run the tests

		@exception T_Fail Unexpected behaviour from the API
	 */
	protected void setupTest() throws T_Fail {
		String rollbackOff = PropertyUtil.getSystemProperty(TEST_ROLLBACK_OFF);
		testRollback = !Boolean.valueOf(rollbackOff).booleanValue();

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
								testService, startParams);
			if (factory == null) {
				throw T_Fail.testFailMsg(getModuleToTestProtocolName() + " service not started.");
			}

			lf = factory.getLockFactory();
			if (lf == null) {
				throw T_Fail.testFailMsg("LockFactory.MODULE not found");
			}
		} catch (StandardException mse) {
			throw T_Fail.exceptionFail(mse);
		}

		t_util = new T_Util(factory, lf, contextService);
		commonContainer = commonContainer();

		return;
	}


	/**
	 * T_MultiThreadedIteration method
	 *
	 * @exception T_Fail Unexpected behaviour from the API
	 */
	protected void joinSetupTest() throws T_Fail {

		T_Fail.T_ASSERT(factory != null, "raw store factory not setup ");
		T_Fail.T_ASSERT(contextService != null, "Context service not setup ");
		T_Fail.T_ASSERT(commonContainer != -1, "common container not setup ");

		t_util = new T_Util(factory, lf, contextService);

	}

	protected T_MultiThreadedIterations newTestObject() {
		return new T_StreamFile();
	}

	/**
	  run the test

	  @exception T_Fail Unexpected behaviour from the API
	*/
	protected void runTestSet() throws T_Fail {

		// get a utility helper

		ContextManager cm1 = contextService.newContextManager();
		contextService.setCurrentContextManager(cm1);

		try {

			// boundry case: 1 row, 13 columns, string types
			SF001(1, 13, T_RowSource.STRING_ROW_TYPE, false);

			// boundry case: 1 rows, 1 null column, string types
			SF001(1, 1, T_RowSource.STRING_ROW_TYPE, false);

			// boundry case: 1000 rows, 1 null column, string types
			SF001(1000, 1, T_RowSource.STRING_ROW_TYPE, false);

			// boundry case: 1000 rows, 2 column (first null, second not null), string types
			SF001(1000, 2, T_RowSource.STRING_ROW_TYPE, false);

			// boundry case: 500 rows, 9 columns (first & last column null), string types
			SF001(500, 9, T_RowSource.STRING_ROW_TYPE, false);

			// 500 rows, 13 column, string type
			SF001(500, 13, T_RowSource.STRING_ROW_TYPE, false);

			// test error condition
			SF001(1000, 2, T_RowSource.STRING_ROW_TYPE, true);

			// The following test tests externalizable types, but we currently don't support it.
			// do, don't run the test yet.
			// 100 rows, 5 column, Integer object type
			//SF001(100, 5, T_RowSource.INTEGER_ROW_TYPE, false);
			// 100 rows, 1 column, Integer object type
			//SF001(100, 1, T_RowSource.INTEGER_ROW_TYPE, false);

			// SF002() tests are used to check performance of the stream file.
			// no need to run them regularly.
			//SF002(0);
			//SF002(1);

		} catch (StandardException se) {

            //Assume database is not active. DERBY-4856 thread dump
            cm1.cleanupOnError(se, false);
			throw T_Fail.exceptionFail(se);
		}
		finally {
			contextService.resetCurrentContextManager(cm1);
		}
	}

	/*
	 * create a container that all threads can use
	 */
	private long commonContainer() throws T_Fail {

		ContextManager cm1 = contextService.newContextManager();
		contextService.setCurrentContextManager(cm1);
		long cid;

		try {
			Transaction t = t_util.t_startTransaction();
			cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);
			t.close();
		}
		catch (StandardException se) {

            //Assume database is not active. DERBY-4856 thread dump
            cm1.cleanupOnError(se, false);
			throw T_Fail.exceptionFail(se);
		}
		finally {
			contextService.resetCurrentContextManager(cm1);
		}
		return cid;
	}

	/*
	 * create a stream container load with rowCount number of rows.
	 * fetch it all back, and check to make sure all rows are correct.
	 */
	protected void SF001(int rowCount, int columnCount, int columnType, boolean forceAbort)
		throws StandardException, T_Fail {

		Transaction t = t_util.t_startTransaction();

		int segmentId = StreamContainerHandle.TEMPORARY_SEGMENT;
		Properties properties = new Properties();
		properties.put(RawStoreFactory.STREAM_FILE_BUFFER_SIZE_PARAMETER, "16384");
 
		// create row source
		T_RowSource testRowSource = null;
		if (forceAbort)
			testRowSource = new T_RowSource(rowCount, columnCount, columnType, forceAbort, t);
		else
			testRowSource = new T_RowSource(rowCount, columnCount, columnType, forceAbort, null);

		long startms = System.currentTimeMillis();

		long containerId = t.addAndLoadStreamContainer(segmentId, properties, testRowSource);

		long endms = System.currentTimeMillis();
		long time = endms - startms;
		REPORT("SF001 - write: " + time + "ms");

		// open the container, and start fetching...
		StreamContainerHandle scHandle = 
            t.openStreamContainer(segmentId, containerId, false);

		// set up the template row
		DataValueDescriptor template[] = null;
		template = testRowSource.getTemplate();

		DataValueDescriptor readRow[] = null;
		readRow = testRowSource.getTemplate();
		segmentId = StreamContainerHandle.TEMPORARY_SEGMENT;
		int fetchCount = 0;
		startms = System.currentTimeMillis();
		while (scHandle.fetchNext(readRow)) {
			fetchCount++;
			// check to make sure the row is what we inserted.
			// this depends on T_RowSource generate identical rows.
			if (!readRow.toString().equals(template.toString()))
				throw T_Fail.testFailMsg("Record's value incorrect, expected :"
					+ template.toString() + ": - got :" + readRow.toString());
		}
		endms = System.currentTimeMillis();
		time = endms - startms;
		// check to make sure we got the right number of rows.
		if (fetchCount != rowCount)
			throw T_Fail.testFailMsg("incorrect number of row fetched.  Expecting " + rowCount
				+ " rows, got " + fetchCount + ", rows instead.");
		REPORT("SF001 - fetch: " + time + "ms");

		scHandle.close();

		t_util.t_commit(t);
		t.close();

		PASS("SF001, rowCount = " + rowCount + ", columnCount = " + columnCount + ", clumn type: " + columnType);
	}

	// this test test the rowSource over head.
	// when param set to 1, also gets the overhead for writeExternal for Storables
	protected void SF002(int param) throws StandardException, T_Fail {

		T_RowSource rowSource = new T_RowSource(500000, 13, 2, false, null);

		DynamicByteArrayOutputStream out = new DynamicByteArrayOutputStream(16384);
		FormatIdOutputStream logicalDataOut = new FormatIdOutputStream(out);

		long startms = System.currentTimeMillis();
		System.out.println("starting rowSource test, time: " + startms);
		try {

			FormatableBitSet validColumns = rowSource.getValidColumns();

			int numberFields = 0;
			if (validColumns != null) {
				for (int i = validColumns.size() - 1; i >= 0; i--) {
					if (validColumns.get(i)) {
						numberFields = i + 1;
						break;
					}
				}
			}

			DataValueDescriptor[] row = rowSource.getNextRowFromRowSource();
			while (row != null) {
				if (SanityManager.DEBUG) {
					SanityManager.ASSERT(row != null, 
										 "RowSource returned null");
				}

				int arrayPosition = -1;
				for (int i = 0; i < numberFields; i++) {
					// write each column out
					if (validColumns.get(i)) {
						arrayPosition++;
						DataValueDescriptor column = row[arrayPosition];

						if (param == 1) {
							try {
								Storable sColumn = (Storable) column;
								if (!sColumn.isNull()) {
									sColumn.writeExternal(logicalDataOut);
									out.reset();
								}
							} catch (IOException ioe) {
								throw T_Fail.exceptionFail(ioe);
							}
						}

					}
				}

				row = rowSource.getNextRowFromRowSource();
			}

		} finally {

		}

		long endms = System.currentTimeMillis();
		long time2 = endms - startms;
		if (param != 1)
			System.out.println("ended rowSource test, time: " + endms
				+ ", time spent = " + time2);
		else
			System.out.println("------ writeExternal called....\n ended rowSource test, time: " + endms
				+ ", time spent = " + time2);
		
		PASS("SF002");
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
