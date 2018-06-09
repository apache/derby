/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_FileSystemData

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

import org.apache.derby.impl.store.raw.data.*;

import org.apache.derbyTesting.unitTests.harness.T_MultiThreadedIterations;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.locks.*;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.raw.*;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.shared.common.reference.Property;
import java.io.*;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedAction;
import java.security.AccessController;
import java.util.Properties;
/**
	An Impl unittest for rawstore data that is based on the FileSystem
*/

public class T_FileSystemData extends T_MultiThreadedIterations {

	private static final String testService = "fileSystemDataTest";

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

	public T_FileSystemData() 
	{
		super();
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
		Run the tests

		@exception T_Fail Unexpected behaviour from the API
	 */
	protected void setupTest() throws T_Fail 
	{
		String rollbackOff = PropertyUtil.getSystemProperty(TEST_ROLLBACK_OFF);
		testRollback = !Boolean.valueOf(rollbackOff).booleanValue();


		// don't automatic boot this service if it gets left around
		if (startParams == null) {
			startParams = new Properties();
		}

		// see if we are testing encryption
		startParams = T_Util.setEncryptionParam(startParams);

		startParams.put(Property.NO_AUTO_BOOT, Boolean.TRUE.toString());
		// remove the service directory to ensure a clean run
		startParams.put(Property.DELETE_ON_CREATE, Boolean.TRUE.toString());

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
		return new T_FileSystemData();
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

			runCostEstimationTests();
			runAllocationTests();

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
	private long commonContainer() throws T_Fail
	{
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

	protected void runCostEstimationTests() throws T_Fail, StandardException
	{
		CostEstimationTest1();
	}

	protected void runAllocationTests() throws T_Fail, StandardException
	{
		// don't run these for > 2 threads
		if (threadNumber < 2)
		{
			AllocTest1();			// test remove and reuse of page
			AllocTest2();			// test remove and drop and rollback of remove 
			AllocTest3();			// test multiple alloc page
			AllocTest4();			// test preallocation
		}

		// can't get this test to pass consistently because it depends on
		// timing of the cache.
		// AllocTest5();			// test gettting 1/2 filled page for insert

		AllocMTest1(commonContainer); // test multi thread access to the same container
	}

	/**
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Standard Derby error policy
	*/
	protected void CostEstimationTest1() throws StandardException, T_Fail 
	{
		// getEstimatedRowCount(0), setEstimatedRowCount(long count, int flag),
		// getEstimatedPageCount(int flag);

		Transaction t = t_util.t_startTransaction();
		long cid = t_util.t_addContainer(t, 0);
		t_util.t_commit(t);

		ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

		try
		{
			int numRows = 10;
			T_RawStoreRow row = new T_RawStoreRow(REC_001);
			RecordHandle rh[] = new RecordHandle[numRows];

			// insert numRows rows into container
			for (int i = 0; i < numRows; i++)
				rh[i] = t_util.t_insert(c, row);

			t_util.t_commit(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			if ((c.getEstimatedRowCount(0) != numRows) &&
			    (c.getEstimatedRowCount(0) != (numRows - 1)))
            {
                // due to timing, sometimes estimate row count is 9 rather than
                // 10.

				throw T_Fail.testFailMsg(
                    "expect estimated row count to be " + (numRows - 1) + 
                    " or " + numRows +
                         ", got " + c.getEstimatedRowCount(0));
            }

			// now update them that cause overflowing - expect the same row count
			T_RawStoreRow longRow = new T_RawStoreRow(REC_007);
			for (int i = 0; i < numRows; i++)
				t_util.t_update(c, rh[i], longRow);

			t_util.t_commit(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			if (c.getEstimatedRowCount(0) != numRows)

			if ((c.getEstimatedRowCount(0) != numRows) &&
			    (c.getEstimatedRowCount(0) != (numRows - 1)))
            {
                // due to timing, sometimes estimate row count is 9 rather than
                // 10.
                
				throw T_Fail.testFailMsg(
                    "expect after update same estimated row count, but it is not." +
                    "expect estimated row count to be " + (numRows - 1) + 
                    " or " + numRows + ", got " + c.getEstimatedRowCount(0));
            }

			// now focibly set the row count
			c.setEstimatedRowCount(2*numRows, 0);

			if (c.getEstimatedRowCount(0) != 2*numRows)
				throw T_Fail.testFailMsg("forcibly setting estimated row count doesn't seem to work");

			// now purge some rows, this should alter the row count.
			Page p = null;
			long pnum = 0;
			long purgedCount = 0;
			for (p = c.getFirstPage(); p != null; p = c.getNextPage(pnum)) 
			{
				int rcount = p.recordCount()/3;
				pnum = p.getPageNumber();

				p.deleteAtSlot(0, true, (LogicalUndo)null);
				p.purgeAtSlot(rcount, rcount, true); // purget the middle 1/3 of the page
				purgedCount += rcount + 1;

				p.unlatch();
			}
		
			t_util.t_commit(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			if (c.getEstimatedRowCount(0) != (2*numRows - purgedCount))
				throw T_Fail.testFailMsg("expect " + (2*numRows-purgedCount) + 
										 " after purge"); 
		
			// now get rid of some pages to alter the row count
			REPORT("before page delete, estRC = " + (2*numRows) + " - " + purgedCount);

			for (p = c.getFirstPage(); p != null; p = c.getNextPage(pnum))
			{
				pnum = p.getPageNumber();
				if ((pnum%2) == 0)
				{
					purgedCount += p.nonDeletedRecordCount();
					c.removePage(p);
				}
				else
					p.unlatch();
			}

			t_util.t_commit(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			if (c.getEstimatedRowCount(0) != (2*numRows - purgedCount))
				throw T_Fail.testFailMsg("expect " + (2*numRows-purgedCount) + 
										 " after page remove, got " + c.getEstimatedRowCount(0)); 

			PASS("CostEstimationTest1");
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}


	}

	protected void AllocTest1() throws StandardException, T_Fail 
	{
		/**
		  test remove and reuse of page
		*/
		Transaction t = t_util.t_startTransaction();

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			t_util.t_commit(t);

			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

		// create 5 pages, each insert a row into it, then remove 2 of them

			Page page1 = t_util.t_getPage(c, ContainerHandle.FIRST_PAGE_NUMBER);
			long p1 = page1.getPageNumber();
			T_RawStoreRow row1 = new T_RawStoreRow(REC_001);
			t_util.t_insert(page1, row1);

			Page page2 = t_util.t_addPage(c);
			long p2 = page2.getPageNumber();
			T_RawStoreRow row2 = new T_RawStoreRow(REC_002);
			int rid2 = t_util.t_insert(page2, row2).getId();

			Page page3 = t_util.t_addPage(c);
			long p3 = page3.getPageNumber();
			T_RawStoreRow row3 = new T_RawStoreRow(REC_003);
			t_util.t_insert(page3, row3);

			Page page4 = t_util.t_addPage(c);
			long p4 = page4.getPageNumber();
			T_RawStoreRow row4 = new T_RawStoreRow(REC_004);
			int rid4 = t_util.t_insert(page4, row4).getId();

			Page page5 = t_util.t_addPage(c);
			long p5 = page5.getPageNumber();
			T_RawStoreRow row5 = new T_RawStoreRow(REC_005);
			t_util.t_insert(page5, row5);

			t_util.t_removePage(c, page2);
			t_util.t_removePage(c, page4);
			t_util.t_commit(t);

		// now all the pages are unlatched
		// pages 2, 4 has been removed, pages 1, 3, 5 has not
		// make sure pages that are removed cannot be found again
			c = t_util.t_openContainer(t, 0, cid, true);

            if (SanityManager.DEBUG)
                SanityManager.DEBUG("SpaceTrace", "containeropened");

			Page p = c.getFirstPage();
			if (p == null)
				throw T_Fail.testFailMsg("get first page failed: expect " + p1 + " got null");
			if (p.getPageNumber() != p1)
				throw T_Fail.testFailMsg("get first page failed: expect " + p1
										 + " got " + p.getPageNumber());

			t_util.t_commit(t);		

		// closing the transaction many times to see if we can get the
		// deallocated page to free

			c = t_util.t_openContainer(t, 0, cid, true);
			p = c.getNextPage(p1);
			if (p == null || p.getPageNumber() != p3)
				throw T_Fail.testFailMsg("get next page failed");
			t_util.t_commit(t);

			c = t_util.t_openContainer(t, 0, cid, true);
			p = c.getNextPage(p3);
			if (p == null || p.getPageNumber() != p5)
				throw T_Fail.testFailMsg("get next page failed");
			t_util.t_commit(t);
		
			c = t_util.t_openContainer(t, 0, cid, true);
			p = t_util.t_getLastPage(c);	// make sure it skips over p5
			if (p == null || p.getPageNumber() != p5)
				throw T_Fail.testFailMsg("getLastPage failed");
			t_util.t_commit(t);

		// see if we can get any deallocated page back in 10 attempts
		// of add page
			int tries = 100;
			T_RawStoreRow row6 = new T_RawStoreRow(REC_001);

			long pnums[] = new long[tries];
			int  rids[] = new int[tries];
			pnums[0] = p2;			// pages 2 and 4 have been removed for a long time
			rids[0] = rid2;
			pnums[1] = p4;
			rids[1] = rid4;

			int match = -1;
			int i;
			for (i = 2 ; match < 0 && i < tries; i++)
			{
				c = t_util.t_openContainer(t, 0, cid, true);
				p = t_util.t_addPage(c);
				pnums[i] =  p.getPageNumber();

				for (int j = 0; j < i-1; j++)
				{
					if (pnums[j] == pnums[i])
					{
						match = j;
						break;
					}
				}

				if (match >= 0)
				{
					// p is a reused one, make sure it is empty
					t_util.t_checkEmptyPage(p);
					RecordHandle rh = t_util.t_insert(p, row6);
					if (rh.getId() == rids[match])
						throw T_Fail.testFailMsg("reused page recordId is not preserved");
					break;
				}
				else
					rids[i] = t_util.t_insert(p, row6).getId();

				t_util.t_removePage(c, p);
				t_util.t_commit(t);
			}
			t_util.t_dropContainer(t, 0, cid); // cleanup

			if (match >= 0)
				PASS("AllocTest1 success in " + i + " tries");
			else
				REPORT("AllocTest1 Not successful in " + i + 
					   " tries.  This is a timing depenedent test so this is not necessarily an indication of failure.");
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}

	}

	protected void AllocTest2() throws StandardException, T_Fail 
	{
		/**
		  More Test remove and reuse of page
		*/

		Transaction t = t_util.t_startTransaction();
		int numpages = 30;

		try
		{
			long cid = t_util.t_addContainer(t, 0);
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

			Page[] page = new Page[numpages];

			for (int i = 0; i < numpages; i++)
			{
				page[i] = t_util.t_addPage(c);
				t_util.t_removePage(c, page[i]);
			}

			// make sure a dropped container does not cause problem for page
			// that's been removed
			t_util.t_dropContainer(t, 0, cid); 

			t_util.t_commit(t);

			if (testRollback)
			{
				cid = t_util.t_addContainer(t, 0);
				c = t_util.t_openContainer(t, 0, cid, true);

				for (int i = 0; i < numpages; i++)
				{
					page[i] = t_util.t_addPage(c);
					t_util.t_removePage(c, page[i]);
				}

				t_util.t_abort(t);
			}
		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}


		PASS("AllocTest2");
	}

	protected void AllocTest3() throws StandardException, T_Fail 
	{
		/* test multiple alloc pages */

		if (!SanityManager.DEBUG)
		{
			REPORT("allocTest3 cannot be run on an insane server");
			return;
		}
        else
        {
            SanityManager.DEBUG_SET(AllocPage.TEST_MULTIPLE_ALLOC_PAGE);

            Transaction t = t_util.t_startTransaction();

            try
            {
                long cid = t_util.t_addContainer(t, 0);
                t_util.t_commit(t);

                ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);

                T_RawStoreRow row = new T_RawStoreRow(REC_001);
                int numrows = 10; // create 10 pages with 1 row each
                
                String threadName = Thread.currentThread().getName();

                Page page;
                for (int i = 0; i < numrows; i++)
                {
                    page = t_util.t_addPage(c);
                    t_util.t_insert(page, row);
                    page.unlatch();
                }

                int checkrows = 0;
                long pnum;
                for (page = c.getFirstPage();
                     page != null;
                     page = c.getNextPage(pnum))
                {
                    pnum = page.getPageNumber();
                    if (page.recordCount() > 0)
                    {
                        t_util.t_checkFetchFirst(page, REC_001);
                        checkrows++;
                    }
                    page.unlatch();
                }
                if (checkrows != numrows)
                    throw T_Fail.testFailMsg("number of rows differ");

                t.setSavePoint(SP1, null);

                // now remove 1/2 of the pages and check results
                int removedPages = 0;
                for (page = c.getFirstPage();
                     page != null;
                     page = c.getNextPage(pnum))
                {
                    pnum = page.getPageNumber();
                    if ((pnum % 2) == 0)
                    {
                        t_util.t_removePage(c, page);
                        removedPages++;
                    }
                    else
                        page.unlatch();
                }

                checkrows = 0;
                for (page = c.getFirstPage();
                     page != null;
                     page = c.getNextPage(pnum))
                {
                    pnum = page.getPageNumber();
                    if (page.recordCount() > 0)
                    {
                        t_util.t_checkFetchFirst(page, REC_001);
                        checkrows++;
                    }
                    page.unlatch();
                }
                if (checkrows != numrows - removedPages)
                    throw T_Fail.testFailMsg("number of rows differ");

                // remove every page backwards
                long lastpage = ContainerHandle.INVALID_PAGE_NUMBER;
                while((page = t_util.t_getLastPage(c)) != null)	// remove the last page
                {
                    if (lastpage == page.getPageNumber())
                        throw T_Fail.testFailMsg("got a removed last page");

                    lastpage = page.getPageNumber();
                    t_util.t_removePage(c, page);
                }

                if (c.getFirstPage() != null)
                    throw T_Fail.testFailMsg("get last page returns null but get fisrt page retuns a page");

                t.rollbackToSavePoint(SP1, null);	// roll back removes
                c = t_util.t_openContainer(t, 0, cid, true);

                checkrows = 0;
                for (page = c.getFirstPage();
                     page != null;
                     page = c.getNextPage(pnum))
                {
                    pnum = page.getPageNumber();
                    if (page.recordCount() > 0)
                    {
                        t_util.t_checkFetchFirst(page, REC_001);
                        checkrows++;
                    }
                    page.unlatch();
                }
                if (checkrows != numrows)
                    throw T_Fail.testFailMsg(threadName + "number of rows differ expect " +
                                             numrows + " got " + checkrows);


                t_util.t_abort(t);	// abort the whole thing, no rows left
                c = t_util.t_openContainer(t, 0, cid, true);

                int countPages = 0;
                for (page = c.getFirstPage();
                     page != null;
                     page = c.getNextPage(pnum))
                {
                    countPages++;
                    pnum = page.getPageNumber();
                    if (page.nonDeletedRecordCount() > 0)
                    {
                        throw T_Fail.testFailMsg("failed to remove everything " +
                                                 page.nonDeletedRecordCount() + 
                                                 " rows left on page " + pnum);
                    }
                    page.unlatch();
                }			

                if (countPages < numrows)
                    throw T_Fail.testFailMsg("rollback of user transaction should not remove allocated pages");

                t_util.t_dropContainer(t, 0, cid); 

            }
            finally
            {
                SanityManager.DEBUG_CLEAR(AllocPage.TEST_MULTIPLE_ALLOC_PAGE);
                t_util.t_commit(t);
                t.close();
            }
            PASS("AllocTest3");
        }
	}

	protected void AllocTest4() throws StandardException, T_Fail
	{
		if (!SanityManager.DEBUG)
		{
			REPORT("allocTest3 cannot be run on an insane server");
			return;
		}
        else
        {

            SanityManager.DEBUG_SET(AllocPage.TEST_MULTIPLE_ALLOC_PAGE);
            Transaction t = t_util.t_startTransaction();

            try
            {
                ////////////////////////////////////////////////////////
                // first test preallocation large table
                ////////////////////////////////////////////////////////
                Properties tableProperties = new Properties();
                tableProperties.put(Property.PAGE_SIZE_PARAMETER, Integer.toString(1024));
                tableProperties.put(RawStoreFactory.CONTAINER_INITIAL_PAGES, Integer.toString(100));

                long cid1 = 
                    t.addContainer(
                        0, ContainerHandle.DEFAULT_ASSIGN_ID, 
                        ContainerHandle.MODE_DEFAULT, tableProperties, 0);

                if (cid1 < 0)
                    throw T_Fail.testFailMsg("addContainer");

                ContainerHandle c1 = t_util.t_openContainer(t, 0, cid1, true);

                Page p1 = c1.getFirstPage();
                if (p1.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
                    throw T_Fail.testFailMsg("expect first page to have FIRST_PAGE_NUMBER");
                p1.unlatch();

                if (c1.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
                    throw T_Fail.testFailMsg("expect to have only 1 page allocated");

                t_util.t_commit(t);

                REPORT("AllocTest4 - create preallocated container " + cid1);

                ////////////////////////////////////////////////////////
                // next test special addpage interface
                ////////////////////////////////////////////////////////
                long cid2 = t_util.t_addContainer(t, 0, 1024, 0, 1, false);
                t_util.t_commit(t);

                ContainerHandle c2 = t_util.t_openContainer(t, 0, cid2, true);

                // add page for bulk load
                p1 = c2.addPage(ContainerHandle.ADD_PAGE_BULK);	
                long pnum1 = p1.getPageNumber();
                p1.unlatch();

                // since the interface does not guarentee that anything special will
                // actually happen, can't really test that. Just make sure that
                // everything else works
                Page p2 = c2.addPage();
                long pnum2 = p2.getPageNumber();
                p2.unlatch();

                Page p3 = c2.addPage(ContainerHandle.ADD_PAGE_BULK);	
                long pnum3 = p3.getPageNumber();
                p3.unlatch();

                Page p = c2.getFirstPage(); // this is the first page that came with the
                                       // container when it was created

                try
                {
                    long pnum0 = p.getPageNumber();
                    p.unlatch();
                    p = c2.getNextPage(pnum0);
                    if (p.getPageNumber() != pnum1)
                        throw T_Fail.testFailMsg("expected pagenum " + pnum1 + " got " + p.getPageNumber());
                    p.unlatch();
                    p = null;

                    p = c2.getNextPage(pnum1);
                    if (p.getPageNumber() != pnum2)
                        throw T_Fail.testFailMsg("expected pagenum " + pnum2 + " got " + p.getPageNumber());
                    p.unlatch();
                    p = null;

                    p = c2.getNextPage(pnum2);
                    if (p.getPageNumber() != pnum3)
                        throw T_Fail.testFailMsg("expected pagenum " + pnum3 + " got " + p.getPageNumber());
                    p.unlatch();
                    p = null;

                    p = c2.getNextPage(pnum3);
                    if (p != null)
                        throw T_Fail.testFailMsg("expected null page after " + pnum3 +
                                             " got " + p.getPageNumber());

                    // make sure rollback is unaffected
                    if (testRollback)
                    {
                        t_util.t_abort(t);
                        c2 = t_util.t_openContainer(t, 0, cid2, true);
                        p = t_util.t_getPage(c2, pnum0);
                        t_util.t_checkEmptyPage(p);
                        p.unlatch();
                        p = null;
                
                        p = t_util.t_getPage(c2, pnum1);
                        t_util.t_checkEmptyPage(p);
                        p.unlatch();
                        p = null;

                        p = t_util.t_getPage(c2, pnum2);
                        t_util.t_checkEmptyPage(p);
                        p.unlatch();
                        p = null;

                        p = t_util.t_getPage(c2, pnum3);
                        t_util.t_checkEmptyPage(p);
                        p.unlatch();
                        p = null;

                        p = t_util.t_getLastPage(c2);
                        if (p.getPageNumber() != pnum3)
                            throw T_Fail.testFailMsg("expect last page to be " + pnum3
                                                 + " got " + p.getPageNumber());
                        p.unlatch();
                        p = null;
                    }
                }
                finally
                {
                    if (p != null)
                        p.unlatch();
                    p = null;
                }
                REPORT("AllocTest4 - special addPage interface " + cid2);


                ////////////////////////////////////////////////////////
                // next test preallocate interface
                ////////////////////////////////////////////////////////			
                long cid3 = t_util.t_addContainer(t, 0, 1024);
                ContainerHandle c3 = t_util.t_openContainer(t, 0, cid3, true);

                // now preallocate 10 pages
                c3.preAllocate(10);

                p1 = c3.getFirstPage();
                if (p1.getPageNumber() != ContainerHandle.FIRST_PAGE_NUMBER)
                    throw T_Fail.testFailMsg("expect first page to have FIRST_PAGE_NUMBER");
                p1.unlatch();

                if (c3.getNextPage(ContainerHandle.FIRST_PAGE_NUMBER) != null)
                    throw T_Fail.testFailMsg("expect to have only 1 page allocated");

                REPORT("AllocTest4 - preallocate interface " + cid3);

                PASS("AllocTest4 ");

            }
            finally
            {
                SanityManager.DEBUG_CLEAR(AllocPage.TEST_MULTIPLE_ALLOC_PAGE);
                t_util.t_commit(t);
                t.close();
            }
        }
	}

	protected void AllocTest5() throws StandardException, T_Fail
	{
		// first create 10 1/2 filled pages with various degree of fillness
		Transaction t = t_util.t_startTransaction();

		try
		{
			long cid = t_util.t_addContainer(t, 0, 1024, 0, 90, false);
			ContainerHandle c = t_util.t_openContainer(t, 0, cid, true);
			Page p;

			// the number of rows that is expected to fit into one page
			// secret raw store calculation for 1 column rows
			int numRows = (1024-60)/(95+8);

			T_RawStoreRow rows[] = new T_RawStoreRow[numRows];

			for (int j = 0; j < numRows; j++)
				rows[j] = new T_RawStoreRow("row " + j);

			for (int i = 0; i < numRows; i++)
			{
				p = t_util.t_addPage(c);

				// validate allocation cache by getting the first page
				t_util.t_getPage(c, 1).unlatch();

				// insert different number of rows into these pages
				for (int j = 0; j <= i; j++)
				{
					if (t_util.t_insert(p, rows[j]) == null)
						throw T_Fail.testFailMsg("failed to insert " + (j+1) +
												 " rows into page " + p);
				}

				p.unlatch();
			}

			// page 1 has 0 row
			// page 2 has 1 row
			// page 3 has 2 rows
			// page 4 has 3 rows
			// page 5 has 4 rows
			// page 6 has 5 rows (filled)
			// page 7 has 6 rows (filled)
			// page 8 has 7 rows (filled)
			// page 9 has 8 rows (filled)
			// page 10 has 9 rows (filled)


			// these pages should be accounted for correctly because each
			// subsequent page has > 1/8 for all the records in the container

			// now go thru and use up all the space
			p =  c.getPageForInsert(0);
			if (p != null)
				throw T_Fail.testFailMsg("Expect last page to be full");

			// now go thru and use up all the space - since we skipped page 1
			// on the first loop, it won't know it is a 1/2 filled page.
			for (int i = 2; i < 6; i++)
			{
				p = c.getPageForInsert(ContainerHandle.GET_PAGE_UNFILLED);
				if (p == null)
					throw T_Fail.testFailMsg("Expect next unfilled page to be " + i);

				if (p.getPageNumber() != i)
					throw T_Fail.testFailMsg("Expect next unfilled page to be "
											 + i + ", it is " + p.getPageNumber());

				t_util.t_insert(p, rows[i]);
				p.unlatch();

				// we should keep getting the same page back until it is full
				while ((p = c.getPageForInsert(0)) != null)
				{
					if (p.getPageNumber() != i)
						throw T_Fail.testFailMsg("Don't expect page number to change from " +
												 i + " to " + p.getPageNumber());
					t_util.t_insert(p, rows[i]);
					p.unlatch();
				}

			}
		
			p = c.getPageForInsert(ContainerHandle.GET_PAGE_UNFILLED);
			if (p != null)
				throw T_Fail.testFailMsg("don't expect any more pages to be found");

		}
		finally
		{
			t_util.t_commit(t);
			t.close();
		}
		PASS("AllocTest5 ");

	}

	/*
	 * MT tests on  the same container
	 */
	protected void AllocMTest1(long cid) throws StandardException, T_Fail 
	{
        if (SanityManager.DEBUG)
        {
            SanityManager.DEBUG_SET(AllocPage.TEST_MULTIPLE_ALLOC_PAGE);

            // each thread will add N pages and remove N pages and still finds
            // its own pages.  Do that serveral times.
            int N = 20;

            RecordHandle rh[] = new RecordHandle[N];

            Transaction t = t_util.t_startTransaction();

            try
            {
                T_RawStoreRow row = new T_RawStoreRow(REC_002);
                ContainerHandle c;
                Page p;

                for (int iteration = 0; iteration < 5; iteration++)
                {
                    for (int i = 0; i < N; i++)
                    {
                        c = t_util.t_openContainer(t, 0, cid, true);

                        p = t_util.t_addPage(c);
                        rh[i] = t_util.t_insert(p, row);
                        p.unlatch();

                        t_util.t_commit(t);
                    }

                    for (int i = 0; i < N; i++)
                    {
                        c = t_util.t_openContainer(t, 0, cid, true);
                        t_util.t_checkFetch(c, rh[i], REC_002);

                        t.setSavePoint(SP1, null);

                        p = t_util.t_getPage(c, rh[i].getPageNumber());
                        t_util.t_removePage(c, p);

                        if ((iteration%3) == 1)
                        {
                            t.rollbackToSavePoint(SP1, null);
                        }

                        // sometimes commit sometimes abort
                        if (iteration % 2 == 0)
                            t_util.t_abort(t);
                        else
                            t_util.t_commit(t);
                    }

                    // if I aborted, remove them now
                    if ((iteration % 2) == 0 ||
                        (iteration % 3) == 1)
                    {
                        for (int i = 0; i < N; i++)
                        {
                            c = t_util.t_openContainer(t, 0, cid, true);
                            t_util.t_checkFetch(c, rh[i], REC_002);

                            p = t_util.t_getPage(c, rh[i].getPageNumber());
                            t_util.t_removePage(c, p);
                            t_util.t_commit(t);
                        }
                    }

                    // at any given time, there should be <= (N*numthread)+1 pages
                    int max = (N*getNumThreads())+1;

                    c = t_util.t_openContainer(t, 0, cid, false);
                    long pnum = 0;
                    int countPages = 0;

                    for (p = c.getFirstPage();
                         p != null;
                         p = c.getNextPage(pnum))
                    {
                        countPages++;
                        pnum = p.getPageNumber();
                        p.unlatch();
                        t_util.t_commit(t);	// release container lock

                        c = t_util.t_openContainer(t, 0, cid, false);
                    }

                    t_util.t_commit(t);	// release container lock

                    if (countPages > max)
                        throw T_Fail.testFailMsg("some pages may not be reused, expect " +
                                                 max + " got " + countPages);
                    else
                        REPORT("AllocMTest1 got " + countPages );
                }
                
            }
            finally
            {
                SanityManager.DEBUG_CLEAR(AllocPage.TEST_MULTIPLE_ALLOC_PAGE);
                t_util.t_commit(t);
                t.close();
            }

            PASS("AllocMTest1");
        }
        else
        {
			REPORT("AllocMTest1 cannot be run on an insane server");
			return;
        }
	}
    
    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ContextService    getContextService()
    {
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getFactory();
        }
        else
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
