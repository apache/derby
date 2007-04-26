/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_Util

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

import org.apache.derby.iapi.store.raw.*;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.reference.Property;

// impl imports are the preferred way to create unit tests.
import org.apache.derbyTesting.unitTests.harness.T_MultiThreadedIterations;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.locks.*;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.shared.common.sanity.AssertFailure;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.types.DataValueDescriptor;


import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.error.ExceptionSeverity;
import java.io.*;
import java.util.Properties;
import org.apache.derby.iapi.types.SQLChar;


/*
  Utility class to help test raw store functionality.  

  If you write a raw store unit test, be that a protocol test or an
  implementation test, and find youself needing to do certain operations over
  and over again, chances are that functionality is either in here or should be
  added.  This class is here entirely for the convenience of people writing
  unit tests for the RawStore.
*/
public class T_Util
{ 

	RawStoreFactory	rsFactory;
	LockFactory  lFactory;
	ContextService csFactory;

	private int		openContainerMode;	// mode flags used in openContainer

	public T_Util(RawStoreFactory rsf, LockFactory lf, 
				  ContextService csf)
	{
		rsFactory = rsf;
		lFactory = lf;
		csFactory = csf;

		openContainerMode = 0; // logged by default
	}

	public void setOpenMode(int newMode) {
		openContainerMode = newMode;
	}

	/*
	 * function that checks for a condition, throws T_Fail exception if the condition
	 * is not met.
	 */

	/*
	 * check that transaction does not hold any lock
	 */
	public void t_checkNullLockCount(Transaction t) throws T_Fail {
		if (lFactory.areLocksHeld(t.getCompatibilitySpace()))
			throw T_Fail.testFailMsg("Previous action did not clean up all locks.");
	}

	/*
	 * check that page number on the page matches the input page number
	 */
	public static void t_checkPageNumber(Page page, long pageNumber) throws T_Fail {
		if (page.getPageNumber() != pageNumber)
			throw T_Fail.testFailMsg("page number expected to be " + pageNumber + ", is " +
				page.getPageNumber());
	}

	/*
	 * check that the number of record on the page matches input.  
	 * @param page the page in question
	 * @param count the total number of record - this include deleted as well as non-deleted
	 * @param nonDeleted the number of non-deleted record
	 */
	public static void t_checkRecordCount(Page page, int count, int nonDeleted) throws T_Fail, StandardException {
		if (page.recordCount() != count)
			throw T_Fail.testFailMsg("recordCount() expected to be " + count + ", is " + page.recordCount());

		if (page.nonDeletedRecordCount() != nonDeleted)
			throw T_Fail.testFailMsg("nonDeletedRecordCount() expected to be " + nonDeleted + ", is " + page.nonDeletedRecordCount());
	}

	/*
	 * check the number of fields in the slot
	 */
	public static void t_checkFieldCount(Page page, int slot, int count) throws T_Fail, StandardException {
		if (page.fetchNumFieldsAtSlot(slot) != count)
			throw T_Fail.testFailMsg("number of fields at slot " + slot + " expected to be " + count
									 + ", is " + page.fetchNumFieldsAtSlot(slot));
	}

	/**
		Fetch a record that is expected to exist using a record handle.
		The record has a T_RawStoreRow of 1 column and this column as value as
		specified by data, which could be null.

		Calls recordExists() before fetch to ensure that the record
		is there.

		@param page the page in question
		@param rh the record handle
		@param data the string value that is expected in the row

		@exception T_Fail Implementation failed expectation
		@exception StandardException Unexpected exception from the implementation

		@see Page#recordExists
	*/
	public static void t_checkFetch(Page page, RecordHandle rh, String data, int stringLen)
		throws T_Fail, StandardException {

		t_checkFetch(page, rh, T_Util.getStringFromData(data, stringLen));
	}
	
	public static void t_checkFetch(Page page, RecordHandle rh, String data)
		throws T_Fail, StandardException {

		if (!page.recordExists(rh, false))
			throw T_Fail.testFailMsg("Record does not exist");

		T_RawStoreRow readRow = new T_RawStoreRow((String) null);

		int slot = page.getSlotNumber(rh);

		RecordHandle rhf = 
            page.fetchFromSlot(
                rh, slot, readRow.getRow(), 
                (FetchDescriptor) null,
                false);

		if (rhf == null)
			throw T_Fail.testFailMsg("Failed to read record");

		if ((data == null) || readRow.getStorableColumn(0).isNull()) {

			if ((data == null) && readRow.getStorableColumn(0).isNull())
				return;

			throw T_Fail.testFailMsg("Record's value incorrect");
		}

		if (!readRow.toString().equals(data))
			throw T_Fail.testFailMsg("Record's value incorrect, expected :" + data + ": - got :" + readRow.toString());
	}

	/**
		Fetch a record from a container that is expected to exist using a record handle.
		Calls recordExists() before fetch to ensure that the record
		is there.

		@exception T_Fail Implementation failed expectation
		@exception StandardException Unexpected exception from the implementation

		@see Page#recordExists
	*/
	public void t_checkFetch(ContainerHandle c, RecordHandle rh, String data)
		throws T_Fail, StandardException {

		Page page = t_getPage(c, rh.getPageNumber());

		try
		{
			t_checkFetch(page, rh, data);
		}
		finally
		{
			page.unlatch();
		}
	}

	/**
		Check to make sure record is NOT there

		@exception T_Fail Implementation failed expectation
		@exception StandardException Unexpected exception from the implementation
	 */
	public void t_checkFetchFail(ContainerHandle c, RecordHandle rh)
		throws T_Fail, StandardException 
	{
		Page page = t_getPage(c, rh.getPageNumber());

		try
		{
			if (page.recordExists(rh, true))
				throw T_Fail.testFailMsg("Record Exists");
		}
		finally
		{
			page.unlatch();
		}
	}

	/**
		Fetch a deleted record from a container using a record handle.

		@exception T_Fail Implementation failed expectation
		@exception StandardException Unexpected exception from the implementation

		@see Page#recordExists
		@see Page#fetchFromSlot
	*/
	public void t_checkFetchDeleted(ContainerHandle c, RecordHandle rh, 
									String data)
		throws T_Fail, StandardException 
	{
		Page p = t_getPage(c, rh.getPageNumber());
		if (p == null)
			throw T_Fail.testFailMsg("Page not found " + rh);

		T_RawStoreRow readRow = new T_RawStoreRow((String) null);

		try
		{
			int slot = p.getSlotNumber(rh);
			if (p.fetchFromSlot(
                    rh, slot, readRow.getRow(), 
                    (FetchDescriptor) null,
                    false) != null)
            {
				throw T_Fail.testFailMsg(
                    "Record at slot " + slot + " not deleted");
            }
		}
		finally
		{
			p.unlatch();
		}
	}


	/*
		Fetch a record that is expected to exist using a record handle.
		The record contains the values in the passed in row, which is a
		T_RawStoreRow.  A T_RawStoreRow of the same number of columns will be made and fetched
		from the page and compared with the passed in row.

	*/
	public static void t_checkFetch(Page page, RecordHandle rh, T_RawStoreRow row)
		throws T_Fail, StandardException 
	{
		if (!page.recordExists(rh, false))
			throw T_Fail.testFailMsg("Record does not exist");

		// try to fetch the same number of columns as the passed in row
		int ncol = row.nColumns();
		T_RawStoreRow readRow = new T_RawStoreRow(ncol);
		for (int i = 0; i < ncol; i++)
			readRow.setColumn(i, (String) null);

		RecordHandle rhf = page.fetchFromSlot(rh, page.getSlotNumber(rh),
											  readRow.getRow(), null, false);
		if (rhf == null)
			throw T_Fail.testFailMsg("Failed to read record");
		if (!readRow.toString().equals(row.toString()))
			throw T_Fail.testFailMsg("Record's value incorrect, expected :" +
									 row.toString() + ": - got :" + readRow.toString());
	}


	/*
	    Using sparse row representation:
		Fetch a column of a record that is expected to exist, using a record 
		handle and a FormatableBitSet object.
		Check that column colNum has value data.
	*/
	public static void t_checkFetchCol(Page page, RecordHandle rh, int colNum,
									   int numCols, String data)
		throws T_Fail, StandardException 
	{
		if (!page.recordExists(rh, false))
			throw T_Fail.testFailMsg("Record does not exist");

		T_RawStoreRow readRow = new T_RawStoreRow(numCols);
		for (int i = 0; i < numCols; i++)
			readRow.setColumn(i, (String) null);
		FormatableBitSet colList = new FormatableBitSet(numCols);
		colList.set(colNum);
		FetchDescriptor desc = new FetchDescriptor(numCols, colList, null);

		RecordHandle rhf = page.fetchFromSlot(rh, page.getSlotNumber(rh),
											  readRow.getRow(), desc, false);
		if (rhf == null)
			throw T_Fail.testFailMsg("Failed to read record");
		String col = readRow.getStorableColumn(colNum).toString();
		if (!col.equals(data))
			throw T_Fail.testFailMsg("Record's value for column " + colNum +
									 " incorrect, expected :" + data +
									 ": - got :" + readRow.toString());
	}


	/*
	 * the following is a sequence of fetches, fetching the first row, fetching
	 * the next and previous rows, and fetching the last row in the page.
	 *
	 * The row is assumed to be a T_RawStoreRow with 1 column, which value is the
	 * string specified in data.
	 */

	/*
	 * fetch and check the first row in the page.  
	 * Return the first row's recordHandle. 
	 */
	public static RecordHandle t_checkFetchFirst(Page page, String data)
		throws T_Fail, StandardException {
		T_RawStoreRow readRow = new T_RawStoreRow((String) null);

        int slot = 0;
        while (page.isDeletedAtSlot(slot))
        {
            slot++;
        }

		RecordHandle rhf = 
            page.fetchFromSlot(
                (RecordHandle) null, slot, 
                readRow.getRow(), 
                (FetchDescriptor) null,
                false);

		if (rhf == null)
			throw T_Fail.testFailMsg("Failed to read record");
		if (!readRow.toString().equals(data))
			throw T_Fail.testFailMsg("Record's value incorrect, expected :" + data + ": - got :" + readRow.toString());

		return rhf;
	}

	/*
	 * Fetch and check the next (next to rh) row in the page.
	 * Return the next row's recordHandle
	 */
	public static RecordHandle t_checkFetchNext(Page page, RecordHandle rh, String data)
		throws T_Fail, StandardException {

		if (!page.recordExists(rh, false))
			throw T_Fail.testFailMsg("Record does not exist");

		T_RawStoreRow readRow = new T_RawStoreRow((String) null);

        int slot = page.getSlotNumber(rh) + 1;
        while (page.isDeletedAtSlot(slot))
        {
            slot++;
        }

		RecordHandle rhf = 
            page.fetchFromSlot(
                (RecordHandle) null, 
                slot,
                readRow.getRow(), 
                (FetchDescriptor) null,
                false);

		if (rhf == null)
			throw T_Fail.testFailMsg("Failed to read record");
		if (!readRow.toString().equals(data))
			throw T_Fail.testFailMsg("Record's value incorrect, expected :" + data + ": - got :" + readRow.toString());

		return rhf;
	}
	
	/*
	 * Fetch and check the previous (previous to rh) row in the page.
	 * Return the previous row's recordHandle
	 */
	public static RecordHandle t_checkFetchPrevious(Page page, RecordHandle rh, String data)
		throws T_Fail, StandardException {

		if (!page.recordExists(rh, false))
			throw T_Fail.testFailMsg("Record does not exist");

		T_RawStoreRow readRow = new T_RawStoreRow((String) null);

        int slot = page.getSlotNumber(rh) - 1;

        while (page.isDeletedAtSlot(slot) && slot >= 0)
        {
            slot--;
        }

        if (slot == -1)
            return(null);


		RecordHandle rhf = 
            page.fetchFromSlot(
                (RecordHandle) null, 
                slot,
                readRow.getRow(), 
                (FetchDescriptor) null,
                false);

		if (rhf == null)
			throw T_Fail.testFailMsg("Failed to read record");
		if (!readRow.toString().equals(data))
			throw T_Fail.testFailMsg("Record's value incorrect, expected :" + data + ": - got :" + readRow.toString());

		return rhf;
	}

	/*
	 * Fetch and check the last row in the page.
	 * Return the last row's recordHandle
	 */
	public static RecordHandle t_checkFetchLast(Page page, String data)
		throws T_Fail, StandardException {
		T_RawStoreRow readRow = new T_RawStoreRow((String) null);

        int slot = page.recordCount() - 1;
        while (page.isDeletedAtSlot(slot) && slot >= 0)
        {
            slot--;
        }

        if (slot == -1)
            return(null);

		RecordHandle rhf = 
            page.fetchFromSlot(
                (RecordHandle) null, 
                slot,
                readRow.getRow(),
                (FetchDescriptor) null,
                false);

		if (rhf == null)
			throw T_Fail.testFailMsg("Failed to read record");
		if (!readRow.toString().equals(data))
			throw T_Fail.testFailMsg("Record's value incorrect, expected :" + data + ": - got :" + readRow.toString());

		return rhf;
	}


	/*
	 * Fetch and check the slot on the page.  
	 *
	 * The slot number is NOT a stable reference once the page is unlatched,
	 * this check is only valid if you know the page has not been unlatched
	 * since you put the row in, or you know nobody has touched the page since
	 * you determined the slot number
	 *
	 * The slot refers to a row in the page which has a T_RawStoreRow of 1 column, the
	 * column has the value of data input.
	 *
	 * @param page the page in question
	 * @param slot the slot number (see above)
	 * @param data the column value
	 * @param deleted if the row is deleted, set to true
	 * @param forUpdate If you want to lock the row for update, set forUpdate to true.
	 *
	 */
	public static void t_checkFetchBySlot(Page page, int slot, 
									String data, boolean deleted, 
									boolean forUpdate)
		throws T_Fail, StandardException 
	{
		T_RawStoreRow readRow = new T_RawStoreRow((String) null);
		RecordHandle rh = 
            page.fetchFromSlot(
                (RecordHandle) null, slot, 
                readRow.getRow(),
                (FetchDescriptor) null,
                true);

		if (rh == null)
			throw T_Fail.testFailMsg("Failed to read record");
		if (!readRow.toString().equals(data))
			throw T_Fail.testFailMsg("Record's value incorrect, expected :" + data + ": - got :" + readRow.toString());

		if (page.isDeletedAtSlot(slot) != deleted)
			throw T_Fail.testFailMsg("Record at slot " + slot + " deleted=" +
									 page.isDeletedAtSlot(slot) + ", expect " + deleted);

		// RESOLVE: check locking
	}

	/*
	 * check a column value from a slot on the page
	 *
	 * The slot number is NOT a stable reference once the page is unlatched,
	 * this check is only valid if you know the page has not been unlatched
	 * since you put the row in, or you know nobody has touched the page since
	 * you determined the slot number
	 *
	 * The storable in the specified column put into the input column and it
	 * is check for the same value as the input data 
	 *
	 * @param page the page in question
	 * @param slot the slot number (see above)
	 * @param fieldId the field Id on the row
	 * @param column the storable to put the column in
	 * @param forUpdate true if you want to lock the row for update
	 * @param data the expected value in the column
	 */
	public static void t_checkFetchColFromSlot(Page page,
										 int slot,
										 int fieldId,
										 DataValueDescriptor column,
										 boolean forUpdate,
										 String data,
										 int stringLen)
		 throws StandardException, T_Fail
	{
		t_checkFetchColFromSlot(page, slot, fieldId, column, forUpdate, T_Util.getStringFromData(data, stringLen));
	}
	
	public static void t_checkFetchColFromSlot(
    Page                page,
    int                 slot,
    int                 fieldId,
    DataValueDescriptor column,
    boolean             forUpdate,
    String              data)
		 throws StandardException, T_Fail
	{
        DataValueDescriptor[] fetch_row = new DataValueDescriptor[fieldId + 1];
        fetch_row[fieldId] = column;
        FormatableBitSet validCols = new FormatableBitSet(fieldId + 1);
        validCols.set(fieldId);

		RecordHandle rh =
			page.fetchFromSlot(
                null, slot, fetch_row,
                new FetchDescriptor(
                    fetch_row.length, validCols, (Qualifier[][]) null), 
                true);

		if (rh == null)
			throw T_Fail.testFailMsg("Failed to fetch record: slot "
							 + slot + " field " + fieldId);

		// RESOLVE - how to check rh lock mode?

		if (data == null)
		{
			if (!column.isNull())
				throw T_Fail.testFailMsg("Failed to fetch null column: slot "
								 + slot + " field " + fieldId + " column is " + column);
		}
		else
		{
			if (column.isNull())
				throw T_Fail.testFailMsg("expect non null column, got null: slot "
								 + slot + " field " + fieldId);
			if (!column.toString().equals(data))
				throw T_Fail.testFailMsg("expect " + data + " got " + column.toString()
								 + ": slot " + slot + " field " + fieldId);
		}
	}


	/**
		Take an empty page and check it does actually seem to be empty.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	public static void t_checkEmptyPage(Page page) throws T_Fail, StandardException {

		// check the counts
		t_checkRecordCount(page, 0, 0);

        try
        {
            page.fetchFromSlot(
                (RecordHandle) null, 0, null, 
                (FetchDescriptor) null,
                false);

            throw T_Fail.testFailMsg(
                "fetchFromSlot() must throw exception on fetch from slot 0 on an empty page");
        }
        catch (StandardException se)
        {
            // expected exception.
        }

		// check we can't get a record handle. NB here we are guessing that 0
		// and RecordHandle.FIRST_RECORD_ID might be valid record identifiers,
		// nothing in the API states that they will be.  Eother way we
		// shouldn't get a valid RecordHandle back.
		if (page.getRecordHandle(0) != null)
			throw T_Fail.testFailMsg("obtained a RecordHandle for an empty page");

		if (page.getRecordHandle(RecordHandle.FIRST_RECORD_ID) != null)
			throw T_Fail.testFailMsg("obtained a RecordHandle for an empty page");
		
		// should be no aux object
		if (page.getAuxObject() != null)
			throw T_Fail.testFailMsg("empty page has an aux object");

		t_readOnlySlotOutOfRange(page, Page.FIRST_SLOT_NUMBER);

		if (!page.spaceForInsert())
			throw T_Fail.testFailMsg("spaceForInsert() returned false on an empty page");
	}

	/*
		Check to see the correct behaviour for read only operations
		that take a slot when the slot is out of range.
	*/
	public static void t_readOnlySlotOutOfRange(Page page, int slot) throws T_Fail, StandardException {

		try {
			page.fetchFromSlot(
                (RecordHandle) null, slot, 
                new DataValueDescriptor[0], 
                (FetchDescriptor) null,
                true);

			throw T_Fail.testFailMsg("fetchFromSlot succeeded on out of range slot " + slot);
		} catch (StandardException se0) {
			// Statement exception expected, throw if not a statement exception.
            if (se0.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
                throw se0;
		}
		try {
			page.isDeletedAtSlot(slot);
			throw T_Fail.testFailMsg("isDeletedAtSlot succeeded on out of range slot " + slot);
		} catch (StandardException se2) {
			// Statement exception expected, throw if not a statement exception.
            if (se2.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
                throw se2;
		}
	}

	/*
		Check to see the correct behaviour for update operations
		that take a slot when the slot is out of range.
	*/
	public static void t_updateSlotOutOfRange(Page page, int slot) throws T_Fail, StandardException {

		try {
			page.deleteAtSlot(slot, false, (LogicalUndo)null);
			throw T_Fail.testFailMsg("deleteAtSlot succeeded on out of range slot " + slot);
		} catch (StandardException se0) {
			// Statement exception expected, throw if not a statement exception.
            if (se0.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
                throw se0;
		}
		try {
			page.deleteAtSlot(slot, true, (LogicalUndo)null);
			throw T_Fail.testFailMsg("deleteAtSlot succeeded on out of range slot " + slot);
		} catch (StandardException se0) {
			// Statement exception expected, throw if not a statement exception.
            if (se0.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
                throw se0;
		}

		T_RawStoreRow row = new T_RawStoreRow((String) null);

		// insert at the last slot will succeed, so don't do it.
		if (page.recordCount() != slot) {
			try {			
					page.insertAtSlot(slot, row.getRow(), (FormatableBitSet) null, (LogicalUndo)null,
						Page.INSERT_DEFAULT, 100);
					throw T_Fail.testFailMsg("insertAtSlot succeeded, on out of range slot " + slot);
            } catch (StandardException se0) {
                // Statement exception expected, throw if not a statement exception.
                if (se0.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
                    throw se0;
            }
		}

		try {			
			page.updateAtSlot(slot, row.getRow(), (FormatableBitSet) null);
			throw T_Fail.testFailMsg("updateAtSlot succeeded on out of range slot " + slot);
        } catch (StandardException se0) {
            // Statement exception expected, throw if not a statement exception.
            if (se0.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
                throw se0;
        }
	}


	/*
	 * Save point checks
	 */

	/**
		Negative test - check that an invalid savepoint is detected.
	    
		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	public static void t_checkInvalidSavePoint(Transaction t, String name)
		throws T_Fail, StandardException {

		// check a non-existent save point is trapped
		try {
			t.rollbackToSavePoint(name, null);

			throw T_Fail.testFailMsg("non existent save point did not cause exception on rollbackToSavePoint");
		} catch (StandardException se) {
			// we expected this ...
		}
		try {
			t.releaseSavePoint(name, null);
			throw T_Fail.testFailMsg("non existent save point did not cause exception on releaseSavePoint");

		} catch (StandardException se) {
			// we expected this ...
		}
	}

	/* 
	 * same as above, check an invalid savepoint in the given transaction
	 * context
	 */
	public void t_checkInvalidSavePoint(T_TWC ctx, String name)
		throws T_Fail, StandardException {
		csFactory.setCurrentContextManager(ctx.cm);
		try {
		t_checkInvalidSavePoint(ctx.tran, name);
		} finally {
			csFactory.resetCurrentContextManager(ctx.cm);
		}
	}


	/*
	 * function that actually do something, start, commit, abort a trasaction,
	 * get a page, insert a row, etc.
	 */


	/*
		Start a user transaction, ensures that the startTransaction method
		does not return null (which it shouldn't).
	*/
	public Transaction t_startTransaction() 
		throws StandardException, T_Fail {
			
			Transaction t1 = 
                rsFactory.startTransaction(
                    csFactory.getCurrentContextManager(),
					AccessFactoryGlobals.USER_TRANS_NAME);

			if (t1 == null)
				throw T_Fail.testFailMsg("Start a transaction");
			t_checkNullLockCount(t1);
			return t1;
	}

	/*
		Start a user transaction, ensures that the startTransaction method
		does not return null (which it shouldn't).
	*/
	public Transaction t_startGlobalTransaction(
    int     format_id,
    byte[]  global_id,
    byte[]  branch_id) 
		throws StandardException, T_Fail {

			Transaction t1 = 
                rsFactory.startGlobalTransaction(
                    csFactory.getCurrentContextManager(),
                    format_id, global_id, branch_id);

			if (t1 == null)
				throw T_Fail.testFailMsg("Start a transaction");
			t_checkNullLockCount(t1);
			return t1;
	}

	/*
	 * start a user transaction with its own context (T_TWC)
	 */
	public T_TWC t_startTransactionWithContext()
		throws StandardException, T_Fail
	{
		T_TWC ctx = new T_TWC(csFactory, lFactory, rsFactory);
		ctx.startUserTransaction();
		return ctx;
	}

	/*
	 * start an internal transaction
	 */
	public Transaction t_startInternalTransaction() 
		throws StandardException, T_Fail {

			Transaction t1 = rsFactory.startInternalTransaction(csFactory.getCurrentContextManager());

			if (t1 == null)
				throw T_Fail.testFailMsg("Failed to start an internal transaction");
			t_checkNullLockCount(t1);
			return t1;
	}

	/*
	 * commit a transaction
	 */
	public void t_commit(Transaction t) 
		throws StandardException, T_Fail {
		t.commit();
		t_checkNullLockCount(t);
	}

	/*
	 * commit a transaction with context
	 */
	public void t_commit(T_TWC ctx) 
		throws StandardException, T_Fail 
	{
		csFactory.setCurrentContextManager(ctx.cm);
		try {
		t_commit(ctx.tran);
		} finally {
			csFactory.resetCurrentContextManager(ctx.cm);
		}
	}

	/*
	 * close a transaction with context
	 */
	public void t_close(T_TWC ctx)
		throws StandardException, T_Fail 
	{
		ctx.tran.close();
		ctx.tran = null;
		ctx.cm = null;		// no need to close a context ???
	}

	/*
	 * abort a transaction
	 */
	public void t_abort(Transaction t) 
		throws StandardException, T_Fail {
		t.abort();
		t_checkNullLockCount(t);
	}

	/*
	 * abort a transaction with context
	 */
	public void t_abort(T_TWC ctx) 
		throws StandardException, T_Fail 
	{
		csFactory.setCurrentContextManager(ctx.cm);
		try {
		t_abort(ctx.tran);
		} finally {
			csFactory.resetCurrentContextManager(ctx.cm);
		}
	}

	/**
		Add a new container in the transaction

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	public long t_addContainer(Transaction t, long segmentId)
		throws StandardException, T_Fail {
		
		long cid = 
            t.addContainer(
                segmentId, ContainerHandle.DEFAULT_ASSIGN_ID, 
                ContainerHandle.MODE_DEFAULT, (Properties) null, 0);

		if (cid < 0)
			throw T_Fail.testFailMsg("add container");

		return cid;		
	}

	public long t_addContainer(T_TWC ctx, long segmentId)
		throws StandardException, T_Fail 
	{
		csFactory.setCurrentContextManager(ctx.cm);
		try {
		return t_addContainer(ctx.tran, segmentId);
		} finally {
			csFactory.resetCurrentContextManager(ctx.cm);
		}
	}

	/**

		Add a new container in the transaction with a specified page size

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	public long t_addContainer(Transaction t, long segmentId, int pageSize)
		throws StandardException, T_Fail {

		Properties tableProperties = new Properties();
		tableProperties.put(Property.PAGE_SIZE_PARAMETER, Integer.toString(pageSize));
		
		long cid = 
            t.addContainer(
                segmentId, ContainerHandle.DEFAULT_ASSIGN_ID, 
                ContainerHandle.MODE_DEFAULT, tableProperties, 0);

		if (cid < 0)
			throw T_Fail.testFailMsg("add container");

		return cid;		
	}

	public long t_addContainer(T_TWC ctx, long segmentId, int pageSize)
		throws StandardException, T_Fail {

		csFactory.setCurrentContextManager(ctx.cm);
		try {
		return t_addContainer(ctx.tran, segmentId, pageSize);
		} finally {
			csFactory.resetCurrentContextManager(ctx.cm);
		}
	}

	public long t_addContainer(Transaction t, long segmentId, Properties tableProperties)
		throws StandardException, T_Fail {

		long cid = 
            t.addContainer(
                segmentId, ContainerHandle.DEFAULT_ASSIGN_ID, 
                ContainerHandle.MODE_DEFAULT, tableProperties, 0);

		if (cid < 0)
			throw T_Fail.testFailMsg("add container");

		return cid;		
	}
				

	/**

		Add a new container in the transaction with specified 
        pageSize, spareSpace, minimumRecordSize, and reusableRecordId

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	public long t_addContainer(Transaction t, long segmentId, int pageSize, int spareSpace, 
			int minimumRecordSize, boolean reusableRecordId)
		throws StandardException, T_Fail {

		Properties tableProperties = new Properties();
		tableProperties.put(Property.PAGE_SIZE_PARAMETER, Integer.toString(pageSize));
		tableProperties.put(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER, Integer.toString(spareSpace));
		tableProperties.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER, Integer.toString(minimumRecordSize));

        if (reusableRecordId) {
            tableProperties.put(RawStoreFactory.PAGE_REUSABLE_RECORD_ID, "true");
        }
		
		long cid = 
            t.addContainer(
                segmentId, ContainerHandle.DEFAULT_ASSIGN_ID, 
                ContainerHandle.MODE_DEFAULT, tableProperties, 0);

		if (cid < 0)
			throw T_Fail.testFailMsg("add container");

		return cid;		
	}

	public long t_addContainer(T_TWC ctx, long segmentId, int pageSize, int spareSpace, int minimumRecordSize)
		throws StandardException, T_Fail {
		csFactory.setCurrentContextManager(ctx.cm);
		try {
		return t_addContainer(ctx.tran, segmentId, pageSize, spareSpace, minimumRecordSize, false);
		} finally {
			csFactory.resetCurrentContextManager(ctx.cm);
		}
	}

	/**
		Open a container.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	
	public ContainerHandle t_openContainer(Transaction t, long segmentId, long containerId, boolean forUpdate)
		throws StandardException, T_Fail 
	{
		ContainerKey id = new ContainerKey(segmentId, containerId);
		ContainerHandle c = t.openContainer(id,
			forUpdate ? (ContainerHandle.MODE_FORUPDATE | openContainerMode) : ContainerHandle.MODE_READONLY);
		if (c == null)
			throw T_Fail.testFailMsg("ContainerHandle failed to open: (" +
									 segmentId + "," + containerId + ")");

		return c;
	}
	public ContainerHandle t_openContainer(T_TWC ctx, long segmentId, long containerId, boolean forUpdate)
		throws StandardException, T_Fail 
	{
		csFactory.setCurrentContextManager(ctx.cm);
		try {
			return t_openContainer(ctx.tran, segmentId, containerId, forUpdate);
		} finally {
			csFactory.resetCurrentContextManager(ctx.cm);
		}
	}

	/**
		Drop a container 

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	public void t_dropContainer(Transaction t, long segmentId, long containerId) 
		 throws StandardException, T_Fail
	{
		t.dropContainer(new ContainerKey(segmentId, containerId));
	}

	/**
		Get the last page in a container.
		Always returns a valid page or null if there is no page in the container.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	public Page t_getLastPage(ContainerHandle c) throws T_Fail, StandardException {

		Page page = c.getFirstPage();
		if (page != null)
		{
			Page nextPage;
			while((nextPage = c.getNextPage(page.getPageNumber())) != null)
			{
				page.unlatch();
				page = nextPage;
			}
		}

		return page;
	}


	/**
		Get a specific page in a container.
		Always returns a valid page.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	public Page t_getPage(ContainerHandle c, long pageNumber) throws T_Fail, StandardException {

		Page page = c.getPage(pageNumber);
		if (page == null)
			throw T_Fail.testFailMsg("fail to get page " + pageNumber + " from container " + c);

		if (page.getPageNumber() != pageNumber)
			throw T_Fail.testFailMsg("page expected to have page number " +
				pageNumber + ", has " + page.getPageNumber() + " Container " + c);

		return page;
	}

	/**
		Add a page to a container.

		@exception T_Fail Unexpected behaviour from the API
		@exception StandardException Unexpected exception from the implementation
	*/
	public Page t_addPage(ContainerHandle c) throws T_Fail, StandardException {

		Page page = c.addPage();

		if (page == null)
			throw T_Fail.testFailMsg("addPage() returned null");

		return page;
	}

	/**
		Remove a page from a container.

		@exception T_Fail Record handle returned is null.
		@exception StandardException Unexpected exception from the implementation
	*/
	public void t_removePage(ContainerHandle c, Page p) throws T_Fail, StandardException
	{
		long pnum = p.getPageNumber();
		c.removePage(p);

		// we should not be able to get this page 
		Page badp = c.getPage(pnum);
		if (badp != null)
			throw T_Fail.testFailMsg("got a deallcated page back");
	} 

	/**
	 * Check that it's not possible to get a page which is latched.
	 *
	 * @param c a container handle
	 * @param pageNumber the page number to check
	 * @exception StandardException if an unexpected error occurs
	 * @exception T_Fail if the test fails
	 */
	public void t_checkGetLatchedPage(ContainerHandle c, long pageNumber)
			throws StandardException, T_Fail {
		// we expect to hang in getPage() so make sure we are interrupted
		final Thread me = Thread.currentThread();
		Runnable r = new Runnable() {
				public void run() {
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) { }
					me.interrupt();
				}
			};
		Thread interrupter = new Thread(r);
		interrupter.start();

		try {
			Page p = c.getPage(pageNumber);
			throw T_Fail.testFailMsg("got latched page");
		} catch (StandardException se) {
			// expect thread interrupted exception
			if (!se.getMessageId().equals("08000")) {
				throw se;
			}
		} catch (AssertFailure af) {
			// When running in sane mode, an AssertFailure will be thrown if we
			// try to double latch a page.
			if (!(SanityManager.DEBUG &&
				  af.getMessage().endsWith("Attempted to latch page twice"))) {
				throw af;
			}
		}

		try {
			interrupter.join();
		} catch (InterruptedException ie) { }
	}

	/**
		Call page.insert() and ensure that the return record handle is not null.
		This assumes the caller has called spaceForInsert.

		@exception T_Fail Record handle returned is null.
		@exception StandardException Unexpected exception from the implementation

		@see Page#insert
	*/
	public static RecordHandle t_insert(Page page, T_RawStoreRow row)
		throws T_Fail, StandardException {
		
		RecordHandle rh = page.insert(row.getRow(), (FormatableBitSet) null, Page.INSERT_DEFAULT, 100);

		return rh;
	}

	/**
		Call page.insert() and ensure that the return record handle is not null.
		This assumes the caller has called spaceForInsert.

		@exception T_Fail Record handle returned is null.
		@exception StandardException Unexpected exception from the implementation

		@see Page#insert
	*/
	public static RecordHandle t_insertAtSlot(Page page, int slot, T_RawStoreRow row)
		throws T_Fail, StandardException {
		
		RecordHandle rh = page.insertAtSlot(slot, row.getRow(), (FormatableBitSet) null,
			(LogicalUndo) null, Page.INSERT_DEFAULT, 100);

		return rh;
	}

	/**
		Call page.insert() and ensure that the return record handle is not null.
		This assumes the caller has called spaceForInsert.

		@exception T_Fail Record handle returned is null.
		@exception StandardException Unexpected exception from the implementation

		@see Page#insert
	*/
	public static RecordHandle t_insertAtSlot(Page page, int slot, T_RawStoreRow row, byte insertFlag)
		throws T_Fail, StandardException {
		
		RecordHandle rh = page.insertAtSlot(slot, row.getRow(), (FormatableBitSet) null,
			(LogicalUndo) null, insertFlag, 100);

		return rh;
	}

	/**
		Call page.insert() and ensure that the return record handle is not null.
		This assumes the caller has called spaceForInsert.

		@exception T_Fail Record handle returned is null.
		@exception StandardException Unexpected exception from the implementation

		@see Page#insert
	*/
	public static RecordHandle t_insertAtSlot(Page page, int slot, T_RawStoreRow row, byte insertFlag,
			int overflowThreshold) throws T_Fail, StandardException {
		
		RecordHandle rh = page.insertAtSlot(slot, row.getRow(), (FormatableBitSet) null,
			(LogicalUndo) null, insertFlag, overflowThreshold);

		return rh;
	}

	/**
		Insert a record on the last page, if the row doesn't fit on the
		last page create a new page and insert there.

		@exception T_Fail Record handle returned is null.
		@exception StandardException Unexpected exception from the implementation

		@see Page#insert
	*/
	public RecordHandle t_insert(ContainerHandle c, T_RawStoreRow row)
		throws T_Fail, StandardException {

		Page page = c.getPageForInsert(0);
		boolean addedPage = false;

		if (page == null)
		{
			page = t_addPage(c);
			addedPage = true;
		}
		else if (!page.spaceForInsert(row.getRow(), (FormatableBitSet) null, 100)) {
			page.unlatch();
			page = t_addPage(c);
			addedPage = true;
		}

		RecordHandle rh = t_insert(page, row);
		page.unlatch();

		if (rh == null) {
			if (addedPage)
				throw T_Fail.testFailMsg("insert returned null on an empty page");

			page = t_addPage(c);
			rh = t_insert(page, row);
			page.unlatch();
		}
		return rh;
	}

	/**
		Update a record.

		@exception T_Fail Record handle returned is null.
		@exception StandardException Unexpected exception from the implementation

		@see Page#updateAtSlot
	*/
	public void t_update(ContainerHandle c, RecordHandle rh, T_RawStoreRow row)
		 throws T_Fail, StandardException
	{
		Page page = t_getPage(c, rh.getPageNumber());
		try
		{
			int slot = page.getSlotNumber(rh);
			if (page.updateAtSlot(slot, row.getRow(), null) == null)
				throw T_Fail.testFailMsg("update failed");

			t_checkFetch(page, rh, row);
		}
		finally
		{
			page.unlatch();
		}
	}



	/**
	    Using sparse representation:
		Update a column of a record and check resulting value.

		@exception T_Fail Record handle returned is null.
		@exception StandardException Unexpected exception from the implementation

		@see Page#updateAtSlot
	*/
	public void t_checkUpdateCol(Page page, RecordHandle rh, int colNum, int
								 numCols, String data)
		 throws T_Fail, StandardException
	{
		if (!page.recordExists(rh, false))
			throw T_Fail.testFailMsg("Record does not exist");

		T_RawStoreRow writeRow = new T_RawStoreRow(numCols);
		for (int i = 0; i < numCols; i++)	
			writeRow.setColumn(i, (String) null);
		writeRow.setColumn(colNum, data);
		FormatableBitSet colList = new FormatableBitSet(numCols);
		colList.set(colNum);

		int slot = page.getSlotNumber(rh);
		if (page.updateAtSlot(slot, writeRow.getRow(), colList) == null)
			throw T_Fail.testFailMsg("update failed");
		
		t_checkFetchCol(page, rh, colNum, numCols, data);
	}

	/**
		Check to make sure a row (possibly with overflow) is of the correct length

		@exception T_Fail Record handle returned is null.
		@exception StandardException Unexpected exception from the implementation

	 */
	public void t_checkStringLengthFetch(Page page, int slot, int expectedLength) throws T_Fail, StandardException {

		T_RawStoreRow rr = new T_RawStoreRow((String) null);

		page.fetchFromSlot(
            (RecordHandle) null, slot, rr.getRow(), 
            (FetchDescriptor) null,
            true);

		String s = ((SQLChar) (rr.getStorableColumn(0))).getString();


		if ((s == null) && (expectedLength < 0))
			return;

		if ((s != null) && (expectedLength < 0))
			throw T_Fail.testFailMsg("Expected null string, fetched one of length " + s.length());

		if (s == null)
			throw T_Fail.testFailMsg("Expected string length " + expectedLength + " got null string");

		if (s.length() != expectedLength)
			throw T_Fail.testFailMsg("fetch string length incorrect expected " + expectedLength + " got " + s.length());
	}

	/**
		Lazy people's random file generator:
		Generate a random file with specified name and file size

		@exception T_Fail Record handle returned is null.
	*/
	public void t_genRandomFile(String fileName, String mode, int size) throws T_Fail {

		RandomAccessFile iFile = null;
		try {
			iFile = new RandomAccessFile(fileName, mode);
			for (int i = 0; i < size; i++){
				byte b = (byte) (i & 0xff);
				b = (byte) (((b >= ' ') && (b <= '~')) ? b : ' ');
				iFile.write(b);
			}
			iFile.close();
		} catch (FileNotFoundException fnfe) {
			throw T_Fail.testFailMsg("cannot create new file");
		} catch (IOException ioe) {
			throw T_Fail.testFailMsg("io error, test failed");
		}

	}

	/**
		Return a string of stringLen characters that starts with data
		and is padded with nulls.
	*/
	public static String getStringFromData(String data, int stringLen) {
		char[] ca = new char[stringLen];

		char[] sd = data.toCharArray();

		System.arraycopy(sd, 0, ca, 0, sd.length);
		
		return new String(ca);
	}

	/**
		Make this thread wait a bit, probably for post commit to finish
	 */
	public static void t_wait(int milliSecond)
	{
		Thread.currentThread().yield();
		try
		{
			Thread.currentThread().sleep(milliSecond);
		}
		catch (InterruptedException ie)
		{
		}
	}

	/**
		Add in encryption parameters to the startParam if "testDataEncryption"
		is set to a non-null string.
	 */
	public static Properties setEncryptionParam(Properties startParams)
	{
		// see if we are testing encryption
		String encryptionPassword = 
					PropertyUtil.getSystemProperty("testDataEncryption");
		//look for alternate encryption provider
		String encryptionProvider = 
					PropertyUtil.getSystemProperty("testEncryptionProvider");
		if (encryptionPassword != null)
		{
			if (startParams == null)
				startParams = new Properties();

			startParams.put(Attribute.DATA_ENCRYPTION, "true");
			startParams.put(Attribute.BOOT_PASSWORD, encryptionPassword);
			if (encryptionProvider != null) {
			    startParams.put(Attribute.CRYPTO_PROVIDER, encryptionProvider);
			}

			//			System.out.println("Setting encryption password to " + encryptionPassword);

		}

		return startParams;
	}

}
