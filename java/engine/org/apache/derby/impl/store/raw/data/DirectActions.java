/*

   Derby - Class org.apache.derby.impl.store.raw.data.DirectActions

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.types.DataValueDescriptor;


import java.io.ByteArrayOutputStream;
import java.io.IOException;


public class DirectActions implements PageActions  {

	protected DynamicByteArrayOutputStream outBytes;	
	protected ArrayInputStream	limitIn;
	
	public DirectActions() {
		outBytes = new DynamicByteArrayOutputStream();
		limitIn = new ArrayInputStream();
	}

	public void actionDelete(
    RawTransaction  t, 
    BasePage        page, 
    int             slot, 
    int             recordId, 
    boolean         delete, 
    LogicalUndo     undo)
		throws StandardException
	{
		try {

			page.setDeleteStatus((LogInstant)null, slot, delete);

		} catch (IOException ioe) {

			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}
		
	}

	public int actionUpdate(
    RawTransaction          t, 
    BasePage                page, 
    int                     slot, 
    int                     recordId,
    Object[]   row, 
    FormatableBitSet                 validColumns,
    int                     realStartColumn, 
    DynamicByteArrayOutputStream  logBuffer, 
    int                     realSpaceOnPage, 
    RecordHandle            headRowHandle)
		throws StandardException
	{
		if (logBuffer == null)
			outBytes.reset();
		else
			outBytes = (DynamicByteArrayOutputStream) logBuffer;

		try {

			// manufactures the new row  into outBytes
			int nextColumn = 
                page.logRow(
                    slot, false, recordId, row, validColumns, outBytes, 0,
                    Page.INSERT_OVERFLOW, realStartColumn, 
                    realSpaceOnPage, 100);

			limitIn.setData(outBytes.getByteArray());
			limitIn.setPosition(outBytes.getBeginPosition());
			limitIn.setLimit(outBytes.getPosition() - outBytes.getBeginPosition());

			// copy the new row from outBytes into the page
			page.storeRecord((LogInstant) null, slot, false, limitIn);

			return nextColumn;

		} catch (IOException ioe) {

			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

	}

	public void actionPurge(RawTransaction t, BasePage page, int slot, int
							num_rows, int[] recordIds, boolean needDataLogged)
		throws StandardException
	{
		// purge the records in the stored version
		// we need to remove from high to low because the slots will be moved down
		// as soon as one is removed.

		// we could get the slot with the recordId but that will be a waste
		// since the page was never unlatch and the slot number is good

		try {
			for (int i = num_rows-1; i >= 0; i--)
			{
				page.purgeRecord((LogInstant) null, slot+i, recordIds[i]);
			}
		} catch (IOException ioe) {

			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

	}

	public void actionUpdateField(
    RawTransaction          t, 
    BasePage                page, 
    int                     slot, 
    int                     recordId, 
    int                     fieldId, 
    Object     newValue, 
    LogicalUndo             undo)
		throws StandardException
	{
		outBytes.reset();

		try {

			page.logColumn(slot, fieldId, newValue, (DynamicByteArrayOutputStream) outBytes, 100);

			limitIn.setData(outBytes.getByteArray());
			limitIn.setPosition(outBytes.getBeginPosition());
			limitIn.setLimit(outBytes.getPosition() - outBytes.getBeginPosition());

			page.storeField((LogInstant) null, slot, fieldId, limitIn);

		} catch (IOException ioe) {

			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

	}

	public int actionInsert(
    RawTransaction          t, 
    BasePage                page, 
    int                     slot, 
    int                     recordId,
    Object[]                row, 
    FormatableBitSet                 validColumns, 
    LogicalUndo             undo, 
    byte                    insertFlag, 
    int                     startColumn,
    boolean                 isLongColumn, 
    int                     realStartColumn, 
    DynamicByteArrayOutputStream  logBuffer, 
    int                     realSpaceOnPage,
    int                     overflowThreshold)
		throws StandardException
	{
		if (logBuffer == null)
			outBytes.reset();
		else
			outBytes = (DynamicByteArrayOutputStream) logBuffer;

		try {
			if (isLongColumn) {
				startColumn = page.logLongColumn(slot, recordId,
					row[0], (DynamicByteArrayOutputStream) outBytes);
			} else {
				startColumn = page.logRow(slot, true, recordId, row, validColumns,
					(DynamicByteArrayOutputStream) outBytes, startColumn, insertFlag, realStartColumn, realSpaceOnPage,
					overflowThreshold);
			}
	
			limitIn.setData(outBytes.getByteArray());
			limitIn.setPosition(outBytes.getBeginPosition());
			limitIn.setLimit(outBytes.getPosition() - outBytes.getBeginPosition());

			page.storeRecord((LogInstant) null, slot, true, limitIn);
			return (startColumn);

		} catch (IOException ioe) {

			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

	}

	public void actionCopyRows(RawTransaction t, BasePage destPage, BasePage srcPage, int destSlot, int numRows, int srcSlot, int[] recordIds)
		throws StandardException
	{
		try {

			// check to make sure the destination page have the necessary space to
			// take the rows - count the bytes once for checking, then once for
			// real

			// this one is for checking
			int[] spaceNeeded = new int[numRows];
			for (int i = 0; i < numRows; i++)
			{
				outBytes.reset();
				srcPage.logRecord(srcSlot + i, BasePage.LOG_RECORD_DEFAULT, 
								  recordIds[i],  (FormatableBitSet) null,
								  outBytes, (RecordHandle)null);
				spaceNeeded[i] = outBytes.getUsed();

				// do not worry about reserve space since we cannot rollback
			}

			if (!destPage.spaceForCopy(numRows, spaceNeeded))
            {
				throw StandardException.newException(
                        SQLState.DATA_NO_SPACE_FOR_RECORD);
            }

			// this one is for real
			for (int i = 0; i < numRows; i++)
			{
				// the recordId passed in is the record Id this row will have at
				// the destination page, not the record Id this row has on the
				// srcPage.
				outBytes.reset();
				srcPage.logRecord(srcSlot + i, BasePage.LOG_RECORD_DEFAULT, 
								  recordIds[i],  (FormatableBitSet) null,
								  outBytes, (RecordHandle)null);

				limitIn.setData(outBytes.getByteArray());
				limitIn.setPosition(outBytes.getBeginPosition());
				limitIn.setLimit(outBytes.getPosition() - outBytes.getBeginPosition());

				destPage.storeRecord((LogInstant) null, destSlot+i, true, limitIn);
			}
		} catch (IOException ioe) {

			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

	}

	public void actionInvalidatePage(RawTransaction t, BasePage page)
		 throws StandardException
	{
		page.setPageStatus((LogInstant)null, BasePage.INVALID_PAGE);
	}

	public void actionInitPage(RawTransaction t, BasePage page, int initFlag,
							   int pageFormatId, long pageOffset)
		 throws StandardException
	{
		boolean overflowPage = ((initFlag & BasePage.INIT_PAGE_OVERFLOW) != 0);
		boolean reuse = ((initFlag & BasePage.INIT_PAGE_REUSE) != 0);

		int nextRecordId = ((initFlag & BasePage.INIT_PAGE_REUSE_RECORDID) == 0) ?
			page.newRecordId() : RecordHandle.FIRST_RECORD_ID;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(page.getTypeFormatId() == pageFormatId, 
				"Direct initPage somehow got the wrong page formatId"); 

		page.initPage((LogInstant)null, BasePage.VALID_PAGE,
					  nextRecordId, overflowPage, reuse);
	}

	public void actionShrinkReservedSpace(RawTransaction t, BasePage page, 
				int slot, int recordId, int newValue, int oldValue)
		 throws StandardException
	{
		try
		{
			page.setReservedSpace((LogInstant)null, slot, newValue);
		}
		catch (IOException ioe) 
		{
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}
	}

}
