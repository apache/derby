/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_AccessFactory

   Copyright 1997, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.unitTests.store;

import org.apache.derbyTesting.unitTests.harness.T_Generic;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.store.access.*;

import org.apache.derby.iapi.types.SQLLongint;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.i18n.MessageService;
import java.io.File;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Properties;
import org.apache.derby.iapi.types.SQLInteger;

import org.apache.derby.iapi.types.SQLChar;

public class T_AccessFactory extends T_Generic
{
    private static final String testService = "accessTest";

    AccessFactory store = null;

	public T_AccessFactory()
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
		TransactionController tc = null;
		boolean pass = false;

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
			store = (AccessFactory) Monitor.createPersistentService(
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

            ContextManager cm = 
                    ContextService.getFactory().getCurrentContextManager();

			tc = store.getAndNameTransaction(
                    cm, AccessFactoryGlobals.USER_TRANS_NAME);


			if (
				dropTest(tc)
				&& holdCursor(tc)
				&& readUncommitted(tc)
                && updatelocks(tc)
				&& nestedUserTransaction(tc)
                && sortCost(tc)
                && storeCost(tc)
                && partialScan(tc)
                && scanInfo(tc)
				&& insertAndUpdateExample(tc)
				&& insertAndFetchExample(tc)
				&& scanExample(tc)
                && alterTable(tc)
				&& tempTest(tc)
                && getTableProperties(tc)
                && insert_bench(tc)
				&& transactionalProperties(tc)
				&& commitTest(tc))
			{
				pass = true;
			}

			// Make sure commitNoSync gets executed sometimes.
			tc.commitNoSync(TransactionController.RELEASE_LOCKS);	

			tc.destroy();

			if (!pass)
				throw T_Fail.testFailMsg("test failed");

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

	/*
	** Methods of T_AccessFactory.
	*/

    private void flush_cache()
        throws StandardException
    {
        // flush and empty cache to make sure rereading stuff works.
        RawStoreFactory rawstore = 
            (RawStoreFactory) Monitor.findServiceModule(
                this.store, RawStoreFactory.MODULE);

        rawstore.checkpoint();
    }

	protected boolean insertAndFetchExample(TransactionController tc)
		throws StandardException, T_Fail
	{
		REPORT("(insertAndFetchExample)");

        // First a negative test - make sure heap requires a template:

        try
        {
            // Create a heap conglomerate.
            long conglomid = 
                tc.createConglomerate(
                    "heap", // create a heap conglomerate
                    null,   // ERROR - Heap requires a template!!!
					null, 	// column sort order not required for heap
                    null,   // default properties
                    TransactionController.IS_DEFAULT); // not temporary

            throw T_Fail.testFailMsg("Allowed heap create without template.");
        }
        catch (Throwable t)
        {
            // expected error, just continue.
        }

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(1);
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary


		// Insert and fetch some values.
		if (insertAndFetch(tc, conglomid, 33)
			&& insertAndFetch(tc, conglomid, -1)
			&& insertAndFetch(tc, conglomid, -1000000000))
		{
			return true;
		}
		else
        {
			return false;
        }
	}

	// Insert a single row with a single column containing
	// the argument integer, and fetch it back, making sure that
	// we read the correct value.
	//
	protected boolean insertAndFetch(
    TransactionController tc, 
    long                  conglomid, 
    int                   value)
		throws StandardException, T_Fail
	{
        StaticCompiledOpenConglomInfo static_info =
            tc.getStaticCompiledConglomInfo(conglomid);

        DynamicCompiledOpenConglomInfo dynamic_info =
            tc.getDynamicCompiledConglomInfo(conglomid);

        String curr_xact_name = tc.getTransactionIdString();

        REPORT("(insertAndFetch) xact id = " + curr_xact_name);

		// Open the conglomerate.
		ConglomerateController cc =	
            tc.openCompiledConglomerate(
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                static_info,
                dynamic_info);

		// Create a row.
		T_AccessRow r1 = new T_AccessRow(1);
		SQLInteger c1 = new SQLInteger(value);
		r1.setCol(0, c1);

		// Get a location template
		RowLocation rowloc = cc.newRowLocationTemplate();



		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc);

        // quick test to make sure we can hash insert and find row location.
        Hashtable test_rowloc_hash = new Hashtable();
        test_rowloc_hash.put(rowloc, rowloc);

        RowLocation hash_find = (RowLocation) test_rowloc_hash.get(rowloc);

        if (!hash_find.equals(rowloc))
            throw T_Fail.testFailMsg("(insertAndFetch) bad hash lookup 1");

        hash_find = (RowLocation) test_rowloc_hash.remove(rowloc);

        if (!hash_find.equals(rowloc))
            throw T_Fail.testFailMsg("(insertAndFetch) bad hash lookup 2");

        hash_find = (RowLocation) test_rowloc_hash.remove(rowloc);

        if (hash_find != null)
            throw T_Fail.testFailMsg("(insertAndFetch) bad hash lookup 3");


		// Create a new row of the same type (since the interface expects
		// the callers to be keeping the row types straight), but with
		// a different column value.
		T_AccessRow r2 = new T_AccessRow(1);
		SQLInteger c2 = new SQLInteger(0);
		r2.setCol(0, c2);

		// Fetch the stored value.
		if (!cc.fetch(rowloc, r2.getRowArray(), (FormatableBitSet) null))
        {
			throw T_Fail.testFailMsg("(insertAndFetch) fetch found no row.");
        }

        // Fetch using the fetch partial column interface
        SQLInteger c3 = new SQLInteger(0);
		FormatableBitSet singleColumn = new FormatableBitSet(1);
		singleColumn.set(0);
		DataValueDescriptor[] c3row = new DataValueDescriptor[1];
		c3row[0] = c3;

        if (!cc.fetch(rowloc, c3row, singleColumn))
        {
			throw T_Fail.testFailMsg("(insertAndFetch) fetch found no row.");
        }

		// Close the conglomerate.
		cc.close();

		// Make sure we read back the value we wrote.
		if (c2.getInt() != value)
			throw T_Fail.testFailMsg("(insertAndFetch) Fetched value != inserted value.");

        if (c3.getInt() != value)
			throw T_Fail.testFailMsg("(insertAndFetch) Fetched value != inserted value.");
		
        return true;
	}

	protected boolean insertAndUpdateExample(TransactionController tc)
		throws StandardException, T_Fail
	{
		// Create a heap conglomerate.
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                new T_AccessRow(1).getRowArray(), // 1 column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

		REPORT("(insertAndUpdateExample)");

		// Insert and update some values
		if (insertAndUpdate(tc, conglomid, -1, -1003152)
			&& insertAndUpdate(tc, conglomid, 0, 2000000000)
			&& deletetest(tc, conglomid, 1, 2))
		{
			return true;
		}

		return false;
	}

	// Insert a single row with a single column containing
	// the first argument integer, delete it, make sure subsequent
	// delete, replace, and replace a single column return false.
	//
	protected boolean deletetest(
	TransactionController tc, 
	long				  conglomid,
	int					  value1,
	int					  value2)
		throws StandardException, T_Fail
	{
		boolean ret_val;

		// Open the conglomerate.
		ConglomerateController cc =	
			tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row.
		T_AccessRow r1 = new T_AccessRow(1);
		r1.setCol(0, new SQLInteger(value1));

		// Get a location template
		RowLocation rowloc = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc);

		// delete it.
		if (!cc.delete(rowloc))
		{
			throw T_Fail.testFailMsg("(deleteTest) delete of row failed");
		}

		// subsequent replace, update a single column, and delete 
        // should return false 

        // update single column
        DataValueDescriptor[] update_row  = new DataValueDescriptor[1];
        FormatableBitSet   update_desc = new FormatableBitSet(1);
        update_desc.set(0);
		if (cc.replace(rowloc, update_row, update_desc))
		{
			throw T_Fail.testFailMsg(
            "(deleteTest) partial column row replace returned true on del row");
		}

        // update whole row.
		if (cc.replace(rowloc, r1.getRowArray(), (FormatableBitSet) null))
		{
			throw T_Fail.testFailMsg("(deleteTest) update returned true on del row");
		}
		if (cc.delete(rowloc))
		{
			throw T_Fail.testFailMsg("(deleteTest) delete returned true on del row");
		}

		// Close the conglomerate.
		cc.close();

		return true;
	}

	// Insert a single row with a single column containing
	// the first argument integer, update it to the second
	// value, and make sure the update happened.
	//
	protected boolean insertAndUpdate(TransactionController tc, long conglomid,
		int value1, int value2)
		throws StandardException, T_Fail
	{
		// Open the conglomerate.
		ConglomerateController cc =	
            tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row.
		T_AccessRow r1 = new T_AccessRow(1);
		r1.setCol(0, new SQLInteger(value1));

		// Get a location template
		RowLocation rowloc = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc);

		// Update it to the second value
        DataValueDescriptor[] update_row  = new DataValueDescriptor[1];
        update_row[0] = new SQLInteger(value2);

        FormatableBitSet update_desc = new FormatableBitSet(1);
        update_desc.set(0);

		cc.replace(rowloc, update_row, update_desc);

		// Create a new row (of the same type, since the interface expects
		// the callers to be keeping the row types straight.
		T_AccessRow r2 = new T_AccessRow(1);
		SQLInteger c2 = new SQLInteger(0);
		r2.setCol(0, c2);

		// Fetch the stored value.
		if (!cc.fetch(rowloc, r2.getRowArray(), (FormatableBitSet) null))
        {
			throw T_Fail.testFailMsg("(insertAndUpdate) Fetch val not there.");
        }

		// Close the conglomerate.
		cc.close();

		// Make sure we read back the value we wrote.
		if (c2.getInt() != value2)
			throw T_Fail.testFailMsg("(insertAndUpdate) Fetch value != updated value.");
		else
			return true;
	}

	protected boolean scanExample(TransactionController tc)
		throws StandardException, T_Fail
	{
        tc.commit();

        if (!tc.isPristine() || !tc.isIdle() || tc.isGlobal())
            throw T_Fail.testFailMsg(
                "(scanExample) bad xact state after commit.");

        if ((tc.countOpens(TransactionController.OPEN_TOTAL) > 0)        ||
            (tc.countOpens(TransactionController.OPEN_CONGLOMERATE) > 0) ||
            (tc.countOpens(TransactionController.OPEN_SCAN) > 0)         ||
            (tc.countOpens(TransactionController.OPEN_CREATED_SORTS) > 0)    ||
            (tc.countOpens(TransactionController.OPEN_SORT) > 0))
        {
            System.out.println("OPENED 0:\n" + tc.debugOpened());
            return(FAIL("unexpected open count."));
        }

		// Create a heap conglomerate.
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                new T_AccessRow(1).getRowArray(), // 1 SQLInteger() column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

		REPORT("(scanExample) starting");


		// Open it.
		ConglomerateController cc =	
            tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);


		// Insert some values.
		int values[] = { 11, 22, 33, 44, 55, 66 };
		T_AccessRow row = new T_AccessRow(1);
		for (int i = 0; i < values.length; i++)
		{
			row.setCol(0, new SQLInteger(values[i]));
			if (cc.insert(row.getRowArray()) != 0)
				throw T_Fail.testFailMsg("(scanExample after insert) insert failed ");
		}

        // For test coverage call the debugging output routine - can't diff it.
        REPORT("(scanExample) debug output testing: " + tc.debugOpened());

		// Close the conglomerate.
		cc.close();

        if ((tc.countOpens(TransactionController.OPEN_TOTAL) > 0)        ||
            (tc.countOpens(TransactionController.OPEN_CONGLOMERATE) > 0) ||
            (tc.countOpens(TransactionController.OPEN_SCAN) > 0)         ||
            (tc.countOpens(TransactionController.OPEN_CREATED_SORTS) > 0)    ||
            (tc.countOpens(TransactionController.OPEN_SORT) > 0))
        {
            System.out.println("OPENED 1:\n" + tc.debugOpened());
            return(FAIL("unexpected open count."));
        }

		REPORT("(scanExample) rows inserted");

		// Correlates our position in the upcoming scan to the values array.
		int scanindex = 0;

		// Put a specific column in the row so we can look at it.
		SQLInteger col = new SQLInteger(0);
		row.setCol(0, col);

        flush_cache();

        StaticCompiledOpenConglomInfo static_info =
            tc.getStaticCompiledConglomInfo(conglomid);

		// Open a scan on the conglomerate.
		ScanController scan1 = tc.openCompiledScan(
			false, // don't hold
			0,     // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0,    // unused if stop position is null.
            static_info,
            tc.getDynamicCompiledConglomInfo(conglomid));

        // check out the RowCountable interface's.
        
        if (scan1.getEstimatedRowCount() != 6)
        {
            throw T_Fail.testFailMsg(
                "(scanExample) estimated row count not 6:" + 
                scan1.getEstimatedRowCount());
        }

        // Test 2 - ASSERT(should be able to set arbitrary row count)

        scan1.setEstimatedRowCount(5);

        if (scan1.getEstimatedRowCount() != 5)
        {
            throw T_Fail.testFailMsg("(scanExample) estimated row count not 5");
        }


		// Iterate through and check that the rows are still there.
		while (scan1.next())
		{
			scan1.fetch(row.getRowArray());

			// Check we got the value we put in.
			if (col.getInt() != values[scanindex])
				throw T_Fail.testFailMsg("(scanExample after insert) Row "
					+ scanindex
					+ " should have been "
					+ values[scanindex]
					+ ", was "
					+ col.getInt());

			scanindex++;
		}

        // make sure another next() call continues to return false.
        if (scan1.next())
            throw T_Fail.testFailMsg("(scanExample after insert) should continue to return false after reaching end of scan");

        // see if reopen scan interfaces work
		scan1.reopenScan(
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        scan1.next();
        scan1.next();
        scan1.next();
        RowLocation third_row_rowloc = scan1.newRowLocationTemplate();
        scan1.fetchLocation(third_row_rowloc);

        // see if reopen scan interfaces work
		scan1.reopenScanByRowLocation(
            third_row_rowloc,
			null);

        scanindex = 2;
		while (scan1.next())
		{
			scan1.fetch(row.getRowArray());

			// Check we got the value we put in.
			if (col.getInt() != values[scanindex])
				throw T_Fail.testFailMsg("(scanExample after insert) Row "
					+ scanindex
					+ " should have been "
					+ values[scanindex]
					+ ", was "
					+ col.getInt());

			scanindex++;
		}

		scan1.close();

		// Check we saw the right number of rows.
		if (scanindex != values.length)
			throw T_Fail.testFailMsg("(scanExample after insert) Expected "
				+ values.length
				+ "rows, got "
				+ scanindex);

		REPORT("(scanExample) rows present and accounted for");

		// Open another scan on the conglomerate.
		ScanController scan2 = tc.openScan(
			conglomid,
			false, // don't hold
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

		// Iterate with the second scan and fiddle with the values so they
		// look like the new value array.
		int newvalues[] = { 22, 33, 444, 55, 6666 };
 		while (scan2.next())
		{
			scan2.fetch(row.getRowArray());

			switch(((SQLInteger) row.getCol(0)).getInt())
			{
			case 11:
				if (!scan2.delete())
					throw T_Fail.testFailMsg("(scanExample) delete failed.");
				break;
			case 22:
			case 33:
			case 55:
				// leave these alone
				break;
			case 44:
                DataValueDescriptor[] update_row  = new DataValueDescriptor[1];
                update_row[0] = new SQLInteger(444);

                FormatableBitSet update_desc = new FormatableBitSet(1);
                update_desc.set(0);

				if (!scan2.replace(update_row, update_desc))
                {
					throw T_Fail.testFailMsg(
                        "(scanExample) partial column row replace failed.");
                }
				break;
			case 66:
				row.setCol(0, new SQLInteger(6666));
				if (!scan2.replace(row.getRowArray(), (FormatableBitSet) null))
					throw T_Fail.testFailMsg("(scanExample) replace failed.");
				break;
			default:
				throw T_Fail.testFailMsg("(scanExample) Read unexpected value.");
			}
		}
		scan2.close();

		REPORT("(scanExample) rows fiddled with");

		// Open a third scan on the conglomerate.
		ScanController scan3 = tc.openScan(
			conglomid,
			false, // don't hold
			0, // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

		// Iterate through and inspect the changes.
		scanindex = 0;
		row.setCol(0, col);
		while (scan3.next())
		{
			scan3.fetch(row.getRowArray());

			REPORT("(scanExample) scan3 fetched " + col.getInt());

			// Check we got the value we put in.
			if (col.getInt() != newvalues[scanindex])
				throw T_Fail.testFailMsg("(scanExample after changes) Row "
					+ scanindex
					+ " should have been "
					+ newvalues[scanindex]
					+ ", was "
					+ col.getInt());

			scanindex++;
		}
		scan3.close();

		// Open a third scan on the conglomerate.
		scan3 = tc.openScan(
			conglomid,
			false, // don't hold
			0, // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_READ_UNCOMMITTED,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

		// Iterate through and inspect the changes.
		scanindex = 0;
		row.setCol(0, col);
		while (scan3.next())
		{
			scan3.fetch(row.getRowArray());

			REPORT("(scanExample) scan3 fetched " + col.getInt());

			// Check we got the value we put in.
			if (col.getInt() != newvalues[scanindex])
				throw T_Fail.testFailMsg("(scanExample after changes) Row "
					+ scanindex
					+ " should have been "
					+ newvalues[scanindex]
					+ ", was "
					+ col.getInt());

			scanindex++;
		}
		scan3.close();

		// Check we saw the right number of rows.
		if (scanindex != newvalues.length)
			throw T_Fail.testFailMsg("(scanExample after changes) Expected "
				+ newvalues.length
				+ "rows, got "
				+ scanindex);

		REPORT("(scanExample) fiddled rows present and accounted for");

		REPORT("(scanExample) testing expected delete errors");

		// Open 4th scan on conglomerate and test "expected" error returns
		// from replace, partial column replace, delete.
		ScanController scan4 = tc.openScan(
			conglomid,
			false,	// don't hold
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

		// Iterate with the second scan find the "22" row, delete it and
		// then test that operations on that deleted entry FAIL as expected.
 		while (scan4.next())
		{
			scan4.fetch(row.getRowArray());

            if (!scan4.doesCurrentPositionQualify())
            {
                throw T_Fail.testFailMsg("(scanExample doesCurrentPositionQualify() errors) Expected requalify of current row to succeed");
            }

			if (((SQLInteger) row.getCol(0)).getInt() == 22)
			{
				if (!scan4.delete())
				{
					throw T_Fail.testFailMsg("(scanExample delete errors) Delete failed.");
				}
				break;
			}
		}

        if (scan4.doesCurrentPositionQualify())
        {
			throw T_Fail.testFailMsg("(scanExample doesCurrentPositionQualify() errors) Expected qualify of deleted row to FAIL");
        }

        DataValueDescriptor[] update_row  = new DataValueDescriptor[1];

        FormatableBitSet update_desc = new FormatableBitSet(1);
        update_desc.set(0);

		if (scan4.replace(update_row,  update_desc))
		{
			throw T_Fail.testFailMsg("(scanExample delete errors) Expected partial column replace to FAIL");
		}
		if (scan4.replace(row.getRowArray(), (FormatableBitSet) null))
		{
			throw T_Fail.testFailMsg("(scanExample after changes) Expected replace to FAIL");
		}
		if (scan4.delete())
		{
			throw T_Fail.testFailMsg("(scanExample after changes) Expected delete to FAIL");
		}

		scan4.close();

        if ((tc.countOpens(TransactionController.OPEN_TOTAL) > 0)           ||
            (tc.countOpens(TransactionController.OPEN_CONGLOMERATE) > 0)    ||
            (tc.countOpens(TransactionController.OPEN_SCAN) > 0)            ||
            (tc.countOpens(TransactionController.OPEN_CREATED_SORTS) > 0)   ||
            (tc.countOpens(TransactionController.OPEN_SORT) > 0))
        {
            System.out.println("OPENED:\n" + tc.debugOpened());
            return(FAIL("unexpected open count."));
        }


		REPORT("(scanExample) completed");
		return true;
	}

	protected boolean dropTest(TransactionController tc)
		throws StandardException, T_Fail
	{
        ConglomerateController cc;

		REPORT("(dropTest) starting");

        // Test of drop conglomerate with abort by doing the following:
        //     create table
        //     commit
        //     drop table
        //     make sure table is not still there.
        //     abort
        //     make sure table is still there.

		// Create a heap conglomerate.
		long orig_conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                new T_AccessRow(1).getRowArray(), // 1 SQLInteger() column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

        tc.commit();

        tc.dropConglomerate(orig_conglomid);


		// Try and Open it - it should fail.
        try
        {
            cc = tc.openConglomerate(
                    orig_conglomid, 
                    false,
                    TransactionController.OPENMODE_FORUPDATE, 
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

            throw T_Fail.testFailMsg("Open conglom on deleted conglom worked.");
        }
        catch (StandardException e)
        {
			if (!e.getMessageId().equals(
                    SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST))
            {
                throw e;
            }

            // normal path through the test - conglomerate does not exist, 
            // ignore the expected error
        }

		// Try and Open a random non-existant conglomerate - it should fail.
        try
        {
            cc = tc.openConglomerate(
                    42424242, 
                    false,
                    TransactionController.OPENMODE_FORUPDATE, 
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

            throw T_Fail.testFailMsg("Open conglom on deleted conglom worked.");
        }
        catch (StandardException e)
        {
			if (!e.getMessageId().equals(
                    SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST))
            {
                throw e;
            }

            // normal path through the test - conglomerate does not exist, 
            // ignore the expected error
        }

		// Try and delete it again - it should fail.
        try
        {
            tc.dropConglomerate(orig_conglomid);

            throw T_Fail.testFailMsg("Delete conglom on deleted conglom worked.");
        }
        catch (StandardException e)
        {
            // normal path through the test, ignore the expected error
        }


        // cursory test to make sure conglom directory is not screwed up.
        
		// Create a heap conglomerate.
		long conglomid = 
            tc.createConglomerate(
                "heap",         // create a heap conglomerate
                new T_AccessRow(1).getRowArray(),   // 1 SQLInteger() column template.
				null, 	// column sort order not required for heap
                null,           // default properties
                TransactionController.IS_DEFAULT);         // not temporary

        cc = tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        tc.abort();

        // the original conglomerate should be still around after the abort.
        cc = tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        cc.close();

		return true;
	}

    /**
     * Test the access level getTableProperties() call.
     * <p>
     *
	 * @return true if the test succeeded.
     *
     * @param tc The transaction controller to use in the test.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail Unexpected behaviour from the API
     **/
	protected boolean getTableProperties(
    TransactionController tc)
		throws StandardException, T_Fail
	{
        int key_value;

        Properties prop = new Properties();

        prop.put(Property.PAGE_SIZE_PARAMETER,           "8192");
        prop.put(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER, "99");
        prop.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER,   "42");
		prop.put(RawStoreFactory.CONTAINER_INITIAL_PAGES,	"22");

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(1);
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, 	// column sort order not required for heap
                prop,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary
		// Open the conglomerate.
		ConglomerateController cc =	
            tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // verify that input properties were used.
        Properties ret_prop = tc.getUserCreateConglomPropList();

        cc.getTableProperties(ret_prop);

        if (ret_prop.getProperty(Property.PAGE_SIZE_PARAMETER).
                compareTo("8192") != 0         ||
            ret_prop.getProperty(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER).
                compareTo("99") != 0           ||
            ret_prop.getProperty(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER).
                compareTo("42") != 0           ||
			ret_prop.getProperty(RawStoreFactory.CONTAINER_INITIAL_PAGES).
				compareTo("22") != 0)
        {
			throw T_Fail.testFailMsg(
                "(getTableProperties) Did not get expected table propertes(1)." +
                "\nGot pageSize = " + 
                    ret_prop.getProperty(Property.PAGE_SIZE_PARAMETER) +
                "\nGot reserved = " +
                    ret_prop.getProperty(
                        RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER) +
                "\nGot minimum record size = " +
                    ret_prop.getProperty(
                        RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER) +
                "\nGot initial pages = " + 
                    ret_prop.getProperty(
                        RawStoreFactory.CONTAINER_INITIAL_PAGES));
        }

        ret_prop = cc.getInternalTablePropertySet(null);

        if (ret_prop.getProperty(Property.PAGE_SIZE_PARAMETER).
                compareTo("8192") != 0         ||
            ret_prop.getProperty(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER).
                compareTo("99") != 0           ||
            ret_prop.getProperty(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER).
                compareTo("42") != 0           ||
			ret_prop.getProperty(RawStoreFactory.CONTAINER_INITIAL_PAGES).
				compareTo("22") != 0)
        {
			throw T_Fail.testFailMsg(
                "(getTableProperties) Did not get expected table propertes(2)." +
                "\nGot pageSize = " + 
                    ret_prop.getProperty(Property.PAGE_SIZE_PARAMETER) +
                "\nGot reserved = " +
                    ret_prop.getProperty(
                        RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER) +
                "\nGot minimum record size = " +
                    ret_prop.getProperty(
                        RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER) +
                "\nGot initial pages = " + 
                    ret_prop.getProperty(
                        RawStoreFactory.CONTAINER_INITIAL_PAGES));
        }

        ret_prop = new Properties();
        
        ret_prop = cc.getInternalTablePropertySet(ret_prop);

        if (ret_prop.getProperty(Property.PAGE_SIZE_PARAMETER).
                compareTo("8192") != 0         ||
            ret_prop.getProperty(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER).
                compareTo("99") != 0           ||
            ret_prop.getProperty(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER).
                compareTo("42") != 0           ||
			ret_prop.getProperty(RawStoreFactory.CONTAINER_INITIAL_PAGES).
				compareTo("22") != 0)
        {
			throw T_Fail.testFailMsg(
                "(getTableProperties) Did not get expected table propertes(3)." +
                "\nGot pageSize = " + 
                    ret_prop.getProperty(Property.PAGE_SIZE_PARAMETER) +
                "\nGot reserved = " +
                    ret_prop.getProperty(
                        RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER) +
                "\nGot minimum record size = " +
                    ret_prop.getProperty(
                        RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER) +
                "\nGot initial pages = " + 
                    ret_prop.getProperty(
                        RawStoreFactory.CONTAINER_INITIAL_PAGES));
        }

        return(true);
    }

    /**
     * Test the access level alter table interface for adding columns.
     * <p>
     *
	 * @return true if the test succeeded.
     *
     * @param tc The transaction controller to use in the test.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail Unexpected behaviour from the API
     **/
	protected boolean alterTable(
    TransactionController tc)
		throws StandardException, T_Fail
	{
        int key_value;

		REPORT("(alterTable) starting");

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(1);
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary
		// Open the conglomerate.
		ConglomerateController cc =	
            tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a 1 column row. int column = 1.
		T_AccessRow r1 = new T_AccessRow(1);
		SQLInteger c1 = new SQLInteger(1);
		r1.setCol(0, c1);

		// Get a location template
		RowLocation rowloc1 = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc1);

        // create another 1 column row. int column = 2.
		// Get a location template
        r1.setCol(0, new SQLInteger(2));
		RowLocation rowloc2 = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc2);

        // At this point the table looks like:
        // col1
        // ----
        // 1
        // 2

        // RESOLVE - currently the store can't catch the following error:
        /*
        // Test that we can't alter while it is open.
        try
        {
            tc.addColumnToConglomerate(conglomid, 1, c1);
			throw T_Fail.testFailMsg(
                "(alterTable) Allowed alter table while table was open.");
        }
        catch (StandardException t)
        {
            // expected error continue the test.
        }
        */

        // Test that we can't add data to columns that don't exist

        // Currently we only error check in debug code.  
        // RESOLVE - should this be a runtime error?
        if (SanityManager.DEBUG)
        {
            try
            {
                T_AccessRow two_column_row = new T_AccessRow(2);
                SQLInteger col1        = new SQLInteger(3);
                SQLInteger col2        = new SQLInteger(3);
                cc.insert(two_column_row.getRowArray());
                throw T_Fail.testFailMsg(
                    "(alterTable) Allowed insert of bad row.");
            }
            catch (StandardException t)
            {
                // expected error continue the test.
            }
        }

        // Test that we can't fetch data columns that don't exist

        // Currently we only error check for this in sanity code.
        // RESOLVE - (mikem) should we check for this in released runtime?
        if (SanityManager.DEBUG)
        {
            try
            {
                T_AccessRow two_column_row = new T_AccessRow(2);
                if (!cc.fetch(
                        rowloc1, two_column_row.getRowArray(), (FormatableBitSet) null))
                {
                    throw T_Fail.testFailMsg(
                        "(alterTable) Allowed fetch of bad row, bad ret val.");
                }

                throw T_Fail.testFailMsg(
                    "(alterTable) Allowed fetch of bad row.");
            }
            catch (StandardException t)
            {
                // expected error continue the test.
            }
        }

        // Test that we can't fetch data columns that don't exist
        // Currently we only error check for this in sanity code.
        // RESOLVE - (mikem) should we check for this in released runtime?
        if (SanityManager.DEBUG)
        {
            try
            {
                DataValueDescriptor[] third_column_row = 
                    new DataValueDescriptor[3];

                third_column_row[2]         = new SQLInteger(3);

                FormatableBitSet   fetch_desc        = new FormatableBitSet(3);
                fetch_desc.set(2);

                if (!cc.fetch(
                        rowloc1, third_column_row, fetch_desc))
                {
                    throw T_Fail.testFailMsg(
                        "(alterTable) Allowed fetch of bad row, bad ret val.");
                }

                throw T_Fail.testFailMsg(
                    "(alterTable) Allowed fetch of bad row.");
            }
            catch (StandardException t)
            {
                // expected error continue the test.
            }
        }

        // Test that we can't replace data columns that don't exist

        // Currently we only error check for this in sanity code.
        // RESOLVE - (mikem) should we check for this in released runtime?
        if (SanityManager.DEBUG)
        {
            try
            {
                T_AccessRow two_column_row = new T_AccessRow(2);
                SQLInteger col1        = new SQLInteger(3);
                SQLInteger col2        = new SQLInteger(3);
                cc.replace(rowloc1, two_column_row.getRowArray(), null);
                throw T_Fail.testFailMsg(
                    "(alterTable) Allowed replace of bad row.");
            }
            catch (StandardException t)
            {
                // expected error continue the test.
            }
        }

        // Test that we can't replace data columns that don't exist
        if (SanityManager.DEBUG)
        {
            try
            {
                DataValueDescriptor[] second_column_row  = 
                    new DataValueDescriptor[2];
                second_column_row[1]        = new SQLInteger(3);

                FormatableBitSet   update_desc        = new FormatableBitSet(2);
                update_desc.set(1);

                cc.replace(rowloc1, second_column_row, update_desc);
                throw T_Fail.testFailMsg(
                    "(alterTable) Allowed partial row update of bad column.");
            }
            catch (StandardException t)
            {
                // expected error continue the test.
            }
        }

		// Make sure commitNoSync gets executed sometimes.
        tc.commitNoSync(TransactionController.RELEASE_LOCKS);


        // now alter the conglomerate, add another int column
        tc.addColumnToConglomerate(conglomid, 1, c1);

        // Open the table after the close done by commit.
		cc = tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        T_AccessRow two_column_row = new T_AccessRow(2);
        SQLInteger col1        = new SQLInteger(3);
        SQLInteger col2        = new SQLInteger(3);

        // fetch the rows and make sure you get null's in new fields.
        if (!cc.fetch(
                rowloc1, two_column_row.getRowArray(), (FormatableBitSet) null))
        {
			throw T_Fail.testFailMsg(
                "(alterTable) Row not there.");
        }

        if ((((SQLInteger)two_column_row.getCol(0)).getInt() != 1) ||
            (!two_column_row.getCol(1).isNull()))
        {
			throw T_Fail.testFailMsg(
                "(alterTable) Bad column value after alter.");
        }
        if (!cc.fetch(
                rowloc2, two_column_row.getRowArray(), (FormatableBitSet) null))
        {
			throw T_Fail.testFailMsg(
                "(alterTable) Row not there.");
        }

        if ((((SQLInteger)two_column_row.getCol(0)).getInt() != 2) ||
            (!two_column_row.getCol(1).isNull()))
        {
			throw T_Fail.testFailMsg(
                "(alterTable) Bad column value after alter.");
        }

        // make sure insert of 2 column row works.
        two_column_row = new T_AccessRow(2);
        two_column_row.setCol(0, new SQLInteger(3));
        two_column_row.setCol(1, new SQLInteger(300));
        cc.insert(two_column_row.getRowArray());


        // At this point the table looks like:
        // col1 col2
        // ---- ----
        // 1    NA
        // 2    NA
        // 3    300

        
		ScanController scan = tc.openScan(
			conglomid,
			false,	// don't hold
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        while (scan.next())
        {
            scan.fetch(two_column_row.getRowArray());

            key_value = ((SQLInteger)two_column_row.getCol(0)).getInt();

            switch(key_value)
            {
                case 1:
                {
                    // Set non-existent column value to 100
                    if (!two_column_row.getCol(1).isNull())
                    {
                        throw T_Fail.testFailMsg(
                            "(alterTable) Bad column value after alter.");
                    }

                    // test that replace field works on alter added column
                    // make result row be: (1, 100)

                    two_column_row.setCol(1, new SQLInteger(100));

                    scan.replace(two_column_row.getRowArray(), (FormatableBitSet) null);
                    break;
                }
                case 2:
                {
                    if (!two_column_row.getCol(1).isNull())
                    {
                        throw T_Fail.testFailMsg(
                            "(alterTable) Bad column value after alter.");
                    }

                    // test that replace row works on alter added column row.
                    // make result row be: (2, 200)
                    two_column_row.setCol(1, new SQLInteger(200));

                    scan.replace(two_column_row.getRowArray(), (FormatableBitSet) null);

                    break;
                }
                case 3:
                {
                    break;
                }
                default:
                {
                    throw T_Fail.testFailMsg(
                        "(alterTable) bad row value found in table.");
                }

            }
        }

        // reposition the scan
		scan.reopenScan(
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        while (scan.next())
        {
            scan.fetch(two_column_row.getRowArray());

            key_value = ((SQLInteger) two_column_row.getCol(0)).getInt();

            switch(key_value)
            {
                case 1:
                case 2:
                case 3:
                {
                    int second_col_val = 
                        ((SQLInteger) two_column_row.getCol(1)).getInt();

                    if (second_col_val != (key_value * 100))
                    {
                        throw T_Fail.testFailMsg(
                            "(alterTable) Bad column value after alter." +
                            "expected: (" + 
                                key_value + ", " + key_value * 100 + ")\n" +
                            "got     : (" +
                                key_value + ", " + second_col_val  + ")\n");
                    }

                    break;
                }
                default:
                {
                    throw T_Fail.testFailMsg(
                        "(alterTable) bad row value found in table.");
                }
            }
        }

        scan.close();

        tc.commit();

		REPORT("(alterTable) completed");
		
        return true;
	}


    /**
     * Test the access level ScanInfo interface.
     * <p>
     *
	 * @return true if the test succeeded.
     *
     * @param tc The transaction controller to use in the test.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail Unexpected behaviour from the API
     **/
	protected boolean scanInfo(
    TransactionController tc)
		throws StandardException, T_Fail
	{
        int key_value;

		REPORT("(scanInfo) starting");

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(2);
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary
		// Open the conglomerate.
		ConglomerateController cc =	
            tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a 1 column row. int column = 1.
		T_AccessRow r1 = new T_AccessRow(2);
		SQLInteger c1 = new SQLInteger(1);
		SQLInteger c2 = new SQLInteger(100);
		r1.setCol(0, c1);
		r1.setCol(1, c2);

		// Get a location template
		RowLocation rowloc1 = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc1);

        // create another 2 column row. int column = 2.
		// Get a location template
        r1.setCol(0, new SQLInteger(2));
        r1.setCol(1, new SQLInteger(200));
		RowLocation rowloc2 = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc2);

        cc.delete(rowloc2);

        if (tc.isPristine() || tc.isIdle())
        {
            throw T_Fail.testFailMsg(
                "(scanInfo) bad xact state after update xact.");
        }

        tc.commit();

		ScanController scan = tc.openScan(
			conglomid,
			false,	// don't hold
            0,    // for read
            TransactionController.MODE_TABLE,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.


        if (!scan.isTableLocked())
        {
            throw T_Fail.testFailMsg(
                "(scanInfo) table should be table locked.");
        }


        while (scan.next())
        {
            scan.fetch(r1.getRowArray());
        }
        ScanInfo   scan_info = scan.getScanInfo();
        Properties prop      = scan_info.getAllScanInfo(null);

        if (!tc.isPristine() || tc.isIdle())
        {
            throw T_Fail.testFailMsg(
                "(scanInfo) bad xact state after update xact.");
        }

        REPORT(("return from full row scan heap.getScanInfo() = " + prop));

        if (Integer.parseInt(prop.getProperty(
		   MessageService.getTextMessage(SQLState.STORE_RTS_NUM_PAGES_VISITED)))
				!= 1)
        {
            throw T_Fail.testFailMsg(
                "(scanInfo) wrong numPagesVisited.  Expected 1, got " + 
                Integer.parseInt(prop.getProperty(
		  			MessageService.getTextMessage(
									SQLState.STORE_RTS_NUM_PAGES_VISITED))));
        }
        if (Integer.parseInt(prop.getProperty(
			MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_VISITED)))
				!= 2)
        {
            throw T_Fail.testFailMsg(
                "(scanInfo) wrong numRowsVisited. Expected 2, got " + 
                Integer.parseInt(prop.getProperty(
		  			MessageService.getTextMessage(
									SQLState.STORE_RTS_NUM_ROWS_VISITED))));
        }
        if (Integer.parseInt(prop.getProperty(
		  MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_QUALIFIED)))
				!= 1)
        {
            throw T_Fail.testFailMsg(
                "(scanInfo) wrong numRowsQualified. Expected 1, got " + 
                Integer.parseInt(prop.getProperty(
		  			MessageService.getTextMessage(
									SQLState.STORE_RTS_NUM_ROWS_QUALIFIED))));
        }

        // Try a partial Row scan

        // only get the 2nd column.
        FormatableBitSet validColumns = new FormatableBitSet(3);
        validColumns.set(1);

		scan = tc.openScan(
			conglomid,
			false,	                // don't hold
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			validColumns,           // only get the second column
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        if (scan.isTableLocked())
        {
            throw T_Fail.testFailMsg(
                "(scanInfo) table should be row locked.");
        }

        scan_info = scan.getScanInfo();
        prop      = scan_info.getAllScanInfo(null);
        REPORT(("return from partial scan heap.getScanInfo() = " + prop));

        // RESOLVE - should test the btree one also.

		REPORT("(scanInfo) finishing");

        return true;
	}

    /**
     * Test partial scans.
     * <p>
     *
	 * @return true if the test succeeded.
     *
     * @param tc The transaction controller to use in the test.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail Unexpected behaviour from the API
     **/
	protected boolean partialScan(
    TransactionController tc)
		throws StandardException, T_Fail
	{
        int key_value;

		REPORT("(partialScan) starting");

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(2);
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary
		// Open the conglomerate.
		ConglomerateController cc =	
            tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a 1 column row. int column = 1.
		T_AccessRow r1 = new T_AccessRow(2);
		SQLInteger c1 = new SQLInteger(1);
		SQLInteger c2 = new SQLInteger(100);
		r1.setCol(0, c1);
		r1.setCol(1, c2);

		// Get a location template
		RowLocation rowloc1 = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc1);

        // create another 2 column row. int column = 2.
		// Get a location template
        r1.setCol(0, new SQLInteger(2));
        r1.setCol(1, new SQLInteger(200));
		RowLocation rowloc2 = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc2);

        cc.delete(rowloc2);

        tc.commit();

        // Try a partial Row scan with no columns.

        // only get the 2nd column.
        FormatableBitSet validColumns = new FormatableBitSet();

		ScanController scan = tc.openScan(
			conglomid,
			false,	                // don't hold
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			validColumns,           // only get the second column
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        // should see one row.

        if (!scan.next())
        {
            throw T_Fail.testFailMsg("(partialScan) did not see first row.");
        }

        if (scan.next())
        {
            throw T_Fail.testFailMsg("(partialScan) saw more than one row.");
        }

        // RESOLVE - should test the btree one also.

		REPORT("(partialScan) finishing");

        return true;
	}


	// Simple insert into heap performance test
	protected boolean insert_bench(TransactionController tc)
		throws StandardException, T_Fail
	{
        ConglomerateController cc = null;	
        ScanController scan       = null;	
        long conglomid            = -1;
        long before, after;


		// Create a row.
		T_AccessRow r1   = new T_AccessRow(1);
        long  iter = 100; 

        for (int numcols = 1; numcols < 101; numcols *= 10)
        {
            // Create a heap conglomerate.
            conglomid = 
                tc.createConglomerate(
                    "heap",               // create a heap conglomerate
                    new T_AccessRow(numcols).getRowArray(),   // 1 SQLInteger() column template.
					null, 	// column sort order not required for heap
                    null,                 // default properties
                    TransactionController.IS_DEFAULT);               // not temporary

            tc.commit();

            // Open the conglomerate.
            cc = tc.openConglomerate(
                    conglomid, 
                    false,
                    TransactionController.OPENMODE_FORUPDATE, 
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

            for (int i = 0; i < numcols; i++)
            {
                r1.setCol(i, new SQLInteger(numcols));
            }

            // time before 
            before = System.currentTimeMillis();

            for (int i = 0; i < iter; i++)
            {
                if (cc.insert(r1.getRowArray()) != 0) 
                    throw T_Fail.testFailMsg("(insert_bench) insert failed ");
            }

            // time after 
            after = System.currentTimeMillis();

            REPORT(
                "insert " + iter + " rows of " + numcols + " integer cols = " +
                (after - before) + " milliseconds.\n");


            // time before 
            before = System.currentTimeMillis();

            for (int i = 0; i < iter; i++)
            {
               if (cc.insert(r1.getRowArray()) != 0) 
                    throw T_Fail.testFailMsg("(insert_bench) insert failed ");
            }

            // time after 
            after = System.currentTimeMillis();

            REPORT(
                "second insert " + iter + " rows of " + numcols + 
                " integer cols = " +
                (after - before) + " milliseconds.\n");

            // Open a scan on the conglomerate.
            before = System.currentTimeMillis();

            scan = tc.openScan(
                conglomid,
                false, // don't hold
                0, // not for update
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
				(FormatableBitSet) null, // all columns, all as objects
                null, // start position - first row in conglomerate
                0,    // unused if start position is null.
                null, // qualifier - accept all rows
                null, // stop position - last row in conglomerate
                0);   // unused if stop position is null.

            // time before 
            before = System.currentTimeMillis();

            // Iterate through and check that the rows are still there.
            while (scan.next())
            {
                scan.fetch(r1.getRowArray());
            }

            // time after 
            after = System.currentTimeMillis();

            REPORT(
                "scan " + (2 * iter) + " rows of " + numcols + " integer cols = " +
                (after - before) + " milliseconds.\n");

            // Close the conglomerate.
            cc.close();
            tc.commit();
        }

        return(true);
	}

    /**
     * Test the access level SortCost interface.
     * <p>
     *
	 * @return true if the test succeeded.
     *
     * @param tc The transaction controller to use in the test.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail Unexpected behaviour from the API
     **/
	protected boolean sortCost(
    TransactionController tc)
		throws StandardException, T_Fail
	{
        int key_value;

		REPORT("(sortCost) starting");

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(2);
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary
		// Open the conglomerate.
		ConglomerateController cc =	
            tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a 2 column row.
		T_AccessRow r1 = new T_AccessRow(2);
		SQLInteger c1 = new SQLInteger(1);
		SQLInteger c2 = new SQLInteger(100);
		r1.setCol(0, c1);
		r1.setCol(1, c2);

		// Get a location template
		RowLocation rowloc1 = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc1);

        cc.close();

        tc.commit();

        // flush the cache to get the row count updated.
        flush_cache();

        // Test 1 - Just call for various types of sorts.  Not sure how 
        // to test the validity.
		SortCostController scc = tc.openSortCostController(null);

        double estimated_cost = 
            scc.getSortCost(
                template_row.getRowArray(),
                null,
                false,
                10000,
                100,
                100);

        if (estimated_cost <= 0)
        {
            throw T_Fail.testFailMsg(
                "(storeCost) estimated sort cost :" + estimated_cost);
        }

		REPORT("(sortCost) finishing");

        return true;
	}

    /**
     * Test the access level StoreCost interface.
     * <p>
     *
	 * @return true if the test succeeded.
     *
     * @param tc The transaction controller to use in the test.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail Unexpected behaviour from the API
     **/
	protected boolean storeCost(
    TransactionController tc)
		throws StandardException, T_Fail
	{
        int key_value;

		REPORT("(storeCost) starting");

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(2);
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary
		// Open the conglomerate.
		ConglomerateController cc =	
            tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a 2 column row.
		T_AccessRow r1 = new T_AccessRow(2);
		SQLInteger c1 = new SQLInteger(1);
		SQLInteger c2 = new SQLInteger(100);
		r1.setCol(0, c1);
		r1.setCol(1, c2);

		// Get a location template
		RowLocation rowloc1 = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc1);

        cc.close();

        tc.commit();

        // flush the cache to get the row count updated.
        flush_cache();

        // Test 1 - ASSERT(initial row count after 1 insert should be 1)
		StoreCostController scc = tc.openStoreCost(conglomid);


        if (scc.getEstimatedRowCount() != 1)
        {
            throw T_Fail.testFailMsg(
                "(storeCost) estimated row count not 1:" + 
                scc.getEstimatedRowCount());
        }

        // Test 2 - ASSERT(should be able to set arbitrary row count)

        scc.setEstimatedRowCount(5);

        if (scc.getEstimatedRowCount() != 5)
        {
            throw T_Fail.testFailMsg("(storeCost) estimated row count not 5");
        }

        scc.setEstimatedRowCount(1);


        // Test 3 - ASSERT(should implement getFetchFromRowLocationCost()) 
        //     should figure out some way to determine reasonable number is
        //     returned.
        double fetch_cost = 
            scc.getFetchFromRowLocationCost((FormatableBitSet) null, 0);
        fetch_cost = 
            scc.getFetchFromRowLocationCost(
                (FormatableBitSet) new FormatableBitSet(0), 0);
        REPORT("fetch cost (full row) of row loc = " + fetch_cost);
        fetch_cost = 
            scc.getFetchFromRowLocationCost(
                (FormatableBitSet) new FormatableBitSet(1), 0);
        FormatableBitSet bit_set = new FormatableBitSet(2);
        REPORT("fetch cost (no cols) of row loc = " + fetch_cost);
        bit_set.set(1);
        fetch_cost = 
            scc.getFetchFromRowLocationCost(
                (FormatableBitSet) new FormatableBitSet(1), 0);
        REPORT("fetch cost (1 col) of row loc = " + fetch_cost);

        // Test 4 - ASSERT(should implement getFetchFromFullKeyCost()) 
        //     should figure out some way to determine reasonable number is
        //     returned.
        /* - RESOLVE HEAP does not implement this.
        fetch_cost = 
            scc.getFetchFromFullKeyCost((FormatableBitSet) null, (int[]) null, 0);
        REPORT("fetch full key cost (full row) of row loc = " + fetch_cost);

        fetch_cost = 
            scc.getFetchFromFullKeyCost(
                (FormatableBitSet) new FormatableBitSet(0), (int[]) null, 0);
        REPORT("fetch full key cost (no cols) of row loc = " + fetch_cost);

        fetch_cost = 
            scc.getFetchFromFullKeyCost(
                (FormatableBitSet) new FormatableBitSet(1), (int[]) null, 0);
        REPORT("fetch full key cost (no cols) of row loc = " + fetch_cost);

        bit_set = new FormatableBitSet(2);
        bit_set.set(1);
        fetch_cost = 
            scc.getFetchFromFullKeyCost(
                (FormatableBitSet) new FormatableBitSet(1), (int[]) null, 0);
        REPORT("fetch full key cost (1 col) of row loc = " + fetch_cost);
        */

        // Test 5 - ASSERT(should implement getScanCost()) 
        //     should figure out some way to determine reasonable number is
        //     returned.
        StoreCostResult cost_result = new T_StoreCostResult();

        scc.getScanCost(
            StoreCostController.STORECOST_SCAN_NORMAL,
            -1,             // row count
            1,              // number of rows fetched at a time from access.
            false,          // forUpdate
            (FormatableBitSet) null,  // validColumns
            new T_AccessRow(2).getRowArray(),  // template
            null,           // start position - first row in conglomerate
            0,              // unused if start position is null.
            null,           // stop position - last row in conglomerate
            0,              // unused if stop position is null.
            false,          // reopen_scan?
            0,              // access_type
            cost_result);   // cost result.

        REPORT("fetch scan cost (full row) of row loc = " + cost_result);

        scc.getScanCost(
            StoreCostController.STORECOST_SCAN_NORMAL,
            -1,             // row count
            1,              // number of rows fetched at a time from access.
            false,          // forUpdate
            new FormatableBitSet(0),  // validColumns
            new T_AccessRow(2).getRowArray(),  // template
            null,           // start position - first row in conglomerate
            0,              // unused if start position is null.
            null,           // stop position - last row in conglomerate
            0,              // unused if stop position is null.
            false,          // reopen_scan?
            0,              // access_type
            cost_result);   // cost result.

        REPORT("fetch scan cost (no cols) of row loc = " + cost_result);

        scc.getScanCost(
            StoreCostController.STORECOST_SCAN_NORMAL,
            -1,             // row count
            1,              // number of rows fetched at a time from access.
            false,          // forUpdate
            new FormatableBitSet(1),  // validColumns
            new T_AccessRow(2).getRowArray(),  // template
            null,           // start position - first row in conglomerate
            0,              // unused if start position is null.
            null,           // stop position - last row in conglomerate
            0,              // unused if stop position is null.
            false,          // reopen_scan?
            0,              // access_type
            cost_result);   // cost result.

        REPORT("fetch scan cost (no cols) of row loc = " + cost_result);

        bit_set = new FormatableBitSet(2);
        bit_set.set(1);
        scc.getScanCost(
            StoreCostController.STORECOST_SCAN_NORMAL,
            -1,             // row count
            1,              // number of rows fetched at a time from access.
            false,          // forUpdate
            bit_set,        // validColumns
            new T_AccessRow(2).getRowArray(),  // template
            null,           // start position - first row in conglomerate
            0,              // unused if start position is null.
            null,           // stop position - last row in conglomerate
            0,              // unused if stop position is null.
            false,          // reopen_scan?
            0,              // access_type
            cost_result);   // cost result.

        REPORT("fetch scan cost (1 cols) of row loc = " + cost_result);

        // make sure you can get a row location.
		rowloc1 = scc.newRowLocationTemplate();

		REPORT("(storeCost) finishing");

        return true;
	}

	/**
		Test transactional properties

		@exception StandardException test failure
		@exception T_Fail test failure
	*/
	protected boolean transactionalProperties(TransactionController tc)
		throws StandardException, T_Fail
	{
		REPORT("start transactionalProperties");

		// put a couple of properties in with different values and types

		tc.setProperty("T_Key_Frog", new SQLLongint(479), false);
		tc.setProperty("T_Key_Tiger", "Roar, ROAR", false);


		long lvalue = ((SQLLongint) (tc.getProperty("T_Key_Frog"))).getLong();
		if (lvalue != 479)
			throw T_Fail.testFailMsg("setProperty() - expected 479 - got " + lvalue);

		String svalue = (String) tc.getProperty("T_Key_Tiger");
		if (!svalue.equals("Roar, ROAR"))
			throw T_Fail.testFailMsg("setProperty() - expected 'Roar, ROAR' - got " + svalue);

		tc.commit();

		// should still be accessable after the commit
		lvalue = ((SQLLongint) (tc.getProperty("T_Key_Frog"))).getLong();
		if (lvalue != 479)
			throw T_Fail.testFailMsg("setProperty() - expected 479 - got " + lvalue);

		svalue = (String) tc.getProperty("T_Key_Tiger");
		if (!svalue.equals("Roar, ROAR"))
			throw T_Fail.testFailMsg("setProperty() - expected 'Roar, ROAR' - got " + svalue);

		tc.commit();

		// see if update works
		tc.setProperty("T_Key_Tiger", "mieow, mieow", false);
		svalue = (String) tc.getProperty("T_Key_Tiger");
		if (!svalue.equals("mieow, mieow"))
			throw T_Fail.testFailMsg("setProperty() - expected 'mieow, mieow' - got " + svalue);

		tc.commit();
		svalue = (String) tc.getProperty("T_Key_Tiger");
		if (!svalue.equals("mieow, mieow"))
			throw T_Fail.testFailMsg("setProperty() - expected 'mieow, mieow' - got " + svalue);

		// see if an update to a different type works
		tc.setProperty("T_Key_Tiger", new SQLLongint(570), false);
		lvalue = ((SQLLongint) (tc.getProperty("T_Key_Tiger"))).getLong();

		if (lvalue != 570)
			throw T_Fail.testFailMsg("setProperty() - expected 570 - got " + lvalue);

		tc.commit();

		lvalue = ((SQLLongint) (tc.getProperty("T_Key_Tiger"))).getLong();
		if (lvalue != 570)
			throw T_Fail.testFailMsg("setProperty() - expected 570 - got " + lvalue);

		tc.commit();

		// delete a key
		tc.setProperty("T_Key_Frog", (Serializable) null, false);
		if (tc.getProperty("T_Key_Frog") != null)
			throw T_Fail.testFailMsg("setProperty() - delete failed");
		tc.commit();

		if (tc.getProperty("T_Key_Frog") != null)
			throw T_Fail.testFailMsg("setProperty() - delete failed");

		tc.commit();

		// now see if rollback works.
		tc.setProperty("T_Key_Tiger", new SQLLongint(457), false);

		tc.abort();
		lvalue = ((SQLLongint) (tc.getProperty("T_Key_Tiger"))).getLong();
		if (lvalue != 570)
			throw T_Fail.testFailMsg("setProperty() - expected 570 - got " + lvalue);

		tc.commit();
		PASS("transactionalProperties");

		return true;
	}



	// Test temporary conglomerates.
	protected boolean tempTest(TransactionController tc)
		throws StandardException, T_Fail
	{
		REPORT("(tempTest) starting");

		// Create some conglomerates, some temporary, some not.
		long cid5252t = createAConglom(tc, 5252, true);  // temporary
		long cid87t = createAConglom(tc, 87, true); // temporary
		long cid999p = createAConglom(tc, 999, false); // permanent
		long cid3t = createAConglom(tc, 3, true); // temporary

		// Create an index on two of them
		long cid5252ti = createBtree(tc, cid5252t, true);
		long cid999pi = createBtree(tc, cid999p, false);

		int r;

		// Make sure we can read them.
		if ((r = checkAConglom(
                    tc, (DataValueDescriptor[]) null, cid5252t, 5252)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after create checkAConglom(cid5252t) == " + r);
        }
		if ((r = checkAConglom(
                    tc, getBtreeTemplate(tc, cid5252t), cid5252ti, 5252)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after create checkAConglom(cid5252ti) == " + r);
        }

		if ((r = checkAConglom(
                    tc, (DataValueDescriptor[]) null, cid999p, 999)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after create checkAConglom(cid999p) == " + r);
        }

		if ((r = checkAConglom(
                    tc, getBtreeTemplate(tc, cid999p), cid999pi, 999)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after create checkAConglom(cid999pi) == " + r);
        }

		if ((r = checkAConglom(
                    tc, (DataValueDescriptor[]) null, cid3t, 3)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after create checkAConglom(cid3t) == " + r);
        }

		if ((r = checkAConglom(
                    tc, (DataValueDescriptor[]) null, cid87t, 87)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after create checkAConglom(cid87t) == " + r);
        }

		// Drop two of them.
		tc.dropConglomerate(cid999pi);
		tc.dropConglomerate(cid999p);
		tc.dropConglomerate(cid87t);

		// Try dropping the ones we already dropped - expect exceptions
        try
        {
			tc.dropConglomerate(cid999p);
            throw T_Fail.testFailMsg("(tempTest) drop of dropped cid999p succeeded");
        }
        catch (StandardException e)
        {
            // normal path through the test, ignore the expected error
        }
        try
        {
			tc.dropConglomerate(cid999pi);
            throw T_Fail.testFailMsg("(tempTest) drop of dropped cid999pi succeeded");
        }
        catch (StandardException e)
        {
            // normal path through the test, ignore the expected error
        }
        try
        {
			tc.dropConglomerate(cid87t);
            throw T_Fail.testFailMsg("(tempTest) drop of dropped cid87t succeeded");
        }
        catch (StandardException e)
        {
            // normal path through the test, ignore the expected error
        }

		// Make sure the correct ones remain
		if ((r = checkAConglom(
                    tc, (DataValueDescriptor[]) null, cid5252t, 5252)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after drop checkAConglom(cid5252t) == " + r);
        }

		if ((r = checkAConglom(
                    tc, getBtreeTemplate(tc, cid5252t), cid5252ti, 5252)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after drop checkAConglom(cid5252ti) == " + r);
        }

		if ((r = checkAConglom(
                    tc, (DataValueDescriptor[]) null, cid3t, 3)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after drop checkAConglom(cid3t) == " + r);
        }

		// Make sure commitNoSync gets executed sometimes.
		tc.commitNoSync(TransactionController.RELEASE_LOCKS);

		// After committing the transaction, the congloms
		// should still be there (with their rows).
		if ((r = checkAConglom(
                    tc, (DataValueDescriptor[]) null, cid5252t, 5252)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after commit checkAConglom(cid5252t) == " + r);
        }

		if ((r = checkAConglom(
                    tc, getBtreeTemplate(tc, cid5252t), cid5252ti, 5252)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after commit checkAConglom(cid5252ti) == " + r);
        }

		if ((r = checkAConglom(
                    tc, (DataValueDescriptor[]) null, cid3t, 3)) != 1)
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after commit checkAConglom(cid3t) == " + r);
        }


		// open cid3t for update to force its truncation on the abort.
		ScanController sc = tc.openScan(
			cid3t,
			false, // don't hold
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

		sc.close();


		tc.abort();

		// After an abort, the congloms that were opened for update should be there,
		// but truncated
		if ((r = checkAConglom(
                    tc, (DataValueDescriptor[]) null, cid5252t, 5252)) != 1) 
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after abort checkAConglom(cid5252t) == " + r);
		}

		// RESOLVE(mikem): track 1825
		// don't want to open temp cantainer with IS_KEPT always.
		// if ((r = checkAConglom(tc, (DataValueDescriptor[]) null, cid3t, 3)) != 0) {
		//	throw T_Fail.testFailMsg("(tempTest) after abort checkAConglom(cid3t) == " + r);
		// }

		if ((r = checkAConglom(
                    tc, (DataValueDescriptor[]) null, cid3t, 3)) != 1) 
        {
			throw T_Fail.testFailMsg(
                    "(tempTest) after abort checkAConglom(cid3t) == " + r);
		}

		// Due to bug STO84, temp btrees are corrupted after aborts,
		// so the following will cause problems:
		/*
		if ((r = checkAConglom(tc, (DataValueDescriptor[]) null, cid5252ti, 5252)) != 0)
		 	throw T_Fail.testFailMsg("(tempTest) after abort checkAConglom(cid5252ti) == " + r);
		*/

		// Drop index on conglomerate to make sure we can do a drop after truncate.
		tc.dropConglomerate(cid5252ti);
		if (tc.conglomerateExists(cid5252ti))
			throw T_Fail.testFailMsg("(tempTest) after drop cid5252ti still exists");

		// Drop one conglomerate to make sure we can do a drop after truncate.
		tc.dropConglomerate(cid5252t);
		if (tc.conglomerateExists(cid5252t))
			throw T_Fail.testFailMsg("(tempTest) after drop cid5252t still exists");

		// Leave the last one -  raw store is supposed to delete
		// it when the system reboots

		// Success!
		REPORT("(tempTest) succeeded");
		return true;
	}

	private long createAConglom(TransactionController tc, int testValue, boolean temporary)
		throws StandardException
	{
		// Create a heap conglomerate.
		long cid = 
            tc.createConglomerate(
                "heap",         // create a heap conglomerate
                new T_AccessRow(1).getRowArray(),   // 1 SQLInteger() column template.
				null, 	// column sort order not required for heap
                null,           // default properties
                temporary ? TransactionController.IS_TEMPORARY : TransactionController.IS_DEFAULT);

		ConglomerateController cc = 
            tc.openConglomerate(
                cid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row.
		T_AccessRow row = new T_AccessRow(1);
		SQLLongint col = new SQLLongint(testValue);
		row.setCol(0, col);

		// Stuff in the test value so we can recognize this conglom later.
		cc.insert(row.getRowArray());

		cc.close();

		return cid;
	}

    private DataValueDescriptor[] getBtreeTemplate(
    TransactionController tc, 
    long                  baseConglomId)
        throws StandardException
    {
		// Open a scan on the base conglomerate which will return all rows.
		FormatableBitSet singleColumn = new FormatableBitSet(1);
		singleColumn.set(0);
		ScanController sc = tc.openScan(baseConglomId, false,
			0, // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			singleColumn, // all columns, all as objects
			null, 0, null, null, 0);

		// Create the template for the index. This method "knows" that
		// all rows in the base table have one IntCol
		T_AccessRow template = new T_AccessRow(2);
		SQLLongint col0 = new SQLLongint(0);
		RowLocation col1 = sc.newRowLocationTemplate();
		template.setCol(0, col0);
		template.setCol(1, col1);

        sc.close();

        return(template.getRowArray());
    }


	private long createBtree(TransactionController tc, long baseConglomId, boolean temporary)
		throws StandardException
	{
		// Create the properties for the index.
		// This method knows that there is just one column in the base table
		Properties indexProps = new Properties();
		indexProps.put("baseConglomerateId", Long.toString(baseConglomId));
		indexProps.put("nUniqueColumns", "1");
		indexProps.put("rowLocationColumn", "1");
		indexProps.put("nKeyFields", "2");

		// Open a scan on the base conglomerate which will return all rows.
		FormatableBitSet singleColumn = new FormatableBitSet(1);
		singleColumn.set(0);
		ScanController sc = tc.openScan(baseConglomId, false,
			0, // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			singleColumn, // just the first column.
			null, 0, null, null, 0);

		// Create the template for the index. This method "knows" that
		// all rows in the base table have one IntCol
		T_AccessRow template = new T_AccessRow(2);
		SQLLongint col0 = new SQLLongint(0);
		RowLocation col1 = sc.newRowLocationTemplate();
		template.setCol(0, col0);
		template.setCol(1, col1);

		DataValueDescriptor[] baseRow = new DataValueDescriptor[1];
		baseRow[0] = col0;

		// Create a btree secondary index conglomerate.
		long iid = tc.createConglomerate("BTREE", template.getRowArray(), null, indexProps,
			temporary ? TransactionController.IS_TEMPORARY : TransactionController.IS_DEFAULT);

		// Open the index so we can stuff in index rows.
		ConglomerateController cc = 
            tc.openConglomerate(
                iid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// build the index.
		while (sc.next())
		{
			sc.fetch(baseRow);
			sc.fetchLocation(col1);
			cc.insert(template.getRowArray());
		}

		cc.close();

		return iid;
	}

	/**
	Open a scan on the conglomerate for the given conglom id, and verify
	that it has rows with the given test value.  This is a way of
	verifying that we got the right conglomerate.  Returns the number of
	rows that it checked (-1 if the conglomerate doesn't exist).
	**/
	private int checkAConglom(
    TransactionController tc, 
    DataValueDescriptor[] scratch_template,
    long                  conglomId, 
    int                   testValue)
		throws StandardException
	{
		if (!tc.conglomerateExists(conglomId))
			return -1;
		
		ScanController sc = tc.openScan(
			conglomId,
			false, // don't hold
			0, // not for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

		// An empty row
		T_AccessRow row = new T_AccessRow(1);
		SQLLongint col = new SQLLongint(0);
		row.setCol(0, col);

		// Iterate through and check that the rows are still there.
		// Note this part of the test will inten
		int nrows = 0;
		while (sc.next())
		{
			sc.fetch(row.getRowArray());
			if (((SQLLongint) row.getCol(0)).getLong() != testValue)
				return -2;
			nrows++;
		}

		sc.close();

		return nrows;
	}

    protected boolean updatelocks(
    TransactionController tc)
		throws StandardException, T_Fail
    {
		REPORT("(updatelocks starting.)");

        updatelocks_0(
            tc, TransactionController.ISOLATION_SERIALIZABLE);
        updatelocks_0(
            tc, TransactionController.ISOLATION_READ_COMMITTED);

		REPORT("(updatelocks ending.)");

        return(true);
    }

	protected boolean updatelocks_0(
    TransactionController tc,
    int                   isolation_level)
		throws StandardException, T_Fail
	{
		// Create a 2 column row.
		T_AccessRow r1 = new T_AccessRow(2);
		SQLInteger c1 = new SQLInteger(1);
		SQLInteger c2 = new SQLInteger(100);
		r1.setCol(0, c1);
		r1.setCol(1, c2);

		// Create a heap conglomerate.
		long orig_conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                r1.getRowArray(),
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

        // add rows 1 and 2
        ConglomerateController cc = 
            tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // insert (1, 100)
		r1.setCol(0, new SQLInteger(1));
		r1.setCol(1, new SQLInteger(100));
        cc.insert(r1.getRowArray());

        // insert (2, 200)
		r1.setCol(0, new SQLInteger(2));
		r1.setCol(1, new SQLInteger(200));
        cc.insert(r1.getRowArray());

        // insert (3, 300)
		r1.setCol(0, new SQLInteger(3));
		r1.setCol(1, new SQLInteger(300));
        cc.insert(r1.getRowArray());

        cc.close();

        tc.commit();
        
		REPORT("(updatelocks ending.)");

		ScanController sc = tc.openScan(
			orig_conglomid,
			false, // don't hold
			(TransactionController.OPENMODE_FORUPDATE |
			 TransactionController.OPENMODE_USE_UPDATE_LOCKS),
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        int key_value;

        boolean found_row_2 = false;

        while (sc.next())
        {
            sc.fetch(r1.getRowArray());

            key_value = ((SQLInteger) r1.getCol(0)).getInt();

            switch(key_value)
            {
                case 1:
                {
                    // delete first row
                    sc.delete();
                    break;
                }
                    
                    
                case 2:
                {
                    // leave second alone - no update, lock will get coverted 
                    // down.
                    found_row_2 = true;
                    break;
                }

                case 3:
                {
                    // update the third row.
                    T_AccessRow update_row = new T_AccessRow(2);
                    r1.setCol(0, new SQLInteger(30));
                    r1.setCol(1, new SQLInteger(3000));
                    sc.replace(r1.getRowArray(), null);
                    break;
                }

                default:
                {
                    throw T_Fail.testFailMsg(
                        "(updatelock) bad row value found in table.");
                }
            }
            
        }

        if (!found_row_2)
            throw T_Fail.testFailMsg(
                "(updatelock) did not find row in first scan.");

        // reposition the scan
		sc.reopenScan(
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.


        found_row_2 = false;

        while (sc.next())
        {
            sc.fetch(r1.getRowArray());

            key_value = ((SQLInteger) r1.getCol(0)).getInt();

            switch(key_value)
            {
                case 2:
                {
                    // leave second alone - no update, lock will get coverted 
                    // down.
                    found_row_2 = true;

                    break;
                }

                case 30:
                {
                    // update the third row.
                    T_AccessRow update_row = new T_AccessRow(2);
                    r1.setCol(0, new SQLInteger(40));
                    r1.setCol(1, new SQLInteger(4000));
                    sc.replace(r1.getRowArray(), null);
                    break;
                }

                default:
                {
                    throw T_Fail.testFailMsg(
                        "(updatelock) bad row value found in table.");
                }
            }
            
        }

        if (!found_row_2)
            throw T_Fail.testFailMsg(
                "(updatelock) did not find row in second scan.");

        sc.close();

        tc.commit();

        // try the scan after the first xact has completed.

		sc = tc.openScan(
			orig_conglomid,
			false, // don't hold
			(TransactionController.OPENMODE_FORUPDATE |
			 TransactionController.OPENMODE_USE_UPDATE_LOCKS),
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        found_row_2 = false;

        while (sc.next())
        {
            sc.fetch(r1.getRowArray());

            key_value = ((SQLInteger) r1.getCol(0)).getInt();

            switch(key_value)
            {
                case 2:
                {
                    // leave second alone - no update, lock will get coverted 
                    // down.
                    found_row_2 = true;
                    break;
                }

                case 40:
                {
                    // update the third row.
                    T_AccessRow update_row = new T_AccessRow(2);
                    r1.setCol(0, new SQLInteger(30));
                    r1.setCol(1, new SQLInteger(3000));
                    sc.replace(r1.getRowArray(), null);
                    break;
                }

                default:
                {
                    throw T_Fail.testFailMsg(
                        "(updatelock) bad row value found in table.");
                }
            }
            
        }

        if (!found_row_2)
            throw T_Fail.testFailMsg(
                "(updatelock) did not find row in first scan.");

        return(true);
    }


	protected boolean nestedUserTransaction(TransactionController tc)
		throws StandardException, T_Fail
	{

		REPORT("(nestedUserTransaction) starting");

        // Test of drop conglomerate with abort by doing the following:
        //     create table
        //     commit
        //     drop table
        //     make sure table is not still there.
        //     abort
        //     make sure table is still there.

		// Create a heap conglomerate.
		long orig_conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                new T_AccessRow(1).getRowArray(), // 1 SQLInteger() column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

		// Create a temporary heap conglomerate.
		long tmp_conglomid = 
            tc.createConglomerate(
                "heap",         // create a heap conglomerate
                new T_AccessRow(1).getRowArray(),   // 1 SQLInteger() column template.
				null, 	// column sort order not required for heap
                null,           // default properties
                TransactionController.IS_TEMPORARY);

        TransactionController current_xact = 
            store.getTransaction(
                ContextService.getFactory().getCurrentContextManager());

        // get a nested user transaction
        TransactionController child_tc = tc.startNestedUserTransaction(true);

        TransactionController current_xact_after_nest = 
            store.getTransaction(
                ContextService.getFactory().getCurrentContextManager());

        if (current_xact_after_nest != current_xact)
        {
			throw T_Fail.testFailMsg(
                "(nestedUserTransaction) getTransaction() return changed after startNestedUserTransaction()." +
                "current_xact = " + current_xact +
                ";current_xact_after_nest = "  + current_xact_after_nest);
        }

        if ((tc.getLockObject() != child_tc.getLockObject()) ||
            !(tc.getLockObject().equals(child_tc.getLockObject())))

        {
			throw T_Fail.testFailMsg(
                "(nestedUserTransaction) getLockObject should return same object from botht these calls.");
        }

        // the locks of the nested transaction should not conflict, so this
        // open should work.
        ConglomerateController cc = 
            child_tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // Make sure you can access the temporary conglomerate in the 
        // nested transaction.
        ConglomerateController tmp_cc = 
            child_tc.openConglomerate(
                tmp_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        cc.close();
        tmp_cc.close();

        child_tc.commit();
        child_tc.destroy();

        tc.dropConglomerate(orig_conglomid);

        // trying to double nest a nested transaction should not work.
        child_tc = tc.startNestedUserTransaction(true);

        try
        {
            child_tc.startNestedUserTransaction(true);

			throw T_Fail.testFailMsg(
                "(nestedUserTransaction) double nest xact not allowed.");
        }
        catch (StandardException se)
        {
            // expected exception, fall through.
        }

        child_tc.commit();
        child_tc.destroy();

        // make sure internal and ntt's work.  Just add a bunch of data to
        // the table causing page allocation.
        String twok_string = 
            new String("0123456789012345");

        for (int i = 0; i < 7; i++)
        {
            twok_string += twok_string;
        }

        T_AccessRow big_row = new T_AccessRow(2);
        
        big_row.setCol(1, new SQLChar(twok_string));

		// Create a heap conglomerate.
		orig_conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                big_row.getRowArray(),
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

        child_tc = tc.startNestedUserTransaction(true);

        // add 20 pages worth of data, causing allocation
        
        // the locks of the nested transaction should not conflict, so this
        // open should work.
        cc = 
            child_tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        child_tc.abort();
        child_tc.destroy();

        // after the abort the table should not still exist, the parent 
        // xact should have been aborted.

        try 
        {
            // the locks of the nested transaction should not conflict, so this
            // open should work.
            cc = 
                tc.openConglomerate(
                    orig_conglomid, 
                    false,
                    TransactionController.OPENMODE_FORUPDATE, 
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

			throw T_Fail.testFailMsg(
                "(nestedUserTransaction) conglom should have been aborted.");
        }
        catch (StandardException se)
        {
            // expected exception, fall through.
        }

        tc.commit();

        // same test as above, but this time commit parent xact create to
        // make sure it stays around after the child abort.

		// Create a heap conglomerate.
		orig_conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                big_row.getRowArray(),
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

        tc.commit();


        child_tc = tc.startNestedUserTransaction(true);

        // add 20 pages worth of data, causing allocation
        
        // the locks of the nested transaction should not conflict, so this
        // open should work.
        cc = 
            child_tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        /*
        for (int i = 0; i < 40; i++)
        {
            big_row.setCol(0, new SQLInteger(i));
			cc.insert(big_row.getRowArray());
        }
        */

        child_tc.abort();
        child_tc.destroy();

        // after the abort the table should not still exist, the parent 
        // xact should have been aborted.

        try 
        {
            // the locks of the nested transaction should not conflict, so this
            // open should work.
            cc = 
                tc.openConglomerate(
                    orig_conglomid, 
                    false,
                    TransactionController.OPENMODE_FORUPDATE, 
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

            cc.close();
        }
        catch (StandardException se)
        {
            // expected exception, fall through.

			throw T_Fail.testFailMsg(
                "(nestedUserTransaction) conglom should have not be aborted.");
        }

        // start an read only nested user transaction.
        child_tc = tc.startNestedUserTransaction(true);

        ConglomerateController child_cc = 
            child_tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        try 
        {
            // should not be able to do an update in a read only transaction.
            big_row.setCol(0, new SQLInteger(1042));
			child_cc.insert(big_row.getRowArray());

			throw T_Fail.testFailMsg(
                "(nestedUserTransaction) read only xact does not allow upd.");

        }
        catch (StandardException se)
        {
            // expected exception, fall through.
            child_tc.commit();
            child_tc.destroy();
        }

        tc.commit();

        // start an update nested user transaction.
        child_tc = tc.startNestedUserTransaction(false);

        child_cc = 
            child_tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        try 
        {
            // should be able to do an update in a read only transaction.
            big_row.setCol(0, new SQLInteger(1043));
			child_cc.insert(big_row.getRowArray());


        }
        catch (StandardException se)
        {
			throw T_Fail.testFailMsg(
                "(nestedUserTransaction) read only xact does not allow upd.");
        }

        // expected exception, fall through.
        child_tc.commit();
        child_tc.destroy();

        tc.commit();


        cc = 
            tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE);

        // start an update nested user transaction.
        child_tc = tc.startNestedUserTransaction(false);

        try 
        {
            // the following should time out, since locks are not compatible.
            child_cc = 
                child_tc.openConglomerate(
                    orig_conglomid, 
                    false,
                    TransactionController.OPENMODE_FORUPDATE, 
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

			throw T_Fail.testFailMsg(
                "(nestedUserTransaction) lock should have timed out.");
        }
        catch (StandardException se)
        {
            // expected timeout, fall through.
        }

        // expected exception, fall through.
        child_tc.commit();
        child_tc.destroy();

        tc.commit();

		REPORT("(nestedUserTransaction) finishing");

		return true;
	}

	// test various flavors of commit
	protected boolean commitTest(TransactionController tc)
		throws StandardException, T_Fail
	{
		REPORT("(commitTest)");

		// Create a heap conglomerate.
        T_AccessRow template_row = new T_AccessRow(1);
		long conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                template_row.getRowArray(), // 1 column template.
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

		tc.commit();

		// Open it.
		ConglomerateController cc =	
            tc.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row.
		T_AccessRow r1 = new T_AccessRow(1);
		SQLInteger c1 = new SQLInteger(0);
		r1.setCol(0, c1);

		// Get a location template
		RowLocation rowloc = cc.newRowLocationTemplate();

		// Insert the row and remember its location.
		cc.insertAndFetchLocation(r1.getRowArray(), rowloc);

		// now commit nosync without releasing the row lock
		tc.commitNoSync(TransactionController.KEEP_LOCKS);

		// cc should be closed
		try
		{
			cc.newRowLocationTemplate();
			throw T_Fail.testFailMsg("conglomerate controller is not closed after commit");
		}				
		catch (StandardException se)
		{
			// expect fail
		}


		// get another transaction going
		ContextManager cm2 = ContextService.getFactory().newContextManager();

		ContextService.getFactory().setCurrentContextManager(cm2);

		TransactionController tc2 = null;
		ConglomerateController cc2 = null;
		try {

			tc2 = store.getTransaction(cm2);

			cc2 = 
            tc2.openConglomerate(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// try to lock the row, it should fail
			// Mikem RESOLVE: this does not get a row lock
			// cc2.fetch(rowloc, r1.getRowArray(), (FormatableBitSet)null)

			cc2.delete(rowloc);
			throw T_Fail.testFailMsg("expected time out did not happen");
		}
		catch (StandardException lfe)
		{
			if (!lfe.getMessageId().equals(SQLState.LOCK_TIMEOUT))
				throw lfe;
		}
		finally {
			ContextService.getFactory().resetCurrentContextManager(cm2);
		}


		// see whether anyone is blocked at all
		if (tc.anyoneBlocked())
		{
			throw T_Fail.testFailMsg(
								"No transactions should be blocked");
		}

		// now really commit the transaction
		tc.commit();
		
		ContextService.getFactory().setCurrentContextManager(cm2);

		try {
		cc2.fetch(rowloc, r1.getRowArray(), (FormatableBitSet)null);
		// get rid of the other transaction 
		tc2.commitNoSync(TransactionController.RELEASE_LOCKS);
		tc2.destroy();
		}
		finally {
			ContextService.getFactory().resetCurrentContextManager(cm2);
		}

		REPORT("(commitTest) succeeded");
		return true;
	}

    private void testOpsBeforeFirstNext(
    ScanController          scan,
    DataValueDescriptor[]   row)
        throws StandardException, T_Fail
    {

        // none of the following operations is allowed before a next() or
        // fetchNext() is called.
        //
        try
        {
            scan.delete();
			throw T_Fail.testFailMsg(
                "(holdCursor) delete() does not work until next() is called.");
        }
		catch (StandardException lfe)
        {
            // expect error
        }
        try
        {
            scan.doesCurrentPositionQualify();
			throw T_Fail.testFailMsg(
                "(holdCursor) doesCurrentPositionQualify() does not work until next() is called.");
        }
		catch (StandardException lfe)
        {
            // expect error
        }

        try
        {
            scan.fetch(row);
			throw T_Fail.testFailMsg(
                "(holdCursor) fetch() does not work until next() is called.");
        }
		catch (StandardException lfe)
        {
            // expect error
        }
        try
        {
			scan.fetchLocation(null);
			throw T_Fail.testFailMsg(
                "(holdCursor) fetchLocation() does not work until next() is called.");
        }
		catch (StandardException lfe)
        {
            // expect error
        }
        try
        {
            scan.isCurrentPositionDeleted();
			throw T_Fail.testFailMsg(
                "(holdCursor) isCurrentPositionDeleted() does not work until next() is called.");
        }
		catch (StandardException lfe)
        {
            // expect error
        }
        try
        {
            scan.replace(row, null);
			throw T_Fail.testFailMsg(
                "(holdCursor) isCurrentPositionDeleted() does not work until next() is called.");
        }
		catch (StandardException lfe)
        {
            // expect error
        }
    }

	// test various flavors of commit
	protected boolean holdCursor(TransactionController tc)
		throws StandardException, T_Fail
	{
		REPORT("(holdCursor)");

		// Create a conglomerates and an index on that conglomerate.
		long base_id = createAConglom(tc, 0, false);

		// Open it.
		ConglomerateController cc =	
            tc.openConglomerate(
                base_id, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // insert 5 rows

        T_AccessRow r1        = null;
		SQLLongint c1   = null;

        for (int i = 1; i < 5; i++)
        {
            // Create a row.
            r1  = new T_AccessRow(1);
            c1  = new SQLLongint(i);
            r1.setCol(0, c1);

            // Get a location template
            RowLocation rowloc = cc.newRowLocationTemplate();

            // Insert the row and remember its location.
            cc.insertAndFetchLocation(r1.getRowArray(), rowloc);
        }
        

		// Create an index on the base table.
		long index_id = createBtree(tc, base_id, false);

		tc.commit();

        cc.close();

        tc.commit();


        // HEAP - test that scan is closed on commit of non-held cursor.

		// Open scan on the base table.
		ScanController base_scan = tc.openScan(
			base_id,
			false, // don't hold
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

		// Open scan on the index table.
		ScanController index_scan = tc.openScan(
			index_id,
			false, // don't hold
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        testOpsBeforeFirstNext(base_scan, r1.getRowArray());
        testOpsBeforeFirstNext(index_scan, r1.getRowArray());

        base_scan.next();
        index_scan.next();

        base_scan.next();
        index_scan.next();

        base_scan.next();
        index_scan.next();

        base_scan.next();
        index_scan.next();

        base_scan.next();
        index_scan.next();

        base_scan.next();
        index_scan.next();

        base_scan.next();
        index_scan.next();

        // should be able call get and set even after next'ing through the rows.
        long row_count = base_scan.getEstimatedRowCount();
        base_scan.setEstimatedRowCount(10);
        row_count = base_scan.getEstimatedRowCount();

        // should be able call get and set even after next'ing through the rows.
        row_count = index_scan.getEstimatedRowCount();
        index_scan.setEstimatedRowCount(10);
        row_count = index_scan.getEstimatedRowCount();

        if (row_count != 10)
            throw T_Fail.testFailMsg(
                "(holdCursor) some problem with get/set row count.");

        tc.commit();

        testOpsBeforeFirstNext(base_scan, r1.getRowArray());
        testOpsBeforeFirstNext(index_scan, r1.getRowArray());


		// see if commit closed the base_scan.
        if (base_scan.next())
			throw T_Fail.testFailMsg(
                "(holdCursor) next() should return false, commit should close base_scan.");

		// see if commit closed the base_scan.
        if (index_scan.next())
			throw T_Fail.testFailMsg(
                "(holdCursor) next() should return false, commit should close base_scan.");


        tc.commit();


		// Open another scan on the conglomerate.
		base_scan = tc.openScan(
			base_id,
			true, // hold cursor open across commit
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

		// Open scan on the index table.
		index_scan = tc.openScan(
			index_id,
			true, // don't hold
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        tc.commit();

        testOpsBeforeFirstNext(base_scan, r1.getRowArray());
        testOpsBeforeFirstNext(index_scan, r1.getRowArray());

		// try to move scan after commit to 1st row.
        // move cursor to be positioned on 0
        if (!base_scan.next())
            throw T_Fail.testFailMsg(
                "(holdCursor) next() should not fail, commit should close hold scan.");
		// try to move scan after commit to 1st row.
        // move cursor to be positioned on 0
        if (!index_scan.next())
            throw T_Fail.testFailMsg(
                "(holdCursor) next() should not fail, commit should close hold scan.");


        // the 1st next should return the 1st row - ie. 0

        base_scan.fetch(r1.getRowArray());
        long key_value = ((SQLLongint) r1.getCol(0)).getLong();

        if (key_value != 0)
            throw T_Fail.testFailMsg(
                "(holdCursor) 1st row is not 0.");

        index_scan.fetch(r1.getRowArray());
        key_value = ((SQLLongint) r1.getCol(0)).getLong();

        if (key_value != 0)
            throw T_Fail.testFailMsg(
                "(holdCursor) 1st row is not 0.");

        // move cursor to be positioned on 1
        base_scan.next();
        index_scan.next();

        tc.commit();

        testOpsBeforeFirstNext(base_scan, r1.getRowArray());
        testOpsBeforeFirstNext(index_scan, r1.getRowArray());

        // should be able call get and set even after next'ing through the rows.
        row_count = base_scan.getEstimatedRowCount();
        base_scan.setEstimatedRowCount(5);
        row_count = base_scan.getEstimatedRowCount();

        // should be able call get and set even after next'ing through the rows.
        row_count = index_scan.getEstimatedRowCount();
        index_scan.setEstimatedRowCount(5);
        row_count = index_scan.getEstimatedRowCount();

        if (row_count != 5)
            throw T_Fail.testFailMsg(
                "(holdCursor) some problem with get/set row count.");


		// try to move to row with key "2"

        // move cursor to be positioned on 2
        if (!base_scan.next())
            throw T_Fail.testFailMsg(
                "(holdCursor) next() should not fail, commit should close hold base_scan.");

        if (!index_scan.next())
            throw T_Fail.testFailMsg(
                "(holdCursor) next() should not fail, commit should close hold base_scan.");


        // the 1st next should return the 1st row - ie. 0
        base_scan.fetch(r1.getRowArray());
        key_value = ((SQLLongint) r1.getCol(0)).getLong();

        if (key_value != 2)
            throw T_Fail.testFailMsg(
                "(holdCursor) 1st row is not 0.");

        index_scan.fetch(r1.getRowArray());
        key_value = ((SQLLongint) r1.getCol(0)).getLong();

        if (key_value != 2)
            throw T_Fail.testFailMsg(
                "(holdCursor) 1st row is not 0.");


        // move cursor to be positioned on 3
        base_scan.next();
        base_scan.delete();

        index_scan.next();
        index_scan.delete();


        // move cursor to be positioned on 4
        base_scan.next();
        index_scan.next();

        // move cursor past the end, thus closing it.
        base_scan.next();
        index_scan.next();

        tc.commit();

        // should be able call get and set even after next'ing through the rows.
        row_count = base_scan.getEstimatedRowCount();
        base_scan.setEstimatedRowCount(15);
        row_count = base_scan.getEstimatedRowCount();

        if (row_count != 15)
            throw T_Fail.testFailMsg(
                "(holdCursor) some problem with get/set row count.");

        row_count = index_scan.getEstimatedRowCount();
        index_scan.setEstimatedRowCount(15);
        row_count = index_scan.getEstimatedRowCount();

        if (row_count != 15)
            throw T_Fail.testFailMsg(
                "(holdCursor) some problem with get/set row count.");

        testOpsBeforeFirstNext(base_scan, r1.getRowArray());
        testOpsBeforeFirstNext(index_scan, r1.getRowArray());

		// see what happens committing a closed base_scan and trying next.

        if (base_scan.next())
            throw T_Fail.testFailMsg(
                "(holdCursor) next() should fail, the base_scan has been closed by progressing to end.");
        if (index_scan.next())
            throw T_Fail.testFailMsg(
                "(holdCursor) next() should fail, the base_scan has been closed by progressing to end.");


        tc.commit();

        base_scan.close();
        index_scan.close();
        
		REPORT("(holdCursor) succeeded");
		return true;
	}

    /**
     * Test critical cases for read uncommitted.
     * <p>
     * test 1 - test heap fetch of row on page which does not exist.  
     * test 2 - test heap fetch of row on page where row does not exist.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected boolean readUncommitted(TransactionController tc)
		throws StandardException, T_Fail
	{
		REPORT("(readUncommitted)");

        /*
         * TEST 1 - test heap fetch of row on page which does not exist.
         * <p>
         * Do this by inserting a few pages worth of data and then deleting 
         * all the rows, while remembering the rowlocation of one of the pages.
         * You need to at least get to the 2nd page, because the 1st page is
         * never totally reclaimed and deleted by the system in a heap (it has
         * some internal catalog information stored internally in row "0").
         */

        String twok_string = 
            new String("0123456789012345");

        for (int i = 0; i < 7; i++)
        {
            twok_string += twok_string;
        }

        T_AccessRow big_row = new T_AccessRow(2);
        
        big_row.setCol(1, new SQLChar(twok_string));

		// Create a heap conglomerate.
		long orig_conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                big_row.getRowArray(),
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

		ConglomerateController cc =	
            tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_READ_UNCOMMITTED);

        // add 5 pages worth of data.

        for (int i = 0; i < 10; i++)
        {
            big_row.setCol(0, new SQLInteger(i));
			cc.insert(big_row.getRowArray());
        }
        cc.close();

		// Open another scan on the conglomerate.
		ScanController base_scan = tc.openScan(
			orig_conglomid,
			true, // hold cursor open across commit
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        // now delete all the rows and remember the row location of the
        // last row.

        RowLocation deleted_page_rowloc = base_scan.newRowLocationTemplate();


        for (int i = 0; i < 10; i++)
        {
            base_scan.next();
            base_scan.fetchLocation(deleted_page_rowloc);
            base_scan.delete();

            tc.commit();
        }
        base_scan.close();
        tc.commit();

        // at this point the post commit thread should have reclaimed all the 5
        // pages.  Open it, at read uncommitted level.
		cc = tc.openConglomerate(
                orig_conglomid, 
                false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_READ_UNCOMMITTED);

        if (cc.fetch(deleted_page_rowloc, big_row.getRowArray(), null))
        {
            throw T_Fail.testFailMsg(
                "(readUncommitted) fetch should ret false for reclaimed page.");
        }
        cc.close();

        /*
         * TEST 2 - test heap fetch of row on page where row does not exist.
         * <p>
         * Do this by inserting enough rows to put 1 row on the 2nd page.
         * Then delete this one row, which will queue a post commit to reclaim
         * the row and page.  Then insert one more row on the same page in
         * the same xact.  Now commit the xact, which will cause post commit
         * to run which will reclaim the row but not the page.  Then try and
         * fetch the row which was deleted.
         */

        // string column will be 1500 bytes, allowing 2 rows per page to fit.
        SQLChar stringcol = new SQLChar();
        stringcol.setValue(T_AccessFactory.repeatString("012345678901234", 100));
        big_row.setCol(1, stringcol);

		// Create a heap conglomerate.
		orig_conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                big_row.getRowArray(),
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

		cc =	
            tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_READ_UNCOMMITTED);

        // add 3 rows, should result in 1 row on second page.

        for (int i = 0; i < 3; i++)
        {
            big_row.setCol(0, new SQLInteger(i));
			cc.insert(big_row.getRowArray());
        }

		// Open another scan on the conglomerate.
		base_scan = tc.openScan(
			orig_conglomid,
			true, // hold cursor open across commit
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        // now delete all the rows and remember the row location of the
        // last row.

        RowLocation deleted_row_rowloc  = base_scan.newRowLocationTemplate();

        for (int i = 0; i < 3; i++)
        {
            base_scan.next();
            base_scan.fetchLocation(deleted_row_rowloc);
            base_scan.delete();
        }

        // insert another row on page 2 to make sure page does not go away.
        cc.insert(big_row.getRowArray());

        cc.close();
        base_scan.close();
        tc.commit();

        // at this point the post commit thread should have reclaimed all the 
        // deleted row on page 2, but not the page.
        //
		// Open it, at read uncommitted level.
		cc = tc.openConglomerate(
                orig_conglomid, 
                false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_READ_UNCOMMITTED);

        // the following will be attempting to fetch a row which has been
        // reclaimed by post commit, on an existing page.

        if (cc.fetch(deleted_row_rowloc, big_row.getRowArray(), null))
        {
            throw T_Fail.testFailMsg(
                "(readUncommitted) fetch should ret false for reclaimed row.");
        }
        cc.close();

        /*
         * TEST 3 - test heap scan fetch of row on page prevents page from 
         *          disappearing, but handles row being deleted.
         * <p>
         * A heap scan will maintain a scan lock on a page even if it is doing
         * a read uncommitted scan.  This will prevent the row/page from being
         * reclaimed by post commit while the scan is positioned on the page. 
         * This presents no other concurrency issues for read uncommitted, it
         * should be invisible to the user (deletes can still happen and the
         * read uncommitted scanner will not block anyone).
         *
         * You need to at least get to the 2nd page, because the 1st page is
         * never totally reclaimed and deleted by the system in a heap (it has
         * some internal catalog information stored internally in row "0").
         */

        big_row = new T_AccessRow(2);
        
        big_row.setCol(1, new SQLChar(twok_string));

		// Create a heap conglomerate.
		orig_conglomid = 
            tc.createConglomerate(
                "heap",       // create a heap conglomerate
                big_row.getRowArray(),
				null, 	// column sort order not required for heap
                null,         // default properties
                TransactionController.IS_DEFAULT);       // not temporary

		cc =	
            tc.openConglomerate(
                orig_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_READ_UNCOMMITTED);

        // add 5 pages worth of data.

        for (int i = 0; i < 10; i++)
        {
            big_row.setCol(0, new SQLInteger(i));
			cc.insert(big_row.getRowArray());
        }
        cc.close();

		// Open scan on the conglomerate, and position it on the second page.
		base_scan = tc.openScan(
			orig_conglomid,
			true, // hold cursor open across commit
            0,    // for read
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_READ_UNCOMMITTED,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        base_scan.next();
        base_scan.next();
        base_scan.next();

        // test that the row is accessible from all these interfaces, before
        // the page disappears, and then test again after all rows have been
        // deleted from the page.

        if (!base_scan.doesCurrentPositionQualify())
			throw T_Fail.testFailMsg(
                "(readUncommitted) doesCurrentPositionQualify() failed.");

        base_scan.fetch(big_row.getRowArray());

        base_scan.fetchLocation(deleted_row_rowloc);

        if (base_scan.isCurrentPositionDeleted())
			throw T_Fail.testFailMsg(
                    "(readUncommitted) isCurrentPositionDeleted() failed.");

		// Open another scan on the conglomerate.
		ScanController delete_scan = tc.openScan(
			orig_conglomid,
			true, // hold cursor open across commit
			TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_SERIALIZABLE,
			(FormatableBitSet) null, // all columns, all as objects
			null, // start position - first row in conglomerate
            0,    // unused if start position is null.
			null, // qualifier - accept all rows
			null, // stop position - last row in conglomerate
            0);   // unused if stop position is null.

        // now delete all the rows and commit.

        for (int i = 0; i < 10; i++)
        {
            delete_scan.next();
            delete_scan.fetchLocation(deleted_page_rowloc);
            delete_scan.delete();

        }
        delete_scan.close();

        // Now test that the row is accessible from all these interfaces, after
        // it has been deleted.

        if (base_scan.doesCurrentPositionQualify())
			throw T_Fail.testFailMsg(
                "(readUncommitted) doesCurrentPositionQualify() failed.");

        try
        {
            base_scan.fetch(big_row.getRowArray());
            throw T_Fail.testFailMsg(
                "(readUncommitted) fetch of deleted row should throw exception.");
        }
        catch (StandardException se)
        {
            // expect SQLState.AM_RECORD_NOT_FOUND exception.

            if (!se.getMessageId().equals(SQLState.AM_RECORD_NOT_FOUND))
            {
                throw T_Fail.testFailMsg(
                    "(readUncommitted) fetch of deleted row should throw SQLState.AM_RECORD_NOT_FOUND.");
            }
        }

        base_scan.fetchLocation(deleted_row_rowloc);

        if (!base_scan.isCurrentPositionDeleted())
			throw T_Fail.testFailMsg(
                    "(readUncommitted) isCurrentPositionDeleted() failed.");

        base_scan.close();
        tc.commit();

		REPORT("(readUncommitted) succeeded");
		return true;
	}

	public static String repeatString(String data, int repeat) {

		String s = data;
		for (int i = 1; i < repeat; i++)
			s += data;

		return s;
	}

}
