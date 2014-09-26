/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_SortController

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

// impl imports are the preferred way to create unit tests.
import org.apache.derbyTesting.unitTests.harness.T_Generic;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.store.access.*;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.types.SQLInteger;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.Vector;
import java.util.StringTokenizer;
import java.io.File;
import org.apache.derby.shared.common.sanity.SanityManager;

/**

  Unit test for sorting.

**/

public class T_SortController extends T_Generic
{
	private static final String testService = "sortTest";

	/** Set this to print out the rows that are inserted into
	 ** and returned from each sort. **/
	protected boolean verbose = false;

	public String getModuleToTestProtocolName() {
		return AccessFactory.MODULE;
	}

	private void setSortBufferSize(final String buf_length) {
    	AccessController.doPrivileged(new PrivilegedAction<Void>() {
		    public Void run()  {
		    	System.setProperty("derby.storage.sortBufferMax", buf_length);
		    	return null;
		    }
	    });
	}

	/*
	** Methods of T_SortController
	*/

	/**
		@exception T_Fail test has failed
	*/
	protected void runTests() throws T_Fail
	{
		int failcount = 0;

		// Get the AccessFactory to test.

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
			REPORT("(unitTestMain) Testing " + "sortTest with default sort buffer size 1024");
			AccessFactory store1024 = null;
			failcount = runEachTest(store1024, "1024");

			setSortBufferSize("4");
			REPORT("(unitTestMain) Testing " + "sortTest with minimum sort buffer size 4");
			AccessFactory store4 = null;
			failcount += runEachTest(store4, "4");
		}
		catch (StandardException e)
		{
			String  msg = e.getMessage();
			if (msg == null)
				msg = e.getClass().getName();
			REPORT("(unitTestMain) unexpected exception: " + msg);
			throw T_Fail.exceptionFail(e);
		}

		if (failcount != 0)
			throw T_Fail.testFailMsg("(unitTestMain)" + failcount + " cases failed.");

		REPORT("(unitTestMain) succeeded");
	}

	protected int runEachTest(AccessFactory store, String tail) throws T_Fail, StandardException {

		TransactionController tc = null;
		int failcount = 0;

		try {
			store = (AccessFactory) createPersistentService(getModuleToTestProtocolName(), 
				testService + tail, startParams);
		} catch (StandardException mse) {
			throw T_Fail.exceptionFail(mse);
		}
		if (store == null) {
			throw T_Fail.testFailMsg(getModuleToTestProtocolName() + " service not started.");
		}

		tc = store.getTransaction(
                getContextService().getCurrentContextManager());

		if (!sortExample(tc))
			failcount++;
		if (!sortBufferCoverage(tc))
			failcount++;
		if (!sortBoundaries(tc))
			failcount++;
		if (!sortAllDuplicates(tc))
			failcount++;
		if (!sortDescending(tc))
			failcount++;

		tc.commit();
		tc.destroy();

		return failcount;
	}

	/**
	This test is more of an example, with lots of comments to
	explain what's going on.
	**/
	boolean sortExample(TransactionController tc)
		throws StandardException
	{
		REPORT("(sortExample)");

		// Create the rows to be sorted.
		T_AccessRow row[] = new T_AccessRow[4];
		row[0] = new T_AccessRow(18,  1,  2);
		row[1] = new T_AccessRow( 6,  1, 18);
		row[2] = new T_AccessRow(18,  1,  2);
		row[3] = new T_AccessRow( 8, 14,  3);

		// Decide on what kind of sort we want.  The properties
		// can select different sorting techniques and options.
		// But all sorts will result in the rows being in order.
		// We don't care which sort technique is used, so set
		// the properties to null.
		Properties implParameters = null;

		// Define the type of rows to be sorted by constructing
		// a template.  Any row with the correct column types
		// will do (the values in the template are never used,
		// just the types).  The first row to be inserted will
		// make a good template.
		T_AccessRow template = row[0];

		// Define the column ordering: sort on column 1
		// (the second column) ascending, then column 2
		// (the third column) ascending.
		ColumnOrdering order[] = new ColumnOrdering[2];
		order[0] = new T_ColumnOrderingImpl(1, true); // ascending
		order[1] = new T_ColumnOrderingImpl(2, true); // ascending

		// Tell the sort that the rows are not already in order.
		boolean alreadyInOrder = false;

		// Tell the sort that we're estimating that about 10
		// rows will be inserted into the sort.  This is just
		// a hint, the sort will still work if more rows or
		// fewer rows are inserted.  But if the guess is close
		// the sort will probably run faster.
		long estimatedRows = 10;

		// Tell the sort that we're estimating that the rows 
        // are about 24 bytes long (3 int columns).
		// This is just a hint, the sort will still work if rows of
		// less or greater size are inserted.  But if the guess is close
		// the sort will probably run faster.
		int estimatedRowSize = 12;

		// Create the sort.
		long sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DuplicateEliminator(template), alreadyInOrder, estimatedRows, 
            estimatedRowSize);

		// For the above sort, on the above input rows, we expect
		// the output rows to look like this:
		T_AccessRow expectedRow[] = new T_AccessRow[3];
		expectedRow[0] = new T_AccessRow(18,  1,  2);
		expectedRow[1] = new T_AccessRow( 6,  1, 18);
		expectedRow[2] = new T_AccessRow( 8, 14,  3);

		return testSort(tc, row, expectedRow, sortid);
	}

	/**
	This test covers specific code paths in the external sort's
	sort buffer.  It really should live closer to the sort buffer
	since the effectiveness of this test is very very implementation
	dependent.
	**/
	boolean sortBufferCoverage(TransactionController tc)
		throws StandardException
	{
		REPORT("(sortBufferCoverage)");

		// Create the rows to be sorted.  This sequence of values
		// will provoke both single and double rotations on insert
		// and both single and double rotations on removal.  Every
		// row has a duplicate so that we can test duplicate handling
		// in every tree position and through all manipulations.
		T_AccessRow row[] = new T_AccessRow[16];
		row[0] = new T_AccessRow(2, 0,  0); // first node
		row[1] = new T_AccessRow(2, 0,  0);

		row[2] = new T_AccessRow(4, 0,  0); // This makes the tree get higher [A7 case (i)]
		row[3] = new T_AccessRow(4, 0,  0);

		row[4] = new T_AccessRow(1, 0,  0); // This makes the tree more balanced [A7 case (ii)]
		row[5] = new T_AccessRow(1, 0,  0);

		row[6] = new T_AccessRow(7, 0,  0); // Tree getting higher again [A7 case (i)]
		row[7] = new T_AccessRow(7, 0,  0);

		row[8] = new T_AccessRow(8, 0,  0); // Tree getting out of balance [A7 case iii]
									  // Single rotation will fix	[A8]
		row[9] = new T_AccessRow(8, 0,  0);

		row[10] = new T_AccessRow(3, 0,  0); // Tree getting out of balance [A7 case iii]
									  // Double rotation will fix	[A9]
		row[11] = new T_AccessRow(3, 0,  0);

		row[12] = new T_AccessRow(5, 0,  0); // Tree more balanced [A7 case (ii)]
		row[13] = new T_AccessRow(5, 0,  0);

		row[14] = new T_AccessRow(6, 0,  0); // Tree getting higher again [A7 case (i)]
		row[15] = new T_AccessRow(6, 0,  0);

		// RESOLVE (nat) Should select the sort that being tested here.
		Properties implParameters = null;

		T_AccessRow template = row[0];

		// Sort on column 0 (the first column) ascending
		ColumnOrdering order[] = new ColumnOrdering[1];
		order[0] = new T_ColumnOrderingImpl(0, true); // ascending

		// The rows are not already in order.
		boolean alreadyInOrder = false;

		long estimatedRows = 20;
		int estimatedRowSize = 12;

		// Create the sort.
		long sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DummySortObserver(template), alreadyInOrder, estimatedRows,
            estimatedRowSize);

		// Rows should come out in order
		T_AccessRow expectedRow[] = new T_AccessRow[16];
		expectedRow[0] = new T_AccessRow(1, 0,  0);
		expectedRow[1] = new T_AccessRow(1, 0,  0);
		expectedRow[2] = new T_AccessRow(2, 0,  0);
		expectedRow[3] = new T_AccessRow(2, 0,  0);
		expectedRow[4] = new T_AccessRow(3, 0,  0);
		expectedRow[5] = new T_AccessRow(3, 0,  0);
		expectedRow[6] = new T_AccessRow(4, 0,  0);
		expectedRow[7] = new T_AccessRow(4, 0,  0);
		expectedRow[8] = new T_AccessRow(5, 0,  0);
		expectedRow[9] = new T_AccessRow(5, 0,  0);
		expectedRow[10] = new T_AccessRow(6, 0,  0);
		expectedRow[11] = new T_AccessRow(6, 0,  0);
		expectedRow[12] = new T_AccessRow(7, 0,  0);
		expectedRow[13] = new T_AccessRow(7, 0,  0);
		expectedRow[14] = new T_AccessRow(8, 0,  0);
		expectedRow[15] = new T_AccessRow(8, 0,  0);

		return testSort(tc, row, expectedRow, sortid);
	}


	/**
	Test a sorts with one or zero rows.
	**/
	boolean sortBoundaries(TransactionController tc)
		throws StandardException
	{
		int failcount = 0;
		long sortid;
		Properties implParameters;
		T_AccessRow template;
		ColumnOrdering order[];
		boolean alreadyInOrder;
		long estimatedRows;
		int estimatedRowSize;
		T_AccessRow input[];
		T_AccessRow expected[];

		/*
		** The following sort parameters are the same for
		** every sort tested in this method.
		*/

		implParameters = null;
		template = new T_AccessRow(1, 1, 1);
		order = new ColumnOrdering[1];
		order[0] = new T_ColumnOrderingImpl(0, true); // ascending
		estimatedRows = 10;
		estimatedRowSize = 12;

		/*
		** A no-row sort.
		*/

		REPORT("(sortBoundaries) Sorting no rows");

		input = new T_AccessRow[0];  // no rows in..
		expected = new T_AccessRow[0];  // .. ==> no rows out!
		alreadyInOrder = false;

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DummySortObserver(template), alreadyInOrder, estimatedRows, 
            estimatedRowSize);

		if (!testSort(tc, input, expected, sortid))
			failcount++;

		/*
		** A no-row already in order sort.
		*/

		REPORT("(sortBoundaries) Sorting no rows - \"already in order\"");

		input = new T_AccessRow[0];  // no rows in..
		expected = new T_AccessRow[0];  // .. ==> no rows out!
		alreadyInOrder = true;

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DummySortObserver(template), alreadyInOrder, estimatedRows, 
            estimatedRowSize);

		if (!testSort(tc, input, expected, sortid))
			failcount++;

		/*
		** A single-row sort.
		*/

		REPORT("(sortBoundaries) Sorting a single row");

		input = new T_AccessRow[1];
		input[0] = new T_AccessRow(99, 88, 77);
		expected = new T_AccessRow[1];
		expected[0] = new T_AccessRow(99, 88, 77);
		alreadyInOrder = false;

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DummySortObserver(template), alreadyInOrder, estimatedRows,
            estimatedRowSize);

		if (!testSort(tc, input, expected, sortid))
			failcount++;

		/*
		** A single-row already-in-order sort.
		*/

		REPORT("(sortBoundaries) Sorting a single row - \"already in order\"");

		input = new T_AccessRow[1];
		input[0] = new T_AccessRow(99, 88, 77);
		expected = new T_AccessRow[1];
		expected[0] = new T_AccessRow(99, 88, 77);
		alreadyInOrder = true;

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DummySortObserver(template), alreadyInOrder, estimatedRows, 
            estimatedRowSize);

		if (!testSort(tc, input, expected, sortid))
			failcount++;

		/*
		** A single-row sort, eliminating duplicates
		*/

		REPORT("(sortBoundaries) Sorting a single row - \"eliminate duplicates\"");

		input = new T_AccessRow[1];
		input[0] = new T_AccessRow(99, 88, 77);
		expected = new T_AccessRow[1];
		expected[0] = new T_AccessRow(99, 88, 77);
		alreadyInOrder = false;

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DuplicateEliminator(template), alreadyInOrder, estimatedRows, 
            estimatedRowSize);

		if (!testSort(tc, input, expected, sortid))
			failcount++;

		return failcount == 0;
	}

	/**
	Test a sort where all the rows are duplicates
	**/
	boolean sortAllDuplicates(TransactionController tc)
		throws StandardException
	{
		int failcount = 0;
		long sortid;
		Properties implParameters;
		T_AccessRow template;
		ColumnOrdering order[];
		boolean alreadyInOrder;
		long estimatedRows;
		int estimatedRowSize;
		T_AccessRow input[];
		T_AccessRow expected[];

		/*
		** The following sort parameters will be used in every
		** sort in this method.
		*/
		
		implParameters = null;
		template = new T_AccessRow(1, 1, 1);

		// Ordering first two columns, ascending
		order = new ColumnOrdering[2];
		order[0] = new T_ColumnOrderingImpl(0, true); // ascending
		order[1] = new T_ColumnOrderingImpl(1, true); // ascending

		alreadyInOrder = false;
		estimatedRows = 10;
		estimatedRowSize = 12;

		input = new T_AccessRow[5];
		input[0] = new T_AccessRow(1, 1, 1);
		input[1] = new T_AccessRow(1, 1, 1);
		input[2] = new T_AccessRow(1, 1, 1);
		input[3] = new T_AccessRow(1, 1, 1);
		input[4] = new T_AccessRow(1, 1, 1);

		/*
		** When doing no aggregation, we expect every duplicate
		** to come back out.
		*/

		REPORT("(sortAllDuplicates) no aggregation");

		expected = new T_AccessRow[5];
		expected[0] = new T_AccessRow(1, 1, 1);
		expected[1] = new T_AccessRow(1, 1, 1);
		expected[2] = new T_AccessRow(1, 1, 1);
		expected[3] = new T_AccessRow(1, 1, 1);
		expected[4] = new T_AccessRow(1, 1, 1);

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DummySortObserver(template), alreadyInOrder, estimatedRows, 
            estimatedRowSize);

		if (!testSort(tc, input, expected, sortid))
			failcount++;

		/*
		** If we're doing duplicate elimination, we expect
		** one row back (since they're all duplicates).
		*/
		
		REPORT("(sortAllDuplicates) eliminate duplicates");

		expected = new T_AccessRow[1];
		expected[0] = new T_AccessRow(1, 1, 1);

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DuplicateEliminator(template), alreadyInOrder, estimatedRows, 
            estimatedRowSize);

		if (!testSort(tc, input, expected, sortid))
			failcount++;

		/*
		** Another aggregation, this time summing up the 
		** third column.
		*/

		REPORT("(sortAllDuplicates) sum aggregate");

		expected = new T_AccessRow[1];
		expected[0] = new T_AccessRow(1, 1, 5);

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_SumForIntCol(2), alreadyInOrder, estimatedRows, 
            estimatedRowSize);

		if (!testSort(tc, input, expected, sortid))
			failcount++;

		return failcount == 0;
	}

	/**
	Test a sort where we have some ascending and descending keys.
	**/
	boolean sortDescending(TransactionController tc)
		throws StandardException
	{
		int failcount = 0;
		long sortid;
		Properties implParameters;
		T_AccessRow template;
		ColumnOrdering order[];
		boolean alreadyInOrder;
		long estimatedRows;
		int estimatedRowSize;
		T_AccessRow expected[];

		/*
		** The following sort parameters will be used in every
		** sort in this method.
		*/
		
		implParameters = null;
		template = new T_AccessRow(1, 1, 1);

		alreadyInOrder = false;
		estimatedRows = 10;
		estimatedRowSize = 12;

		/*
		** Straight descending sort.
		*/

		REPORT("(sortDescending) no aggregation");

		order = new ColumnOrdering[2];
		order[0] = new T_ColumnOrderingImpl(0, false); // descending
		order[1] = new T_ColumnOrderingImpl(1, false); // descending

		expected = new T_AccessRow[10];
		expected[0] = new T_AccessRow(8, 1, 1);
		expected[1] = new T_AccessRow(4, 8, 1);
		expected[2] = new T_AccessRow(4, 2, 1);
		expected[3] = new T_AccessRow(4, 1, 1);
		expected[4] = new T_AccessRow(3, 8, 1);
		expected[5] = new T_AccessRow(3, 5, 1);
		expected[6] = new T_AccessRow(3, 3, 1);
		expected[7] = new T_AccessRow(3, 3, 1);
		expected[8] = new T_AccessRow(3, 3, 1);
		expected[9] = new T_AccessRow(1, 1, 1);

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DummySortObserver(template), alreadyInOrder, estimatedRows,
            estimatedRowSize);

		if (!testSort(tc, getSortDescendingInput(), expected, sortid))
			failcount++;

		/*
		** Descending sort eliminating duplicates
		*/

		REPORT("(sortDescending) eliminate duplicates");

		order = new ColumnOrdering[2];
		order[0] = new T_ColumnOrderingImpl(0, false); // descending
		order[1] = new T_ColumnOrderingImpl(1, false); // descending

		expected = new T_AccessRow[8];
		expected[0] = new T_AccessRow(8, 1, 1);
		expected[1] = new T_AccessRow(4, 8, 1);
		expected[2] = new T_AccessRow(4, 2, 1);
		expected[3] = new T_AccessRow(4, 1, 1);
		expected[4] = new T_AccessRow(3, 8, 1);
		expected[5] = new T_AccessRow(3, 5, 1);
		expected[6] = new T_AccessRow(3, 3, 1);
		expected[7] = new T_AccessRow(1, 1, 1);

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DuplicateEliminator(template), alreadyInOrder, estimatedRows,
            estimatedRowSize);

		if (!testSort(tc, getSortDescendingInput(), expected, sortid))
			failcount++;

		/*
		** Eliminate duplicates, ascending on second column.
		*/

		REPORT("(sortDescending) descending/ascending - eliminate duplicates");

		order = new ColumnOrdering[2];
		order[0] = new T_ColumnOrderingImpl(0, false); // descending
		order[1] = new T_ColumnOrderingImpl(1, true); // ascending

		expected = new T_AccessRow[8];
		expected[0] = new T_AccessRow(8, 1, 1);
		expected[1] = new T_AccessRow(4, 1, 1);
		expected[2] = new T_AccessRow(4, 2, 1);
		expected[3] = new T_AccessRow(4, 8, 1);
		expected[4] = new T_AccessRow(3, 3, 1);
		expected[5] = new T_AccessRow(3, 5, 1);
		expected[6] = new T_AccessRow(3, 8, 1);
		expected[7] = new T_AccessRow(1, 1, 1);

		sortid = tc.createSort(implParameters, template.getRowArray(),
			order, new T_DuplicateEliminator(template), alreadyInOrder, estimatedRows,
            estimatedRowSize);

		if (!testSort(tc, getSortDescendingInput(), expected, sortid))
			failcount++;


		return failcount == 0;
	}

	private T_AccessRow[] getSortDescendingInput()
	{
		T_AccessRow[] input;

		input = new T_AccessRow[10];
		input[0] = new T_AccessRow(8, 1, 1);
		input[1] = new T_AccessRow(1, 1, 1);
		input[2] = new T_AccessRow(3, 5, 1);
		input[3] = new T_AccessRow(4, 1, 1);
		input[4] = new T_AccessRow(3, 3, 1);
		input[5] = new T_AccessRow(3, 8, 1);
		input[6] = new T_AccessRow(3, 3, 1);
		input[7] = new T_AccessRow(3, 3, 1);
		input[8] = new T_AccessRow(4, 2, 1);
		input[9] = new T_AccessRow(4, 8, 1);

		return input;
	}


	/**
	Insert the given rows into the given sort, and check that the
	rows retrieved from the sort match the output rows.
	**/
	boolean testSort(TransactionController tc, T_AccessRow in[], T_AccessRow outrow[], long sortid)
		throws StandardException
	{
		// Open a sort controller for inserting the rows.
		SortController sort = tc.openSort(sortid);

		// Insert the rows to be sorted.
		for (int i = 0; i < in.length; i++)
		{
			if (verbose)
				REPORT("(testSort) in: " + in[i]);
			sort.insert(in[i].getRowArray());
		}

		// Close the sort controller.  This makes the rows
		// available to be retrieved.
        // It also means we are getting final sort statistics.
        sort.completedInserts();

        // Test the SortInfo part of sort.
        SortInfo   sort_info = sort.getSortInfo();
        Properties sortprop      = sort_info.getAllSortInfo(null);

        String sortType = sortprop.getProperty(
				MessageService.getTextMessage(SQLState.STORE_RTS_SORT_TYPE));
        int numRowsInput = Integer.parseInt(sortprop.getProperty(
			MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_INPUT)));
        int numRowsOutput = Integer.parseInt(sortprop.getProperty(
			MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_OUTPUT)));

		String external =
			  MessageService.getTextMessage(SQLState.STORE_RTS_EXTERNAL);
		String internal =
			 MessageService.getTextMessage(SQLState.STORE_RTS_INTERNAL);
        if (sortType.compareTo(internal) != 0 &&
            sortType.compareTo(external) != 0)
            FAIL("(testSort) unknown sortType.  Expected internal or external, got " + sortType);

        if (numRowsInput != in.length)
            FAIL("(testSort) SortInfo.numRowsInput (value: " + numRowsInput +
                ") is not equal to in.length (value: " + in.length + ")");

        if (numRowsOutput != outrow.length)
            FAIL("(testSort) SortInfo.numRowsOutput (value: " +
                numRowsOutput + ") is not equal to outrow.length (value: " + outrow.length + ")");

		if (sortType.equals(external))
        {
            int numMergeRuns = Integer.parseInt(sortprop.getProperty(
			 MessageService.getTextMessage(SQLState.STORE_RTS_NUM_MERGE_RUNS)));
            Vector<Integer> mergeRuns = new Vector<Integer>();
            StringTokenizer st = new StringTokenizer(sortprop.getProperty(
			 MessageService.getTextMessage(SQLState.STORE_RTS_MERGE_RUNS_SIZE)),
			 "[],",false);
            while (st.hasMoreTokens())
                mergeRuns.addElement(Integer.valueOf(st.nextToken().trim()));

            if (mergeRuns.size() != numMergeRuns)
                FAIL("(testSort) the number of elements in vector SortInfo.mergeRunsSize (value: " +
                mergeRuns.size() + " ) is not equal to SortInfo.numMergeRuns (value: " +
                numMergeRuns + " )");

            int totRunSize = 0;
            for (int i = 0; i < mergeRuns.size(); i++)
                totRunSize += mergeRuns.elementAt(i);
            if (totRunSize != numRowsInput)
               FAIL("(testSort) the sum of the elements of the vector SortInfo.mergeRunsSize (value: " +
                totRunSize + " ) is not equal to SortInfo.numRowsInput (value: " +
                numRowsInput + " )");
        }

		sort = null;

		// Open a sort scan for reading the rows back.
		ScanController scan = tc.openSortScan(sortid, false);

		// Things that could go wrong.
		boolean mismatch = false;
		boolean toofew = false;
		boolean toomany = false;

		// Fetch the sorted rows and compare them to the rows
		// in the outrow array.
		T_AccessRow result = new T_AccessRow(3);
		for (int i = 0; i < outrow.length; i++)
		{
			if (scan.next() == false)
			{
				// We were expecting the i'th row from outrow, but
				// it didn't show up!
				toofew = true;
				FAIL("(testSort) Too few rows in sort output");
				break;
			}

			scan.fetch(result.getRowArray());
			if (verbose)
				REPORT("(testSort) out: " + result);

			if (!result.equals(outrow[i]))
			{
				// The i'th row from the sort didn't match the
				// i'th row from out.
				mismatch = true;
				FAIL("(testSort) row " + result + " != " + outrow[i]);
			}
		}

		// We should not see any more rows out of the sort,
		// since we've exhausted the out array.
		while (scan.next() == true)
		{
			scan.fetch(result.getRowArray());
			if (verbose)
				REPORT("(testSort) out: " + result);
			toomany = true;
			FAIL("(testSort) Extra row");
		}

        // Test the ScanInfo part of sort.
        ScanInfo   scan_info = scan.getScanInfo();
        Properties prop      = scan_info.getAllScanInfo(null);

        if (prop.getProperty(
				MessageService.getTextMessage(SQLState.STORE_RTS_SCAN_TYPE)
							).compareTo(
				MessageService.getTextMessage(SQLState.STORE_RTS_SORT)) != 0)
        {
            FAIL("(testSort) wrong scanType.  Expected sort, got " +
                prop.getProperty(
					MessageService.getTextMessage(
												SQLState.STORE_RTS_SCAN_TYPE)));
        }

        if (tc.countOpens(TransactionController.OPEN_CREATED_SORTS) != 1)
        {
            FAIL("(testSort) sort count before close is wrong: " +
                 tc.countOpens(TransactionController.OPEN_CREATED_SORTS));
        }

		// Close the scan controller (which implicitly destroys the sort).
		scan.close();
		scan = null;

        if (tc.countOpens(TransactionController.OPEN_CREATED_SORTS) != 1)
        {
            FAIL("(testSort) sort count after close is wrong: " +
                 tc.countOpens(TransactionController.OPEN_CREATED_SORTS));
        }

        tc.dropSort(sortid);

        if (tc.countOpens(TransactionController.OPEN_CREATED_SORTS) > 0)
        {
            FAIL("(testSort) a sort is still open.");
        }

		return (!mismatch && !toofew && !toomany);
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


class T_DummySortObserver implements SortObserver
{
	T_AccessRow  template;
	Vector<DataValueDescriptor[]> vector;

	T_DummySortObserver(T_AccessRow template)
	{
		this.template = template;
		vector = new Vector<DataValueDescriptor[]>();
	}

	/*
	 * Methods of SortObserver
	 */
	public DataValueDescriptor[] insertNonDuplicateKey(
    DataValueDescriptor[] insertRow)	
	{
		return insertRow;
	}

	public DataValueDescriptor[] insertDuplicateKey(
    DataValueDescriptor[]   insertRow, 
    DataValueDescriptor[]   existingRow)
	{
		return insertRow;
	}

	public void addToFreeList(
    DataValueDescriptor[]   objectArray, 
    int                     maxFreeListSize)
	{
		if (vector.size() < maxFreeListSize)
		{
			vector.addElement(objectArray);
		}
	}

	public DataValueDescriptor[] getArrayClone()
		throws StandardException
	{
		int lastElement = vector.size();

		if (lastElement > 0)
		{
            return vector.remove(lastElement - 1);
		}
		return template.getRowArrayClone();
	}

    public boolean deferred() {
        return false;
    }

    public boolean deferrable() {
        return false;
    }

    public void rememberDuplicate(DataValueDescriptor[] row)
            throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.NOTREACHED();
        }
    }
}

class T_DuplicateEliminator extends T_DummySortObserver
{

	T_DuplicateEliminator(T_AccessRow template)
	{
		super(template);
	}
	/*
	 * Methods of SortObserver
	 */
	public DataValueDescriptor[] insertNonDuplicateKey(
    DataValueDescriptor[] insertRow)	
	{
		return insertRow;
	}

	public DataValueDescriptor[] insertDuplicateKey(
    DataValueDescriptor[] insertRow, 
    DataValueDescriptor[] existingRow)
	{
		return null;
	}
}

class T_SumForIntCol implements SortObserver
{
	private int columnId;
	
	T_SumForIntCol(int columnId)
	{
		this.columnId = columnId;
	}

	/*
	 * Methods of SortObserver
	 */

	public DataValueDescriptor[] insertNonDuplicateKey(
    DataValueDescriptor[] insertRow)	
	{
		return insertRow;
	}

	public DataValueDescriptor[] insertDuplicateKey(
    DataValueDescriptor[] insertRow, 
    DataValueDescriptor[] existingRow)
		throws StandardException
	{

		// We know, because this is a test program and it's only
		// used this way, that we can safely cast the arguments
		// to SQLInteger.
		SQLInteger increment = (SQLInteger) insertRow[columnId];
		SQLInteger sum = (SQLInteger) existingRow[columnId];

		// Perform the aggregation.
		sum.plus(sum, increment, sum);

		return null;
	}

	public void addToFreeList(
    DataValueDescriptor[]   objectArray, 
    int                     maxFreeListSize)
	{
	}

	public DataValueDescriptor[] getArrayClone()
		throws StandardException
	{
		return null;
	}

    public boolean deferred() {
        return false;
    }

    public boolean deferrable() {
        return false;
    }

    public void rememberDuplicate(DataValueDescriptor[] row)
            throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.NOTREACHED();
        }
    }
}

