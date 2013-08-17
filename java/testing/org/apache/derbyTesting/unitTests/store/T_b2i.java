/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_b2i

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
import org.apache.derbyTesting.unitTests.harness.T_MultiIterations;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.impl.store.access.btree.index.*;

import org.apache.derby.iapi.types.SQLLongint;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.ScanInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ColumnOrdering;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.impl.store.access.btree.BTree;
import org.apache.derby.impl.store.access.conglomerate.TemplateRow;
import org.apache.derby.iapi.types.SQLChar;


import java.util.Properties;



public class T_b2i extends T_MultiIterations
{ 

    private static final String testService = "b2iTest";

    private Object store_module = null; 

    private ContextService contextService = null;

    /* Methods required by T_MultiIterations */


    /**
     * Routine one once per invocation of the test by the driver.
     * <p>
     * Do work that should only be done once, no matter how many times
     * runTests() may be executed.
     *
	 * @exception  T_Fail  Thrown on any error.
     **/
    protected void setupTest()
		throws T_Fail
    {
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
			store_module = Monitor.createPersistentService(
				getModuleToTestProtocolName(), testService, startParams);
			
			contextService = ContextService.getFactory();

		} catch (StandardException mse) {
			throw T_Fail.exceptionFail(mse);
		}
    }

	/*
	** Methods required by T_Generic
	*/

	public String getModuleToTestProtocolName() {
		return AccessFactory.MODULE;
	}

    /**
     * Driver routine for the btree secondary index tests.
     * <p>
     *
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
	protected void runTestSet() throws T_Fail
	{
		AccessFactory store = null;
		TransactionController tc = null;
		boolean pass = false;

        out.println("executing b2i test");

        store = (AccessFactory) store_module;

		if (store == null)
        {
			throw T_Fail.testFailMsg(
                    getModuleToTestProtocolName() + " service not started.");
		}

		ContextManager cm1 = contextService.newContextManager();
		contextService.setCurrentContextManager(cm1);

		REPORT("(unitTestMain) Testing " + testService);

		try {

			tc = store.getTransaction(cm1);

				pass = true;

            if (
                t_005(tc)   &&
                t_001(tc)   &&
                t_003(tc)   &&
                t_004(tc)   &&
                t_005(tc)   &&
                t_006(tc)   &&
                t_009(tc)   &&
                t_010(tc)   &&
                t_011(tc)   &&
                t_012(tc)   &&
                t_013(tc)   &&
                t_014(tc)   &&
				t_017(tc)   &&
				t_018(tc)   &&
				t_019(tc)   &&
				t_020(tc)   &&
				t_021(tc)
                )
                
			{
				pass = true;

                if (SanityManager.DEBUG)
                {
                    pass = false;

                    // The following tests depend on SanityManager functionality
                    // so can not be run in an insane server.

                    if (t_002(tc) &&
                        t_007(tc) &&
                        t_008(tc) &&
                        t_015(tc) &&
                        t_016(tc)
                        )
                        pass = true;
                }
			}

			tc.commit();
			tc.destroy();
		}
		catch (StandardException e)
		{
			String  msg = e.getMessage();
			if (msg == null)
				msg = e.getClass().getName();
			REPORT(msg);

            e.printStackTrace(out.getPrintWriter());
            cm1.cleanupOnError(e, isdbActive());

            pass = false;
		}
        catch (Throwable t)
        {
			String  msg = t.getMessage();
			if (msg == null)
				msg = t.getClass().getName();
			REPORT(msg);

            t.printStackTrace(out.getPrintWriter());
            cm1.cleanupOnError(t, isdbActive());

            pass = false;
        }
		finally {
			contextService.resetCurrentContextManager(cm1);
		}

		if (!pass)
			throw T_Fail.testFailMsg("");
	}

    /**
     * Utility routine to create base table for tests.
     * <p>
     * A little utility routine to create base tables for tests.  Just
     * here to make tests a little more readable.  It currently just 
     * creates a heap table with "num_cols" SQLLongint columns.
     *
     *
     * @param num_cols the number of columns in the base table.
     *
     * @exception  StandardException  Standard exception policy.
     **/

    void createCongloms(
    TransactionController   tc,
    int                     num_cols,
    boolean                 unique,
    boolean                 varying_first_col,
    int                     max_btreerows_per_page,
    T_CreateConglomRet        ret_val)
		throws StandardException
    {
        T_SecondaryIndexRow index_row  = new T_SecondaryIndexRow();
        DataValueDescriptor[] base_row = TemplateRow.newU8Row(num_cols);

        if (varying_first_col)
        {
            SQLChar    string_col = new SQLChar();

            base_row[0] = string_col;
        }

        long base_conglomid = 0;

        // create the base table
        base_conglomid = 
            tc.createConglomerate(
                "heap",   // create a heap conglomerate
                base_row, // base table template row
                null, //column sort order - not required for heap
                null, //default collation
                null,     // default properties
                TransactionController.IS_DEFAULT);// not temporary

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // initialize the secondary index row - pointing it at base row
        RowLocation base_rowloc = base_cc.newRowLocationTemplate();
        index_row.init(base_row, base_rowloc, num_cols + 1);
        
        // create the secondary index
        Properties properties = 
            createProperties(
                null,           // no current properties list 
                false,          // don't allow duplicates
                num_cols + 1,   // index on all base row cols + row location
                (unique ? num_cols : num_cols + 1),   // non-unique index
                true,           // maintain parent links
                base_conglomid, // base conglomid
                num_cols);      // row loc in last column

        if (max_btreerows_per_page > 1)
        {
            if (BTree.PROPERTY_MAX_ROWS_PER_PAGE_PARAMETER != null)
            {
                properties.put(
                    BTree.PROPERTY_MAX_ROWS_PER_PAGE_PARAMETER,
                    String.valueOf(max_btreerows_per_page));
            }
        }

        long index_conglomid = 
            tc.createConglomerate(
                "BTREE",    	    // create a btree secondary
                index_row.getRow(), // index row template
				null, //column sort order - default
                null, //default collation
                properties,         // properties
                TransactionController.IS_DEFAULT); // not temporary

        // return values to caller
        ret_val.base_conglomid     = base_conglomid;
        ret_val.index_conglomid    = index_conglomid;
        // RESOLVE (mikem - 04/29/98 - why is following line commented out?
        // ret_val.base_template_row  = TemplateRow.newU8Row(num_cols);
        ret_val.index_template_row = index_row.getRow();

        return;
    }

    protected static Properties createProperties(
    Properties  input_properties,
    boolean     input_allowduplicates,
    int         input_nkeyfields,
    int         input_nuniquecolumns,
    boolean     input_maintainparentlinks,
    long        input_baseconglomerateid,
    int         input_rowlocationcolumn)
        throws StandardException
    {
        Properties properties = 
            ((input_properties == null) ? new Properties() : input_properties);
        properties.put(
            "allowDuplicates",     String.valueOf(input_allowduplicates));
        properties.put(
            "nKeyFields",          String.valueOf(input_nkeyfields));
        properties.put(
            "nUniqueColumns",      String.valueOf(input_nuniquecolumns));
        properties.put(
            "maintainParentLinks", String.valueOf(input_maintainparentlinks));
        properties.put(
            "baseConglomerateId",  String.valueOf(input_baseconglomerateid));
        properties.put(
            "rowLocationColumn",   String.valueOf(input_rowlocationcolumn));
        return(properties);
    }

    /**
     * Test a single scan.
     * 
	 * @exception  StandardException  Standard exception policy.
     */
    protected boolean t_scan(
    TransactionController   tc,
    long                    conglomid,
    DataValueDescriptor[]	template,
    DataValueDescriptor[]	start_key, 
    int                     start_op,
    Qualifier               qualifier[][],
    DataValueDescriptor[]	stop_key,
    int                     stop_op,
    int                     expect_numrows,
    int                     expect_key)
        throws StandardException
    {

        // open a new scan

        ScanController scan = 
            tc.openScan(
                conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                start_key, start_op,
                qualifier,
                stop_key, stop_op);

        long key     = -42;
        long numrows = 0;

        while (scan.next())
        {
            scan.fetch(template);

            key = ((SQLLongint)template[2]).getLong();

            if (key != expect_key)
            {
                return(
                    FAIL("(t_scan) wrong key, expected (" + expect_key + ")" +
                     "but got (" + key + ")."));
            }
            else
            {
                expect_key++;
                numrows++;
            }
        }

        ((B2IForwardScan)scan).checkConsistency();
        
        scan.close();

        if (numrows != expect_numrows)
        {
            return(FAIL("(t_scan) wrong number of rows. Expected " +
                 expect_numrows + " rows, but got " + numrows + "rows."));
        }

        return(true);
    }

    /**
     * delete a single key, given key value.  assumes 3 column table, first
     * column = 1, second column is a unique key, and 3rd column is a 
     * RecordHandle into the base table.
     *
	 * @exception  StandardException  Standard exception policy.
     */
    protected boolean t_delete(
    TransactionController   tc,
    long                    conglomid,
    DataValueDescriptor[]   search_key,
    boolean useUpdateLocks)
        throws StandardException
    {
        SQLLongint column0 = new SQLLongint(-1);
        SQLLongint column1 = new SQLLongint(-1);

        int openmode = TransactionController.OPENMODE_FORUPDATE;
        if (useUpdateLocks) {
            openmode |= TransactionController.OPENMODE_USE_UPDATE_LOCKS;
        }

        // open a new scan

        ScanController scan = 
            tc.openScan(conglomid, false,
                        openmode,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE,
                        (FormatableBitSet) null,
                        search_key, ScanController.GE, 
                        null,
                        search_key, ScanController.GT); 

        long expect_key = 
            ((SQLLongint) search_key[1]).getLong();

        int numrows = 0;
		DataValueDescriptor[] partialRow = new DataValueDescriptor[2];
		partialRow[0] = column0;
		partialRow[1] = column1;

        while (scan.next())
        {
            numrows++;

			scan.fetch(partialRow);

            if (column0.getLong() != 1)
                return(FAIL("(t_delete) column[0] value is not 1"));

            if (column1.getLong() != expect_key)
                return(
                    FAIL("(t_delete) column[1]  value is not " + expect_key));

            if (!scan.delete())
            {
                return(FAIL("(t_delete): delete of row failed"));
            }
            if (scan.delete())
            {
                return(FAIL("(t_delete): re-delete of row succeeded"));
            }
        }

        scan.close();

        // This function expects unique keys, so scan should find single row.
        if (numrows != 1)
        {
            return(FAIL("(t_delete) wrong number of rows. Expected " +
                   "1 row, but got " + numrows + "rows."));
        }

        return(true);
    }

    /**
     * Test BTreeController.insert()
     * <p>
     * Just verify that insert code works for a secondary index.  Just call
     * the interface and make sure the row got there.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
    protected boolean t_001(TransactionController tc)
        throws StandardException, T_Fail
    {
        REPORT("Starting t_001");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        createCongloms(tc, 2, false, false, 0, create_ret);

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the secondary index
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        if (!(index_cc instanceof B2IController))
        {
			throw T_Fail.testFailMsg("openConglomerate returned wrong type");
        }

        if (!index_cc.isKeyed())
        {
			throw T_Fail.testFailMsg("btree is not keyed.");
        }

        index_cc.checkConsistency();

		// Create a row and insert into base table, remembering it's location.
		DataValueDescriptor[] r1             = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow  index_row1      = new T_SecondaryIndexRow();
        RowLocation          base_rowloc1    = base_cc.newRowLocationTemplate();

        index_row1.init(r1, base_rowloc1, 3);
		((SQLLongint) r1[0]).setValue(2);
		((SQLLongint) r1[1]).setValue(2);

		// Insert the row into the base table and remember its location.
		base_cc.insertAndFetchLocation(r1, base_rowloc1);

        // Insert the row into the secondary index.
        if (index_cc.insert(index_row1.getRow()) != 0)
			throw T_Fail.testFailMsg("insert failed");

		// Make sure we read back the value we wrote from base and index table.
		DataValueDescriptor[] r2            = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow   index_row2    = new T_SecondaryIndexRow();
        RowLocation           base_rowloc2  = base_cc.newRowLocationTemplate();

        index_row2.init(r2, base_rowloc2, 3);

        // base table check:
        if (!base_cc.fetch(base_rowloc1, r2, (FormatableBitSet) null))
        {
            return(FAIL("(t_001) insert into base table failed"));
        }

        if (((SQLLongint) r2[0]).getLong() != 2 ||
            ((SQLLongint) r2[1]).getLong() != 2)
        {
            return(FAIL("(t_001) insert into base table failed"));
        }

        // index check - there should be only one record:
        ScanController scan = 
            tc.openScan(create_ret.index_conglomid, false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE,
						(FormatableBitSet) null,
                        null, ScanController.NA,
                        null,
                        null, ScanController.NA);

        scan.next();
        scan.fetch(index_row2.getRow());

        // delete the only row in the table, and make sure 
        // isCurrentPositionDeleted() works.
        if (scan.isCurrentPositionDeleted())
            throw T_Fail.testFailMsg("current row should not be deleted\n");
        if (!scan.doesCurrentPositionQualify())
            throw T_Fail.testFailMsg("current row should still qualify\n");
        scan.delete();
        if (!scan.isCurrentPositionDeleted())
            throw T_Fail.testFailMsg("current row should be deleted\n");
        if (scan.doesCurrentPositionQualify())
            throw T_Fail.testFailMsg("deleted row should not qualify\n");

        // just call the debugging code to make sure it doesn't fail.
        REPORT("Calling scan.tostring(): " + scan);

        if (scan.next()                                                     ||
            ((SQLLongint)(index_row2.getRow()[0])).getLong() != 2 ||
            ((SQLLongint)(index_row2.getRow()[1])).getLong() != 2)
        {
            return(FAIL("(t_001) insert into index failed in base cols"));
        }

        // test the scaninfo interface.

        ScanInfo   scan_info = scan.getScanInfo();
        Properties prop      = scan_info.getAllScanInfo(null);

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
				!= 1)
        {
            throw T_Fail.testFailMsg(
                "(scanInfo) wrong numRowsVisited. Expected 1, got " + 
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


        int compare_result = base_rowloc1.compare(base_rowloc2);
        if (compare_result != 0)
        {
            return(FAIL("(t_001) insert into index failed in recordhandle.\n" +
                        "\texpected RecordHandle = " + base_rowloc1 + "\n" +
                        "\tgot      RecordHandle = " + base_rowloc2 + 
                        "\tcompare result = " + compare_result));
        }

        index_cc.checkConsistency();

		// Close the conglomerates.
		base_cc.close();
		index_cc.close();

        try 
        {
            base_cc.insert(r1);
            return(FAIL("(t_001) insert on closed conglomerate worked"));
        }
        catch (StandardException e)
        {
            // e.printStackTrace();
        }

        try 
        {
            if (index_cc.insert(r1) != 0)
                throw T_Fail.testFailMsg("insert failed");
            return(FAIL("(t_001) insert on closed conglomerate worked"));
        }
        catch (StandardException e)
        {
            // e.printStackTrace();
        }

        tc.commit();
        REPORT("Ending t_001");

        return true;
    }

    /**
     * Test backout during critical times of splits.
     * <p>
     * Use trace points to force errors in split at critical points:
     *     leaf_split_abort{1,2,3,4}
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
    protected boolean t_002(TransactionController tc)
        throws StandardException, T_Fail
    {
        ScanController scan         = null;

        // SanityManager.DEBUG_SET("LockTrace");

        REPORT("Starting t_002");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        // Create the btree so that it only allows 2 rows per page.
        createCongloms(tc, 2, false, false, 2, create_ret);

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the secondary index
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        if (!(index_cc instanceof B2IController))
        {
			throw T_Fail.testFailMsg("openConglomerate returned wrong type");
        }

        index_cc.checkConsistency();

		// Create a row and insert into base table, remembering it's location.
		DataValueDescriptor[]   r1           = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow     index_row1   = new T_SecondaryIndexRow();
        RowLocation             base_rowloc1 = base_cc.newRowLocationTemplate();

        index_row1.init(r1, base_rowloc1, 3);

        // Commit the create of the tables so that the following aborts don't
        // undo that work.
        tc.commit();

        // Now try aborts of transactions during splits, using magic
        // trace flags.  This test inserts enough rows to cause a split
        // and then forces the split to fail at various key points.  The
        // split should be backed out and also the rows before the split.
        // The test makes sure that there are some inserts before the forced
        // split abort.
        String[] debug_strings = {
            "leaf_split_growRoot1",
            "leaf_split_growRoot2",
            "leaf_split_growRoot3",
            "leaf_split_growRoot4",
            "leaf_split_growRoot5",
            "leaf_split_abort1",
            "leaf_split_abort2",
            "leaf_split_abort3",
            "leaf_split_abort4",
            "branch_split_abort1",
            "branch_split_abort2",
            "branch_split_abort3",
            "branch_split_abort4",
            "BTreeController_doIns2"
            };

        for (int errs = 0; errs < debug_strings.length; errs++)
        {
            REPORT("Doing abort test: " + debug_strings[errs]);

            // set the debug flag, which will cause an error
            // RESOLVE (mmm) these tests should be run from the language to
            // make sure error handling really works.

            if (SanityManager.DEBUG)
                SanityManager.DEBUG_SET(debug_strings[errs]);
            
            try 
            {

                // Open the base table
                base_cc = 
                    tc.openConglomerate(
                        create_ret.base_conglomid,
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE);

                // Open the secondary index
                index_cc =	
                    tc.openConglomerate(
                        create_ret.index_conglomid,
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE);

                // insert one row that does not cause failure.
                ((SQLLongint)r1[0]).setValue(2);
                ((SQLLongint)r1[1]).setValue(10000 + errs);

                // Insert the row into the base table;remember its location.
                base_cc.insertAndFetchLocation(r1, base_rowloc1);

                // Insert the row into the secondary index.
                if (index_cc.insert(index_row1.getRow()) != 0)
                    throw T_Fail.testFailMsg("insert failed");


                // set the debug flag, which will cause an error
                // RESOLVE (mmm) these tests should be run from the 
                // language to make sure error handling really works.
                if (SanityManager.DEBUG)
                    SanityManager.DEBUG_SET(debug_strings[errs]);

                // now insert enough rows to cause failure
                for (int i = 100; i > 0; i -= 2)
                {
                    ((SQLLongint)r1[0]).setValue(2);
                    ((SQLLongint)r1[1]).setValue(i);

                    // Insert the row into the base table;remember its location.
                    base_cc.insertAndFetchLocation(r1, base_rowloc1);

                    // Insert the row into the secondary index.
                    if (index_cc.insert(index_row1.getRow()) != 0)
                    {
                        throw T_Fail.testFailMsg("insert failed");
                    }
                }

                throw T_Fail.testFailMsg(
                    "debug flag (" + debug_strings[errs] + 
                    ")did not cause exception.");
            }
            catch (StandardException e)
            {
                ContextService contextFactory = ContextService.getFactory();

                // Get the context manager.
                ContextManager cm = contextFactory.getCurrentContextManager();

                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(cm != null);

                cm.cleanupOnError(e, isdbActive());
                
                // RESOLVE (mikem) - when split abort works come up with 
                // a good sanity check here.
                //
                // index check - there should be no records:
                scan = tc.openScan(create_ret.index_conglomid, false, 
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE,
						(FormatableBitSet) null,
                        null, ScanController.NA,
                        null,
                        null, ScanController.NA);


                index_cc =	
                    tc.openConglomerate(
                        create_ret.index_conglomid,
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE);

                index_cc.checkConsistency();
                index_cc.close();

                if (scan.next())
                {
                    throw T_Fail.testFailMsg(
                            "t_002: there are still rows in table.");
                }


                scan.close();
            }

            // Unset the flag.
            if (SanityManager.DEBUG)
                SanityManager.DEBUG_CLEAR(debug_strings[errs]);
        }

        // Try a simple abort.  The following adds enough rows to cause a 
        // split.  The result of the split should be a tree with no rows, but
        // the splits will not be undone.  It is up to the implementation
        // whether the undo's cause shrinks in the tree.  In the initial
        // implementation it won't.
        {
            tc.commit();

            // Open the base table
            base_cc = 
                tc.openConglomerate(
                    create_ret.base_conglomid,
                    false,
                    TransactionController.OPENMODE_FORUPDATE,
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

            // Open the secondary index
            index_cc =	
                tc.openConglomerate(
                    create_ret.index_conglomid,
                    false,
                    TransactionController.OPENMODE_FORUPDATE,
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

            // BTree.PROPERTY_MAX_ROWS_PER_PAGE_PARAMETER has been set to 2 so
            // inserting 3 rows will cause a split at the leaf level.
            // Make sure that normal abort leaves the committed split.
            for (int i = 0; i < 3; i++)
            {
                ((SQLLongint)r1[0]).setValue(2);
                ((SQLLongint)r1[1]).setValue(i);

                // Insert the row into the base table;remember its location.
                base_cc.insertAndFetchLocation(r1, base_rowloc1);

                // Insert the row into the secondary index.
                if (index_cc.insert(index_row1.getRow()) != 0)
                    throw T_Fail.testFailMsg("insert failed");
            }

            tc.abort();

            // index check - there should be no records left.
            ScanController empty_scan = 
                tc.openScan(create_ret.index_conglomid, false, 
                            0,
                            TransactionController.MODE_RECORD,
                            TransactionController.ISOLATION_SERIALIZABLE,
							(FormatableBitSet) null,
                            null, ScanController.NA,
                            null,
                            null, ScanController.NA);

            if (empty_scan.next())
            {
                throw T_Fail.testFailMsg(
                    "t_002: there are still rows in table.");
            }
        }

        tc.commit();
        REPORT("Ending t_002");

        return true;
    }

    private boolean t_003_scan_test_cases(
    TransactionController   tc,
    long                    index_conglomid,
    T_SecondaryIndexRow     template
    )
        throws StandardException, T_Fail
    {
        boolean     ret_val = true;

        // run through a predicates as described in the openScan() interface //
        DataValueDescriptor[] start_key  = TemplateRow.newU8Row(1);
        DataValueDescriptor[] stop_key   = TemplateRow.newU8Row(1);


        // test predicate x = 5
        //
        //     result set should be: {5,2,16}, {5,4,17}, {5,6,18}
        //
        REPORT("scan (x = 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE, 
                   null,
                   stop_key,  ScanController.GT,
                   3, 16, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }
                   
        //  +---------------------------------------------------------+
        //  |pred  |start|key|stop |key|rows returned |rows locked    |
        //  |      |value|op |value|op |              |(serialization)|
        //  +------+-----+---+-----+---+--------------+---------------+
        //  |x > 5 |{5}  |GT |null |   |{6,1} .. {9,1}|{5,6} .. {9,1} |
        //  +-----------------------------------------+---------------+
        REPORT("scan (x > 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GT, 
                   null,
                   null,  ScanController.NA,
                   3, 19, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        //  +---------------------------------------------------------+
        //  |pred  |start|key|stop |key|rows returned |rows locked    |
        //  |      |value|op |value|op |              |(serialization)|
        //  +------+-----+---+-----+---+--------------+---------------+
        //  |x >= 5|{5}  |GE |null |   |{5,2} .. {9,1}|{4,6} .. {9,1} |
        //  +-----------------------------------------+---------------+
        REPORT("scan (x >= 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE, 
                   null,
                   null,  ScanController.NA,
                   6, 16, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        //
        //  +---------------------------------------------------------+
        //  |pred  |start|key|stop |key|rows returned |rows locked    |
        //  |      |value|op |value|op |              |(serialization)|
        //  +------+-----+---+-----+---+--------------+---------------+
        //  |x <= 5|null |   |{5}  |GT |{1,1} .. {5,6}|first .. {5,6} |
        //  +-----------------------------------------+---------------+
        REPORT("scan (x <= 5)");
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   null, ScanController.NA, 
                   null,
                   stop_key,  ScanController.GT,
                   8, 11, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }
        //
        //  +---------------------------------------------------------+
        //  |pred  |start|key|stop |key|rows returned |rows locked    |
        //  |      |value|op |value|op |              |(serialization)|
        //  +------+-----+---+-----+---+--------------+---------------+
        // 	|x < 5 |null |   |{5}  |GE |{1,1} .. {4,6}|first .. {4,6} |
        //  +-----------------------------------------+---------------+
        REPORT("scan (x < 5)");
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   null, ScanController.NA, 
                   null,
                   stop_key,  ScanController.GE,
                   5, 11, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        //long        col1[]  = { 1,  3,  4,  4,  4,  5,  5,  5,  6,  7,  9};
        //long        col2[]  = { 1,  1,  2,  4,  6,  2,  4,  6,  1,  1,  1};
        //long        col3[]  = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};
        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
	    //  |x >= 5 and x <= 7|{5},  |GE|{7}  |GT|{5,2} .. {7,1}|{4,6} .. {7,1}|
        // 	+------------------------------------------------------------------+
        REPORT("scan (x >= 5 and x <= 7)");
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(7);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   5, 16, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
	    //  |x = 5 and y > 2  |{5,2} |GT|{5}  |GT|{5,4} .. {5,6}|{5,2} .. {9,1}|
        // 	+------------------------------------------------------------------+
        REPORT("scan (x = 5 and y > 2)");
        start_key  = TemplateRow.newU8Row(2);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)start_key[1]).setValue(2);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GT,
                   null,
                   stop_key,  ScanController.GT,
                   2, 17, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
        // 	|x = 5 and y >= 2 | {5,2}|GE| {5} |GT|{5,2} .. {5,6}|{4,6} .. {9,1}|
        // 	+------------------------------------------------------------------+
        REPORT("scan (x = 5 and y >= 2)");
        start_key  = TemplateRow.newU8Row(2);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)start_key[1]).setValue(2);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   3, 16, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
        // 	|x = 5 and y < 5  | {5}  |GE|{5,5}|GE|{5,2} .. {5,4}|{4,6} .. {5,4}|
        // 	+------------------------------------------------------------------+
        REPORT("scan (x = 5 and y < 5)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(2);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        ((SQLLongint)stop_key[1]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GE,
                   2, 16, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
        // 	|x = 2            | {2}  |GE| {2} |GT|none          |{1,1} .. {1,1}|
        // 	+------------------------------------------------------------------+
        REPORT("scan (x = 2)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(2);
        ((SQLLongint)stop_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   0, 0, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        //  +-----------------------------+
        // 	|max on btree - row locked    |
        // 	+-----------------------------+
        //
        REPORT("max on btree, row locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no max.");
        }
        else
        {
            // make sure right max was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 21)
            {
                throw T_Fail.testFailMsg("wrong max found.");
            }
        }

        //  +-----------------------------+
        // 	|max on btree - table locked    |
        // 	+-----------------------------+
        //
        REPORT("max on btree, table locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no max.");
        }
        else
        {
            // make sure right max was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 21)
            {
                throw T_Fail.testFailMsg("wrong max found.");
            }
        }

        //  +-----------------------------+
        // 	|max on btree - row locked    |
        // 	+-----------------------------+
        //
        REPORT("max on btree, row locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_READ_COMMITTED,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no max.");
        }
        else
        {
            // make sure right max was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 21)
            {
                throw T_Fail.testFailMsg("wrong max found.");
            }
        }

        //  +-----------------------------+
        // 	|max on btree - table locked    |
        // 	+-----------------------------+
        //
        REPORT("max on btree, table locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_READ_COMMITTED,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no max.");
        }
        else
        {
            // make sure right max was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 21)
            {
                throw T_Fail.testFailMsg("wrong max found.");
            }
        }

        return(ret_val);
    }

    /**
     * Test BTree.openScan(), BtreeScan.init(), BtreeScan.next(), 
     * BtreeScan.fetch().
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_003(TransactionController tc)
        throws StandardException, T_Fail
    {
        T_SecondaryIndexRow index_row = new T_SecondaryIndexRow();

        // base row template - last column is just to make row long so that
        // multiple pages are spanned.
        DataValueDescriptor[] base_row             = TemplateRow.newU8Row(4);
        base_row[3] = new SQLChar();

        String   string_1500char = new String();
        for (int i = 0; i < 300; i++)
            string_1500char += "mikem";

        boolean     ret_val = true;
        long        value   = -1;
        long        col1[]  = { 1,  3,  4,  4,  4,  5,  5,  5,  6,  7,  9};
        long        col2[]  = { 1,  1,  2,  4,  6,  2,  4,  6,  1,  1,  1};
        long        col3[]  = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};

        // set of deleted rows to make scans more interesting
        long d_col1[] ={ 0,  2,  3,  4,  4,  5,  5,  5,  6,  7,  8, 10, 11, 12};
        long d_col2[] ={ 1,  1,  2,  3,  5,  0,  3,  5,  0,  0,  1, 42, 42, 1};
        long d_col3[] ={91, 92, 93, 94, 95, 96, 97, 98, 99,100,101,102,103,104};

        REPORT("Starting t_003");

        // create the base table
        long base_conglomid = 
            tc.createConglomerate(
                "heap",                            // create a heap conglomerate
                base_row,                          // base table template row
				null, //column sort order - not required for heap
                null, //default collation
                null,                              // default properties
                TransactionController.IS_DEFAULT); // not temporary

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // initialize the secondary index row - pointing it at base row
        index_row.init(base_row, base_cc.newRowLocationTemplate(), 5);

        Properties properties = 
            createProperties(
                null,               // no current properties list 
                false,              // don't allow duplicates
                5,                  // 4 columns in index row
                5,                  // non-unique index
                true,               // maintain parent links
                base_conglomid,     // base conglom id
                4);                 // row loc in last column

        long index_conglomid = 
            tc.createConglomerate(
                "BTREE",    				// create a btree secondary
                index_row.getRow(),         // row template
				null, //column sort order - default
                null, //default collation
                properties,                 // properties
                TransactionController.IS_DEFAULT);   // not temporary

		// Open the conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row.
        T_SecondaryIndexRow template = new T_SecondaryIndexRow();
        RowLocation         row_loc  = base_cc.newRowLocationTemplate();
        template.init(base_row, row_loc, 5);

        // insert them in reverse order just to make sure btree is sorting them
        for (int i = col1.length - 1; i >= 0; i--)
        {
            ((SQLLongint)(template.getRow()[0])).setValue(col1[i]);
            ((SQLLongint)(template.getRow()[1])).setValue(col2[i]);
            ((SQLLongint)(template.getRow()[2])).setValue(col3[i]);
            base_row[3] = new SQLChar(string_1500char);

            base_cc.insertAndFetchLocation(base_row, row_loc);

            // Insert the row.
            // System.out.println("Adding record (" + -(i - (col1.length -1)) +
            //                ")" + template);
            if (index_cc.insert(template.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        index_cc.checkConsistency();

        ((B2IController)index_cc).debugConglomerate();

        ret_val = t_003_scan_test_cases(tc, index_conglomid, template);

        // insert and delete some interesting rows, deleted space management
        // may or may not clean these up.
        for (int i = d_col1.length - 1; i >= 0; i--)
        {
            ((SQLLongint)(template.getRow()[0])).setValue(d_col1[i]);
            ((SQLLongint)(template.getRow()[1])).setValue(d_col2[i]);
            ((SQLLongint)(template.getRow()[2])).setValue(d_col3[i]);
            base_row[3] = new SQLChar(string_1500char);

            base_cc.insertAndFetchLocation(base_row, row_loc);

            // Insert the row.
            // System.out.println("Adding record (" + -(i - (col1.length -1)) +
            //                ")" + template);
            if (index_cc.insert(template.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");

            // now delete the row.
            base_cc.delete(row_loc);

            ScanController delete_scan = 
                tc.openScan(index_conglomid, false,
                            TransactionController.OPENMODE_FORUPDATE,
                            TransactionController.MODE_RECORD,
                            TransactionController.ISOLATION_SERIALIZABLE,
                            (FormatableBitSet) null,
                            template.getRow(), ScanController.GE, 
                            null,
                            template.getRow(), ScanController.GT); 

            if (!delete_scan.next())
            {
                throw T_Fail.testFailMsg("delete could not find key");
            }
            else
            {
                delete_scan.delete();

                if (delete_scan.next())
                    throw T_Fail.testFailMsg("delete found more than one key");
            }

            delete_scan.close();
        }

        ret_val = t_003_scan_test_cases(tc, index_conglomid, template);


		// Close the conglomerate.
		index_cc.close();

        tc.commit();
        REPORT("Ending t_003");

        return(ret_val);
    }

    /**
     * Test qualifiers.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_004(TransactionController tc)
        throws StandardException, T_Fail
    {
        REPORT("Starting t_004");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        createCongloms(tc, 2, false, false, 0, create_ret);

        Properties properties = 
            createProperties(
                null,       // no current properties list 
                false,      // don't allow duplicates
                4,          // 3 columns in index row
                4,          // non-unique index
                true,       // maintain parent links
                create_ret.base_conglomid,        // fake base conglom for now
                3);         // row loc in last column

        T_QualifierTest q_test = 
            new T_QualifierTest(
                "BTREE",    				    // create a btree secondary
                properties,                     // properties
                false,                          // not temporary
                out,
                T_QualifierTest.ORDER_FORWARD); // ordered 

        boolean test_result = q_test.t_testqual(tc);

        REPORT("Ending t_004");

        return(test_result);
    }

    /**
     * Test Branch splits - number of rows necessary to cause splits is raw
     * store implementation dependant (currently 5 rows per page in in-memory
     * implementation).
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_005(TransactionController tc)
        throws StandardException, T_Fail
    {
        boolean     ret_val = true;

        REPORT("Starting t_005");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        createCongloms(tc, 2, false, false, 0, create_ret);

		// Open the base conglomerate.
		ConglomerateController base_cc =	
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the index conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);


		// Create a row.
		T_SecondaryIndexRow index_row = new T_SecondaryIndexRow();
        RowLocation             rowloc    = base_cc.newRowLocationTemplate();
        DataValueDescriptor[]   base_row  = TemplateRow.newU8Row(2);
        index_row.init(base_row, rowloc, 3); 

        // insert them in reverse order just to make sure btree is sorting them
        for (int i = 200; i >= 0; i -= 4)
        {
            ((SQLLongint)base_row[0]).setValue(1);
            ((SQLLongint)base_row[1]).setValue(i);
            base_cc.insertAndFetchLocation(base_row, rowloc);

            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }
        for (int i = 199; i >= 0; i -= 4)
        {
            ((SQLLongint)base_row[0]).setValue(1);
            ((SQLLongint)base_row[1]).setValue(i);

            base_cc.insertAndFetchLocation(base_row, rowloc);
            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        index_cc.checkConsistency();

		// Close the conglomerate.
		index_cc.close();

        tc.commit();

        // Search for each of the keys and delete them one at a time.
        DataValueDescriptor[] delete_key = TemplateRow.newU8Row(2);
        for (int i = 200; i >= 0; i -= 4)
        {
            ((SQLLongint)delete_key[0]).setValue(1);
            ((SQLLongint)delete_key[1]).setValue(i);

            if (!t_delete(
                tc, create_ret.index_conglomid, delete_key, false))
            {
                ret_val = false;
            }
        }
        for (int i = 199; i >= 0; i -= 4)
        {
            ((SQLLongint)delete_key[0]).setValue(1);
            ((SQLLongint)delete_key[1]).setValue(i);

            if (!t_delete(
                tc, create_ret.index_conglomid, delete_key, false))
            {
                ret_val = false;
            }
        }

        tc.commit();

		// Open the base conglomerate.
		base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);


		// Open the conglomerate.
		index_cc = 
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // flush and empty cache to make sure rereading stuff works.
        RawStoreFactory rawstore = 
            (RawStoreFactory) Monitor.findServiceModule(
                this.store_module, RawStoreFactory.MODULE);

        rawstore.idle();

        // now make sure that additional splits don't cause delete bits to 
        // be enabled (one early bug did so).
        
        for (int i = 200; i >= 0; i -= 3)
        {
            ((SQLLongint)base_row[0]).setValue(1);
            ((SQLLongint)base_row[1]).setValue(i);

            base_cc.insertAndFetchLocation(base_row, rowloc);
            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }
        for (int i = 200; i >= 0; i -= 3)
        {
            ((SQLLongint)delete_key[0]).setValue(1);
            ((SQLLongint)delete_key[1]).setValue(i);

            if (!t_delete(
                tc, create_ret.index_conglomid, delete_key, false))
            {
                ret_val = false;
            }
        }

        // index check - there should be no records left.
        ScanController empty_scan = 
            tc.openScan(create_ret.index_conglomid, false, 
                        0,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE,
						(FormatableBitSet) null,
                        null, ScanController.NA,
                        null,
                        null, ScanController.NA);
        if (empty_scan.next())
			throw T_Fail.testFailMsg("t_005: there are still rows in table.");

        index_cc.checkConsistency();

        for (int i = 600; i >= 400; i -= 3)
        {
            ((SQLLongint)base_row[0]).setValue(1);
            ((SQLLongint)base_row[1]).setValue(i);

            base_cc.insertAndFetchLocation(base_row, rowloc);
            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        index_cc.checkConsistency();

        tc.abort();

        // index check - there should be no records left.
        empty_scan = 
            tc.openScan(create_ret.index_conglomid, false,
                        0,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE,
						(FormatableBitSet) null,
                        null, ScanController.NA,
                        null,
                        null, ScanController.NA);
        if (empty_scan.next())
			throw T_Fail.testFailMsg("t_005: there are still rows in table.");


        REPORT("Ending t_005");

        return(ret_val);
    }
    /**
     * Test unimplemented interfaces.  
     *
     * The following ScanController interfaces are not supported by the
     * btree implementation, because row locations are not returned outside
     * the interface.  At some point we may package a key as a row location
     * but that does not really give any more functionality than using scan
     * to find your key:
     *     ScanController.fetchLocation()
     *     ScanController.newRowLocationTemplate()
     *     ScanController.replace()
     *     ConglomerateController.delete()
     *     ConglomerateController.fetch()
	 *     ConglomerateController.insertAndFetchLocation()
	 *     ConglomerateController.newRowLocationTemplate()
     *     ConglomerateController.replace()
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_006(TransactionController tc)
        throws StandardException, T_Fail
    {
        REPORT("Starting t_006");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        createCongloms(tc, 2, false, false, 0, create_ret);

		// Open the base conglomerate.
		ConglomerateController base_cc =	
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the index conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a base row template.
        DataValueDescriptor[]   base_row    = TemplateRow.newU8Row(2);
        RowLocation             base_rowloc = base_cc.newRowLocationTemplate();

        T_SecondaryIndexRow index_row_from_base_row = new T_SecondaryIndexRow();
        index_row_from_base_row.init(base_row, base_rowloc, 3);
        ((SQLLongint)base_row[0]).setValue(1);

		// Create a row.
		T_SecondaryIndexRow index_row = new T_SecondaryIndexRow();
        index_row.init(TemplateRow.newU8Row(2), 
                        base_cc.newRowLocationTemplate(), 3);

        // test: make sure scan position is right after inserts before scan
        //       no split case.  In this case the slot position of the current
        //       position should change, but the code will keep a record handle
        //       and not need to reposition by key.
        // before keys: 1000, 3000
        // last key gotten froms scan : 0
        // insert keys:1-900
        // next key from scan should be: 5

        // insert 1000
        ((SQLLongint)base_row[1]).setValue(1000);
        base_cc.insertAndFetchLocation(base_row, base_rowloc);
        if (index_cc.insert(index_row_from_base_row.getRow()) != 0)
        {
			throw T_Fail.testFailMsg("insert failed");
        }

        // try each of the unsupported interfaces:
        try
        {
            index_cc.delete(null);
            return(FAIL("t_006: ConglomerateController.delete() succeeded."));
        }
        catch (StandardException e)
        {
        }
        try
        {
            if (!index_cc.fetch(
                    null, RowUtil.EMPTY_ROW, (FormatableBitSet) null))
            {
                return(FAIL("t_006: ConglomerateController.fetch() bad ret."));
            }
            return(FAIL("t_006: ConglomerateController.fetch() succeeded."));
        }
        catch (StandardException e)
        {
        }
        try
        {
            index_cc.insertAndFetchLocation((DataValueDescriptor[]) null, null);
            return(FAIL(
                "t_006: ConglomerateController.insertAndFetchLocation() succeeded."));
        }
        catch (StandardException e)
        {
        }
        try
        {
            RowLocation rowloc = index_cc.newRowLocationTemplate();
            return(FAIL(
                "t_006: ConglomerateController.newRowLocationTemplate() succeeded."));
        }
        catch (StandardException e)
        {
        }
        try
        {
            index_cc.replace(null, null, null);
            return(FAIL("t_006: ConglomerateController.replace() succeeded."));
        }
        catch (StandardException e)
        {
        }

        index_cc.close();

        // open a new scan

        ScanController scan = 
            tc.openScan(create_ret.index_conglomid, false, 
                        0,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE,
						(FormatableBitSet) null, 
                        null, ScanController.NA,
                        null,
                        null, ScanController.NA);

        int numrows = 0;
        while (scan.next())
        {
            numrows++;

            scan.fetch(index_row_from_base_row.getRow());

            try
            {
                scan.fetchLocation(null);
                return(FAIL("t_006: scan.fetchLocation() succeeded"));
            }
            catch (StandardException e)
            {
            }

            try
            {
                RowLocation rowloc = scan.newRowLocationTemplate();
                return(FAIL("t_006: scan.newRowLocationTemplate() succeeded"));
            }
            catch (StandardException e)
            {
            }

            try
            {
                scan.replace(index_row_from_base_row.getRow(), (FormatableBitSet) null);
                return(FAIL("t_006: scan.replace() succeeded"));
            }
            catch (StandardException e)
            {
            }

        }

        // make sure that scan.next() continues to return false
        if (scan.next())
            return(FAIL("t_006: scan.next() returned true after false."));

        scan.close();

        if (numrows != 1)
        {
            return(FAIL("(t_scan) wrong number of rows. Expected " +
                   "1 row, but got " + numrows + "rows."));
        }

        REPORT("Ending t_006");
        return(true);
    }

    /**
     * Test multiple scans in a single page/no split 
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_007(TransactionController tc)
        throws StandardException, T_Fail
    {
        boolean     ret_val     = true;

        REPORT("Starting t_007");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        createCongloms(tc, 2, false, false, 0, create_ret);

		// Open the base conglomerate.
		ConglomerateController base_cc =	
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the index conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row.
		T_SecondaryIndexRow   index_row = new T_SecondaryIndexRow();
        DataValueDescriptor[] base_row  = TemplateRow.newU8Row(2);
        RowLocation           row_loc   = base_cc.newRowLocationTemplate();
        index_row.init(base_row, row_loc, 3);

		// Create a row.
        ((SQLLongint)(index_row.getRow()[0])).setValue(1);

        // test: make sure scan position is right after inserts before scan
        //       no split case.  In this case the slot position of the current
        //       position should change, but the code will keep a record handle
        //       and not need to reposition by key.
        // before keys: 3, 5
        // last key gotten froms scan : 0
        // insert keys:1, 2
        // next key from scan should be: 5

        // insert 3
        ((SQLLongint)(index_row.getRow()[1])).setValue(3);
        base_cc.insertAndFetchLocation(base_row, row_loc);
        if (index_cc.insert(index_row.getRow()) != 0)
			throw T_Fail.testFailMsg("insert failed");

        // insert 5
        ((SQLLongint)(index_row.getRow()[1])).setValue(5);
        base_cc.insertAndFetchLocation(base_row, row_loc);
        if (index_cc.insert(index_row.getRow()) != 0)
			throw T_Fail.testFailMsg("insert failed");

        // open a new scan

        ScanController scan = 
            tc.openScan(create_ret.index_conglomid, false,
                        0,                        
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE,
						(FormatableBitSet) null,
                        null, ScanController.NA,
                        null,
                        null, ScanController.NA);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(scan.next());

        scan.fetch(index_row.getRow());
        long key = ((SQLLongint)(index_row.getRow()[1])).getLong();

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(key == 3);

        // insert 1
        ((SQLLongint)(index_row.getRow()[1])).setValue(1);
        base_cc.insertAndFetchLocation(base_row, row_loc);
        if (index_cc.insert(index_row.getRow()) != 0)
			throw T_Fail.testFailMsg("insert failed");

        // insert 2
        ((SQLLongint)(index_row.getRow()[1])).setValue(2);
        base_cc.insertAndFetchLocation(base_row, row_loc);
        if (index_cc.insert(index_row.getRow()) != 0)
			throw T_Fail.testFailMsg("insert failed");

        // current position should not have changed
        scan.fetch(index_row.getRow());
        key = ((SQLLongint)(index_row.getRow()[1])).getLong();

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(key == 3);

        // next position should be 5
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(scan.next());
        scan.fetch(index_row.getRow());
        key = ((SQLLongint)(index_row.getRow()[1])).getLong();

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(key == 5);

        index_cc.close();
        scan.close();

        REPORT("Ending t_007");

        return(ret_val);
    }
    /**
     * Test multiple scans in a single table/with splits
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_008(TransactionController tc)
        throws StandardException, T_Fail
    {
        boolean     ret_val     = true;

        REPORT("Starting t_008");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        createCongloms(tc, 2, false, false, 0, create_ret);

		// Open the base conglomerate.
		ConglomerateController base_cc =	
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the index conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a base row template.
        DataValueDescriptor[]   base_row    = TemplateRow.newU8Row(2);
        RowLocation             base_rowloc = base_cc.newRowLocationTemplate();

        T_SecondaryIndexRow index_row_from_base_row = new T_SecondaryIndexRow();
        index_row_from_base_row.init(base_row, base_rowloc, 3);
        ((SQLLongint)base_row[0]).setValue(1);

		// Create a row.
		T_SecondaryIndexRow index_row = new T_SecondaryIndexRow();
        index_row.init(TemplateRow.newU8Row(2), 
                        base_cc.newRowLocationTemplate(), 3);

        // test: make sure scan position is right after inserts before scan
        //       no split case.  In this case the slot position of the current
        //       position should change, but the code will keep a record handle
        //       and not need to reposition by key.
        // before keys: 1000, 3000
        // last key gotten froms scan : 0
        // insert keys:1-900
        // next key from scan should be: 5

        // insert 1000
        ((SQLLongint)base_row[1]).setValue(1000);
        base_cc.insertAndFetchLocation(base_row, base_rowloc);
        if (index_cc.insert(index_row_from_base_row.getRow()) != 0)
        {
			throw T_Fail.testFailMsg("insert failed");
        }

        // open a new scan

        ScanController scan = 
            tc.openScan(create_ret.index_conglomid, false,
                        0,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE,
						(FormatableBitSet) null,
                        null, ScanController.NA,
                        null,
                        null, ScanController.NA);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(scan.next());

        scan.fetch(index_row.getRow());
        long key = ((SQLLongint)(index_row.getRow()[1])).getLong();

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(key == 1000);

        // The following test works best if 5 rows fit on one page

        //for (int i = 1; i < 900; i++)
        for (int i = 0; i < 6; i++)
        {
            // insert i
            ((SQLLongint)base_row[1]).setValue(i);
            base_cc.insertAndFetchLocation(base_row, base_rowloc);

            if (index_cc.insert(index_row_from_base_row.getRow()) != 0)
            {
                throw T_Fail.testFailMsg("insert failed");
            }

            // About every 2nd split page, recheck the position
            
            if (i % 10 == 0)
            {
                // current position should not have changed
                scan.fetch(index_row.getRow());
                key = ((SQLLongint)(index_row.getRow()[1])).getLong();
                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(key == 1000);
            }

        }

        // insert 3000
        ((SQLLongint)base_row[1]).setValue(3000);
        base_cc.insertAndFetchLocation(base_row, base_rowloc);
        if (index_cc.insert(index_row_from_base_row.getRow()) != 0)
        {
			throw T_Fail.testFailMsg("insert failed");
        }

        // current position should not have changed
        scan.fetch(index_row.getRow());
        key = ((SQLLongint)(index_row.getRow()[1])).getLong();

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(key == 1000);

        // next position should be 3000
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(scan.next());
        scan.fetch(index_row.getRow());
        key = ((SQLLongint)(index_row.getRow()[1])).getLong();

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(key == 3000);

        index_cc.checkConsistency();

        index_cc.close();
        scan.close();

        REPORT("Ending t_008");

        return(ret_val);
    }

    /**
     * Test unique/nonunique indexes - both positive and negative cases.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
    protected boolean t_009(TransactionController tc)
        throws StandardException, T_Fail
    {
        ScanController scan         = null;

        REPORT("Starting t_009");

        // NON-UNIQUE INDEX
        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        createCongloms(tc, 2, false, false, 2, create_ret);

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the secondary index
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row and insert into base table, remembering it's location.
		DataValueDescriptor[] r1             = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow  index_row1      = new T_SecondaryIndexRow();
        RowLocation          base_rowloc1    = base_cc.newRowLocationTemplate();

        index_row1.init(r1, base_rowloc1, 3);

        ((SQLLongint)r1[0]).setValue(1);
        ((SQLLongint)r1[1]).setValue(1000);

        // Insert the row into the base table;remember its location.
        base_cc.insertAndFetchLocation(r1, base_rowloc1);

        // Insert the row into the secondary index.
        if (index_cc.insert(index_row1.getRow()) != 0)
			throw T_Fail.testFailMsg("insert failed");

        if (index_cc.insert(index_row1.getRow()) != 
                ConglomerateController.ROWISDUPLICATE)
        {
            throw T_Fail.testFailMsg(
                "insert of duplicate returned wrong return value:");
        }

        // Delete the only entry and make sure it can be reinserted in same
        // xact.
        DataValueDescriptor[] delete_key = TemplateRow.newU8Row(2);
        ((SQLLongint)delete_key[0]).setValue(1);
        ((SQLLongint)delete_key[1]).setValue(1000);

        if (!t_delete(tc, create_ret.index_conglomid, 
                 delete_key, false))
        {
            throw T_Fail.testFailMsg(
                "t_008: could not delete key.");
        }

        if (index_cc.insert(index_row1.getRow()) != 0)
            throw T_Fail.testFailMsg("insert failed");

        tc.commit();

        // UNIQUE INDEX
        create_ret = new T_CreateConglomRet();

        // Create the btree so that it only allows 2 rows per page.
        createCongloms(tc, 2, true, false, 2, create_ret);

        // Open the base table
        base_cc = tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the secondary index
		index_cc =	tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row and insert into base table, remembering it's location.
		r1              = TemplateRow.newU8Row(2);
        index_row1      = new T_SecondaryIndexRow();
        base_rowloc1    = base_cc.newRowLocationTemplate();

        index_row1.init(r1, base_rowloc1, 3);

        ((SQLLongint)r1[0]).setValue(1);
        ((SQLLongint)r1[1]).setValue(1000);

        // Insert the row into the base table;remember its location.
        base_cc.insertAndFetchLocation(r1, base_rowloc1);

        // Insert the row into the secondary index.
        if (index_cc.insert(index_row1.getRow()) != 0)
			throw T_Fail.testFailMsg("insert failed");

        // Insert the row into the base table;remember its location.
        base_cc.insertAndFetchLocation(r1, base_rowloc1);

        // Insert the row into the secondary index.
        if (index_cc.insert(index_row1.getRow()) != 
                ConglomerateController.ROWISDUPLICATE)
        {
            throw T_Fail.testFailMsg(
                "insert of duplicate returned wrong return value:");
        }

        // Delete the only entry and make sure it can be reinserted in same
        // xact.
        delete_key = TemplateRow.newU8Row(2);
        ((SQLLongint)delete_key[0]).setValue(1);
        ((SQLLongint)delete_key[1]).setValue(1000);

        if (!t_delete(tc, create_ret.index_conglomid, 
                 delete_key, false))
        {
            throw T_Fail.testFailMsg(
                "t_008: could not delete key.");
        }


        if (index_cc.insert(index_row1.getRow()) != 0)
            throw T_Fail.testFailMsg("insert failed");

        REPORT("Ending t_009");

        return(true);
    }

    /**
     * Test restoreToNull
     *
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_010(TransactionController tc)
        throws StandardException, T_Fail
    {
        REPORT("Starting t_006");

        B2I testbtree = new B2I();

        // test restoreToNull()
        testbtree.restoreToNull();

        if (!(testbtree.isNull()))
            throw T_Fail.testFailMsg("bad restoreToNull/isNull");

        // test bad container open not working.
        try
        {

            // the following open should fail with a containerNotFound error
            
            // for testing purposes - assume TransactionController can be casted
            TransactionManager tm = (TransactionManager) tc;
            ConglomerateController cc = 
                testbtree.open(
                    tm, tm.getRawStoreXact(), false, 0, 0, 
                    (LockingPolicy) null, 
                    null, null);

            throw T_Fail.testFailMsg("bad open succeeded.");
        }
        catch(StandardException t)
        {
            // expected path comes here.
        }


        // create the base table
        DataValueDescriptor[]   base_row        = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow     index_row1      = new T_SecondaryIndexRow();

        long base_conglomid = 
            tc.createConglomerate(
                "heap",   // create a heap conglomerate
                base_row, // base table template row
                null, //column sort order - not required for heap
                null, //default collation
                null,     // default properties
                TransactionController.IS_DEFAULT);

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                base_conglomid, 
                false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        RowLocation         base_rowloc1    = base_cc.newRowLocationTemplate();

        index_row1.init(base_row, base_rowloc1, 3);

        // create the secondary index
        Properties properties = 
            createProperties(
                null,           // no current properties list 
                false,          // don't allow duplicates
                3,   // index on all base row cols + row location
                2,   // non-unique index
                true,           // maintain parent links
                -42, // fake base conglom for now
                2);      // row loc in last column

        TransactionManager tm = (TransactionManager) tc;

        // test bad property
        try
        {
            testbtree.create(
                tm, -1, ContainerHandle.DEFAULT_ASSIGN_ID, index_row1.getRow(),
                null, null, null, TransactionController.IS_TEMPORARY); 

            throw T_Fail.testFailMsg("bad create succeeded.");
        }
        catch (StandardException t)
        {
            // expected path comes here.
        }

        // try bad properties

        // create the secondary index
        properties = createProperties(
                            null,           // no current properties list 
                            false,          // don't allow duplicates
                            3,   // index on all base row cols + row location
                            1,   // 1 unique field leaving 1 non key field 
                                 // other than the rowlocation - should not be
                                 // allowed.
                            true,           // maintain parent links
                            -42, // fake base conglom for now
                            2);      // row loc in last column
        try
        {
            long index_conglomid = 
                tc.createConglomerate(
                    "BTREE",    				// create a btree secondary
                    index_row1.getRow(), // row template
					null, //column sort order - default
                    null, //default collation
                    properties,                 // properties
                    TransactionController.IS_DEFAULT); // not temporary

            throw T_Fail.testFailMsg("bad create succeeded.");

        }
        catch (Throwable t)
        {
            // expected path comes here
        }


        REPORT("Ending t_010");
        return(true);
    }

    /**
     * Test Special cases of split.
     * <p>
     * Testing: restartSplitFor() call in LeafControlRow().
     *
     * The first case is where we split
     * down the tree and reach the leaf, pick a split point, and then find
     * that there is not enough room to insert row into parent branch page.
     *
     * The second case is the same as the first except the calling code is
     * trying to split a branch page and the parent branch page doesn't have
     * room for the row.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
    protected boolean t_011(TransactionController tc)
        throws StandardException, T_Fail
    {
        boolean     ret_val = true;

        REPORT("Starting t_011");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        createCongloms(tc, 2, false, true, 0, create_ret);

		// Open the base conglomerate.
		ConglomerateController base_cc =	
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the index conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);


		// Create a row.
		T_SecondaryIndexRow index_row = new T_SecondaryIndexRow();
        RowLocation             rowloc    = base_cc.newRowLocationTemplate();
        DataValueDescriptor[]   base_row  = TemplateRow.newU8Row(2);
        base_row[0] = new SQLChar("aaaaaaaaaa");
        index_row.init(base_row, rowloc, 3); 

        ((SQLChar)base_row[0]).setValue(T_b2i.repeatString("a", 1000));
        ((SQLLongint)base_row[1]).setValue(1);
        base_cc.insertAndFetchLocation(base_row, rowloc);

        // CAUSE LEAF splitFor to loop:
        // pick numbers so that split will happen in middle of page.  Do this
        // by first inserting last row in table and then insert smaller rows,
        // then insert rows before it until the table is just ready to split
        // the root, and finally insert some shorter rows in such a way as
        // they cause a split but the split point is chosen with one of the
        // larger rows as the descriminator causing 1st splitfor pass to fail
        // and loop back and do a splitFor the larger row.
        
        ((SQLChar)base_row[0]).setValue(T_b2i.repeatString("m", 1000));
        {
            ((SQLLongint)base_row[1]).setValue(0);
            base_cc.insertAndFetchLocation(base_row, rowloc);

            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        // insert enough rows to make a 2 level btree where if another row
        // with a 1000 byte string would cause a root split.
        ((SQLChar)base_row[0]).setValue(T_b2i.repeatString("a", 1000));
        for (int i = 0; i < 5; i++)
        {
            ((SQLLongint)base_row[1]).setValue(i);
            base_cc.insertAndFetchLocation(base_row, rowloc);

            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        // insert a shorter leaf row, such that it will fit in the root, but
        // make the split point pick a longer row descriminator which won't
        // fit in the root page.
        ((SQLChar)base_row[0]).setValue(T_b2i.repeatString("z", 500));
        for (int i = 10; i > 8; i--)
        {
            ((SQLLongint)base_row[1]).setValue(i);
            base_cc.insertAndFetchLocation(base_row, rowloc);

            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        index_cc.checkConsistency();

		// Close the conglomerate.
		index_cc.close();

        tc.dropConglomerate(create_ret.index_conglomid);
        tc.dropConglomerate(create_ret.base_conglomid);

        tc.abort();

        REPORT("Ending t_011");

        return(ret_val);
    }
    /**
     * Test Special cases of split.
     * <p>
     * Testing: restartSplitFor() call in BranchControlRow().
     *
     * The second case is the same as the first except the calling code is
     * trying to split a branch page and the parent branch page doesn't have
     * room for the row.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
    protected boolean t_012(TransactionController tc)
        throws StandardException, T_Fail
    {
        boolean     ret_val = true;

        REPORT("Starting t_011");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        createCongloms(tc, 2, false, true, 0, create_ret);

		// Open the base conglomerate.
		ConglomerateController base_cc =	
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the index conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);


		// Create a row.
		T_SecondaryIndexRow     index_row = new T_SecondaryIndexRow();
        RowLocation             rowloc    = base_cc.newRowLocationTemplate();
        DataValueDescriptor[]   base_row  = TemplateRow.newU8Row(2);
        base_row[0] = new SQLChar("aaaaaaaaaa");
        index_row.init(base_row, rowloc, 3); 

        ((SQLChar)base_row[0]).setValue(T_b2i.repeatString("a", 1000));
        ((SQLLongint)base_row[1]).setValue(1);
        base_cc.insertAndFetchLocation(base_row, rowloc);


        // CAUSE BRANCH splitFor to loop:
        // pick numbers so that split will happen in middle of page.  Do this
        // by first inserting last row in table and then insert smaller rows,
        // then insert rows before it until the table is just ready to split
        // the root, and finally insert some shorter rows in such a way as
        // they cause a split but the split point is chosen with one of the
        // larger rows as the descriminator causing 1st splitfor pass to fail
        // and loop back and do a splitFor the larger row.
        

        // insert enough rows so the tree is 3 levels, just ready to go to 
        // 4 levels.
        ((SQLChar)base_row[0]).setValue(T_b2i.repeatString("ma", 500));
        for (int i = 0; i < 3; i++)
        {
            ((SQLLongint)base_row[1]).setValue(i);
            base_cc.insertAndFetchLocation(base_row, rowloc);

            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }
        ((SQLChar)base_row[0]).setValue(T_b2i.repeatString("m", 1000));
        for (int i = 3; i < 23; i++)
        {
            ((SQLLongint)base_row[1]).setValue(i);
            base_cc.insertAndFetchLocation(base_row, rowloc);

            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }
        
        ((SQLChar)base_row[0]).setValue(T_b2i.repeatString("a", 600));
        for (int i = 123; i > 111; i--)
        {
            ((SQLLongint)base_row[1]).setValue(i * 2);
            base_cc.insertAndFetchLocation(base_row, rowloc);

            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }
        {
            ((SQLLongint)base_row[1]).setValue(227);
            base_cc.insertAndFetchLocation(base_row, rowloc);

            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        // ((B2IController)index_cc).printTree();
        tc.commit();

		// Close the conglomerate.
		index_cc.close();


        REPORT("Ending t_012");

        return(ret_val);
    }

    /**
     * Test backout during critical times of splits.
     * <p>
     * Force logical undo of an operation which generated an internal update
     * of a btree record:
     * case 1:
     *     o insert into unique btree key1, rowlocation_1
     *     o delete from btree        key1, rowlocation_1
     *         - this will mark the record logically deleted.
     *     o insert enough records to move the logically deleted row to another
     *       page to exercise logical undo of the delete.
     *     o insert into btree        key1, rowlocation_2
     *         - this internally will generate a logical update field on the
     *           record.
     *     o insert enough records to move the logically deleted row to another
     *       page to exercise logical undo of the delete.
     *     o abort.
     *
     * case 2:
     *     o same as case 1 but don't change the rowlocation_1 value.  This 
     *       simulates what the language will generate on an update of a key
     *       field.
     *
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
    protected boolean t_013(TransactionController tc)
        throws StandardException, T_Fail
    {
        ScanController scan         = null;

        // SanityManager.DEBUG_SET("LockTrace");

        REPORT("Starting t_013");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        // Create the btree so that it only allows 2 rows per page.
        createCongloms(tc, 2, true, false, 5, create_ret);

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the secondary index
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create an index row object for the "delete row"
		DataValueDescriptor[]         r1     = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow     index_row1   = new T_SecondaryIndexRow();
        RowLocation             base_rowloc1 = base_cc.newRowLocationTemplate();

        index_row1.init(r1, base_rowloc1, 3);

        // Create another index row object for the other inserts.
		DataValueDescriptor[]         r2              =  TemplateRow.newU8Row(2);
        T_SecondaryIndexRow index_row2      = new T_SecondaryIndexRow();
        RowLocation         base_rowloc2    = base_cc.newRowLocationTemplate();

        index_row2.init(r2, base_rowloc2, 3);

        // Commit the create of the tables so that the following aborts don't
        // undo that work.
        tc.commit();

        // CASE 1:
        tc.commit();

        // Open the base table
        base_cc = tc.openConglomerate(
                create_ret.base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // Open the secondary index
        index_cc =	tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        ((SQLLongint)r1[0]).setValue(1);

        // insert row which will be deleted (key = 100, base_rowloc1):
        ((SQLLongint)r1[1]).setValue(100);
        base_cc.insertAndFetchLocation(r1, base_rowloc1);

        // Insert the row into the secondary index.
        if (index_cc.insert(index_row1.getRow()) != 0)
            throw T_Fail.testFailMsg("insert failed");

        // insert enough rows so that the logical undo of the insert will
        // need to search the tree.  The tree has been set to split after
        // 5 rows are on a page, so 10 should be plenty.
        for (int i = 0; i < 10; i++)
        {
            ((SQLLongint)r2[1]).setValue(i);

            // Insert the row into the base table;remember its location.
            base_cc.insertAndFetchLocation(r2, base_rowloc2);

            // Insert the row into the secondary index.
            if (index_cc.insert(index_row2.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        // delete row which was inserted (key = 100, base_rowloc1):
        if (!t_delete(tc, create_ret.index_conglomid, 
                index_row1.getRow(), false))
        {
            throw T_Fail.testFailMsg(
                "t_008: could not delete key.");
        }
        base_cc.delete(base_rowloc1);

        // insert enough rows so that the logical undo of the delete will
        // need to search the tree.  The tree has been set to split after
        // 5 rows are on a page, so 10 should be plenty.
        for (int i = 10; i < 20; i++)
        {
            ((SQLLongint)r2[1]).setValue(i);

            // Insert the row into the base table;remember its location.
            base_cc.insertAndFetchLocation(r2, base_rowloc2);

            // Insert the row into the secondary index.
            if (index_cc.insert(index_row2.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        // insert row which will be deleted (key = 100, base_rowloc1):
        ((SQLLongint)r1[1]).setValue(100);
        base_cc.insertAndFetchLocation(r1, base_rowloc1);

        // Insert the row into the secondary index.
        if (index_cc.insert(index_row1.getRow()) != 0)
            throw T_Fail.testFailMsg("insert failed");

        // insert enough rows so that the logical undo of the update field will
        // need to search the tree.  The tree has been set to split after
        // 5 rows are on a page, so 10 should be plenty.
        for (int i = 20; i < 30; i++)
        {
            ((SQLLongint)r2[1]).setValue(i);

            // Insert the row into the base table;remember its location.
            base_cc.insertAndFetchLocation(r2, base_rowloc2);

            // Insert the row into the secondary index.
            if (index_cc.insert(index_row2.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        // RESOLVE (mikem) - check that the right row is at key 100.
        

        tc.abort();

        // index check - there should be no records left.
        ScanController empty_scan = 
            tc.openScan(create_ret.index_conglomid, false,
                        0,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE,
						(FormatableBitSet) null,
                        null, ScanController.NA,
                        null,
                        null, ScanController.NA);

        if (empty_scan.next())
            throw T_Fail.testFailMsg("t_002: there are still rows in table.");

        tc.commit();
        REPORT("Ending t_013");

        return true;
    }

    /**
     * Test getTableProperties() of BTreeController.
     * <p>
     *
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
    protected boolean t_014(TransactionController tc)
        throws StandardException, T_Fail
    {
        ScanController scan         = null;

        // SanityManager.DEBUG_SET("LockTrace");

        REPORT("Starting t_014");


        // create the base table
        DataValueDescriptor[] base_row        = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow   index_row1      = new T_SecondaryIndexRow();

        long base_conglomid = 
            tc.createConglomerate(
                "heap",   // create a heap conglomerate
                base_row, // base table template row
                null, //column sort order - not required for heap
                null, //default collation
                null,     // default properties
                TransactionController.IS_DEFAULT);

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                base_conglomid, 
                false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        RowLocation         base_rowloc1    = base_cc.newRowLocationTemplate();

        index_row1.init(base_row, base_rowloc1, 3);

        // create the secondary index
        Properties properties = 
            createProperties(
                null,           // no current properties list 
                false,          // don't allow duplicates
                3,              // index on all base row cols + row location
                2,              // non-unique index
                true,           // maintain parent links
                base_conglomid, // fake base conglom for now
                2);             // row loc in last column

        properties.put(Property.PAGE_SIZE_PARAMETER,           "8192");
        properties.put(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER, "99");
        properties.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER, "42");

        TransactionManager tm = (TransactionManager) tc;

		// Create a index.
		long conglomid = 
            tc.createConglomerate(
                "BTREE",       // create a heap conglomerate
                index_row1.getRow(), // 1 column template.
				null, //column sort order - default
                null, //default collation
                properties,         // default properties
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
        Properties ret_prop = new Properties();
        ret_prop.put(Property.PAGE_SIZE_PARAMETER,           "");
        ret_prop.put(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER, "");
        ret_prop.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER,   "");

        cc.getTableProperties(ret_prop);

        if (ret_prop.getProperty(Property.PAGE_SIZE_PARAMETER).
                compareTo("8192") != 0         ||
            ret_prop.getProperty(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER).
                compareTo("0") != 0           ||
            ret_prop.getProperty(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER).
                compareTo("1") != 0)
        {
			throw T_Fail.testFailMsg(
                "(getTableProperties) Did not get expected table propertes." +
                "\nGot pageSize = " + 
                    ret_prop.getProperty(Property.PAGE_SIZE_PARAMETER) +
                "\nGot reserved = " +
                    ret_prop.getProperty(
                        RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER) +
                "\nGot minimum record size = " +
                    ret_prop.getProperty(
                        RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER));
        }

        tc.commit();

        REPORT("Ending t_014");
        return(true);
    }

    /**
     * Test latch release during critical time during row level locking.
     * <p>
     * Use trace points to force errors in split at critical points:
     *     leaf_split_abort{1,2,3,4}
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
    protected boolean t_015(TransactionController tc)
        throws StandardException, T_Fail
    {
        ScanController scan         = null;

        // SanityManager.DEBUG_SET("LockTrace");

        REPORT("Starting t_015");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        // Create the btree so that it only allows 2 rows per page.
        createCongloms(tc, 2, false, false, 2, create_ret);

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the secondary index
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        if (!(index_cc instanceof B2IController))
        {
			throw T_Fail.testFailMsg("openConglomerate returned wrong type");
        }

        index_cc.checkConsistency();

		// Create a row and insert into base table, remembering it's location.
		DataValueDescriptor[] r1           = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow   index_row1   = new T_SecondaryIndexRow();
        RowLocation           base_rowloc1 = base_cc.newRowLocationTemplate();

        index_row1.init(r1, base_rowloc1, 3);

        // Commit the create of the tables so that the following aborts don't
        // undo that work.
        tc.commit();

        // Now load up the table with multiple pages of data.

        // Open the base table
        base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // Open the secondary index
        index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // now insert enough rows to cause failure
        for (int i = 100; i > 0; i -= 2)
        {
            ((SQLLongint)r1[0]).setValue(2);
            ((SQLLongint)r1[1]).setValue(i);

            // Insert the row into the base table;remember its location.
            base_cc.insertAndFetchLocation(r1, base_rowloc1);

            // Insert the row into the secondary index.
            if (index_cc.insert(index_row1.getRow()) != 0)
            {
                throw T_Fail.testFailMsg("insert failed");
            }
        }

        // Now try simulated lock wait/latch release paths through the code.
        String[] latch_debug_strings = {
            "B2iRowLocking3_1_lockScanRow1",
            "B2iRowLocking3_2_lockScanRow1",
            "BTreeScan_positionAtStartPosition1",
            // "BTreeScan_reposition1",
            "BTreeScan_fetchNextGroup1",
        };

        for (int errs = 0; errs < latch_debug_strings.length; errs++)
        {
            REPORT("Doing latch release tests: " + latch_debug_strings[errs]);

            // set the debug flag, which will cause a simulated lock wait
            // latch release path through the code.
            if (SanityManager.DEBUG)
                SanityManager.DEBUG_SET(latch_debug_strings[errs]);

            // Just scan the rows and make sure you see them all, mostly just
            // a test to make sure no errors are thrown by the latch release
            // code paths.
            scan = tc.openScan(create_ret.index_conglomid, false, 
                    0,
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE,
                    (FormatableBitSet) null,
                    null, ScanController.NA,
                    null,
                    null, ScanController.NA);

            int row_count = 0;
            while (scan.next())
            {
                row_count++;
            }

            scan.close();

            if (row_count != 50)
                throw T_Fail.testFailMsg("wrong scan count = " + row_count);
        }

        tc.abort();
        REPORT("Ending t_015");

        return true;
    }

    /**
     * Test deadlocks during critical times of row level locking.
     * <p>
     * Use trace points to force errors in split at critical points:
     *     leaf_split_abort{1,2,3,4}
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
    protected boolean t_016(TransactionController tc)
        throws StandardException, T_Fail
    {
        ScanController scan         = null;

        // SanityManager.DEBUG_SET("LockTrace");

        REPORT("Starting t_016");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        // Create the btree so that it only allows 2 rows per page.
        createCongloms(tc, 2, false, false, 2, create_ret);

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the secondary index
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        if (!(index_cc instanceof B2IController))
        {
			throw T_Fail.testFailMsg("openConglomerate returned wrong type");
        }

        index_cc.checkConsistency();

		// Create a row and insert into base table, remembering it's location.
		DataValueDescriptor[] r1            = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow   index_row1    = new T_SecondaryIndexRow();
        RowLocation           base_rowloc1  = base_cc.newRowLocationTemplate();

        index_row1.init(r1, base_rowloc1, 3);

        // Commit the create of the tables so that the following aborts don't
        // undo that work.
        tc.commit();

        // Open the base table
        base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // Open the secondary index
        index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // now insert enough rows to cause failure
        for (int i = 100; i > 0; i -= 2)
        {
            ((SQLLongint)r1[0]).setValue(2);
            ((SQLLongint)r1[1]).setValue(i);

            // Insert the row into the base table;remember its location.
            base_cc.insertAndFetchLocation(r1, base_rowloc1);

            // Insert the row into the secondary index.
            if (index_cc.insert(index_row1.getRow()) != 0)
            {
                throw T_Fail.testFailMsg("insert failed");
            }
        }

        tc.abort();

        // Now try simulated deadlocks
        // RESOLVE (Mikem) - test out aborts and errors during inserts.
        String[] deadlock_debug_strings = {
            "B2iRowLocking3_1_lockScanRow2",
            "B2iRowLocking3_2_lockScanRow2",
            // "BTreeController_doIns2",
            "BTreeScan_positionAtStartPosition2",
            // "BTreeScan_reposition2",
            "BTreeScan_fetchNextGroup2"
        };

        for (int errs = 0; errs < deadlock_debug_strings.length; errs++)
        {
            try
            {
                REPORT("Doing deadlock tests: " + deadlock_debug_strings[errs]);

                // set the debug flag, which will cause a simulated lock wait
                // latch release path through the code.
                if (SanityManager.DEBUG)
                    SanityManager.DEBUG_SET(deadlock_debug_strings[errs]);

                // Just scan the rows and make sure you see them all, mostly just
                // a test to make sure no errors are thrown by the latch release
                // code paths.
                scan = tc.openScan(create_ret.index_conglomid, false, 
                        0,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_SERIALIZABLE,
                        (FormatableBitSet) null,
                        null, ScanController.NA,
                        null,
                        null, ScanController.NA);

                int row_count = 0;
                while (scan.next())
                {
                    row_count++;
                }

                scan.close();

                throw T_Fail.testFailMsg("expected deadlock");
            }
            catch (StandardException e)
            {
				if (!e.getMessageId().equals(SQLState.DEADLOCK))
					throw e;

                ContextService contextFactory = 
                    ContextService.getFactory();

                // Get the context manager.
                ContextManager cm = contextFactory.getCurrentContextManager();

                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(cm != null);

                cm.cleanupOnError(e, isdbActive());
            }
        }

        tc.commit();
        REPORT("Ending t_016");

        return true;
    }

    /**
     * Test simple btree insert performance 
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_perf(TransactionController tc)
        throws StandardException, T_Fail
    {
        boolean     ret_val = true;

        REPORT("Starting t_005");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        createCongloms(tc, 2, false, false, 0, create_ret);

		// Open the base conglomerate.
		ConglomerateController base_cc =	
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the index conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row.
		T_SecondaryIndexRow index_row = new T_SecondaryIndexRow();
        RowLocation             rowloc    = base_cc.newRowLocationTemplate();
        DataValueDescriptor[]   base_row  = TemplateRow.newU8Row(2);
        index_row.init(base_row, rowloc, 3); 

        ((SQLLongint)base_row[0]).setValue(1);
        ((SQLLongint)base_row[1]).setValue(1);
        base_cc.insertAndFetchLocation(base_row, rowloc);

        long startms = System.currentTimeMillis();

        // insert them in reverse order just to make sure btree is sorting them
        for (int i = 0; i < 2000; i++)
        {
            ((SQLLongint)base_row[1]).setValue(i);
            // base_cc.insertAndFetchLocation(base_row, rowloc);
            // ((HeapRowLocation)rowloc).setFrom(0xffffffffffffffffl, 0xfffffff); 

            if (index_cc.insert(index_row.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        // ((B2IController)index_cc).printTree();
        tc.commit();
        long endms = System.currentTimeMillis();
        long elapsedms = endms - startms;

		System.out.println("  Elapsed (ms)      " + elapsedms);
		System.out.println("  inserts/second " + (1000 * 1000 / elapsedms));

		// Close the conglomerate.
		index_cc.close();


        REPORT("Ending t_011");

        return(ret_val);
    }

   private boolean t_desc_scan_test_cases(
    TransactionController   tc,
    long                    index_conglomid,
    T_SecondaryIndexRow     template
    )
        throws StandardException, T_Fail
    {
        boolean     ret_val = true;

        // run through a predicates as described in the openScan() interface //
        DataValueDescriptor[] start_key  = TemplateRow.newU8Row(1);
        DataValueDescriptor[] stop_key   = TemplateRow.newU8Row(1);


        // test predicate x = 5
        //
        //     result set should be:{5,6,18}, {5,4,17}, {5,6,16}  //descending 
        //
        REPORT("scan (x = 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE, 
                   null,
                   stop_key,  ScanController.GT,
                   3, 18, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }
                   
        //  +---------------------------------------------------------+
        //  |pred  |start|key|stop |key|rows returned |rows locked    |
        //  |      |value|op |value|op |              |(serialization)|
        //  +------+-----+---+-----+---+--------------+---------------+
        //  |x > 5 |null |na |5    |GE |{9,1} .. {6,1}|{5,6} .. {9,1} |
        //  +-----------------------------------------+---------------+
        REPORT("scan (x > 5)");
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   null, ScanController.NA, 
                   null,
                   stop_key,  ScanController.GE,
                   3, 21, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        //  +---------------------------------------------------------+
        //  |pred  |start|key|stop |key|rows returned |rows locked    |
        //  |      |value|op |value|op |              |(serialization)|
        //  +------+-----+---+-----+---+--------------+---------------+
        //  |x >= 5| null|na |5    |GT |{9,1} .. {5,2}|{4,6} .. {9,1} |
        //  +-----------------------------------------+---------------+
        REPORT("scan (x >= 5)");
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   null, ScanController.NA, 
                   null,
                   stop_key,  ScanController.GT,
                   6, 21, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        //
        //  +---------------------------------------------------------+
        //  |pred  |start|key|stop |key|rows returned |rows locked    |
        //  |      |value|op |value|op |              |(serialization)|
        //  +------+-----+---+-----+---+--------------+---------------+
        //  |x <= 5|5    |GE |null |na |{5,6} .. {1,1}|first .. {5,6} |
        //  +-----------------------------------------+---------------+
        REPORT("scan (x <= 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE, 
                   null,
                   null,  ScanController.NA,
                   8, 18, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }
        //
        //  +---------------------------------------------------------+
        //  |pred  |start|key|stop |key|rows returned |rows locked    |
        //  |      |value|op |value|op |              |(serialization)|
        //  +------+-----+---+-----+---+--------------+---------------+
        // 	|x < 5 |5    | GT|null |   |{4,6} .. {1,1}|first .. {4,6} |
        //  +-----------------------------------------+---------------+
        REPORT("scan (x < 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GT, 
                   null,
                   null,  ScanController.NA,
                   5, 15, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        //long        col1[]  = { 1,  3,  4,  4,  4,  5,  5,  5,  6,  7,  9};
        //long        col2[]  = { 1,  1,  2,  4,  6,  2,  4,  6,  1,  1,  1};
        //long        col3[]  = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};
        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
	    //  |x >= 5 and x <= 7|{7},  |GE|{5}  |GT|{5,2} .. {7,1}|{4,6} .. {7,1}|
        // 	+------------------------------------------------------------------+
        REPORT("scan (x >= 5 and x <= 7)");
        ((SQLLongint)start_key[0]).setValue(7);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   5, 20, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
	    //  |x = 5 and y > 2  |{5}   |GE|{5,2 |GE|{5,4} .. {5,6}|{5,2} .. {9,1}|
        // 	+------------------------------------------------------------------+
        REPORT("scan (x = 5 and y > 2)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(2);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        ((SQLLongint)stop_key[1]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GE,
                   2, 18, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
        // 	|x = 5 and y >= 2 |{5}  |GE|{5,2}|GT|{5,2} .. {5,6}|{4,6} .. {9,1}|
        // 	+------------------------------------------------------------------+
        REPORT("scan (x = 5 and y >= 2)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(2);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        ((SQLLongint)stop_key[1]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   3, 18, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
        // 	|x = 5 and y < 5  | {5,5}} |GE|{5}|GT|{5,2} .. {5,4}|{4,6} .. {5,4}|
        // 	+------------------------------------------------------------------+
        REPORT("scan (x = 5 and y < 5)");
        start_key  = TemplateRow.newU8Row(2);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)start_key[1]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   2, 17, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
        // 	|x = 2            | {2}  |GE| {2} |GT|none          |{1,1} .. {1,1}|
        // 	+------------------------------------------------------------------+
        REPORT("scan (x = 2)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(2);
        ((SQLLongint)stop_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   0, 0, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        //  +-----------------------------+
        // 	|minium on btree - row locked    |
        // 	+-----------------------------+
        //
        REPORT("minimum on btree, row locked.");
		// the following function actually returns 
		// the minimum values because the index is in descending order
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no min.");
        }
        else
        {
            // make sure right min was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 11)
            {
                throw T_Fail.testFailMsg("wrong minimum found.");
            }
        }

        //  +-----------------------------+
        // 	|min on btree - table locked    |
        // 	+-----------------------------+
        //
        REPORT("min on btree, table locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no min.");
        }
        else
        {
            // make sure right min was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 11)
            {
                throw T_Fail.testFailMsg("wrong min found.");
            }
        }

        //  +-----------------------------+
        // 	|min on btree - row locked    |
        // 	+-----------------------------+
        //
        REPORT("min on btree, row locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_READ_COMMITTED,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no max.");
        }
        else
        {
            // make sure right min was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 11)
            {
                throw T_Fail.testFailMsg("wrong min found.");
            }
        }

        //  +-----------------------------+
        // 	|min on btree - table locked    |
        // 	+-----------------------------+
        //
        REPORT("min on btree, table locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_READ_COMMITTED,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no min.");
        }
        else
        {
            // make sure right min was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 11)
            {
                throw T_Fail.testFailMsg("wrong min found.");
            }
        }

        return(ret_val);
    }


    /**
     * Test BTree.openScan(), BtreeScan.init(), BtreeScan.next(), 
     * BtreeScan.fetch() with descending indexes.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_017(TransactionController tc)
        throws StandardException, T_Fail
    {
        T_SecondaryIndexRow index_row = new T_SecondaryIndexRow();

        // base row template - last column is just to make row long so that
        // multiple pages are spanned.
        DataValueDescriptor[] base_row             = TemplateRow.newU8Row(4);
        base_row[3] = new SQLChar();

        String   string_1500char = new String();
        for (int i = 0; i < 300; i++)
            string_1500char += "mikem";

        boolean     ret_val = true;
        long        value   = -1;
        long        col1[]  = { 1,  3,  4,  4,  4,  5,  5,  5,  6,  7,  9};
        long        col2[]  = { 1,  1,  2,  4,  6,  2,  4,  6,  1,  1,  1};
        long        col3[]  = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};

        // set of deleted rows to make scans more interesting
        long d_col1[] ={ 0,  2,  3,  4,  4,  5,  5,  5,  6,  7,  8, 10, 11, 12};
        long d_col2[] ={ 1,  1,  2,  3,  5,  0,  3,  5,  0,  0,  1, 42, 42, 1};
        long d_col3[] ={91, 92, 93, 94, 95, 96, 97, 98, 99,100,101,102,103,104};

		

        REPORT("Starting t_017");

        // create the base table
        long base_conglomid = 
            tc.createConglomerate(
                "heap",                            // create a heap conglomerate
                base_row,                          // base table template row
				null, //column sort order - not required for heap
                null, //default collation
                null,                              // default properties
                TransactionController.IS_DEFAULT); // not temporary

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // initialize the secondary index row - pointing it at base row
        index_row.init(base_row, base_cc.newRowLocationTemplate(), 5);

        Properties properties = 
            createProperties(
                null,               // no current properties list 
                false,              // don't allow duplicates
                5,                  // 4 columns in index row
                5,                  // non-unique index
                true,               // maintain parent links
                base_conglomid,     // base conglom id
                4);                 // row loc in last column

		// create the index with all the columns in descending order
        ColumnOrdering order[] = new ColumnOrdering[5];
		order[0] =  new T_ColumnOrderingImpl(0, false); // descending
		order[1] =  new T_ColumnOrderingImpl(1, false); // descending
		order[2] =  new T_ColumnOrderingImpl(2, false); // descending
		order[3] =  new T_ColumnOrderingImpl(3, false); // descending
		order[4] =  new T_ColumnOrderingImpl(4, true); // asccending

		long index_conglomid = 
            tc.createConglomerate(
                "BTREE",    				// create a btree secondary
                index_row.getRow(),         // row template
				order, //column sort order - default
                null, //default collation
                properties,                 // properties
                TransactionController.IS_DEFAULT);   // not temporary

		// Open the conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row.
        T_SecondaryIndexRow template = new T_SecondaryIndexRow();
        RowLocation         row_loc  = base_cc.newRowLocationTemplate();
        template.init(base_row, row_loc, 5);

        // insert them in reverse order just to make sure btree is sorting them
        for (int i = col1.length - 1; i >= 0; i--)
        {
            ((SQLLongint)(template.getRow()[0])).setValue(col1[i]);
            ((SQLLongint)(template.getRow()[1])).setValue(col2[i]);
            ((SQLLongint)(template.getRow()[2])).setValue(col3[i]);
            base_row[3] = new SQLChar(string_1500char);

            base_cc.insertAndFetchLocation(base_row, row_loc);

            // Insert the row.
            // System.out.println("Adding record (" + -(i - (col1.length -1)) +
            //                ")" + template);
            if (index_cc.insert(template.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        index_cc.checkConsistency();

        ((B2IController)index_cc).debugConglomerate();

        ret_val = t_desc_scan_test_cases(tc, index_conglomid, template);

        // insert and delete some interesting rows, deleted space management
        // may or may not clean these up.
        for (int i = d_col1.length - 1; i >= 0; i--)
        {
            ((SQLLongint)(template.getRow()[0])).setValue(d_col1[i]);
            ((SQLLongint)(template.getRow()[1])).setValue(d_col2[i]);
            ((SQLLongint)(template.getRow()[2])).setValue(d_col3[i]);
            base_row[3] = new SQLChar(string_1500char);

            base_cc.insertAndFetchLocation(base_row, row_loc);

            // Insert the row.
            // System.out.println("Adding record (" + -(i - (col1.length -1)) +
            //                ")" + template);
            if (index_cc.insert(template.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");

            // now delete the row.
            base_cc.delete(row_loc);

            ScanController delete_scan = 
                tc.openScan(index_conglomid, false,
                            TransactionController.OPENMODE_FORUPDATE,
                            TransactionController.MODE_RECORD,
                            TransactionController.ISOLATION_SERIALIZABLE,
                            (FormatableBitSet) null,
                            template.getRow(), ScanController.GE, 
                            null,
                            template.getRow(), ScanController.GT); 

            if (!delete_scan.next())
            {
                throw T_Fail.testFailMsg("delete could not find key");
            }
            else
            {
                delete_scan.delete();

                if (delete_scan.next())
                    throw T_Fail.testFailMsg("delete found more than one key");
            }

            delete_scan.close();
        }

        ret_val = t_desc_scan_test_cases(tc, index_conglomid, template);


		// Close the conglomerate.
		index_cc.close();

        tc.commit();
        REPORT("Ending t_017");

        return(ret_val);
    }


	/*  test cases for ASC DESC ASC DESC column sort order index 
	SORTED DATA
	col1, col2, col3
	AS DS AS  -- sort order
	1, 1, 11
	3, 2, 12
	4, 6, 15
	4, 4, 14
	4, 2, 13
	5, 6, 18
	5, 4, 17
	5, 2, 16
	6, 1, 19
	7, 1, 20
	9, 1, 21
	*/

	private boolean t_ascdesc_scan_test_cases(
    TransactionController   tc,
    long                    index_conglomid,
    T_SecondaryIndexRow     template
    )
        throws StandardException, T_Fail
    {
        boolean     ret_val = true;

        // run through a predicates as described in the openScan() interface //
        DataValueDescriptor[] start_key  = TemplateRow.newU8Row(1);
        DataValueDescriptor[] stop_key   = TemplateRow.newU8Row(1);


        // test predicate x = 5
        //
        //     result set should be:{5,6,18}, {5,4,17}, {5,6,16}   
        //
        REPORT("scan (x = 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE, 
                   null,
                   stop_key,  ScanController.GT,
                   3, 18, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }
 

        REPORT("scan (x > 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GT, 
                   null,
                   null,  ScanController.NA,
                   3, 19, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        REPORT("scan (x >= 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE, 
                   null,
                   null,  ScanController.NA,
                   6, 16, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }

        REPORT("scan (x <= 5)");
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   null, ScanController.NA, 
                   null,
                   stop_key,  ScanController.GT,
                   8, 11, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }
  
        REPORT("scan (x < 5)");
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   null, ScanController.NA, 
                   null,
                   stop_key,  ScanController.GE,
                   5, 11, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }

        REPORT("scan (x >= 5 and x <= 7)");
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(7);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   5, 16, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }

        REPORT("scan (x = 5 and y > 2)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(2);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        ((SQLLongint)stop_key[1]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GE,
                   2, 18, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }


        REPORT("scan (x = 5 and y >= 2)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(2);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        ((SQLLongint)stop_key[1]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   3, 18, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }


        REPORT("scan (x = 5 and y < 5)");
        start_key  = TemplateRow.newU8Row(2);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)start_key[1]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   2, 17, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        REPORT("scan (x = 2)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(2);
        ((SQLLongint)stop_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   0, 0, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        //  +-----------------------------+---------------
        // 	|last leaf last row  on btree - row locked    |
        // 	+-----------------------------+---------------
        //
        REPORT("minimum on btree, row locked.");

        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found last row in the last leaf.");
        }
        else
        {
            // make sure right min was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 21)
            {
                throw T_Fail.testFailMsg("wrong last row in the last leaf.");
            }
        }

        //  +-----------------------------+--------------
        // 	|last row in the last leaf - table locked    |
        // 	+-----------------------------+--------------
        //
        REPORT("last row in the last leaf, table locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no min.");
        }
        else
        {
            // make sure right last row in the last leaf  found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 21)
            {
                throw T_Fail.testFailMsg("wrong last row in the last leaf found.");
            }
        }

        //  +-----------------------------+-----------
        // 	|last row in the last leaf- row locked    |
        // 	+-----------------------------+-----------
        //
        REPORT("last row in the last leaf, row locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_READ_COMMITTED,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no max.");
        }
        else
        {
            // make sure right last row in the last leaf found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 21)
            {
                throw T_Fail.testFailMsg("wrong last row in the last leaf found.");
            }
        }

        //  +-----------------------------+-------------
        // 	|last row in the last leaf- table locked    |
        // 	+-----------------------------+-------------
        //
        REPORT("last row in the last leaf, table locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_READ_COMMITTED,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no last row in the last leaf");
        }
        else
        {
            // make sure right min was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 21)
            {
                throw T_Fail.testFailMsg("wrong last row in the last leaf found.");
            }
        }

        return(ret_val);
    }


 /**
     * Test BTree.openScan(), BtreeScan.init(), BtreeScan.next(), 
     * BtreeScan.fetch() with alternating ascending and descending coulmn 
	 * sort order indexes.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_018(TransactionController tc)
        throws StandardException, T_Fail
    {
        T_SecondaryIndexRow index_row = new T_SecondaryIndexRow();

        // base row template - last column is just to make row long so that
        // multiple pages are spanned.
        DataValueDescriptor[] base_row             = TemplateRow.newU8Row(4);
        base_row[3] = new SQLChar();

        String   string_1500char = new String();
        for (int i = 0; i < 300; i++)
            string_1500char += "mikem";

        boolean     ret_val = true;
        long        value   = -1;
        long        col1[]  = { 1,  3,  4,  4,  4,  5,  5,  5,  6,  7,  9};
        long        col2[]  = { 1,  1,  2,  4,  6,  2,  4,  6,  1,  1,  1};
        long        col3[]  = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};

        // set of deleted rows to make scans more interesting
        long d_col1[] ={ 0,  2,  3,  4,  4,  5,  5,  5,  6,  7,  8, 10, 11, 12};
        long d_col2[] ={ 1,  1,  2,  3,  5,  0,  3,  5,  0,  0,  1, 42, 42, 1};
        long d_col3[] ={91, 92, 93, 94, 95, 96, 97, 98, 99,100,101,102,103,104};

		

        REPORT("Starting t_018");

        // create the base table
        long base_conglomid = 
            tc.createConglomerate(
                "heap",                            // create a heap conglomerate
                base_row,                          // base table template row
				null, //column sort order - not required for heap
                null, //default collation
                null,                              // default properties
                TransactionController.IS_DEFAULT); // not temporary

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // initialize the secondary index row - pointing it at base row
        index_row.init(base_row, base_cc.newRowLocationTemplate(), 5);

        Properties properties = 
            createProperties(
                null,               // no current properties list 
                false,              // don't allow duplicates
                5,                  // 4 columns in index row
                5,                  // non-unique index
                true,               // maintain parent links
                base_conglomid,     // base conglom id
                4);                 // row loc in last column

		// create the index with all the columns in descending order
        ColumnOrdering order[] = new ColumnOrdering[5];
		order[0] =  new T_ColumnOrderingImpl(0, true); // Ascending
		order[1] =  new T_ColumnOrderingImpl(1, false); // descending
		order[2] =  new T_ColumnOrderingImpl(2, true); // Ascending
		order[3] =  new T_ColumnOrderingImpl(3, false); // descending
		order[4] =  new T_ColumnOrderingImpl(4, true); // asccending

		long index_conglomid = 
            tc.createConglomerate(
                "BTREE",    				// create a btree secondary
                index_row.getRow(),         // row template
				order, //column sort order - default
                null, //default collation
                properties,                 // properties
                TransactionController.IS_DEFAULT);   // not temporary

		// Open the conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row.
        T_SecondaryIndexRow template = new T_SecondaryIndexRow();
        RowLocation         row_loc  = base_cc.newRowLocationTemplate();
        template.init(base_row, row_loc, 5);

        // insert them in reverse order just to make sure btree is sorting them
        for (int i = col1.length - 1; i >= 0; i--)
        {
            ((SQLLongint)(template.getRow()[0])).setValue(col1[i]);
            ((SQLLongint)(template.getRow()[1])).setValue(col2[i]);
            ((SQLLongint)(template.getRow()[2])).setValue(col3[i]);
            base_row[3] = new SQLChar(string_1500char);

            base_cc.insertAndFetchLocation(base_row, row_loc);

            // Insert the row.
            // System.out.println("Adding record (" + -(i - (col1.length -1)) +
            //                ")" + template);
            if (index_cc.insert(template.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        index_cc.checkConsistency();

        ((B2IController)index_cc).debugConglomerate();

        ret_val = t_ascdesc_scan_test_cases(tc, index_conglomid, template);

        // insert and delete some interesting rows, deleted space management
        // may or may not clean these up.
        for (int i = d_col1.length - 1; i >= 0; i--)
        {
            ((SQLLongint)(template.getRow()[0])).setValue(d_col1[i]);
            ((SQLLongint)(template.getRow()[1])).setValue(d_col2[i]);
            ((SQLLongint)(template.getRow()[2])).setValue(d_col3[i]);
            base_row[3] = new SQLChar(string_1500char);

            base_cc.insertAndFetchLocation(base_row, row_loc);

            // Insert the row.
            // System.out.println("Adding record (" + -(i - (col1.length -1)) +
            //                ")" + template);
            if (index_cc.insert(template.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");

            // now delete the row.
            base_cc.delete(row_loc);

            ScanController delete_scan = 
                tc.openScan(index_conglomid, false,
                            TransactionController.OPENMODE_FORUPDATE,
                            TransactionController.MODE_RECORD,
                            TransactionController.ISOLATION_SERIALIZABLE,
                            (FormatableBitSet) null,
                            template.getRow(), ScanController.GE, 
                            null,
                            template.getRow(), ScanController.GT); 

            if (!delete_scan.next())
            {
                throw T_Fail.testFailMsg("delete could not find key");
            }
            else
            {
                delete_scan.delete();

                if (delete_scan.next())
                    throw T_Fail.testFailMsg("delete found more than one key");
            }

            delete_scan.close();
        }

        ret_val = t_ascdesc_scan_test_cases(tc, index_conglomid, template);


		// Close the conglomerate.
		index_cc.close();

        tc.commit();
        REPORT("Ending t_018");

        return(ret_val);
    }



	/*  test cases for ASC DESC DESC ASC column sort order index 
	SORTED DATA 
	col1, col2, col3
	DS AS DS  -- sort order
	9, 1, 21
    7, 1, 20
	6, 1, 19
	5, 2, 16
	5, 4, 17
	5, 6, 18
	4, 2, 13
	4, 4, 14
	4, 6, 15
	3, 1, 12
	1, 1, 11
	*/
	private boolean t_ascdesc1_scan_test_cases(
    TransactionController   tc,
    long                    index_conglomid,
    T_SecondaryIndexRow     template
    )
        throws StandardException, T_Fail
    {
        boolean     ret_val = true;

        // run through a predicates as described in the openScan() interface //
        DataValueDescriptor[] start_key  = TemplateRow.newU8Row(1);
        DataValueDescriptor[] stop_key   = TemplateRow.newU8Row(1);


        REPORT("scan (x = 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE, 
                   null,
                   stop_key,  ScanController.GT,
                   3, 16, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }
 
        REPORT("scan (x > 5)");
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   null, ScanController.NA, 
                   null,
                   stop_key,  ScanController.GE,
                   3, 21, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        REPORT("scan (x >= 5)");
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   null, ScanController.NA, 
                   null,
                   stop_key,  ScanController.GT,
                   6, 16, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }

  
        REPORT("scan (x <= 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE, 
                   null,
                   null,  ScanController.NA,
                   8, 11, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }
  
        REPORT("scan (x < 5)");
        ((SQLLongint)start_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GT, 
                   null,
                   null,  ScanController.NA,
                   5, 11, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }

        REPORT("scan (x >= 5 and x <= 7)");
        ((SQLLongint)start_key[0]).setValue(7);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   5, 16, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }

        REPORT("scan (x = 5 and y > 2)");
        start_key  = TemplateRow.newU8Row(2);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)start_key[1]).setValue(2);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GT,
                   null,
                   stop_key,  ScanController.GT,
                   2, 17, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        REPORT("scan (x = 5 and y >= 2)");
        start_key  = TemplateRow.newU8Row(2);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)start_key[1]).setValue(2);
        ((SQLLongint)stop_key[0]).setValue(5);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   3, 16, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        REPORT("scan (x = 5 and y < 5)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(2);
        ((SQLLongint)start_key[0]).setValue(5);
        ((SQLLongint)stop_key[0]).setValue(5);
		((SQLLongint)stop_key[1]).setValue(5);

        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   2, 16, T_QualifierTest.ORDER_FORWARD))
        {
            ret_val = false;
        }

        REPORT("scan (x = 2)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(2);
        ((SQLLongint)stop_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   0, 0, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }

        // values '2' does not exist as such in the data set 
        REPORT("scan (x > 2)");
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)stop_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   null, ScanController.NA,
                   null,
                   stop_key,  ScanController.GE,
                   10, 12, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }



        // values '2' does not exist as such in the data set 
        REPORT("scan (x >= 2)");
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)stop_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   null, ScanController.NA,
                   null,
                   stop_key,  ScanController.GT,
                   10, 12, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }


        // values '2' does not exist as such in the data set 
        REPORT("scan (x < 2)");
        start_key  = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GT,
                   null,
                   null,  ScanController.NA,
                   1, 11, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }


        // values '2' does not exist as such in the data set 
        REPORT("scan (x <= 2)");
        start_key  = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   null,  ScanController.NA,
                   1, 11, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }


        REPORT("scan (x >= 2 and x <= 7)");
        ((SQLLongint)start_key[0]).setValue(7);
        ((SQLLongint)stop_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   9, 12, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }

        REPORT("scan (x = 2 and y > 2)");
        start_key  = TemplateRow.newU8Row(2);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(2);
        ((SQLLongint)start_key[1]).setValue(2);
        ((SQLLongint)stop_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GT,
                   null,
                   stop_key,  ScanController.GT,
                   0, 0, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }

        REPORT("scan (x = 2 and y >= 2)");
        start_key  = TemplateRow.newU8Row(2);
        stop_key   = TemplateRow.newU8Row(1);
        ((SQLLongint)start_key[0]).setValue(2);
        ((SQLLongint)start_key[1]).setValue(2);
        ((SQLLongint)stop_key[0]).setValue(2);
        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   0, 0, T_QualifierTest.ORDER_NONE))
        {
            ret_val = false;
        }

        REPORT("scan (x = 4 and y <=2)");
        start_key  = TemplateRow.newU8Row(1);
        stop_key   = TemplateRow.newU8Row(2);
        ((SQLLongint)start_key[0]).setValue(4);
        ((SQLLongint)stop_key[0]).setValue(4);
		((SQLLongint)stop_key[1]).setValue(2);

        if (!T_QualifierTest.t_scan(tc, index_conglomid, template.getRow(),
                   template.getRow(),
                   start_key, ScanController.GE,
                   null,
                   stop_key,  ScanController.GT,
                   1, 13, T_QualifierTest.ORDER_DESC))
        {
            ret_val = false;
        }




        //  +-----------------------------+---------------
        // 	|last leaf last row  on btree - row locked    |
        // 	+-----------------------------+---------------
        //
        REPORT("last row in the last leaf, row locked.");

        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found last row in the last leaf.");
        }
        else
        {
            // make sure right min was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 11)
            {
                throw T_Fail.testFailMsg("wrong last row in the last leaf.");
            }
        }

        //  +-----------------------------+--------------
        // 	|last row in the last leaf - table locked    |
        // 	+-----------------------------+--------------
        //
        REPORT("last row in the last leaf, table locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no min.");
        }
        else
        {
            // make sure right last row in the last leaf  found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 11)
            {
                throw T_Fail.testFailMsg("wrong last row in the last leaf found.");
            }
        }

        //  +-----------------------------+-----------
        // 	|last row in the last leaf- row locked    |
        // 	+-----------------------------+-----------
        //
        REPORT("last row in the last leaf, row locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_READ_COMMITTED,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no max.");
        }
        else
        {
            // make sure right last row in the last leaf found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 11)
            {
                throw T_Fail.testFailMsg("wrong last row in the last leaf found.");
            }
        }

        //  +-----------------------------+-------------
        // 	|last row in the last leaf- table locked    |
        // 	+-----------------------------+-------------
        //
        REPORT("last row in the last leaf, table locked.");
        if (!tc.fetchMaxOnBtree(
                index_conglomid,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_READ_COMMITTED,
                (FormatableBitSet) null,
                template.getRow()))
        {
            throw T_Fail.testFailMsg("found no last row in the last leaf");
        }
        else
        {
            // make sure right min was found.
            long key = ((SQLLongint) template.getRow()[2]).getLong();
            
            if (key != 11)
            {
                throw T_Fail.testFailMsg("wrong last row in the last leaf found.");
            }
        }

        return(ret_val);
    }


 /**
     * Test BTree.openScan(), BtreeScan.init(), BtreeScan.next(), 
     * BtreeScan.fetch() with alternating ascending and descending coulmn 
	 * sort order indexes.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     */
    protected boolean t_019(TransactionController tc)
        throws StandardException, T_Fail
    {
        T_SecondaryIndexRow index_row = new T_SecondaryIndexRow();

        // base row template - last column is just to make row long so that
        // multiple pages are spanned.
        DataValueDescriptor[] base_row             = TemplateRow.newU8Row(4);
        base_row[3] = new SQLChar();

        String   string_1500char = new String();
        for (int i = 0; i < 300; i++)
            string_1500char += "mikem";

        boolean     ret_val = true;
        long        value   = -1;
        long        col1[]  = { 1,  3,  4,  4,  4,  5,  5,  5,  6,  7,  9};
        long        col2[]  = { 1,  1,  2,  4,  6,  2,  4,  6,  1,  1,  1};
        long        col3[]  = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};

        // set of deleted rows to make scans more interesting
        long d_col1[] ={ 0,  2,  3,  4,  4,  5,  5,  5,  6,  7,  8, 10, 11, 12};
        long d_col2[] ={ 1,  1,  2,  3,  5,  0,  3,  5,  0,  0,  1, 42, 42, 1};
        long d_col3[] ={91, 92, 93, 94, 95, 96, 97, 98, 99,100,101,102,103,104};

		

        REPORT("Starting t_019");

        // create the base table
        long base_conglomid = 
            tc.createConglomerate(
                "heap",                            // create a heap conglomerate
                base_row,                          // base table template row
				null, //column sort order - not required for heap
                null, //default collation
                null,                              // default properties
                TransactionController.IS_DEFAULT); // not temporary

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // initialize the secondary index row - pointing it at base row
        index_row.init(base_row, base_cc.newRowLocationTemplate(), 5);

        Properties properties = 
            createProperties(
                null,               // no current properties list 
                false,              // don't allow duplicates
                5,                  // 4 columns in index row
                5,                  // non-unique index
                true,               // maintain parent links
                base_conglomid,     // base conglom id
                4);                 // row loc in last column

		// create the index with all the columns in descending order
        ColumnOrdering order[] = new ColumnOrdering[5];
		order[0] =  new T_ColumnOrderingImpl(0, false); // Descending
		order[1] =  new T_ColumnOrderingImpl(1, true); // Ascending
		order[2] =  new T_ColumnOrderingImpl(2, true); // Ascending
		order[3] =  new T_ColumnOrderingImpl(3, false); // descending
		order[4] =  new T_ColumnOrderingImpl(4, true); // asccending

		long index_conglomid = 
            tc.createConglomerate(
                "BTREE",    				// create a btree secondary
                index_row.getRow(),         // row template
				order, //column sort order - default
                null, //default collation
                properties,                 // properties
                TransactionController.IS_DEFAULT);   // not temporary

		// Open the conglomerate.
		ConglomerateController index_cc =	
            tc.openConglomerate(
                index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Create a row.
        T_SecondaryIndexRow template = new T_SecondaryIndexRow();
        RowLocation         row_loc  = base_cc.newRowLocationTemplate();
        template.init(base_row, row_loc, 5);

        // insert them in reverse order just to make sure btree is sorting them
        for (int i = col1.length - 1; i >= 0; i--)
        {
            ((SQLLongint)(template.getRow()[0])).setValue(col1[i]);
            ((SQLLongint)(template.getRow()[1])).setValue(col2[i]);
            ((SQLLongint)(template.getRow()[2])).setValue(col3[i]);
            base_row[3] = new SQLChar(string_1500char);

            base_cc.insertAndFetchLocation(base_row, row_loc);

            // Insert the row.
            // System.out.println("Adding record (" + -(i - (col1.length -1)) +
            //                ")" + template);
            if (index_cc.insert(template.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");
        }

        index_cc.checkConsistency();

        ((B2IController)index_cc).debugConglomerate();

        ret_val = t_ascdesc1_scan_test_cases(tc, index_conglomid, template);

        // insert and delete some interesting rows, deleted space management
        // may or may not clean these up.
        for (int i = d_col1.length - 1; i >= 0; i--)
        {
            ((SQLLongint)(template.getRow()[0])).setValue(d_col1[i]);
            ((SQLLongint)(template.getRow()[1])).setValue(d_col2[i]);
            ((SQLLongint)(template.getRow()[2])).setValue(d_col3[i]);
            base_row[3] = new SQLChar(string_1500char);

            base_cc.insertAndFetchLocation(base_row, row_loc);

            // Insert the row.
            // System.out.println("Adding record (" + -(i - (col1.length -1)) +
            //                ")" + template);
            if (index_cc.insert(template.getRow()) != 0)
                throw T_Fail.testFailMsg("insert failed");

            // now delete the row.
            base_cc.delete(row_loc);

            ScanController delete_scan = 
                tc.openScan(index_conglomid, false,
                            TransactionController.OPENMODE_FORUPDATE,
                            TransactionController.MODE_RECORD,
                            TransactionController.ISOLATION_SERIALIZABLE,
                            (FormatableBitSet) null,
                            template.getRow(), ScanController.GE, 
                            null,
                            template.getRow(), ScanController.GT); 

            if (!delete_scan.next())
            {
                throw T_Fail.testFailMsg("delete could not find key");
            }
            else
            {
                delete_scan.delete();

                if (delete_scan.next())
                    throw T_Fail.testFailMsg("delete found more than one key");
            }

            delete_scan.close();
        }

        ret_val = t_ascdesc1_scan_test_cases(tc, index_conglomid, template);


		// Close the conglomerate.
		index_cc.close();

        tc.commit();
        REPORT("Ending t_019");

        return(ret_val);
    }

    /**
     * Test read uncommitted cases on scan.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
	 * @exception  T_Fail  Throws T_Fail on any test failure.
     **/
    protected boolean t_020(TransactionController tc)
        throws StandardException, T_Fail
    {
        ScanController scan         = null;

        REPORT("Starting t_020");

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        // Create the btree so that it only allows 2 rows per page.
        createCongloms(tc, 2, false, false, 2, create_ret);

        // Open the base table
        ConglomerateController base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid, 
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Open the secondary index
		ConglomerateController index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		// objects used to insert rows into base and index tables.
		DataValueDescriptor[] r1            = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow   index_row1    = new T_SecondaryIndexRow();
        RowLocation           base_rowloc1  = base_cc.newRowLocationTemplate();

        index_row1.init(r1, base_rowloc1, 3);

        // insert one row into the table/index

        // Open the base table
        base_cc = 
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // Open the secondary index
        index_cc =	
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // insert one row that does not cause failure.
        ((SQLLongint)r1[0]).setValue(2);
        ((SQLLongint)r1[1]).setValue(10000);

        // Insert the row into the base table;remember its location.
        base_cc.insertAndFetchLocation(r1, base_rowloc1);

        // Insert the row into the secondary index.
        if (index_cc.insert(index_row1.getRow()) != 0)
            throw T_Fail.testFailMsg("insert failed");

        // Commit the create of the tables so that the following aborts don't
        // undo that work.
        tc.commit();


        // TEST 1 - position a read uncommitted scan on a row which is
        // purged by a split trying to get space on the page, and then see
        // what happens when the scan trys to continue.  This can only happen
        // currently if the same transaction deletes the row and while having
        // the scan open also does inserts onto the same page.  Otherwise the
        // btree scan will maintain a "page scan locks" which will prevent
        // the row from being purged out from underneath it.


        tc.commit();
        REPORT("Ending t_020");

        return true;
    }

    /**
     * Test latch release at critical time during delete on an index scan that
     * uses update locks.
     */
    protected boolean t_021(TransactionController tc)
        throws StandardException, T_Fail
    {
        REPORT("Starting t_021");

        boolean ret_val = true;

        T_CreateConglomRet create_ret = new T_CreateConglomRet();

        // Create the btree so that it only allows 2 rows per page.
        createCongloms(tc, 2, false, false, 2, create_ret);

        // Open the base table
        ConglomerateController base_cc =
            tc.openConglomerate(
                create_ret.base_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // Open the secondary index
        ConglomerateController index_cc =
            tc.openConglomerate(
                create_ret.index_conglomid,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

        // objects used to insert rows into base and index tables.
        DataValueDescriptor[] r1            = TemplateRow.newU8Row(2);
        T_SecondaryIndexRow   index_row1    = new T_SecondaryIndexRow();
        RowLocation           base_rowloc1  = base_cc.newRowLocationTemplate();

        index_row1.init(r1, base_rowloc1, 3);

        // insert one row into the table/index
        ((SQLLongint)r1[0]).setValue(1);
        ((SQLLongint)r1[1]).setValue(1);

        // Insert the row into the base table;remember its location.
        base_cc.insertAndFetchLocation(r1, base_rowloc1);

        // Insert the row into the secondary index.
        if (index_cc.insert(index_row1.getRow()) != 0)
            throw T_Fail.testFailMsg("insert failed");

        // Commit the create of the tables.
        tc.commit();

        // Enable the debug code that releases the latch at critical time.
        if (SanityManager.DEBUG) {
            SanityManager.DEBUG_SET("BTreeScan_delete_useUpdateLocks1");
        }

        // Delete the row using the index and update locks. Before DERBY-4083,
        // the call to delete() would fail with record not found if the latch
        // was released.
        DataValueDescriptor[] delete_key = TemplateRow.newU8Row(2);
        ((SQLLongint)delete_key[0]).setValue(1);
        ((SQLLongint)delete_key[1]).setValue(1);
        if (!t_delete(tc, create_ret.index_conglomid, delete_key, true)) {
            ret_val = false;
        }

        // Disable the debug code that releases the latch at critical time.
        if (SanityManager.DEBUG) {
            SanityManager.DEBUG_CLEAR("BTreeScan_delete_useUpdateLocks1");
        }

        tc.commit();
        REPORT("Ending t_021");

        return ret_val;
    }

	public static String repeatString(String data, int repeat) {

		String s = data;
		for (int i = 1; i < repeat; i++)
			s += data;

		return s;
	}
    
    /** Check wheather the database is active or not
     * @return {@code true} if the database is active, {@code false} otherwise
     */
    public boolean isdbActive() {
        LanguageConnectionContext lcc = (LanguageConnectionContext) ContextService
                .getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
        Database db = (Database) (lcc != null ? lcc.getDatabase() : null);
        return (db != null ? db.isActive() : false);
    }

}

class T_CreateConglomRet 
{
    public long                     base_conglomid;
    public long                     index_conglomid;
    // public DataValueDescriptor[]	base_template_row;
    public DataValueDescriptor[]	index_template_row;
}
