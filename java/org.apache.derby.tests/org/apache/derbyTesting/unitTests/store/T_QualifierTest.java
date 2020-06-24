/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_QualifierTest

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

import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.impl.store.access.conglomerate.*;

import java.util.Properties;
import java.util.HashSet;
import java.util.List;

import java.io.PrintWriter;
import org.apache.derby.shared.common.stream.HeaderPrintWriter;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.types.SQLLongint;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import java.util.Enumeration;

public class T_QualifierTest 
{
    private String              init_conglomerate_type;
    private Properties          init_properties;
    private boolean             init_temporary;
    private HeaderPrintWriter   init_out;
    private int                 init_order = ORDER_NONE;

    public static final int ORDER_FORWARD  = 1;
    public static final int ORDER_BACKWARD = 2;
    public static final int ORDER_NONE     = 3;
	public static final int ORDER_DESC = 4;  // ordered in descending order

    /* Constructor */
    public T_QualifierTest(
    String              conglomerate_type,
    Properties          properties,
    boolean             temporary,
    HeaderPrintWriter   out,
    int                 order)
    {
        this.init_conglomerate_type = conglomerate_type;
        this.init_properties        = properties;
        this.init_temporary         = temporary;
        this.init_out               = out;
        this.init_order             = order;

        return;
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */
    private static HashSet<Long> create_hash_set(
    int     expect_key,
    int     expect_numrows,
	int 	order)
    {
        HashSet<Long> set = new HashSet<Long>(10, 0.8f);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

        int key_val = expect_key;
        for (int i = 0; i < expect_numrows; i++)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
          set.add((long)key_val);
			if (order == ORDER_DESC)
				key_val--;
			else
				key_val++;
        }

        return(set);
    }
    private static int flip_scan_op(int op)
    {
        int ret_op = -42;

        if (op == ScanController.GE)
            ret_op = ScanController.GT;
        else if (op == ScanController.GT)
            ret_op = ScanController.GE;

        return(ret_op);
    }

    /**
     * Test a single scan.
     */
    public static boolean t_scan(
    TransactionController   tc,
    long                    conglomid,
    DataValueDescriptor[]	init_scan_template,
    DataValueDescriptor[]	fetch_template,
    DataValueDescriptor[]	start_key, 
    int                     start_op,
    Qualifier               qualifier[][],
    DataValueDescriptor[]	stop_key,
    int                     stop_op,
    int                     expect_numrows,
    int                     lowest_expect_key,
    int                     order)
        throws StandardException, T_Fail
    {
        boolean ret_val;

        // call scan which does next(), fetch(row)
        ret_val = t_scanNext(tc, conglomid, 
                fetch_template, 
                start_key, start_op, qualifier, stop_key, stop_op, 
                expect_numrows, lowest_expect_key, order);

        if (!ret_val)
            return(ret_val);

        // call scan which does fetchNext(row), fetch(row)
        ret_val = t_scanFetchNext(tc, conglomid,  init_scan_template,
                fetch_template, 
                start_key, start_op, qualifier, stop_key, stop_op, 
                expect_numrows, lowest_expect_key, order);

        if (!ret_val)
            return(ret_val);

        // call scan which does fetchNext(partial_row), fetch(partial_row)

        ret_val = t_scanFetchNextPartial(tc, conglomid, 
                fetch_template, 
                start_key, start_op, qualifier, stop_key, stop_op, 
                expect_numrows, lowest_expect_key, order);

        if (!ret_val)
            return(ret_val);

        // call scan which does createBackingStoreHashtable()
        ret_val = t_scanFetchHashtable(tc, conglomid,
                fetch_template, 
                start_key, start_op, qualifier, stop_key, stop_op, 
                expect_numrows, lowest_expect_key, order);

        if (!ret_val)
            return(ret_val);

        // call scan which does fetchNextGroup() - fetching 1 at a time.
        // this tests the edge case.
        ret_val = t_scanFetchNextGroup(tc, 1, conglomid, 
                fetch_template, 
                start_key, start_op, qualifier, stop_key, stop_op, 
                expect_numrows, lowest_expect_key, order);

        if (!ret_val)
            return(ret_val);

        // call scan which does fetchNextGroup() - fetching 2 at a time.
        // this tests the general case of fetching N rows which is usually
        // less than the remaining rows in the result set.
        ret_val = t_scanFetchNextGroup(tc, 2, conglomid, 
                fetch_template, 
                start_key, start_op, qualifier, stop_key, stop_op, 
                expect_numrows, lowest_expect_key, order);

        if (!ret_val)
            return(ret_val);

        // call scan which does fetchNextGroup() - fetching 1000 at a time.
        // this will get the entire result set in one fetch.
        ret_val = t_scanFetchNextGroup(tc, 1000, conglomid,
                fetch_template, 
                start_key, start_op, qualifier, stop_key, stop_op, 
                expect_numrows, lowest_expect_key, order);

        if (!ret_val)
            return(ret_val);

        return(ret_val);

    }
    private static boolean t_scanNext(
    TransactionController   tc,
    long                    conglomid,
    DataValueDescriptor[]	fetch_template,
    DataValueDescriptor[]	start_key, 
    int                     start_op,
    Qualifier               qualifier[][],
    DataValueDescriptor[]	stop_key,
    int                     stop_op,
    int                     expect_numrows,
    int                     input_expect_key,
    int                     order)
        throws StandardException, T_Fail
    {
        HashSet set         = null;
        boolean ordered     = (order == T_QualifierTest.ORDER_FORWARD || 
							   order == T_QualifierTest.ORDER_DESC);
        int     expect_key  = input_expect_key;

        if (!ordered)
        {
            set = create_hash_set(input_expect_key, expect_numrows, order);
        }

        /**********************************************************************
         * Forward scan test case
         **********************************************************************
         */

        ScanController scan = 
            tc.openScan(
                conglomid, false, 
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                start_key, start_op,
                qualifier,
                stop_key, stop_op);

        long key     = -42;
        int numrows = 0;

        while (scan.next())
        {
            scan.fetch(fetch_template);

            key = ((SQLLongint)(fetch_template[2])).getLong();

            if (ordered)
            {
                if (key != expect_key)
                {
                    return(
                        fail("(t_scanNext) wrong key, expected (" + 
                              expect_key + ")" + "but got (" + key + ")."));
                }
                else
                {
					if (order == ORDER_DESC)
						expect_key--;
					else
						expect_key++;
                }
            }
            else
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                if (!set.remove(key))
                {
                    return(
                        fail("(t_scanNext) wrong key, expected (" + 
                              expect_key + ")" + "but got (" + key + ")."));
                }
            }
            numrows++;

        }

        scan.close();

        if (numrows != expect_numrows)
        {
            return(fail("(t_scanNext) wrong number of rows. Expected " +
                 expect_numrows + " rows, but got " + numrows + "rows."));
        }

        /**********************************************************************
         * Backward scan test case
         **********************************************************************
         */
        /*

        if (!ordered)
        {
            set = create_hash_set(input_expect_key, expect_numrows, order);
        }

        // flip start and stop keys for backward scan and also flip 
        // start and stop operators (ie. GE->GT and GT->GE).
        scan = 
            tc.openBackwardScan(
                conglomid, false, 
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                stop_key, flip_scan_op(stop_op),
                qualifier,
                start_key, flip_scan_op(start_op));

        key     = -42;
        numrows = 0;

        // rows are going to come back in reverse order in the ordered case.
        expect_key = input_expect_key + expect_numrows - 1;

        while (scan.next())
        {
            scan.fetch(fetch_template);

            key = ((SQLLongint)(fetch_template[2])).getLong();

            if (ordered)
            {
                if (key != expect_key)
                {
                    return(
                        fail("(t_scanNext-backward) wrong key, expected (" + 
                              expect_key + ")" + "but got (" + key + ")."));
                }
                else
                {
                    expect_key--;
                }
            }
            else
            {
                if (!set.remove(new Long(key)))
                {
                    return(
                        fail("(t_scanNext-backward) wrong key, expected (" + 
                              expect_key + ")" + "but got (" + key + ")."));
                }
            }
            numrows++;
        }

        if (numrows != expect_numrows)
        {
            return(fail("(t_scanNext-backward) wrong num of rows. Expected " +
                 expect_numrows + " rows, but got " + numrows + "rows."));
        }

        scan.close();
        */

        return(true);
    }

    /**
     * Test scan which does FetchNext with all of the fields.
     * <p>
     * FetchNext() may be optimized by the underlying scan code to try and
     * not do multiple fetches of the same row for the user, but if the user
     * asks for one column, but the stop position depends on the whole row
     * this optimization is not possible.
     * <p>
     *
	 * @return Whether the test succeeded or not.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static boolean t_scanFetchNext(
    TransactionController   tc,
    long                    conglomid,
    DataValueDescriptor[]	init_scan_template,
    DataValueDescriptor[]	fetch_template,
    DataValueDescriptor[]	start_key, 
    int                     start_op,
    Qualifier               qualifier[][],
    DataValueDescriptor[]	stop_key,
    int                     stop_op,
    int                     expect_numrows,
    int                     input_expect_key,
    int                     order)
        throws StandardException, T_Fail
    {
        HashSet set         = null;
        boolean ordered     = (order == ORDER_FORWARD || order == ORDER_DESC);

        if (!ordered)
        {
            set = create_hash_set(input_expect_key, expect_numrows, order);
        }

        /**********************************************************************
         * Forward scan test case
         **********************************************************************
         */


        ScanController scan = 
            tc.openScan(
                conglomid, false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                start_key, start_op,
                qualifier,
                stop_key, stop_op);

        int  expect_key = input_expect_key;
        long key        = -42;
        long numrows    = 0;

        while (scan.fetchNext(fetch_template))
        {
            scan.fetch(init_scan_template);

            // make sure all columns from fetchNext() match subsequent fetch().
            for (int i = 0; i < init_scan_template.length; i++)
            {
                if ((fetch_template[i]).compare(
                        (init_scan_template[i])) != 0)
                {
                    return(
                        fail("(t_scanFetchNext) wrong key, expected (" + 
                              fetch_template[i] + ")" + "but got (" + 
                              init_scan_template[i] + ")."));
                }
            }

            // see if we are getting the right keys.
            key = ((SQLLongint)(init_scan_template[2])).getLong();

            if (ordered)
            {
                if (key != expect_key)
                {
                    return(
                        fail("(t_scanFetchNext) wrong key, expected (" + 
                             expect_key + ")" + "but got (" + key + ")."));
                }
                else
                {
					if (order == ORDER_DESC)
						expect_key--;
					else
						expect_key++;
                }
            }
            else
            {
                if (!set.remove(key))
                {
                    return(
                        fail("(t_scanFetchNext) wrong key, expected (" + 
                              expect_key + ")" + "but got (" + key + ")."));
                }
            }
            numrows++;

        }

        scan.close();

        if (numrows != expect_numrows)
        {
            return(fail("(t_scanFetchNext) wrong number of rows. Expected " +
                 expect_numrows + " rows, but got " + numrows + "rows."));
        }


        /**********************************************************************
         * Backward scan test case
         **********************************************************************
         */

        /*
        if (!ordered)
        {
            set = create_hash_set(input_expect_key, expect_numrows, order);
        }

        scan = 
            tc.openBackwardScan(
                conglomid, false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                stop_key, flip_scan_op(stop_op),
                qualifier,
                start_key, flip_scan_op(start_op));

        expect_key = input_expect_key + expect_numrows - 1;
        key        = -42;
        numrows    = 0;

        while (scan.fetchNext(fetch_template))
        {
            scan.fetch(init_scan_template);

            // make sure all columns from fetchNext() match subsequent fetch().
            for (int i = 0; i < init_scan_template.length; i++)
            {
                if (((Orderable)fetch_template[i]).compare(
                        ((Orderable)init_scan_template[i])) != 0)
                {
                    return(
                        fail("(t_scanFetchNext) wrong key, expected (" + 
                              fetch_template[i] + ")" + "but got (" + 
                              init_scan_template[i] + ")."));
                }
            }

            // see if we are getting the right keys.
            key = ((SQLLongint)(init_scan_template[2])).getLong();

            if (ordered)
            {
                if (key != expect_key)
                {
                    return(
                        fail("(t_scanFetchNext) wrong key, expected (" + 
                             expect_key + ")" + "but got (" + key + ")."));
                }
                else
                {
                    expect_key--;
                }
            }
            else
            {
                if (!set.remove(new Long(key)))
                {
                    return(
                        fail("(t_scanFetchNext) wrong key, expected (" + 
                              expect_key + ")" + "but got (" + key + ")."));
                }
            }
            numrows++;

        }

        scan.close();

        if (numrows != expect_numrows)
        {
            return(fail("(t_scanFetchNext) wrong number of rows. Expected " +
                 expect_numrows + " rows, but got " + numrows + "rows."));
        }
        */

        return(true);
    }

    /**
     * Test scan which does FetchNextGroup with all of the fields.
     * <p>
     *
	 * @return Whether the test succeeded or not.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static boolean t_scanFetchNextGroup(
    TransactionController   tc,
    int                     group_size,
    long                    conglomid,
    DataValueDescriptor[]	fetch_template,
    DataValueDescriptor[]	start_key, 
    int                     start_op,
    Qualifier               qualifier[][],
    DataValueDescriptor[]	stop_key,
    int                     stop_op,
    int                     expect_numrows,
    int                     input_expect_key,
    int                     order)
        throws StandardException, T_Fail
    {
        HashSet set = null;
        boolean ordered = (order == ORDER_FORWARD || order == ORDER_DESC);

        if (!ordered)
        {
            set = create_hash_set(input_expect_key, expect_numrows, order);
        }

        /**********************************************************************
         * Forward scan test case
         **********************************************************************
         */

        GroupFetchScanController scan = 
            tc.openGroupFetchScan(
                conglomid, false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                start_key, start_op,
                qualifier,
                stop_key, stop_op);

        // create an array of "group_size" rows to use in the fetch group call.
        DataValueDescriptor[][] row_array = 
            new DataValueDescriptor[group_size][];
        row_array[0] = TemplateRow.newRow(fetch_template);

        int  expect_key         = input_expect_key;
        long key                = -42;
        long numrows            = 0;
        int  group_row_count    = 0;

        // loop asking for "group_size" rows at a time.
        while ((group_row_count = 
                    scan.fetchNextGroup(row_array, (RowLocation[]) null)) != 0)
        {

            // loop through the rows returned into the row_array.
            for (int i = 0; i < group_row_count; i++)
            {
                // see if we are getting the right keys.
                key = ((SQLLongint)(row_array[i][2])).getLong();

                if (ordered)
                {
                    if (key != expect_key)
                    {
                        return(fail(
                            "(t_scanFetchNextGroup-forward) wrong key, expect (" + 
                            expect_key + ")" + "but got (" + key + 
                            "). num rows = " + numrows));
                    }
                    else
                    {
						if (order == ORDER_DESC)
							expect_key--;
						else
							expect_key++;
                    }
                }
                else
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                    if (!set.remove(key))
                    {
                        return(fail(
                            "(t_scanFetchNextGroup-forward) wrong key, expected (" + 
                            expect_key + ")" + "but got (" + key + ")."));
                    }
                }
                numrows++;
            }
        }

        scan.close();

        if (numrows != expect_numrows)
        {
            return(
                fail("(t_scanFetchNextGroup-forward) wrong number of rows. Expected " +
                expect_numrows + " rows, but got " + numrows + "rows."));
        }

        /**********************************************************************
         * Backward scan test case
         **********************************************************************
         */

        /*
        if (!ordered)
        {
            set = create_hash_set(input_expect_key, expect_numrows, order);
        }

        scan = 
            tc.openGroupFetchBackwardScan(
                conglomid, false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                stop_key, flip_scan_op(stop_op),
                qualifier,
                start_key, flip_scan_op(start_op));

        // create an array of "group_size" rows to use in the fetch group call.
        expect_key      = input_expect_key + expect_numrows - 1;
        key             = -42;
        numrows         = 0;
        group_row_count = 0;

        // loop asking for "group_size" rows at a time.
        while ((group_row_count = 
                    scan.fetchNextGroup(row_array, (RowLocation[]) null)) != 0)
        {
            // loop through the rows returned into the row_array.
            for (int i = 0; i < group_row_count; i++)
            {
                // see if we are getting the right keys.
                key = ((SQLLongint)(row_array[i][2])).getLong();

                if (ordered)
                {
                    if (key != expect_key)
                    {
                        return(fail(
                            "(t_scanFetchNextGroup-backward) wrong key, expected (" + 
                            expect_key + ")" + "but got (" + key + ")."));
                    }
                    else
                    {
                        expect_key--;
                    }
                }
                else
                {
                    if (!set.remove(new Long(key)))
                    {
                        return(fail(
                            "(t_scanFetchNextGroup-backward) wrong key, expected (" + 
                            expect_key + ")" + "but got (" + key + ")."));
                    }
                }
                numrows++;
            }
        }

        scan.close();

        if (numrows != expect_numrows)
        {
            return(
                fail("(t_scanFetchNextGroup-backward) wrong number of rows. Expected " +
                expect_numrows + " rows, but got " + numrows + "rows."));
        }

        */

        return(true);
    }

    /**
     * Test scan which does FetchNext with subset of fields.
     * <p>
     * FetchNext() may be optimized by the underlying scan code to try and
     * not do multiple fetches of the same row for the user, but if the user
     * asks for one column, but the stop position depends on the whole row
     * this optimization is not possible.
     * <p>
     *
	 * @return Whether the test succeeded or not.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static boolean t_scanFetchNextPartial(
    TransactionController   tc,
    long                    conglomid,
    DataValueDescriptor[]	fetch_template,
    DataValueDescriptor[]	start_key, 
    int                     start_op,
    Qualifier               qualifier[][],
    DataValueDescriptor[]	stop_key,
    int                     stop_op,
    int                     expect_numrows,
    int                     input_expect_key,
    int                     order)
        throws StandardException, T_Fail
    {
        HashSet set = null;
        boolean ordered = (order == ORDER_FORWARD || order == ORDER_DESC);


        /**********************************************************************
         * setup shared by both.
         **********************************************************************
         */

        // In the fetchNext call only ask the minimum set of columns 
        // necessary, which is the union of the "key" (col[2]) and other
        // columns referenced in the qualifier list.
        FormatableBitSet fetch_row_validColumns = RowUtil.getQualifierBitSet(qualifier);

        // now add in column 2, as we always need the key field.
		fetch_row_validColumns.grow(3);// grow to length of 3 
        fetch_row_validColumns.set(2);

        // add in any fields in start and stop positions
        if (start_key != null)
        {
            for (int i = 0; i < start_key.length; i++)
            {
                fetch_row_validColumns.set(i);
            }
        }
        if (stop_key != null)
        {
            for (int i = 0; i < stop_key.length; i++)
            {
                fetch_row_validColumns.set(i);
            }
        }

        // point key at the right column in the fetch_template
        SQLLongint key_column = (SQLLongint) fetch_template[2];

        /**********************************************************************
         * Forward scan test case
         **********************************************************************
         */

        if (!ordered)
        {
            set = create_hash_set(input_expect_key, expect_numrows, order);
        }

        ScanController scan = 
            tc.openScan(
                conglomid, false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) fetch_row_validColumns, 
                start_key, start_op,
                qualifier,
                stop_key, stop_op);

        int  expect_key = input_expect_key;
        long key        = -42;
        long key2       = -42;
        long numrows    = 0;

        while (scan.fetchNext(fetch_template))
        {
            // see if we are getting the right keys.
            key = key_column.getLong();

            // make sure a subsequent fetch also works.
            key_column.setValue(-42);

            scan.fetch(fetch_template);
            key2 = key_column.getLong();

            if (ordered)
            {
                if ((key != expect_key) || (key2 != expect_key))
                {
                    return(
                        fail("(t_scanFetchNext) wrong key, expected (" + 
                             expect_key + ")" + "but got (" + key + ")."));
                }
                else
                {
					if (order == ORDER_DESC)
						expect_key--;
					else
						expect_key++;
                }
            }
            else
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                if (!set.remove(key))
                {
                    return(
                        fail("(t_scanFetchNext) wrong key, expected (" + 
                             expect_key + ")" + "but got (" + key + ")."));
                }
            }
            numrows++;

        }

        scan.close();

        if (numrows != expect_numrows)
        {
            return(fail("(t_scanFetchNext) wrong number of rows. Expected " +
                 expect_numrows + " rows, but got " + numrows + "rows."));
        }

        /**********************************************************************
         * Backward scan test case
         **********************************************************************
         */

        /*
        if (!ordered)
        {
            set = create_hash_set(expect_key, expect_numrows, order);
        }

        scan = 
            tc.openBackwardScan(
                conglomid, false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) fetch_row_validColumns, 
                stop_key, flip_scan_op(stop_op),
                qualifier,
                start_key, flip_scan_op(start_op));

        expect_key  = input_expect_key + expect_numrows - 1;
        key         = -42;
        key2        = -42;
        numrows     = 0;

        while (scan.fetchNext(fetch_template))
        {
            // see if we are getting the right keys.
            key = key_column.getValue();

            // make sure a subsequent fetch also works.
            key_column.setValue(-42);

            scan.fetch(fetch_template);
            key2 = key_column.getValue();

            if (ordered)
            {
                if ((key != expect_key) || (key2 != expect_key))
                {
                    return(
                        fail("(t_scanFetchNext) wrong key, expected (" + 
                             expect_key + ")" + "but got (" + key + ")."));
                }
                else
                {
                    expect_key--;
                }
            }
            else
            {
                if (!set.remove(new Long(key)))
                {
                    return(
                        fail("(t_scanFetchNext) wrong key, expected (" + 
                             expect_key + ")" + "but got (" + key + ")."));
                }
            }
            numrows++;

        }

        scan.close();

        if (numrows != expect_numrows)
        {
            return(fail("(t_scanFetchNext) wrong number of rows. Expected " +
                 expect_numrows + " rows, but got " + numrows + "rows."));
        }
        */

        return(true);
    }

    /**
     * Test scan which does FetchSet.
     * <p>
     * FetchSet() returns the entire result set in the hash table.
     * <p>
     *
	 * @return Whether the test succeeded or not.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static boolean t_scanFetchHashtable(
    TransactionController   tc,
    long                    conglomid,
    DataValueDescriptor[]	fetch_template,
    DataValueDescriptor[]	start_key, 
    int                     start_op,
    Qualifier               qualifier[][],
    DataValueDescriptor[]	stop_key,
    int                     stop_op,
    int                     expect_numrows,
    int                     input_expect_key,
    int                     order)
        throws StandardException, T_Fail
    {
        HashSet set = null;
        long    key;
        long numrows = 0;
        boolean ordered = (order == ORDER_FORWARD || order == ORDER_DESC);

        set = create_hash_set(input_expect_key, expect_numrows, order);

        // select entire data set into a hash table, with first column key 
		int[] keyColumns = new int[1];
		keyColumns[0] = 0;

        BackingStoreHashtable result_set = 
            tc.createBackingStoreHashtableFromScan(
                conglomid, 
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                start_key, start_op,
                qualifier,
                stop_key, stop_op,
                -1,             // no limit on total rows.
                keyColumns,     // first column is hash key column
                false,          // don't remove duplicates
                -1,             // no estimate of rows
                -1,             // put it all into memory
                -1,             // use default initial capacity
                -1,             // use default load factor
                false,          // don't maintain runtime statistics
                false,          // don't skip null key columns
                false,          // don't keep after commit
                false);         // don't include row locations

        // make sure the expected result set is the same as the actual result
        // set.

        Enumeration e = result_set.elements();

        while (e.hasMoreElements())
        {
            Object   obj;
            DataValueDescriptor[] row = null;

            if ((obj = e.nextElement()) instanceof DataValueDescriptor[] )
            {
                row = (DataValueDescriptor[] ) obj;
                key = ((SQLLongint)(row[2])).getLong();

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                if (!set.remove(key))
                {
                    return(
                        fail("(t_scanFetchHashtable-obj) wrong key, expected (" + 
                              input_expect_key + ")" + 
                              "but got (" + key + ")."));
                }
                numrows++;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-2493
            else if (obj instanceof List)
            {
                List row_vect = (List) obj;

                for (int i = 0; i < row_vect.size(); i++)
                {
                    row = (DataValueDescriptor[]) row_vect.get(i);

                    key = ((SQLLongint)(row[2])).getLong();

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                    if (!set.remove(key))
                    {
                        return(fail(
                            "(t_scanFetchHashtable-vector) wrong key, expected (" + 
                             input_expect_key + ")" + 
                             "but got (" + key + ")."));
                    }
                    numrows++;
                }
            }
            else
            {
                return(fail(
                    "(t_scanFetchHashtable) got bad type for data: " + obj));
            }
        }

        if (numrows != expect_numrows)
        {
            return(
                fail(
                    "(t_scanFetchHashtable) wrong number of rows. Expected " +
                     expect_numrows + " rows, but got " + numrows + "rows."));
        }

        result_set.close();

        // select entire data set into a hash table, with key being 
        // the third column, which is the unique id used to verify the
        // right result set is being returned.:

        // open a new scan
		keyColumns[0] = 2;

        result_set = 
            tc.createBackingStoreHashtableFromScan(
                conglomid,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet) null,
                start_key, start_op,
                qualifier,
                stop_key, stop_op,
                -1,             // no limit on total rows.
                keyColumns,              // third column is hash key column
                false,          // don't remove duplicates
                -1,             // no estimate of rows
                -1,             // put it all into memory
                -1,             // use default initial capacity
                -1,             // use default load factor
                false,         // don't maintain runtime statistics
				false,			// don't skip null key columns
                false,          // don't keep after commit
                false);         // don't include row locations

        Object removed_obj;
        for (numrows = 0; numrows < expect_numrows; numrows++)
        {
			long exp_key ;
			if (order == ORDER_DESC)
				exp_key = input_expect_key - numrows;
			else
				exp_key = input_expect_key + numrows;
            if ((removed_obj = 
                    result_set.remove(
                        new SQLLongint(exp_key))) == null)
            {
                fail("(t_scanFetchHashtable-2-vector) wrong key, expected (" + 
                      (exp_key) + ")" + 
                      "but did not find it.");
            }
        }

        if (numrows != expect_numrows)
        {
            return(fail("(t_scanFetchHashtable-2) wrong number of rows. Expected " +
                 expect_numrows + " rows, but got " + numrows + "rows."));
        }

        return(true);
    }

    /* public methods of T_QualifierTest */

    public boolean t_testqual(TransactionController tc)
        throws StandardException, T_Fail
    {
        boolean                 ret_val             = true;
        DataValueDescriptor[]   openscan_template   = null;
        DataValueDescriptor[]   fetch_template      = null;
        DataValueDescriptor[]   base_row            = null;
        T_SecondaryIndexRow     index_row           = null;
        long                    value               = -1;
        long        col1[]  = { 1,  3,  4,  4,  4,  5,  5,  5,  6,  7,  9};
        long        col2[]  = { 1,  1,  2,  4,  6,  2,  4,  6,  1,  1,  1};
        long        col3[]  = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};
        long                    conglomid;
        long                    base_conglomid;
        long                    index_conglomid;
        ConglomerateController  base_cc             = null;
        ConglomerateController  index_cc            = null;
        RowLocation             base_rowloc         = null;

        base_row = TemplateRow.newU8Row(3);

        if (init_conglomerate_type.compareTo("BTREE") == 0)
        {
            base_conglomid = 
                tc.createConglomerate(
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                    "heap", base_row, null,  null, null, 
                    TransactionController.IS_DEFAULT);

            index_row = new T_SecondaryIndexRow();

            base_cc = 
                tc.openConglomerate(
                    base_conglomid,
                    false,
                    TransactionController.OPENMODE_FORUPDATE,
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

            base_rowloc = base_cc.newRowLocationTemplate();

            index_row.init(base_row, base_rowloc, 4);

            index_conglomid = 
                tc.createConglomerate(
                    init_conglomerate_type, index_row.getRow(), 
                    null,
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                    null,
					init_properties,
					init_temporary ? TransactionController.IS_TEMPORARY : TransactionController.IS_DEFAULT);

            index_cc =	
                tc.openConglomerate(
                    index_conglomid,
                    false,
                    TransactionController.OPENMODE_FORUPDATE,
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

            conglomid = index_conglomid;
            openscan_template = index_row.getRow();

            // make another template
            T_SecondaryIndexRow fetch_index_row = new T_SecondaryIndexRow();
            fetch_index_row.init(
                TemplateRow.newU8Row(3),
                base_cc.newRowLocationTemplate(), 
                4);
            fetch_template = fetch_index_row.getRow();
        }
        else
        {
            base_conglomid = 
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                tc.createConglomerate(
                    init_conglomerate_type, 
                    base_row, 
                    null,                   // default order 
                    null,                   // default collation
                    init_properties,
                    init_temporary ? 
                        TransactionController.IS_TEMPORARY : 
                        TransactionController.IS_DEFAULT);

            base_cc =	
                tc.openConglomerate(
                    base_conglomid,
                    false,
                    TransactionController.OPENMODE_FORUPDATE,
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_SERIALIZABLE);

            base_rowloc = base_cc.newRowLocationTemplate();

            conglomid = base_conglomid;
            openscan_template = base_row;
            fetch_template    = TemplateRow.newU8Row(3);
        }

        // insert them in reverse order just to make sure btree is sorting them
        for (int i = col1.length - 1; i >= 0; i--)
        {
            ((SQLLongint)(base_row[0])).setValue(col1[i]);
            ((SQLLongint)(base_row[1])).setValue(col2[i]);
            ((SQLLongint)(base_row[2])).setValue(col3[i]);

            base_cc.insertAndFetchLocation(base_row, base_rowloc);

            if (init_conglomerate_type.compareTo("BTREE") == 0)
            {
                index_cc.insert(index_row.getRow());
            }
        }

        tc.commit();


        // run through a predicates as described in the openScan() interface,
        // and implement them in qualifiers rather than start and stop.
        //

        // Use the following SQLLongint's for qualifier values //
        SQLLongint qual_col1 = new SQLLongint(-1);
        SQLLongint qual_col2 = new SQLLongint(-1);
        SQLLongint qual_col3 = new SQLLongint(-1);
        SQLLongint qual_col4 = new SQLLongint(-1);
        SQLLongint qual_col5 = new SQLLongint(-1);
        SQLLongint qual_col6 = new SQLLongint(-1);
        SQLLongint qual_col7 = new SQLLongint(-1);


        // test predicate x = 5
        //
        //     result set should be: {5,2,16}, {5,4,17}, {5,6,18}
        //
        progress("qual scan (x = 5)");
        qual_col1.setValue(5);
        Qualifier q1[][] = 
        {
            {
                new QualifierUtil(0, qual_col1,
                            Orderable.ORDER_OP_EQUALS, 
                            false, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA, 
                   q1,
                   null,  ScanController.NA,
                   3, 16, init_order))
        {
            ret_val = false;
        }
                   
        //  +---------------------------------------------------------+
        //  |pred  |start|key|stop |key|rows returned |rows locked    |
        //  |      |value|op |value|op |              |(serialization)|
        //  +------+-----+---+-----+---+--------------+---------------+
        //  |x > 5 |{5}  |GT |null |   |{6,1} .. {9,1}|{5,6} .. {9,1} |
        //  +-----------------------------------------+---------------+
        progress("qual scan (x > 5)");
        qual_col1.setValue(5);
        Qualifier q2[][] = 
        {
            {
                new QualifierUtil(
                    0, qual_col1, Orderable.ORDER_OP_LESSOREQUALS, 
                    true, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA, 
                   q2,
                   null,  ScanController.NA,
                   3, 19, init_order))
        {
            ret_val = false;
        }

        //  +---------------------------------------------------------+
        //  |pred  |start|key|stop |key|rows returned |rows locked    |
        //  |      |value|op |value|op |              |(serialization)|
        //  +------+-----+---+-----+---+--------------+---------------+
        //  |x >= 5|{5}  |GE |null |   |{5,2} .. {9,1}|{4,6} .. {9,1} |
        //  +-----------------------------------------+---------------+
        progress("qual scan (x >= 5)");
        qual_col1.setValue(5);
        Qualifier q3[][] = 
        {
            {
                new QualifierUtil(0, qual_col1,
                                Orderable.ORDER_OP_LESSTHAN, 
                                true, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA, 
                   q3,
                   null, ScanController.NA,
                   6, 16, init_order))
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
        progress("qual scan (x <= 5)");
        qual_col1.setValue(5);
        Qualifier q4[][] = 
        {
            {
                new QualifierUtil(0, qual_col1,
                                Orderable.ORDER_OP_LESSOREQUALS, 
                                false, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA, 
                   q4,
                   null, ScanController.NA,
                   8, 11, init_order))
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
        progress("qual scan (x < 5)");
        qual_col1.setValue(5);
        Qualifier q5[][] = 
        {
            {
                new QualifierUtil(0, qual_col1,
                                Orderable.ORDER_OP_LESSTHAN, 
                                false, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA, 
                   q5,
                   null, ScanController.NA,
                   5, 11, init_order))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
	    //  |x >= 5 and x <= 7|{5},  |GE|{7}  |GT|{5,2} .. {7,1}|{4,6} .. {7,1}|
        // 	+------------------------------------------------------------------+
        progress("qual scan (x >= 5 and x <= 7)");
        qual_col1.setValue(5);
        qual_col2.setValue(7);
        Qualifier q6[][] = {
            {
                new QualifierUtil(0, qual_col1,
                            Orderable.ORDER_OP_LESSTHAN, 
                            true, true, true),
                new QualifierUtil(0, qual_col2,
                            Orderable.ORDER_OP_LESSOREQUALS, 
                            false, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA,
                   q6,
                   null, ScanController.NA,
                   5, 16, init_order))
        {
            ret_val = false;
        }

        // passing qualifier in q6[0][0], q6[0][1] should evaluate same as 
        // passing in q6[0][0], q6[1][0]

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
	    //  |x >= 5 and x <= 7|{5},  |GE|{7}  |GT|{5,2} .. {7,1}|{4,6} .. {7,1}|
        // 	+------------------------------------------------------------------+
        progress("qual scan (x >= 5 and x <= 7)");
        qual_col1.setValue(5);
        qual_col2.setValue(7);
        Qualifier q6_2[][] = {
            {
                new QualifierUtil(0, qual_col1,
                            Orderable.ORDER_OP_LESSTHAN, 
                            true, true, true)
            },
            {
                new QualifierUtil(0, qual_col2,
                            Orderable.ORDER_OP_LESSOREQUALS, 
                            false, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA,
                   q6_2,
                   null, ScanController.NA,
                   5, 16, init_order))
        {
            ret_val = false;
        }


        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
	    //  |x = 5 and y > 2  |{5,2} |GT|{5}  |GT|{5,4} .. {5,6}|{5,2} .. {9,1}|
        // 	+------------------------------------------------------------------+
        progress("qual scan (x = 5 and y > 2)");
        qual_col1.setValue(5);
        qual_col2.setValue(2);
        Qualifier q7[][] = {
            {
                new QualifierUtil(0, qual_col1,
                            Orderable.ORDER_OP_EQUALS, 
                            false, true, true),
                new QualifierUtil(1, qual_col2,
                            Orderable.ORDER_OP_LESSOREQUALS, 
                            true, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA,
                   q7,
                   null, ScanController.NA,
                   2, 17, init_order))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
        // 	|x = 5 and y >= 2 | {5,2}|GE| {5} |GT|{5,2} .. {5,6}|{4,6} .. {9,1}|
        // 	+------------------------------------------------------------------+
        progress("qual scan (x = 5 and y >= 2)");
        qual_col1.setValue(5);
        qual_col2.setValue(2);
        Qualifier q8[][] = {
            {
                new QualifierUtil(0, qual_col1,
                            Orderable.ORDER_OP_EQUALS, 
                            false, true, true),
                new QualifierUtil(1, qual_col2,
                            Orderable.ORDER_OP_LESSTHAN, 
                            true, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA,
                   q8,
                   null, ScanController.NA,
                   3, 16, init_order))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
        // 	|x = 5 and y < 5  | {5}  |GE|{5,5}|GE|{5,2} .. {5,4}|{4,6} .. {5,4}|
        // 	+------------------------------------------------------------------+
        progress("qual scan (x = 5 and y < 5)");
        qual_col1.setValue(5);
        qual_col2.setValue(5);
        Qualifier q9[][] = {
            { 
                new QualifierUtil(0, qual_col1,
                            Orderable.ORDER_OP_EQUALS, 
                            false, true, true),
                new QualifierUtil(1, qual_col1,
                            Orderable.ORDER_OP_LESSTHAN, 
                            false, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA,
                   q9,
                   null, ScanController.NA,
                   2, 16, init_order))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred             |start|key|stop |key|rows returned|rows locked   |
        //  |                 |value|op |value|op |             |(serialized)  |
        //  +-----------------+------+--+-----+--+--------------+--------------+
        // 	|x = 2            | {2}  |GE| {2} |GT|none          |{1,1} .. {1,1}|
        // 	+------------------------------------------------------------------+
        progress("qual scan (x = 2)");
        qual_col1.setValue(2);
        Qualifier q10[][] = {
            {
                new QualifierUtil(0, qual_col1,
                                Orderable.ORDER_OP_EQUALS, 
                                false, true, true)
            }
        };
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA,
                   q10,
                   null, ScanController.NA,
                   0, 0, init_order))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred            |start|key|stop |key|rows returned |rows locked   |
        //  |                |value|op |value|op |              |(serialized)  |
        //  +----------------+-----+---+-----+-- +--------------+--------------+
        // 	|x >= 5 or y = 6 | null|   | null|   |{4,6} .. {9,1}|{1,1} .. {9,1}|
        // 	+------------------------------------------------------------------+
        progress("qual scan (x >= 5) or (y = 6)");
        qual_col1.setValue(5);
        qual_col2.setValue(6);
        Qualifier q11[][] =  new Qualifier[2][];
        q11[0] = new Qualifier[0];
        q11[1] = new Qualifier[2];

        q11[1][0] = 
                new QualifierUtil(
                        0, qual_col1,
                        Orderable.ORDER_OP_GREATEROREQUALS, 
                        false, true, true);
        q11[1][1] = 
                new QualifierUtil(
                        1, qual_col2,
                        Orderable.ORDER_OP_EQUALS, 
                        false, true, true);
        
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA,
                   q11,
                   null, ScanController.NA,
                   7, 15, init_order))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred            |start|key|stop |key|rows returned |rows locked   |
        //  |                |value|op |value|op |              |(serialized)  |
        //  +----------------+-----+---+-----+-- +--------------+--------------+
        // 	|(x = 1 or y = 1 or y = 6)|
        // 	|     and        |
        // 	|(x > 5 or y = 1)|
        // 	|     and        |
        // 	|(x = 9 or x = 7)|null |   | null|   |{7,1} .. {9,1}|{1,1} .. {9,1}|
        // 	+------------------------------------------------------------------+

        progress("qual scan (x = 1 or y = 1 or y = 6) and (x > 5 or y = 1) and (x = 9 or x = 7)");
        qual_col1.setValue(1);
        qual_col2.setValue(1);
        qual_col3.setValue(6);
        qual_col4.setValue(5);
        qual_col5.setValue(1);
        qual_col6.setValue(9);
        qual_col7.setValue(7);

        Qualifier q12[][] = new Qualifier[4][];
        q12[0] = new Qualifier[0];
        q12[1] = new Qualifier[3];
        q12[2] = new Qualifier[2];
        q12[3] = new Qualifier[2];


        q12[1][0] = 
            new QualifierUtil(
                    0, qual_col1,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);

        q12[1][1] = 
            new QualifierUtil(
                    1, qual_col2,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);

        q12[1][2] = 
            new QualifierUtil(
                    1, qual_col3,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);

        q12[2][0] = 
            new QualifierUtil(
                    0, qual_col4,
                    Orderable.ORDER_OP_GREATERTHAN, 
                    false, true, true);

        q12[2][1] = 
            new QualifierUtil(
                    1, qual_col5,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);

        q12[3][0] = 
            new QualifierUtil(
                    0, qual_col6,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);

        q12[3][1] = 
            new QualifierUtil(
                    0, qual_col7,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);
        
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA,
                   q12,
                   null, ScanController.NA,
                   2, 20, init_order))
        {
            ret_val = false;
        }

        //  +------------------------------------------------------------------+
        //  |pred            |start|key|stop |key|rows returned |rows locked   |
        //  |                |value|op |value|op |              |(serialized)  |
        //  +----------------+-----+---+-----+-- +--------------+--------------+
        // 	|(y = 4 or y = 1)|
        // 	|     and        |
        // 	|(x = 1 or x = 4 or x= 9)|
        // 	|     and        |
        // 	|(z = 15 or z = 14)|null |   | null|   |{4,4} .. {4,4}| ALL        |
        // 	+------------------------------------------------------------------+

        progress("qual scan (x = 1 or x = 4 or x= 9) and (y = 4 or y = 1) and (z = 15 or z = 14)");

        qual_col1.setValue(4);
        qual_col2.setValue(1);
        qual_col3.setValue(1);
        qual_col4.setValue(4);
        qual_col5.setValue(9);
        qual_col6.setValue(15);
        qual_col7.setValue(14);

        Qualifier q13[][] = new Qualifier[4][];
        q13[0] = new Qualifier[0];
        q13[1] = new Qualifier[2];
        q13[2] = new Qualifier[3];
        q13[3] = new Qualifier[2];


        q13[1][0] = 
            new QualifierUtil(
                    1, qual_col1,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);

        q13[1][1] = 
            new QualifierUtil(
                    1, qual_col2,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);


        q13[2][0] = 
            new QualifierUtil(
                    0, qual_col4,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);

        q13[2][1] = 
            new QualifierUtil(
                    0, qual_col5,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);

        q13[2][2] = 
            new QualifierUtil(
                    0, qual_col3,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);

        q13[3][0] = 
            new QualifierUtil(
                    2, qual_col6,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);

        q13[3][1] = 
            new QualifierUtil(
                    2, qual_col7,
                    Orderable.ORDER_OP_EQUALS, 
                    false, true, true);
        
        if (!t_scan(tc, conglomid, openscan_template, fetch_template,
                   null, ScanController.NA,
                   q13,
                   null, ScanController.NA,
                   1, 14, init_order))
        {
            ret_val = false;
        }

        tc.commit();
        progress("Ending t_testqual");

        return(ret_val);
    }

	private static boolean fail(String msg)
        throws T_Fail
	{
        throw T_Fail.testFailMsg("T_QualifierTest failure: " + msg);
	}

	private void progress(String msg)
	{
		this.init_out.println("T_QualifierTest progress: " + msg);
	}
}
