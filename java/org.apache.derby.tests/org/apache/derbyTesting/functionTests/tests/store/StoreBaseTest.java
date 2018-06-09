/*
Class org.apache.derbyTesting.functionTests.tests.jdbc4.Derby3650Test

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import junit.framework.Assert;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;



/**

Utility functions useful when testing store.

Maybe move this stuff to BaseJDBCTestCase if they seem useful to others.

**/

public class StoreBaseTest extends BaseJDBCTestCase 
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public StoreBaseTest(String name) 
    {
        super(name);
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */

    /**
     * call the space table vti.
     * <p>
     * Utility test function to call the space table vti to get information
     * about allocated and free pages.  Information is passed back in an
     * int array as follows:
     *   is_index                 = ret_info[0];
     *   num_alloc                = ret_info[1];
     *   num_free                 = ret_info[2];
     *   page_size                = ret_info[3];
     *   estimate_space_savings   = ret_info[4];
     * <p>
     *
     *
     **/
    protected static final int SPACE_INFO_IS_INDEX          = 0;
    protected static final int SPACE_INFO_NUM_ALLOC         = 1;
    protected static final int SPACE_INFO_NUM_FREE          = 2;
    protected static final int SPACE_INFO_NUM_UNFILLED      = 3;
    protected static final int SPACE_INFO_PAGE_SIZE         = 4;
    protected static final int SPACE_INFO_ESTIMSPACESAVING  = 5;

    protected static final int SPACE_INFO_NUMCOLS           = 6;

    protected int[] getSpaceInfo(
    String      schemaName,
    String      tableName,
    boolean     commit_xact)
		throws SQLException
    {
        String stmt_str = 
            "select " + 
                "conglomeratename, " +
                "isindex, "           + 
                "numallocatedpages, " + 
                "numfreepages, "      + 
                "numunfilledpages, "  + 
                "pagesize, "          + 
                "estimspacesaving "   + 
            "from new org.apache.derby.diag.SpaceTable('" +
                tableName + "') t where isindex = 0";

        PreparedStatement space_stmt = prepareStatement(stmt_str);
        ResultSet         rs         = space_stmt.executeQuery();

        boolean rows_found = rs.next();

        Assert.assertTrue(
            "No rows returned from space table query on table: " +
            schemaName + "." + tableName, rows_found);

        int[] ret_info = new int[SPACE_INFO_NUMCOLS];
        String conglomerate_name        = rs.getString(1);
        for (int i = 0; i < SPACE_INFO_NUMCOLS; i++)
        {
            ret_info[i] = rs.getInt(i + 2);
        }

        rows_found = rs.next();

        Assert.assertFalse(
            "More than one row returned from space query on table: " +
            schemaName + "." + tableName, rows_found);

        // debug info
        println(
            "Space information for " + schemaName + "." + tableName + ":");
        println(
            "isindex = " + ret_info[SPACE_INFO_IS_INDEX]);
        println(
            "num_alloc = " + ret_info[SPACE_INFO_NUM_ALLOC]);
        println(
            "num_free = " + ret_info[SPACE_INFO_NUM_FREE]);
        println(
            "num_unfilled = " + ret_info[SPACE_INFO_NUM_UNFILLED]);
        println(
            "page_size = " + ret_info[SPACE_INFO_PAGE_SIZE]);
        println(
            "estimspacesaving = " + ret_info[SPACE_INFO_ESTIMSPACESAVING]);

        rs.close();

        if (commit_xact)
            commit();

        return(ret_info);
    }
}
