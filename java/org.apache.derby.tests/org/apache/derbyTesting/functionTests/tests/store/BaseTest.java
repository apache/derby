/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.procedure

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

package org.apache.derbyTesting.functionTests.tests.store;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.tools.ij;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;



/**
Common utility functions that can be shared across store .java tests.
<p>
If more than one store tests wants a function, put it here rather than copy
it.  Hopefully going forward, with enough utility functions adding new store
tests will be easier.  New store tests should extend this test to pick
up access to utility routines - see OnlineCompressTest.java as an example.

**/
public abstract class BaseTest
{
    private   static boolean debug_system_procedures_created = false;
    protected static boolean verbose = false;

    abstract public void testList(Connection conn) throws SQLException;

    void runTests(String[] argv)
        throws Throwable
    {
   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        System.out.println("conn from ij.startJBMS() = " + conn);
        conn.setAutoCommit(false);

        try
        {
            testList(conn);
        }
        catch (SQLException sqle)
        {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }

    public BaseTest()
    {
    }

    protected void beginTest(
    Connection  conn,
    String      str)
        throws SQLException
    {
        log("Beginning test: " + str);
        conn.commit();
    }

    protected void testProgress(
    String      str)
        throws SQLException
    {
        log("Executing test: " + str);
    }

    protected void endTest(
    Connection  conn,
    String      str)
        throws SQLException
    {
        conn.commit();
        log("Ending test: " + str);
    }

    protected void log(String   str)
    {
        System.out.println(str);
    }

    protected void logError(String   str)
    {
        System.out.println("ERROR: " + str);
    }

    /**
     * Simple wrapper to execute a sql string.
     **/
    public void executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-361
    Connection  conn,
    String      stmt_str,
    boolean     commit_query)
        throws SQLException
    {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(stmt_str);
        stmt.close();
        if (commit_query)
            conn.commit();
    }

    /**
     * Call consistency checker on the table.
     * <p>
     **/
    protected boolean checkConsistency(
    Connection  conn,
    String      schemaName,
    String      tableName)
		throws SQLException
    {
        Statement s = conn.createStatement();

        ResultSet rs = 
            s.executeQuery(
                "values SYSCS_UTIL.SYSCS_CHECK_TABLE('" + 
                schemaName + "', '" + 
                tableName  + "')");

        if (!rs.next())
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT("no value from values clause.");
            }
        }

        boolean consistent = rs.getBoolean(1);

        rs.close();

        conn.commit();

        return(consistent);
    }

    /**
     * Call consistency checker on all the tables.
     * <p>
     **/
    protected boolean checkAllConsistency(
    Connection  conn)
		throws SQLException
    {
        Statement s = conn.createStatement();

        ResultSet rs = 
            s.executeQuery(
                "select schemaname, tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE(schemaname, tablename) " + 
                "from sys.systables a,  sys.sysschemas b where a.schemaid = b.schemaid");

        int table_count = 0;

        while (rs.next())
        {
            table_count++;
            if (rs.getInt(3) != 1)
            {
                System.out.println(
                    "Bad return from consistency check of " + 
                    rs.getString(1) + "." + rs.getString(2));
            }
        }

        if (table_count < 5)
        {
            // there are at least 5 system catalogs.
            System.out.println(
                "Something wrong with consistency check query, found only " + 
                table_count + " tables.");
        }

        rs.close();
        s.close();

        conn.commit();

        return(true);
    }

    /**
     * Create a system procedures to access SANE debug table routines.
     * <p>
     **/
    protected void createDebugSystemProcedures(
    Connection  conn)
		throws SQLException
    {
        Statement s = conn.createStatement();
        s.executeUpdate(
            "CREATE FUNCTION D_CONGLOMID_PRINT(DBNAME VARCHAR(128), CONGLOMID INT) RETURNS VARCHAR(32000) RETURNS NULL ON NULL INPUT EXTERNAL NAME 'org.apache.derby.impl.store.raw.data.D_DiagnosticUtil.diag_conglomid' LANGUAGE JAVA PARAMETER STYLE JAVA");
        s.executeUpdate(
            "CREATE FUNCTION DIAG_CONGLOMID(DBNAME VARCHAR(128), CONGLOMID INT) RETURNS VARCHAR(32000) RETURNS NULL ON NULL INPUT EXTERNAL NAME 'org.apache.derby.impl.store.raw.data.D_DiagnosticUtil.diag_conglomid' LANGUAGE JAVA PARAMETER STYLE JAVA");
        s.close();
        conn.commit();

        debug_system_procedures_created = true;
    }

    /**
     * Return string with table information.
     * <p>
     * Dumps summary store information about the table, also dumps extra
     * information about individual pages into the error log file.
     **/
    String dump_table(
    Connection  conn,
    String      schemaName,
//IC see: https://issues.apache.org/jira/browse/DERBY-132
    String      tableName,
    boolean     commit_transaction)
		throws SQLException
    {
        if (!debug_system_procedures_created)
            createDebugSystemProcedures(conn);

        // run the following query:
        //
        // select
        //     sys.systables.tablename,
        //     sys.sysconglomerates.conglomeratenumber,
        //     DIAG_CONGLOMID('wombat', conglomeratenumber)
        // from sys.systables, sys.sysconglomerates
        // where
        //     sys.systables.tableid = sys.sysconglomerates.tableid and
        //     sys.systables.schemaid = sys.sysconglomerates.schemaid and
        //     sys.systables.tablename = tableName;
        //
        // TODO - really should join with schemaName too.

        PreparedStatement ps = 
            conn.prepareStatement(
                "select sys.systables.tablename, sys.sysconglomerates.conglomeratenumber, DIAG_CONGLOMID('wombat', conglomeratenumber) from sys.systables, sys.sysconglomerates where sys.systables.tableid = sys.sysconglomerates.tableid and sys.systables.schemaid = sys.sysconglomerates.schemaid and sys.systables.tablename = ?");
        ps.setString(1, tableName);
        ResultSet rs = ps.executeQuery();

        if (!rs.next())
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT("no value from values clause.");
            }
        }

        String dump_table_info = rs.getString(3);

        rs.close();

//IC see: https://issues.apache.org/jira/browse/DERBY-132
        if (commit_transaction)
            conn.commit();

        return(dump_table_info);

    }

    /**
     * Get lock table.
     * <p>
     * Returns a single string with a dump of the entire lock table.
     * <p>
     *
	 * @return The lock table.
     *
     * @param conn                  The connection to use.
     * @param include_system_locks  If true include non-user locks like those
     *                              requested by background internal threads.
     *
     **/
    protected String get_lock_info(
    Connection  conn,
    boolean     include_system_locks)
		throws SQLException
    {
        // Run the following query to get the current locks in the system,
        // toggling the "t.type='UserTransaction'" based on 
        // include_system_locks input:
        //
        // select
        //     cast(l.xid as char(8)) as xid,
        //     cast(username as char(8)) as username,
        //     cast(t.type as char(8)) as trantype,
        //     cast(l.type as char(8)) as type,
        //     cast(lockcount as char(3)) as cnt,
        //     cast(mode as char(4)) as mode,
        //     cast(tablename as char(12)) as tabname,
        //     cast(lockname as char(10)) as lockname,
        //     state,
        //     status
        // from
        //     SYSCS_DIAG.LOCK_TABLE l  
        // right outer join SYSCS_DIAG.TRANSACTION_TABLE t
        //     on l.xid = t.xid where l.tableType <> 'S' and 
        //        t.type='UserTransaction'
        // order by
        //     tabname, type desc, mode, cnt, lockname;
        String lock_query = 
//IC see: https://issues.apache.org/jira/browse/DERBY-571
            "select cast(l.xid as char(8)) as xid, cast(username as char(8)) as username, cast(t.type as char(8)) as trantype, cast(l.type as char(8)) as type, cast(lockcount as char(3)) as cnt, cast(mode as char(4)) as mode, cast(tablename as char(12)) as tabname, cast(lockname as char(10)) as lockname, state, status from SYSCS_DIAG.LOCK_TABLE l right outer join SYSCS_DIAG.LOCK_TABLE t on l.xid = t.xid where l.tableType <> 'S' ";
        if (!include_system_locks)
            lock_query += "and t.type='UserTransaction' ";
        
        lock_query += "order by tabname, type desc, mode, cnt, lockname";

        PreparedStatement ps = conn.prepareStatement(lock_query);

        ResultSet rs = ps.executeQuery();

        String lock_output = 
        "xid     |username|trantype|type    |cnt|mode|tabname     |lockname  |state|status\n" +
        "---------------------------------------------------------------------------------\n";
        while (rs.next())
        {
            String username     = rs.getString(1);
            String trantype     = rs.getString(2);
            String type         = rs.getString(3);
            String lockcount    = rs.getString(4);
            String mode         = rs.getString(5);
            String tabname      = rs.getString(6);
            String lockname     = rs.getString(7);
            String state        = rs.getString(8);
            String status       = rs.getString(9);

            lock_output +=
                username + "|" +
                trantype + "|" +
                type     + "|" +
                lockcount+ "|" +
                mode     + "|" +
                tabname  + "|" +
                lockname + "|" +
                state    + "|" +
                status   + "\n";
        }

        rs.close();

        return(lock_output);
    }

    /**
     * create given table on the input connection.
     * <p>
     * Takes care of dropping the table if it exists already.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void createTable(
    Connection  conn,
    String      tbl_name,
    String      create_str)
		throws SQLException
    {
        Statement  stmt = conn.createStatement();

        // drop table, ignore table does not exist error.

        try
        {
            stmt.executeUpdate("drop table " + tbl_name);
        }
        catch (Exception e)
        {
            // ignore drop table errors.
        }

        stmt.executeUpdate(create_str);
    }

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
    Connection  conn,
    String      schemaName,
    String      tableName,
    boolean     commit_xact)
		throws SQLException
    {
        String stmt_str = 
//IC see: https://issues.apache.org/jira/browse/DERBY-1187
//IC see: https://issues.apache.org/jira/browse/DERBY-1188
            "select conglomeratename, isindex, numallocatedpages, numfreepages, numunfilledpages, pagesize, estimspacesaving from new org.apache.derby.diag.SpaceTable('" +
            tableName + "') t where isindex = 0";
        PreparedStatement space_stmt = conn.prepareStatement(stmt_str);
        ResultSet rs = space_stmt.executeQuery();

        if (!rs.next())
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "No rows returned from space table query on table: " +
                    schemaName + "." + tableName);
            }
        }

        int[] ret_info = new int[SPACE_INFO_NUMCOLS];
        String conglomerate_name        = rs.getString(1);
//IC see: https://issues.apache.org/jira/browse/DERBY-1187
//IC see: https://issues.apache.org/jira/browse/DERBY-1188
        for (int i = 0; i < SPACE_INFO_NUMCOLS; i++)
        {
            ret_info[i] = rs.getInt(i + 2);
        }

        if (rs.next())
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "More than one row returned from space query on table: " +
                    schemaName + "." + tableName);
            }
        }

        if (verbose)
        {
            System.out.println(
                "Space information for " + schemaName + "." + tableName + ":");
            System.out.println(
//IC see: https://issues.apache.org/jira/browse/DERBY-1187
//IC see: https://issues.apache.org/jira/browse/DERBY-1188
                "isindex = " + ret_info[SPACE_INFO_IS_INDEX]);
            System.out.println(
                "num_alloc = " + ret_info[SPACE_INFO_NUM_ALLOC]);
            System.out.println(
                "num_free = " + ret_info[SPACE_INFO_NUM_FREE]);
            System.out.println(
                "num_unfilled = " + ret_info[SPACE_INFO_NUM_UNFILLED]);
            System.out.println(
                "page_size = " + ret_info[SPACE_INFO_PAGE_SIZE]);
            System.out.println(
                "estimspacesaving = " + ret_info[SPACE_INFO_ESTIMSPACESAVING]);
        }

        rs.close();

        if (commit_xact)
            conn.commit();

        return(ret_info);
    }

    /**
     * Given output from getSpaceInfo(), return total pages in file.
     * <p>
     * simply the sum of allocated and free pages.
     *
     **/
    protected int total_pages(int[] space_info)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1187
//IC see: https://issues.apache.org/jira/browse/DERBY-1188
        return(space_info[SPACE_INFO_NUM_FREE] + 
               space_info[SPACE_INFO_NUM_ALLOC]);
    }
}
