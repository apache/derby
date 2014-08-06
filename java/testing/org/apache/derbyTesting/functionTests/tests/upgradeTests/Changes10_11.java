/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_11

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Upgrade test cases for 10.11.
 */
public class Changes10_11 extends UpgradeChange
{

    //////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    //////////////////////////////////////////////////////////////////

    private static  final   String  SYNTAX_ERROR = "42X01";
    private static  final   String  HARD_UPGRADE_REQUIRED = "XCL47";
    private static  final   String  NOT_IMPLEMENTED = "0A000";
    private static  final   String  NO_ROWS_AFFECTED = "02000";
    private static  final   String  UNKNOWN_OPTIONAL_TOOL = "X0Y88";
    private static  final   String  UNRECOGNIZED_PROCEDURE = "42Y03";

    //////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    //////////////////////////////////////////////////////////////////

    public Changes10_11(String name) {
        super(name);
    }

    //////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    //////////////////////////////////////////////////////////////////

    /**
     * Return the suite of tests to test the changes made in 10.11.
     *
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        return new BaseTestSuite(Changes10_11.class, "Upgrade test for 10.11");
    }

    //////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    //////////////////////////////////////////////////////////////////

    public void testTriggerWhenClause() throws SQLException {
        String createTrigger =
                "create trigger d534_tr1 after insert on d534_t1 "
                + "referencing new as new for each row mode db2sql "
                + "when (new.x <> 2) insert into d534_t2 values new.x";

        Statement s = createStatement();
        switch (getPhase()) {
            case PH_CREATE:
                s.execute("create table d534_t1(x int)");
                s.execute("create table d534_t2(y int)");
                assertCompileError(SYNTAX_ERROR, createTrigger);
                break;
            case PH_SOFT_UPGRADE:
                assertCompileError(HARD_UPGRADE_REQUIRED, createTrigger);
                break;
            case PH_POST_SOFT_UPGRADE:
                assertCompileError(SYNTAX_ERROR, createTrigger);
                break;
            case PH_HARD_UPGRADE:
                s.execute(createTrigger);
                s.execute("insert into d534_t1 values 1, 2, 3");
                JDBC.assertFullResultSet(
                        s.executeQuery("select * from d534_t2 order by y"),
                        new String[][]{{"1"}, {"3"}});
                break;
        }
    }

    /**
     * Test how dropping trigger dependencies works across upgrade and
     * downgrade. Regression test for DERBY-2041.
     */
    public void testDropTriggerDependencies() throws SQLException {
        if (!oldAtLeast(10, 2)) {
            // Support for SYNONYMS was added in 10.1. Support for CALL
            // statements in trigger actions was added in 10.2. Since this
            // test case uses both of those features, skip it on the oldest
            // versions.
            return;
        }

        setAutoCommit(false);
        Statement s = createStatement();
        switch (getPhase()) {
            case PH_CREATE:
                // Let's create some objects to use in the triggers.
                s.execute("create table d2041_t(x int)");
                s.execute("create table d2041_table(x int)");
                s.execute("create table d2041_synonym_table(x int)");
                s.execute("create synonym d2041_synonym "
                        + "for d2041_synonym_table");
                s.execute("create view d2041_view(x) as values 1");
                s.execute("create function d2041_func(i int) returns int "
                        + "language java parameter style java "
                        + "external name 'java.lang.Math.abs' no sql");
                s.execute("create procedure d2041_proc() "
                        + "language java parameter style java "
                        + "external name 'java.lang.Thread.yield' no sql");

                // Create the triggers with the old version.
                createDerby2041Triggers(s);
                commit();
                break;
            case PH_SOFT_UPGRADE:
                // Drop the trigger dependencies. Since the triggers were
                // created with the old version, the dependencies were not
                // registered, so expect the DROP operations to succeed.
                dropDerby2041TriggerDeps(s, false);

                // The triggers still exist, so it is possible to drop them.
                dropDerby2041Triggers(s);

                // We want to use the objects further, so roll back the
                // DROP operations.
                rollback();

                // Recreate the triggers with the new version.
                dropDerby2041Triggers(s);
                createDerby2041Triggers(s);
                commit();

                // Dropping the dependencies now should fail.
                dropDerby2041TriggerDeps(s, true);
                break;
            case PH_POST_SOFT_UPGRADE:
                // After downgrade, the behaviour isn't quite consistent. The
                // dependencies were registered when the triggers were created
                // with the new version, but the old versions only have code
                // to detect some of the dependencies. So some will fail and
                // others will succeed.

                // Dependencies on tables and synonyms are detected.
                assertStatementError("X0Y25", s, "drop table d2041_table");
                assertStatementError("X0Y25", s, "drop synonym d2041_synonym");

                // Dependencies on views, functions and procedures are not
                // detected.
                s.execute("drop view d2041_view");
                s.execute("drop function d2041_func");
                s.execute("drop procedure d2041_proc");

                // Restore the database state.
                rollback();
                break;
            case PH_HARD_UPGRADE:
                // In hard upgrade, we should be able to detect the
                // dependencies registered when the triggers were created
                // in the soft-upgraded database.
                dropDerby2041TriggerDeps(s, true);
        }
    }

    private void createDerby2041Triggers(Statement s) throws SQLException {
        s.execute("create trigger d2041_tr1 after insert on d2041_t "
                + "for each row mode db2sql insert into d2041_table values 1");
        s.execute("create trigger d2041_tr2 after insert on d2041_t "
                + "for each row mode db2sql "
                + "insert into d2041_synonym values 1");
        s.execute("create trigger d2041_tr3 after insert on d2041_t "
                + "for each row mode db2sql select * from d2041_view");
        s.execute("create trigger d2041_tr4 after insert on d2041_t "
                + "for each row mode db2sql values d2041_func(1)");
        s.execute("create trigger d2041_tr5 after insert on d2041_t "
                + "for each row mode db2sql call d2041_proc()");
    }

    private void dropDerby2041Triggers(Statement s) throws SQLException {
        for (int i = 1; i <= 5; i++) {
            s.execute("drop trigger d2041_tr" + i);
        }
    }

    private void dropDerby2041TriggerDeps(Statement s, boolean expectFailure)
            throws SQLException {
        String[] stmts = {
            "drop table d2041_table",
            "drop synonym d2041_synonym",
            "drop view d2041_view",
            "drop function d2041_func",
            "drop procedure d2041_proc",
        };

        for (String stmt : stmts) {
            if (expectFailure) {
                assertStatementError("X0Y25", s, stmt);
            } else {
                assertUpdateCount(s, 0, stmt);
            }
        }
    }

    /**
     * Create a trigger in each upgrade phase and verify that they fire in
     * the order in which they were created. DERBY-5866 changed how the
     * trigger creation timestamp was stored (from local time zone to UTC),
     * and we want to test that this change doesn't affect the trigger
     * execution order when the triggers have been created with different
     * versions.
     */
    public void testDerby5866TriggerExecutionOrder() throws SQLException {
        Statement s = createStatement();
        switch (getPhase()) {
            case PH_CREATE:
                s.execute("create table d5866_t1(x int)");
                s.execute("create table d5866_t2(x int "
                        + "generated always as identity, y varchar(100))");
                s.execute("create trigger d5866_create after insert "
                        + "on d5866_t1 for each statement mode db2sql "
                        + "insert into d5866_t2(y) values 'CREATE'");
                break;
            case PH_SOFT_UPGRADE:
                s.execute("create trigger d5866_soft after insert on d5866_t1 "
                        + "insert into d5866_t2(y) values 'SOFT UPGRADE'");
                break;
            case PH_POST_SOFT_UPGRADE:
                s.execute("create trigger d5866_post_soft after insert "
                        + "on d5866_t1 for each statement mode db2sql "
                        + "insert into d5866_t2(y) values 'POST SOFT UPGRADE'");
                break;
            case PH_HARD_UPGRADE:
                s.execute("create trigger d5866_hard after insert on d5866_t1 "
                        + "insert into d5866_t2(y) values 'HARD UPGRADE'");

                // Fire all the triggers and verify that they executed in
                // the right order.
                s.execute("insert into d5866_t1 values 1,2,3");
                JDBC.assertFullResultSet(
                        s.executeQuery("select y from d5866_t2 order by x"),
                        new String[][] {
                            { "CREATE" }, { "SOFT UPGRADE" },
                            { "POST SOFT UPGRADE" }, { "HARD UPGRADE" }
                        });
                break;
        }
    }

    /**
     * Test how deferrable constraints work across upgrade and
     * downgrade. Regression test for DERBY-532.
     * 
     * @throws java.sql.SQLException
     */
    public void testDeferrableConstraints() throws SQLException {
        if (!oldAtLeast(10, 4)) {
            // Support for nullable UNIQUE constraints wasn't added before
            // 10.4
            return;
        }

        setAutoCommit(false);
        Statement st = createStatement();
        
        String[] cDeferrableCol = new String[]{
            "create table t532(i int not null primary key deferrable)",
            "create table t532(i int unique deferrable)",
            "create table t532(i int not null unique deferrable)",
            "create table t532(i int check (i > 0) deferrable)",
            "create table t532(i int references referenced(i) deferrable)"};

        String[] cDeferrableTab = new String[]{
            "create table t532(i int not null, constraint c primary key(i) deferrable)",
            "create table t532(i int, constraint c unique(i) deferrable)",
            "create table t532(i int not null, constraint c unique(i) " + 
                "deferrable)",
            "create table t532(i int, constraint c check (i > 0) deferrable)",
            "create table t532(i int, constraint c foreign key(i) " + 
                "references referenced(i) deferrable)"};

        st.executeUpdate("create table referenced(i int primary key)");
        commit();
        
        try {
            switch (getPhase()) {
            
            case PH_CREATE:
                for (String s : cDeferrableCol) {
                    assertStatementError(SYNTAX_ERROR, st, s);
                    assertStatementError(SYNTAX_ERROR, st, s);
                }
                break;
                
            case PH_POST_SOFT_UPGRADE:
                for (String s : cDeferrableCol) {
                    assertStatementError(SYNTAX_ERROR, st, s);
                    assertStatementError(SYNTAX_ERROR, st, s);
                }
                break;
                
            case PH_SOFT_UPGRADE:
                for (String s : cDeferrableCol) {
                    assertStatementError(HARD_UPGRADE_REQUIRED, st, s);
                    assertStatementError(HARD_UPGRADE_REQUIRED, st, s);
                }
                break;
                
            case PH_HARD_UPGRADE:
                for (String s : cDeferrableCol) {
                    st.execute(s);
                    rollback();
                    st.execute(s);
                    rollback();
                }
                break;
            }
        } finally {
            st.executeUpdate("drop table referenced");
            commit();
        }
    }

    /** Test the MERGE statement introduced by 10.11 */
    public void testMerge() throws Exception
    {
        String mergeStatement =
            "merge into targetTable t using sourceTable s on t.a = s.a\n" +
            "when matched then delete\n";

        Statement s = createStatement();
        switch (getPhase())
        {
            case PH_CREATE:
                s.execute("create table targetTable( a int )");
                s.execute("create table sourceTable( a int )");
                assertCompileError( SYNTAX_ERROR, mergeStatement );
                break;
            case PH_SOFT_UPGRADE:
                assertCompileError( HARD_UPGRADE_REQUIRED,  mergeStatement );
                break;
            case PH_POST_SOFT_UPGRADE:
                assertCompileError( SYNTAX_ERROR, mergeStatement );
                break;
            case PH_HARD_UPGRADE:
                expectExecutionWarning( getConnection(), NO_ROWS_AFFECTED, mergeStatement );
                break;
        }
    }

    /** Test that identity columns handle self-deadlock in soft-upgrade mode */
    public void test_derby6692() throws Exception
    {
        Connection  conn = getConnection();
        
        switch (getPhase())
        {
            case PH_CREATE:
            case PH_SOFT_UPGRADE:
            case PH_POST_SOFT_UPGRADE:
            case PH_HARD_UPGRADE:

                boolean originalAutoCommit = conn.getAutoCommit();
                try
                {
                    conn.setAutoCommit( false );

                    conn.prepareStatement( "create table t_6692(i int generated always as identity)" ).execute();
                    conn.prepareStatement( "insert into t_6692 values (default)" ).execute();

                    conn.rollback();
                }
                finally
                {
                    conn.setAutoCommit( originalAutoCommit );
                }
                break;
        }
    }

    /** Test the Lucene plugin */
    public void testLuceneSupport() throws Exception
    {
        Properties  properties = TestConfiguration.getSystemProperties();
        if ( getBooleanProperty( properties, TestConfiguration.KEY_OMIT_LUCENE ) )  { return; }

        Version initialVersion = new Version( getOldMajor(), getOldMinor(), 0, 0 );
        Version firstVersionHavingBooleanType = new Version( 10, 7, 0, 0 );
        Version firstVersionHavingOptionalTools = new Version( 10, 10, 0, 0 );

        boolean hasBooleanDatatype = initialVersion.compareTo( firstVersionHavingBooleanType ) >= 0;
        boolean hasOptionalTools  = initialVersion.compareTo( firstVersionHavingOptionalTools ) >= 0;

        String  originalSQLState;
        if ( !hasBooleanDatatype ) { originalSQLState = SYNTAX_ERROR; }
        else if ( !hasOptionalTools ) { originalSQLState = UNRECOGNIZED_PROCEDURE; }
        else { originalSQLState = UNKNOWN_OPTIONAL_TOOL; }

        String  softUpgradeSQLState;
        if ( !hasOptionalTools ) { softUpgradeSQLState = UNRECOGNIZED_PROCEDURE; }
        else { softUpgradeSQLState = HARD_UPGRADE_REQUIRED; }

        String loadTool = "call syscs_util.syscs_register_tool( 'luceneSupport', true )";
        String unloadTool = "call syscs_util.syscs_register_tool( 'luceneSupport', false )";

        Statement statement = createStatement();
        switch (getPhase())
        {
            case PH_CREATE:
            case PH_POST_SOFT_UPGRADE:
                assertStatementError( originalSQLState, statement, loadTool );
                break;
            case PH_SOFT_UPGRADE:
                assertStatementError( softUpgradeSQLState, statement, loadTool );
                break;
            case PH_HARD_UPGRADE:
                statement.executeUpdate( loadTool );
                statement.executeUpdate( unloadTool );
                break;
        }
    }

    /** Test the addition of sequence generators to back identity columns */
    public void testIdentitySequence() throws Exception
    {
        Properties  properties = TestConfiguration.getSystemProperties();
        if ( getBooleanProperty( properties, TestConfiguration.KEY_OMIT_LUCENE ) )  { return; }

        Version initialVersion = new Version( getOldMajor(), getOldMinor(), 0, 0 );
        Version firstVersionHavingSequences = new Version( 10, 6, 0, 0 );
        boolean hasSequences = initialVersion.compareTo( firstVersionHavingSequences ) >= 0;

        Statement statement = createStatement();

        String  peek = "values syscs_util.syscs_peek_at_identity( 'APP', 'IDSEQ1' )";
        
        switch ( getPhase() )
        {
            case PH_CREATE:
                statement.executeUpdate
                    (
                     "create function uuidToSequenceName( uuid char( 36 ) ) returns varchar( 128 )\n" +
                     "language java parameter style java no sql\n" +
                     "external name 'org.apache.derbyTesting.functionTests.tests.lang.IdentitySequenceTest.uuidToSequenceName'\n"
                     );
                statement.executeUpdate
                    ( "create table idseq1( a int generated always as identity ( start with 10, increment by 20 ), b int )" );
                statement.executeUpdate( "insert into idseq1( b ) values ( 1 ), ( 20 )" );
                if ( hasSequences ) { assertEquals( 0, countSequences( statement ) ); }
                assertStatementError( UNRECOGNIZED_PROCEDURE, statement, peek );
                break;
            case PH_POST_SOFT_UPGRADE:
                statement.executeUpdate( "create table idseq2( a int generated always as identity, b int )" );
                if ( hasSequences ) { assertEquals( 0, countSequences( statement ) ); }
                assertStatementError( UNRECOGNIZED_PROCEDURE, statement, peek );
                break;
            case PH_SOFT_UPGRADE:
                statement.executeUpdate( "create table idseq3( a int generated always as identity, b int )" );
                if ( hasSequences ) { assertEquals( 0, countSequences( statement ) ); }
                assertStatementError( UNRECOGNIZED_PROCEDURE, statement, peek );
                break;
            case PH_HARD_UPGRADE:
                statement.executeUpdate( "create table idseq4( a int generated always as identity, b int )" );
                assertEquals
                    (
                     4,
                     count
                     (
                      statement,
                      "select count(*)\n" +
                      "from sys.systables t, sys.syssequences s\n" +
                      "where uuidToSequenceName( t.tableid ) = s.sequencename\n" +
                      "and t.tablename like 'IDSEQ%'"
                      )
                     );
                JDBC.assertFullResultSet
                    (
                     statement.executeQuery( peek ),
                     new String[][]
                     {
                         { "50" },
                     }
                     );
                JDBC.assertFullResultSet
                    (
                     statement.executeQuery
                     (
                      "select sch.schemaName,\n" +
                      "s.currentvalue, s.startvalue, s.minimumvalue, s.maximumvalue, s.increment, s.cycleoption\n" +
                      "from sys.sysschemas sch, sys.systables t, sys.syssequences s\n" +
                      "where t.tablename = 'IDSEQ1'\n" +
                      "and uuidToSequenceName( t.tableid ) = s.sequencename\n" +
                      "and sch.schemaid = s.schemaid\n"
                      ),
                     new String[][]
                     {
                         { "SYS", "50", "10", "-2147483648", "2147483647", "20", "N" },
                     }
                     );
                break;
        }
    }
    private int countSequences( Statement statement )
        throws Exception
    {
        return count( statement, "select count(*) from sys.syssequences" );
    }
    private int count( Statement statement, String query ) throws Exception
    {
        ResultSet   rs = statement.executeQuery( query );
        rs.next();

        try {
            return rs.getInt( 1 );
        }
        finally
        {
            rs.close();
        }
    }

    /** Return the boolean value of a system property */
    private static  boolean getBooleanProperty( Properties properties, String key )
    {
        return Boolean.valueOf( properties.getProperty( key ) ).booleanValue();
    }

    /**
     * Assert that the statement text, when executed, raises a warning.
     */
    private void    expectExecutionWarning( Connection conn, String sqlState, String query )
        throws Exception
    {
        expectExecutionWarnings( conn, new String[] { sqlState }, query );
    }

    /**
     * Assert that the statement text, when executed, raises a warning.
     */
    private void    expectExecutionWarnings( Connection conn, String[] sqlStates, String query )
        throws Exception
    {
        println( "\nExpecting warnings " + fill( sqlStates ).toString() + " when executing:\n\t"  );
        PreparedStatement   ps = chattyPrepare( conn, query );

        ps.execute();

        int idx = 0;

        for ( SQLWarning sqlWarning = ps.getWarnings(); sqlWarning != null; sqlWarning = sqlWarning.getNextWarning() )
        {
            String          actualSQLState = sqlWarning.getSQLState();

            if ( idx >= sqlStates.length )
            {
                fail( "Got more warnings than we expected." );
            }

            String  expectedSqlState = sqlStates[ idx++ ];

            assertEquals( expectedSqlState, actualSQLState );
        }

        assertEquals( idx, sqlStates.length );

        ps.close();
    }

    /**
     * <p>
     * Fill an ArrayList from an array.
     * </p>
     */
    protected <T> ArrayList<T> fill( T[] raw )
    {
        return new ArrayList<T>(Arrays.asList(raw));
    }
}
