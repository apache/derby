/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SequenceTest

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
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derby.iapi.reference.SQLState;

/**
 * Test sequences.
 */
public class SequenceTest extends GeneratedColumnsHelper {

    private static final String TEST_DBO = "TEST_DBO";
    private static final String ALPHA = "ALPHA";
    private static final String BETA = "BETA";
    private static final String[] LEGAL_USERS = {TEST_DBO, ALPHA, BETA};

    public SequenceTest(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }

    // SETUP

    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite() {
        TestSuite suite = new TestSuite(SequenceTest.class, "Sequence Test");
        // Need atleast JSR169 to run these tests
        if (!JDBC.vmSupportsJSR169() && !JDBC.vmSupportsJDBC3()) {
            return suite;
        }

        Test cleanTest = new CleanDatabaseTestSetup(suite);
        Test authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
                (cleanTest, LEGAL_USERS, "sequence");
        Test authorizedTest = TestConfiguration.sqlAuthorizationDecorator(authenticatedTest);

        return authorizedTest;
    }

    public void test_01_CreateSequence() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE SEQUENCE mySeq");
    }

    public void test_02_DropSequence() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE SEQUENCE mySeq1");
        s.executeUpdate("DROP SEQUENCE mySeq1 restrict");
    }

    public void test_03_DuplicateCreationFailure() throws SQLException {
        Statement s = null;
        try {
            s = createStatement();
            s.executeUpdate("CREATE SEQUENCE mySeq1");
            s.executeUpdate("CREATE SEQUENCE mySeq1");
        } catch (SQLException sqle) {
            assertSQLState("X0Y68", sqle);
        }finally{
            s.executeUpdate("DROP SEQUENCE mySeq1 restrict"); // Drop the one created.
        }
    }

    public void test_04_ImplicitSchemaCreation() throws SQLException {
        Connection adminCon = openUserConnection(TEST_DBO);

        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmt = alphaCon.createStatement();

        // should implicitly create schema ALPHA
        stmt.executeUpdate("CREATE SEQUENCE alpha_seq");
        stmt.executeUpdate("DROP SEQUENCE alpha_seq restrict");
        stmt.close();
        alphaCon.close();
        adminCon.close();
    }

    public void test_05CreateWithSchemaSpecified() throws SQLException {

        // create DB
        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmt = alphaCon.createStatement();

        // should implicitly create schema ALPHA
        stmt.executeUpdate("CREATE SEQUENCE alpha.alpha_seq");
        stmt.executeUpdate("DROP SEQUENCE alpha.alpha_seq restrict");
        stmt.close();
        alphaCon.close();
    }

    public void test_06_CreateWithSchemaSpecifiedCreateTrue() throws SQLException {
        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmt = alphaCon.createStatement();

        // should implicitly create schema ALPHA
        stmt.executeUpdate("CREATE SEQUENCE alpha.alpha_seq");
        stmt.executeUpdate("DROP SEQUENCE alpha.alpha_seq restrict");
        stmt.close();
        alphaCon.close();
    }

    public void test_07_CreateWithSchemaDropWithNoSchema() throws SQLException {
        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmt = alphaCon.createStatement();

        // should implicitly create schema ALPHA
        stmt.executeUpdate("CREATE SEQUENCE alpha.alpha_seq");
        stmt.executeUpdate("DROP SEQUENCE alpha_seq restrict");
        stmt.close();
        alphaCon.close();
    }

    /**
     * Test trying to drop a sequence in a schema that doesn't belong to one
     */
    public void test_08_DropOtherSchemaSequence() throws SQLException {
        Connection adminCon = openUserConnection(TEST_DBO);

        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmtAlpha = alphaCon.createStatement();
        stmtAlpha.executeUpdate("CREATE SEQUENCE alpha_seq");

        Connection betaCon = openUserConnection(BETA);
        Statement stmtBeta = betaCon.createStatement();

        // should implicitly create schema ALPHA
        assertStatementError("42507", stmtBeta, "DROP SEQUENCE alpha.alpha_seq restrict");

        // Cleanup:
        stmtAlpha.executeUpdate("DROP SEQUENCE alpha_seq restrict");
        
        stmtAlpha.close();
        stmtBeta.close();
        alphaCon.close();
        betaCon.close();
        adminCon.close();
    }

    /**
     * Test trying to create a sequence in a schema that doesn't belong to one
     */
    public void test_09_CreateOtherSchemaSequence() throws SQLException {
        // create DB
        Connection adminCon = openUserConnection(TEST_DBO);

        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmtAlpha = alphaCon.createStatement();
        stmtAlpha.executeUpdate("CREATE SEQUENCE alpha_seq");

        Connection betaCon = openUserConnection(BETA);
        Statement stmtBeta = betaCon.createStatement();

        // should implicitly create schema ALPHA
        assertStatementError("42507", stmtBeta, "CREATE SEQUENCE alpha.alpha_seq3");

        // Cleanup:
        stmtAlpha.executeUpdate("DROP SEQUENCE alpha_seq restrict");
        
        stmtAlpha.close();
        stmtBeta.close();
        alphaCon.close();
        betaCon.close();
        adminCon.close();
    }

    public void test_09a_createSequenceWithArguments() throws Exception {
        Connection alphaCon = openUserConnection(ALPHA);

        goodStatement(alphaCon,
                "CREATE SEQUENCE small1 AS SMALLINT START WITH 0 INCREMENT BY 1");

        goodStatement(alphaCon,
                "CREATE SEQUENCE small2 AS SMALLINT START WITH " + Short.MIN_VALUE
                        + " MAXVALUE " + Short.MAX_VALUE);

        goodStatement(alphaCon,
                "CREATE SEQUENCE small3 AS SMALLINT START WITH 1200"
                        + " INCREMENT BY -5 MAXVALUE 32000 NO MINVALUE CYCLE");

        // maxvalue out of range
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                "CREATE SEQUENCE small3 AS SMALLINT START WITH " + Short.MIN_VALUE
                        + " MAXVALUE " + Integer.MAX_VALUE);

         // start with out of range
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_INVALID_START,
                "CREATE SEQUENCE small4 AS SMALLINT START WITH " + Integer.MIN_VALUE);

        // minvalue larger than maxvalue negative
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_MIN_EXCEEDS_MAX,
                "CREATE SEQUENCE small5 AS SMALLINT MAXVALUE -20000 MINVALUE -1");

        goodStatement(alphaCon,
                "CREATE SEQUENCE int1 AS INTEGER START WITH " + Integer.MIN_VALUE + " INCREMENT BY -10 CYCLE");

        goodStatement(alphaCon,
                "CREATE SEQUENCE int2 AS INTEGER INCREMENT BY 5"
                        + " MAXVALUE " + Integer.MAX_VALUE);

        goodStatement(alphaCon,
                "CREATE SEQUENCE int3 AS INTEGER START WITH 1200 INCREMENT BY 5 "
                        + "NO MAXVALUE MINVALUE -320000 CYCLE");

        // minvalue out of range
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                "CREATE SEQUENCE int4 AS INTEGER START WITH " + Integer.MIN_VALUE
                        + " MAXVALUE " + Short.MAX_VALUE
                        + " MINVALUE " + Long.MIN_VALUE);

        // increment 0
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_INCREMENT_ZERO,
                "CREATE SEQUENCE int5 AS INTEGER INCREMENT BY 0");

        goodStatement(alphaCon,
                "CREATE SEQUENCE long1 AS BIGINT START WITH " + Long.MIN_VALUE + " INCREMENT BY -100 NO CYCLE");

        goodStatement(alphaCon,
                "CREATE SEQUENCE long2 AS BIGINT INCREMENT BY 25"
                        + " MAXVALUE " + Integer.MAX_VALUE);

        goodStatement(alphaCon,
                "CREATE SEQUENCE long3 AS BIGINT START WITH 0 INCREMENT BY 5 "
                        + "NO MAXVALUE MINVALUE " + Long.MIN_VALUE + " CYCLE");

        // invalid minvalue
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                "CREATE SEQUENCE long4 AS BIGINT START WITH " + Integer.MAX_VALUE
                        + " MINVALUE " + Long.MAX_VALUE);

        // minvalue larger than maxvalue
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_MIN_EXCEEDS_MAX,
                "CREATE SEQUENCE long5 AS BIGINT START WITH 0 MAXVALUE 100000 MINVALUE 100001");

        // should fail for non-int TYPES
        expectCompilationError(alphaCon,
                SQLState.LANG_SYNTAX_ERROR,
                "CREATE SEQUENCE char1 AS CHAR INCREMENT BY 1");

    }

    /**
     * initial test for next value
     * @throws SQLException on error
     */
    public void test_10_NextValue() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE SEQUENCE mySeq1");
        s.execute("SELECT NEXT VALUE FOR mySeq1 from sys.systables");
        s.execute("DROP SEQUENCE mySeq1 restrict");
    }

    /**
     * Verify that sequences can't be used in many contexts.
     */
    public void test_11_forbiddenContexts() throws Exception
    {
        Connection conn = openUserConnection(ALPHA);

        goodStatement( conn, "create sequence seq_11_a\n" );
        goodStatement( conn, "create sequence seq_11_b\n" );

        String illegalSequence = SQLState.LANG_NEXT_VALUE_FOR_ILLEGAL;
        
        // sequences not allowed in WHERE clause
        expectCompilationError( conn, illegalSequence, "select * from sys.systables where ( next value for seq_11_a ) > 100\n" );

        // sequences not allowed in HAVING clause
        expectCompilationError
            ( conn, illegalSequence,
              "select max( conglomeratenumber ), tableid\n" +
              "from sys.sysconglomerates\n" +
              "group by tableid\n" +
              "having max( conglomeratenumber ) > ( next value for seq_11_a )\n"
              );
        
        // sequences not allowed in ON clause
        expectCompilationError
            ( conn, illegalSequence, "select * from sys.sysconglomerates left join sys.sysschemas on conglomeratenumber = ( next value for seq_11_a )\n" );

        // sequences not allowed in CHECK constraints
        expectCompilationError
            ( conn, illegalSequence, "create table t_11_1( a int check ( a > ( next value for seq_11_a ) ) )\n" );

        // sequences not allowed in generated columns
        expectCompilationError
            ( conn, illegalSequence, "create table t_11_1( a int, b generated always as ( a + ( next value for seq_11_a ) ) )\n" );

        // sequences not allowed in aggregates
        expectCompilationError
            ( conn, illegalSequence, "select max( next value for seq_11_a ) from sys.systables\n" );

        // sequences not allowed in CASE expressions
        expectCompilationError
            ( conn, illegalSequence, "values case when ( next value for seq_11_a ) < 0 then 100 else 200 end\n" );

        // sequences not allowed in DISTINCT clauses
        expectCompilationError
            ( conn, illegalSequence, "select distinct( next value for seq_11_a ) from sys.systables\n" );

        // sequences not allowed in ORDER BY clauses
        expectCompilationError
            ( conn, illegalSequence, "select tableid, ( next value for seq_11_a ) a from sys.systables order by a\n" );

        // sequences not allowed in GROUP BY expressions
        expectCompilationError
            ( conn, illegalSequence, "select max( tableid ), ( next value for seq_11_a ) from sys.systables group by ( next value for seq_11_a )\n" );

        // given sequence only allowed once per statement. see DERBY-4513.
        expectCompilationError
            ( conn, SQLState.LANG_SEQUENCE_REFERENCED_TWICE, "select next value for seq_11_a, next value for seq_11_a from sys.systables where 1=2\n" );

        // however, two different sequences can appear in a statement
        goodStatement( conn, "select next value for seq_11_a, next value for seq_11_b from sys.systables where 1=2\n" );
    }

    /**
     * Verify that optional clauses can appear in any order and redundant clauses
     * are forbidden.
     */
    public void test_12_clauseOrder() throws Exception
    {
        Connection conn = openUserConnection(ALPHA);

        goodSequence
            (
             conn,
             "seq_12_a", // name
             "", // clauses
             "INTEGER", // datatype
             Integer.MIN_VALUE, // initial
             Integer.MIN_VALUE, // min
             Integer.MAX_VALUE, // max
             1L, // step
             false // cycle
             );

        goodSequence
            (
             conn,
             "seq_12_b", // name
             "minvalue 5 increment by 3 cycle start with 100 maxvalue 1000000 as bigint", // clauses
             "BIGINT", // datatype
             100L, // initial
             5L, // min
             1000000L, // max
             3L, // step
             true // cycle
             );

        goodSequence
            (
             conn,
             "seq_12_c", // name
             "increment by 3 as smallint no cycle no maxvalue", // clauses
             "SMALLINT", // datatype
             Short.MIN_VALUE, // initial
             Short.MIN_VALUE, // min
             Short.MAX_VALUE, // max
             3L, // step
             false // cycle
             );

        goodSequence
            (
             conn,
             "seq_12_d", // name
             "maxvalue 1000000000 start with -50 increment by -3 cycle no minvalue", // clauses
             "INTEGER", // datatype
             -50L, // initial
             Integer.MIN_VALUE, // min
             1000000000, // max
             -3L, // step
             true // cycle
             );

        expectCompilationError
            ( conn, DUPLICATE_CLAUSE, "create sequence bad_12 as smallint as bigint\n" );
        expectCompilationError
            ( conn, DUPLICATE_CLAUSE, "create sequence bad_12 start with 3 start with 7\n" );
        expectCompilationError
            ( conn, DUPLICATE_CLAUSE, "create sequence bad_12 minvalue 5 no minvalue\n" );
        expectCompilationError
            ( conn, DUPLICATE_CLAUSE, "create sequence bad_12 maxvalue 5 no maxvalue\n" );
        expectCompilationError
            ( conn, DUPLICATE_CLAUSE, "create sequence bad_12 increment by 7 increment by -7\n" );
        expectCompilationError
            ( conn, DUPLICATE_CLAUSE, "create sequence bad_12 no cycle cycle\n" );
    }

    private void goodSequence
        (
         Connection conn,
         String sequenceName,
         String clauses,
         String datatype,
         long initialValue,
         long minValue,
         long maxValue,
         long stepValue,
         boolean cycle
         )
        throws Exception
    {
        String statement = "create sequence " + sequenceName + " " + clauses;
    
        goodStatement( conn, statement );

        PreparedStatement ps = chattyPrepare
            (
             conn,
             "select sequencedatatype, startvalue, minimumvalue, maximumvalue, increment, cycleoption\n" +
             "from sys.syssequences\n" +
             "where sequencename = ?"
             );
        ps.setString( 1, sequenceName.toUpperCase() );

        ResultSet rs = ps.executeQuery();

        rs.next();
        int col = 1;

        assertEquals( datatype, rs.getString( col++ ) );
        assertEquals( initialValue, rs.getLong( col++ ) );
        assertEquals( minValue, rs.getLong( col++ ) );
        assertEquals( maxValue, rs.getLong( col++ ) );
        assertEquals( stepValue, rs.getLong( col++ ) );
        assertEquals( cycle, rs.getString( col++ ).equals( "Y" ) );

        rs.close();
        ps.close();
    }

    /**
     * Verify that restricted drops prevent objects from being orphaned.
     */
    public void test_13_restrictedDrop() throws Exception
    {
        Connection conn = openUserConnection(ALPHA);

        goodStatement( conn, "create table t_13_a( a int )" );
        goodStatement( conn, "create table t_13_b( a int )" );

        String createStatement;
        String dropStatement;
        String createDependentObject;
        String dropDependentObject;
        String badDropState;

        createStatement = "create sequence seq_13_a";
        dropStatement = "drop sequence seq_13_a restrict";
        createDependentObject = "create trigger trig_13 after insert on t_13_a for each row insert into t_13_b( a ) values ( next value for seq_13_a )\n";
        dropDependentObject = "drop trigger trig_13";
        badDropState = FORBIDDEN_DROP_TRIGGER;
        verifyRestrictedDrop
            (
             conn,
             createDependentObject,
             dropDependentObject,
             createStatement,
             dropStatement,
             badDropState
             );
        
        createStatement = "create sequence seq_13_b";
        dropStatement = "drop sequence seq_13_b restrict";
        createDependentObject = "create view v_13( a, b ) as select a, next value for seq_13_b from t_13_a\n";
        dropDependentObject = "drop view v_13";
        badDropState = VIEW_DEPENDENCY;
        verifyRestrictedDrop
            (
             conn,
             createDependentObject,
             dropDependentObject,
             createStatement,
             dropStatement,
             badDropState
             );

    }

    /**
     * Verify that you can use sequences in insert statements driven
     * by selects. See DERBY-4803.
     */
    public void test_14_insertSelect() throws Exception
    {
        Connection conn = openUserConnection(ALPHA);

        goodStatement( conn, "create sequence sequence_is" );
        goodStatement( conn, "create table tis_1( a int )" );
        goodStatement( conn, "create table tis_2( a int, b int )" );
        goodStatement( conn, "insert into tis_1( a ) values ( 1 ), ( 2 )" );
        goodStatement( conn, "insert into tis_2 select next value for sequence_is, a from tis_1" );

        assertResults
            (
             conn,
             "select * from tis_2 order by b",
             new String[][]
             {
                 { "-2147483648", "1" },
                 { "-2147483647", "2" },
             },
             true
             );
    }
    
    /**
     * Verify that the new sequence-related keywords are non-reserved keywords.
     */
    public void test_15_5254() throws Exception
    {
        Connection conn = openUserConnection(ALPHA);

        goodStatement( conn, "create table t_5254( cycle int, minvalue int, maxvalue int )" );
        goodStatement( conn, "drop table t_5254" );
    }
    
    /**
     * Verify that trigger recompilation doesn't choke trying to create
     * two nested writable transactions.
     */
    public void test_16_6137() throws Exception
    {
        Connection conn = openUserConnection( TEST_DBO );

        goodStatement( conn, "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', '2' )" );
        goodStatement( conn, "create table t_6137( rateID int generated always as identity primary key, amount decimal( 5,2 ) )" );
        goodStatement( conn, "create table t_history_6137( changeID int primary key, amount decimal( 5,2 ) )" );
        goodStatement( conn, "create sequence seq_6137 start with 1" );
        goodStatement
            (
             conn,
             "create trigger trg_t_6137_hist_del\n" +
             "after delete on t_6137\n" +
             "referencing old row as old\n" +
             "for each row\n" +
             " insert into t_history_6137 ( changeID, amount ) values (( next value for seq_6137 ), old.amount )\n"
             );
        goodStatement( conn, "insert into t_6137( amount ) values ( 30.04 ), ( 60.04 ), ( 90.04 )" );

        // invalidate the stored statements so that the trigger will have to be recompiled
        goodStatement( conn, "call syscs_util.syscs_invalidate_stored_statements()" );

        // put the sequence in the cache
        assertResults
            (
             conn,
             "values next value for seq_6137",
             new String[][]
             {
                 { "1" },
             },
             true
             );

        // verify that the trigger recompiles and fires correctly
        goodStatement( conn, "delete from t_6137 where rateID = 1" );
        goodStatement( conn, "delete from t_6137 where rateID = 2" );
        assertResults
            (
             conn,
             "select * from t_history_6137 order by changeID",
             new String[][]
             {
                 { "2", "30.04" },
                 { "3", "60.04" },
             },
             true
             );

        // verify current value of sequence
        String  peekAtSequence = "values syscs_util.syscs_peek_at_sequence('" + TEST_DBO + "', 'SEQ_6137')";
        assertResults
            (
             conn,
             peekAtSequence,
             new String[][]
             {
                 { "4" },
             },
             true
             );

        // tidy up
        goodStatement( conn, "drop trigger trg_t_6137_hist_del" );
        goodStatement( conn, "drop table t_history_6137" );
        goodStatement( conn, "drop table t_6137" );
        goodStatement( conn, "drop sequence seq_6137 restrict" );
        goodStatement( conn, "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', null )" );

        // verify that the uncleared cache does not leave a phantom sequence hanging around
        expectExecutionError( conn, MISSING_OBJECT, peekAtSequence );
        expectCompilationError( conn, OBJECT_DOES_NOT_EXIST, "values next value for seq_6137" );
    }
}
