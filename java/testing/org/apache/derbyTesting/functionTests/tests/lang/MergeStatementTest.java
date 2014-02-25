/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.util.ArrayList;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.catalog.DefaultInfo;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

/**
 * <p>
 * Test the MERGE statement (see DERBY-3155).
 * </p>
 */
public class MergeStatementTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      RUTH = "RUTH";
    private static  final   String      ALICE = "ALICE";
    private static  final   String      FRANK = "FRANK";
    private static  final   String      TONY = "TONY";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, ALICE, RUTH, FRANK, TONY  };

    private static  final   String      TARGET_MUST_BE_BASE = "42XAK";
    private static  final   String      SOURCE_MUST_BE_BASE_VIEW_OR_VTI = "42XAL";
    private static  final   String      SAME_EXPOSED_NAME = "42XAM";
    private static  final   String      DUPLICATE_COLUMNS = "42X13";
    private static  final   String      COLUMN_NOT_IN_TABLE = "42X14";
    private static  final   String      COLUMN_COUNT_MISMATCH = "42802";
    private static  final   String      DUPLICATE_SET_COLUMNS = "42X16";
    private static  final   String      MISSING_TABLE = "42X05";
    private static  final   String      NO_ROWS_AFFECTED = "02000";
    private static  final   String      NO_DML_IN_BEFORE_TRIGGERS = "42Z9D";
    private static  final   String      NO_SUBQUERIES_IN_MATCHED_CLAUSE = "42XAO";
    private static  final   String      NO_SYNONYMS_IN_MERGE = "42XAP";
    private static  final   String      NO_DCL_IN_MERGE = "42XAQ";
    private static  final   String      PARAMETER_NOT_SET = "07000";
    private static  final   String      CARDINALITY_VIOLATION = "21000";

    private static  final   String[]    TRIGGER_HISTORY_COLUMNS = new String[] { "ACTION", "ACTION_VALUE" };

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // In-memory table for recording trigger actions. Each row is a { actionName, actionValue } pair.
    private static  ArrayList<String[]> _triggerHistory = new ArrayList<String[]>();

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Create a new instance.
     */

    public MergeStatementTest(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite()
    {
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(MergeStatementTest.class);

        Test        cleanTest = new CleanDatabaseTestSetup( suite );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( cleanTest, LEGAL_USERS, "MergeStatementPermissions" );
        Test        authorizedTest = TestConfiguration.sqlAuthorizationDecorator( authenticatedTest );

        return authorizedTest;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test some bad syntax.
     * </p>
     */
    public  void    test_001_badSyntax()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        goodStatement
            ( dboConnection,
              "create table t1( c1 int generated always as identity, c2 int, c3 int generated always as ( c1 + c2 ), c1_4 int )" );
        goodStatement( dboConnection, "create table t2( c1 int generated always as identity, c2 int, c3 int, c4 int, c5 varchar( 5 ) )" );
        goodStatement( dboConnection, "create table t3( c1 int generated always as identity, c2 int, c3 int )" );
        goodStatement( dboConnection, "create view v1 as select * from t1" );
        goodStatement( dboConnection, "create view v2 as select * from t2" );
        goodStatement
            (
             dboConnection,
             "create function integerList()\n" +
             "returns table( s_r int, s_nr int, ns_r int, ns_nr int )\n" +
             "language java\n" +
             "parameter style derby_jdbc_result_set\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.RestrictedVTITest.integerList'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function illegalFunction() returns int\n" +
             "language java parameter style java contains sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.illegalFunction'\n"
             );

        // INSERT only allowed with NOT MATCHED
        expectCompilationError
            ( dboConnection, SYNTAX_ERROR,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched then insert\n"
              );
        // DELETE only allowed with MATCHED
        expectCompilationError
            ( dboConnection, SYNTAX_ERROR,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then delete\n"
              );
        // UPDATE only allowed with MATCHED
        expectCompilationError
            ( dboConnection, SYNTAX_ERROR,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then update\n"
              );

        // Target table must be a base table.
        expectCompilationError
            ( dboConnection, TARGET_MUST_BE_BASE,
              "merge into v1\n" +
              "using t2\n" +
              "on v1.c1 = t2.c1\n" +
              "when matched then delete\n"
              );
        expectCompilationError
            ( dboConnection, TARGET_MUST_BE_BASE,
              "merge into table( integerList() ) i\n" +
              "using t2\n" +
              "on i.s_r = t2.c1\n" +
              "when matched then delete\n"
              );
        expectCompilationError
            ( dboConnection, TARGET_MUST_BE_BASE,
              "merge into (t1 left join t3 on t1.c3 = t3.c3)\n" +
              "using t2\n" +
              "on c4 is not null\n" +
              "when matched then delete\n"
              );
        expectCompilationError
            ( dboConnection, TARGET_MUST_BE_BASE,
              "merge into v1 v\n" +
              "using t2 t\n" +
              "on v.c1 = t.c1\n" +
              "when matched then delete\n"
              );

        // Source must be a base table, view, or table function.
        expectCompilationError
            ( dboConnection, SOURCE_MUST_BE_BASE_VIEW_OR_VTI,
              "merge into t1\n" +
              "using ( t3 left join t2 on t3.c3 = t2.c3 )\n" +
              "on t1.c1 is not null\n" +
              "when matched then delete\n"
              );

        // Source and target may not have the same correlation names.
        expectCompilationError
            ( dboConnection, SAME_EXPOSED_NAME,
              "merge into t1 t2\n" +
              "using t2\n" +
              "on c4 is null\n" +
              "when matched then delete\n"
              );
        expectCompilationError
            ( dboConnection, SAME_EXPOSED_NAME,
              "merge into t1\n" +
              "using t2 t1\n" +
              "on c4 is not null\n" +
              "when matched then delete\n"
              );
        expectCompilationError
            ( dboConnection, SAME_EXPOSED_NAME,
              "merge into t1 v\n" +
              "using t2 v\n" +
              "on c4 is not null\n" +
              "when matched then delete\n"
              );

        // ON clause may only refer to columns in the source and target tables
        expectCompilationError
            ( dboConnection, COLUMN_OUT_OF_SCOPE,
              "merge into t1\n" +
              "using t2\n" +
              "on t3.c1 = t2.c1\n" +
              "when matched then delete\n"
              );

        //
        // The matching refinement clauses can only mention columns in the
        // source and target tables.
        //
        expectCompilationError
            ( dboConnection, COLUMN_OUT_OF_SCOPE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched and t3.c2 is null then insert ( c2 ) values ( t2.c2 )\n"
              );
        expectCompilationError
            ( dboConnection, COLUMN_OUT_OF_SCOPE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t3.c2 = t2.c2 then update set c1_4 = t2.c3\n"
              );
        expectCompilationError
            ( dboConnection, COLUMN_OUT_OF_SCOPE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t3.c2 = t2.c2 then delete\n"
              );

        //
        // The refining restriction of a WHEN NOT MATCHED clause may not
        // reference columns in the target table.
        //
        expectCompilationError
            ( dboConnection, COLUMN_OUT_OF_SCOPE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched and t1.c2 is null then insert ( c2 ) values ( t2.c2 )\n"
              );

        // Boolean expressions can't contain functions which issue SQL
        expectCompilationError
            ( dboConnection, ROUTINE_CANT_ISSUE_SQL,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1 and t2.c2 = illegalFunction()\n" +
              "when matched then delete\n"
              );
        expectCompilationError
            ( dboConnection, ROUTINE_CANT_ISSUE_SQL,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t2.c2 = illegalFunction() then delete\n"
              );
        expectCompilationError
            ( dboConnection, ROUTINE_CANT_ISSUE_SQL,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t2.c2 = illegalFunction() then update set c2 = t2.c2\n"
              );
        expectCompilationError
            ( dboConnection, ROUTINE_CANT_ISSUE_SQL,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched and t2.c2 = illegalFunction() then insert ( c2 ) values ( t2.c2 )\n"
              );

        // Can only specify DEFAULT as the value of a generated column.
        expectCompilationError
            ( dboConnection, CANT_MODIFY_IDENTITY,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c1, c2 ) values ( t2.c1, t2.c2 )\n"
              );
        expectCompilationError
            ( dboConnection, CANT_OVERRIDE_GENERATION_CLAUSE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c2, c3 ) values ( t2.c2, t2.c3 )\n"
              );
        expectCompilationError
            ( dboConnection, CANT_MODIFY_IDENTITY,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 != t2.c2 then update set c1 = t2.c2\n"
              );
        expectCompilationError
            ( dboConnection, CANT_OVERRIDE_GENERATION_CLAUSE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 != t2.c2 then update set c3 = t2.c2\n"
              );

        // and you can't update an identity column at all
        expectCompilationError
            ( dboConnection, CANT_MODIFY_IDENTITY,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 != t2.c2 then update set c1 = default, c2 = t2.c2\n"
              );

        // Column may not appear twice in INSERT list.
        expectCompilationError
            ( dboConnection, DUPLICATE_COLUMNS,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c2, c2 ) values ( t2.c2, t2.c3 )\n"
              );

        // INSERTed column name must be in the target table
        expectCompilationError
            ( dboConnection, COLUMN_NOT_IN_TABLE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c2, c5 ) values ( t2.c2, t2.c3 )\n"
              );

        // INSERTed value must be storable in target column.
        expectCompilationError
            ( dboConnection, ILLEGAL_STORAGE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c2 ) values ( t2.c5 )\n"
              );

        // INSERT refinement clause can't mention columns in the target table
        expectCompilationError
            ( dboConnection, COLUMN_OUT_OF_SCOPE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c2 ) values ( c1_4 )\n"
              );

        // Must have same number of columns in INSERT and VALUES lists
        expectCompilationError
            ( dboConnection, COLUMN_COUNT_MISMATCH,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c2, c1_4 ) values ( t2.c2 )\n"
              );
        expectCompilationError
            ( dboConnection, COLUMN_COUNT_MISMATCH,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c2 ) values ( t2.c2, t2.c3 )\n"
              );

        // Trigger transition tables may not be used as target tables.
        expectCompilationError
            ( dboConnection, TARGET_MUST_BE_BASE,
              "create trigger trig1 after update on t1\n" +
              "referencing old table as old_cor new table as new_cor\n" +
              "for each statement\n" +
              "merge into new_cor\n" +
              "using t2\n" +
              "on new_cor.c1 = t2.c1\n" +
              "when not matched then insert ( c2 ) values ( t2.c2 )\n"
              );
        expectCompilationError
            ( dboConnection, TARGET_MUST_BE_BASE,
              "create trigger trig2 after update on t1\n" +
              "referencing old table as old_cor new table as new_cor\n" +
              "for each statement\n" +
              "merge into old_cor\n" +
              "using t2\n" +
              "on old_cor.c1 = t2.c1\n" +
              "when not matched then insert ( c2 ) values ( t2.c2 )\n"
              );

        // Columns may not be SET twice in a MATCHED ... THEN UPDATE clause
        expectCompilationError
            ( dboConnection, DUPLICATE_SET_COLUMNS,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 != t2.c2 then update set c2 = t2.c2, c2 = t2.c1\n"
              );

        // SET columns must be in target table
        expectCompilationError
            ( dboConnection, COLUMN_NOT_IN_TABLE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 != t2.c2 then update set c5 = t2.c2\n"
              );

        // SET value must be storable in target column.
        expectCompilationError
            ( dboConnection, ILLEGAL_STORAGE,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 != t2.c2 then update set c2 = t2.c5\n"
              );

        //
        // The following syntax is actually good and the statements affect no rows.
        //
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched then delete\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using v2\n" +
              "on t1.c1 = v2.c1\n" +
              "when matched then delete\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 = t2.c2 then delete\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c2 ) values ( t2.c2 )\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched and t2.c2 is null then insert ( c2 ) values ( t2.c2 )\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c1, c2 ) values ( default, t2.c2 )\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c2, c3 ) values ( t2.c2, default )\n"
              );

        // good statement. no rows affected but no warning because sourceTable is not empty
        expectNoWarning
            ( dboConnection,
              "merge into t1\n" +
              "using table( integerList() ) i\n" +
              "on t1.c1 = i.s_r\n" +
              "when matched then delete\n"
              );
        
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched then update set c2 = t2.c3\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 = t2.c2 then update set c2 = t2.c3\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 != t2.c2 then update set c2 = t2.c2\n" +
              "when not matched then insert ( c2 ) values ( t2.c2 )\n"
              );

        // Using a trigger transition table as a source table is probably ok.
        goodStatement
            ( dboConnection,
              "create trigger trig3 after update on t2\n" +
              "referencing old table as old_cor new table as new_cor\n" +
              "for each statement\n" +
              "merge into t1\n" +
              "using new_cor\n" +
              "on t1.c1 = new_cor.c1\n" +
              "when matched then delete\n"
              );
        goodStatement
            ( dboConnection,
              "create trigger trig4 after update on t2\n" +
              "referencing old table as old_cor new table as new_cor\n" +
              "for each statement\n" +
              "merge into t1\n" +
              "using old_cor\n" +
              "on t1.c1 = old_cor.c1\n" +
              "when matched then delete\n"
              );

        // it's probably ok to specify default values for generated columns in MATCHED ... THEN UPDATE
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 != t2.c2 then update set c3 = default, c2 = t2.c2\n"
              );
        

        goodStatement( dboConnection, "drop function illegalFunction" );
        goodStatement( dboConnection, "drop function integerList" );
        goodStatement( dboConnection, "drop view v2" );
        goodStatement( dboConnection, "drop view v1" );
        goodStatement( dboConnection, "drop table t3" );
        goodStatement( dboConnection, "drop table t2" );
        goodStatement( dboConnection, "drop table t1" );
        truncateTriggerHistory();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test the delete action.
     * </p>
     */
    public  void    test_002_deleteAction()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        goodStatement
            ( dboConnection,
              "create table t1_002( c1 int, c2 int, c3 int generated always as ( c1 + c2 ), c1_4 int )" );
        goodStatement
            ( dboConnection,
              "create table t2_002( c1 int, c2 int, c3 int, c4 int, c5 varchar( 5 ) )" );

        // a DELETE action without a matching refinement clause
        vet_002
            (
             dboConnection,
             "merge into t1_002\n" +
             "using t2_002\n" +
             "on 2 * t1_002.c2 = 2 * t2_002.c2\n" +
             "when matched then delete\n",
             4,
             new String[][]
             {
                 { "5", "5", "10", "5" },   
                 { "6", "20", "26", "40" },    
             }
             );
        
        // a DELETE action with a matching refinement clause
        vet_002
            (
             dboConnection,
             "merge into t1_002\n" +
             "using t2_002\n" +
             "on 2 * t1_002.c2 = 2 * t2_002.c2\n" +
             "when matched and c1_4 = 5 then delete\n",
             3,
             new String[][]
             {
                 { "1", "2", "3", "4" },  
                 { "5", "5", "10", "5" },   
                 { "6", "20", "26", "40" },    
             }
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_002" );
        goodStatement( dboConnection, "drop table t1_002" );
        truncateTriggerHistory();
    }
    private void    vet_002
        (
         Connection conn,
         String query,
         int    rowsAffected,
         String[][] expectedResults
         )
        throws Exception
    {
        vet_002( conn, query, rowsAffected, expectedResults, false );
        vet_002( conn, query, rowsAffected, expectedResults, true );
    }
    private void    vet_002
        (
         Connection conn,
         String query,
         int    rowsAffected,
         String[][] expectedResults,
         boolean    useHashJoinStrategy
         )
        throws Exception
    {
        if ( useHashJoinStrategy ) { query = makeHashJoinMerge( query ); }

        populate_002( conn );
        goodUpdate( conn, query, rowsAffected );
        assertResults( conn, "select * from t1_002 order by c1", expectedResults, false );
    }
    private void    populate_002( Connection conn )
        throws Exception
    {
        goodStatement( conn, "delete from t2_002" );
        goodStatement( conn, "delete from t1_002" );

        goodStatement
            ( conn,
              "insert into t1_002( c1, c2, c1_4 ) values ( 1, 2, 4 ), (2, 2, 5), (3, 3, 5), (4, 4, 5), (5, 5, 5), ( 6, 20, 40 )"
              );
        goodStatement
            ( conn,
              "insert into t2_002( c1, c2, c3, c4, c5 ) values ( 1, 2, 3, 4, 'five' ), ( 2, 3, 3, 4, 'five' ), ( 3, 4, 3, 4, 'five' ), ( 4, 200, 300, 400, 'five' )"
              );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test delete action involving subsequent cascaded deletes.
     * </p>
     */
    public  void    test_003_cascadingDeleteAction()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        goodStatement
            ( dboConnection,
              "create table t1_003( c1 int, c2 int primary key, c3 int )" );
        goodStatement
            ( dboConnection,
              "create table t2_003( c4 int, c5 int, c6 int )" );
        goodStatement
            ( dboConnection,
              "create table t3_003( c1 int, c2 int references t1_003( c2 ) on delete cascade, c3 int )" );

        // a cascading DELETE action without a matching refinement clause
        vet_003
            (
             dboConnection,
             "merge into t1_003\n" +
             "using t2_003\n" +
             "on 2 * t1_003.c1 = 2 * t2_003.c4\n" +
             "when matched then delete\n",
             2,
             new String[][]
             {
                 { "-3", "30", "200" },  
                 { "5", "50", "500" },    
                 { "6", "60", "600" },    
             },
             new String[][]
             {
                 { "-3", "30", "300" },  
                 { "5", "50", "500" },    
                 { "6", "60", "600" },    
             }
             );

        // a cascading DELETE action with a matching refinement clause
        vet_003
            (
             dboConnection,
             "merge into t1_003\n" +
             "using t2_003\n" +
             "on 2 * t1_003.c1 = 2 * t2_003.c4\n" +
             "when matched and c3 = 200 then delete\n",
             1,
             new String[][]
             {
                 { "-3", "30", "200" },  
                 { "1", "10", "100" },   
                 { "5", "50", "500" },    
                 { "6", "60", "600" },    
             },
             new String[][]
             {
                 { "-3", "30", "300" },  
                 { "1", "10", "100" },   
                 { "5", "50", "500" },    
                 { "6", "60", "600" },    
             }
             );

        goodStatement( dboConnection, "drop table t3_003" );
        goodStatement( dboConnection, "drop table t2_003" );
        goodStatement( dboConnection, "drop table t1_003" );
        truncateTriggerHistory();
    }
    private void    vet_003
        (
         Connection conn,
         String query,
         int    rowsAffected,
         String[][] expectedT1Results,
         String[][] expectedT3Results
         )
        throws Exception
    {
        vet_003( conn, query, rowsAffected, expectedT1Results, expectedT3Results, false );
        vet_003( conn, query, rowsAffected, expectedT1Results, expectedT3Results, true );
    }
    private void    vet_003
        (
         Connection conn,
         String query,
         int    rowsAffected,
         String[][] expectedT1Results,
         String[][] expectedT3Results,
         boolean    useHashJoinStrategy
         )
        throws Exception
    {
        if ( useHashJoinStrategy ) { query = makeHashJoinMerge( query ); }

        populate_003( conn );
        goodUpdate( conn, query, rowsAffected );
        assertResults( conn, "select * from t1_003 order by c1", expectedT1Results, false );
        assertResults( conn, "select * from t3_003 order by c1", expectedT3Results, false );
    }
    private void    populate_003( Connection conn )
        throws Exception
    {
        goodStatement( conn, "delete from t3_003" );
        goodStatement( conn, "delete from t2_003" );
        goodStatement( conn, "delete from t1_003" );

        goodStatement
            ( conn,
              "insert into t1_003( c1, c2, c3 ) values ( 1, 10, 100 ), (2, 20, 200), ( -3, 30, 200 ), ( 5, 50, 500 ), ( 6, 60, 600 )"
              );
        goodStatement
            ( conn,
              "insert into t2_003( c4, c5, c6 ) values ( 1, 10, 100 ), (2, 20, 200), ( 3, 30, 300 ), ( 4, 40, 400 )"
              );
        goodStatement
            ( conn,
              "insert into t3_003( c1, c2, c3 ) values ( 1, 10, 100 ), (2, 20, 200), ( -3, 30, 300 ), ( 5, 50, 500 ), ( 6, 60, 600 )"
              );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test delete action involving before and after statement triggers.
     * </p>
     */
    public  void    test_004_deleteActionStatementTriggers()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            ( dboConnection,
              "create table t1_004( c1 int, c2 int, c3 int )" );
        goodStatement
            ( dboConnection,
              "create table t2_004( c1 int generated always as identity, c2 int )" );
        goodStatement
            ( dboConnection,
              "create procedure countRows_004\n" +
              "(\n" +
              "    candidateName varchar( 20 ),\n" +
              "    actionString varchar( 20 )\n" +
              ")\n" +
              "language java parameter style java reads sql data\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.countRows'\n"
              );
        goodStatement
            ( dboConnection,
              "create procedure truncateTriggerHistory_004()\n" +
              "language java parameter style java no sql\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.truncateTriggerHistory'\n"
              );
        goodStatement
            ( dboConnection,
              "create function history_004()\n" +
              "returns table\n" +
              "(\n" +
              "    action varchar( 20 ),\n" +
              "    actionValue int\n" +
              ")\n" +
              "language java parameter style derby_jdbc_result_set\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.history'\n"
              );
        goodStatement
            ( dboConnection,
              "create trigger t1_004_del_before\n" +
              "no cascade before delete on t1_004\n" +
              "for each statement\n" +
              "call countRows_004( 't1_004', 'before' )\n"
              );
        goodStatement
            ( dboConnection,
              "create trigger t1_004_del_after\n" +
              "after delete on t1_004\n" +
              "for each statement\n" +
              "call countRows_004( 't1_004', 'after' )\n"
              );

        // a statement-trigger-invoking DELETE action without a matching refinement clause
        vet_004
            (
             dboConnection,
             "merge into t1_004\n" +
             "using t2_004\n" +
             "on 2 * t1_004.c2 = 2 * t2_004.c2\n" +
             "when matched then delete\n",
             4,
             new String[][]
             {
                 { "3", "30", "300" },   
             },
             new String[][]
             {
                 { "before", "5" },  
                 { "after", "1" },   
             }
             );

        // a statement-trigger-invoking DELETE action with a matching refinement clause
        vet_004
            (
             dboConnection,
             "merge into t1_004\n" +
             "using t2_004\n" +
             "on 2 * t1_004.c2 = 2 * t2_004.c2\n" +
             "when matched and c3 = 200 then delete\n",
             1,
             new String[][]
             {
                 { "1", "10", "100" },  
                 { "3", "30", "300" },   
                 { "4", "40", "400" },    
                 { "5", "50", "500" },    
             },
             new String[][]
             {
                 { "before", "5" },  
                 { "after", "4" },   
             }
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop trigger t1_004_del_before" );
        goodStatement( dboConnection, "drop trigger t1_004_del_after" );
        goodStatement( dboConnection, "drop function history_004" );
        goodStatement( dboConnection, "drop procedure truncateTriggerHistory_004" );
        goodStatement( dboConnection, "drop procedure countRows_004" );
        goodStatement( dboConnection, "drop table t2_004" );
        goodStatement( dboConnection, "drop table t1_004" );
        truncateTriggerHistory();
    }
    private void    vet_004
        (
         Connection conn,
         String query,
         int    rowsAffected,
         String[][] expectedT1Results,
         String[][] expectedHistoryResults
         )
        throws Exception
    {
        vet_004( conn, query, rowsAffected, expectedT1Results, expectedHistoryResults, false );
        vet_004( conn, query, rowsAffected, expectedT1Results, expectedHistoryResults, true );
    }
    private void    vet_004
        (
         Connection conn,
         String query,
         int    rowsAffected,
         String[][] expectedT1Results,
         String[][] expectedHistoryResults,
         boolean    useHashJoinStrategy
         )
        throws Exception
    {
        if ( useHashJoinStrategy ) { query = makeHashJoinMerge( query ); }

        populate_004( conn );
        goodUpdate( conn, query, rowsAffected );
        assertResults( conn, "select * from t1_004 order by c1", expectedT1Results, false );
        assertResults( conn, "select * from table( history_004() ) s", expectedHistoryResults, false );
    }
    private void    populate_004( Connection conn )
        throws Exception
    {
        goodStatement( conn, "delete from t2_004" );
        goodStatement( conn, "delete from t1_004" );
        goodStatement( conn, "call truncateTriggerHistory_004()" );

        goodStatement
            ( conn,
              "insert into t1_004( c1, c2, c3 ) values ( 1, 10, 100 ), ( 2, 20, 200 ), ( 3, 30, 300 ), ( 4, 40, 400 ), ( 5, 50, 500 )"
              );
        goodStatement
            ( conn,
              "insert into t2_004( c2 ) values ( 10 ), ( 20 ), ( 40 ), ( 50 ), ( 60 ), ( 70 )"
              );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test delete action involving before and after row triggers.
     * </p>
     */
    public  void    test_005_deleteActionRowTriggers()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            ( dboConnection,
              "create table t1_005( c1 int, c2 int, c3 int )" );
        goodStatement
            ( dboConnection,
              "create table t2_005( c1 int generated always as identity, c2 int )" );
        goodStatement
            ( dboConnection,
              "create procedure addHistoryRow_005\n" +
              "(\n" +
              "    actionString varchar( 20 ),\n" +
              "    actionValue int\n" +
              ")\n" +
              "language java parameter style java reads sql data\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.addHistoryRow'\n"
              );
        goodStatement
            ( dboConnection,
              "create procedure truncateTriggerHistory_005()\n" +
              "language java parameter style java no sql\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.truncateTriggerHistory'\n"
              );
        goodStatement
            ( dboConnection,
              "create function history_005()\n" +
              "returns table\n" +
              "(\n" +
              "    action varchar( 20 ),\n" +
              "    actionValue int\n" +
              ")\n" +
              "language java parameter style derby_jdbc_result_set\n" +
              "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.history'\n"
              );
        goodStatement
            ( dboConnection,
              "create trigger t1_005_del_before\n" +
              "no cascade before delete on t1_005\n" +
              "referencing old as old\n" +
              "for each row\n" +
              "call addHistoryRow_005( 'before', old.c1 )\n" 
              );
        goodStatement
            ( dboConnection,
              "create trigger t1_005_del_after\n" +
              "after delete on t1_005\n" +
              "referencing old as old\n" +
              "for each row\n" +
              "call addHistoryRow_005( 'after', old.c1 )\n"
              );
        goodStatement
            ( dboConnection,
              "insert into t1_005( c1, c2, c3 ) values ( 1, 10, 100 ), ( 2, 20, 200 ), ( 3, 30, 300 ), ( 4, 40, 400 ), ( 5, 50, 500 )"
              );
        goodStatement
            ( dboConnection,
              "insert into t2_005( c2 ) values ( 10 ), ( 20 ), ( 40 ), ( 50 ), ( 60 ), ( 70 )"
              );

        // a row-trigger-invoking DELETE action without a matching refinement clause
        vet_005
            (
             dboConnection,
             "merge into t1_005\n" +
             "using t2_005\n" +
             "on 2 * t1_005.c2 = 2 * t2_005.c2\n" +
             "when matched then delete\n",
             4,
             new String[][]
             {
                 { "3", "30", "300" },   
             },
             new String[][]
             {
                 { "before", "1" },  
                 { "before", "2" },  
                 { "before", "4" },  
                 { "before", "5" },  
                 { "after", "1" },   
                 { "after", "2" },   
                 { "after", "4" },   
                 { "after", "5" },   
             }
             );

        // a row-trigger-invoking DELETE action with a matching refinement clause
        vet_005
            (
             dboConnection,
             "merge into t1_005\n" +
             "using t2_005\n" +
             "on 2 * t1_005.c2 = 2 * t2_005.c2\n" +
             "when matched and c3 = 200 then delete\n",
             1,
             new String[][]
             {
                 { "1", "10", "100" },  
                 { "3", "30", "300" },   
                 { "4", "40", "400" },    
                 { "5", "50", "500" },    
             },
             new String[][]
             {
                 { "before", "2" },  
                 { "after", "2" },   
             }
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop trigger t1_005_del_before" );
        goodStatement( dboConnection, "drop trigger t1_005_del_after" );
        goodStatement( dboConnection, "drop function history_005" );
        goodStatement( dboConnection, "drop procedure truncateTriggerHistory_005" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_005" );
        goodStatement( dboConnection, "drop table t2_005" );
        goodStatement( dboConnection, "drop table t1_005" );
        truncateTriggerHistory();
    }
    private void    vet_005
        (
         Connection conn,
         String query,
         int    rowsAffected,
         String[][] expectedT1Results,
         String[][] expectedHistoryResults
         )
        throws Exception
    {
        vet_005( conn, query, rowsAffected, expectedT1Results, expectedHistoryResults, false );
        vet_005( conn, query, rowsAffected, expectedT1Results, expectedHistoryResults, true );
    }
    private void    vet_005
        (
         Connection conn,
         String query,
         int    rowsAffected,
         String[][] expectedT1Results,
         String[][] expectedHistoryResults,
         boolean    useHashJoinStrategy
         )
        throws Exception
    {
        if ( useHashJoinStrategy ) { query = makeHashJoinMerge( query ); }

        populate_005( conn );
        goodUpdate( conn, query, rowsAffected );
        assertResults( conn, "select * from t1_005 order by c1", expectedT1Results, false );
        assertResults( conn, "select * from table( history_005() ) s", expectedHistoryResults, false );
    }
    private void    populate_005( Connection conn )
        throws Exception
    {
        goodStatement( conn, "delete from t2_005" );
        goodStatement( conn, "delete from t1_005" );
        goodStatement( conn, "call truncateTriggerHistory_005()" );

        goodStatement
            ( conn,
              "insert into t1_005( c1, c2, c3 ) values ( 1, 10, 100 ), ( 2, 20, 200 ), ( 3, 30, 300 ), ( 4, 40, 400 ), ( 5, 50, 500 )"
              );
        goodStatement
            ( conn,
              "insert into t2_005( c2 ) values ( 10 ), ( 20 ), ( 40 ), ( 50 ), ( 60 ), ( 70 )"
              );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test delete action whose source table is a trigger transition table.
     * </p>
     */
    public  void    test_006_deleteWithTransitionTableSource()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            ( dboConnection,
              "create table t1_006( c1 int, c2 int, c3 int generated always as ( c1 + c2 ), c1_4 int )" );
        goodStatement
            ( dboConnection,
              "create table t2_006( c1 int, c2 int, c3 int, c4 int, c5 varchar( 5 ) )" );
        
        //
        // New transition table as source.
        // No matching refinement.
        //
        vet_006
            (
             dboConnection,
             "create trigger trig1_006 after update on t2_006\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_006\n" +
             "using new_cor\n" +
             "on 2 * t1_006.c2 = 2 * new_cor.c2\n" +
             "when matched then delete\n",
             new String[][]
             {
                 { "2", "2", "4", "200" },
                 { "4", "4", "8", "400" }, 
             }
             );

        //
        // New transition table as source.
        // With matching refinement.
        //
        vet_006
            (
             dboConnection,
             "create trigger trig1_006 after update on t2_006\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_006\n" +
             "using new_cor\n" +
             "on 2 * t1_006.c2 = 2 * new_cor.c2\n" +
             "when matched and c1_4 = 300 then delete\n",
             new String[][]
             {
                 { "1", "1", "2", "100" },
                 { "2", "2", "4", "200" },
                 { "4", "4", "8", "400" }, 
             }
             );

        //
        // Old transition table as source.
        // No matching refinement.
        //
        vet_006
            (
             dboConnection,
             "create trigger trig1_006 after update on t2_006\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_006\n" +
             "using old_cor\n" +
             "on 2 * t1_006.c2 = 2 * old_cor.c2\n" +
             "when matched then delete\n",
             new String[][]
             {
                 { "1", "1", "2", "100" },
                 { "3", "3", "6", "300" }, 
             }
             );

        //
        // Old transition table as source.
        // With matching refinement.
        //
        vet_006
            (
             dboConnection,
             "create trigger trig1_006 after update on t2_006\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_006\n" +
             "using old_cor\n" +
             "on 2 * t1_006.c2 = 2 * old_cor.c2\n" +
             "when matched and c1_4 = 200 then delete\n",
             new String[][]
             {
                 { "1", "1", "2", "100" },
                 { "3", "3", "6", "300" }, 
                 { "4", "4", "8", "400" }, 
             }
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_006" );
        goodStatement( dboConnection, "drop table t1_006" );
        truncateTriggerHistory();
    }
    private void    vet_006
        (
         Connection conn,
         String triggerDefinition,
         String[][] expectedT1Results
         )
        throws Exception
    {
        vet_006( conn, triggerDefinition, expectedT1Results, false );
        vet_006( conn, triggerDefinition, expectedT1Results, true );
    }
    private void    vet_006
        (
         Connection conn,
         String triggerDefinition,
         String[][] expectedT1Results,
         boolean    useHashJoinStrategy
         )
        throws Exception
    {
        if ( useHashJoinStrategy ) { triggerDefinition = makeHashJoinMerge( triggerDefinition ); }

        goodStatement( conn, triggerDefinition );
        populate_006( conn );
        goodStatement( conn, "update t2_006 set c2 = -c2" );
        assertResults( conn, "select * from t1_006 order by c1", expectedT1Results, false );
        
        goodStatement( conn, "drop trigger trig1_006" );
    }
    private void    populate_006( Connection conn )
        throws Exception
    {
        goodStatement( conn, "delete from t2_006" );
        goodStatement( conn, "delete from t1_006" );

        goodStatement
            ( conn,
              "insert into t1_006( c1, c2, c1_4 ) values ( 1, 1, 100 ), ( 2, 2, 200 ), ( 3, 3, 300 ), ( 4, 4, 400 )"
              );
        goodStatement
            ( conn,
              "insert into t2_006( c1, c2, c3, c4, c5 ) values ( 1, -1, -10, -100, 'one' ), ( 2, 2, -2, -200, 'two' ), ( 3, -3, -30, -300, 'three' ), ( 4, 4, -40, -400, 'four' )"
              );
    }

    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test insert action with generated columns and defaults.
     * </p>
     */
    public  void    test_007_insertGeneratedColumnsAndDefaults()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_007\n" +
             "(\n" +
             "    c1 int generated always as identity,\n" +
             "    c2 int,\n" +
             "    c3 int generated always as ( c1 + c2 ),\n" +
             "    c1_4 int,\n" +
             "    c5 int default 1000\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_007( c1 int generated always as identity, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t3_007\n" +
             "(\n" +
             "    c1 int generated by default as identity,\n" +
             "    c2 int,\n" +
             "    c3 int generated always as ( c1 + c2 ),\n" +
             "    c1_4 int,\n" +
             "    c5 int default 1000\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create function nop_007( a int ) returns int\n" +
             "language java parameter style java no sql deterministic\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.nop'\n"
             );

        //
        // populate tables
        //
        goodStatement
            (
             dboConnection,
             "insert into t1_007( c2, c1_4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_007( c2, c3, c4, c5 ) values\n" +
             "( -1, -101, -100, 'one' ), ( 2, -201, -200, 'two' ), ( -3, -301, -300, 'three' ), ( 4, -401, -400, 'four' )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t3_007( c2, c1_4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 )\n"
             );

        //
        // Simple insert. Identity column declared ALWAYS. No matching refinement. No DEFAULT keywords.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_007\n" +
             "using t2_007\n" +
             "on t1_007.c2 = t2_007.c2\n" +
             "when not matched then insert ( c2, c1_4 ) values ( 10 * t2_007.c2, t2_007.c3 )\n",
             2
             );
        assertResults
            (
             dboConnection,
             "select * from t1_007 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
                 { "5", "-10", "-5", "-101", "1000" },
                 { "6", "-30", "-24", "-301", "1000" },
             },
             false
             );

        //
        // Identity column declared ALWAYS. With matching refinement and DEFAULT keywords.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_007\n" +
             "using t2_007\n" +
             "on t1_007.c2 = t2_007.c2\n" +
             "when not matched and t2_007.c5 = 'three'\n" +
             "    then insert ( c1, c2, c3, c1_4, c5 ) values ( default, 100 * t2_007.c2, default, t2_007.c3, default )\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_007 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
                 { "5", "-10", "-5", "-101", "1000" },
                 { "6", "-30", "-24", "-301", "1000" },
                 { "7", "-300", "-293", "-301", "1000" },
             },
             false
             );

        //
        // Identity column declared BY DEFAULT. No matching refinement. No DEFAULT keywords.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t3_007\n" +
             "using t2_007\n" +
             "on t3_007.c2 = t2_007.c2\n" +
             "when not matched then insert ( c2, c1_4 ) values ( 10 * t2_007.c2, t2_007.c3 )\n",
             2
             );
        assertResults
            (
             dboConnection,
             "select * from t3_007 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
                 { "5", "-10", "-5", "-101", "1000" },
                 { "6", "-30", "-24", "-301", "1000" },
             },
             false
             );
        
        //
        // Identity column declared BY DEFAULT. With matching refinement and DEFAULT keywords.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t3_007\n" +
             "using t2_007\n" +
             "on t3_007.c2 = t2_007.c2\n" +
             "when not matched and t2_007.c5 = 'three'\n" +
             "    then insert ( c1, c2, c3, c1_4, c5 ) values ( default, 100 * t2_007.c2, default, t2_007.c3, default )\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t3_007 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
                 { "5", "-10", "-5", "-101", "1000" },
                 { "6", "-30", "-24", "-301", "1000" },
                 { "7", "-300", "-293", "-301", "1000" },
             },
             false
             );

        //
        // DEFAULT is the only explicit value allowed for ALWAYS identity columns.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_007\n" +
             "using t2_007\n" +
             "on t1_007.c2 = t2_007.c2\n" +
             "when not matched and t2_007.c5 = 'three'\n" +
             "    then insert ( c1 ) values ( default )\n",
             1
             );
        expectCompilationError
            ( dboConnection, CANT_MODIFY_IDENTITY,
              "merge into t1_007\n" +
              "using t2_007\n" +
              "on t1_007.c2 = t2_007.c2\n" +
              "when not matched and t2_007.c5 = 'three'\n" +
              "    then insert ( c1 ) values ( -1 )\n"
              );
        expectCompilationError
            ( dboConnection, CANT_MODIFY_IDENTITY,
              "merge into t1_007\n" +
              "using t2_007\n" +
              "on t1_007.c2 = t2_007.c2\n" +
              "when not matched and t2_007.c5 = 'three'\n" +
              "    then insert ( c1 ) values ( null )\n"
              );
        assertResults
            (
             dboConnection,
             "select * from t1_007 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
                 { "5", "-10", "-5", "-101", "1000" },
                 { "6", "-30", "-24", "-301", "1000" },
                 { "7", "-300", "-293", "-301", "1000" },
                 { "8", null, null, null, "1000" },
             },
             false
             );
        
        //
        // NULL value not allowed for BY DEFAULT identity columns.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t3_007\n" +
             "using t2_007\n" +
             "on t3_007.c2 = t2_007.c2\n" +
             "when not matched and t2_007.c5 = 'three'\n" +
             "    then insert ( c1 ) values ( default )\n",
             1
             );
        goodUpdate
            (
             dboConnection,
             "merge into t3_007\n" +
             "using t2_007\n" +
             "on t3_007.c2 = t2_007.c2\n" +
             "when not matched and t2_007.c5 = 'three'\n" +
             "    then insert ( c1 ) values ( -1 )\n",
             1
             );
        expectCompilationError
            ( dboConnection, NOT_NULL_VIOLATION,
              "merge into t3_007\n" +
              "using t2_007\n" +
              "on t3_007.c2 = t2_007.c2\n" +
              "when not matched and t2_007.c5 = 'three'\n" +
              "    then insert ( c1 ) values ( null )\n"
              );
        goodUpdate
            (
             dboConnection,
             "merge into t3_007\n" +
             "using t2_007\n" +
             "on t3_007.c2 = t2_007.c2\n" +
             "when not matched and t2_007.c5 = 'three'\n" +
             "    then insert ( c1 ) values ( nop_007( -2 ) )\n",
             1
             );
        expectExecutionError
            ( dboConnection, NOT_NULL_VIOLATION,
              "merge into t3_007\n" +
              "using t2_007\n" +
              "on t3_007.c2 = t2_007.c2\n" +
              "when not matched and t2_007.c5 = 'three'\n" +
              "    then insert ( c1 ) values ( nop_007( null ) )\n"
              );
        assertResults
            (
             dboConnection,
             "select * from t3_007 order by c1",
             new String[][]
             {
                 { "-2", null, null, null, "1000" },
                 { "-1", null, null, null, "1000" },
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
                 { "5", "-10", "-5", "-101", "1000" },
                 { "6", "-30", "-24", "-301", "1000" },
                 { "7", "-300", "-293", "-301", "1000" },
                 { "8", null, null, null, "1000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop function nop_007" );
        goodStatement( dboConnection, "drop table t3_007" );
        goodStatement( dboConnection, "drop table t2_007" );
        goodStatement( dboConnection, "drop table t1_007" );
        truncateTriggerHistory();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test insert action with a check constraint.
     * </p>
     */
    public  void    test_008_insertAndCheckConstraint()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_008\n" +
             "(\n" +
             "    c1 int generated always as identity,\n" +
             "    c2 int,\n" +
             "    c3 int generated always as ( c1 + c2 ),\n" +
             "    c1_4 int,\n" +
             "    c5 int default 1000,\n" +
             "    check( (c1_4 + c3) > -325 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_008( c1 int generated always as identity, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_008( c2, c1_4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_008( c2, c3, c4, c5 ) values\n" +
             "( -1, -101, -100, 'one' ), ( 2, -201, -200, 'two' ), ( -3, -301, -300, 'three' ), ( 4, -401, -400, 'four' )\n"
             );

        //
        // Check constraint violation. Constraint involves a complex expression
        // including a generated column.
        //
        expectExecutionError
            ( dboConnection, CONSTRAINT_VIOLATION,
              "merge into t1_008\n" +
              "using t2_008\n" +
              "on t1_008.c2 = t2_008.c2\n" +
              "when not matched then insert ( c2, c1_4 ) values ( 10 * t2_008.c2, t2_008.c3 )\n"
              );

        //
        // Same constraint but slightly different MERGE statement, which succeeds.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_008\n" +
             "using t2_008\n" +
             "on t1_008.c2 = t2_008.c2\n" +
             "when not matched then insert ( c2, c1_4 ) values ( ( 10 * t2_008.c2 ) + 1, t2_008.c3 )\n",
             2
             );
        assertResults
            (
             dboConnection,
             "select * from t1_008 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
                 { "7", "-9", "-2", "-101", "1000" },
                 { "8", "-29", "-21", "-301", "1000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_008" );
        goodStatement( dboConnection, "drop table t1_008" );
        truncateTriggerHistory();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test insert action with a unique and foreign key constraints.
     * </p>
     */
    public  void    test_009_insertAndUniqueForeignConstraint()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t3_009( c2 int primary key )\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_009\n" +
             "(\n" +
             "    c1 int generated always as identity primary key,\n" +
             "    c2 int unique,\n" +
             "    c3 int generated always as ( c1 + c2 ),\n" +
             "    c1_4 int,\n" +
             "    c5 int default 1000,\n" +
             "    foreign key ( c2 ) references t3_009( c2 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_009( c1 int generated always as identity, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t3_009( c2 ) values ( 1 ), ( 2 ), ( 3 ), ( 4 ), ( -10 ), ( -30 ), ( -100 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_009( c2, c1_4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_009( c2, c3, c4, c5 ) values\n" +
             "( -1, -101, -100, 'one' ), ( 2, -201, -200, 'two' ), ( -3, -301, -300, 'three' ), ( 4, -401, -400, 'four' )\n"
             );

        //
        // Unique constraint violation.
        //
        String  nonRepeatableMerge = 
             "merge into t1_009\n" +
             "using t2_009\n" +
             "on t1_009.c2 = t2_009.c2\n" +
            "when not matched then insert ( c2, c1_4 ) values ( 10 * t2_009.c2, t2_009.c3 )\n";
        goodUpdate
            (
             dboConnection,
             nonRepeatableMerge,
             2
             );
        expectExecutionError
            ( dboConnection, ILLEGAL_DUPLICATE,
              nonRepeatableMerge
              );
        expectExecutionError
            ( dboConnection, ILLEGAL_DUPLICATE,
              nonRepeatableMerge
              );

        //
        // Foreign key violation.
        //
        expectExecutionError
            ( dboConnection, FOREIGN_KEY_VIOLATION,
              "merge into t1_009\n" +
              "using t2_009\n" +
              "on t1_009.c2 = t2_009.c2\n" +
              "when not matched then insert ( c2, c1_4 ) values ( 100 * t2_009.c2, t2_009.c3 )\n"
              );
        assertResults
            (
             dboConnection,
             "select * from t1_009 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
                 { "5", "-10", "-5", "-101", "1000" },
                 { "6", "-30", "-24", "-301", "1000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_009" );
        goodStatement( dboConnection, "drop table t1_009" );
        goodStatement( dboConnection, "drop table t3_009" );
        truncateTriggerHistory();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test insert action with before and after statement level triggers.
     * </p>
     */
    public  void    test_010_insertStatementTriggers()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_010( c1 int generated always as identity, c2 int, c3 int )\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_010( c1 int generated always as identity, c2 int )\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure countRows_010\n" +
             "(\n" +
             "    candidateName varchar( 20 ),\n" +
             "    actionString varchar( 20 )\n" +
             ")\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.countRows'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function history_010()\n" +
             "returns table\n" +
             "(\n" +
             "    action varchar( 20 ),\n" +
             "    actionValue int\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.history'\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_010( c2, c3 ) values ( 10, 100 ), ( -20, 200 ), ( 30, 300 ), ( -40, 400 ), ( 50, 500 ), ( -60, 600 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_010( c2 ) values ( 10 ), ( 20 ), ( 30 ), ( 50 ), ( 100 )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_010_del_before\n" +
             "no cascade before insert on t1_010\n" +
             "for each statement\n" +
             "call countRows_010( 't1_010', 'before' )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_010_del_after\n" +
             "after insert on t1_010\n" +
             "for each statement\n" +
             "call countRows_010( 't1_010', 'after' )\n"
             );

        //
        // Verify the firing of before and after triggers.
        //
        //
        truncateTriggerHistory();
        goodUpdate
            (
             dboConnection,
             "merge into t1_010\n" +
             "using t2_010\n" +
             "on t1_010.c2 = t2_010.c2\n" +
             "when not matched then insert ( c2, c3 ) values ( 10 * t2_010.c2, t2_010.c1 )\n",
             2
             );
        assertResults
            (
             dboConnection,
             "select * from t1_010 order by c1",
             new String[][]
             {
                 { "1", "10", "100" },
                 { "2", "-20", "200" },
                 { "3", "30", "300" },
                 { "4", "-40", "400" },
                 { "5", "50", "500" },
                 { "6", "-60", "600" },
                 { "7", "200", "2" },
                 { "8", "1000", "5" },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "select * from table( history_010() ) s",
             new String[][]
             {
                 { "before", "6" },
                 { "after", "8" },
             },
             false
             );

        
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_010" );
        goodStatement( dboConnection, "drop table t1_010" );
        goodStatement( dboConnection, "drop procedure countRows_010" );
        goodStatement( dboConnection, "drop function history_010" );
        truncateTriggerHistory();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test insert action with before and after row level triggers.
     * </p>
     */
    public  void    test_011_insertRowTriggers()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_011( c1 int generated always as identity, c2 int, c3 int )\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_011( c1 int generated always as identity, c2 int )\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_011\n" +
             "(\n" +
             "    actionString varchar( 20 ),\n" +
             "    actionValue int\n" +
             ")\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.addHistoryRow'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function history_011()\n" +
             "returns table\n" +
             "(\n" +
             "    action varchar( 20 ),\n" +
             "    actionValue int\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.history'\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_011( c2, c3 ) values ( 10, 100 ), ( -20, 200 ), ( 30, 300 ), ( -40, 400 ), ( 50, 500 ), ( -60, 600 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_011( c2 ) values ( 10 ), ( 20 ), ( 30 ), ( 50 ), ( 100 )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_011_ins_before\n" +
             "no cascade before insert on t1_011\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_011( 'before', new.c1 )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_011_ins_after\n" +
             "after insert on t1_011\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_011( 'after', new.c1 )\n"
             );

        //
        // Verify the firing of before and after triggers.
        //
        //
        truncateTriggerHistory();
        goodUpdate
            (
             dboConnection,
             "merge into t1_011\n" +
             "using t2_011\n" +
             "on t1_011.c2 = t2_011.c2\n" +
             "when not matched then insert ( c2, c3 ) values ( 10 * t2_011.c2, t2_011.c1 )\n",
             2
             );
        assertResults
            (
             dboConnection,
             "select * from t1_011 order by c1",
             new String[][]
             {
                 { "1", "10", "100" },
                 { "2", "-20", "200" },
                 { "3", "30", "300" },
                 { "4", "-40", "400" },
                 { "5", "50", "500" },
                 { "6", "-60", "600" },
                 { "7", "200", "2" },
                 { "8", "1000", "5" },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "select * from table( history_011() ) s",
             new String[][]
             {
                 { "before", "7" },
                 { "before", "8" },
                 { "after", "7" },
                 { "after", "8" },
             },
             false
             );

        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_011" );
        goodStatement( dboConnection, "drop table t1_011" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_011" );
        goodStatement( dboConnection, "drop function history_011" );
        truncateTriggerHistory();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test insert action whose source table is a trigger transition table.
     * </p>
     */
    public  void    test_012_insertWithTransitionTableSource()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_012( c1 int, c2 int, c3 int generated always as ( c1 + c2 ), c1_4 int )\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_012( c1 int, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );

        String  update = "update t2_012 set c2 = -c2";

        //
        // NEW transition table as source table.
        //
        populate_012( dboConnection );
        goodStatement
            (
             dboConnection,
             "create trigger trig1_012 after update on t2_012\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_012\n" +
             "using new_cor\n" +
             "on t1_012.c2 = new_cor.c2\n" +
             "when not matched then insert ( c1, c2, c1_4 ) values ( new_cor.c1, new_cor.c2, new_cor.c4 )\n"
             );
        goodUpdate( dboConnection, update, 4 );
        assertResults
            (
             dboConnection,
             "select * from t1_012 order by c1, c2",
             new String[][]
             {
                 { "1", "1", "2", "100" },
                 { "2", "-2", "0", "-200" },
                 { "2", "2", "4", "200" },
                 { "3", "3", "6", "300" },
                 { "4", "-4", "0", "-400" },
                 { "4", "4", "8", "400" },
             },
             false
             );
        goodStatement( dboConnection, "drop trigger trig1_012" );

        //
        // OLD transition table as source table.
        //
        populate_012( dboConnection );
        goodStatement
            (
             dboConnection,
             "create trigger trig1_012 after update on t2_012\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_012\n" +
             "using old_cor\n" +
             "on t1_012.c2 = old_cor.c2\n" +
             "when not matched then insert ( c1, c2, c1_4 ) values ( old_cor.c1, old_cor.c2, old_cor.c4 )\n"
             );
        goodUpdate( dboConnection, update, 4 );
        assertResults
            (
             dboConnection,
             "select * from t1_012 order by c1, c2",
             new String[][]
             {
                 { "1", "-1", "0", "-100" },
                 { "1", "1", "2", "100" },
                 { "2", "2", "4", "200" },
                 { "3", "-3", "0", "-300" },
                 { "3", "3", "6", "300" },
                 { "4", "4", "8", "400" },
             },
             false
             );
        goodStatement( dboConnection, "drop trigger trig1_012" );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_012" );
        goodStatement( dboConnection, "drop table t1_012" );
    }
    private void    populate_012( Connection conn ) throws Exception
    {
        goodStatement( conn, "delete from t1_012" );
        goodStatement( conn, "delete from t2_012" );
        goodStatement
            (
             conn,
             "insert into t1_012( c1, c2, c1_4 ) values ( 1, 1, 100 ), ( 2, 2, 200 ), ( 3, 3, 300 ), ( 4, 4, 400 )\n"
             );
        goodStatement
            (
             conn,
"insert into t2_012( c1, c2, c3, c4, c5 ) values ( 1, -1, -10, -100, 'one' ), ( 2, 2, -2, -200, 'two' ), ( 3, -3, -30, -300, 'three' ), ( 4, 4, -40, -400, 'four' )\n"
             );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test combined insert and delete actions.
     * </p>
     */
    public  void    test_013_insertAndDelete()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_013\n" +
             "(\n" +
             "    c1 int generated always as identity,\n" +
             "    c2 int,\n" +
             "    c3 int generated always as ( c1 + c2 ),\n" +
             "    c1_4 int,\n" +
             "    c5 int default 1000\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_013( c1 int generated always as identity, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_013( c2, c1_4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_013( c2, c3, c4, c5 ) values\n" +
             "( -1, -101, -100, 'one' ), ( 2, -201, -200, 'two' ), ( -3, -301, -300, 'three' ), ( 4, -401, -400, 'four' )\n"
             );

        //
        // INSERT first then DELETE
        //
                goodUpdate
            (
             dboConnection,
             "merge into t1_013\n" +
             "using t2_013\n" +
             "on t1_013.c2 = t2_013.c2\n" +
             "when not matched then insert ( c2, c1_4 ) values ( 10 * t2_013.c2, t2_013.c3 )\n" +
             "when matched and c1_4 = 200 then delete\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t1_013 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
                 { "5", "-10", "-5", "-101", "1000" },
                 { "6", "-30", "-24", "-301", "1000" },
             },
             false
             );

        //
        // DELETE first then INSERT
        //
                goodUpdate
            (
             dboConnection,
             "merge into t1_013\n" +
             "using t2_013\n" +
             "on t1_013.c2 = t2_013.c2\n" +
             "when matched and c1_4 = 400 then delete\n" +
             "when not matched then insert ( c2, c1_4 ) values ( 10 * t2_013.c2, t2_013.c3 )\n",
             4
             );
        assertResults
            (
             dboConnection,
             "select * from t1_013 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "5", "-10", "-5", "-101", "1000" },
                 { "6", "-30", "-24", "-301", "1000" },
                 { "7", "-10", "-3", "-101", "1000" },
                 { "8", "20", "28", "-201", "1000" },
                 { "9", "-30", "-21", "-301", "1000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_013" );
        goodStatement( dboConnection, "drop table t1_013" );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test basic update action.
     * </p>
     */
    public  void    test_014_basicUpdate()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_014\n" +
             "(\n" +
             "    c1 int generated always as identity,\n" +
             "    c2 int,\n" +
             "    c3 int generated always as ( c1 + c2 ),\n" +
             "    c1_4 int,\n" +
             "    c5 int default 1000\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_014( c1 int generated always as identity, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_014( c2, c1_4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_014( c2, c3, c4, c5 ) values\n" +
             "( -1, -101, -100, 'one' ), ( 2, -201, -200, 'two' ), ( -3, -301, -300, 'three' ), ( 4, -401, -400, 'four' )\n"
             );

        //
        // This case tracks a bug in which bind() wasn't poking the table name
        // into resolved column references. This resulted in an NPE.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_014\n" +
             "using t2_014\n" +
             "on t1_014.c2 = t2_014.c2\n" +
             "when matched then update set c1_4 = (2 * c1_4) / 2\n",
             2
             );

        //
        // Update with a matching refinement. Don't touch the generated column.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_014\n" +
             "using t2_014\n" +
             "on t1_014.c2 = t2_014.c2\n" +
             "when matched and c1_4 = 200 then update set c5 = 10 * t2_014.c1\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_014 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "20" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
             },
             false
             );

        //
        // Update with a default for the generated column.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_014\n" +
             "using t2_014\n" +
             "on t1_014.c1 = t2_014.c1\n" +
             "when matched and t1_014.c2 != t2_014.c2 then update set c3 = default, c2 = t2_014.c2\n",
             2
             );
        assertResults
            (
             dboConnection,
             "select * from t1_014 order by c1",
             new String[][]
             {
                 { "1", "-1", "0", "100", "1000" },
                 { "2", "2", "4", "200", "20" },
                 { "3", "-3", "0", "300", "1000" },
                 { "4", "4", "8", "400", "1000" },
             },
             false
             );

        //
        // Update with a default for the generated column and the column which has an explicit default.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_014\n" +
             "using t2_014\n" +
             "on t1_014.c2 = t2_014.c2\n" +
             "when matched then update set c3 = default, c2 = 10 * t2_014.c2, c5 = default\n",
             4
             );
        assertResults
            (
             dboConnection,
             "select * from t1_014 order by c1",
             new String[][]
             {
                 { "1", "-10", "-9", "100", "1000" },
                 { "2", "20", "22", "200", "1000" },
                 { "3", "-30", "-27", "300", "1000" },
                 { "4", "40", "44", "400", "1000" },
             },
             false
             );

        //
        // The following statement fails because of derby-6414. Revisit this
        // case when that bug is fixed.
        //
        expectCompilationError
            ( dboConnection, CANT_MODIFY_IDENTITY,
              "merge into t1_014\n" +
              "using t2_014\n" +
              "on t1_014.c2 = t2_014.c2\n" +
              "when matched then update set c1 = default, c3 = default, c2 = 2 * t2_014.c2, c5 = default\n"
              );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_014" );
        goodStatement( dboConnection, "drop table t1_014" );
    }
    
    /**
     * <p>
     * Verify that the UPDATE actions of MERGE statements behave like ordinary UPDATE statements
     * in their treatment of DEFAULT values for identity columns. Derby's behavior here is wrong but
     * we would like it to be consistent. We need to correct the MERGE behavior when we correct
     * the behavior for standalone UPDATE statements.
     * </p>
     */
    public  void    test_015_bug_6414()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_bug_6414( a int generated always as identity, b int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_bug_6414( a int generated by default as identity, b int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t3_bug_6414( a int generated always as identity, b int )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_bug_6414( a, b ) values ( default, 100 )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_bug_6414( a, b ) values ( default, 100 )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t3_bug_6414( a, b ) values ( default, 100 )"
             );

        //
        // Derby, incorrectly, won't let you update a GENERATED ALWAYS identity
        // column to the next DEFAULT value, i.e., the next value from the
        // sequence generator.
        //
        expectCompilationError
            ( dboConnection, CANT_MODIFY_IDENTITY,
              "update t1_bug_6414 set a = default, b = -b"
              );
        expectCompilationError
            ( dboConnection, CANT_MODIFY_IDENTITY,
              "merge into t1_bug_6414\n" +
              "using t3_bug_6414\n" +
              "on t1_bug_6414.a = t3_bug_6414.a\n" +
              "when matched then update set a = default, b = -t3_bug_6414.b\n"
              );


        //
        // Derby, incorrectly, won't let you update a GENERATED BY DEFAULT identity
        // column to the next DEFAULT value, i.e., the next value from the
        // sequence generator.
        //
        expectExecutionError
            ( dboConnection, NOT_NULL_VIOLATION,
              "update t2_bug_6414 set a = default, b = -b\n"
              );
        expectExecutionError
            ( dboConnection, NOT_NULL_VIOLATION,
              "merge into t2_bug_6414\n" +
              "using t3_bug_6414\n" +
              "on t2_bug_6414.a = t3_bug_6414.a\n" +
              "when matched then update set a = default, b = -t3_bug_6414.b\n"
              );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t3_bug_6414" );
        goodStatement( dboConnection, "drop table t2_bug_6414" );
        goodStatement( dboConnection, "drop table t1_bug_6414" );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test before and after statement level triggers fired by MERGE statements.
     * </p>
     */
    public  void    test_016_updateWithStatementTriggers()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_016( c1 int, c2 int, c3 int )\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_016( c1 int generated always as identity, c2 int )\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure sumColumn_016\n" +
             "(\n" +
             "    candidateName varchar( 20 ),\n" +
             "    columnName varchar( 20 ),\n" +
             "    actionString varchar( 20 )\n" +
             ")\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.sumColumn'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure truncateTriggerHistory_016()\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.truncateTriggerHistory'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function history_016()\n" +
             "returns table\n" +
             "(\n" +
             "    action varchar( 20 ),\n" +
             "    actionValue int\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.history'\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_016_upd_before\n" +
             "no cascade before update on t1_016\n" +
             "for each statement\n" +
             "call sumColumn_016( 't1_016', 'c3', 'before' )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_016_upd_after\n" +
             "after update on t1_016\n" +
             "for each statement\n" +
             "call sumColumn_016( 't1_016', 'c3', 'after' )\n"
             );
        goodStatement
            (
             dboConnection,
             "call truncateTriggerHistory_016()\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_016( c1, c2, c3 ) values ( 1, 10, 100 ), ( 2, 20, 200 ), ( 3, 30, 300 ), ( 4, 40, 400 ), ( 5, 50, 500 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_016( c2 ) values ( 10 ), ( 20 ), ( 40 ), ( 50 ), ( 60 ), ( 70 )\n"
             );

        //
        // UPDATE without matching refinement.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_016\n" +
             "using t2_016\n" +
             "on   t1_016.c2 =   t2_016.c2\n" +
             "when matched then update set c3 = -100\n",
             4
             );
        assertResults
            (
             dboConnection,
             "select * from t1_016 order by c1",
             new String[][]
             {
                 { "1", "10", "-100" },
                 { "2", "20", "-100" },
                 { "3", "30", "300" },
                 { "4", "40", "-100" },
                 { "5", "50", "-100" },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "select * from table( history_016() ) s",
             new String[][]
             {
                 { "before", "1500" },
                 { "after", "-100" },
             },
             false
             );

        //
        // UPDATE with matching refinement.
        //
        goodStatement
            (
             dboConnection,
             "update t1_016 set c3 = 100 * c1"
             );
        goodStatement
            (
             dboConnection,
             "call truncateTriggerHistory_016()"
             );
        goodUpdate
            (
             dboConnection,
             "merge into t1_016\n" +
             "using t2_016\n" +
             "on   t1_016.c2 =   t2_016.c2\n" +
             "when matched and c3 = 200 then update set c3 = -200\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_016 order by c1",
             new String[][]
             {
                 { "1", "10", "100" },
                 { "2", "20", "-200" },
                 { "3", "30", "300" },
                 { "4", "40", "400" },
                 { "5", "50", "500" },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "select * from table( history_016() ) s",
             new String[][]
             {
                 { "before", "1500" },
                 { "after", "1100" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t1_016" );
        goodStatement( dboConnection, "drop table t2_016" );
        goodStatement( dboConnection, "drop procedure truncateTriggerHistory_016" );
        goodStatement( dboConnection, "drop procedure sumColumn_016" );
        goodStatement( dboConnection, "drop function history_016" );
        truncateTriggerHistory();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test before and after row level triggers fired by MERGE statements.
     * </p>
     */
    public  void    test_017_updateWithRowTriggers()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_017( c1 int, c2 int, c3 int )\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_017( c1 int generated always as identity, c2 int )\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_017\n" +
             "(\n" +
             "    actionString varchar( 20 ),\n" +
             "    actionValue int\n" +
             ")\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.addHistoryRow'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure truncateTriggerHistory_017()\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.truncateTriggerHistory'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function history_017()\n" +
             "returns table\n" +
             "(\n" +
             "    action varchar( 20 ),\n" +
             "    actionValue int\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.history'\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_017_upd_before\n" +
             "no cascade before update on t1_017\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_017( 'before', old.c1 )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_017_upd_after\n" +
             "after update on t1_017\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_017( 'after', old.c1 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_017( c1, c2, c3 ) values ( 1, 10, 100 ), ( 2, 20, 200 ), ( 3, 30, 300 ), ( 4, 40, 400 ), ( 5, 50, 500 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_017( c2 ) values ( 10 ), ( 20 ), ( 40 ), ( 50 ), ( 60 ), ( 70 )\n"
             );

        //
        // UPDATE without matching refinement.
        //
        goodStatement
            ( dboConnection,
              "call truncateTriggerHistory_017()"
              );
        goodUpdate
            (
             dboConnection,
             "merge into t1_017\n" +
             "using t2_017\n" +
             "on t1_017.c2 = t2_017.c2\n" +
             "when matched then update set c3 = -100\n",
             4
             );
        assertResults
            (
             dboConnection,
             "select * from t1_017 order by c1",
             new String[][]
             {
                 { "1", "10", "-100" },
                 { "2", "20", "-100" },
                 { "3", "30", "300" },
                 { "4", "40", "-100" },
                 { "5", "50", "-100" },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "select * from table( history_017() ) s",
             new String[][]
             {
                 { "before", "1" },
                 { "before", "2" },
                 { "before", "4" },
                 { "before", "5" },
                 { "after", "1" },
                 { "after", "2" },
                 { "after", "4" },
                 { "after", "5" },
             },
             false
             );

        //
        // UPDATE with matching refinement.
        //
        goodStatement
            ( dboConnection,
              "update t1_017 set c3 = 100 * c1"
              );
        goodStatement
            ( dboConnection,
              "call truncateTriggerHistory_017()"
              );
        goodUpdate
            (
             dboConnection,
             "merge into t1_017\n" +
             "using t2_017\n" +
             "on t1_017.c2 = t2_017.c2\n" +
             "when matched and c3 = 200 then update set c3 = -200\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_017 order by c1",
             new String[][]
             {
                 { "1", "10", "100" },
                 { "2", "20", "-200" },
                 { "3", "30", "300" },
                 { "4", "40", "400" },
                 { "5", "50", "500" },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "select * from table( history_017() ) s",
             new String[][]
             {
                 { "before", "2" },
                 { "after", "2" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t1_017" );
        goodStatement( dboConnection, "drop table t2_017" );
        goodStatement( dboConnection, "drop procedure truncateTriggerHistory_017" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_017" );
        goodStatement( dboConnection, "drop function history_017" );
        truncateTriggerHistory();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test MERGE statements with UPDATE actions whose source tables are
     * trigger transition tables.
     * </p>
     */
    public  void    test_018_updateFromTriggerTransitionTables()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_018( c1 int, c2 int, c3 int generated always as ( c1 + c2 ), c1_4 int )\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_018( c1 int, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );

        //
        // Source table is new transition table. No matching refinement.
        //
        vet_018
            (
             dboConnection,
             "create trigger trig1_018 after update on t2_018\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_018\n" +
             "using new_cor\n" +
             "on t1_018.c2 = new_cor.c2\n" +
             "when matched then update set c1_4 = 2 * c1_4\n",
             new String[][]
             {
                 { "1", "1", "2", "200" },
                 { "2", "2", "4", "200" },
                 { "3", "3", "6", "600" },
                 { "4", "4", "8", "400" },
             }
             );

        //
        // Source table is new transition table. With matching refinement.
        //
        vet_018
            (
             dboConnection,
             "create trigger trig1_018 after update on t2_018\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_018\n" +
             "using new_cor\n" +
             "on   t1_018.c2 =   new_cor.c2\n" +
             "when matched and c1_4 = 300 then update set c1_4 = 2 * c1_4\n",
             new String[][]
             {
                 { "1", "1", "2", "100" },
                 { "2", "2", "4", "200" },
                 { "3", "3", "6", "600" },
                 { "4", "4", "8", "400" },
             }
             );

        //
        // Source table is old transition table. No matching refinement.
        //
        vet_018
            (
             dboConnection,
             "create trigger trig1_018 after update on t2_018\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_018\n" +
             "using old_cor\n" +
             "on t1_018.c2 = old_cor.c2\n" +
             "when matched then update set c1_4 = 2 * c1_4\n",
             new String[][]
             {
                 { "1", "1", "2", "100" },
                 { "2", "2", "4", "400" },
                 { "3", "3", "6", "300" },
                 { "4", "4", "8", "800" },
             }
             );

        //
        // Source table is old transition table. With matching refinement.
        //
        vet_018
            (
             dboConnection,
             "create trigger trig1_018 after update on t2_018\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_018\n" +
             "using old_cor\n" +
             "on t1_018.c2 = old_cor.c2\n" +
             "when matched and c1_4 = 200 then update set c1_4 = 2 * c1_4\n",
             new String[][]
             {
                 { "1", "1", "2", "100" },
                 { "2", "2", "4", "400" },
                 { "3", "3", "6", "300" },
                 { "4", "4", "8", "400" },
             }
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t1_018" );
        goodStatement( dboConnection, "drop table t2_018" );
    }
    private void    vet_018
        (
         Connection conn,
         String triggerDefinition,
         String[][] expectedResults
         )
        throws Exception
    {
        populate_018( conn );
        goodStatement( conn, triggerDefinition );
        goodUpdate( conn, "update t2_018 set c2 = -c2", 4 );
        assertResults( conn, "select * from t1_018 order by c1", expectedResults, false );
        goodStatement( conn, "drop trigger trig1_018" );
    }
    private void    populate_018( Connection conn )
        throws Exception
    {
        goodStatement( conn, "delete from t2_018" );
        goodStatement( conn, "delete from t1_018" );

        goodStatement
            ( conn,
              "insert into t1_018( c1, c2, c1_4 ) values ( 1, 1, 100 ), ( 2, 2, 200 ), ( 3, 3, 300 ), ( 4, 4, 400 )\n"
              );
        goodStatement
            ( conn,
"insert into t2_018( c1, c2, c3, c4, c5 ) values ( 1, -1, -10, -100, 'one' ), ( 2, 2, -2, -200, 'two' ), ( 3, -3, -30, -300, 'three' ), ( 4, 4, -40,    -400, 'four' )\n"
              );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test combined insert, update, delete actions.
     * </p>
     */
    public  void    test_019_insertUpdateDelete()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_019\n" +
             "(\n" +
             "    c1 int,\n" +
             "    c2 int,\n" +
             "    c3 int generated always as ( c1 + c2 ),\n" +
             "    c1_4 int,\n" +
             "    c5 int default 1000\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_019( c1 int generated always as identity, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_019( c2, c3, c4, c5 ) values\n" +
             "( -1, -101, -100, 'one' ), ( 2, -201, -200, 'two' ), ( -3, -301, -300, 'three' ), ( 4, -401, -400, 'four' )\n"
             );

        //
        // Try the WHEN [ NOT ] MATCHED clauses in various orders.
        //
        vet_019
            (
             dboConnection,
             "merge into t1_019\n" +
             "using t2_019\n" +
             "on t1_019.c2 = t2_019.c2\n" +
             "when not matched then insert ( c2, c1_4 ) values ( 10 * t2_019.c2, t2_019.c3 )\n" +
             "when matched and c1_4 = 200 then delete\n" +
             "when matched and c1_4 = 400 then update set c1_4 = t2_019.c4, c5 = 2 * t2_019.c1\n"
             );
        vet_019
            (
             dboConnection,
             "merge into t1_019\n" +
             "using t2_019\n" +
             "on t1_019.c2 = t2_019.c2\n" +
             "when matched and c1_4 = 200 then delete\n" +
             "when matched and c1_4 = 400 then update set c1_4 = t2_019.c4, c5 = 2 * t2_019.c1\n" +
             "when not matched then insert ( c2, c1_4 ) values ( 10 * t2_019.c2, t2_019.c3 )\n"
             );
        vet_019
            (
             dboConnection,
             "merge into t1_019\n" +
             "using t2_019\n" +
             "on t1_019.c2 = t2_019.c2\n" +
             "when matched and c1_4 = 400 then update set c1_4 = t2_019.c4, c5 = 2 * t2_019.c1\n" +
             "when not matched then insert ( c2, c1_4 ) values ( 10 * t2_019.c2, t2_019.c3 )\n" +
             "when matched and c1_4 = 200 then delete\n"
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_019" );
        goodStatement( dboConnection, "drop table t1_019" );
    }
    private void    vet_019
        (
         Connection conn,
         String mergeStatement
         )
        throws Exception
    {
        populate_019( conn );
        goodUpdate( conn, mergeStatement, 4 );
        assertResults
            (
             conn,
             "select * from t1_019 order by c2",
             new String[][]
             {
                 { null, "-30", null, "-301", "1000" },
                 { null, "-10", null, "-101", "1000" },
                 { "1", "1", "2", "100", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "-400", "8" },
                 { "5", "5", "10", "500", "1000" },
             },
             false
             );
    }
    private void    populate_019( Connection conn ) throws Exception
    {
        goodStatement( conn, "delete from t1_019" );
        goodStatement
            (
             conn,
             "insert into t1_019( c1, c2, c1_4 ) values ( 1, 1, 100 ), ( 2, 2, 200 ), ( 3, 3, 300 ), ( 4, 4, 400 ), ( 5, 5, 500 )\n"
             );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test check constraints fired by UPDATE actions.
     * </p>
     */
    public  void    test_020_updateWithCheckConstraint()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_020\n" +
             "(\n" +
             "    c1 int generated always as identity,\n" +
             "    c2 int,\n" +
             "    c3 int generated always as ( c1 + c2 ),\n" +
             "    c1_4 int,\n" +
             "    c5 int default 1000,\n" +
             "    check( c1_4 > 2 * c3 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_020( c1 int generated always as identity, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_020( c2, c1_4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_020( c2, c3, c4, c5 ) values\n" +
             "( -1, -101, -100, 'one' ), ( 2, -201, -200, 'two' ), ( -3, -301, -300, 'three' ), ( 4, -401, -400, 'four' )\n"
             );

        //
        // Fail the constraint.
        //
        expectExecutionError
            ( dboConnection, CONSTRAINT_VIOLATION,
              "merge into t1_020\n" +
              "using t2_020\n" +
              "on t1_020.c2 = t2_020.c2\n" +
              "when matched then update set c1_4 = -c1_4\n"
              );

        //
        // Pass the constraint.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_020\n" +
             "using t2_020\n" +
             "on t1_020.c2 = t2_020.c2\n" +
             "when matched then update set c1_4 = 2 * c1_4\n",
             2
             );
        assertResults
            (
             dboConnection,
             "select * from t1_020 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "400", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "800", "1000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_020" );
        goodStatement( dboConnection, "drop table t1_020" );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test foreign key constraints with check constraints fired by UPDATE actions.
     * The CHECK constraint is satisfied but the foreign key is not.
     * </p>
     */
    public  void    test_021_updateWithForeignAndCheckConstraint()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t3_021\n" +
             "(\n" +
             "    c2 int,\n" +
             "    c4 int,\n" +
             "    primary key( c2, c4 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_021\n" +
             "(\n" +
             "    c1 int generated always as identity,\n" +
             "    c2 int,\n" +
             "    c3 int generated always as ( c1 + c2 ),\n" +
             "    c1_4 int,\n" +
             "    c5 int default 1000,\n" +
             "    check( c1_4 > 2 * c3 ),\n" +
             "    foreign key ( c2, c1_4 ) references t3_021( c2, c4 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_021( c1 int generated always as identity, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t3_021( c2, c4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 ), ( 4, 500 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_021( c2, c1_4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_021( c2, c3, c4, c5 ) values\n" +
             "( -1, -101, -100, 'one' ), ( 2, -201, -200, 'two' ), ( -3, -301, -300, 'three' ), ( 4, -401, -400, 'four' )\n"
             );

        //
        // Foreign key violation.
        //
        expectExecutionError
            ( dboConnection, FOREIGN_KEY_VIOLATION,
              "merge into t1_021\n" +
              "using t2_021\n" +
              "on t1_021.c2 = t2_021.c2\n" +
              "when matched and c1_4 = 400 then update set c1_4 = 600\n"
              );

        //
        // Successful update.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_021\n" +
             "using t2_021\n" +
             "on t1_021.c2 = t2_021.c2\n" +
             "when matched and c1_4 = 400 then update set c1_4 = 500\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_021 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "500", "1000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_021" );
        goodStatement( dboConnection, "drop table t1_021" );
        goodStatement( dboConnection, "drop table t3_021" );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test primary key constraints with check constraints fired by UPDATE actions.
     * The CHECK constraint is satisfied but the foreign key is not.
     * </p>
     */
    public  void    test_022_updateWithForeignPrimaryAndCheckConstraint()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t3_022\n" +
             "(\n" +
             "    c2 int,\n" +
             "    c4 int,\n" +
             "    primary key( c2, c4 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_022\n" +
             "(\n" +
             "    c1 int generated always as identity,\n" +
             "    c2 int,\n" +
             "    c3 int generated always as ( c1 + c2 ),\n" +
             "    c1_4 int primary key,\n" +
             "    c5 int default 1000,\n" +
             "    check( c1_4 > 2 * c3 ),\n" +
             "    foreign key ( c2, c1_4 ) references t3_022( c2, c4 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_022( c1 int generated always as identity, c2 int, c3 int, c4 int, c5 varchar( 5 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t3_022( c2, c4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 ), ( 4, 300 ), ( 4, 500 ), ( 5, 500 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_022( c2, c1_4 ) values ( 1, 100 ), (2, 200 ), ( 3, 300 ), ( 4, 400 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_022( c2, c3, c4, c5 ) values\n" +
             "( -1, -101, -100, 'one' ), ( 2, -201, -200, 'two' ), ( -3, -301, -300, 'three' ), ( 4, -401, -400, 'four' )\n"
             );

        //
        // Violate primary key but not foreign key or CHECK constraint.
        //
        expectExecutionError
            ( dboConnection, ILLEGAL_DUPLICATE,
              "merge into t1_022\n" +
              "using t2_022\n" +
              "on t1_022.c2 = t2_022.c2\n" +
              "when matched and c1_4 = 400 then update set c1_4 = 300\n"
              );

        //
        // Successfully update primary key.
        //
        goodUpdate
            (
             dboConnection,
             "merge into t1_022\n" +
             "using t2_022\n" +
             "on t1_022.c2 = t2_022.c2\n" +
             "when matched and c1_4 = 400 then update set c1_4 = 500\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_022 order by c1",
             new String[][]
             {
                 { "1", "1", "2", "100", "1000" },
                 { "2", "2", "4", "200", "1000" },
                 { "3", "3", "6", "300", "1000" },
                 { "4", "4", "8", "500", "1000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_022" );
        goodStatement( dboConnection, "drop table t1_022" );
        goodStatement( dboConnection, "drop table t3_022" );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test correlation names in MERGE statements.
     * </p>
     */
    public  void    test_023_correlationNames()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_023\n" +
             "(\n" +
             "    a_public int primary key,\n" +
             "    b_select_t1_ruth int,\n" +
             "    c_select_t1_alice int,\n" +
             "    d_select_t1_frank int,\n" +
             "    e_update_t1_ruth int,\n" +
             "    f_update_t1_alice int,\n" +
             "    g_update_t1_frank int,\n" +
             "    h_select_t1_ruth generated always as ( a_public )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_023\n" +
             "(\n" +
             "    a_public int primary key,\n" +
             "    b_select_t2_ruth int,\n" +
             "    c_select_t2_alice int,\n" +
             "    d_select_t2_frank int,\n" +
             "    e_select_t2_ruth generated always as ( a_public )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t3_023\n" +
             "(\n" +
             "    a int primary key,\n" +
             "    b int,\n" +
             "    c int,\n" +
             "    d int,\n" +
             "    e int,\n" +
             "    f int,\n" +
             "    g int,\n" +
             "    h generated always as ( a )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t4_023\n" +
             "(\n" +
             "    a int primary key,\n" +
             "    b int,\n" +
             "    c int,\n" +
             "    d int,\n" +
             "    e generated always as ( a )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create function integerList_023()\n" +
             "returns table( a int, b int, c int, d int )\n" +
             "language java\n" +
             "parameter style derby_jdbc_result_set\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.integerList_023'\n"
             );

        //
        // Correlation names in DELETE actions
        //
        populate_023( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t1_023 a using test_dbo.t2_023 b\n" +
             "on a.a_public = b.a_public\n" +
             "when matched and a.b_select_t1_ruth = 11 then delete\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "2", "12", "102", "1002", "10002", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
             },
             false
             );

        //
        // Correlation names in UPDATE actions
        //
        populate_023( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t1_023 a using test_dbo.t2_023 b\n" +
             "on a.a_public = b.a_public\n" +
             "when matched and a.b_select_t1_ruth = 12 then update set e_update_t1_ruth = a.g_update_t1_frank + b.c_select_t2_alice\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "1", "11", "101", "1001", "10001", "100001", "1000001", "1" },
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
             },
             false
             );

        //
        // Correlation names in INSERT actions
        //
        populate_023( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t1_023 a using test_dbo.t2_023 b\n" +
             "on a.a_public = b.a_public\n" +
             "when not matched and b.b_select_t2_ruth = 14 then insert\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t1_ruth,\n" +
             "    c_select_t1_alice,\n" +
             "    d_select_t1_frank,\n" +
             "    e_update_t1_ruth,\n" +
             "    f_update_t1_alice,\n" +
             "    g_update_t1_frank\n" +
             ")\n" +
             "values ( b.a_public, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "1", "11", "101", "1001", "10001", "100001", "1000001", "1" },
                 { "2", "12", "102", "1002", "10002", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // Correlation names in all actions
        //
        populate_023( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t1_023 a using test_dbo.t2_023 b\n" +
             "on a.a_public = b.a_public\n" +
             "when matched and a.b_select_t1_ruth = 11 then delete\n" +
             "when matched and a.b_select_t1_ruth = 12 then update set e_update_t1_ruth = a.g_update_t1_frank + b.c_select_t2_alice\n" +
             "when not matched and b.b_select_t2_ruth = 14 then insert\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t1_ruth,\n" +
             "    c_select_t1_alice,\n" +
             "    d_select_t1_frank,\n" +
             "    e_update_t1_ruth,\n" +
             "    f_update_t1_alice,\n" +
             "    g_update_t1_frank\n" +
             ")\n" +
             "values ( b.a_public, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // Correlation names only where needed.
        //
        populate_023( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t1_023 a using test_dbo.t2_023 b\n" +
             "on a.a_public = b.a_public\n" +
             "when matched and b_select_t1_ruth = 11 then delete\n" +
             "when matched and b_select_t1_ruth = 12 then update set e_update_t1_ruth = g_update_t1_frank + c_select_t2_alice\n" +
             "when not matched and b_select_t2_ruth = 14 then insert\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t1_ruth,\n" +
             "    c_select_t1_alice,\n" +
             "    d_select_t1_frank,\n" +
             "    e_update_t1_ruth,\n" +
             "    f_update_t1_alice,\n" +
             "    g_update_t1_frank\n" +
             ")\n" +
             "values ( b.a_public, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // Correlation names to remove ambiguities.
        //
        populate_023_2( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t3_023 a using test_dbo.t4_023 b\n" +
             "on a.a = b.a\n" +
             "when matched and a.b = 11 then delete\n" +
             "when matched and a.b = 12 then update set e = a.g + b.c\n" +
             "when not matched and b.b = 14 then insert\n" +
             "(\n" +
             "    a,\n" +
             "    b,\n" +
             "    c,\n" +
             "    d,\n" +
             "    e,\n" +
             "    f,\n" +
             "    g\n" +
             ")\n" +
             "values ( b.a, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t3_023 order by a",
             new String[][]
             {
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // No correlation names.
        //
        populate_023( dboConnection );
        populate_023_2( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t1_023 using test_dbo.t4_023\n" +
             "on a_public = a\n" +
             "when matched and b_select_t1_ruth = 11 then delete\n" +
             "when matched and b_select_t1_ruth = 12 then update set e_update_t1_ruth = g_update_t1_frank + c\n" +
             "when not matched and b = 14 then insert\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t1_ruth,\n" +
             "    c_select_t1_alice,\n" +
             "    d_select_t1_frank,\n" +
             "    e_update_t1_ruth,\n" +
             "    f_update_t1_alice,\n" +
             "    g_update_t1_frank\n" +
             ")\n" +
             "values ( a, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // No correlation names. Columns are table-qualified, however.
        //
        populate_023( dboConnection );
        populate_023_2( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t1_023 using test_dbo.t4_023\n" +
             "on t1_023.a_public = t4_023.a\n" +
             "when matched and t1_023.b_select_t1_ruth = 11 then delete\n" +
             "when matched and t1_023.b_select_t1_ruth = 12 then update set e_update_t1_ruth = t1_023.g_update_t1_frank + t4_023.c\n" +
             "when not matched and t4_023.b = 14 then insert\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t1_ruth,\n" +
             "    c_select_t1_alice,\n" +
             "    d_select_t1_frank,\n" +
             "    e_update_t1_ruth,\n" +
             "    f_update_t1_alice,\n" +
             "    g_update_t1_frank\n" +
             ")\n" +
             "values ( t4_023.a, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // No correlation names. Tables aren't schema-qualified. Columns are table-qualified, however.
        //
        populate_023( dboConnection );
        populate_023_2( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into t1_023 using t4_023\n" +
             "on t1_023.a_public = t4_023.a\n" +
             "when matched and t1_023.b_select_t1_ruth = 11 then delete\n" +
             "when matched and t1_023.b_select_t1_ruth = 12 then update set e_update_t1_ruth = t1_023.g_update_t1_frank + t4_023.c\n" +
             "when not matched and t4_023.b = 14 then insert\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t1_ruth,\n" +
             "    c_select_t1_alice,\n" +
             "    d_select_t1_frank,\n" +
             "    e_update_t1_ruth,\n" +
             "    f_update_t1_alice,\n" +
             "    g_update_t1_frank\n" +
             ")\n" +
             "values ( t4_023.a, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // With correlation names.
        //
        populate_023( dboConnection );
        populate_023_2( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t3_023 a using test_dbo.t4_023 b\n" +
             "on a.a = b.a\n" +
             "when matched and a.b = 11 then delete\n" +
             "when matched and a.b = 12 then update set e = a.g + b.c\n" +
             "when not matched and b.b = 14 then insert\n" +
             "(\n" +
             "    a,\n" +
             "    b,\n" +
             "    c,\n" +
             "    d,\n" +
             "    e,\n" +
             "    f,\n" +
             "    g\n" +
             ")\n" +
             "values ( b.a, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t3_023 order by a",
             new String[][]
             {
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // Source is a table function. Column names unambiguous and unqualified.
        //
        populate_023( dboConnection );
        populate_023_2( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t1_023 using table( test_dbo.integerList_023() ) i\n" +
             "on a_public = a\n" +
             "when matched and b = 11 then delete\n" +
             "when matched and b = 12 then update set e_update_t1_ruth = g_update_t1_frank + c\n" +
             "when not matched and b = 14 then insert\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t1_ruth,\n" +
             "    c_select_t1_alice,\n" +
             "    d_select_t1_frank,\n" +
             "    e_update_t1_ruth,\n" +
             "    f_update_t1_alice,\n" +
             "    g_update_t1_frank\n" +
             ")\n" +
             "values ( a, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // Source is a table function. With correlation names. Tables are schema-qualified.
        // Column names unambiguous but qualified.
        //
        populate_023( dboConnection );
        populate_023_2( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into test_dbo.t1_023 a using table( test_dbo.integerList_023() ) i\n" +
             "on a.a_public = i.a\n" +
             "when matched and i.b = 11 then delete\n" +
             "when matched and i.b = 12 then update set e_update_t1_ruth = a.g_update_t1_frank + i.c\n" +
             "when not matched and i.b = 14 then insert\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t1_ruth,\n" +
             "    c_select_t1_alice,\n" +
             "    d_select_t1_frank,\n" +
             "    e_update_t1_ruth,\n" +
             "    f_update_t1_alice,\n" +
             "    g_update_t1_frank\n" +
             ")\n" +
             "values ( i.a, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // Source is a table function. With correlation names. Tables are not schema-qualified.
        // Column names unambiguous but qualified.
        //
        populate_023( dboConnection );
        populate_023_2( dboConnection );
        goodUpdate
            (
             dboConnection,
             "merge into t1_023 a using table( integerList_023() ) i\n" +
             "on a.a_public = i.a\n" +
             "when matched and i.b = 11 then delete\n" +
             "when matched and i.b = 12 then update set e_update_t1_ruth = a.g_update_t1_frank + i.c\n" +
             "when not matched and i.b = 14 then insert\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t1_ruth,\n" +
             "    c_select_t1_alice,\n" +
             "    d_select_t1_frank,\n" +
             "    e_update_t1_ruth,\n" +
             "    f_update_t1_alice,\n" +
             "    g_update_t1_frank\n" +
             ")\n" +
             "values ( i.a, 18, 108, 1008, 10008, 100008, 1000008 )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t1_023 order by a_public",
             new String[][]
             {
                 { "2", "12", "102", "1002", "1000104", "100002", "1000002", "2" },
                 { "3", "13", "103", "1003", "10003", "100003", "1000003", "3" },
                 { "4", "18", "108", "1008", "10008", "100008", "1000008", "4" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop function integerList_023" );
        goodStatement( dboConnection, "drop table t4_023" );
        goodStatement( dboConnection, "drop table t3_023" );
        goodStatement( dboConnection, "drop table t2_023" );
        goodStatement( dboConnection, "drop table t1_023" );
    }
    private void    populate_023( Connection conn ) throws Exception
    {
        goodStatement( conn, "delete from t2_023" );
        goodStatement( conn, "delete from t1_023" );

        goodStatement
            (
             conn,
             "insert into t1_023\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t1_ruth,\n" +
             "    c_select_t1_alice,\n" +
             "    d_select_t1_frank,\n" +
             "    e_update_t1_ruth,\n" +
             "    f_update_t1_alice,\n" +
             "    g_update_t1_frank\n" +
             ")\n" +
             "values\n" +
             "( 1, 11, 101, 1001, 10001, 100001, 1000001 ),\n" +
             "( 2, 12, 102, 1002, 10002, 100002, 1000002 ),\n" +
             "( 3, 13, 103, 1003, 10003, 100003, 1000003 )\n"
             );
        goodStatement
            (
             conn,
             "insert into t2_023\n" +
             "(\n" +
             "    a_public,\n" +
             "    b_select_t2_ruth,\n" +
             "    c_select_t2_alice,\n" +
             "    d_select_t2_frank\n" +
             ")\n" +
             "values\n" +
             "( 1, 11, 101, 1001 ),\n" +
             "( 2, 12, 102, 1002 ),\n" +
             "( 3, 13, 103, 1003 ),\n" +
             "( 4, 14, 104, 1004 )\n"
             );
    }
    private void    populate_023_2( Connection conn ) throws Exception
    {
        goodStatement( conn, "delete from t4_023" );
        goodStatement( conn, "delete from t3_023" );

        goodStatement
            (
             conn,
             "insert into t3_023\n" +
             "(\n" +
             "    a,\n" +
             "    b,\n" +
             "    c,\n" +
             "    d,\n" +
             "    e,\n" +
             "    f,\n" +
             "    g\n" +
             ")\n" +
             "values\n" +
             "( 1, 11, 101, 1001, 10001, 100001, 1000001 ),\n" +
             "( 2, 12, 102, 1002, 10002, 100002, 1000002 ),\n" +
             "( 3, 13, 103, 1003, 10003, 100003, 1000003 )\n"
             );
        goodStatement
            (
             conn,
             "insert into t4_023\n" +
             "(\n" +
             "    a,\n" +
             "    b,\n" +
             "    c,\n" +
             "    d\n" +
             ")\n" +
             "values\n" +
             "( 1, 11, 101, 1001 ),\n" +
             "( 2, 12, 102, 1002 ),\n" +
             "( 3, 13, 103, 1003 ),\n" +
             "( 4, 14, 104, 1004 )\n"
             );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Verify that BEFORE triggers can't fire MERGE statements.
     * </p>
     */
    public  void    test_024_mergeNotAllowedInBeforeTriggers()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_024( c1 int, c2 int, c3 int generated always as ( c1 + c2 ), c1_4 int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_024( c1 int, c2 int, c3 int, c4 int, c5 varchar( 5 ) )"
             );
        goodStatement
            (
             dboConnection,
             "create table t3_024( a int )"
             );

        //
        // BEFORE DELETE triggers can't fire DML statements.
        //
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_del_insert_before\n" +
              "no cascade before delete on t3_024\n" +
              "for each statement\n" +
              "insert into t1_024( c1, c2 ) values ( 1, 2 )\n"
              );
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_del_update_before\n" +
              "no cascade before delete on t3_024\n" +
              "for each statement\n" +
              "update t1_024 set c1 = 1\n"
              );
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_del_delete_before\n" +
              "no cascade before delete on t3_024\n" +
              "for each statement\n" +
              "delete from t1_024\n"
              );
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_del_merge_before\n" +
              "no cascade before delete on t3_024\n" +
              "for each statement\n" +
              "merge into t1_024\n" +
              "using t2_024\n" +
              "on t1_024.c1 = t2_024.c1\n" +
              "when matched then delete\n"
              );

        //
        // BEFORE INSERT triggers can't fire DML statements.
        //
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_ins_insert_before\n" +
              "no cascade before insert on t3_024\n" +
              "for each statement\n" +
              "insert into t1_024( c1, c2 ) values ( 1, 2 )\n"
              );
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_ins_update_before\n" +
              "no cascade before insert on t3_024\n" +
              "for each statement\n" +
              "update t1_024 set c1 = 1\n"
              );
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_ins_delete_before\n" +
              "no cascade before insert on t3_024\n" +
              "for each statement\n" +
              "delete from t1_024\n"
              );
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_ins_merge_before\n" +
              "no cascade before insert on t3_024\n" +
              "for each statement\n" +
              "merge into t1_024\n" +
              "using t2_024\n" +
              "on t1_024.c1 = t2_024.c1\n" +
              "when matched then delete\n"
              );
        
        //
        // BEFORE UPDATE triggers can't fire DML statements.
        //
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_upd_insert_before\n" +
              "no cascade before update on t3_024\n" +
              "for each statement\n" +
              "insert into t1_024( c1, c2 ) values ( 1, 2 )\n"
              );
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_upd_update_before\n" +
              "no cascade before update on t3_024\n" +
              "for each statement\n" +
              "update t1_024 set c1 = 1\n"
              );
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_upd_delete_before\n" +
              "no cascade before update on t3_024\n" +
              "for each statement\n" +
              "delete from t1_024\n"
              );
        expectCompilationError
            ( dboConnection, NO_DML_IN_BEFORE_TRIGGERS,
              "create trigger t3_024_upd_merge_before\n" +
              "no cascade before update on t3_024\n" +
              "for each statement\n" +
              "merge into t1_024\n" +
              "using t2_024\n" +
              "on t1_024.c1 = t2_024.c1\n" +
              "when matched then delete\n"
              );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t3_024" );
        goodStatement( dboConnection, "drop table t2_024" );
        goodStatement( dboConnection, "drop table t1_024" );
    }
    
    /**
     * <p>
     * Verify that the INSERT list can be omitted.
     * </p>
     */
    public  void    test_025_noInsertList()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_025( a int, b int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_025( a int, b int )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_025 values ( 1, 100 ), ( 2, 200 )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_025 values ( 1, 100 ), ( 3, 300 )"
             );

        //
        // Omitting the INSERT column list is OK as long as this is
        // a short-hand for the full column list.
        //
        goodStatement
            (
             dboConnection,
             "merge into t1_025\n" +
             "using t2_025 on t1_025.a = t2_025.a\n" +
             "when not matched then insert values ( t2_025.a, t2_025.b )\n"
             );
        assertResults
            (
             dboConnection,
             "select * from t1_025 order by a",
             new String[][]
             {
                 { "1", "100" },
                 { "2", "200" },
                 { "3", "300" },
             },
             false
             );

        //
        // Fails because the omitted INSERT column list implies that a value
        // must be supplied for every column in the table.
        //
        expectCompilationError
            ( dboConnection, COLUMN_COUNT_MISMATCH,
              "merge into t1_025\n" +
              "using t2_025 on t1_025.a = t2_025.a\n" +
              "when not matched then insert values ( t2_025.a )\n"
              );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_025" );
        goodStatement( dboConnection, "drop table t1_025" );
    }

    /**
     * <p>
     * Verify that MERGE works with system tables and global temporary tables.
     * </p>
     */
    public  void    test_026_otherTableTypes()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_026( a int )"
             );
        goodStatement
            (
             dboConnection,
             "declare global temporary table session.t2_temp_026( a int )\n" +
             "on commit preserve rows not logged\n"
             );

        // allow system tables as source tables
        goodStatement
            (
             dboConnection,
             "merge into t1_026 t\n" +
             "using sys.syscolumns c on t.a = c.columnNumber\n" +
             "when not matched then insert ( a ) values ( c.columnNumber )\n"
             );

        // but don't allow system tables as target tables
        expectCompilationError
            ( dboConnection, TARGET_MUST_BE_BASE,
              "merge into sys.syscolumns c\n" +
              "using t1_026 t on t.a = c.columnNumber\n" +
              "when not matched then insert ( columnNumber ) values ( t.a )\n"
              );

        // allow temporary tables as source tables
        goodStatement
            (
             dboConnection,
             "delete from t1_026"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_026 values ( 1 )"
             );
        goodStatement
            (
             dboConnection,
             "insert into session.t2_temp_026 values ( 1 ), ( 2 )"
             );
        goodUpdate
            (
             dboConnection,
             "merge into t1_026\n" +
             "using session.t2_temp_026 s on s.a = t1_026.a\n" +
             "when not matched then insert values ( s.a )\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_026 order by a",
             new String[][]
             {
                 { "1" },
                 { "2" },
             },
             false
             );

        // allow temporary tables as target tables
        goodStatement
            (
             dboConnection,
             "delete from t1_026"
             );
        goodStatement
            (
             dboConnection,
             "delete from session.t2_temp_026"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_026 values ( 1 ), ( 2 )"
             );
        goodStatement
            (
             dboConnection,
             "insert into session.t2_temp_026 values ( 1 )"
             );
        goodUpdate
            (
             dboConnection,
             "merge into session.t2_temp_026 s\n" +
             "using t1_026 on t1_026.a = s.a\n" +
             "when not matched then insert values ( t1_026.a )\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from session.t2_temp_026 order by a",
             new String[][]
             {
                 { "1" },
                 { "2" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table session.t2_temp_026" );
        goodStatement( dboConnection, "drop table t1_026" );
    }
    
    /**
     * <p>
     * Verify that correlation names on the left side of SET clauses are replaced properly.
     * </p>
     */
    public  void    test_027_correlationNamesInSetClauses()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_027( a int, b int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_027( c int, d int )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_027 values ( 1, 100 ), ( 2, 200 )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_027 values ( 1, 1000 ), (2, 2000), ( 3, 3000 ), ( 4, 4000 )"
             );

        // test that correlation names are replaced properly
        goodUpdate
            (
             dboConnection,
             "merge into t1_027 x\n" +
             "using t2_027 y on x.a = y.c\n" +
             "when matched and x.b > 100 then update set x.b = y.d\n" +
             "when matched and x.b <= 100 then delete\n" +
             "when not matched and y.d > 3000 then insert values ( y.c, y.d )\n",
             3
             );
        assertResults
            (
             dboConnection,
             "select * from t1_027 order by a",
             new String[][]
             {
                 { "2", "2000" },
                 { "4", "4000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_027" );
        goodStatement( dboConnection, "drop table t1_027" );
    }
    
    /**
     * <p>
     * Verify that you can drive MERGE statements from row-based triggers.
     * </p>
     */
    public  void    test_028_basicRowTrigger()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create view singlerow_028( x ) as values 1"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_028( x int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_028( y int )"
             );
        goodStatement
            (
             dboConnection,
             "create trigger tr after insert on t1_028\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "merge into t2_028\n" +
             "using singlerow_028 on t2_028.y = new.x\n" +
             "when not matched then insert ( y ) values ( new.x )\n"
             );

        // now exercise the trigger
        goodStatement
            (
             dboConnection,
             "insert into t1_028 values 1,2,3,4,5,4,3,2,1,1,1,2,3,100"
             );
        assertResults
            (
             dboConnection,
             "select * from t2_028 order by y",
             new String[][]
             {
                 { "1" },
                 { "2" },
                 { "3" },
                 { "4" },
                 { "5" },
                 { "100" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t1_028" );
        goodStatement( dboConnection, "drop table t2_028" );
        goodStatement( dboConnection, "drop view singlerow_028" );
    }
    
    /**
     * <p>
     * This case tests a problem query which causes an index scan to
     * be selected for the target table. Row locations weren't being treated
     * as columns in the result row and conglomerate info was not being
     * propagated to copied ResultColumnLists.
     * </p>
     */
    public  void    test_029_scanViaIndex()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_029(x int primary key)"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_029(x int)"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_029 values( 33 )"
             );

        // this was the problem query
        goodUpdate
            (
             dboConnection,
             "merge into t1_029\n" +
             "using t2_029 on t1_029.x = 42\n" +
             "when not matched then insert (x) values (42)\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select  * from t1_029 order by x",
             new String[][]
             {
                 { "42" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t1_029" );
        goodStatement( dboConnection, "drop table t2_029" );
    }
    
    /**
     * <p>
     * Verify the fix to a query which broke the serialization of
     * row locations.
     * </p>
     */
    public  void    test_030_SQLRef_serialization()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_030(x int, y varchar(100))"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_030(x int)"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_030 values 1, 1, 2"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_030 values (1, null), (2, '')"
             );

        // verify the fix
        goodUpdate
            (
             dboConnection,
             "merge into t1_030\n" +
             "using t2_030 on t1_030.x = t2_030.x\n" +
             "when matched and y is not null then update set y = y || 'x'\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select  * from t1_030 order by x, y",
             new String[][]
             {
                 { "1", null },
                 { "2", "x" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t1_030" );
        goodStatement( dboConnection, "drop table t2_030" );
    }
    
    /**
     * <p>
     * Verify the fix to a query involving a view.
     * </p>
     */
    public  void    test_031_view()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_031(x int, y int)"
             );
        goodStatement
            (
             dboConnection,
             "create table tv_031(x int, y int)"
             );
        goodStatement
            (
             dboConnection,
             "create view v_031 as select * from tv_031"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_031 values ( 1, 100 ), ( 2, 200 ), ( 3, 300 )"
             );
        goodStatement
            (
             dboConnection,
             "insert into tv_031 values ( 1, 1000 ), ( 3, 3000 ), ( 4, 4000 )"
             );

        // verify the fix
        goodUpdate
            (
             dboConnection,
             "merge into t1_031\n" +
             "using v_031 on t1_031.x = v_031.x\n" +
             "when matched then update set t1_031.y = v_031.y\n",
             2
             );
        assertResults
            (
             dboConnection,
             "select * from t1_031 order by x",
             new String[][]
             {
                 { "1", "1000" },
                 { "2", "200" },
                 { "3", "3000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop view v_031" );
        goodStatement( dboConnection, "drop table t1_031" );
        goodStatement( dboConnection, "drop table tv_031" );
    }
    
    /**
     * <p>
     * For the time being, forbid subqueries in WHEN [ NOT ] MATCHED clauses.
     * </p>
     */
    public  void    test_032_noSubqueriesInMatchedClauses()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_032( a int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_032( a int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t3_032( a int )"
             );

        //
        // All of these statements should be rejected because they carry
        // subqueries in the WHEN [ NOT ] MATCHED clause.
        //
        expectCompilationError
            ( dboConnection, NO_SUBQUERIES_IN_MATCHED_CLAUSE,
              "merge into t1_032\n" +
              "using t2_032 on t1_032.a = t2_032.a\n" +
              "when matched and t1_032.a > (select max( a ) from t3_032 ) then update set a = t2_032.a * 2\n"
              );
        expectCompilationError
            ( dboConnection, NO_SUBQUERIES_IN_MATCHED_CLAUSE,
              "merge into t1_032\n" +
              "using t2_032 on t1_032.a = t2_032.a\n" +
              "when matched then update set a = (select max( a ) from t3_032 )\n"
              );
        expectCompilationError
            ( dboConnection, NO_SUBQUERIES_IN_MATCHED_CLAUSE,
              "merge into t1_032\n" +
              "using t2_032 on t1_032.a = t2_032.a\n" +
              "when matched and t1_032.a > (select max( a ) from t3_032 ) then delete\n"
              );
        expectCompilationError
            ( dboConnection, NO_SUBQUERIES_IN_MATCHED_CLAUSE,
              "merge into t1_032\n" +
              "using t2_032 on t1_032.a = t2_032.a\n" +
              "when not matched and t1_032.a > (select max( a ) from t3_032 ) then insert values ( 1 )\n"
              );
        expectCompilationError
            ( dboConnection, NO_SUBQUERIES_IN_MATCHED_CLAUSE,
              "merge into t1_032\n" +
              "using t2_032 on t1_032.a = t2_032.a\n" +
              "when not matched then insert values ( (select max( a ) from t3_032 ) )\n"
              );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t3_032" );
        goodStatement( dboConnection, "drop table t2_032" );
        goodStatement( dboConnection, "drop table t1_032" );
    }
    
    /**
     * <p>
     * Correctly resolve column references using a source and target
     * whose table and column names are identical but which live in different
     * schemas.
     * </p>
     */
    public  void    test_033_identicalNames()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  aliceConnection = openUserConnection( ALICE );

        //
        // create schema
        //
        goodStatement
            (
             ruthConnection,
             "create table t1_033( x int, y int )"
             );
        goodStatement
            (
             aliceConnection,
             "create table t1_033( x int, y int )"
             );
        goodStatement
            (
             ruthConnection,
             "insert into t1_033 values ( 1, 100 ), ( 2, 200 ), ( 3, 300 )"
             );
        goodStatement
            (
             aliceConnection,
             "insert into t1_033 values ( 1, 1000 ), ( 3, 3000 ), ( 4, 4000 )"
             );

        // verify the behavior
        goodUpdate
            (
             dboConnection,
             "merge into ruth.t1_033 r\n" +
             "using alice.t1_033 a on r.x = a.x\n" +
             "when matched then update set x = a.x * 10\n",
             2
             );
        assertResults
            (
             dboConnection,
             "select * from ruth.t1_033 order by x",
             new String[][]
             {
                 { "2", "200" },
                 { "10", "100" },
                 { "30", "300" },
             },
             false
             );
        
        //
        // drop schema
        //
        goodStatement( ruthConnection, "drop table t1_033" );
        goodStatement( aliceConnection, "drop table t1_033" );
    }
    
    /**
     * <p>
     * Synonyms not allowed as source or target tables in MERGE statements.
     * </p>
     */
    public  void    test_034_noSynonyms()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_034( x int, y int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_034( x int, y int )"
             );
        goodStatement
            (
             dboConnection,
             "create synonym syn_t1_034 for t1_034"
             );
        goodStatement
            (
             dboConnection,
             "create synonym syn_t2_034 for t2_034"
             );

        // verify that synonyms are forbidden
        expectCompilationError
            ( dboConnection, NO_SYNONYMS_IN_MERGE,
              "merge into syn_t1_034\n" +
              "using t2_034 on syn_t1_034.x  = t2_034.x\n" +
              "when matched then update set syn_t1_034.y = t2_034.y\n"
              );
        expectCompilationError
            ( dboConnection, NO_SYNONYMS_IN_MERGE,
              "merge into syn_t1_034 a\n" +
              "using t2_034 on a.x  = t2_034.x\n" +
              "when matched then update set a.y = t2_034.y\n"
              );
        expectCompilationError
            ( dboConnection, NO_SYNONYMS_IN_MERGE,
              "merge into t1_034\n" +
              "using syn_t2_034 on t1_034.x = syn_t2_034.x\n" +
              "when matched then update set t1_034.y = syn_t2_034.y\n"
              );
        expectCompilationError
            ( dboConnection, NO_SYNONYMS_IN_MERGE,
              "merge into t1_034\n" +
              "using syn_t2_034 a on t1_034.x = a.x\n" +
              "when matched then update set t1_034.y = a.y\n"
              );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop synonym syn_t2_034" );
        goodStatement( dboConnection, "drop synonym syn_t1_034" );
        goodStatement( dboConnection, "drop table t2_034" );
        goodStatement( dboConnection, "drop table t1_034" );
    }
    
    /**
     * <p>
     * Verify that table identifiers can be used or omitted on the left
     * side of SET clauses.
     * </p>
     */
    public  void    test_035_leftSideOfSet()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_035( x int, y int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_035( x int, y int )"
             );

        // verify with and without aliases and with and without identifiers on the left side of SET clauses
        vet_035
            (
             dboConnection,
             "merge into t1_035\n" +
             "using t2_035 on t1_035.x = t2_035.x\n" +
             "when matched then update set t1_035.y = t2_035.y\n"
             );
        vet_035
            (
             dboConnection,
             "merge into t1_035\n" +
             "using t2_035 on t1_035.x = t2_035.x\n" +
             "when matched then update set y = t2_035.y\n"
             );
        vet_035
            (
             dboConnection,
             "merge into t1_035 a\n" +
             "using t2_035 on a.x = t2_035.x\n" +
             "when matched then update set a.y = t2_035.y\n"
             );
        vet_035
            (
             dboConnection,
             "merge into t1_035 a\n" +
             "using t2_035 on a.x = t2_035.x\n" +
             "when matched then update set y = t2_035.y\n" 
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_035" );
        goodStatement( dboConnection, "drop table t1_035" );
    }
    private void    vet_035( Connection conn, String query )
        throws Exception
    {
        goodStatement
            (
             conn,
             "delete from t1_035"
             );
        goodStatement
            (
             conn,
             "delete from t2_035"
             );
        goodStatement
            (
             conn,
             "insert into t1_035 values ( 1, 100 ), ( 2, 200 ), ( 3, 300 )"
             );
        goodStatement
            (
             conn,
             "insert into t2_035 values ( 1, 1000 ), ( 3, 3000 ), ( 4, 4000 )"
             );

        String[][]  expectedResults = new String[][]
            {
                { "1", "1000" },
                { "2", "200" },
                { "3", "3000" },
            };
        goodUpdate( conn, query, 2 );
        assertResults( conn, "select * from t1_035 order by x", expectedResults, false );
    }
    
   /**
     * <p>
     * Don't allow derived column lists in MERGE statements..
     * </p>
     */
    public  void    test_036_derivedColumnLists()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_036( a int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_036( a int )"
             );

        // verify that derived column lists are not allowed
        expectCompilationError
            ( dboConnection, NO_DCL_IN_MERGE,
              "merge into t1_036 r( x )\n" +
              "using t2_036 on r.x = t2_036.a\n" +
              "when matched then delete\n"
              );
        expectCompilationError
            ( dboConnection, NO_DCL_IN_MERGE,
              "merge into t1_036\n" +
              "using t2_036 r( x ) on t1_036.a = r.x\n" +
              "when matched then delete\n"
              );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_036" );
        goodStatement( dboConnection, "drop table t1_036" );
    }
    
   /**
     * <p>
     * Verify that you can use ? parameters in MERGE statements.
     * </p>
     */
    public  void    test_037_parameters()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_037( x int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_037( x int )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_037 values ( 100 ), ( 200 )"
             );

        //
        // No verify the setting of a ? parameter.
        //
        PreparedStatement   ps = chattyPrepare
            (
             dboConnection,
             "merge into t1_037 using t2_037 on ? when not matched then insert values ( t2_037.x )"
             );
        try {
            ps.execute();
            fail( "Expected statement to raise an error because a parameter isn't set." );
        }
        catch (SQLException se)
        {
            assertEquals( PARAMETER_NOT_SET, se.getSQLState() );
        }

        ps.setBoolean( 1, true );
        ps.execute();
        assertResults
            (
             dboConnection,
             "select * from t1_037 order by x",
             new String[][]
             {
                 { "100" },
                 { "200" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_037" );
        goodStatement( dboConnection, "drop table t1_037" );
    }
    
   /**
     * <p>
     * Verify that you can use ? parameters in all search conditions as well
     * as in INSERT values and on the left side of SET operators.
     * </p>
     */
    public  void    test_038_parameters()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_038( x int, y int, z int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_038( x int, y int, z int )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_038 values \n" +
             "( 1, -1, 100 ), ( 2, -2, 200 ), ( 3, -3, 300 ), ( 5, -5, 500 ),\n" +
             "( 100, -10, 100 ), ( 200, -20, 200 ), ( 300, -30, 300 ), ( 500, -50, 500 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_038 values\n" +
             "( 10, -100, 1000 ), ( 30, -300, 3000), ( 40, -400, 4000 ), ( 40, -401, 4001 ), ( 50, -500, 5000 ), ( 50, -501, 5001 ),\n" +
             "( -1000, -10, 1000 ), ( -3000, -30, 3000), ( -4000, -40, 4000 ), ( -4000, -41, 4001 ),\n" +
             "( -5000, -50, 5000 ), ( -5000, -51, 5001 )\n"
             );

        //
        // now verify the correct operation of ? parameters in all clauses
        //
        PreparedStatement    ps = chattyPrepare
            (
             dboConnection,
             "merge into t1_038\n" +
             "using t2_038 on t2_038.x = t1_038.x * ?\n" +
             "when not matched and t2_038.y = ? then insert values (  t2_038.x, t2_038.y, t2_038.z * ? )\n" +
             "when matched and t2_038.y = ? then delete\n" +
             "when matched and t2_038.y = ? then update set z = t2_038.z * ?"  
             );
        
        ps.setInt( 1, 10 );
        ps.setInt( 2, -401 );
        ps.setInt( 3, 2 );
        ps.setInt( 4, -300 );
        ps.setInt( 5, -501 );
        ps.setInt( 6, 3 );
        ps.execute();
        assertResults
            (
             dboConnection,
             "select * from t1_038 order by x, y, z",
             new String[][]
             {
                 { "1", "-1", "100" },
                 { "2", "-2", "200" },
                 { "5", "-5", "15003" },
                 { "40", "-401", "8002" },
                 { "100", "-10", "100" },
                 { "200", "-20", "200" },
                 { "300", "-30", "300" },
                 { "500", "-50", "500" },
             },
             false
             );

        // verify that you can change the values
        ps.setInt( 1, -10 );
        ps.setInt( 2, -41 );
        ps.setInt( 3, 3 );
        ps.setInt( 4, -30 );
        ps.setInt( 5, -51 );
        ps.setInt( 6, 4 );
        ps.execute();
        assertResults
            (
             dboConnection,
             "select * from t1_038 order by x, y, z",
             new String[][]
             {
                 { "-4000", "-41", "12003" },
                 { "1", "-1", "100" },
                 { "2", "-2", "200" },
                 { "5", "-5", "15003" },
                 { "40", "-401", "8002" },
                 { "100", "-10", "100" },
                 { "200", "-20", "200" },
                 { "500", "-50", "20004" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_038" );
        goodStatement( dboConnection, "drop table t1_038" );
    }
    
   /**
     * <p>
     * Verify correct behavior when the target table is read via index probing.
     * </p>
     */
    public  void    test_039_indexProbe()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create view sr_039( i ) as values 1"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_039( x int, y int, z int )"
             );
        goodStatement
            (
             dboConnection,
             "create unique index idx on t1_039( x, y )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_039 values ( 1, 100, 1000 ), ( 2, 200, 2000 )"
             );

        // now verify the behavior
        goodUpdate
            (
             dboConnection,
             "merge into t1_039\n" +
             "using sr_039 on ( x = 1 )\n" +
             "when matched then delete\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_039 order by x",
             new String[][]
             {
                 { "2", "200", "2000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop view sr_039" );
        goodStatement( dboConnection, "drop table t1_039" );
    }
    
   /**
     * <p>
     * Verify correct behavior when source table is a values clause wrapped in a view.
     * </p>
     */
    public  void    test_040_valuesView()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create view sr_040( i ) as values 1"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_040( x int, y int, z int )"
             );
        goodStatement
            (
             dboConnection,
             "create unique index idx on t1_040( x, y )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_040 values\n" +
             "( 1, 100, 1000 ), ( 1, 101, 1000 ), ( 1, 102, 1000 ), ( 1, 103, 1000 ), ( 2, 200, 2000 )\n"
             );

        // verify the behavior
        goodUpdate
            (
             dboConnection,
             "merge into t1_040\n" +
             "using sr_040 on ( x = 1 )\n" +
             "when matched and y = 101 then delete\n" +
             "when matched and y = 102 then update set z = -1000\n" +
             "when not matched and i > 1 then insert values ( -1, i, 0 )\n",
             2
             );
        assertResults
            (
             dboConnection,
             "select * from t1_040 order by x, y, z",
             new String[][]
             {
                 { "1", "100", "1000" },
                 { "1", "102", "-1000" },
                 { "1", "103", "1000" },
                 { "2", "200", "2000" },
             },
             false
             );

        goodUpdate
            (
             dboConnection,
             "merge into t1_040\n" +
             "using sr_040 on ( x = 3 )\n" +
             "when matched and y = 103 then delete\n" +
             "when matched and y = 102 then update set z = -10000\n" +
             "when not matched and i = 1 then insert values ( -1, i, 0 )\n",
             1
             );
        assertResults
            (
             dboConnection,
             "select * from t1_040 order by x, y, z",
             new String[][]
             {
                 { "-1", "1", "0" },
                 { "1", "100", "1000" },
                 { "1", "102", "-1000" },
                 { "1", "103", "1000" },
                 { "2", "200", "2000" },
             },
             false
             );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop view sr_040" );
        goodStatement( dboConnection, "drop table t1_040" );
    }
    
   /**
     * <p>
     * Verify the same target row can't be touched twice by a MERGE statement.
     * </p>
     */
    public  void    test_041_cardinalityViolations()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create view sr_041( i ) as values ( 1 ), ( 3 )"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_041( x int, y int, z int )"
             );
        goodStatement
            (
             dboConnection,
             "create unique index idx on t1_041( x, y )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_041( x int, y int, z int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t3_041( x int, y int, z int )"
             );
        goodStatement
            (
             dboConnection,
             "create unique index t2_idx_041 on t2_041( x, y )"
             );
        goodStatement
            (
             dboConnection,
             "create unique index t3_idx_041 on t3_041( x, y )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_041 values\n" +
             "( 1, 100, 1000 ), ( 1, 101, 1000 ), ( 1, 102, 1000 ), ( 1, 103, 1000 ), ( 2, 200, 2000 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_041 values\n" +
             "( 1, 100, 1000 ), ( 1, 101, 1000 ), ( 2, 200, 2000 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t3_041 values\n" +
             "( 1, 100, 1000 ), ( 1, -101, 1000 ), ( 3, 300, 3000 )\n"
             );

        //
        // Attempt to delete the same row twice.
        //
        expectExecutionError
            ( dboConnection, CARDINALITY_VIOLATION,
              "merge into t1_041\n" +
              "using sr_041 on ( x = 1 )\n" +
              "when matched and y = 101 then delete\n" +
              "when matched and y = 102 then update set z = -1000\n" +
              "when not matched and i > 1 then insert values ( -1, i, 0 )\n"
              );
        expectExecutionError
            ( dboConnection, CARDINALITY_VIOLATION,
              "merge into t2_041\n" +
              "using t3_041 on t2_041.x = t3_041.x\n" +
              "when matched and t2_041.y = 101 then delete\n"
              );

        //
        // attempt to update the same row twice
        //
        expectExecutionError
            ( dboConnection, CARDINALITY_VIOLATION,
              "merge into t2_041\n" +
              "using t3_041 on t2_041.x = t3_041.x\n" +
              "when matched and t2_041.y = 101 then update set z = t3_041.z\n" 
              );

        //
        // attempt to delete and update the same row
        //
        expectExecutionError
            ( dboConnection, CARDINALITY_VIOLATION,
              "merge into t2_041\n" +
              "using t3_041 on t2_041.x = t3_041.x\n" +
              "when matched and t2_041.y = t3_041.y then delete\n" +
              "when matched and t2_041.y = 100 and -101 = t3_041.y then update set z = 2 * t3_041.z\n"
              );
        expectExecutionError
            ( dboConnection, CARDINALITY_VIOLATION,
              "merge into t2_041\n" +
              "using t3_041 on t2_041.x = t3_041.x\n" +
              "when matched and t2_041.y = 100 and -101 = t3_041.y then update set z = 2 * t3_041.z\n" +
              "when matched and t2_041.y = t3_041.y then delete\n"
              );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop view sr_041" );
        goodStatement( dboConnection, "drop table t1_041" );
        goodStatement( dboConnection, "drop table t2_041" );
        goodStatement( dboConnection, "drop table t3_041" );
    }
    
    /**
     * <p>
     * Verify that we don't unnecessarily raise missing schema errors.
     * </p>
     */
    public  void    test_042_missingSchema()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );
        
        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create table deleteTable_042\n" +
             "(\n" +
             "    publicSelectColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table selectTable_042\n" +
             "(\n" +
             "    selectColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "grant select on  selectTable_042 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant select on  deleteTable_042 to public"
             );
        goodStatement
            (
             dboConnection,
             "grant delete on deleteTable_042 to ruth"
             );
        
        //
        // Verify that the unqualified reference to publicSelectColumn
        // does not fail because the RUTH schema does not exist.
        //
        String  mergeStatement =
            "merge into test_dbo.deleteTable_042\n" +
            "using test_dbo.selectTable_042\n" +
            "on publicSelectColumn = selectColumn\n" +
            "when matched then delete\n";
        expectExecutionWarning( ruthConnection, NO_ROWS_AFFECTED, mergeStatement );
        expectExecutionWarning( dboConnection, NO_ROWS_AFFECTED, mergeStatement );
        
        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table deleteTable_042" );
        goodStatement( dboConnection, "drop table selectTable_042" );
    }

    /**
     * <p>
     * Verify correlation names with columns added in order to
     * support triggers.
     * </p>
     */
    public  void    test_043_correlationNamesAddedColumns()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // create schema
        //
        goodStatement
            (
             dboConnection,
             "create type BeforeTriggerType_043 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type AfterTriggerType_043 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function beforeTriggerFunction_043( hashMap BeforeTriggerType_043, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function afterTriggerFunction_043( hashMap AfterTriggerType_043, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_043\n" +
             "(\n" +
             "    actionString varchar( 20 ),\n" +
             "    actionValue int\n" +
             ")\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.addHistoryRow'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table primaryTable_043\n" +
             "(\n" +
             "    key1 int primary key\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_043\n" +
             "(\n" +
             "    sourceChange int,\n" +
             "    sourceOnClauseColumn int,\n" +
             "    sourceMatchingClauseColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_043\n" +
             "(\n" +
             "    privateForeignColumn int references primaryTable_043( key1 ),\n" +
             "    privatePrimaryColumn int primary key,\n" +
             "    privateBeforeTriggerSource BeforeTriggerType_043,\n" +
             "    privateAfterTriggerSource AfterTriggerType_043,\n" +
             "    targetOnClauseColumn int,\n" +
             "    targetMatchingClauseColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table foreignTable_043\n" +
             "(\n" +
             "    key1 int references targetTable_043( privatePrimaryColumn )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeDeleteTrigger_043\n" +
             "no cascade before delete on targetTable_043\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_043( 'before', beforeTriggerFunction_043( old.privateBeforeTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterDeleteTrigger_043\n" +
             "after delete on targetTable_043\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_043( 'after', afterTriggerFunction_043( old.privateAfterTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeUpdateTrigger_043\n" +
             "no cascade before update on targetTable_043\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_043( 'before', beforeTriggerFunction_043( old.privateBeforeTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterUpdateTrigger_043\n" +
             "after update on targetTable_043\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_043( 'after', afterTriggerFunction_043( old.privateAfterTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeInsertTrigger_043\n" +
             "no cascade before insert on targetTable_043\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_043( 'before', beforeTriggerFunction_043( new.privateBeforeTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterInsertTrigger_043\n" +
             "after insert on targetTable_043\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_043( 'after', afterTriggerFunction_043( new.privateAfterTriggerSource, 'foo' ) )\n"
             );

        //
        // Now verify that column name are correctly resolved, including columns
        // added to satisfy triggers and constraints.
        //

        // delete
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043\n" +
              "using sourceTable_043\n" +
              "on targetOnClauseColumn = sourceOnClauseColumn\n" +
              "when matched and targetMatchingClauseColumn = sourceMatchingClauseColumn\n" +
              "     then delete\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on targetOnClauseColumn = sourceOnClauseColumn\n" +
              "when matched and targetMatchingClauseColumn = sourceMatchingClauseColumn\n" +
              "     then delete\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on targetOnClauseColumn = s.sourceOnClauseColumn\n" +
              "when matched and targetMatchingClauseColumn = s.sourceMatchingClauseColumn\n" +
              "     then delete\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on t.targetOnClauseColumn = s.sourceOnClauseColumn\n" +
              "when matched and t.targetMatchingClauseColumn = s.sourceMatchingClauseColumn\n" +
              "     then delete\n"
              );

        // update
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043\n" +
              "using sourceTable_043\n" +
              "on targetOnClauseColumn = sourceOnClauseColumn\n" +
              "when matched and targetMatchingClauseColumn = sourceMatchingClauseColumn\n" +
              "     then update set privateForeignColumn = sourceChange\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on targetOnClauseColumn = sourceOnClauseColumn\n" +
              "when matched and targetMatchingClauseColumn = sourceMatchingClauseColumn\n" +
              "     then update set privateForeignColumn = sourceChange\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on targetOnClauseColumn = s.sourceOnClauseColumn\n" +
              "when matched and targetMatchingClauseColumn = s.sourceMatchingClauseColumn\n" +
              "     then update set privateForeignColumn = s.sourceChange\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on t.targetOnClauseColumn = s.sourceOnClauseColumn\n" +
              "when matched and t.targetMatchingClauseColumn = s.sourceMatchingClauseColumn\n" +
              "     then update set t.privateForeignColumn = s.sourceChange\n"
              );

        // insert
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043\n" +
              "using sourceTable_043\n" +
              "on targetOnClauseColumn = sourceOnClauseColumn\n" +
              "when not matched and 1 = sourceMatchingClauseColumn\n" +
              "     then insert ( privateForeignColumn ) values ( sourceChange )\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on targetOnClauseColumn = sourceOnClauseColumn\n" +
              "when not matched and 1 = sourceMatchingClauseColumn\n" +
              "     then insert ( privateForeignColumn ) values ( sourceChange )\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on targetOnClauseColumn = s.sourceOnClauseColumn\n" +
              "when not matched and 1 = s.sourceMatchingClauseColumn\n" +
              "     then insert ( privateForeignColumn ) values ( s.sourceChange )\n"
              );

        // all clauses together
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043\n" +
              "using sourceTable_043\n" +
              "on targetOnClauseColumn = sourceOnClauseColumn\n" +
              "when matched and targetMatchingClauseColumn = sourceMatchingClauseColumn\n" +
              "     then delete\n" +
              "when matched and targetMatchingClauseColumn = sourceMatchingClauseColumn\n" +
              "     then update set privateForeignColumn = sourceChange\n" +
              "when not matched and 1 = sourceMatchingClauseColumn\n" +
              "     then insert ( privateForeignColumn ) values ( sourceChange )\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on targetOnClauseColumn = sourceOnClauseColumn\n" +
              "when matched and targetMatchingClauseColumn = sourceMatchingClauseColumn\n" +
              "     then delete\n" +
              "when matched and targetMatchingClauseColumn = sourceMatchingClauseColumn\n" +
              "     then update set privateForeignColumn = sourceChange\n" +
              "when not matched and 1 = sourceMatchingClauseColumn\n" +
              "     then insert ( privateForeignColumn ) values ( sourceChange )\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on targetOnClauseColumn = s.sourceOnClauseColumn\n" +
              "when matched and targetMatchingClauseColumn = s.sourceMatchingClauseColumn\n" +
              "     then delete\n" +
              "when matched and targetMatchingClauseColumn = s.sourceMatchingClauseColumn\n" +
              "     then update set privateForeignColumn = s.sourceChange\n" +
              "when not matched and 1 = s.sourceMatchingClauseColumn\n" +
              "     then insert ( privateForeignColumn ) values ( s.sourceChange )\n"
              );
        expectExecutionWarning
            ( dboConnection, NO_ROWS_AFFECTED,
              "merge into targetTable_043 t\n" +
              "using sourceTable_043 s\n" +
              "on t.targetOnClauseColumn = s.sourceOnClauseColumn\n" +
              "when matched and t.targetMatchingClauseColumn = s.sourceMatchingClauseColumn\n" +
              "     then delete\n" +
              "when matched and t.targetMatchingClauseColumn = s.sourceMatchingClauseColumn\n" +
              "     then update set t.privateForeignColumn = s.sourceChange\n" +
              "when not matched and 1 = s.sourceMatchingClauseColumn\n" +
              "     then insert ( privateForeignColumn ) values ( s.sourceChange )\n"
              );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table foreignTable_043" );
        goodStatement( dboConnection, "drop table targetTable_043" );
        goodStatement( dboConnection, "drop table sourceTable_043" );
        goodStatement( dboConnection, "drop table primaryTable_043" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_043" );
        goodStatement( dboConnection, "drop function afterTriggerFunction_043" );
        goodStatement( dboConnection, "drop function beforeTriggerFunction_043" );
        goodStatement( dboConnection, "drop type AfterTriggerType_043 restrict" );
        goodStatement( dboConnection, "drop type BeforeTriggerType_043 restrict" );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ROUTINES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Illegal function which performs sql updates */
    public  static  int illegalFunction() throws Exception
    {
        Connection  conn = getNestedConnection();

        conn.prepareStatement( "insert into t1( c2 ) values ( 1 )" ).executeUpdate();

        return 1;
    }

    /** Procedure to truncation the table which records trigger actions */
    public  static  void    truncateTriggerHistory()
    {
        _triggerHistory.clear();
    }

    /** Table function for listing the contents of the trigger record */
    public  static  ResultSet   history()
    {
        String[][]  rows = new String[ _triggerHistory.size() ][];
        _triggerHistory.toArray( rows );

        return new StringArrayVTI( TRIGGER_HISTORY_COLUMNS, rows );
    }

    /** Table function for returning some tuples of ints */
    public static IntegerArrayVTI integerList_023()
    {
        // A
        // B
        // C
        // D
        return new IntegerArrayVTI
            (
             new String[] { "A", "B", "C", "D" },
             new int[][]
             {
                 new int[] { 1, 11, 101, 1001 },
                 new int[] { 2, 12, 102, 1002 },
                 new int[] { 3, 13, 103, 1003 },
                 new int[] { 4, 14, 104, 1004 },
                 new int[] { 5, 15, 105, 1005 },
             }
             );
    }
    
    /**
     * <p>
     * Trigger-called procedure for counting rows in a candidate table and then inserting
     * the result in a history table. The history table has the following shape:
     * </p>
     *
     * <ul>
     * <li>id</li>
     * <li>actionString</li>
     * <li>rowCount</li>
     * </ul>
     */
    public  static  void    countRows
        ( String candidateName, String actionString )
        throws SQLException
    {
        Connection  conn = getNestedConnection();
        
        String  selectCount = "select count(*) from " + candidateName;
        ResultSet   selectRS = conn.prepareStatement( selectCount ).executeQuery();
        selectRS.next();
        int rowCount = selectRS.getInt( 1 );
        selectRS.close();

        addHistoryRow( actionString, rowCount );
    }

    /**
     * <p>
     * Trigger-called procedure for summing a column in a candidate table and then inserting
     * the result in a history table. The history table has the following shape:
     * </p>
     *
     * <ul>
     * <li>id</li>
     * <li>actionString</li>
     * <li>rowCount</li>
     * </ul>
     */
    public  static  void    sumColumn
        ( String candidateName, String columnName, String actionString )
        throws SQLException
    {
        Connection  conn = getNestedConnection();
        
        String  selectSum = "select sum( " + columnName + " ) from " + candidateName;
        ResultSet   selectRS = conn.prepareStatement( selectSum ).executeQuery();
        selectRS.next();
        int sum = selectRS.getInt( 1 );
        selectRS.close();

        addHistoryRow( actionString, sum );
    }

    /** Procedure for adding trigger history */
    public  static  void    addHistoryRow( String actionString, int actionValue )
    {
        _triggerHistory.add( new String[] { actionString, Integer.toString( actionValue ) } );
    }

    /** Function for returning an arbitrary integer value */
    public  static  Integer nop( Integer value ) { return value; }

    public  static  Connection  getNestedConnection()   throws SQLException
    {
        return DriverManager.getConnection( "jdbc:default:connection" );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Convert a MERGE statement which uses a nested join strategy
     * into an equivalent MERGE statement which uses a hash join
     * strategy. To do this, we replace the ON clause with an equivalent
     * ON clause which joins on key columns instead of expressions.
     * </p>
     *
     * <p>
     * The original query is a MERGE statement whose ON clauses joins
     * complex expressions, making the optimizer choose a nested-loop
     * strategy. This method transforms the MERGE statement into one
     * whose ON clause joins simple keys. This will make the optimizer
     * choose a hash-join strategy.
     * </p>
     */
    private String  makeHashJoinMerge( String original )
    {
        return original.replace ( "2 *", " " );
    }
    
}
