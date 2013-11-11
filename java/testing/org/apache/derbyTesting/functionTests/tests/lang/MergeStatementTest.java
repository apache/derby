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
        
        //
        // The following syntax is actually good, but the compiler rejects these
        // statements because we haven't finished implementing MERGE.
        //
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched then update set c2 = t2.c3\n"
              );
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 = t2.c2 then update set c2 = t2.c3\n"
              );
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
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
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
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
