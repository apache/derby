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

import java.io.File;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import junit.framework.Test;
import org.apache.derby.iapi.types.HarmonySerialBlob;
import org.apache.derby.iapi.types.HarmonySerialClob;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

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

    private static  final   String      TRACE_FILE_NAME = "mergeStatementTest.xml";

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
    private static  final   String      NO_AGGREGATE_IN_MATCHING = "42Z09";

    private static  final   String[]    TRIGGER_HISTORY_COLUMNS = new String[] { "ACTION", "ACTION_VALUE" };

    private static  final   String      BEGIN_HTML = "<html>";
    private static  final   String      END_HTML = "</html>";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // In-memory table for recording trigger actions. Each row is a { actionName, actionValue } pair.
    private static  ArrayList<String[]> _triggerHistory = new ArrayList<String[]>();

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  class   Collated    extends MergeStatementTest
    {
        public  Collated( String name ) { super( name ); }
        public  String  expectedCollation() { return "TERRITORY_BASED"; }
    }

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
    // OVERRIDABLE BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return the expected collation of this database */
    public  String  expectedCollation() { return "UCS_BASIC"; }

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
        BaseTestSuite suite = new BaseTestSuite();

        suite.addTest( standardDecoration( false ) );
        suite.addTest( standardDecoration( true ) );
        
        return suite;
    }

    /**
     * Decorate a test with standard decorators.
     */
    private static  Test    standardDecoration( boolean withCollation )
    {
        Test        cleanTest;
        if ( withCollation )
        {
            cleanTest = Decorator.territoryCollatedDatabase
                (
                 TestConfiguration.embeddedSuite( MergeStatementTest.Collated.class ),
                 "en"
                 );
        }
        else
        {
            cleanTest = new CleanDatabaseTestSetup
                (
                 TestConfiguration.embeddedSuite( MergeStatementTest.class )
                 );
        }
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( cleanTest, LEGAL_USERS, "MergeStatementPermissions" );
        Test        authorizedTest = TestConfiguration.sqlAuthorizationDecorator( authenticatedTest );

        return new SupportFilesSetup( authorizedTest );
    }

    protected void setUp() throws Exception
    {
        super.setUp();

        // in case decoration cleverness didn't really turn on sql authorization
        enableSQLAuthorization();
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

        // Variant of the above, where no table name is specified in the
        // column reference that is out of scope. Used to fail with a
        // NullPointerException (DERBY-6703).
        expectCompilationError(dboConnection, COLUMN_OUT_OF_SCOPE,
                "merge into t1 using t2 on no_such_column " +
                "when matched then delete");

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
        // allowed by sql standard, but we have problems using views
        // as the source for MERGE statements with UPDATE clauses
        //        expectExecutionWarning
        //            ( dboConnection, NO_ROWS_AFFECTED,
        //              "merge into t1\n" +
        //              "using v2\n" +
        //              "on t1.c1 = v2.c1\n" +
        //              "when matched then delete\n"
        //              );
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
        dboConnection = openUserConnection( TEST_DBO );
        goodStatement( dboConnection, "drop table t2_006" );
        goodStatement( dboConnection, "drop table t1_006" );
        truncateTriggerHistory();
    }
    private void    vet_006
        (
         String triggerDefinition,
         String[][] expectedT1Results
         )
        throws Exception
    {
        vet_006( triggerDefinition, expectedT1Results, false );
        vet_006( triggerDefinition, expectedT1Results, true );
    }
    private void    vet_006
        (
         String triggerDefinition,
         String[][] expectedT1Results,
         boolean    useHashJoinStrategy
         )
        throws Exception
    {
        vet_006( triggerDefinition, expectedT1Results, useHashJoinStrategy, false );
        vet_006( triggerDefinition, expectedT1Results, useHashJoinStrategy, true );
    }
    private void    vet_006
        (
         String triggerDefinition,
         String[][] expectedT1Results,
         boolean    useHashJoinStrategy,
         boolean    bounceDatabase    // true if we want to test the (de)serialization of the MERGE statement in the trigger
         )
        throws Exception
    {
        Connection  conn = openUserConnection( TEST_DBO );

        if ( useHashJoinStrategy ) { triggerDefinition = makeHashJoinMerge( triggerDefinition ); }

        goodStatement( conn, triggerDefinition );
        populate_006( conn );

        if ( bounceDatabase ) { conn = bounceDatabase( TEST_DBO ); }
        
        goodStatement( conn, "update t2_006 set c2 = -c2" );
        assertResults( conn, "select * from t1_006 order by c1", expectedT1Results, false );
        
        goodStatement( conn, "drop trigger trig1_006" );
    }
    private Connection  bounceDatabase( String newUser )
        throws Exception
    {
        println( "Bouncing the database..." );
        getTestConfiguration().shutdownDatabase();
        
        return openUserConnection( newUser );
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

        /* update an identity column with default should work once we change 
         * MergeStatement implementation to handle auto generated keys
        goodStatement
            ( dboConnection, 
              "merge into t1_007\n" +
              "using t2_007\n" +
              "on t1_007.c1 = t2_007.c1\n" +
              "when matched and t1_007.c2 != t2_007.c2 then update set c1 = default, c2 = t2_007.c2\n"
              );*/

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
        vet_012
            (
             "create trigger trig1_012 after update on t2_012\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_012\n" +
             "using new_cor\n" +
             "on t1_012.c2 = new_cor.c2\n" +
             "when not matched then insert ( c1, c2, c1_4 ) values ( new_cor.c1, new_cor.c2, new_cor.c4 )\n",
             update,
             new String[][]
             {
                 { "1", "1", "2", "100" },
                 { "2", "-2", "0", "-200" },
                 { "2", "2", "4", "200" },
                 { "3", "3", "6", "300" },
                 { "4", "-4", "0", "-400" },
                 { "4", "4", "8", "400" },
             }
             );

        //
        // OLD transition table as source table.
        //
        vet_012
            (
             "create trigger trig1_012 after update on t2_012\n" +
             "referencing old table as old_cor new table as new_cor\n" +
             "for each statement\n" +
             "merge into t1_012\n" +
             "using old_cor\n" +
             "on t1_012.c2 = old_cor.c2\n" +
             "when not matched then insert ( c1, c2, c1_4 ) values ( old_cor.c1, old_cor.c2, old_cor.c4 )\n",
             update,
             new String[][]
             {
                 { "1", "-1", "0", "-100" },
                 { "1", "1", "2", "100" },
                 { "2", "2", "4", "200" },
                 { "3", "-3", "0", "-300" },
                 { "3", "3", "6", "300" },
                 { "4", "4", "8", "400" },
             }
             );

        //
        // drop schema
        //
        dboConnection = openUserConnection( TEST_DBO );
        goodStatement( dboConnection, "drop table t2_012" );
        goodStatement( dboConnection, "drop table t1_012" );
    }
    private void    vet_012
        (
         String triggerDefinition,
         String update,
         String[][] expectedResults
         )
        throws Exception
    {
        vet_012( triggerDefinition, update, expectedResults, false );
        vet_012( triggerDefinition, update, expectedResults, true );
    }
    private void    vet_012
        (
         String triggerDefinition,
         String update,
         String[][] expectedResults,
         boolean    bounceDatabase  // when true we test the (de)serialization of MERGE statements inside triggers
         )
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        populate_012( dboConnection );
        goodStatement( dboConnection, triggerDefinition );
        
        if ( bounceDatabase ) { dboConnection = bounceDatabase( TEST_DBO ); }
                
        goodUpdate( dboConnection, update, 4 );
        assertResults( dboConnection, "select * from t1_012 order by c1, c2", expectedResults, false );
        goodStatement( dboConnection, "drop trigger trig1_012" );
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
        /*update an identity column with default should work once we change
        // MergeStatement implementation to handle auto generated keys
        expectCompilationError
            ( dboConnection, CANT_MODIFY_IDENTITY,
              "merge into t1_014\n" +
              "using t2_014\n" +
              "on t1_014.c2 = t2_014.c2\n" +
              "when matched then update set c1 = default, c3 = default, c2 = 2 * t2_014.c2, c5 = default\n"
              );*/

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
    public  void    atest_015_bug_6414()
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
        goodStatement
            ( dboConnection, 
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
        dboConnection = openUserConnection( TEST_DBO );
        goodStatement( dboConnection, "drop table t1_018" );
        goodStatement( dboConnection, "drop table t2_018" );
    }
    private void    vet_018
        (
         String triggerDefinition,
         String[][] expectedResults
         )
        throws Exception
    {
        vet_018( triggerDefinition, expectedResults, false );
        vet_018( triggerDefinition, expectedResults, true );
    }
    private void    vet_018
        (
         String triggerDefinition,
         String[][] expectedResults,
         boolean    bounceDatabase
         )
        throws Exception
    {
        Connection  conn = openUserConnection( TEST_DBO );

        populate_018( conn );
        goodStatement( conn, triggerDefinition );

        if ( bounceDatabase ) { conn = bounceDatabase( TEST_DBO ); }
        
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
             "create function singlerow_028() returns table\n" +
             "( x int )\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.singlerow_028'\n"
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
             "using table( singlerow_028() ) sr on t2_028.y = new.x\n" +
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
        goodStatement( dboConnection, "drop function singlerow_028" );
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
     * Verify the fix to a query involving a view. This test was disabled when
     * we disabled views as sources. See DERBY-6652.
     * </p>
     */
    //    public  void    test_031_view()
    //        throws Exception
    //    {
    //        Connection  dboConnection = openUserConnection( TEST_DBO );
    //
    //        //
    //        // create schema
    //        //
    //        goodStatement
    //            (
    //             dboConnection,
    //             "create table t1_031(x int, y int)"
    //             );
    //        goodStatement
    //            (
    //             dboConnection,
    //             "create table tv_031(x int, y int)"
    //             );
    //        goodStatement
    //            (
    //             dboConnection,
    //             "create view v_031 as select * from tv_031"
    //             );
    //        goodStatement
    //            (
    //             dboConnection,
    //             "insert into t1_031 values ( 1, 100 ), ( 2, 200 ), ( 3, 300 )"
    //             );
    //        goodStatement
    //            (
    //             dboConnection,
    //             "insert into tv_031 values ( 1, 1000 ), ( 3, 3000 ), ( 4, 4000 )"
    //             );
    //
    //        // verify the fix
    //        goodUpdate
    //            (
    //             dboConnection,
    //             "merge into t1_031\n" +
    //             "using v_031 on t1_031.x = v_031.x\n" +
    //             "when matched then update set t1_031.y = v_031.y\n",
    //             2
    //             );
    //        assertResults
    //            (
    //             dboConnection,
    //             "select * from t1_031 order by x",
    //             new String[][]
    //             {
    //                 { "1", "1000" },
    //                 { "2", "200" },
    //                 { "3", "3000" },
    //             },
    //             false
    //             );
    //
    //        //
    //        // drop schema
    //        //
    //        goodStatement( dboConnection, "drop view v_031" );
    //        goodStatement( dboConnection, "drop table t1_031" );
    //        goodStatement( dboConnection, "drop table tv_031" );
    //    }
    
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
             "create function sr_039() returns table\n" +
             "( i int )\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.singlerow_028'\n"
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
             "using table( sr_039() ) sr on ( x = 1 )\n" +
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
        goodStatement( dboConnection, "drop function sr_039" );
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
             "create function sr_040() returns table\n" +
             "( i int )\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.singlerow_028'\n"
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
             "using table( sr_040() ) sr on ( x = 1 )\n" +
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
             "using table( sr_040() ) sr on ( x = 3 )\n" +
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
        goodStatement( dboConnection, "drop function sr_040" );
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
             "create function sr_041() returns table\n" +
             "( i int )\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.tworow_041'\n"
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
              "using table( sr_041() ) sr on ( x = 1 )\n" +
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
        goodStatement( dboConnection, "drop function sr_041" );
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
    
    /**
     * <p>
     * Verify privileges needed for DELETE actions.
     * </p>
     */
    public  void    test_044_deletePrivileges()
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
             "create type SourceOnClauseType_044 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type SourceMatchingClauseType_044 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type BeforeTriggerType_044 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type AfterTriggerType_044 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceOnClauseFunction_044( hashMap SourceOnClauseType_044, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceMatchingClauseFunction_044( hashMap SourceMatchingClauseType_044, hashKey varchar( 32672 ) ) returns int\n"   +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function beforeTriggerFunction_044( hashMap BeforeTriggerType_044, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function afterTriggerFunction_044( hashMap AfterTriggerType_044, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_044\n" +
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
             "create table primaryTable_044\n" +
             "(\n" +
             "    key1 int primary key\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_044\n" +
             "(\n" +
             "    sourceUnreferencedColumn int,\n" +
             "    sourceOnClauseColumn SourceOnClauseType_044,\n" +
             "    sourceMatchingClauseColumn SourceMatchingClauseType_044\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_044\n" +
             "(\n" +
             "    privateForeignColumn int references primaryTable_044( key1 ),\n" +
             "    privatePrimaryColumn int primary key,\n" +
             "    privateBeforeTriggerSource BeforeTriggerType_044,\n" +
             "    privateAfterTriggerSource AfterTriggerType_044,\n" +
             "    targetOnClauseColumn int,\n" +
             "    targetMatchingClauseColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table foreignTable_044\n" +
             "(\n" +
             "    key1 int references targetTable_044( privatePrimaryColumn )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeDeleteTrigger_044\n" +
             "no cascade before delete on targetTable_044\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_044( 'before', beforeTriggerFunction_044( old.privateBeforeTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterDeleteTrigger_044\n" +
             "after delete on targetTable_044\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_044( 'after', afterTriggerFunction_044( old.privateAfterTriggerSource, 'foo' ) )\n"
             );

        //
        // Privileges
        //
        Permission[]    permissions = new Permission[]
        {
            new Permission( "delete on targetTable_044", NO_TABLE_PERMISSION ),
            new Permission( "execute on function sourceOnClauseFunction_044", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function sourceMatchingClauseFunction_044", NO_GENERIC_PERMISSION ),
            new Permission( "select ( sourceOnClauseColumn ) on sourceTable_044", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( sourceMatchingClauseColumn ) on sourceTable_044", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( targetOnClauseColumn ) on targetTable_044", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( targetMatchingClauseColumn ) on targetTable_044", NO_SELECT_OR_UPDATE_PERMISSION ),
        };
        for ( Permission permission : permissions )
        {
            grantPermission( dboConnection, permission.text );
        }

        //
        // Try adding and dropping privileges.
        //
        String  mergeStatement =
            "merge into test_dbo.targetTable_044\n" +
            "using test_dbo.sourceTable_044\n" +
            "on targetOnClauseColumn = test_dbo.sourceOnClauseFunction_044( sourceOnClauseColumn, 'foo' )\n" +
"when matched and targetMatchingClauseColumn = test_dbo.sourceMatchingClauseFunction_044( sourceMatchingClauseColumn, 'foo' )\n " +
            "     then delete\n";

        // ruth can execute the MERGE statement
        expectExecutionWarning( ruthConnection, NO_ROWS_AFFECTED, mergeStatement );
        
        //
        // Verify that revoking each permission in isolation raises
        // the correct error.
        //
        for ( Permission permission : permissions )
        {
            vetPermission( permission, dboConnection, ruthConnection, mergeStatement );
        }
        
        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table foreignTable_044" );
        goodStatement( dboConnection, "drop table targetTable_044" );
        goodStatement( dboConnection, "drop table sourceTable_044" );
        goodStatement( dboConnection, "drop table primaryTable_044" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_044" );
        goodStatement( dboConnection, "drop function afterTriggerFunction_044" );
        goodStatement( dboConnection, "drop function beforeTriggerFunction_044" );
        goodStatement( dboConnection, "drop function sourceMatchingClauseFunction_044" );
        goodStatement( dboConnection, "drop function sourceOnClauseFunction_044" );
        goodStatement( dboConnection, "drop type AfterTriggerType_044 restrict" );
        goodStatement( dboConnection, "drop type BeforeTriggerType_044 restrict" );
        goodStatement( dboConnection, "drop type SourceMatchingClauseType_044 restrict" );
        goodStatement( dboConnection, "drop type SourceOnClauseType_044 restrict" );
    }
    
    /**
     * Verify that the MERGE statement fails with the correct error after you revoke
     * a permission and that the MERGE statement succeeds after you add the permission back.
     */
    private void    vetPermission
        (
         Permission permission,
         Connection dboConnection,
         Connection ruthConnection,
         String mergeStatement
         )
        throws Exception
    {
        revokePermission( dboConnection, permission.text );
        expectExecutionError( ruthConnection, permission.sqlStateWhenMissing, mergeStatement );
        grantPermission( dboConnection, permission.text );
        expectExecutionWarning( ruthConnection, NO_ROWS_AFFECTED, mergeStatement );
    }
    private void    grantPermission( Connection conn, String permission )
        throws Exception
    {
        String  command = "grant " + permission + " to ruth";

        goodStatement( conn, command );
    }
    private void    revokePermission( Connection conn, String permission )
        throws Exception
    {
        String  command = "revoke " + permission + " from ruth";
        if ( permission.startsWith( "execute" ) || permission.startsWith( "usage" ) )   { command += " restrict"; }

        goodStatement( conn, command );
    }
    
    /**
     * <p>
     * Verify privileges needed for INSERT actions.
     * </p>
     */
    public  void    test_045_insertPrivileges()
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
             "create type SourceOnClauseType_045 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type SourceMatchingClauseType_045 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type SourceValueType_045 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type TargetValueType_045 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type BeforeTriggerType_045 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type AfterTriggerType_045 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceOnClauseFunction_045( hashMap SourceOnClauseType_045, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceMatchingClauseFunction_045( hashMap SourceMatchingClauseType_045, hashKey varchar( 32672 ) ) returns int\n"  +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceValueFunction_045( hashMap SourceValueType_045, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function targetValueFunction_045( hashKey varchar( 32672 ), hashValue int ) returns TargetValueType_045\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.makeHashMap'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function beforeTriggerFunction_045( hashMap BeforeTriggerType_045, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function afterTriggerFunction_045( hashMap AfterTriggerType_045, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_045\n" +
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
             "create table primaryTable_045\n" +
             "(\n" +
             "    key1 int primary key\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_045\n" +
             "(\n" +
             "    sourceUnreferencedColumn int,\n" +
             "    sourceOnClauseColumn SourceOnClauseType_045,\n" +
             "    sourceMatchingClauseColumn SourceMatchingClauseType_045,\n" +
             "    sourceValueColumn SourceValueType_045\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_045\n" +
             "(\n" +
             "    privateForeignColumn int references primaryTable_045( key1 ),\n" +
             "    privatePrimaryColumn int primary key,\n" +
             "    privateBeforeTriggerSource BeforeTriggerType_045,\n" +
             "    privateAfterTriggerSource AfterTriggerType_045,\n" +
             "    targetOnClauseColumn int,\n" +
             "    targetMatchingClauseColumn int,\n" +
             "    targetValueColumn TargetValueType_045\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table foreignTable_045\n" +
             "(\n" +
             "    key1 int references targetTable_045( privatePrimaryColumn )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeInsertTrigger_045\n" +
             "no cascade before insert on targetTable_045\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_045( 'before', beforeTriggerFunction_045( new.privateBeforeTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterInsertTrigger_045\n" +
             "after insert on targetTable_045\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_045( 'after', afterTriggerFunction_045( new.privateAfterTriggerSource, 'foo' ) )\n"
             );

        //
        // Privileges
        //
        Permission[]    permissions = new Permission[]
        {
            new Permission( "insert on targetTable_045", NO_TABLE_PERMISSION ),
            new Permission( "execute on function sourceOnClauseFunction_045", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function sourceMatchingClauseFunction_045", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function sourceValueFunction_045", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function targetValueFunction_045", NO_GENERIC_PERMISSION ),
            new Permission( "select ( sourceOnClauseColumn ) on sourceTable_045", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( sourceMatchingClauseColumn ) on sourceTable_045", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( sourceValueColumn ) on sourceTable_045", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( targetOnClauseColumn ) on targetTable_045", NO_SELECT_OR_UPDATE_PERMISSION ),
        };
        for ( Permission permission : permissions )
        {
            grantPermission( dboConnection, permission.text );
        }

        //
        // Try adding and dropping privileges.
        //
        String  mergeStatement =
            "merge into test_dbo.targetTable_045\n" +
            "using test_dbo.sourceTable_045\n" +
            "on targetOnClauseColumn = test_dbo.sourceOnClauseFunction_045( sourceOnClauseColumn, 'foo' )\n" +
            "when not matched and 1 = test_dbo.sourceMatchingClauseFunction_045( sourceMatchingClauseColumn, 'foo' )\n" +
            "     then insert ( targetValueColumn ) values\n" +
            "     (\n" +
            "        test_dbo.targetValueFunction_045( 'foo', test_dbo.sourceValueFunction_045( sourceValueColumn, 'foo' ) )\n" +
            "     )\n"
            ;

        // ruth can execute the MERGE statement
        expectExecutionWarning( ruthConnection, NO_ROWS_AFFECTED, mergeStatement );
        
        //
        // Verify that revoking each permission in isolation raises
        // the correct error.
        //
        for ( Permission permission : permissions )
        {
            vetPermission( permission, dboConnection, ruthConnection, mergeStatement );
        }
        
        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table foreignTable_045" );
        goodStatement( dboConnection, "drop table targetTable_045" );
        goodStatement( dboConnection, "drop table sourceTable_045" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_045" );
        goodStatement( dboConnection, "drop function afterTriggerFunction_045" );
        goodStatement( dboConnection, "drop function beforeTriggerFunction_045" );
        goodStatement( dboConnection, "drop function targetValueFunction_045" );
        goodStatement( dboConnection, "drop function sourceValueFunction_045" );
        goodStatement( dboConnection, "drop function sourceMatchingClauseFunction_045" );
        goodStatement( dboConnection, "drop function sourceOnClauseFunction_045" );
        goodStatement( dboConnection, "drop type AfterTriggerType_045 restrict" );
        goodStatement( dboConnection, "drop type BeforeTriggerType_045 restrict" );
        goodStatement( dboConnection, "drop type TargetValueType_045 restrict" );
        goodStatement( dboConnection, "drop type SourceValueType_045 restrict" );
        goodStatement( dboConnection, "drop type SourceMatchingClauseType_045 restrict" );
        goodStatement( dboConnection, "drop type SourceOnClauseType_045 restrict" );
    }
    
    /**
     * <p>
     * Verify UDT privileges for CASTs in INSERT and DELETE actions.
     * </p>
     */
    public  void    test_046_udtCasts()
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
             "create type OnClauseType_046 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type MatchingClauseType_046 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type TargetValueType_046 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_046\n" +
             "(\n" +
             "    sourceUnreferencedColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_046\n" +
             "(\n" +
             "    privateUnreferencedColumn int,\n" +
             "    publicUnreferencedColumn int,\n" +
             "    targetValueColumn TargetValueType_046\n" +
             ")\n"
             );

        //
        // Privileges
        //
        goodStatement
            (
             dboConnection,
             "grant select ( publicUnreferencedColumn ) on targetTable_046 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant insert on targetTable_046 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant delete on targetTable_046 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant select on sourceTable_046 to ruth"
             );

        // the statements
        String  insertStatement =
            "merge into test_dbo.targetTable_046\n" +
            "using test_dbo.sourceTable_046\n" +
            "on cast( null as test_dbo.OnClauseType_046 ) is null\n" +
            "when not matched and cast( null as test_dbo.MatchingClauseType_046 ) is null\n" +
            "     then insert ( targetValueColumn ) values ( cast( null as test_dbo.TargetValueType_046 ) )\n";
        String  deleteStatement =
            "merge into test_dbo.targetTable_046\n" +
            "using test_dbo.sourceTable_046\n" +
            "on cast( null as test_dbo.OnClauseType_046 ) is null\n" +
            "when matched and cast( null as test_dbo.MatchingClauseType_046 ) is null\n" +
            "     then delete\n";

        // fails because ruth doesn't have USAGE priv on MatchingClauseType_046
        expectExecutionError( ruthConnection, NO_GENERIC_PERMISSION, insertStatement );
        expectExecutionError( ruthConnection, NO_GENERIC_PERMISSION, deleteStatement );

        goodStatement( dboConnection, "grant usage on type MatchingClauseType_046 to ruth" );

        // fails because ruth doesn't have USAGE priv on OnClauseType_046
        expectExecutionError( ruthConnection, NO_GENERIC_PERMISSION, insertStatement );
        expectExecutionError( ruthConnection, NO_GENERIC_PERMISSION, deleteStatement );

        goodStatement( dboConnection, "grant usage on type OnClauseType_046 to ruth" );
        
        // fails because ruth doesn't have USAGE priv on TargetValueType_046
        expectExecutionError( ruthConnection, NO_GENERIC_PERMISSION, insertStatement );

        goodStatement( dboConnection, "grant usage on type TargetValueType_046 to ruth" );

        // now the statements succeed
        expectExecutionWarning( ruthConnection, NO_ROWS_AFFECTED, insertStatement );
        expectExecutionWarning( ruthConnection, NO_ROWS_AFFECTED, deleteStatement );
        
        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table sourceTable_046" );
        goodStatement( dboConnection, "drop table targetTable_046" );
        goodStatement( dboConnection, "drop type TargetValueType_046 restrict" );
        goodStatement( dboConnection, "drop type MatchingClauseType_046 restrict" );
        goodStatement( dboConnection, "drop type OnClauseType_046 restrict" );
    }
    
    /**
     * <p>
     * Verify privileges needed for UPDATE actions.
     * </p>
     */
    public  void    test_047_updatePrivileges()
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
             "create type SourceOnClauseType_047 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type SourceMatchingClauseType_047 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type SourceValueType_047 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type TargetValueType_047 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type TargetValueInputType_047 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type BeforeTriggerType_047 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type AfterTriggerType_047 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceOnClauseFunction_047( hashMap SourceOnClauseType_047, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceMatchingClauseFunction_047( hashMap SourceMatchingClauseType_047, hashKey varchar( 32672 ) ) returns int\n"  +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceValueFunction_047( hashMap SourceValueType_047, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function targetValueInputFunction_047( hashMap TargetValueInputType_047, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function targetValueFunction_047( hashKey varchar( 32672 ), hashValue int ) returns TargetValueType_047\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.makeHashMap'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function beforeTriggerFunction_047( hashMap BeforeTriggerType_047, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function afterTriggerFunction_047( hashMap AfterTriggerType_047, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_047\n" +
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
             "create table primaryTable_047\n" +
             "(\n" +
             "    key1 int primary key\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_047\n" +
             "(\n" +
             "    sourceUnreferencedColumn int,\n" +
             "    sourceOnClauseColumn SourceOnClauseType_047,\n" +
             "    sourceMatchingClauseColumn SourceMatchingClauseType_047,\n" +
             "    sourceValueColumn SourceValueType_047\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_047\n" +
             "(\n" +
             "    privateForeignColumn int references primaryTable_047( key1 ),\n" +
             "    privatePrimaryColumn int primary key,\n" +
             "    privateBeforeTriggerSource BeforeTriggerType_047,\n" +
             "    privateAfterTriggerSource AfterTriggerType_047,\n" +
             "    targetOnClauseColumn int,\n" +
             "    targetMatchingClauseColumn int,\n" +
             "    targetValueInputColumn TargetValueInputType_047,\n" +
             "    targetValueColumn TargetValueType_047\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table foreignTable_047\n" +
             "(\n" +
             "    key1 int references targetTable_047( privatePrimaryColumn )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeUpdateTrigger_047\n" +
             "no cascade before update on targetTable_047\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_047( 'before', beforeTriggerFunction_047( new.privateBeforeTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterUpdateTrigger_047\n" +
             "after update on targetTable_047\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_047( 'after', afterTriggerFunction_047( new.privateAfterTriggerSource, 'foo' ) )\n"
             );

        //
        // Privileges
        //
        Permission[]    permissions = new Permission[]
        {
            new Permission( "update ( targetValueColumn ) on targetTable_047", LACK_COLUMN_PRIV ),
            new Permission( "execute on function sourceOnClauseFunction_047", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function sourceMatchingClauseFunction_047", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function sourceValueFunction_047", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function targetValueInputFunction_047", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function targetValueFunction_047", NO_GENERIC_PERMISSION ),
            new Permission( "select ( sourceOnClauseColumn ) on sourceTable_047", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( sourceMatchingClauseColumn ) on sourceTable_047", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( sourceValueColumn ) on sourceTable_047", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( targetOnClauseColumn ) on targetTable_047", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( targetMatchingClauseColumn ) on targetTable_047", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( targetValueInputColumn ) on targetTable_047", NO_SELECT_OR_UPDATE_PERMISSION ),
        };
        for ( Permission permission : permissions )
        {
            grantPermission( dboConnection, permission.text );
        }

        //
        // Try adding and dropping privileges.
        //
        String  mergeStatement =
            "merge into test_dbo.targetTable_047\n" +
            "using test_dbo.sourceTable_047\n" +
            "on targetOnClauseColumn = test_dbo.sourceOnClauseFunction_047( sourceOnClauseColumn, 'foo' )\n" +
            "when matched\n" +
            "  and targetMatchingClauseColumn = test_dbo.sourceMatchingClauseFunction_047( sourceMatchingClauseColumn, 'foo' )\n" +
            "     then update set targetValueColumn =\n" +
            "     test_dbo.targetValueFunction_047\n" +
            "     (\n" +
            "        'foo',\n" +
            "        test_dbo.sourceValueFunction_047( sourceValueColumn, 'foo' ) +\n" +
            "        test_dbo.targetValueInputFunction_047( targetValueInputColumn, 'foo' )\n" +
            "     )\n"
            ;

        // ruth can execute the MERGE statement
        expectExecutionWarning( ruthConnection, NO_ROWS_AFFECTED, mergeStatement );
        
        //
        // Verify that revoking each permission in isolation raises
        // the correct error.
        //
        for ( Permission permission : permissions )
        {
            vetPermission( permission, dboConnection, ruthConnection, mergeStatement );
        }
        
        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table foreignTable_047" );
        goodStatement( dboConnection, "drop table targetTable_047" );
        goodStatement( dboConnection, "drop table sourceTable_047" );
        goodStatement( dboConnection, "drop table primaryTable_047" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_047" );
        goodStatement( dboConnection, "drop function afterTriggerFunction_047" );
        goodStatement( dboConnection, "drop function beforeTriggerFunction_047" );
        goodStatement( dboConnection, "drop function targetValueFunction_047" );
        goodStatement( dboConnection, "drop function targetValueInputFunction_047" );
        goodStatement( dboConnection, "drop function sourceValueFunction_047" );
        goodStatement( dboConnection, "drop function sourceMatchingClauseFunction_047" );
        goodStatement( dboConnection, "drop function sourceOnClauseFunction_047" );
        goodStatement( dboConnection, "drop type AfterTriggerType_047 restrict" );
        goodStatement( dboConnection, "drop type BeforeTriggerType_047 restrict" );
        goodStatement( dboConnection, "drop type TargetValueInputType_047 restrict" );
        goodStatement( dboConnection, "drop type TargetValueType_047 restrict" );
        goodStatement( dboConnection, "drop type SourceValueType_047 restrict" );
        goodStatement( dboConnection, "drop type SourceMatchingClauseType_047 restrict" );
        goodStatement( dboConnection, "drop type SourceOnClauseType_047 restrict" );
    }
    
    /**
     * <p>
     * Verify privileges needed for CASTs involving UPDATE actions.
     * </p>
     */
    public  void    test_048_updateUdtCasts()
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
             "create type SourceOnClauseType_048 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type SourceMatchingClauseType_048 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type TargetValueType_048 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_048\n" +
             "(\n" +
             "    sourceUnreferencedColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_048\n" +
             "(\n" +
             "    targetValueColumn TargetValueType_048\n" +
             ")\n"
             );

        //
        // Privileges
        //
        goodStatement
            (
             dboConnection,
             "grant update ( targetValueColumn ) on targetTable_048 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant select on sourceTable_048 to ruth"
             );

        // the statement
        String  updateStatement =
            "merge into test_dbo.targetTable_048\n" +
            "using test_dbo.sourceTable_048\n" +
            "on cast( null as test_dbo.SourceOnClauseType_048 ) is not null\n" +
            "when matched and cast( null as test_dbo.SourceMatchingClauseType_048 ) is not null\n" +
            "     then update set targetValueColumn = cast( null as test_dbo.TargetValueType_048 )\n";

        // fails because ruth doesn't have USAGE priv on SourceMatchingClauseType_048
        expectExecutionError( ruthConnection, NO_GENERIC_PERMISSION, updateStatement );
        goodStatement( dboConnection, "grant usage on type SourceMatchingClauseType_048 to ruth" );

        // fails because ruth doesn't have USAGE priv on TargetValueType_048
        expectExecutionError( ruthConnection, NO_GENERIC_PERMISSION, updateStatement );
        goodStatement( dboConnection, "grant usage on type TargetValueType_048 to ruth" );

        // fails because ruth doesn't have USAGE priv on SourceOnClauseType_048
        expectExecutionError( ruthConnection, NO_GENERIC_PERMISSION, updateStatement );
        goodStatement( dboConnection, "grant usage on type SourceOnClauseType_048 to ruth" );

        // now ruth can run the MERGE statement
        goodStatement( ruthConnection, updateStatement );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table targetTable_048" );
        goodStatement( dboConnection, "drop table sourceTable_048" );
        goodStatement( dboConnection, "drop type TargetValueType_048 restrict" );
        goodStatement( dboConnection, "drop type SourceMatchingClauseType_048 restrict" );
        goodStatement( dboConnection, "drop type SourceOnClauseType_048 restrict" );
    }
    
    /**
     * <p>
     * Verify privileges needed for all actions.
     * </p>
     */
    public  void    test_049_allPrivileges()
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
             "create type SourceOnClauseType_049 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type SourceMatchingClauseType_049 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type SourceValueType_049 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type TargetValueType_049 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type TargetValueInputType_049 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type BeforeTriggerType_049 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type AfterTriggerType_049 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceOnClauseFunction_049( hashMap SourceOnClauseType_049, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceMatchingClauseFunction_049( hashMap SourceMatchingClauseType_049, hashKey varchar( 32672 ) ) returns int\n"  +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceValueFunction_049( hashMap SourceValueType_049, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'"
             );
        goodStatement
            (
             dboConnection,
             "create function targetValueInputFunction_049( hashMap TargetValueInputType_049, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'"
             );
        goodStatement
            (
             dboConnection,
             "create function targetValueFunction_049( hashKey varchar( 32672 ), hashValue int ) returns TargetValueType_049\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.makeHashMap'"
             );
        goodStatement
            (
             dboConnection,
             "create function beforeTriggerFunction_049( hashMap BeforeTriggerType_049, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'"
             );
        goodStatement
            (
             dboConnection,
             "create function afterTriggerFunction_049( hashMap AfterTriggerType_049, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_049\n" +
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
             "create table primaryTable_049\n" +
             "(\n" +
             "    key1 int primary key\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_049\n" +
             "(\n" +
             "    sourceUnreferencedColumn int,\n" +
             "    sourceOnClauseColumn SourceOnClauseType_049,\n" +
             "    sourceMatchingClauseColumn SourceMatchingClauseType_049,\n" +
             "    sourceValueColumn SourceValueType_049\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_049\n" +
             "(\n" +
             "    privateForeignColumn int references primaryTable_049( key1 ),\n" +
             "    privatePrimaryColumn int primary key,\n" +
             "    privateBeforeTriggerSource BeforeTriggerType_049,\n" +
             "    privateAfterTriggerSource AfterTriggerType_049,\n" +
             "    targetOnClauseColumn int,\n" +
             "    targetMatchingClauseColumn int,\n" +
             "    targetValueInputColumn TargetValueInputType_049,\n" +
             "    targetValueColumn TargetValueType_049\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table foreignTable_049\n" +
             "(\n" +
             "    key1 int references targetTable_049( privatePrimaryColumn )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeDeleteTrigger_049\n" +
             "no cascade before delete on targetTable_049\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_049( 'before', beforeTriggerFunction_049( old.privateBeforeTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterDeleteTrigger_049\n" +
             "after delete on targetTable_049\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_049( 'after', afterTriggerFunction_049( old.privateAfterTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeInsertTrigger_049\n" +
             "no cascade before insert on targetTable_049\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_049( 'before', beforeTriggerFunction_049( new.privateBeforeTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterInsertTrigger_049\n" +
             "after insert on targetTable_049\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_049( 'after', afterTriggerFunction_049( new.privateAfterTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeUpdateTrigger_049\n" +
             "no cascade before update on targetTable_049\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_049( 'before', beforeTriggerFunction_049( new.privateBeforeTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterUpdateTrigger_049\n" +
             "after update on targetTable_049\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_049( 'after', afterTriggerFunction_049( new.privateAfterTriggerSource, 'foo' ) )\n"
             );

        //
        // Privileges
        //
        Permission[]    permissions = new Permission[]
        {
            new Permission( "delete on targetTable_049", NO_TABLE_PERMISSION ),
            new Permission( "insert on targetTable_049", NO_TABLE_PERMISSION ),
            new Permission( "update ( targetValueColumn ) on targetTable_049", LACK_COLUMN_PRIV ),
            new Permission( "execute on function sourceOnClauseFunction_049", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function sourceMatchingClauseFunction_049", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function sourceValueFunction_049", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function targetValueInputFunction_049", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function targetValueFunction_049", NO_GENERIC_PERMISSION ),
            new Permission( "select ( sourceOnClauseColumn ) on sourceTable_049", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( sourceMatchingClauseColumn ) on sourceTable_049", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( sourceValueColumn ) on sourceTable_049", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( targetOnClauseColumn ) on targetTable_049", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( targetMatchingClauseColumn ) on targetTable_049", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( targetValueInputColumn ) on targetTable_049", NO_SELECT_OR_UPDATE_PERMISSION ),
        };
        for ( Permission permission : permissions )
        {
            grantPermission( dboConnection, permission.text );
        }

        //
        // Try adding and dropping privileges.
        //
        String  mergeStatement =
            "merge into test_dbo.targetTable_049\n" +
            "using test_dbo.sourceTable_049\n" +
            "on targetOnClauseColumn = test_dbo.sourceOnClauseFunction_049( sourceOnClauseColumn, 'foo' )\n" +
            "when matched\n" +
            "  and targetMatchingClauseColumn = test_dbo.sourceMatchingClauseFunction_049( sourceMatchingClauseColumn, 'foo' )\n" +
            "     then update set targetValueColumn =\n" +
            "     test_dbo.targetValueFunction_049\n" +
            "     (\n" +
            "        'foo',\n" +
            "        test_dbo.sourceValueFunction_049( sourceValueColumn, 'foo' ) +\n" +
            "        test_dbo.targetValueInputFunction_049( targetValueInputColumn, 'foo' )\n" +
            "     )\n" +
            "when matched\n" +
            "  and targetMatchingClauseColumn = 2 * test_dbo.sourceMatchingClauseFunction_049( sourceMatchingClauseColumn, 'foo' )\n" +
            "     then delete\n" +
            "when not matched\n" +
            "  and 0 = test_dbo.sourceMatchingClauseFunction_049( sourceMatchingClauseColumn, 'foo' )\n" +
            "     then insert( targetValueColumn ) values\n" +
            "     (\n" +
            "        test_dbo.targetValueFunction_049\n" +
            "        ( 'foo', test_dbo.sourceValueFunction_049( sourceValueColumn, 'foo' ) )\n" +
            "     )\n";

        // ruth can execute the MERGE statement
        expectExecutionWarning( ruthConnection, NO_ROWS_AFFECTED, mergeStatement );
        
        //
        // Verify that revoking each permission in isolation raises
        // the correct error.
        //
        for ( Permission permission : permissions )
        {
            vetPermission( permission, dboConnection, ruthConnection, mergeStatement );
        }
        
        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table foreignTable_049" );
        goodStatement( dboConnection, "drop table targetTable_049" );
        goodStatement( dboConnection, "drop table sourceTable_049" );
        goodStatement( dboConnection, "drop table primaryTable_049" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_049" );
        goodStatement( dboConnection, "drop function afterTriggerFunction_049" );
        goodStatement( dboConnection, "drop function beforeTriggerFunction_049" );
        goodStatement( dboConnection, "drop function targetValueFunction_049" );
        goodStatement( dboConnection, "drop function targetValueInputFunction_049" );
        goodStatement( dboConnection, "drop function sourceValueFunction_049" );
        goodStatement( dboConnection, "drop function sourceMatchingClauseFunction_049" );
        goodStatement( dboConnection, "drop function sourceOnClauseFunction_049" );
        goodStatement( dboConnection, "drop type AfterTriggerType_049 restrict" );
        goodStatement( dboConnection, "drop type BeforeTriggerType_049 restrict" );
        goodStatement( dboConnection, "drop type TargetValueInputType_049 restrict" );
        goodStatement( dboConnection, "drop type TargetValueType_049 restrict" );
        goodStatement( dboConnection, "drop type SourceValueType_049 restrict" );
        goodStatement( dboConnection, "drop type SourceMatchingClauseType_049 restrict" );
        goodStatement( dboConnection, "drop type SourceOnClauseType_049 restrict" );
    }
    
    /**
     * <p>
     * Test all datatypes in ON clauses, matching restrictions, and as INSERT/UPDATE values.
     * </p>
     */
    public  void    test_050_allDatatypes()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create function lv_equals_050( leftV long varchar, rightV long varchar ) returns boolean\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.equals'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function clob_equals_050( leftV clob, rightV clob ) returns boolean\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.equals'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function vb( inputVal int... ) returns varchar( 10 ) for bit data\n" +
             "language java parameter style derby deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.makeByteArray'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function vb_reverse( inputVal varchar( 10 ) for bit data ) returns varchar( 10 ) for bit data\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.reverse'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function vb_add( leftV varchar( 10 ) for bit data, rightV varchar( 10 ) for bit data ) returns varchar( 10 ) for bit data\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.add'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function ch( inputVal int... ) returns char( 10 ) for bit data\n" +
             "language java parameter style derby deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.makeByteArray'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function ch_reverse( inputVal char( 10 ) for bit data ) returns char( 10 ) for bit data\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.reverse'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function ch_add( leftV char( 10 ) for bit data, rightV char( 10 ) for bit data ) returns char( 10 ) for bit data\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.add'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function vbl( inputVal int... ) returns blob\n" +
             "language java parameter style derby deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.makeBlob'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function vbl_reverse( inputVal blob ) returns blob\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.reverse'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function vbl_add( leftV blob, rightV blob ) returns blob\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.add'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function vbl_equals( leftV blob, rightV blob ) returns boolean\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.equals'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function xmlX( val varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.xmlX'\n"
             );
        goodStatement
            (
             dboConnection,
             "create type IntArray external name 'org.apache.derbyTesting.functionTests.tests.lang.IntArray' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function mia( vals int... ) returns IntArray\n" +
             "language java parameter style derby deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.IntArray.makeIntArray'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function gc( val IntArray, cellNumber int ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.IntArray.getCell'\n"
             );

        //
        // Numeric types
        //
        
        String  intTargetValues =
            "( 1, 'orig', 1, 100, 1000 ), ( 2, 'orig: will delete', 1, 101, 1001 ), ( 3, 'orig: will update', 2, 200, 2000 ),\n" +
            "( 4, 'orig', 2, 201, 2000 ), ( 5, 'orig', 4, 400, 4000 )\n";
        String  intSourceValues =
            "( 1, 101, 10000 ), ( 2, -200, -20000 ), ( 3, 300, 30000 ), ( 5, 5, -5 )\n";
        String  intMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on t.onClauseColumn = s.onClauseColumn\n" +
            "when matched and t.matchingClauseColumn = s.matchingClauseColumn\n" +
            "     then delete\n" +
            "when matched and t.matchingClauseColumn = -s.matchingClauseColumn\n" +
            "     then update set valueColumn = t.valueColumn + s.valueColumn, description = 'updated'\n" +
            "when not matched and s.onClauseColumn = s.matchingClauseColumn\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        String[][]  intExpectedValues = new String[][]
        {
            { "1", "orig", "1", "100", "1000" },
            { "3", "updated", "2", "200", "-18000" },
            { "4", "orig", "2", "201", "2000" },
            { "5", "orig", "4", "400", "4000" },
            { "6", "inserted", "5", "5", "-5" },
        };

        vet_050( dboConnection, "int", true, intTargetValues, intSourceValues, intMergeStatement, intExpectedValues );
        vet_050( dboConnection, "bigint", true, intTargetValues, intSourceValues, intMergeStatement, intExpectedValues );
        vet_050( dboConnection, "smallint", true, intTargetValues, intSourceValues, intMergeStatement, intExpectedValues );
        vet_050( dboConnection, "decimal", true, intTargetValues, intSourceValues, intMergeStatement, intExpectedValues );
        vet_050( dboConnection, "numeric", true, intTargetValues, intSourceValues, intMergeStatement, intExpectedValues );

        String[][]  doubleExpectedValues = new String[][]
        {
            { "1", "orig", "1.0", "100.0", "1000.0" },
            { "3", "updated", "2.0", "200.0", "-18000.0" },
            { "4", "orig", "2.0", "201.0", "2000.0" },
            { "5", "orig", "4.0", "400.0", "4000.0" },
            { "6", "inserted", "5.0", "5.0", "-5.0" },
        };

        vet_050( dboConnection, "double", true, intTargetValues, intSourceValues, intMergeStatement, doubleExpectedValues );
        vet_050( dboConnection, "float", true, intTargetValues, intSourceValues, intMergeStatement, doubleExpectedValues );
        vet_050( dboConnection, "real", true, intTargetValues, intSourceValues, intMergeStatement, doubleExpectedValues );
        
        //
        // String types
        //
        
        String  stringTargetValues =
            "( 1, 'orig', 'b', 'baa', 'baaa' ), ( 2, 'orig: will delete', 'b', 'bab', 'baab' ), ( 3, 'orig: will update', 'c', 'caa', 'caaa' ),\n" +
            "( 4, 'orig', 'c', 'cab', 'caaa' ), ( 5, 'orig', 'e', 'eaa', 'eaaa' )\n";
        String  stringSourceValues =
            "( 'b', 'bab', 'baaaa' ), ( 'c', '-caa', '-caaaa' ), ( 'd', 'daa', 'daaaa' ), ( 'f', 'f', '-f' )\n";
        String  stringMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on t.onClauseColumn = s.onClauseColumn\n" +
            "when matched and t.matchingClauseColumn = s.matchingClauseColumn\n" +
            "     then delete\n" +
            "when matched and '-' || trim( t.matchingClauseColumn ) = trim( s.matchingClauseColumn )\n" +
            "     then update set valueColumn = trim( t.valueColumn ) || trim( s.valueColumn ), description = 'updated'\n" +
            "when not matched and s.onClauseColumn = s.matchingClauseColumn\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        String[][]  stringExpectedValues = new String[][]
        {
            { "1", "orig", "b", "baa", "baaa" },
            { "3", "updated", "c", "caa", "caaa-caaaa" },
            { "4", "orig", "c", "cab", "caaa" },
            { "5", "orig", "e", "eaa", "eaaa" },
            { "6", "inserted", "f", "f", "-f" },
        };

        vet_050
            ( dboConnection, "varchar( 10 )", true, stringTargetValues, stringSourceValues, stringMergeStatement, stringExpectedValues );
        vet_050
            ( dboConnection, "char( 10 )", true, stringTargetValues, stringSourceValues, stringMergeStatement, stringExpectedValues );

        String  lvMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on lv_equals_050( t.onClauseColumn, s.onClauseColumn )\n" +
            "when matched and lv_equals_050( t.matchingClauseColumn, s.matchingClauseColumn )\n" +
            "     then delete\n" +
            "when matched and lv_equals_050( '-' || t.matchingClauseColumn, s.matchingClauseColumn )\n" +
            "     then update set valueColumn = t.valueColumn || s.valueColumn, description = 'updated'\n" +
            "when not matched and lv_equals_050( s.onClauseColumn, s.matchingClauseColumn )\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        
        vet_050
            ( dboConnection, "long varchar", false, stringTargetValues, stringSourceValues, lvMergeStatement, stringExpectedValues );

        String  clobMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on clob_equals_050( t.onClauseColumn, s.onClauseColumn )\n" +
            "when matched and clob_equals_050( t.matchingClauseColumn, s.matchingClauseColumn )\n" +
            "     then delete\n" +
            "when matched and clob_equals_050( '-' || t.matchingClauseColumn, s.matchingClauseColumn )\n" +
            "     then update set valueColumn = t.valueColumn || s.valueColumn, description = 'updated'\n" +
            "when not matched and clob_equals_050( s.onClauseColumn, s.matchingClauseColumn )\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        
        vet_050
            ( dboConnection, "clob", false, stringTargetValues, stringSourceValues, clobMergeStatement, stringExpectedValues );
        
        
        //
        // Binary types
        //
        
        String  varbinaryTargetValues =
            "( 1, 'orig', vb( 1 ), vb( 1, 0, 0 ), vb( 1, 0, 0, 0 ) ),\n" +
            "( 2, 'orig: will delete', vb( 1 ), vb( 1, 0, 1 ), vb( 1, 0, 0, 1 ) ),\n" +
            "( 3, 'orig: will update', vb( 2 ), vb( 2, 0, 0 ), vb( 2, 0, 0, 0 ) ),\n" +
            "( 4, 'orig', vb( 2 ), vb( 2, 0, 1 ), vb( 2, 0, 0, 0 ) ),\n" +
            "( 5, 'orig', vb( 4 ), vb( 4, 0, 0 ), vb( 4, 0, 0, 0 ) )\n";
        String  varbinarySourceValues =
            "( vb( 1 ), vb( 1, 0, 1 ), vb( 1, 0, 0, 0, 0 ) ),\n" +
            "( vb( 2 ), vb( 0, 0, 2 ), vb( -2, 0, 0, 0, 0 ) ),\n" +
            "( vb( 3 ), vb( 3, 0, 0 ), vb( 3, 0, 0, 0, 0 ) ),\n" +
            "( vb( 5 ), vb( 5 ), vb( -5 ) )\n";
        String  varbinaryMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on t.onClauseColumn = s.onClauseColumn\n" +
            "when matched and t.matchingClauseColumn = s.matchingClauseColumn\n" +
            "     then delete\n" +
            "when matched and t.matchingClauseColumn = vb_reverse( s.matchingClauseColumn )\n" +
            "     then update set valueColumn = vb_add( t.valueColumn, s.valueColumn ), description = 'updated'\n" +
            "when not matched and s.onClauseColumn = s.matchingClauseColumn\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        String[][]  varbinaryExpectedValues = new String[][]
        {
            { "1", "orig", "01", "010000", "01000000" },
            { "3", "updated", "02", "020000", "00000000" },
            { "4", "orig", "02", "020001", "02000000" },
            { "5", "orig", "04", "040000", "04000000" },
            { "6", "inserted", "05", "05", "fb" },
        };

        vet_050
            ( dboConnection, "varchar( 10 ) for bit data", true, varbinaryTargetValues, varbinarySourceValues,
              varbinaryMergeStatement, varbinaryExpectedValues );
        
        String  binaryTargetValues =
            "( 1, 'orig', ch( 1 ), ch( 1, 0, 0 ), ch( 1, 0, 0, 0 ) ),\n" +
            "( 2, 'orig: will delete', ch( 1 ), ch( 1, 0, 1 ), ch( 1, 0, 0, 1 ) ),\n" +
            "( 3, 'orig: will update', ch( 2 ), ch( 2, 0, 0, 0, 0, 0, 0, 0, 0, 0 ), ch( 2, 0, 0, 0 ) ),\n" +
            "( 4, 'orig', ch( 2 ), ch( 2, 0, 1 ), ch( 2, 0, 0, 0 ) ),\n" +
            "( 5, 'orig', ch( 4 ), ch( 4, 0, 0 ), ch( 4, 0, 0, 0 ) )\n";
        String  binarySourceValues =
            "( ch( 1 ), ch( 1, 0, 1 ), ch( 1, 0, 0, 0, 0 ) ),\n" +
            "( ch( 2 ), ch( 0, 0, 0, 0, 0, 0, 0, 0, 0, 2 ), ch( -2, 0, 0, 0, 0 ) ),\n" +
            "( ch( 3 ), ch( 3, 0, 0 ), ch( 3, 0, 0, 0, 0 ) ),\n" +
            "( ch( 5 ), ch( 5 ), ch( -5 ) )\n";
        String  binaryMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on t.onClauseColumn = s.onClauseColumn\n" +
            "when matched and t.matchingClauseColumn = s.matchingClauseColumn\n" +
            "     then delete\n" +
            "when matched and t.matchingClauseColumn = ch_reverse( s.matchingClauseColumn )\n" +
            "     then update set valueColumn = ch_add( t.valueColumn, s.valueColumn ), description = 'updated'\n" +
            "when not matched and s.onClauseColumn = s.matchingClauseColumn\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        String[][]  binaryExpectedValues = new String[][]
        {
            { "1", "orig", "01202020202020202020", "01000020202020202020", "01000000202020202020" },
            { "3", "updated", "02202020202020202020", "02000000000000000000", "00000000204040404040" },
            { "4", "orig", "02202020202020202020", "02000120202020202020", "02000000202020202020" },
            { "5", "orig", "04202020202020202020", "04000020202020202020", "04000000202020202020" },
            { "6", "inserted", "05202020202020202020", "05202020202020202020", "fb202020202020202020" },
        };

        vet_050
            ( dboConnection, "char( 10 ) for bit data", true, binaryTargetValues, binarySourceValues,
              binaryMergeStatement, binaryExpectedValues );
        
        String  blobTargetValues =
            "( 1, 'orig', vbl( 1 ), vbl( 1, 0, 0 ), vbl( 1, 0, 0, 0 ) ),\n" +
            "( 2, 'orig: will delete', vbl( 1 ), vbl( 1, 0, 1 ), vbl( 1, 0, 0, 1 ) ),\n" +
            "( 3, 'orig: will update', vbl( 2 ), vbl( 2, 0, 0 ), vbl( 2, 0, 0, 0 ) ),\n" +
            "( 4, 'orig', vbl( 2 ), vbl( 2, 0, 1 ), vbl( 2, 0, 0, 0 ) ),\n" +
            "( 5, 'orig', vbl( 4 ), vbl( 4, 0, 0 ), vbl( 4, 0, 0, 0 ) )\n";
        String  blobSourceValues =
            "( vbl( 1 ), vbl( 1, 0, 1 ), vbl( 1, 0, 0, 0, 0 ) ),\n" +
            "( vbl( 2 ), vbl( 0, 0, 2 ), vbl( -2, 0, 0, 0, 0 ) ),\n" +
            "( vbl( 3 ), vbl( 3, 0, 0 ), vbl( 3, 0, 0, 0, 0 ) ),\n" +
            "( vbl( 5 ), vbl( 5 ), vbl( -5 ) )\n";
        String  blobMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on vbl_equals( t.onClauseColumn, s.onClauseColumn )\n" +
            "when matched and vbl_equals( t.matchingClauseColumn, s.matchingClauseColumn )\n" +
            "     then delete\n" +
            "when matched and vbl_equals( t.matchingClauseColumn, vbl_reverse( s.matchingClauseColumn ) )\n" +
            "     then update set valueColumn = vbl_add( t.valueColumn, s.valueColumn ), description = 'updated'\n" +
            "when not matched and vbl_equals( s.onClauseColumn, s.matchingClauseColumn )\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";

        vet_050
            ( dboConnection, "blob", false, blobTargetValues, blobSourceValues,
              blobMergeStatement, varbinaryExpectedValues );

        //
        // Boolean
        //
        
        String  booleanTargetValues =
            "( 1, 'orig', true, true, false ), ( 2, 'orig: will delete', true, true, true ), ( 3, 'orig: will update', false, false, true )\n";
        String  booleanSourceValues =
            "( true, true, true ), ( null, true, false ), ( false, false, false )\n";
        String  booleanMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on t.onClauseColumn = s.onClauseColumn and t.valueColumn\n" +
            "when matched and t.matchingClauseColumn = s.matchingClauseColumn and s.valueColumn\n" +
            "     then delete\n" +
            "when matched and t.matchingClauseColumn = s.matchingClauseColumn\n" +
            "     then update set valueColumn = t.valueColumn and s.valueColumn, description = 'updated'\n" +
            "when not matched and s.matchingClauseColumn\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        String[][]  booleanExpectedValues = new String[][]
        {
            { "1", "orig", "true", "true", "false" },
            { "3", "updated", "false", "false", "false" },
            { "6", "inserted", null, "true", "false" },
        };

        vet_050
            ( dboConnection, "boolean", true, booleanTargetValues, booleanSourceValues,
              booleanMergeStatement, booleanExpectedValues );
        
        //
        // Date
        //
        
        String  dateTargetValues =
            "( 1, 'orig', date( '0001-02-23' ), date( '0100-02-23' ), date( '1000-02-23' ) ),\n" +
            "( 2, 'orig: will delete', date( '0001-02-23' ), date( '0101-02-23' ), date( '1001-02-23' ) ),\n" +
            "( 3, 'orig: will update', date( '0002-02-23' ), date( '0200-02-23' ), date( '2000-02-23' ) ),\n" +
            "( 4, 'orig', date( '0002-02-23' ), date( '0201-02-23' ), date( '2000-02-23' ) ),\n" +
            "( 5, 'orig', date( '0004-02-23' ), date( '0400-02-23' ), date( '4000-02-23' ) )\n";
        String  dateSourceValues =
            "( date( '0001-02-23' ), date( '0101-02-23' ), date( '1000-01-23' ) ),\n" +
            "( date( '0002-02-23' ), date( '0202-02-23' ), date( '2000-12-23' ) ),\n" +
            "( date( '0003-02-23' ), date( '0300-02-23' ), date( '3000-01-23' ) ),\n" +
            "( date( '0005-02-23' ), date( '0005-02-23' ), date( '5000-01-23' ) )\n";
        String  dateMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on t.onClauseColumn = s.onClauseColumn\n" +
            "when matched and t.matchingClauseColumn = s.matchingClauseColumn\n" +
            "     then delete\n" +
            "when matched and year( t.matchingClauseColumn ) = year( s.matchingClauseColumn ) - 2\n" +
            "     then update set valueColumn = date( month( t.valueColumn ) + month( s.valueColumn ) ), description = 'updated'\n" +
            "when not matched and s.onClauseColumn = s.matchingClauseColumn\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        String[][]  dateExpectedValues = new String[][]
        {
            { "1", "orig", "0001-02-23", "0100-02-23", "1000-02-23" },
            { "3", "updated", "0002-02-23", "0200-02-23", "1970-01-14" },
            { "4", "orig", "0002-02-23", "0201-02-23", "2000-02-23" },
            { "5", "orig", "0004-02-23", "0400-02-23", "4000-02-23" },
            { "6", "inserted", "0005-02-23", "0005-02-23", "5000-01-23" },
        };

        vet_050
            ( dboConnection, "date", true, dateTargetValues, dateSourceValues,
              dateMergeStatement, dateExpectedValues );
        
        //
        // Time
        //
        
        String  timeTargetValues =
            "( 1, 'orig', time( '01:00:01' ), time( '01:01:00' ), time( '01:10:00' ) ),\n" +
            "( 2, 'orig: will delete', time( '01:00:01' ), time( '01:01:01' ), time( '01:10:01' ) ),\n" +
            "( 3, 'orig: will update', time( '01:00:02' ), time( '01:02:00' ), time( '01:20:00' ) ),\n" +
            "( 4, 'orig', time( '01:00:02' ), time( '01:02:01' ), time( '01:20:00' ) ),\n" +
            "( 5, 'orig', time( '01:00:04' ), time( '01:04:00' ), time( '01:40:00' ) )\n";
        String  timeSourceValues =
            "( time( '01:00:01' ), time( '01:01:01' ), time( '01:00:00' ) ),\n" +
            "( time( '01:00:02' ), time( '01:02:02' ), time( '20:02:00' ) ),\n" +
            "( time( '01:00:03' ), time( '01:03:00' ), time( '03:00:00' ) ),\n" +
            "( time( '01:00:05' ), time( '01:00:05' ), time( '05:00:00' ) )\n";
        String  timeMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on t.onClauseColumn = s.onClauseColumn\n" +
            "when matched and t.matchingClauseColumn = s.matchingClauseColumn\n" +
            "     then delete\n" +
            "when matched and second( t.matchingClauseColumn ) = second( s.matchingClauseColumn ) - 2\n" +
            "     then update set valueColumn = coalesce( t.valueColumn, s.valueColumn ), description = 'updated'\n" +
            "when not matched and s.onClauseColumn = s.matchingClauseColumn\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        String[][]  timeExpectedValues = new String[][]
        {
            { "1", "orig", "01:00:01", "01:01:00", "01:10:00" },
            { "3", "updated", "01:00:02", "01:02:00", "01:20:00" },
            { "4", "orig", "01:00:02", "01:02:01", "01:20:00" },
            { "5", "orig", "01:00:04", "01:04:00", "01:40:00" },
            { "6", "inserted", "01:00:05", "01:00:05", "05:00:00" },
        };

        vet_050
            ( dboConnection, "time", true, timeTargetValues, timeSourceValues,
              timeMergeStatement, timeExpectedValues );
        
        //
        // Timestamp
        //
        
        String  timestampTargetValues =
            "( 1, 'orig', timestamp( '1960-01-01 01:00:01' ), timestamp( '1960-01-01 01:01:00' ), timestamp( '1960-01-01 01:10:00' ) ),\n" +
            "( 2, 'orig: will delete', timestamp( '1960-01-01 01:00:01' ), timestamp( '1960-01-01 01:01:01' ), timestamp( '1960-01-01 01:10:01' ) ),\n  " +
            "( 3, 'orig: will update', timestamp( '1960-01-01 01:00:02' ), timestamp( '1960-01-01 01:02:00' ), timestamp( '1960-01-01 01:20:00' ) ),\n" +
            "( 4, 'orig', timestamp( '1960-01-01 01:00:02' ), timestamp( '1960-01-01 01:02:01' ), timestamp( '1960-01-01 01:20:00' ) ),\n" +
            "( 5, 'orig', timestamp( '1960-01-01 01:00:04' ), timestamp( '1960-01-01 01:04:00' ), timestamp( '1960-01-01 01:40:00' ) )\n";
        String  timestampSourceValues =
            "( timestamp( '1960-01-01 01:00:01' ), timestamp( '1960-01-01 01:01:01' ), timestamp( '1960-01-01 01:00:00' ) ),\n" +
            "( timestamp( '1960-01-01 01:00:02' ), timestamp( '1960-01-01 01:02:02' ), timestamp( '1960-01-01 20:02:00' ) ),\n" +
            "( timestamp( '1960-01-01 01:00:03' ), timestamp( '1960-01-01 01:03:00' ), timestamp( '1960-01-01 03:00:00' ) ),\n" +
            "( timestamp( '1960-01-01 01:00:05' ), timestamp( '1960-01-01 01:00:05' ), timestamp( '1960-01-01 05:00:00' ) )\n";
        String  timestampMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on t.onClauseColumn = s.onClauseColumn\n" +
            "when matched and t.matchingClauseColumn = s.matchingClauseColumn\n" +
            "     then delete\n" +
            "when matched and second( t.matchingClauseColumn ) = second( s.matchingClauseColumn ) - 2\n" +
            "     then update set valueColumn = coalesce( t.valueColumn, s.valueColumn ), description = 'updated'\n" +
            "when not matched and s.onClauseColumn = s.matchingClauseColumn\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        String[][]  timestampExpectedValues = new String[][]
        {
            { "1", "orig", "1960-01-01 01:00:01.0", "1960-01-01 01:01:00.0", "1960-01-01 01:10:00.0" },
            { "3", "updated", "1960-01-01 01:00:02.0", "1960-01-01 01:02:00.0", "1960-01-01 01:20:00.0" },
            { "4", "orig", "1960-01-01 01:00:02.0", "1960-01-01 01:02:01.0", "1960-01-01 01:20:00.0" },
            { "5", "orig", "1960-01-01 01:00:04.0", "1960-01-01 01:04:00.0", "1960-01-01 01:40:00.0" },
            { "6", "inserted", "1960-01-01 01:00:05.0", "1960-01-01 01:00:05.0", "1960-01-01 05:00:00.0" },
        };

        vet_050
            ( dboConnection, "timestamp", true, timestampTargetValues, timestampSourceValues,
              timestampMergeStatement, timestampExpectedValues );
        
        //
        // XML
        //
        
        String  xmlTargetValues =
            "(\n" +
            "  1,\n" +
            "  'orig',\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>1</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>100</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>1000</html>' preserve whitespace )\n" +
            "),\n" +
            "(\n" +
            "  2,\n" +
            "  'orig: will delete',\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>1</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>101</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>1001</html>' preserve whitespace )\n" +
            "),\n" +
            "(\n" +
            "  3,\n" +
            "  'orig: will update',\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>2</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>200</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>2000</html>' preserve whitespace )\n" +
            "),\n" +
            "(\n" +
            "  4,\n" +
            "  'orig',\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>2</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>201</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>2000</html>' preserve whitespace )\n" +
            "),\n" +
            "(\n" +
            "  5,\n" +
            "  'orig',\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>4</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>400</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>4000</html>' preserve whitespace )\n" +
            ")\n";
        String  xmlSourceValues =
            "(\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>1</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>101</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>10000</html>' preserve whitespace )\n" +
            "),\n" +
            "(\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>2</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>-200</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>-20000</html>' preserve whitespace )\n" +
            "),\n" +
            "(\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>3</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>300</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>30000</html>' preserve whitespace )\n" +
            "  ),\n" +
            "(\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>5</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>5</html>' preserve whitespace ),\n" +
            "  xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html>50000</html>' preserve whitespace )\n" +
            ")\n";
        String  xmlMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on xmlserialize( t.onClauseColumn as varchar(1000) ) = xmlserialize( s.onClauseColumn as varchar(1000) )\n" +
"when matched and xmlserialize( t.matchingClauseColumn as varchar(1000) ) = xmlserialize( s.matchingClauseColumn as varchar(1000) )\n" +
            "     then delete\n" +
            "when matched\n" +
            "     and xmlX( xmlserialize( t.matchingClauseColumn as varchar(1000) ) ) =\n" +
            "       -xmlX( xmlserialize( s.matchingClauseColumn as varchar(1000) ) )\n" +
            "     then update set valueColumn = coalesce( t.valueColumn, s.valueColumn ), description = 'updated'\n" +
"when not matched and xmlserialize( s.onClauseColumn as varchar(1000) ) = xmlserialize( s.matchingClauseColumn as varchar(1000) )\n"    +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        String[][]  xmlExpectedValues = new String[][]
        {
            { "1", "orig", "1", "100", "1000" },
            { "3", "updated", "2", "200", "2000" },
            { "4", "orig", "2", "201", "2000" },
            { "5", "orig", "4", "400", "4000" },
            { "6", "inserted", "5", "5", "50000" },
        };

        vet_050
            ( dboConnection, "xml", false, xmlTargetValues, xmlSourceValues,
              xmlMergeStatement, xmlExpectedValues,
              "select\n" +
              "  primaryKey,\n" +
              "  description,\n" +
              "  xmlX( xmlserialize( onClauseColumn as varchar(1000) ) ),\n" +
              "  xmlX( xmlserialize( matchingClauseColumn as varchar(1000) ) ),\n" +
              "  xmlX( xmlserialize( valueColumn as varchar(1000) ) )\n" +
              "from targetTable_050 order by primaryKey\n"
              );
        
        //
        // UDT
        //
        
        String  udtTargetValues =
            "( 1, 'orig', mia( 1 ), mia( 100 ), mia( 1000 ) ),\n" +
            "( 2, 'orig: will delete', mia( 1 ), mia( 101 ), mia( 1001 ) ),\n" +
            "( 3, 'orig: will update', mia( 2 ), mia( 200 ), mia( 2000 ) ),\n" +
            "( 4, 'orig', mia( 2 ), mia( 201 ), mia( 2000 ) ),\n" +
            "( 5, 'orig', mia( 4 ), mia( 400 ), mia( 4000 ) )\n";
        String  udtSourceValues =
            "( mia( 1 ), mia( 101 ), mia( 10000 ) ),\n" +
            "( mia( 2 ), mia( -200 ), mia( -20000 ) ),\n" +
            "( mia( 3 ), mia( 300 ), mia( 30000 ) ),\n" +
            "( mia( 5 ), mia( 5 ), mia( -5 ) )\n";
        String  udtMergeStatement =
            "merge into targetTable_050 t\n" +
            "using sourceTable_050 s on gc( t.onClauseColumn, 0 ) = gc( s.onClauseColumn, 0 )\n" +
            "when matched and gc( t.matchingClauseColumn, 0 ) = gc( s.matchingClauseColumn, 0 )\n" +
            "     then delete\n" +
            "when matched and gc( t.matchingClauseColumn, 0 ) = -gc( s.matchingClauseColumn, 0 )\n" +
            "     then update set valueColumn = mia( gc( t.valueColumn, 0 ) + gc( s.valueColumn, 0 ) ), description = 'updated'\n" +
            "when not matched and gc( s.onClauseColumn, 0 ) = gc( s.matchingClauseColumn, 0 )\n" +
            "     then insert values ( 6, 'inserted', s.onClauseColumn, s.matchingClauseColumn, s.valueColumn )\n";
        String  udtSelectStatement =
            "select\n" +
            "  primaryKey,\n" +
            "  description,\n" +
            "  gc( onClauseColumn, 0 ),\n" +
            "  gc( matchingClauseColumn, 0 ),\n" +
            "  gc( valueColumn, 0 )\n" +
            "from targetTable_050 order by primaryKey\n";

        vet_050
            ( dboConnection, "IntArray", false, udtTargetValues, udtSourceValues,
              udtMergeStatement, intExpectedValues, udtSelectStatement );
        
        //
        // drop schema
        //
        goodStatement( dboConnection, "drop function lv_equals_050" );
        goodStatement( dboConnection, "drop function clob_equals_050" );
        goodStatement( dboConnection, "drop function vb" );
        goodStatement( dboConnection, "drop function vb_reverse" );
        goodStatement( dboConnection, "drop function vb_add" );
        goodStatement( dboConnection, "drop function ch" );
        goodStatement( dboConnection, "drop function ch_reverse" );
        goodStatement( dboConnection, "drop function ch_add" );
        goodStatement( dboConnection, "drop function vbl" );
        goodStatement( dboConnection, "drop function vbl_reverse" );
        goodStatement( dboConnection, "drop function vbl_add" );
        goodStatement( dboConnection, "drop function vbl_equals" );
        goodStatement( dboConnection, "drop function xmlX" );
        goodStatement( dboConnection, "drop function mia" );
        goodStatement( dboConnection, "drop function gc" );
        goodStatement( dboConnection, "drop type IntArray restrict" );
    }
    private void    vet_050
        (
         Connection conn,
         String datatype,
         boolean    indexable,
         String initialTargetValues,
         String initialSourceValues,
         String     mergeStatement,
         String[][] expectedResults
         )
        throws Exception
    {
        String  selectStatement = "select * from targetTable_050 order by primaryKey";
        vet_050( conn, datatype, indexable, initialTargetValues, initialSourceValues, mergeStatement, expectedResults, selectStatement );
    }
    private void    vet_050
        (
         Connection conn,
         String datatype,
         boolean    indexable,
         String initialTargetValues,
         String initialSourceValues,
         String     mergeStatement,
         String[][] expectedResults,
         String selectStatement
         )
        throws Exception
    {
        goodStatement
            (
             conn,
             "create table targetTable_050\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    description varchar( 20 ),\n" +
             "    onClauseColumn " + datatype + ",\n" +
             "    matchingClauseColumn " + datatype + ",\n" +
             "    valueColumn " + datatype + "\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table sourceTable_050\n" +
             "(\n" +
             "    onClauseColumn " + datatype + ",\n" +
             "    matchingClauseColumn " + datatype + ",\n" +
             "    valueColumn " + datatype + "\n" +
             ")\n"
             );

        // no index
        vet_050( conn, initialTargetValues, initialSourceValues, mergeStatement, expectedResults, selectStatement );

        // add indexes and repeat
        if ( indexable )
        {
            goodStatement( conn, "create index tt_050_idx on targetTable_050( onClauseColumn )" );
            goodStatement( conn, "create index st_050_idx on sourceTable_050( onClauseColumn )" );
            vet_050( conn, initialTargetValues, initialSourceValues, mergeStatement, expectedResults, selectStatement );
        }

        goodStatement( conn, "drop table targetTable_050" );
        goodStatement( conn, "drop table sourceTable_050" );
    }
    private void    vet_050
        (
         Connection conn,
         String     initialTargetValues,
         String     initialSourceValues,
         String     mergeStatement,
         String[][]     expectedResults,
         String     selectStatement
         )
        throws Exception
    {
        populate_050( conn, initialTargetValues, initialSourceValues );
        goodUpdate( conn, mergeStatement, 3 );
        assertResults( conn, selectStatement, expectedResults, true );
    }
    private void    populate_050
        (
         Connection conn,
         String     initialTargetValues,
         String     initialSourceValues
         )
        throws Exception
    {
        goodStatement( conn, "delete from targetTable_050" );
        goodStatement( conn, "delete from sourceTable_050" );
        goodStatement( conn, "insert into targetTable_050 values " + initialTargetValues );
        goodStatement( conn, "insert into sourceTable_050 values " + initialSourceValues );
    }
    
    /**
     * <p>
     * Test multiple references to blob columns.
     * </p>
     */
    public  void    test_051_multiBlob()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create function mb_051( repeatCount int, vals int... ) returns blob\n" +
             "language java parameter style derby deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.makeBlob'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function bequals_051( leftV blob, rightV blob ) returns boolean\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.equals'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function reverse_051( leftV blob ) returns blob\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.reverse'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function gc_051( leftV blob, idx bigint ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.getCell'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function add_051( leftV blob, rightV blob ) returns blob\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.add'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_051\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    description varchar( 20 ),\n" +
             "    valueColumn blob,\n" +
             "    generatedColumn generated always as ( reverse_051( valueColumn ) )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_051\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    valueColumn blob\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into targetTable_051 ( primaryKey, description, valueColumn ) values\n" +
             "( 1, 'orig', mb_051( 10000, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ) ),\n" +
             "( 2, 'orig: will delete', mb_051( 20000, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 ) ),\n" +
             "( 3, 'orig: will update', mb_051( 30000, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29 ) ),\n" +
             "( 4, 'orig', mb_051( 10000, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39 ) ),\n" +
             "( 5, 'orig: will update', mb_051( 30000, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29 ) ),\n" +
             "( 6, 'orig: will delete', mb_051( 20000, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into sourceTable_051 values\n" +
             "( 20, mb_051( 20000, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 ) ),\n" +
             "( 21, mb_051( 30000, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29 ) ),\n" +
             "( 22, mb_051( 5000, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69 ) ),\n" +
             "( 23, mb_051( 6000, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59 ) )\n"
             );

        //
        // MERGE statement which references the same blob values multiple times.
        //
        goodUpdate
            (
             dboConnection,
             "merge into targetTable_051 t\n" +
             "using sourceTable_051 s on bequals_051( t.valueColumn, s.valueColumn )\n" +
             "when matched and gc_051( t.valueColumn, 1000 ) = 10\n" +
             "     then delete\n" +
             "when matched and gc_051( t.valueColumn, 1001 ) = 21\n" +
             "     then update set valueColumn = add_051( t.valueColumn, s.valueColumn ), description = 'updated'\n" +
             "when not matched and mod( gc_051( s.valueColumn, 3002 ), 10 ) = 2\n" +
             "     then insert ( primaryKey, description, valueColumn ) values ( s.primarykey, 'inserted', s.valueColumn )\n",
             6
             );
        assertResults
            (
             dboConnection,
             "select\n" +
             "  primaryKey,\n" +
             "  description,\n" +
             "  gc_051( valueColumn, 2001 ),\n" +
             "  gc_051( generatedColumn, 2001 )\n" +
             "from targetTable_051 order by primaryKey\n",
             new String[][]
             {
                 { "1", "orig", "1", "8" },
                 { "3", "updated", "42", "56" },
                 { "4", "orig", "31", "38" },
                 { "5", "updated", "42", "56" },
                 { "22", "inserted", "61", "68" },
                 { "23", "inserted", "51", "58" },
             },
             false
             );

        //
        // Drop schema
        //
        goodStatement( dboConnection, "drop table sourceTable_051" );
        goodStatement( dboConnection, "drop table targetTable_051" );
        goodStatement( dboConnection, "drop function mb_051" );
        goodStatement( dboConnection, "drop function bequals_051" );
        goodStatement( dboConnection, "drop function reverse_051" );
        goodStatement( dboConnection, "drop function gc_051" );
        goodStatement( dboConnection, "drop function add_051" );
    }
    
    /**
     * <p>
     * Test multiple references to clob columns.
     * </p>
     */
    public  void    test_052_multiClob()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create function mc_052( repeatCount int, vals varchar( 32672 )... ) returns clob\n" +
             "language java parameter style derby deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.makeClob'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function cequals_052( leftV clob, rightV clob ) returns boolean\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.equals'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function reverse_052( leftV clob ) returns clob\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.reverse'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function gc_052( leftV clob, idx bigint ) returns varchar( 1 )\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.getCell'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function add_052( leftV clob, rightV clob ) returns clob\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.add'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_052\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    description varchar( 20 ),\n" +
             "    valueColumn clob,\n" +
             "    generatedColumn generated always as ( reverse_052( valueColumn ) )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_052\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    valueColumn clob\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into targetTable_052 ( primaryKey, description, valueColumn ) values\n" +
             "( 1, 'orig', mc_052( 10000, 'abcdefghij' ) ),\n" +
             "( 2, 'orig: will delete', mc_052( 20000, 'klmnopqrst' ) ),\n" +
             "( 3, 'orig: will update', mc_052( 30000, 'tuvwxyzabc' ) ),\n" +
             "( 4, 'orig', mc_052( 10000, 'defghijklm' ) ),\n" +
             "( 5, 'orig: will update', mc_052( 30000, 'tuvwxyzabc' ) ),\n" +
             "( 6, 'orig: will delete', mc_052( 20000, 'klmnopqrst' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into sourceTable_052 values\n" +
             "( 20, mc_052( 20000, 'klmnopqrst' ) ),\n" +
             "( 21, mc_052( 30000, 'tuvwxyzabc' ) ),\n" +
             "( 22, mc_052( 5000, 'opqrstuvwx' ) ),\n" +
             "( 23, mc_052( 6000, 'opqrstuvwx' ) )\n"
             );

        //
        // MERGE statement which references the same clob values multiple times.
        //
        goodUpdate
            (
             dboConnection,
             "merge into targetTable_052 t\n" +
             "using sourceTable_052 s on cequals_052( t.valueColumn, s.valueColumn )\n" +
             "when matched and gc_052( t.valueColumn, 1000 ) = 'k'\n" +
             "     then delete\n" +
             "when matched and gc_052( t.valueColumn, 1001 ) = 'u'\n" +
             "     then update set valueColumn = add_052( t.valueColumn, s.valueColumn ), description = 'updated'\n" +
             "when not matched and gc_052( s.valueColumn, 3002 ) = 'q'\n" +
             "     then insert ( primaryKey, description, valueColumn ) values ( s.primarykey, 'inserted', s.valueColumn )\n",
             6
             );
        assertResults
            (
             dboConnection,
             "select\n" +
             "  primaryKey,\n" +
             "  description,\n" +
             "  gc_052( valueColumn, 2001 ),\n" +
             "  gc_052( generatedColumn, 2001 )\n" +
             "from targetTable_052 order by primaryKey\n",
             new String[][]
             {
                 { "1", "orig", "b", "i" },
                 { "3", "updated", "u", "b" },
                 { "4", "orig", "e", "l" },
                 { "5", "updated", "u", "b" },
                 { "22", "inserted", "p", "w" },
                 { "23", "inserted", "p", "w" },
             },
             false
             );

        //
        // Drop schema
        //
        goodStatement( dboConnection, "drop table sourceTable_052" );
        goodStatement( dboConnection, "drop table targetTable_052" );
        goodStatement( dboConnection, "drop function mc_052" );
        goodStatement( dboConnection, "drop function cequals_052" );
        goodStatement( dboConnection, "drop function reverse_052" );
        goodStatement( dboConnection, "drop function gc_052" );
        goodStatement( dboConnection, "drop function add_052" );
    }
    
    /**
     * <p>
     * Test MERGE statements involving generated columns which evaluate to null.
     * </p>
     */
    public  void    test_053_nullGeneratedColumns()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create table targetTable_053\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    description varchar( 20 ),\n" +
             "    valueColumn int,\n" +
             "    notUpdatedColumn int,\n" +
             "    generatedColumn generated always as ( valueColumn + notUpdatedColumn )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_053\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    valueColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into targetTable_053 ( primaryKey, description, valueColumn, notUpdatedColumn ) values\n" +
             "( 1, 'orig', 1, 100 ),\n" +
             "( 2, 'orig: will delete', 2, 200 ),\n" +
             "( 3, 'orig: will update', 3, 300 ),\n" +
             "( 4, 'orig', 4, 400 ),\n" +
             "( 5, 'orig: will update', 5, 500 ),\n" +
             "( 6, 'orig: will delete', 6, 600 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into sourceTable_053 values\n" +
             "( 2, -1 ),\n" +
             "( 3, 300 ),\n" +
             "( 5, null ),\n" +
             "( 6, -1 ),\n" +
             "( 7, 100 ),\n" +
             "( 8, null ),\n" +
             "( 100, null )\n"
             );

        //
        // Run a MERGE statement which causes a generated column to
        // evaluate to null sometimes, and sometimes not.
        //
        goodUpdate
            (
             dboConnection,
             "merge into targetTable_053 t\n" +
             "using sourceTable_053 s on t.primaryKey = s.primaryKey\n" +
             "when matched and s.valueColumn < 0 then delete\n" +
             "when matched and s.valueColumn > 0 or t.valueColumn = 5\n" +
             "     then update set valueColumn = t.valueColumn + s.valueColumn, description = 'updated'\n" +
             "when not matched and s.primaryKey < 10\n" +
             "     then insert ( primaryKey, description, valueColumn, notUpdatedColumn ) values ( s.primarykey, 'inserted', s.valueColumn, 1 )\n",
             6
             );
        assertResults
            (
             dboConnection,
             "select * from targetTable_053 order by primaryKey",
             new String[][]
             {
                 { "1", "orig", "1", "100", "101" },
                 { "3", "updated", "303", "300", "603" },
                 { "4", "orig", "4", "400", "404" },
                 { "5", "updated", null, "500", null },
                 { "7", "inserted", "100", "1", "101" },
                 { "8", "inserted", null, "1", null },
             },
             false
             );

        //
        // Drop schema
        //
        goodStatement( dboConnection, "drop table sourceTable_053" );
        goodStatement( dboConnection, "drop table targetTable_053" );
    }
    
    /**
     * <p>
     * Test MERGE statements involving triggers on generated columns.
     * </p>
     */
    public  void    test_054_triggersOnGeneratedColumns()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create procedure truncateTriggerHistory_054()\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.truncateTriggerHistory'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_054\n" +
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
             "create function history_054()\n" +
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
             "create table targetTable_054\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    description varchar( 20 ),\n" +
             "    valueColumn int,\n" +
             "    notUpdatedColumn int,\n" +
             "    generatedColumn generated always as ( valueColumn + notUpdatedColumn )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_054\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    valueColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_054_del_before\n" +
             "no cascade before delete on targetTable_054\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_054( 'before delete', old.generatedColumn )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_054_del_after\n" +
             "after delete on targetTable_054\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_054( 'after delete', old.generatedColumn )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_054_ins_after\n" +
             "after insert on targetTable_054\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_054( 'after insert', new.generatedColumn )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_054_upd_before\n" +
             "no cascade before update on targetTable_054\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_054( 'before update', old.generatedColumn )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_054_upd_after\n" +
             "after update on targetTable_054\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_054( 'after update', new.generatedColumn )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into targetTable_054 ( primaryKey, description, valueColumn, notUpdatedColumn ) values\n" +
             "( 1, 'orig', 1, 100 ),\n" +
             "( 2, 'orig: will delete', 2, 200 ),\n" +
             "( 3, 'orig: will update', 3, 300 ),\n" +
             "( 4, 'orig', 4, 400 ),\n" +
             "( 5, 'orig: will update', 5, 500 ),\n" +
             "( 6, 'orig: will delete', 6, 600 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into sourceTable_054 values\n" +
             "( 2, -1 ),\n" +
             "( 3, 300 ),\n" +
             "( 5, null ),\n" +
             "( 6, -1 ),\n" +
             "( 7, 100 ),\n" +
             "( 8, null ),\n" +
             "( 100, null )\n"
             );
        goodStatement
            (
             dboConnection,
             "call truncateTriggerHistory_054()"
             );

        //
        // Now MERGE and fire some triggers.
        //
        goodUpdate
            (
             dboConnection,
             "merge into targetTable_054 t\n" +
             "using sourceTable_054 s on t.primaryKey = s.primaryKey\n" +
             "when matched and s.valueColumn < 0 then delete\n" +
             "when matched and s.valueColumn > 0 or t.valueColumn = 5\n" +
             "     then update set valueColumn = t.valueColumn + s.valueColumn, description = 'updated'\n" +
             "when not matched and s.primaryKey < 10\n" +
             "     then insert ( primaryKey, description, valueColumn, notUpdatedColumn ) values ( s.primarykey, 'inserted', s.valueColumn, 1 )\n",
             6
             );
        assertResults
            (
             dboConnection,
             "select * from targetTable_054 order by primaryKey",
             new String[][]
             {
                 { "1", "orig", "1", "100", "101" },
                 { "3", "updated", "303", "300", "603" },
                 { "4", "orig", "4", "400", "404" },
                 { "5", "updated", null, "500", null },
                 { "7", "inserted", "100", "1", "101" },
                 { "8", "inserted", null, "1", null },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "select * from table( history_054() ) h",
             new String[][]
             {
                 { "before delete", "202" },
                 { "before delete", "606" },
                 { "after delete", "202" },
                 { "after delete", "606" },
                 { "before update", "303" },
                 { "before update", "505" },
                 { "after update", "603" },
                 { "after update", null },
                 { "after insert", "101" },
                 { "after insert", null },
             },
             false
             );

        //
        // Drop schema
        //
        goodStatement( dboConnection, "drop table sourceTable_054" );
        goodStatement( dboConnection, "drop table targetTable_054" );
        goodStatement( dboConnection, "drop function history_054" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_054" );
        goodStatement( dboConnection, "drop procedure truncateTriggerHistory_054" );
    }
    
    /**
     * <p>
     * Test MERGE statements which read BLOBs multiple times when running triggers.
     * </p>
     */
    public  void    test_055_triggersMultiBlob()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create procedure truncateTriggerHistory_055()\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.truncateTriggerHistory'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_055\n" +
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
             "create function history_055()\n" +
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
             "create function mb_055( repeatCount int, vals int... ) returns blob\n" +
             "language java parameter style derby deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.makeBlob'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function bequals_055( leftV blob, rightV blob ) returns boolean\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.equals'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function reverse_055( leftV blob ) returns blob\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.reverse'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function gc_055( leftV blob, idx bigint ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.getCell'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function add_055( leftV blob, rightV blob ) returns blob\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.add'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_055\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    description varchar( 20 ),\n" +
             "    valueColumn blob,\n" +
             "    generatedColumn generated always as ( reverse_055( valueColumn ) )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_055\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    valueColumn blob\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_055_del_before\n" +
             "no cascade before delete on targetTable_055\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_055( 'before delete', gc_055( old.generatedColumn, 50153 ) + gc_055( old.generatedColumn, 50154 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_055_del_after\n" +
             "after delete on targetTable_055\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_055( 'after delete', gc_055( old.generatedColumn, 50153 ) + gc_055( old.generatedColumn, 50154 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_055_ins_after\n" +
             "after insert on targetTable_055\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_055( 'after insert', gc_055( new.generatedColumn, 50153 ) + gc_055( new.generatedColumn, 50154 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_055_upd_before\n" +
             "no cascade before update on targetTable_055\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_055( 'before update', gc_055( old.generatedColumn, 50153 ) + gc_055( old.generatedColumn, 50154 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_055_upd_after\n" +
             "after update on targetTable_055\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_055( 'after update', gc_055( new.generatedColumn, 50153 ) + gc_055( new.generatedColumn, 50154 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into targetTable_055 ( primaryKey, description, valueColumn ) values\n" +
             "( 1, 'orig', mb_055( 10000, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ) ),\n" +
             "( 2, 'orig: will delete', mb_055( 20000, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 ) ),\n" +
             "( 3, 'orig: will update', mb_055( 30000, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29 ) ),\n" +
             "( 4, 'orig', mb_055( 10000, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39 ) ),\n" +
             "( 5, 'orig: will update', mb_055( 30000, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29 ) ),\n" +
             "( 6, 'orig: will delete', mb_055( 20000, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into sourceTable_055 values\n" +
             "( 20, mb_055( 20000, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 ) ),\n" +
             "( 21, mb_055( 30000, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29 ) ),\n" +
             "( 22, mb_055( 7000, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69 ) ),\n" +
             "( 23, mb_055( 6000, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "call truncateTriggerHistory_055()"
             );

        //
        // Verify that rows are correctly updated and that triggers fire correctly.
        //
        goodUpdate
            (
             dboConnection,
             "merge into targetTable_055 t\n" +
             "using sourceTable_055 s on bequals_055( t.valueColumn, s.valueColumn )\n" +
             "when matched and gc_055( t.valueColumn, 1000 ) = 10\n" +
             "     then delete\n" +
             "when matched and gc_055( t.valueColumn, 1001 ) = 21\n" +
             "     then update set valueColumn = add_055( t.valueColumn, s.valueColumn ), description = 'updated'\n" +
             "when not matched and mod( gc_055( s.valueColumn, 3002 ), 10 ) = 2\n" +
             "     then insert ( primaryKey, description, valueColumn ) values ( s.primarykey, 'inserted', s.valueColumn )\n",
             6
             );
        assertResults
            (
             dboConnection,
             "select\n" +
             "  primaryKey,\n" +
             "  description,\n" +
             "  gc_055( valueColumn, 2001 ),\n" +
             "  gc_055( generatedColumn, 2001 )\n" +
             "from targetTable_055 order by primaryKey\n",
             new String[][]
             {
                 { "1", "orig", "1", "8" },
                 { "3", "updated", "42", "56" },
                 { "4", "orig", "31", "38" },
                 { "5", "updated", "42", "56" },
                 { "22", "inserted", "61", "68" },
                 { "23", "inserted", "51", "58" },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "select * from table( history_055() ) h",
             new String[][]
             {
                 { "before delete", "31" },
                 { "before delete", "31" },
                 { "after delete", "31" },
                 { "after delete", "31" },
                 { "before update", "51" },
                 { "before update", "51" },
                 { "after update", "102" },
                 { "after update", "102" },
                 { "after insert", "131" },
                 { "after insert", "111" },
             },
             false
             );

        //
        // Drop schema
        //
        goodStatement( dboConnection, "drop table sourceTable_055" );
        goodStatement( dboConnection, "drop table targetTable_055" );
        goodStatement( dboConnection, "drop function add_055" );
        goodStatement( dboConnection, "drop function gc_055" );
        goodStatement( dboConnection, "drop function reverse_055" );
        goodStatement( dboConnection, "drop function bequals_055" );
        goodStatement( dboConnection, "drop function mb_055" );
        goodStatement( dboConnection, "drop function history_055" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_055" );
        goodStatement( dboConnection, "drop procedure truncateTriggerHistory_055" );
    }
    
    /**
     * <p>
     * Test MERGE statements which read CLOBs multiple times when running triggers.
     * </p>
     */
    public  void    test_056_triggersMultiClob()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create procedure truncateTriggerHistory_056()\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.truncateTriggerHistory'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_056\n" +
             "(\n" +
             "    actionString varchar( 20 ),\n" +
             "    actionValue varchar( 20 )\n" +
             ")\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.addHistoryRow'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function history_056()\n" +
             "returns table\n" +
             "(\n" +
             "    action varchar( 20 ),\n" +
             "    actionValue varchar( 20 )\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.history'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function mc_056( repeatCount int, vals varchar( 32672 )... ) returns clob\n" +
             "language java parameter style derby deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.makeClob'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function cequals_056( leftV clob, rightV clob ) returns boolean\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.equals'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function reverse_056( leftV clob ) returns clob\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.reverse'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function gc_056( leftV clob, idx bigint ) returns varchar( 1 )\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.getCell'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function add_056( leftV clob, rightV clob ) returns clob\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.add'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_056\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    description varchar( 20 ),\n" +
             "    valueColumn clob,\n" +
             "    generatedColumn generated always as ( reverse_056( valueColumn ) )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_056\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    valueColumn clob\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_056_del_before\n" +
             "no cascade before delete on targetTable_056\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_056( 'before delete', gc_056( old.generatedColumn, 50153 ) || gc_056( old.generatedColumn, 50154 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_056_del_after\n" +
             "after delete on targetTable_056\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_056( 'after delete', gc_056( old.generatedColumn, 50153 ) || gc_056( old.generatedColumn, 50154 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_056_ins_after\n" +
             "after insert on targetTable_056\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_056( 'after insert', gc_056( new.generatedColumn, 50153 ) || gc_056( new.generatedColumn, 50154 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_056_upd_before\n" +
             "no cascade before update on targetTable_056\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_056( 'before update', gc_056( old.generatedColumn, 50153 ) || gc_056( old.generatedColumn, 50154 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_056_upd_after\n" +
             "after update on targetTable_056\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_056( 'after update', gc_056( new.generatedColumn, 50153 ) || gc_056( new.generatedColumn, 50154 ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into targetTable_056 ( primaryKey, description, valueColumn ) values\n" +
             "( 1, 'orig', mc_056( 10000, 'abcdefghij' ) ),\n" +
             "( 2, 'orig: will delete', mc_056( 20000, 'klmnopqrst' ) ),\n" +
             "( 3, 'orig: will update', mc_056( 30000, 'uvwxyzabcd' ) ),\n" +
             "( 4, 'orig', mc_056( 10000, 'efghijklmn' ) ),\n" +
             "( 5, 'orig: will update', mc_056( 30000, 'uvwxyzabcd' ) ),\n" +
             "( 6, 'orig: will delete', mc_056( 20000, 'klmnopqrst' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into sourceTable_056 values\n" +
             "( 20, mc_056( 20000, 'klmnopqrst' ) ),\n" +
             "( 21, mc_056( 30000, 'uvwxyzabcd' ) ),\n" +
             "( 22, mc_056( 7000, 'efghijklmn' ) ),\n" +
             "( 23, mc_056( 6000, 'opqrstuvwx' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "call truncateTriggerHistory_056()"
             );

        //
        // Verify that rows are correctly updated and that triggers fire correctly.
        //
        goodUpdate
            (
             dboConnection,
             "merge into targetTable_056 t\n" +
             "using sourceTable_056 s on cequals_056( t.valueColumn, s.valueColumn )\n" +
             "when matched and gc_056( t.valueColumn, 1000 ) = 'k'\n" +
             "     then delete\n" +
             "when matched and gc_056( t.valueColumn, 1001 ) = 'v'\n" +
             "     then update set valueColumn = add_056( t.valueColumn, s.valueColumn ), description = 'updated'\n" +
             "when not matched and gc_056( s.valueColumn, 3002 ) = 'g' or gc_056( s.valueColumn, 3003 ) = 'r'\n" +
             "     then insert ( primaryKey, description, valueColumn ) values ( s.primarykey, 'inserted', s.valueColumn )\n",
             6
             );
        assertResults
            (
             dboConnection,
             "select\n" +
             "  primaryKey,\n" +
             "  description,\n" +
             "  gc_056( valueColumn, 2001 ),\n" +
             "  gc_056( generatedColumn, 2001 )\n" +
             "from targetTable_056 order by primaryKey\n",
             new String[][]
             {
                 { "1", "orig", "b", "i" },
                 { "3", "updated", "v", "c" },
                 { "4", "orig", "f", "m" },
                 { "5", "updated", "v", "c" },
                 { "22", "inserted", "f", "m" },
                 { "23", "inserted", "p", "w" },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "select * from table( history_056() ) h",
             new String[][]
             {
                 { "before delete", "qp" },
                 { "before delete", "qp" },
                 { "after delete", "qp" },
                 { "after delete", "qp" },
                 { "before update", "az" },
                 { "before update", "az" },
                 { "after update", "az" },
                 { "after update", "az" },
                 { "after insert", "kj" },
                 { "after insert", "ut" },
             },
             false
             );

        //
        // Drop schema
        //
        goodStatement( dboConnection, "drop table sourceTable_056" );
        goodStatement( dboConnection, "drop table targetTable_056" );
        goodStatement( dboConnection, "drop function add_056" );
        goodStatement( dboConnection, "drop function gc_056" );
        goodStatement( dboConnection, "drop function reverse_056" );
        goodStatement( dboConnection, "drop function cequals_056" );
        goodStatement( dboConnection, "drop function mc_056" );
        goodStatement( dboConnection, "drop function history_056" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_056" );
        goodStatement( dboConnection, "drop procedure truncateTriggerHistory_056" );
    }
    
    /**
     * <p>
     * Test that deferred deletes buffer up the columns needed to satisfy triggers.
     * </p>
     */
    public  void    test_057_deferredDelete()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create procedure truncateTriggerHistory_057()\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.truncateTriggerHistory'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_057\n" +
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
             "create function history_057()\n" +
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
             "create table targetTable_057\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    description varchar( 20 ),\n" +
             "    valueColumn int,\n" +
             "    triggerColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_057\n" +
             "(\n" +
             "    primaryKey int,\n" +
             "    valueColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_057_del_before\n" +
             "no cascade before delete on targetTable_057\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_057( 'before delete', old.triggerColumn )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_057_del_after\n" +
             "after delete on targetTable_057\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_057( 'after delete', old.triggerColumn )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_057_ins_after\n" +
             "after insert on targetTable_057\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_057( 'after insert', new.triggerColumn )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_057_upd_before\n" +
             "no cascade before update on targetTable_057\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_057( 'before update', old.triggerColumn )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger t1_057_upd_after\n" +
             "after update on targetTable_057\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_057( 'after update', new.triggerColumn )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into targetTable_057 ( primaryKey, description, valueColumn, triggerColumn ) values\n" +
             "( 1, 'orig', 10, 100 ),\n" +
             "( 2, 'orig: will delete', 20, 200 ),\n" +
             "( 3, 'orig: will update', 30, 300 ),\n" +
             "( 4, 'orig', 40, 400 ),\n" +
             "( 5, 'orig: will update', 30, 500 ),\n" +
             "( 6, 'orig: will delete', 20, 100 )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into sourceTable_057 values\n" +
             "( 20, 20 ),\n" +
             "( 21, 30 ),\n" +
             "( 22, 70 ),\n" +
             "( 23, 80 )\n"
             );
        goodStatement
            (
             dboConnection,
             "call truncateTriggerHistory_057()"
             );
        
        //
        // Verify that the triggers fire correctly.
        //
        goodUpdate
            (
             dboConnection,
             "merge into targetTable_057 t\n" +
             "using sourceTable_057 s on t.valueColumn = s.valueColumn\n" +
             "when matched and t.valueColumn = 20\n" +
             "     then delete\n" +
             "when matched and t.valueColumn = 30\n" +
             "     then update set valueColumn = t.valueColumn + s.valueColumn, description = 'updated'\n" +
             "when not matched and s.valueColumn = 70 or s.valueColumn = 80\n" +
             "     then insert ( primaryKey, description, valueColumn ) values ( s.primarykey, 'inserted', s.valueColumn )\n",
             6
             );
        assertResults
            (
             dboConnection,
             "select * from targetTable_057 order by primaryKey",
             new String[][]
             {
                 { "1", "orig", "10", "100" },
                 { "3", "updated", "60", "300" },
                 { "4", "orig", "40", "400" },
                 { "5", "updated", "60", "500" },
                 { "22", "inserted", "70", null },
                 { "23", "inserted", "80", null },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "select * from table( history_057() ) h",
             new String[][]
             {
                 { "before delete", "200" },
                 { "before delete", "100" },
                 { "after delete", "200" },
                 { "after delete", "100" },
                 { "before update", "300" },
                 { "before update", "500" },
                 { "after update", "300" },
                 { "after update", "500" },
                 { "after insert", null },
                 { "after insert", null },
             },
             false
             );

        //
        // Drop schema
        //
        goodStatement( dboConnection, "drop table sourceTable_057" );
        goodStatement( dboConnection, "drop table targetTable_057" );
        goodStatement( dboConnection, "drop function history_057" );
        goodStatement( dboConnection, "drop procedure addHistoryRow_057" );
        goodStatement( dboConnection, "drop procedure truncateTriggerHistory_057" );
    }
    
    /**
     * <p>
     * Verify that the collation is what we expect.
     * </p>
     */
    public  void    test_058_collation()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        assertResults
            (
             dboConnection,
             "values syscs_util.syscs_get_database_property( 'derby.database.collation' )",
             new String[][]
             {
                 { expectedCollation() },
             },
             true
             );
    }

    /**
     * <p>
     * Test that the left join can correctly read from an index on the target table and
     * pick up the row id.
     * </p>
     */
    public  void    test_059_targetIndex()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        File    traceFile = SupportFilesSetup.getReadWrite( TRACE_FILE_NAME );
        SupportFilesSetup.deleteFile( traceFile );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create function mb_059( repeatCount int, vals int... ) returns blob\n" +
             "language java parameter style derby deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.makeBlob'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_059\n" +
             "(\n" +
             "    valueColumn int,\n" +
             "    unreferencedBlobColumn blob\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create unique index target_059_primary on targetTable_059( valueColumn )"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_059\n" +
             "(\n" +
             "    valueColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create unique index source_059_primary on sourceTable_059( valueColumn )"
             );


        //
        // Populate tables
        //
        goodStatement
            (
             dboConnection,
             "insert into sourceTable_059 values\n" +
             "( 0 ),\n" +
             "( 2 ),\n" +
             "( 4 ),\n" +
             "( 8 )\n"
             );
        PreparedStatement   ps = chattyPrepare
            (
             dboConnection,
             "insert into targetTable_059 ( valueColumn, unreferencedBlobColumn ) values\n" +
             "( ?, mb_059( 100000, ? ) )\n"
             );
        for ( int i = 0; i < 100; i++ )
        {
            if ( (i % 20) == 0 ) { println( "Inserting row " + i ); }
            ps.setInt( 1, i );
            ps.setInt( 2, i );
            ps.executeUpdate();
        }
        goodStatement
            (
             dboConnection,
             "delete from targetTable_059 where valueColumn = 4 or valueColumn = 8"
             );

        //
        // Run a MERGE which uses the indexes.
        //
        goodStatement( dboConnection, "call syscs_util.syscs_register_tool( 'optimizerTracing', true, 'xml' )" );
        goodUpdate
            (
             dboConnection,
             "merge into targetTable_059 t\n" +
             "using sourceTable_059 s on t.valueColumn = s.valueColumn\n" +
             "when matched and t.valueColumn = 0\n" +
             "     then delete\n" +
             "when matched and t.valueColumn = 2\n" +
             "     then update set valueColumn = -t.valueColumn\n" +
             "when not matched and s.valueColumn = 4\n" +
             "     then insert ( valueColumn ) values ( -s.valueColumn )\n",
             3
             );
        goodStatement( dboConnection, "call syscs_util.syscs_register_tool( 'optimizerTracing', false, '" + traceFile.getPath() + "' )" );

        // verify the plan shape
        goodStatement( dboConnection, "call syscs_util.syscs_register_tool( 'optimizerTracingViews', true, '" + traceFile.getPath() + "' )" );
        assertResults
            (
             dboConnection,
             "select stmtid, qbid, summary from planCost\n" +
             "where type = 'bestPlan'\n" +
             "order by stmtid, qbid\n",
             new String[][]
             {
                 { "1", "1", "ProjectRestrictNode" },
                 { "1", "2", "\"" + TEST_DBO + "\".\"SOURCE_059_PRIMARY\"" },
                 { "1", "3", "\"" + TEST_DBO + "\".\"TARGET_059_PRIMARY\"" },
             },
             false
             );
        goodStatement( dboConnection, "call syscs_util.syscs_register_tool( 'optimizerTracingViews', false )" );
        
        // verify the results
        assertResults
            (
             dboConnection,
             "select valueColumn from targetTable_059 where valueColumn < 10 order by valueColumn",
             new String[][]
             {
                 { "-4" },
                 { "-2" },
                 { "1" },
                 { "3" },
                 { "5" },
                 { "6" },
                 { "7" },
                 { "9" },
             },
             false
             );

        //
        // Drop schema
        //
        goodStatement( dboConnection, "drop table sourceTable_059" );
        goodStatement( dboConnection, "drop table targetTable_059" );
        goodStatement( dboConnection, "drop function mb_059" );
    }
    
    /**
     * <p>
     * Test that the UPDATE actions of MERGE statements work with
     * trigger transition tables and simple column expressions from the transition tables.
     * </p>
     */
    public  void    test_060_transitionTableSimpleColumn()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_060( x int, x1 int )"
             );
        goodStatement
            (
             dboConnection,
             "create table t2_060( y int, y1 int )"
             );
        goodStatement
            (
             dboConnection,
             "create trigger tr1 after insert on t1_060\n" +
             "referencing new table as new\n" +
             "merge into t2_060\n" +
             "using new on x1 = y1\n" +
             "when matched then update set y = x\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t2_060 values ( 1, 100 ), ( 2, 200 )"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_060 values ( 1000, 100 ), ( 3000, 300 )"
             );

        // verify the results
        assertResults
            (
             dboConnection,
             "select * from t2_060 order by y, y1",
             new String[][]
             {
                 { "2", "200" },
                 { "1000", "100" },
             },
             false
             );

        //
        // Drop schema
        //
        goodStatement( dboConnection, "drop table t1_060" );
        goodStatement( dboConnection, "drop table t2_060" );
    }

    public void test_061_Derby6693() throws SQLException {
        Statement s = createStatement();

        try {
            s.execute("create table t2(x int)");
            s.execute("create table t1(x int)");
            s.execute("insert into t2 values 3,4");
            assertCompileError(
                    NO_AGGREGATE_IN_MATCHING,
                    "merge into t1 using t2 on (t1.x=t2.x) " +
                    "when not matched then insert values (max(t2.x))");
        } finally {
            dropTable("t1");
            dropTable("t2");
        }
    }

    public void test_062_Derby6550() throws SQLException {
        Statement s = createStatement();
        
        s.execute("create table t(a bigint generated always as identity " +
                  "    ( start with 9223372036854775806 ),b int)");
        s.execute("create function integerList()\n" +
                "returns table(a int,b int,c int,d int)\n" +
                "language java\n" +
                "parameter style derby_jdbc_result_set\n" +
                "no sql\n" +
                "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.integerList_023'\n");

        // this fails because bulk-insert isn't used and we go past the
        // end of the identity column's range
        assertStatementError("2200H", s,
            "insert into t( b ) values ( 1 ), ( 2 ), ( 3 ), ( 4 ), ( 5 )" );

        // inserting into an empty table from a table function uses bulk-insert
        //
        // this should fail just like the previous statement, but it succeeds
        assertStatementError("2200H", s,
            "insert into t( b ) select b from table( integerList() ) il");

        JDBC.assertEmpty( s.executeQuery("select * from t") );

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
    
    /** Table function returning one row */
    public static IntegerArrayVTI singlerow_028()
    {
        // A
        return new IntegerArrayVTI
            (
             new String[] { "X" },
             new int[][]
             {
                 new int[] { 1 },
             }
             );
    }
    
    /** Table function returning one row */
    public static IntegerArrayVTI tworow_041()
    {
        // A
        return new IntegerArrayVTI
            (
             new String[] { "I" },
             new int[][]
             {
                { 1 },
                { 3 },
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
    public  static  void    addHistoryRow( String actionString, Integer actionValue )
    {
        _triggerHistory.add( new String[] { actionString, actionValue == null ? null : actionValue.toString() } );
    }

    /** Procedure for adding trigger history */
    public  static  void    addHistoryRow( String actionString, String actionValue )
    {
        _triggerHistory.add( new String[] { actionString, actionValue } );
    }

    /** Function for comparing two long varchar values */
    public  static  boolean equals( String left, String right )
    {
        if ( left == null ) { return false; }
        else { return left.equals( right ); }
    }

    /** Function for comparing two clob values */
    public  static  boolean equals( Clob left, Clob right )
        throws SQLException
    {
        if ( left == null ) { return false; }
        else if ( right == null ) { return false; }
        else
        {
            String  leftString = left.getSubString( 1L, (int) left.length() );
            String  rightString = right.getSubString( 1L, (int) right.length() );
            return leftString.equals( rightString );
        }
    }

    /** Function for comparing two blob values */
    public  static  boolean equals( Blob left, Blob right )
        throws SQLException
    {
        if ( left == null ) { return false; }
        else if ( right == null ) { return false; }
        else
        {
            return Arrays.equals( left.getBytes( 1L, (int) left.length() ), right.getBytes( 1L, (int) right.length() ) );
        }
    }

    /** flip the order of bytes in an array */
    public  static  byte[]  reverse( byte[] input )
    {
        if ( input == null ) { return null; }

        int count = input.length;
        byte[]  output = new byte[ count ];

        for ( int i = 0; i < count; i++ ) { output[ ( count - i ) - 1 ] = input[ i ]; }

        return output;
    }

    /** flip the order of characterss in a String */
    public  static  String  reverse( String inputString )
    {
        if ( inputString == null ) { return null; }

        char[]  input = inputString.toCharArray();
        int count = input.length;
        char[]  output = new char[ count ];

        for ( int i = 0; i < count; i++ ) { output[ ( count - i ) - 1 ] = input[ i ]; }

        return new String( output );
    }

    /** flip the order of bytes in a blob */
    public  static  Blob  reverse( Blob inputBlob )
        throws SQLException
    {
        if ( inputBlob == null ) { return null; }

        return new HarmonySerialBlob( reverse( inputBlob.getBytes( 1L, (int) inputBlob.length() ) ) );
    }

    /** flip the order of characters in a clob */
    public  static  Clob  reverse( Clob inputClob )
        throws SQLException
    {
        if ( inputClob == null ) { return null; }

        return new HarmonySerialClob( reverse( inputClob.getSubString( 1L, (int) inputClob.length() ) ) );
    }

    /** add the values of two byte arrays */
    public  static  byte[]  add( byte[] left, byte[] right )
    {
        if ( left ==  null ) { return null; }
        if ( right == null ) { return null; }

        int     count = Math.min( left.length, right.length );
        byte[]  retval = new byte[ count ];

        for ( int i = 0; i < count; i++ ) { retval[ i ] = (byte) (left[ i ] + right[ i ]); }

        return retval;
    }

    /** add the values of two blobs */
    public  static  Blob  add( Blob left, Blob right )
        throws SQLException
    {
        if ( left ==  null ) { return null; }
        if ( right == null ) { return null; }

        return new HarmonySerialBlob
            ( add( left.getBytes( 1L, (int) left.length() ), right.getBytes( 1L, (int) right.length() ) ) );
    }

    /** concatenate two clobs */
    public  static  Clob  add( Clob left, Clob right )
        throws SQLException
    {
        if ( left ==  null ) { return null; }
        if ( right == null ) { return null; }

        return new HarmonySerialClob
            ( left.getSubString( 1L, (int) left.length() ) + right.getSubString( 1L, (int) right.length() ) );
    }

    /** Function for making a byte array from an array of ints */
    public  static  byte[]  makeByteArray( Integer... inputs )
    {
        if ( inputs == null )   { return null; }

        byte[]  retval = new byte[ inputs.length ];
        for ( int i = 0; i < inputs.length; i++ ) { retval[ i ] = (byte) inputs[ i ].intValue(); }

        return retval;
    }

    /** Function for making a byte array from an array of ints */
    public  static  Blob  makeBlob( int... inputs )
    {
        return makeBlob( 1, inputs );
    }

    /** Function for making a big Blob by repeating the inputs a number of times */
    public  static  Blob  makeBlob( int repeatCount, int... inputs )
    {
        if ( inputs == null )   { return null; }
        if ( (inputs.length == 0) || (repeatCount == 0) ) { return null; }

        byte[]  retval = new byte[ repeatCount * inputs.length ];
        int     idx = 0;
        
        for ( int i = 0; i < repeatCount; i++ )
        {
            for ( int val : inputs ) { retval[ idx++ ] = (byte) val; }
        }

        return new HarmonySerialBlob( retval );
    }

    /** Function for making a big Clob by repeating the inputs a number of times */
    public  static  Clob  makeClob( int repeatCount, String... inputs )
    {
        if ( inputs == null )   { return null; }
        if ( (inputs.length == 0) || (repeatCount == 0) ) { return null; }

        StringBuilder   buffer = new StringBuilder();
        int     idx = 0;
        
        for ( int i = 0; i < repeatCount; i++ )
        {
            for ( String val : inputs ) { buffer.append( val ); }
        }

        return new HarmonySerialClob( buffer.toString() );
    }

    /** Get the 0-based index into the blob */
    public  static  int getCell( Blob blob, long idx ) throws Exception
    {
        byte[]  bytes = blob.getBytes( idx + 1, 1 );

        return bytes[ 0 ];
    }

    /** Get the 0-based index into the clob */
    public  static  String getCell( Clob clob, long idx ) throws Exception
    {
        return clob.getSubString( idx + 1, 1 );
    }

    /** Function for returning an arbitrary integer value */
    public  static  Integer nop( Integer value ) { return value; }

    /** Extract the contents of an html document as an integer */
    public  static  int xmlX( String doc ) throws Exception
    {
        int     startIdx = doc.indexOf( BEGIN_HTML ) + BEGIN_HTML.length();
        int     endIdx = doc.indexOf( END_HTML );
        String  number = doc.substring( startIdx, endIdx );

        return Integer.parseInt( number );
    }

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

    /** Make sure that SQL authorization is turned on */
    private void    enableSQLAuthorization()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        ResultSet   rs = chattyPrepare
            (
             dboConnection,
             "values syscs_util.syscs_get_database_property( 'derby.database.sqlAuthorization' )"
             ).executeQuery();

        try {
            if ( rs.next() )
            {
                if ( "true".equals( rs.getString( 1 ) ) )   { return; }
            }
        }
        finally
        {
            rs.close();
        }

        goodStatement( dboConnection, "call syscs_util.syscs_set_database_property( 'derby.database.sqlAuthorization', 'true' )" );
        // bounce the database to turn on SQL authorization
        bounceDatabase( TEST_DBO );
    }
    
}
