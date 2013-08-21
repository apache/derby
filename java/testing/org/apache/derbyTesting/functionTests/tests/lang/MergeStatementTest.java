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

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

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
              "when matched and t3.c2 = t2.c2 then update set c3 = t2.c3\n"
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

        // Should fail at run time because the function modifies sql data.
        // But for now, this doesn't make it past the bind() phase.
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1 and t2.c2 = illegalFunction()\n" +
              "when matched then delete\n"
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

        // Trigger tansition tables may not be used as target tables.
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
        // The following syntax is actually good, but the compiler rejects these
        // statements because we haven't finished implementing MERGE.
        //
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "merge into t1\n" +
              "using table( integerList() ) i\n" +
              "on t1.c1 = i.s_r\n" +
              "when matched then delete\n"
              );
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "merge into t1\n" +
              "using v2\n" +
              "on t1.c1 = v2.c1\n" +
              "when matched then delete\n"
              );
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched then delete\n"
              );
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when matched and t1.c2 = t2.c2 then delete\n"
              );
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
              "when not matched then insert ( c2 ) values ( t2.c2 )\n"
              );
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched and t2.c2 is null then insert ( c2 ) values ( t2.c2 )\n"
              );
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c1, c2 ) values ( default, t2.c2 )\n"
              );
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "merge into t1\n" +
              "using t2\n" +
              "on t1.c1 = t2.c1\n" +
              "when not matched then insert ( c2, c3 ) values ( t2.c2, default )\n"
              );

        // Using a trigger transition table as a source table is probably ok.
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "create trigger trig3 after update on t2\n" +
              "referencing old table as old_cor new table as new_cor\n" +
              "for each statement\n" +
              "merge into t1\n" +
              "using new_cor\n" +
              "on t1.c1 = new_cor.c1\n" +
              "when not matched then insert ( c2 ) values ( new_cor.c2 )\n"
              );
        expectCompilationError
            ( dboConnection, NOT_IMPLEMENTED,
              "create trigger trig4 after update on t2\n" +
              "referencing old table as old_cor new table as new_cor\n" +
              "for each statement\n" +
              "merge into t1\n" +
              "using old_cor\n" +
              "on t1.c1 = old_cor.c1\n" +
              "when not matched then insert ( c2 ) values ( old_cor.c2 )\n"
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

    public  static  Connection  getNestedConnection()   throws SQLException
    {
        return DriverManager.getConnection( "jdbc:default:connection" );
    }

}
