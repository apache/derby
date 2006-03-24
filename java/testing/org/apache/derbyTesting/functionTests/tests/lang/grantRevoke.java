/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.grantRevoke

Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derbyTesting.functionTests.tests.lang;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.sql.*;

import java.util.ArrayList;
import java.util.HashMap;

public class grantRevoke
{
    private static final User[] users = { new User( "DAN", "MakeItFaster"),
                                          new User( "KREG", "visualWhat?"),
                                          new User( "JEFF", "HomeRun61"),
                                          new User( "AMES", "AnyVolunteer?"),
                                          new User( "JERRY", "SacreBleu"),
                                          new User( "HOWARDR", "IamBetterAtTennis"),
                                          new User( "FRANCOIS", "paceesalute"),
                                          new User( "JAMIE", "MrNamePlates")};
    private static final User owner = new User( "OWNER", "BigCheese");
    private static final User publicUser = new User( "PUBLIC", null);
    private DatabaseMetaData dbmd;
    private static boolean routineCalled = false;
    private int errorCount = 0;
    
	public static void main(String[] args)
    {
        grantRevoke tester = new grantRevoke();
        tester.doIt( args);
    }

    private void doIt( String[] args)
    {
		try
        {
			// use the ij utility to read the property file and
			// make the initial connection.
            ij.getPropertyArg(args);
            owner.setConnection( ij.startJBMS());
            dbmd = owner.getConnection().getMetaData();

            runGrantTests();
            // We can't test much of REVOKE unless GRANT works
            if( errorCount == 0)
                runRevokeTests( );

            System.out.println( "Error cases.");
            testErrors( stdErrorCases);
		}
		catch (SQLException sqle) {
			unexpectedException( null, sqle);
		}
		catch (Throwable t) {
            errorCount++;
			t.printStackTrace(System.out);
		}
        if( errorCount == 0)
            System.out.println( "PASSED.");
        else
            System.out.println( "FAILED. " + errorCount + ((errorCount > 1) ? " errors" : " error"));
        System.exit( 0);
	} // end of doIt

    private void runGrantTests( ) throws SQLException
    {
        setup( grantTestSetupSQL);

        // Test simple grant
        testOneStatement( "Grant",
                          "grant select on s1.t1 to " + users[0].name,
                          new PrivCheck[] { new SelectPrivCheck( true, false, users[0], "S1", "T1", null),
                                            new SelectPrivCheck( false, false, users[1], "S1", "T1", null)},
                          "simple grant");
        // all privileges, default schema, multiple users
        owner.stmt.executeUpdate( "set schema s2");
        testOneStatement( "Grant",
                          "grant all privileges on t1 to " + users[1].name + "," + users[2].name,
                          new PrivCheck[] { new SelectPrivCheck( true, false, users[1], "S2", "T1", null),
                                            new DeletePrivCheck( true, false, users[1], "S2", "T1"),
                                            new InsertPrivCheck( true, false, users[1], "S2", "T1"),
                                            new UpdatePrivCheck( true, false, users[1], "S2", "T1", null),
                                            new ReferencesPrivCheck( true, false, users[1], "S2", "T1", null),
                                            new TriggerPrivCheck( true, false, users[1], "S2", "T1"),

                                            new SelectPrivCheck( true, false, users[2], "S2", "T1", null),
                                            new DeletePrivCheck( true, false, users[2], "S2", "T1"),
                                            new InsertPrivCheck( true, false, users[2], "S2", "T1"),
                                            new UpdatePrivCheck( true, false, users[2], "S2", "T1", null),
                                            new ReferencesPrivCheck( true, false, users[2], "S2", "T1", null),
                                            new TriggerPrivCheck( true, false, users[2], "S2", "T1"),
                                     
                                            new SelectPrivCheck( false, false, users[0], "S2", "T1", null),
                                            new DeletePrivCheck( false, false, users[0], "S2", "T1"),
                                            new InsertPrivCheck( false, false, users[0], "S2", "T1"),
                                            new UpdatePrivCheck( false, false, users[0], "S2", "T1", null),
                                            new ReferencesPrivCheck( false, false, users[0], "S2", "T1", null),
                                            new TriggerPrivCheck( false, false, users[0], "S2", "T1"),

                                            new SelectPrivCheck( false, false, users[1], "S1", "T1", null),
                                            new SelectPrivCheck( false, false, users[1], "S2", "T2", null),
                          },
                          "all privileges, multiple users (2)");
        // Column privileges
        testOneStatement( "Grant",
                          "grant select(c1),update(c3,c2),references(c3,c1,c2) on s1.t1 to " + users[3].name,
                          new PrivCheck[] { new SelectPrivCheck( true, false, users[3], "S1", "T1",
                                                                 new String[] {"C1"}),
                                            new SelectPrivCheck( false, false, users[3], "S1", "T1",
                                                                 new String[] {"C2"}),
                                            new SelectPrivCheck( false, false, users[3], "S1", "T1",
                                                                 new String[] {"C3"}),
                                            new SelectPrivCheck( false, false, users[3], "S1", "T1", null),
                                            new UpdatePrivCheck( true, false, users[3], "S1", "T1",
                                                                 new String[] {"C2","C3"}),
                                            new UpdatePrivCheck( false, false, users[3], "S1", "T1",
                                                                 new String[] {"C1"}),
                                            new ReferencesPrivCheck( true, false, users[3], "S1", "T1",
                                                                     new String[] {"C1","C2","C3"}),
                                            new ReferencesPrivCheck( false, false, users[3], "S1", "T1", null)
                          },
                          "Column privileges");
        // Execute on function when there is a procedure with the same name
        testOneStatement( "Grant",
                          "grant execute on function s1.f1 to " + users[0].name,
                          new PrivCheck[] { new ExecutePrivCheck( true, false, users[0], "S1", "F1", true),
                                            new ExecutePrivCheck( false, false, users[0], "S1", "F1", false),
                                            new ExecutePrivCheck( false, false, users[1], "S1", "F1", true),
                          },
                          "execute on function with like named procedure");
        // Execute on procedure
        testOneStatement( "Grant",
                          "grant execute on procedure s1.p1 to " + users[0].name,
                          new PrivCheck[] { new ExecutePrivCheck( true, false, users[0], "S1", "P1", false),
                                            new ExecutePrivCheck( false, false, users[1], "S1", "P1", false),
                          },
                          "execute on procedure");

        // PUBLIC
        testOneStatement( "Grant",
                          "grant select, references(c1) on table s2.t2 to public",
                          new PrivCheck[] { new SelectPrivCheck( true, true, publicUser, "S2", "T2", null),
                                            new SelectPrivCheck( false, true, users[1], "S2", "T2", null),
                                            new SelectPrivCheck( false, false, publicUser, "S2", "NOPERMS", null),
                                            new UpdatePrivCheck( false, false, publicUser, "S2", "T2", null),
                                            new ReferencesPrivCheck( true, true, publicUser, "S2", "T2",
                                                                     new String[] {"C1"}),
                                            new ReferencesPrivCheck( false, false, publicUser, "S2", "T2", null)
                          },
                          "PUBLIC table privileges");
        testOneStatement( "Grant",
                          "grant execute on procedure s1.p1 to Public",
                          new PrivCheck[] { new ExecutePrivCheck( true, true, publicUser, "S1", "P1", false),
                                            // user0 should still have his own execute privilege
                                            new ExecutePrivCheck( true, true, users[0], "S1", "P1", false),
                                            // user1 should not have an individual execute privilege
                                            new ExecutePrivCheck( false, true, users[1], "S1", "P1", false)
                          },
                          "PUBLIC routine privileges");

        testGrantRollbackAndCommit( );

        System.out.println( "Test metadata supports methods.");
        if( dbmd.supportsCatalogsInPrivilegeDefinitions())
            reportFailure( "DatabaseMetaData.supportsCatalogsInPrivilegeDefinitions returned true.");
        if( ! dbmd.supportsSchemasInPrivilegeDefinitions())
            reportFailure( "DatabaseMetaData.supportsSchemasInPrivilegeDefinitions returned false.");
    } // end of runGrantTests

    private void testOneStatement( String stmtName,
                                   String sql,
                                   PrivCheck[] checks,
                                   String testLabel)
    {
        testOneStatement( stmtName, sql, checks, true, testLabel);
    }
    
    private void testOneStatement( String stmtName,
                                   String sql,
                                   PrivCheck[] checks,
                                   boolean runStatements,
                                   String testLabel)
    {
        System.out.println( stmtName + " test: " + testLabel);
        try
        {
            owner.stmt.executeUpdate( sql);
            runChecks( checks, runStatements, false, testLabel);
        }
        catch( SQLException sqle)
        {
            unexpectedException( testLabel, sqle);
        }
    } // end of testOneStatement
    
    private void setup( String[] setupSQL) throws SQLException
    {
        boolean autoCommit = owner.getConnection().getAutoCommit();
        owner.getConnection().setAutoCommit( false);
        for( int i = 0; i < setupSQL.length; i++)
            owner.stmt.executeUpdate( setupSQL[i]);
        owner.getConnection().commit();
        owner.getConnection().setAutoCommit( autoCommit);
    } // end of setup

    private void testGrantRollbackAndCommit( )
    {
        System.out.println( "Test grant rollback and commit");
        PrivCheck[] preExistingPrivChecks =
          new PrivCheck[] { new SelectPrivCheck( true, true, publicUser, "S2", "T2", null),
                            new UpdatePrivCheck( false, false, publicUser, "S2", "T2", null)
                            
          };
        PrivCheck[] tableChecks1 =
          new PrivCheck[] { new SelectPrivCheck( true, false, users[0], "S2", "T3", new String[] {"C2"}),
                            new DeletePrivCheck( true, false, users[0], "S2", "T3")
          };
        PrivCheck[] tableChecks2 =
          new PrivCheck[] { new TriggerPrivCheck( true, true, publicUser, "S2", "T2")
          };
        PrivCheck[] routineChecks =
          new PrivCheck[] { new ExecutePrivCheck( true, false, users[0], "S2", "F1", true)};
        PrivCheck[] noChecks = new PrivCheck[0];

        try
        {
            runChecks( preExistingPrivChecks, false, "transaction test pre-existing table privileges");
            owner.getConnection().setAutoCommit( false);
            for( int i = 0; i < 2; i++)
            {
                // test rollback on i == 0, commit on i == 1
                // Add a new row in the SYSTABLEPERMS table
                testOneStatement( "Grant",
                                  "grant select(c2), delete on s2.t3 to " + users[0].name,
                                  tableChecks1, false,
                                  "table privileges in transaction");
                // Update an existing row in the SYSTABLEPERMS table
                testOneStatement( "Grant",
                                  "grant trigger on s2.t2 to public",
                                  tableChecks2, false,
                                  "table privileges in transaction");
                testOneStatement( "Grant",
                                  "grant execute on function s2.f1 to " + users[0].name,
                                  routineChecks, false,
                                  "routine privileges in transaction");
                if( i == 0)
                    owner.getConnection().rollback();
                else
                    owner.getConnection().commit();
                runChecks( tableChecks1, i == 0, ((i == 0) ? "rolled back" : "committed") + " table privileges");
                runChecks( tableChecks2, i == 0, ((i == 0) ? "rolled back" : "committed") + " table privileges");
                runChecks( routineChecks, i == 0, ((i == 0) ? "rolled back" : "committed") + " routine privileges");
                runChecks( preExistingPrivChecks, false, "transaction test pre-existing table privileges");
            }
        }
        catch( SQLException sqle)
        {
            unexpectedException( "rollback and commit test", sqle);
        }
    } // end of testGrantRollbackAndCommit

    private static final String[] grantTestSetupSQL =
    {
        "create schema s1",
        "create schema s2",
        "create table s1.t1(c1 int, c2 int, c3 int)",
        "create table s2.t1(c1 int, c2 int, c3 int)",
        "create table s2.t2(c1 int, c2 int, c3 int)",
        "create table s2.t3(c1 int, c2 int, c3 int)",
        "create table s2.noPerms(c1 int, c2 int, c3 int)",
        "create function s1.f1() returns int" +
        "  language java parameter style java" +
        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s1F1'" +
        "  no sql called on null input",
        "create function s2.f1() returns int" +
        // RESOLVE Derby does not implement SPECIFIC names
        //         "  specific s2.s2sp1" +
        "  language java parameter style java" +
        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F1a'" +
        "  no sql called on null input",
        /* RESOLVE Derby doesn't seem to support function overloading. It doesn't allow us to create two
         * functions with the same name but different signatures. (Though the StaticMethodCallNode.bindExpression
         * method does have code to handle overloaded methods). So we cannot throughly test
         * grant/revoke on overloaded procedures.
         */
         
        //         "create function s2.f1( p1 char(8)) returns int" +
        //         "  language java parameter style java" +
        //         "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F1b'" +
        //         "  no sql called on null input",
        //         "create function s2.f1( char(8), char(8)) returns int" +
        //         "  language java parameter style java" +
        //         "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F1c'" +
        //         "  no sql called on null input",
        //         "create function s2.f1( int) returns int" +
        //         "  language java parameter style java" +
        //         "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F1d'" +
        //         "  no sql called on null input",
        "create function s2.f2( p1 char(8), p2 integer) returns int" +
        "  language java parameter style java" +
        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F2'" +
        "  no sql called on null input",

        /* functions and procedures are supposed to have separate name spaces. Make sure that this does
         * not confuse grant/revoke.
         */
        "create procedure s1.f1( )" +
        "  language java parameter style java" +
        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s1F1P'" +
        "  no sql called on null input",
        "create procedure s1.p1( )" +
        "  language java parameter style java" +
        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s1P1'" +
        "  no sql called on null input"
    };

    public static int s1F1()
    {
        routineCalled = true;
        return 1;
    }
    public static int s2F1a()
    {
        routineCalled = true;
        return 1;
    }
    public static int s2F1b( String s)
    {
        routineCalled = true;
        return 1;
    }
    public static int s2F1c( String s1, String s2)
    {
        routineCalled = true;
        return 1;
    }
    public static int s2F1d( int i)
    {
        routineCalled = true;
        return 1;
    }
    public static int s2F2()
    {
        routineCalled = true;
        return 1;
    }
    public static void s1F1P( )
    {
        routineCalled = true;
    }
    public static void s1P1( )
    {
        routineCalled = true;
    }
        
    private void runRevokeTests( ) throws SQLException
    {
        setup( revokeTestSetupSQL);
        owner.getConnection().setAutoCommit( true);

        // Revoke when there are no permissions
        PrivCheck[] privCheck1 = { new SelectPrivCheck( false, false, users[0], "R1", "T1", null),
                                   new SelectPrivCheck( false, false, users[0], "R1", "T1", new String[] {"C2"}),
                                   new UpdatePrivCheck( false, false, users[1], "R1", "T1", new String[] {"C1", "C3"}),
                                   new ExecutePrivCheck( false, false, users[0], "R1", "P1", false)};
        owner.stmt.executeUpdate( "set schema r1");
        runChecks( privCheck1, false, "Initial revoke test conditions");
        testOneStatement( "Revoke",
                          "revoke all Privileges on t1 from " + users[0].name,
                          privCheck1,
                          "all with no permissions");
        testOneStatement( "Revoke",
                          "revoke execute on procedure r1.p1 from " + users[0].name + " restrict",
                          privCheck1,
                          "execute with no permissions");
        testOneStatement( "Revoke",
                          "revoke select(c2), update(c1,c3) on table t1 from " + users[1].name,
                          privCheck1,
                          "column with no permissions");

        // Revoke single table permissions, single user
        owner.stmt.executeUpdate( "grant all privileges on r2.t1 to " + users[0].name);
        owner.stmt.executeUpdate( "grant update(c3) on r2.t1 to " + users[0].name);
        testOneStatement( "Revoke",
                          "revoke update on r2.t1 from " + users[0].name,
                          new PrivCheck[] { new SelectPrivCheck( true, false, users[0], "R2", "T1", null),
                                            new UpdatePrivCheck( false, false, users[0], "R2", "T1", null),
                                            new UpdatePrivCheck( false, false, users[0], "R2", "T1",
                                                                 new String[] {"C3"}),
                                            new InsertPrivCheck( true, false, users[0], "R2", "T1"),
                                            new DeletePrivCheck( true, false, users[0], "R2", "T1"),
                                            new ReferencesPrivCheck( true, false, users[0], "R2", "T1", null),
                                            new TriggerPrivCheck( true, false, users[0], "R2", "T1")
                          },
                          "single table privilege, one user");
        testOneStatement( "Revoke",
                          "revoke all privileges on r2.t1 from " + users[0].name,
                          new PrivCheck[] { new SelectPrivCheck( false, false, users[0], "R2", "T1", null),
                                            new UpdatePrivCheck( false, false, users[0], "R2", "T1", null),
                                            new UpdatePrivCheck( false, false, users[0], "R2", "T1",
                                                                 new String[] {"C3"}),
                                            new InsertPrivCheck( false, false, users[0], "R2", "T1"),
                                            new DeletePrivCheck( false, false, users[0], "R2", "T1"),
                                            new ReferencesPrivCheck( false, false, users[0], "R2", "T1", null),
                                            new TriggerPrivCheck( false, false, users[0], "R2", "T1")
                          },
                          "single table privilege, one user");

        // Revoke multiple table & column permissions, multiple users some of which do not have the permission
        // Leave one user some permissions on the table, another no permissions
        owner.stmt.executeUpdate( "grant select on t1 to " + users[0].name + "," + users[1].name + "," + users[2].name);
        owner.stmt.executeUpdate( "grant update(c1,c2,c3) on t1 to " + users[0].name);
        owner.stmt.executeUpdate( "grant update(c3) on t1 to " + users[1].name);
        owner.stmt.executeUpdate( "grant trigger on t1 to " + users[0].name);
        runChecks( new PrivCheck[] { new SelectPrivCheck( true, false, users[0], "R1", "T1", null),
                                     new SelectPrivCheck( true, false, users[1], "R1", "T1", null),
                                     new SelectPrivCheck( true, false, users[2], "R1", "T1", null),
                                     new UpdatePrivCheck( true, false, users[0], "R1", "T1",
                                                          new String[] {"C1", "C2", "C3"}),
                                     new UpdatePrivCheck( true, false, users[1], "R1", "T1",
                                                          new String[] {"C3"}),
                                     new TriggerPrivCheck( true, false, users[0], "R1", "T1"),
                                     new TriggerPrivCheck( false, false, users[1], "R1", "T1")
                   },
                   false,
                   "setup (1)");
        testOneStatement( "Revoke",
                          "revoke select, update(c2,c3) on t1 from " + users[0].name + ","
                          + users[1].name + "," + users[2].name,
                          new PrivCheck[] { new SelectPrivCheck( false, false, users[0], "R1", "T1", null),
                                            new SelectPrivCheck( false, false, users[1], "R1", "T1", null),
                                            new SelectPrivCheck( false, false, users[2], "R1", "T1", null),
                                            new UpdatePrivCheck( true, false, users[0], "R1", "T1",
                                                                 new String[] {"C1"}),
                                            new UpdatePrivCheck( false, false, users[0], "R1", "T1",
                                                                 new String[] {"C2", "C3"}),
                                            new UpdatePrivCheck( false, false, users[1], "R1", "T1",
                                                                 new String[] {"C1", "C2", "C3"}),
                                            new TriggerPrivCheck( true, false, users[0], "R1", "T1"),
                                            new TriggerPrivCheck( false, false, users[1], "R1", "T1")
                          },
                          "multiple table permissions, multiple users");
        testOneStatement( "Revoke",
                          "revoke update on r1.t1 from " + users[0].name,
                          new PrivCheck[] { new UpdatePrivCheck( false, false, users[0], "R1", "T1",
                                                                 new String[] {"C1"}),
                                            new UpdatePrivCheck( false, false, users[0], "R1", "T1", null)
                          },
                          "table privilege implies column privileges");
        // Revoke all
        testOneStatement( "Revoke",
                          "revoke all privileges on r1.t1 from " + users[0].name,
                          new PrivCheck[] {  new UpdatePrivCheck( false, false, users[0], "R1", "T1",
                                                                  new String[] {"C1", "C2", "C3"}),
                                             new TriggerPrivCheck( false, false, users[0], "R1", "T1")
                          },
                          "all privileges");
        
        // Revoke function permission
        owner.stmt.executeUpdate( "grant execute on function f1 to " + users[0].name + "," + users[1].name);
        owner.stmt.executeUpdate( "grant execute on procedure f1 to " + users[0].name);
        runChecks( new PrivCheck[] { new ExecutePrivCheck( true, false, users[0], "R1", "F1", true),
                                     new ExecutePrivCheck( true, false, users[1], "R1", "F1", true),
                                     new ExecutePrivCheck( true, false, users[0], "R1", "F1", false)},
                   false,
                   "setup for revoke execute");
        testOneStatement( "Revoke",
                          "revoke execute on function f1 from " + users[0].name + " restrict",
                          new PrivCheck[] { new ExecutePrivCheck( false, false, users[0], "R1", "F1", true),
                                            new ExecutePrivCheck( true, false, users[1], "R1", "F1", true),
                                            new ExecutePrivCheck( true, false, users[0], "R1", "F1", false)},
                          "function execute permission");

        // Revoke procedure permission
        testOneStatement( "Revoke",
                          "revoke execute on procedure f1 from " + users[0].name + " restrict",
                          new PrivCheck[] { new ExecutePrivCheck( false, false, users[0], "R1", "F1", true),
                                            new ExecutePrivCheck( true, false, users[1], "R1", "F1", true),
                                            new ExecutePrivCheck( false, false, users[0], "R1", "F1", false)},
                          "function execute permission");

        // Revoke privileges from user when there is PUBLIC permission
        owner.stmt.executeUpdate( "grant select, delete on r2.t1 to public");
        owner.stmt.executeUpdate( "grant select, delete on r2.t1 to " + users[1].name + "," + users[2].name);
        owner.stmt.executeUpdate( "grant update(c1,c3) on r2.t1 to public");
        owner.stmt.executeUpdate( "grant update(c1,c3) on r2.t1 to " + users[1].name + "," + users[2].name);
        runChecks( new PrivCheck[] { new SelectPrivCheck( true, true, users[1], "R2", "T1", null),
                                     new SelectPrivCheck( true, true, users[2], "R2", "T1", null),
                                     new SelectPrivCheck( true, true, publicUser, "R2", "T1", null),
                                     new DeletePrivCheck( true, true, users[1], "R2", "T1"),
                                     new DeletePrivCheck( true, true, users[2], "R2", "T1"),
                                     new DeletePrivCheck( true, true, publicUser, "R2", "T1"),
                                     new UpdatePrivCheck( true, true, users[1], "R2", "T1",
                                                          new String[] {"C1", "C3"}),
                                     new UpdatePrivCheck( true, true, users[2], "R2", "T1",
                                                          new String[] {"C1", "C3"}),
                                     new UpdatePrivCheck( true, true, publicUser, "R2", "T1",
                                                          new String[] {"C1", "C3"})},
                   false,
                   "setup for revoke individual permissions leaving public permissions");
        testOneStatement( "Revoke",
                          "revoke select, update(c1,c3), delete on table r2.t1 from " + users[1].name,
                          new PrivCheck[] { new SelectPrivCheck( false, true, users[1], "R2", "T1", null),
                                            new SelectPrivCheck( true, true, users[2], "R2", "T1", null),
                                            new SelectPrivCheck( true, true, publicUser, "R2", "T1", null),
                                            new DeletePrivCheck( false, true, users[1], "R2", "T1"),
                                            new DeletePrivCheck( true, true, users[2], "R2", "T1"),
                                            new DeletePrivCheck( true, true, publicUser, "R2", "T1"),
                                            new UpdatePrivCheck( false, true, users[1], "R2", "T1",
                                                                 new String[] {"C1", "C2", "C3"}),
                                            new UpdatePrivCheck( true, true, users[2], "R2", "T1",
                                                                 new String[] {"C1", "C3"}),
                                            new UpdatePrivCheck( true, true, publicUser, "R2", "T1",
                                                                 new String[] {"C1", "C3"})},
                          "individual permissions leaving public permissions");
        testOneStatement( "Revoke",
                          "revoke select, update(c1,c3), delete on table r2.t1 from public",
                          new PrivCheck[] { new SelectPrivCheck( false, false, users[1], "R2", "T1", null),
                                            new SelectPrivCheck( true, false, users[2], "R2", "T1", null),
                                            new SelectPrivCheck( false, false, publicUser, "R2", "T1", null),
                                            new DeletePrivCheck( false, true, users[1], "R2", "T1"),
                                            new DeletePrivCheck( true, true, users[2], "R2", "T1"),
                                            new DeletePrivCheck( false, true, publicUser, "R2", "T1"),
                                            new UpdatePrivCheck( false, false, users[1], "R2", "T1",
                                                                 new String[] {"C1", "C2", "C3"}),
                                            new UpdatePrivCheck( true, false, users[2], "R2", "T1",
                                                                 new String[] {"C1", "C3"}),
                                            new UpdatePrivCheck( false, false, publicUser, "R2", "T1",
                                                                 new String[] {"C1", "C3"})},
                          "public permissions");

        owner.stmt.executeUpdate( "grant execute on function r2.f1 to public");
        owner.stmt.executeUpdate( "grant execute on function r2.f1 to " + users[2].name + "," + users[0].name);
        runChecks( new PrivCheck[] { new ExecutePrivCheck( true, true, users[0], "R2", "F1", true),
                                     new ExecutePrivCheck( true, true, users[2], "R2", "F1", true),
                                     new ExecutePrivCheck( true, true, publicUser, "R2", "F1", true)},
                   false,
                   "setup for revoke execute leaving public permission");
        testOneStatement( "Revoke",
                          "revoke execute on function r2.f1 from " + users[0].name + " restrict",
                          new PrivCheck[] { new ExecutePrivCheck( false, true, users[0], "R2", "F1", true),
                                            new ExecutePrivCheck( true, true, users[2], "R2", "F1", true),
                                            new ExecutePrivCheck( true, true, publicUser, "R2", "F1", true)},
                          "execute leaving public permission");
        testOneStatement( "Revoke",
                          "revoke execute on function r2.f1 from Public restrict",
                          new PrivCheck[] { new ExecutePrivCheck( false, false, users[0], "R2", "F1", true),
                                            new ExecutePrivCheck( true, false, users[2], "R2", "F1", true),
                                            new ExecutePrivCheck( false, false, publicUser, "R2", "F1", true)},
                          "execute leaving public permission");

        testRevokeRollback( );
        
        testAbandonedView( );
        testAbandonedTrigger( );
        testAbandonedConstraint( );
    } // end of runRevokeTests

    private void testErrors( String[][] errorCases) throws SQLException
    {
		System.out.println("Testing error cases ...");
        for( int i = 0; i < errorCases.length; i++)
        {
            try
            {
				System.out.println("testErrors: " + errorCases[i][0]);
                owner.stmt.executeUpdate( errorCases[i][0]);
                reportFailure( "No error generated by \"" + errorCases[i][0] + "\"");
            }
            catch( SQLException sqle)
            {
                if( ! errorCases[i][1].equals( sqle.getSQLState()))
                    reportFailure( "Incorrect SQLState for error case " + i
                                   + ".  Expected " + errorCases[i][1] + ", got " + sqle.getSQLState()
                                   + ": " + sqle.getMessage());
                else if( ! errorCases[i][2].equals( sqle.getMessage()))
                    reportFailure( new String[] {"Incorrect message for error case " + i + ".",
                                                 "  Expected " + errorCases[i][2],
                                                 "  Got " + sqle.getMessage()});
            }
        }
    } // end of testErrors

    private static final String[][] stdErrorCases =
    {
        {"grant xx on s1.t1 to " + users[0].name, "42X01",
         "Syntax error: Encountered \"xx\" at line 1, column 7."}, // invalid action
        {"grant between on s1.t1 to " + users[0].name, "42X01",
         "Syntax error: Encountered \"between\" at line 1, column 7."}, // invalid reserved word action
        {"grant select on schema t1 to " + users[0].name,
         "42X01", "Syntax error: Encountered \"schema\" at line 1, column 17."},
        {"grant select on decimal t1 to " + users[0].name, "42X01",
         "Syntax error: Encountered \"decimal\" at line 1, column 17."},
        {"grant select(nosuchCol) on s1.t1 to " + users[0].name, "42X14",
         "'NOSUCHCOL' is not a column in table or VTI 'S1.T1'."},

        {"grant select on nosuch.t1 to " + users[0].name, "42Y07", "Schema 'NOSUCH' does not exist"},
        {"grant select on s1.nosuch to " + users[0].name, "42X05", "Table 'S1.NOSUCH' does not exist."},
        {"grant execute on function nosuch.f0 to " + users[0].name, "42Y07", "Schema 'NOSUCH' does not exist"},
        {"grant execute on function s1.nosuch to " + users[0].name, "42Y03",
         "'S1.NOSUCH' is not recognized as a function or procedure."},
        {"grant execute on function s1.p1 to " + users[0].name, "42Y03",
         "'S1.P1' is not recognized as a function or procedure."},
        // 10
        {"grant execute on procedure nosuch.f0 to " + users[0].name, "42Y07", "Schema 'NOSUCH' does not exist"},
        {"grant execute on procedure s1.nosuch to " + users[0].name, "42Y03",
         "'S1.NOSUCH' is not recognized as a function or procedure."},
        {"grant execute on procedure s1.f2 to " + users[0].name, "42Y03",
         "'S1.F2' is not recognized as a function or procedure."},
        {"grant execute on table s1.t1 to " + users[0].name, "42X01",
         "Syntax error: Encountered \"table\" at line 1, column 18."},
        {"grant select on function s1.f1 to " + users[0].name, "42X01",
         "Syntax error: Encountered \"function\" at line 1, column 17."},

        {"grant select on procedure s1.p1 to " + users[0].name, "42X01",
         "Syntax error: Encountered \"procedure\" at line 1, column 17."},
        {"grant execute on function s1.f1 to " + users[0].name + " restrict", "42X01",
         "Syntax error: Encountered \"restrict\" at line 1, column 40."}, // "restrict" invalid in grant
        {"revoke execute on function s1.f1 from " + users[0].name, "42X01",
         "Syntax error: Encountered \"<EOF>\" at line 1, column 41."}, // Missing "restrict"
        {"revoke select on s1.t1 from " + users[0].name + " restrict", "42X01",
         "Syntax error: Encountered \"restrict\" at line 1, column 33."}, // "restrict" invalid in table revoke
        {"grant delete(c1) on s1.t1 to " + users[0].name, "42X01",
         "Syntax error: Encountered \"(\" at line 1, column 13."}, // Column list invalid with delete
        // 20
        {"grant trigger(c1) on s1.t1 to " + users[0].name, "42X01",
         "Syntax error: Encountered \"(\" at line 1, column 14."} // Column list invalid with trigger
    }; // end of String[][] errorCases
        
    private void testRevokeRollback( ) throws SQLException
    {
        owner.getConnection().setAutoCommit( false);
        owner.stmt.executeUpdate( "grant select(c1,c2), update(c1), insert, delete on r2.t3 to " + users[0].name);
        owner.stmt.executeUpdate( "grant select, references on r2.t3 to " + users[1].name);
        owner.stmt.executeUpdate( "grant select on r2.t3 to " + users[2].name);
        owner.stmt.executeUpdate( "grant execute on procedure r1.p1 to " + users[0].name);
        owner.getConnection().commit();
        runChecks( new PrivCheck[] { new SelectPrivCheck( true, false, users[0], "R2", "T3",
                                                          new String[] { "C1", "C2"}),
                                     new UpdatePrivCheck( true, false, users[0], "R2", "T3",
                                                          new String[] { "C1"}),
                                     new InsertPrivCheck( true, false, users[0], "R2", "T3"),
                                     new DeletePrivCheck( true, false, users[0], "R2", "T3"),
                                     new SelectPrivCheck( true, false, users[1], "R2", "T3", null),
                                     new ReferencesPrivCheck( true, false, users[1], "R2", "T3", null),
                                     new SelectPrivCheck( true, false, users[2], "R2", "T3", null),
                                     new ExecutePrivCheck( true, false, users[0], "R1", "P1", false)
                   },
                   false,
                   "setup for rollback test");
        for( int i = 0; i < 2; i++)
        {
            boolean doRollback = (i == 0);
            testOneStatement( "Revoke",
                              "revoke select(c2), update(c1), delete on r2.t3 from " + users[0].name,
                              new PrivCheck[] { new SelectPrivCheck( true, false, users[0], "R2", "T3",
                                                                     new String[] { "C1"}),
                                                new SelectPrivCheck( false, false, users[0], "R2", "T3",
                                                                     new String[] { "C2", "C3"}),
                                                new UpdatePrivCheck( false, false, users[0], "R2", "T3",
                                                                     new String[] { "C1", "C2", "C3"}),
                                                new InsertPrivCheck( true, false, users[0], "R2", "T3"),
                                                new DeletePrivCheck( false, false, users[0], "R2", "T3")
                              },
                              false,
                              "table privileges (uncommitted)");
            testOneStatement( "Revoke",
                              "revoke references on r2.t3 from " + users[1].name,
                              new PrivCheck[] { new SelectPrivCheck( true, false, users[1], "R2", "T3", null),
                                                new ReferencesPrivCheck( false, false, users[1], "R2", "T3", null)},
                              false,
                              "table privileges (uncommitted)");
            testOneStatement( "Revoke",
                              "revoke select on r2.t3 from " + users[2].name,
                              new PrivCheck[] { new SelectPrivCheck( false, false, users[2], "R2", "T3", null)},
                              false,
                              "table privileges (uncommitted)");
            testOneStatement( "Revoke",
                              "revoke execute on procedure r1.p1 from " + users[0].name + " restrict",
                              new PrivCheck[] { new ExecutePrivCheck( false, false, users[0], "R1", "P1", false)},
                              false,
                              "execute privilege (uncommitted)");
            if( doRollback)
                owner.getConnection().rollback();
            else
                owner.getConnection().commit();
            runChecks( new PrivCheck[] { new SelectPrivCheck( doRollback, false, users[0], "R2", "T3",
                                                              new String[] { "C2"}),
                                         new UpdatePrivCheck( doRollback, false, users[0], "R2", "T3",
                                                              new String[] { "C1"}),
                                         new DeletePrivCheck( doRollback, false, users[0], "R2", "T3"),
                                         new ReferencesPrivCheck( doRollback, false, users[1], "R2", "T3", null),
                                         new SelectPrivCheck( doRollback, false, users[2], "R2", "T3", null),
                                         new ExecutePrivCheck( doRollback, false, users[0], "R1", "P1", false)
                       },
                       false,
                       doRollback ? "rollback of revokes" : "commit of revokes");
        }
        owner.getConnection().setAutoCommit( true);
    } // end of testRevokeRollback

    private void testAbandonedView( ) throws SQLException
    {
        // RESOLVE
    }

    private void testAbandonedTrigger( ) throws SQLException
    {
        // RESOLVE
    }

    private void testAbandonedConstraint( ) throws SQLException
    {
        // RESOLVE
    }
    
    private static final String[] revokeTestSetupSQL =
    {
        "create schema r1",
        "create schema r2",
        "create table r1.t1(c1 int, c2 int, c3 int)",
        "create table r2.t1(c1 int, c2 int, c3 int)",
        "create table r2.t2(c1 int, c2 int, c3 int)",
        "create table r2.t3(c1 int, c2 int, c3 int)",
        "create function r1.f1() returns int" +
        "  language java parameter style java" +
        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s1F1'" +
        "  no sql called on null input",

        /* functions and procedures are supposed to have separate name spaces. Make sure that this does
         * not confuse grant/revoke.
         */
        "create procedure r1.f1()" +
        "  language java parameter style java" +
        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s1P1'" +
        "  no sql called on null input",
        "create function r2.f1() returns int" +
        "  language java parameter style java" +
        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F1a'" +
        "  no sql called on null input",
        "create function r2.f2( p1 char(8), p2 integer) returns int" +
        "  language java parameter style java" +
        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F2'" +
        "  no sql called on null input",
        "create procedure r1.p1( )" +
        "  language java parameter style java" +
        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s1P1'" +
        "  no sql called on null input"
    };

    private void runChecks( PrivCheck[] checks,
                            boolean invertExpecation,
                            String testLabel)
    {
        runChecks( checks, true, invertExpecation, testLabel);
    }
    
    private void runChecks( PrivCheck[] checks,
                            boolean runStatements,
                            boolean invertExpecation,
                            String testLabel)
    {
        try
        {
            for( int i = 0; i < checks.length; i++)
            {
                if( invertExpecation)
                    checks[i].invertExpectation();
                checks[i].checkPriv( runStatements, testLabel);
                if( invertExpecation)
                    checks[i].invertExpectation();
            }
        }
        catch( SQLException sqle)
        {
            unexpectedException( testLabel, sqle);
        }
    } // end of runChecks
        
    private void reportFailure( String msg)
    {
        errorCount++;
        System.out.println( msg);
    }
        
    private void reportFailure( String[] msg)
    {
        errorCount++;
        for( int i = 0; i < msg.length; i++)
            System.out.println( msg[i]);
    }
    
    private void unexpectedException( String testLabel, SQLException sqle)
    {
        reportFailure( (testLabel == null) ?  "Unexpected exception"
                       : ( "Unexpected exception in " + testLabel + " test"));
        while( sqle != null)
        {
            System.out.println( sqle.getSQLState() + ": " + sqle.getMessage());
            SQLException next = sqle.getNextException();
            if( next == null)
            {
                sqle.printStackTrace(System.out);
                break;
            }
            sqle = next;
        }
    }

    private abstract class PrivCheck
    {
        boolean expectPriv;
        boolean privIsPublic;
        User user;
        String schema;

        PrivCheck( boolean expectPriv, boolean privIsPublic, User user, String schema)
        {
            this.expectPriv = expectPriv;
            this.privIsPublic = privIsPublic;
            this.user = user;
            this.schema = schema;
        }

        void invertExpectation()
        {
            expectPriv = ! expectPriv;
        }

        void checkPriv( boolean runStatements, String testLabel) throws SQLException
        {
            checkSQL( testLabel);
            checkMetaData( testLabel);
            if( runStatements && ! user.isPublic())
            {
                checkUser( user, testLabel);
            }
        }

        /**
         * Run the appropriate SQL statement to see if Derby really grants the privilege or not
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        abstract void checkUser( User user, String testLabel) throws SQLException;

        /**
         * Use the database metadata to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        abstract void checkMetaData( String testLabel) throws SQLException;

        /**
         * Use SQL to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        abstract void checkSQL( String testLabel) throws SQLException;

        protected void checkSQLException( SQLException sqle,
                                          boolean expected,
                                          String expectedSQLState,
                                          String testLabel,
                                          String[] fixedSegs,
                                          String[][] variables,
                                          boolean[] ignoreCase)
        {
            if( ! expected)
                unexpectedException( testLabel, sqle);
            else if( ! sqle.getSQLState().startsWith( expectedSQLState))
                unexpectedException( testLabel, sqle);
            else
            {
                if( msgTxtOK( sqle.getMessage(), 0, 0, fixedSegs, variables, ignoreCase))
                    return;
                StringBuffer expectedMsg = new StringBuffer();
                for( int segIdx = 0; segIdx < fixedSegs.length; segIdx++)
                {
                    expectedMsg.append( fixedSegs[segIdx]);
                    if( segIdx < variables.length)
                    {
                        if( variables[ segIdx].length == 1)
                            expectedMsg.append( variables[ segIdx][0]);
                        else
                            expectedMsg.append( "{?}");
                    }
                }
                reportFailure( "Incorrect error message. Expected \"" + expectedMsg.toString() +
                               "\" got \"" + sqle.getMessage()  + "\"");
            }
        } // end of checkSQLException

        /* See if actualMsg.substring( offset) looks like
         *  fixedSegs[segIdx] + variables[segIdx] + fixedSegs[segIdx + 1] ...
         */
        private boolean msgTxtOK( String actualMsg,
                                  int offset,
                                  int segIdx,
                                  String[] fixedSegs,
                                  String[][] variables,
                                  boolean[] ignoreCase)
        {
            for( ; segIdx < fixedSegs.length; segIdx++)
            {
                if( ! actualMsg.startsWith( fixedSegs[ segIdx], offset))
                    return false;
                offset += fixedSegs[ segIdx].length();
                if( segIdx < variables.length)
                {
                    if( variables[ segIdx].length == 1)
                    {
                        if( ! actualMsg.regionMatches( ignoreCase[ segIdx],
                                                       offset,
                                                       variables[ segIdx][0],
                                                       0,
                                                       variables[ segIdx][0].length()))
                            return false;
                        offset += variables[ segIdx][0].length();
                    }
                    else
                    {
                        // There is a choice. See if any of them works.
                        int i;
                        for( i = 0; i < variables[ segIdx].length; i++)
                        {
                            if( actualMsg.regionMatches( ignoreCase[ segIdx],
                                                         offset,
                                                         variables[ segIdx][i],
                                                         0,
                                                         variables[ segIdx][i].length())
                                && msgTxtOK( actualMsg,
                                             offset + variables[ segIdx][i].length(),
                                             segIdx + 1,
                                             fixedSegs,
                                             variables,
                                             ignoreCase))
                            {
                                offset += variables[ segIdx][i].length();
                                break;
                            }
                        }
                        if( i >= variables[ segIdx].length)
                            return false;
                    }
                }
            }
            return true;
        } // end of msgTxtOK
                                          
    } // end of class PrivCheck

    private static final String[] columnPrivErrMsgFixedSegs
    = { "User '", "' does not have ", " permission on column '", "' of table '", "'.'", "'."};

    private static final String[] tablePrivErrMsgFixedSegs
    = { "User '", "' does not have ", " permission on table '", "'.'", "'."};

    private static final String[] executePrivErrMsgFixedSegs
    = { "User '", "' does not have execute permission on ", " '", "'.'", "'."};

    private abstract class TablePrivCheck extends PrivCheck
    {
        String table;
        String[] columns;
        private String[] allColumns;
        
        TablePrivCheck( boolean expectPriv,
                        boolean privIsPublic,
                        User user,
                        String schema,
                        String table,
                        String[] columns)
        {
            super( expectPriv, privIsPublic, user, schema);
            this.table = table;
            this.columns = columns;
        }

        /**
         * Use SQL to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         * @param tablePermsColName the name of the column to check in SYS.SYSTABLEPERMS
         * @param colPermsType the value to look for in the SYS.SYSCOLPERMS.TYPE column
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkSQL( String testLabel,
                       String tablePermsColName,
                       String colPermsType)
            throws SQLException
        {
            if( columns == null)
            {
                ResultSet rs = owner.stmt.executeQuery(
                    "select p." + tablePermsColName + " from SYS.SYSTABLEPERMS p, SYS.SYSTABLES t, SYS.SYSSCHEMAS s"
                    + " where p.GRANTEE = '" + user.name + "' and p.TABLEID = t.TABLEID and "
                    + " t.TABLENAME = '" + table + "' and t.SCHEMAID = s.SCHEMAID and "
                    + " s.SCHEMANAME = '" + schema + "'");
                if( rs.next())
                {
                    String hasPerm = rs.getString(1);
                    if( "N".equals( hasPerm))
                    {
                        if( expectPriv)
                            reportFailure( getPrivName() + " permission not in SYSTABLEPERMS for " + user
                                           + " on table " + schema + "." + table);
                    }
                    else if( "y".equals( hasPerm))
                    {
                        if( ! expectPriv)
                            reportFailure( getPrivName() + " permission was in SYSTABLEPERMS for " + user
                                           + " on table " + schema + "." + table);
                    }
                    else if( "Y".equals( hasPerm))
                    {
                        reportFailure( getPrivName() + " WITH GRANT OPTION in SYSTABLEPERMS for " + user
                                       + " on table " + schema + "." + table);
                    }
                    if( rs.next())
                        reportFailure( "Multiple SYS.SYSTABLEPERMS rows for user " + user
                                       + " on table " + schema + "." + table);
                }
                else
                {
                    if( expectPriv)
                        reportFailure( "No SYSTABLEPERMS rows for " + user + " on table " + schema + "." + table);
                }
                rs.close();
            }
            else
            {
                // Column permissions
                ResultSet rs = owner.stmt.executeQuery(
                    "select p.type,p.columns from SYS.SYSCOLPERMS p, SYS.SYSTABLES t, SYS.SYSSCHEMAS s"
                    + " where p.GRANTEE = '" + user.name + "' and (p.type = '" + colPermsType.toLowerCase()
                    + "' or p.type = '" + colPermsType.toUpperCase() + "') and p.TABLEID = t.TABLEID and "
                    + " t.TABLENAME = '" + table + "' and t.SCHEMAID = s.SCHEMAID and "
                    + " s.SCHEMANAME = '" + schema + "'");
                if( rs.next())
                {
                    String type = rs.getString(1);
                    FormatableBitSet colBitSet = (FormatableBitSet) rs.getObject(2);
                    if( type == null || colBitSet == null)
                        reportFailure( "Null type or columns value in SYSCOLPERMS row for "
                                       + user + " on table " + schema + "." + table);
                    else
                    {
                        FormatableBitSet expectedColBitSet = getColBitSet( );
                        colBitSet.and( expectedColBitSet);
                        if( expectPriv)
                        {
                            if( ! colBitSet.equals( expectedColBitSet))
                                reportFailure( "Expected " + getPrivName() + " permissions not all in SYSCOLPERMS for "
                                               + user + " on table " + schema + "." + table);
                        }
                        else
                        {
                            if( colBitSet.anySetBit() >= 0)
                                reportFailure( "Unexpected " + getPrivName() + " permissions in SYSCOLPERMS for "
                                               + user + " on table " + schema + "." + table);
                        }
                    }
                    if( rs.next())
                        reportFailure( "Multiple " + getPrivName() + " rows in SYSCOLPERMS for "
                                       + user + " on table " + schema + "." + table);
                }
                else
                {
                    if( expectPriv)
                        reportFailure( "No " + getPrivName() + " permissions in SYSCOLPERMS for "
                                       + user + " on table " + schema + "." + table);
                }
                rs.close();
            }
        } // end of checkSQL

	String getUserCurrentSchema(User user) throws SQLException
	{
            String schemaString = null;

            Statement s = user.getConnection().createStatement();
            ResultSet rs = s.executeQuery("values current schema");
            while (rs.next())
		schemaString = rs.getString(1);
            return schemaString;
	}

	void setUserCurrentSchema(User user, String schema) throws SQLException
	{
            Statement s = user.getConnection().createStatement();
            try {
            	s.executeUpdate("set schema "+schema);
	    } catch (SQLException sqle) {
                // If schema not present, create it and try again
                if (sqle.getSQLState() == "42Y07") {
                     s.executeUpdate("create schema "+schema);
            	     s.executeUpdate("set schema "+schema);
		}
            }
	}

        private HashMap columnHash;
        
        FormatableBitSet getColBitSet( ) throws SQLException
        {
            if( columns == null)
                return null;
            
            if( columnHash == null)
            {
                columnHash = new HashMap();
                ResultSet rs = dbmd.getColumns( (String) null, schema, table, (String) null);
                while( rs.next())
                {
                    columnHash.put( rs.getString( "COLUMN_NAME"),
                                    new Integer( rs.getInt( "ORDINAL_POSITION") - 1));
                }
                rs.close();
            }
            FormatableBitSet colBitSet = new FormatableBitSet( columnHash.size());
            for( int i = 0; i < columns.length; i++)
            {
                Integer colIdx = (Integer) columnHash.get( columns[i].toUpperCase());
                if( colIdx == null)
                    throw new SQLException("Internal test error: table " + schema + "." + table
                                           + " does not have a " + columns[i].toUpperCase() + " column.");
                colBitSet.set( colIdx.intValue());
            }
            return colBitSet;
        } // end of getColBitSet

        /**
         * Use the database metadata to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkMetaData( String testLabel) throws SQLException
        {
            if( columns == null)
            {
                ResultSet rs = dbmd.getTablePrivileges( (String) null, schema, table);
                boolean found = false;
                while( rs.next())
                {
                    String go = rs.getString( 4); // grantor
                    String ge = rs.getString( 5); // grantee
                    String p = rs.getString( 6); // privilege
                    String ig = rs.getString( 7); // is grantable
                    if( ! dbmd.getUserName().equals( go))
                        reportFailure( "DatabaseMetaData.getTablePrivileges returned incorrect grantor");
                    if( ge == null)
                        reportFailure( "DatabaseMetaData.getTablePrivileges returned null user");
                    if( p == null)
                        reportFailure( "DatabaseMetaData.getTablePrivileges returned null privilege");
                    if( ig == null)
                        reportFailure( "DatabaseMetaData.getTablePrivileges returned null is_grantable");
                    if( ig.equals("YES"))
                        reportFailure( "grantable " + p + " privilege reported by DatabaseMetaData.getTablePrivileges");
                    else if( ! ig.equals("NO"))
                        reportFailure( "DatabaseMetaData.getTablePrivileges returned invalid is_grantable");
                    if( user.name.equals( ge) && getPrivName().equals( p))
                        found = true;
                }
                rs.close();
                if( expectPriv && !found)
                    reportFailure( "DatabaseMetaData.getTablePrivileges did not return expected " + getPrivName()
                                   + " permision");
                else if( found && !expectPriv)
                    reportFailure( "DatabaseMetaData.getTablePrivileges returned an unexpected " + getPrivName()
                                   + " permision");
            }
            else
            {
                FormatableBitSet expectedColBitSet = getColBitSet( );
                FormatableBitSet found = new FormatableBitSet( expectedColBitSet.getLength());
                ResultSet rs = dbmd.getColumnPrivileges( (String) null, schema, table, "%");
                while( rs.next())
                {
                    String colName = rs.getString( "COLUMN_NAME");
                    String go = rs.getString( "GRANTOR");
                    String ge = rs.getString( "GRANTEE");
                    String p = rs.getString( "PRIVILEGE");
                    String ig = rs.getString( "IS_GRANTABLE");
                    if( ! dbmd.getUserName().equals( go))
                        reportFailure( "DatabaseMetaData.getColumnPrivileges returned incorrect grantor");
                    if( ge == null)
                        reportFailure( "DatabaseMetaData.getColumnPrivileges returned null user");
                    if( p == null)
                        reportFailure( "DatabaseMetaData.getColumnPrivileges returned null privilege");
                    if( ig == null)
                        reportFailure( "DatabaseMetaData.getColumnPrivileges returned null is_grantable");
                    if( ig.equals("YES"))
                        reportFailure( "grantable " + p + " privilege reported by DatabaseMetaData.getColumnPrivileges");
                    else if( ! ig.equals("NO"))
                        reportFailure( "DatabaseMetaData.getColumnPrivileges returned invalid is_grantable");
                    Integer cI = (Integer) columnHash.get( colName);
                    if( cI == null)
                        reportFailure( "DatabaseMetaData.getColumnPrivileges returned invalid column name: "
                                       + colName);
                    else if( user.name.equals( ge) && getPrivName().equals( p))
                    {
                        int cIdx = cI.intValue();
                        if( found.isSet( cIdx) )
                            reportFailure( "DatabaseMetaData.getColumnPrivileges returned duplicate rows");
                        else
                            found.set( cIdx);
                    }
                }
                rs.close();
                if( expectPriv)
                {
                    for( int i = expectedColBitSet.anySetBit(); i >= 0; i = expectedColBitSet.anySetBit(i))
                    {
                        if( !found.isSet(i))
                        {
                            reportFailure( "DatabaseMetaData.getColumnPrivileges missed " + getPrivName()
                                           + " permission on column " + (i+1));
                            break;
                        }
                    }
                }
                else
                {
                    for( int i = expectedColBitSet.anySetBit(); i >= 0; i = expectedColBitSet.anySetBit(i))
                    {
                        if( found.isSet(i))
                        {
                            reportFailure( "DatabaseMetaData.getColumnPrivileges returned unexpected " + getPrivName()
                                           + " permission on column " + (i+1));
                            break;
                        }
                    }
                }
            }
        } // end of checkMetaData

        abstract String getPrivName();

        protected String[] getAllColumns( ) throws SQLException
        {
            if( allColumns == null)
            {
                ArrayList columnList = new ArrayList();
                ResultSet rs = dbmd.getColumns( (String) null, schema, table, (String) null);
                String separator = "";
                while( rs.next())
                {
                    columnList.add( rs.getString( 4));
                }
                allColumns = (String[]) columnList.toArray( new String[0]);
            }
            return allColumns;
        } // end of getAllColumns

        protected void appendWhereClause( StringBuffer sb, String[] columns)
            throws SQLException
        {
            if( columns == null)
                columns = getAllColumns( );
            sb.append( " where (");
            for( int i = 0; i < columns.length; i++)
            {
                if( i > 0)
                    sb.append( " or (");
                sb.append( columns[i]);
                sb.append( " is null)");
            }
        } // end of appendWhereClause

        /* Check that the error message looks right. It should be
         * User '{user}' does not have {action} permission on table '{schema}'.'{table}'.
         */
        protected void checkTablePermissionMsg( SQLException sqle,
                                                User user,
                                                String action,
                                                String testLabel)
        {
            checkSQLException( sqle, ! expectPriv, "28506", testLabel,
                               tablePrivErrMsgFixedSegs,
                               new String[][]{ new String[] { user.name},
                                               new String[] { action},
                                               new String[] { schema},
                                               new String[] { table}},
                               new boolean[]{true, true, false, false});
        } // end of checkTablePermissionMsg

        protected void checkColumnPermissionMsg( SQLException sqle,
                                                 User user,
                                                 String action,
                                                 String testLabel)
            throws SQLException
        {
            checkSQLException( sqle, ! expectPriv, "28508", testLabel,
                               columnPrivErrMsgFixedSegs,
                               new String[][]{ new String[] { user.name},
                                               new String[] { action},
                                               (columns == null) ? getAllColumns() : columns,
                                               new String[] { schema},
                                               new String[] { table}},
                               new boolean[]{true, true, false, false, false});
        } // end of checkColumnPermissionMsg
    } // end of class TablePrivCheck

    static void appendAColumnValue( StringBuffer sb, int type)
    {
        switch( type)
        {
        case Types.BIGINT:
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.INTEGER:
        case Types.NUMERIC:
        case Types.REAL:
        case Types.SMALLINT:
        case Types.TINYINT:
            sb.append( "0");
            break;

        case Types.CHAR:
        case Types.VARCHAR:
            sb.append( "' '");
            break;

        case Types.DATE:
            sb.append( "CURRENT_DATE");
            break;

        case Types.TIME:
            sb.append( "CURRENT_TIME");
            break;

        case Types.TIMESTAMP:
            sb.append( "CURRENT_TIMESTAMP");
            break;

        default:
            sb.append( "null");
            break;
        }
    } // end of appendAColumnValue

    private class SelectPrivCheck extends TablePrivCheck
    {
        SelectPrivCheck( boolean expectPriv,
                         boolean privIsPublic,
                         User user,
                         String schema,
                         String table,
                         String[] columns)
        {
            super( expectPriv, privIsPublic, user, schema, table, columns);
        }

        String getPrivName() { return "SELECT";}
        
        /**
         * Use SQL to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkSQL( String testLabel) throws SQLException
        {
            checkSQL( testLabel, "SELECTPRIV", "s");
        }

        /**
         * Run the appropriate SQL statement to see if Derby really grants the privilege or not
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkUser( User user, String testLabel) throws SQLException
        {
            StringBuffer sb = new StringBuffer();
            sb.append( "select ");
            if( columns == null)
                sb.append( "*");
            else
            {
                for( int i = 0; i < columns.length; i++)
                {
                    if( i != 0)
                        sb.append( ",");
                    sb.append( columns[i]);
                }
            }
            sb.append( " from ");
            if( schema != null)
            {
                sb.append( schema);
                sb.append( ".");
            }
            sb.append( table);

            checkUser( user, sb, testLabel);

            // Test using the columns in a where clause.
            sb.setLength( 0);
            sb.append( "select count(*) from \"");
            sb.append( schema);
            sb.append( "\".\"");
            sb.append( table);
            sb.append( "\"");
            appendWhereClause( sb, columns);
            checkUser( user, sb, testLabel);
        } // end of checkUser

        private void checkUser( User user, StringBuffer sb, String testLabel) throws SQLException
        {
			System.out.println("SelectPrivCheck: " + sb.toString());
            PreparedStatement ps = user.getConnection().prepareStatement( sb.toString());
            try
            {
                ResultSet rs = ps.executeQuery();
                rs.next();
                rs.close();
                if( ! (privIsPublic || expectPriv))
                    reportFailure( "A select was performed without permission. (" + testLabel + ")");
            }
            catch( SQLException sqle)
            {
                checkColumnPermissionMsg( sqle, user, "select", testLabel);
            }
            ps.close();
        }
    } // end of class SelectPrivCheck
        
    private class DeletePrivCheck extends TablePrivCheck
    {
        DeletePrivCheck( boolean expectPriv,
                         boolean privIsPublic,
                         User user,
                         String schema,
                         String table)
        {
            super( expectPriv, privIsPublic, user, schema, table, (String[]) null);
        }

        String getPrivName() { return "DELETE";}
        
        /**
         * Use SQL to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkSQL( String testLabel) throws SQLException
        {
            checkSQL( testLabel, "DELETEPRIV", "d");
        }

        /**
         * Run the appropriate SQL statement to see if Derby really grants the privilege or not
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkUser( User user, String testLabel) throws SQLException
        {
            StringBuffer sb = new StringBuffer();
            sb.append( "delete from \"");
            sb.append( schema);
            sb.append( "\".\"");
            sb.append( table);
            sb.append( "\"");
            boolean savedAutoCommit = user.getConnection().getAutoCommit();
            user.getConnection().setAutoCommit( false);
            System.out.println("DeletePrivCheck: " + sb.toString());
            PreparedStatement ps = user.getConnection().prepareStatement( sb.toString());
            try
            {
                ps.executeUpdate();
                if( ! (privIsPublic || expectPriv))
                    reportFailure( "A delete was performed without permission. (" + testLabel + ")");
            }
            catch( SQLException sqle)
            {
                checkTablePermissionMsg( sqle, user, "delete", testLabel);
            }
            finally
            {
                try
                {
                    user.getConnection().rollback();
                }
                finally
                {
                    user.getConnection().setAutoCommit( savedAutoCommit);
                }
            }
        } // end of checkUser                   
                
    } // end of class DeletePrivCheck

    private class InsertPrivCheck extends TablePrivCheck
    {
        InsertPrivCheck( boolean expectPriv,
                         boolean privIsPublic,
                         User user,
                         String schema,
                         String table)
        {
            super( expectPriv, privIsPublic, user, schema, table, (String[]) null);
        }

        String getPrivName() { return "INSERT";}
        
        /**
         * Use SQL to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkSQL( String testLabel) throws SQLException
        {
            checkSQL( testLabel, "INSERTPRIV", "i");
        }

        /**
         * Run the appropriate SQL statement to see if Derby really grants the privilege or not
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkUser( User user, String testLabel) throws SQLException
        {
            StringBuffer sb = new StringBuffer();
            sb.append( "insert into \"");
            sb.append( schema);
            sb.append( "\".\"");
            sb.append( table);
            sb.append( "\" values(");
            ResultSet rs = dbmd.getColumns( (String) null, schema, table, (String) null);
            boolean first = true;
            while( rs.next())
            {
                if( first)
                    first = false;
                else
                    sb.append( ",");
                appendAColumnValue( sb, rs.getInt( 5));
            }
            sb.append(")");
            boolean savedAutoCommit = user.getConnection().getAutoCommit();
            user.getConnection().setAutoCommit( false);
            System.out.println("InsertPrivCheck: " + sb.toString());
            PreparedStatement ps = user.getConnection().prepareStatement( sb.toString());
            try
            {
                ps.executeUpdate();
                if( ! (privIsPublic || expectPriv))
                    reportFailure( "An insert was performed without permission. (" + testLabel + ")");
            }
            catch( SQLException sqle)
            {
                checkTablePermissionMsg( sqle, user, "insert", testLabel);
            }
            finally
            {
                try
                {
                    user.getConnection().rollback();
                }
                finally
                {
                    user.getConnection().setAutoCommit( savedAutoCommit);
                }
            }
        } // end of checkUser                   
            
    } // end of class InsertPrivCheck

    private class UpdatePrivCheck extends TablePrivCheck
    {
        UpdatePrivCheck( boolean expectPriv,
                         boolean privIsPublic,
                         User user,
                         String schema,
                         String table,
                         String[] columns)
        {
            super( expectPriv, privIsPublic, user, schema, table, columns);
        }

        String getPrivName() { return "UPDATE";}
        
        /**
         * Use SQL to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkSQL( String testLabel) throws SQLException
        {
            checkSQL( testLabel, "UPDATEPRIV", "u");
        }

        /**
         * Run the appropriate SQL statement to see if Derby really grants the privilege or not
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkUser( User user, String testLabel) throws SQLException
        {
            String[] checkColumns = (columns == null) ? getAllColumns() : columns;
            StringBuffer sb = new StringBuffer();
            boolean savedAutoCommit = user.getConnection().getAutoCommit();
            user.getConnection().setAutoCommit( false);
            try
            {
                for( int colIdx = 0; colIdx < checkColumns.length; colIdx++)
                {
                    sb.setLength( 0);
                    sb.append( "update ");
                    sb.append( schema);
                    sb.append( ".");
                    sb.append( table);
                    sb.append( " set ");
                    sb.append( checkColumns[ colIdx]);
                    sb.append( "=");
                    ResultSet rs = dbmd.getColumns( null, schema, table, checkColumns[ colIdx]);
                    if( ! rs.next())
                    {
                        rs.close();
                        reportFailure( "Could not get column metadata for " + schema + "." + table +
                                       "." + checkColumns[ colIdx]);
                        continue;
                    }
                    appendAColumnValue( sb, rs.getInt(5));
                    rs.close();
					System.out.println("UpdatePrivCheck: " + sb.toString());
                    PreparedStatement ps = user.getConnection().prepareStatement( sb.toString());
                    try
                    {
                        ps.executeUpdate();
                        if( ! (privIsPublic || expectPriv))
                            reportFailure( "An update of " + schema + "." + table + "." +
                                           checkColumns[ colIdx] + " was performed without permission. ("
                                           + testLabel + ")");
                    }
                    catch( SQLException sqle)
                    {
                        checkColumnPermissionMsg( sqle, user, "update", testLabel);
                    }
                }
            }
            finally
            {
                try
                {
                    user.getConnection().rollback();
                }
                finally
                {
                    user.getConnection().setAutoCommit( savedAutoCommit);
                }
            }
        } // end of checkUser                   
            
    } // end of class UpdatePrivCheck

    private class ReferencesPrivCheck extends TablePrivCheck
    {
        HashMap colNameHash;
        
        ReferencesPrivCheck( boolean expectPriv,
                             boolean privIsPublic,
                             User user,
                             String schema,
                             String table,
                             String[] columns)
        {
            super( expectPriv, privIsPublic, user, schema, table, columns);
            if( columns != null)
            {
                colNameHash = new HashMap( (5*columns.length)/4);
                for( int i = 0; i < columns.length; i++)
                    colNameHash.put( columns[i], columns[i]);
            }
        }

        String getPrivName() { return "REFERENCES";}
        
        /**
         * Use SQL to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkSQL( String testLabel) throws SQLException
        {
            checkSQL( testLabel, "REFERENCESPRIV", "r");
        }

        /**
         * Run the appropriate SQL statement to see if Derby really grants the privilege or not
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkUser( User user, String testLabel) throws SQLException
        {
            // RESOLVE
        } // end of checkUser                   
    } // end of class ReferencesPrivCheck

    private class TriggerPrivCheck extends TablePrivCheck
    {
        TriggerPrivCheck( boolean expectPriv,
                          boolean privIsPublic,
                          User user,
                          String schema,
                          String table)
        {
            super( expectPriv, privIsPublic, user, schema, table, (String[]) null);
        }

        String getPrivName() { return "TRIGGER";}
        
        /**
         * Use SQL to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkSQL( String testLabel) throws SQLException
        {
            checkSQL( testLabel, "TRIGGERPRIV", "t");
        }

        /**
         * Run the appropriate SQL statement to see if Derby really grants the privilege or not
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkUser(User user, String testLabel) throws SQLException
        {
            StringBuffer sb = new StringBuffer();
            sb.append("create trigger ");
            sb.append("\"");
            sb.append(table+"Trig");
            sb.append("\"");
            sb.append(" after insert on ");

            sb.append("\"");
            sb.append(schema);
            sb.append("\".\"");
            sb.append(table);
            sb.append("\"");
            sb.append(" for each row mode db2sql values 1");

            boolean savedAutoCommit = user.getConnection().getAutoCommit();
            String currentSchema = getUserCurrentSchema(user);			
            // DDLs can only be issued in their own schema
            setUserCurrentSchema(user, user.toString());
            user.getConnection().setAutoCommit(false);
            System.out.println("TriggerPrivCheck: " + sb.toString());
            PreparedStatement ps = user.getConnection().prepareStatement(sb.toString());
            try
            {
                ps.executeUpdate();
                if( ! (privIsPublic || expectPriv))
                    reportFailure( "An execute was performed without permission. (" + testLabel + ")");
            }
            catch( SQLException sqle)
            {
                checkTablePermissionMsg( sqle, user, "trigger", testLabel);
            }
            finally
            {
                try
                {
                    user.getConnection().rollback();
                }
                finally
                {
                    user.getConnection().setAutoCommit( savedAutoCommit);
                    setUserCurrentSchema(user, currentSchema);
                }
            }
        } // end of checkUser                   
    } // end of class TriggerPrivCheck

    private class ExecutePrivCheck extends PrivCheck
    {
        String routine;
        boolean isFunction;
        
        ExecutePrivCheck( boolean expectPriv,
                          boolean privIsPublic,
                          User user,
                          String schema,
                          String routine,
                          boolean isFunction)
        {
            super( expectPriv, privIsPublic, user, schema);
            this.routine = routine;
            this.isFunction = isFunction;
        }
        
        /**
         * Use SQL to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkSQL( String testLabel) throws SQLException
        {
            ResultSet rs = owner.stmt.executeQuery(
                "select p.GRANTOPTION from SYS.SYSROUTINEPERMS p, SYS.SYSALIASES a, SYS.SYSSCHEMAS s"
                + " where p.GRANTEE = '" + user.name + "' and p.ALIASID = a.ALIASID and"
                + "  a.ALIAS = '" + routine + "' and a.ALIASTYPE = '"
                + (isFunction ? "F" : "P") + "' and a.SCHEMAID = s.SCHEMAID and"
                + "  s.SCHEMANAME = '" + schema + "'");
            if( rs.next())
            {
                if( ! expectPriv)
                    reportFailure( "Execute permission in SYSROUTINEPERMS for " + user +
                                   " on " + (isFunction ? "function" : "procedure") + " "
                                   + schema + "." + routine);
                else
                {
                    if( ! "N".equals( rs.getString(1)))
                        reportFailure( "WITH GRANT OPTION specified in SYSROUTINEPERMS for " + user +
                                       " on " + (isFunction ? "function" : "procedure") + " "
                                       + schema + "." + routine);
                }
                if( rs.next())
                    reportFailure( "Multiple rows in SYSROUTINEPERMS for " + user +
                                   " on " + (isFunction ? "function" : "procedure") + " "
                                   + schema + "." + routine);
            }
            else
            {
                if( expectPriv)
                    reportFailure( "No execute permission in SYSROUTINEPERMS for " + user +
                                   " on " + (isFunction ? "function" : "procedure") + " "
                                   + schema + "." + routine);
            }
            rs.close();
        } // end of checkSQL

        /**
         * Use the database metadata to check that the privilege is (not) in the the system permission catalogs.
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkMetaData( String testLabel) throws SQLException
        {
            ; // There is no database metadata method for finding function/procedure privileges
        } // end of checkMetaData

        /**
         * Run the appropriate SQL statement to see if Derby really grants the privilege or not
         *
         * @param testLabel A label to use in diagnostic messages.
         *
         * @exception SQLException Indicates a problem with the test program. Should not happen.
         */
        void checkUser( User user, String testLabel) throws SQLException
        {
            StringBuffer sb = new StringBuffer();
            if (isFunction)
            	sb.append( "values \"");
            else
            	sb.append( "call \"");
            sb.append(schema);
            sb.append("\".\"");
            sb.append(routine);
            sb.append("\"");
            sb.append("()");

            boolean savedAutoCommit = user.getConnection().getAutoCommit();
            user.getConnection().setAutoCommit(false);
            System.out.println("ExecutePrivCheck: " + sb.toString());
            PreparedStatement ps = user.getConnection().prepareStatement(sb.toString());
            try
            {
		if (isFunction)
		{
                    ResultSet rs = ps.executeQuery();
                    rs.close();
		}
                else
                    ps.executeUpdate();
                if( ! (privIsPublic || expectPriv))
                    reportFailure( "An execute was performed without permission. (" + testLabel + ")");
            }
            catch( SQLException sqle)
            {
                checkExecutePermissionMsg( sqle, user, testLabel);
            }
            finally
            {
                try
                {
                    user.getConnection().rollback();
                }
                finally
                {
                    user.getConnection().setAutoCommit( savedAutoCommit);
                }
            }
        } // end of checkUser                   

        /* Check that the error message looks right. It should be
         * User '{user}' does not have execute permission on FUNCTION/PROCEDURE '{schema}'.'{table}'.
         */
        protected void checkExecutePermissionMsg( SQLException sqle,
                                                User user,
                                                String testLabel)
        {
            checkSQLException( sqle, ! expectPriv, "2850A", testLabel,
                               executePrivErrMsgFixedSegs,
                               new String[][]{ new String[] { user.name},
                                               new String[] { (isFunction)?"FUNCTION":"PROCEDURE"},
                                               new String[] { schema},
                                               new String[] { routine}},
                               new boolean[]{true, true, false, false});
        } // end of checkExecutePermissionMsg
    } // end of class ExecutePrivCheck
}

class User
{
    public final String name;
    public final String password;
    private final boolean isPublic;
    private Connection conn;
    public Statement stmt;

    User( String name, String password)
    {
        this.name = name;
        this.password = password;
        isPublic = "public".equalsIgnoreCase( name);
		System.out.println("name = "+name+" password = "+password);
    }

    boolean isPublic()
    {
        return isPublic;
    }
    
    void setConnection( Connection conn) throws SQLException
    {
        this.conn = conn;
        stmt = conn.createStatement();
    }

    Connection getConnection() throws SQLException
    {
        if( conn == null)
        {
            if( ! isPublic)
            {
		String connAttrs = "user=" + name + ";password=" + password;
//                conn = DriverManager.getConnection( "jdbc:derby:wombat", name, password);
		conn = TestUtil.getConnection("wombat", connAttrs);
                stmt = conn.createStatement();
            }
        }
        return conn;
    }

    public String toString()
    {
        return name;
    }
} // end of class User
