/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ConglomerateSharingTest

       Licensed to the Apache Software Foundation (ASF) under one
       or more contributor license agreements.  See the NOTICE file
       distributed with this work for additional information
       regarding copyright ownership.  The ASF licenses this file
       to you under the Apache License, Version 2.0 (the
       "License"); you may not use this file except in compliance
       with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License
*/

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test for situations in which Derby attempts to "share" physical
 * conglomerates across multiple constraints.
 */
public final class ConglomerateSharingTest extends BaseJDBCTestCase {

    private final String COUNT_TABLE_CONGLOMS =
        "select count (distinct conglomeratenumber) from " +
        "sys.sysconglomerates where tableid = " +
        " (select tableid from sys.systables where tablename = ?)";

    private final String GET_CONSTRAINT_NAMES =
        "select constraintname from sys.sysconstraints " +
        "where tableid = (select tableid from sys.systables " +
        "where tablename = ?)";

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public ConglomerateSharingTest(String name)
    {
        super(name);
    }

    /**
     * Create a suite of tests.
     */
    public static Test suite()
    {
        return new CleanDatabaseTestSetup(
            TestConfiguration.embeddedSuite(ConglomerateSharingTest.class));
    }

    /**
     * If we have a unique constraint and a non-unique constraint
     * which a) reference the same columns and b) share a single
     * (unique) conglomerate, then test that dropping the unique
     * constraint will convert the physical conglomerate to be
     * non-unique.  This test case is pulled from the repro
     * attached to DERBY-3299.
     */
    public void testConversionToNonUnique() throws SQLException
    {
        PreparedStatement countCongloms =
            prepareStatement(COUNT_TABLE_CONGLOMS);

        Statement st = createStatement();

        st.execute("create table orders (no_w_id int not null, " +
            "no_d_id int not null, no_o_id int not null, info varchar(20), " +
            "constraint orders_pk primary key (no_w_id, no_d_id, no_o_id))");

        st.execute("insert into orders values (1, 2, 3, 'info # one')");
        st.execute("insert into orders values (1, 2, 4, 'info # two')");
        st.execute("insert into orders values (1, 2, 5, 'info # 3')");

        st.execute("create table neworders (no_w_id int not null, " +
            "no_d_id int not null, no_o_id int not null, lname varchar(50))");

        st.execute("alter table neworders add constraint " +
            "neworders_pk primary key (no_w_id, no_d_id, no_o_id)");

        st.execute("alter table neworders add constraint " +
              "no_o_fk foreign key (no_w_id, no_d_id, no_o_id) " +
            "references orders");

        st.execute("insert into neworders values (1, 2, 3, 'Inigo')");
        st.execute("insert into neworders values (1, 2, 4, 'Montoya')");
        st.execute("insert into neworders values (1, 2, 5, 'Tortuga')");

        /* Should have 2 conglomerates on NEWORDERS:
         *
         *  1. Heap
         *  2. NEWORDERS_PK (shared by: NO_O_FK)
         */
        countConglomerates("NEWORDERS", countCongloms, 2);

        // This should fail due to foreign key.
        checkStatementError("23503", st,
            "insert into neworders values (1, 3, 5, 'SHOULD FAIL')",
            "NO_O_FK");

        // This should fail due to primary key (uniqueness violation).
        checkStatementError("23505", st,
            "insert into neworders values (1, 2, 4, 'SHOULD FAIL')",
            "NEWORDERS_PK");

        /* Now drop the primary key from NEWORDERS.  This should
         * drop the implicit uniqueness requirement, as well--i.e.
         * the physical conglomerate should become non-unique.
         */
        st.execute("alter table neworders drop constraint neworders_pk");

        /* Should still have 2 conglomerates because we dropped the
         * unique conglomerate from NEWORDER_PK but created another,
         * non-unique one for NO_O_FK.
         *
         *  1. Heap
         *  2. NO_O_FK
         */
        countConglomerates("NEWORDERS", countCongloms, 2);

        // This should still fail due to the foreign key.
        checkStatementError("23503", st,
            "insert into neworders values (1, 3, 5, 'SHOULD FAIL')",
            "NO_O_FK");

        /* This should now succeed because we dropped the backing
         * unique index and foreign key constraints are not inherently
         * unique. DERBY-3299.
         */
        st.execute("insert into neworders values (1, 2, 4, 'SHOULD SUCCEED')");

        // Sanity check the table contents.
        JDBC.assertUnorderedResultSet(
            st.executeQuery("select * from neworders"),
            new String [][] {
                {"1", "2", "3", "Inigo"},
                {"1", "2", "4", "Montoya"},
                {"1", "2", "5", "Tortuga"},
                {"1", "2", "4", "SHOULD SUCCEED"}
            });

        // Check again using the foreign key's backing index.
        JDBC.assertUnorderedResultSet(st.executeQuery(
            "select * from neworders --DERBY-PROPERTIES constraint=NO_O_FK"),
            new String [][] {
                {"1", "2", "3", "Inigo"},
                {"1", "2", "4", "Montoya"},
                {"1", "2", "5", "Tortuga"},
                {"1", "2", "4", "SHOULD SUCCEED"}
            });

        st.execute("drop table neworders");
        st.execute("drop table orders");
        countConglomerates("NEWORDERS", countCongloms, 0);

        countCongloms.close();
        st.close();
    }

    /**
     * Test various conditions in which a constraint can be dropped,
     * and verify that if the constraint's backing conglomerate is
     * shared, we do the right thing.
     */
    public void testConstraintDrops() throws SQLException
    {
        PreparedStatement countCongloms =
            prepareStatement(COUNT_TABLE_CONGLOMS);

        PreparedStatement getConstraintNames =
            prepareStatement(GET_CONSTRAINT_NAMES);

        Statement st = createStatement();

        st.execute("create table dropc_t0 (i int not null, j int not null)");
        st.execute("alter table dropc_t0 " +
            "add constraint dropc_pk0 primary key (i,j)");

        /* Should have 2 conglomerates on DROPC_T0:
         *
         *  1. Heap
         *  2. DROPC_PK0
         */
        countConglomerates("DROPC_T0", countCongloms, 2);

        st.execute("create table dropc_t1 (i int, j int not null)");
        st.execute("alter table dropc_t1 " +
            "add constraint dropc_pk1 primary key (j)");

        /* Should have 2 conglomerates on DROPC_T1:
         *
         *  1. Heap
         *  2. DROPC_PK1
         */
        countConglomerates("DROPC_T1", countCongloms, 2);

        st.execute("create table dropc_t2 " +
            "(a int, b int not null, c int not null)");
        st.execute("create index dropc_ix1 on dropc_t2 (a,b)");
        st.execute("create unique index dropc_uix2 on dropc_t2 (c)");

        st.execute("alter table dropc_t2 " +
            "add constraint dropc_uc1 unique (c)");
        st.execute("alter table dropc_t2 add constraint " +
            "dropc_fk0 foreign key (a,b) references dropc_t0");
        st.execute("alter table dropc_t2 add constraint " +
            "dropc_fk1 foreign key (a,b) references dropc_t0");
        st.execute("alter table dropc_t2 add constraint " +
            "dropc_fk2 foreign key (c) references dropc_t1");

        /* Should have 3 conglomerates on DROPC_T2:
         *
         *  1. Heap
         *  2. DROPC_IX1 (shared by: DROPC_FK0, DROPC_FK1)
         *  3. DROPC_UIX2 (shared by: DROPC_UC1, DROPC_FK2)
         */
        countConglomerates("DROPC_T2", countCongloms, 3);

        st.execute("insert into dropc_t0 values (1, 2)");
        st.execute("insert into dropc_t1 values (3, 4)");
        st.execute("insert into dropc_t2 values (1, 2, 4)");

        /* DROP 1: First and obvious way to drop a constraint is
         * with an ALTER TABLE DROP CONSTRAINT command.
         */

        /* Drop constraint DROPC_FK0.  Since both DROPC_IX1 and
         * DROPC_FK1 require a physical conglomerate identical
         * to that of DROPC_FK0 (esp. non-unique on the same
         * columns), dropping the latter constraint should have
         * no effect on the physical conglomerate.
         */

        st.execute("alter table DROPC_T2 drop constraint DROPC_FK0");

        /* Should still have 3 conglomerates on DROPC_T2:
         *
         *  1. Heap
         *  2. DROPC_IX1 (shared by: DROPC_FK1)
         *  3. DROPC_UIX2 (shared by: DROPC_UC1, DROPC_FK2)
         */
        countConglomerates("DROPC_T2", countCongloms, 3);

        /* Check that all non-dropped constraint stills exist and
         * can be used for queries.
         */
        verifyConstraints(
            st, getConstraintNames, "DROPC_T2", "DROPC_FK0",
            new String [][] {{"DROPC_FK1"},{"DROPC_FK2"},{"DROPC_UC1"}},
            1);

        // Make sure non-dropped constraints are still enforced.

        // This statement attempts to insert a duplicate in the C column.
        // This violates both the unique index DROPC_UIX2 and the unique
        // constraint DROPC_UC1. Additionally, the backing index of the
        // foreign key DROPC_FK2 is a unique index. It is not deterministic
        // which index will be checked first, so accept any of the three.
        checkStatementError("23505", st,
            "insert into dropc_t2 values (1, 2, 4)",
            "DROPC_UIX2", "DROPC_UC1", "DROPC_FK2");

        // This statement violates the foreign key DROPC_FK1. It also
        // violates the same unique constraints/indexes as the previous
        // statement (duplicate value in column C). Foreign key violations
        // are checked before unique index violations, so expect the error
        // to be reported as a violation of DROPC_FK1.
        checkStatementError("23503", st,
            "insert into dropc_t2 values (2, 2, 4)", "DROPC_FK1");

        // This statement violates the foreign key DROPC_FK2.
        checkStatementError("23503", st,
            "insert into dropc_t2 values (1, 2, 3)", "DROPC_FK2");

        /* Drop constraint DROPC_UC1.  Since DROPC_UIX2 requires
         * a physical conglomerate identical to that of DROPC_UC1
         * (esp. unique on the same columns), dropping the latter
         * constraint should have no effect on the physical
         * conglomerate.
         */

        st.execute("alter table DROPC_T2 drop constraint DROPC_UC1");

        /* Should still have 3 conglomerates on DROPC_T2:
         *
         *  1. Heap
         *  2. DROPC_IX1 (shared by: DROPC_FK1)
         *  3. DROPC_UIX2 (shared by: DROPC_FK2)
         */
        countConglomerates("DROPC_T2", countCongloms, 3);

        /* Check that all non-dropped constraints still exist and
         * can be used for queries.
         */
        verifyConstraints(
            st, getConstraintNames, "DROPC_T2", "DROPC_UC1",
            new String [][] {{"DROPC_FK1"},{"DROPC_FK2"}},
            1);

        // Make sure non-dropped constraints are still enforced.

        // This statement attempts to insert a duplicate into the unique
        // index DROPC_UIX2 and the unique backing index of the foreign
        // key constraint DROPC_FK2. It is not deterministic which of the
        // two indexes will be inserted into first, so accept both in the
        // error message.
        checkStatementError("23505", st,
            "insert into dropc_t2 values (1, 2, 4)", "DROPC_UIX2", "DROPC_FK2");

        // This statement both violates the foreign key DROPC_FK1 and
        // attempts to insert a duplicate value into the column C. Expect
        // foreign key constraint violations to be checked before unique
        // index violations.
        checkStatementError("23503", st,
            "insert into dropc_t2 values (2, 2, 4)", "DROPC_FK1");

        // This statement violates the foreign key DROPC_FK2.
        checkStatementError("23503", st,
            "insert into dropc_t2 values (1, 2, 3)", "DROPC_FK2");

        /* DROP 2: We don't drop the constraint, but we drop a user
         * index that shares a physical conglomerate with a constraint.
         * In this case we drop DROPC_UIX2.  Since DROPC_FK2 is the only
         * constraint that shares with DROPC_UIX2, and since DROPC_FK2
         * is NON-unique while DROPC_UIX2 is unique, we should drop
         * the unique physical conglomerate and create a non-unique
         * one.
         */

        st.execute("drop index dropc_uix2");

        /* Should still have 3 conglomerates on DROPC_T2:
         *
         *  1. Heap
         *  2. DROPC_IX1 (shared by: DROPC_FK1)
         *  3. DROPC_FK2
         */
        countConglomerates("DROPC_T2", countCongloms, 3);

        /* Check that all non-dropped constraints still exist and
         * can be used for queries.
         */
        verifyConstraints(
            st, getConstraintNames, "DROPC_T2", null,
            new String [][] {{"DROPC_FK1"},{"DROPC_FK2"}},
            1);

        // Make sure non-dropped constraints are still enforced.

        checkStatementError("23503", st,
            "insert into dropc_t2 values (2, 2, 4)", "DROPC_FK1");

        checkStatementError("23503", st,
            "insert into dropc_t2 values (1, 2, 3)", "DROPC_FK2");

        /* This should now succeed because there is no longer any
         * requirement for uniqueness.
         */
        st.execute("insert into dropc_t2 values (1, 2, 4)");

        JDBC.assertUnorderedResultSet(
            st.executeQuery("select * from dropc_t2"),
            new String [][] {
                {"1", "2", "4"},
                {"1", "2", "4"}
            });

        /* Recreate the unique constraint DROPC_UC1 for next test, and
         * make DROPC_FK2 share with it again.
         */

        st.execute("delete from dropc_t2");
        st.execute("insert into dropc_t2 values (1, 2, 4)");
        st.execute("alter table dropc_t2 drop constraint dropc_fk2");
        countConglomerates("DROPC_T2", countCongloms, 2);

        st.execute("alter table dropc_t2 " +
            "add constraint dropc_uc1 unique (c)");
        st.execute("alter table dropc_t2 add constraint " +
            "dropc_fk2 foreign key (c) references dropc_t1");

        /* Also create unique index that will be dropped as part of
         * the next test, as well--we want to exercise that code
         * path, even if there is no conglomerate sharing involved
         * for this particular case.
         */
        st.execute("create unique index dropc_uix3 on dropc_t2 (a, c)");

        /* So we should now have:
         *
         *  1. Heap
         *  2. DROPC_IX1 (shared by: DROPC_FK1)
         *  3. DROPC_UC1 (shared by: DROPC_FK2)
         *  4. DROPC_UIX3
         */
        countConglomerates("DROPC_T2", countCongloms, 4);

        /* DROP 3: Third way to drop a constraint is to drop a
         * column on which the constraint depends.  Here we drop
         * column C, which will cause both DROPC_UC1 and DROPC_FK2
         * to be implicitly dropped, as well. Additionally, DROPC_UIX3
         * should be dropped because it is a unique index that relies
         * on the dropped column; since it doesn't share its
         * conglomerate with anything else, that physical conglom
         * should be dropped here, as well.
         */
        st.execute("alter table dropc_t2 drop column c");

        /* Should now only have 2 conglomerates on DROPC_T2:
         *
         *  1. Heap
         *  2. DROPC_IX1 (shared by: DROPC_FK1)
         */
        countConglomerates("DROPC_T2", countCongloms, 2);

        /* Check that all non-dropped constraint still exist and
         * can be used for queries.
         */
        verifyConstraints(
            st, getConstraintNames, "DROPC_T2", "DROPC_FK2",
            new String [][] {{"DROPC_FK1"}},
            1);

        // Make sure non-dropped constraints are still enforced.

        checkStatementError("23503", st,
            "insert into dropc_t2 values (2, 2)", "DROPC_FK1");

        /* DROP 4: If privileges to a table are revoked, a constraint
         * (esp. a foreign key constraint) that references that table
         * will be dropped.  Test case for this should exist in
         * GrantRevokeDDLTest.java.
         */

        /* Make a a non-unique constraint share a conglomerate with
         * a unique constraint, in prep for the next test case.
         */

        st.execute("delete from dropc_t2");
        st.execute("alter table dropc_t2 " +
            "add constraint dropc_uc2 unique (b)");
        st.execute("alter table dropc_t2 add constraint " +
            "dropc_fk3 foreign key (b) references dropc_t1");

        /* So we should now have:
         *
         *  1. Heap
         *  2. DROPC_IX1 (shared by: DROPC_FK1)
         *  3. DROPC_UC2 (shared by: DROPC_FK3)
         */
        countConglomerates("DROPC_T2", countCongloms, 3);

        /* DROP 5: Final way to drop a constraint is to drop the
         * table on which the constraint exists.  Derby will first
         * drop all columns, then drop all constraints, and finally,
         * drop all indexes.  Make sure the drop succeeds without
         * error and that all physical conglomerates are dropped
         * as well.
         */
        st.execute("drop table dropc_t2");

        // There shouldn't be any conglomerates left...
        countConglomerates("DROPC_T2", countCongloms, 0);
        assertStatementError("42X05", st, "select * from dropc_t2");

        // Clean up.
        st.execute("drop table dropc_t1");
        st.execute("drop table dropc_t0");
        getConstraintNames.close();
        countCongloms.close();
        st.close();
    }

    /**
     * Test conglomerate sharing when a unique constraint having one or
     * more nullable columns is in play (possible as of DERBY-3330).
     * @throws SQLException
     */
    public void testUniqueConstraintWithNullsBackingIndex ()
        throws SQLException
    {
        PreparedStatement countCongloms =
            prepareStatement(COUNT_TABLE_CONGLOMS);
        
        Statement stmt = createStatement();
        stmt.execute("create table t1 (i int, j int not null, k int)");
        stmt.executeUpdate("insert into t1 values (1, -1, 1), (2, -2, 4), " +
                "(4, -4, 16), (3, -3, 9)");
        //create a non unique index
        stmt.executeUpdate("create index nuix on t1(i,j)");
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. nuix
         */
        countConglomerates("T1", countCongloms, 2);
        
        stmt.executeUpdate("insert into t1 values (null, 1, -1)");
        stmt.executeUpdate("alter table t1 add constraint uc unique(i,j)"); 
        /* Should have 3 conglomerates on T1:
         *
         *  1. Heap
         *  2. unix
         *  3. uc
         */
        countConglomerates("T1", countCongloms, 3);
        stmt.executeUpdate("insert into t1 values (null, 1, -1)");
        stmt.executeUpdate("insert into t1 values (null, 1, -1)");

        assertStatementError("23505", stmt, 
                "insert into t1 values (1, -1, 1)");
        //clean the table to try unique index
        stmt.executeUpdate("delete from t1");
        stmt.executeUpdate("drop index nuix");
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. uc
         */
        countConglomerates("T1", countCongloms, 2);
        stmt.executeUpdate("alter table t1 drop constraint uc");
        /* Should have 1 conglomerates on T1:
         *
         *  1. Heap
         */
        countConglomerates("T1", countCongloms, 1);
        stmt.executeUpdate("insert into t1 values (1, -1, 1), (2, -2, 4), " +
                "(4, -4, 16), (3, -3, 9)");
        stmt.executeUpdate("create unique index uix on t1(i,j)");
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. uix
         */
        countConglomerates("T1", countCongloms, 2);
        stmt.executeUpdate("insert into t1 values (null, 1, -1)");
        stmt.executeUpdate("alter table t1 add constraint uc unique(i,j)");
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. uix
         * Unique Constraint uc should use uix
         */
        countConglomerates("T1", countCongloms, 2);
        //make sure that unique index is effective
        assertStatementError("23505", stmt, 
                "insert into t1 values (null, 1, -1)");
        //drop unique index
        stmt.executeUpdate("drop index uix");
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. uc
         */
        countConglomerates("T1", countCongloms, 2);  
        //make sure that its a new index and not a unique index
        stmt.executeUpdate("insert into t1 values (null, 1, -1)");
        //drop constraint
        stmt.executeUpdate("alter table t1 drop constraint uc");
        //clean table
        stmt.executeUpdate("delete from t1");
        /* Should have 1 conglomerates on T1:
         *
         *  1. Heap
         */
        countConglomerates("T1", countCongloms, 1);

        stmt.executeUpdate("insert into t1 values (1, -1, 1), (2, -2, 4), " +
                "(4, -4, 16), (3, -3, 9)");
        stmt.executeUpdate("insert into t1 values (null, 1, -1)");
        stmt.executeUpdate("alter table t1 add constraint uc unique(i,j)"); 
        
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. uc
         */
        countConglomerates("T1", countCongloms, 2);  
        
        stmt.executeUpdate("create table t2 (a int not null, b int not null)");
        stmt.executeUpdate("alter table t2 add constraint pkt2 primary key(a,b)");
        
        /* Should have 2 conglomerates on T2:
         *
         *  1. Heap
         *  2. pkt2
         */
        countConglomerates("T2", countCongloms, 2);
        stmt.executeUpdate("insert into t2 values (1, -1), (2, -2), " +
                "(4, -4), (3, -3)"); 
        
        stmt.executeUpdate("alter table t1 add constraint fkt1 " +
                "foreign key (i,j) references t2");
        
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. uc
         * fkt1 should share index with uc
         */
        countConglomerates("T1", countCongloms, 2);  
        
        //ensure there is no change in backing index
        assertStatementError("23505", stmt, "insert into " +
                "t1(i,j) values (1, -1)");
        stmt.executeUpdate("alter table t1 drop constraint uc");
        
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. fkt1
         */
       countConglomerates("T1", countCongloms, 2);  
       
       //ensure that it allows duplicate keys
       stmt.executeUpdate("insert into t1(i,j) values (1, -1)");
        
       //clean tables
       stmt.executeUpdate("alter table t1 drop constraint fkt1");
       stmt.executeUpdate("alter table t2 drop constraint pkt2");
       stmt.executeUpdate("delete from t1");
       stmt.executeUpdate("delete from t2");
       
        /* Should have 1 conglomerates on T1:
         *
         *  1. Heap
         */
       countConglomerates("T1", countCongloms, 1);
        /* Should have 1 conglomerates on T2:
         *
         *  1. Heap
         */
       countConglomerates("T2", countCongloms, 1);  

       stmt.executeUpdate("insert into t1 values (1, -1, 1), (2, -2, 4), " +
               "(4, -4, 16), (3, -3, 9)");

       stmt.executeUpdate("alter table t2 add constraint " +
                                                "pkt2 primary key(a,b)");
        /* Should have 2 conglomerates on T2:
         *
         *  1. Heap
         *  2. pkt2
         */
       countConglomerates("T2", countCongloms, 2);  
       
       stmt.executeUpdate("insert into t2 values (1, -1), (2, -2)," +
                                                        "(4, -4), (3, -3)");

       stmt.executeUpdate("create unique index uix on t1(i,j)");
       
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. uix
         */
       countConglomerates("T1", countCongloms, 2);  

       stmt.executeUpdate("alter table t1 add constraint uc unique(i,j)");

        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. uix
         *  uc should share uix's index
         */
       countConglomerates("T1", countCongloms, 2);  

       //create a foreign key shouldn;t create any new index
       stmt.executeUpdate("alter table t1 add constraint fkt1 " +
               "foreign key (i,j) references t2");
       
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. uix
         *  uc and fkt1 should share uix's index
         */
       countConglomerates("T1", countCongloms, 2);  

        //Should fail due to UIX
        assertStatementError("23505", stmt, "insert into t1(i,j) values (1, -1)");

        //Drop the unique index UIX. The conglomerate for UC and FKT1 should
        //be re-created as non-unique with uniqueWithDuplicateNulls set to true.
        stmt.executeUpdate("drop index uix");
        
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. uc
         *  fkt1 should share uc's index
         */
       countConglomerates("T1", countCongloms, 2);  

       //Should work.
       stmt.executeUpdate("insert into t1(i,j) values (null, 2)");

       //Should also work since UIX is no longer around.
       stmt.executeUpdate("insert into t1(i,j) values (null, 2)");

       //Should fail due to UC
       assertStatementError("23505", stmt,"insert into t1 values (1, -1, 1)");
        
       //drop uc a new non unique should be created
       stmt.executeUpdate("alter table t1 drop constraint uc");
       
        /* Should have 2 conglomerates on T1:
         *
         *  1. Heap
         *  2. fkt1
         */
       countConglomerates("T1", countCongloms, 2);  
       
       //should work because there is no uc
       stmt.executeUpdate("insert into t1 values (1, -1, 1)");
       
       //cleanup
       stmt.executeUpdate("drop table t1");
       stmt.executeUpdate("drop table t2");
       stmt.close();
       countCongloms.close();
    }

    /**
     * Count the number of physical conglomerates that exist for
     * the received table, and assert that the number found matches
     * the expected number.
     */
    private void countConglomerates(String tableName,
        PreparedStatement countCongloms, int expected)
        throws SQLException
    {
        countCongloms.setString(1, tableName);
        JDBC.assertSingleValueResultSet(
            countCongloms.executeQuery(), String.valueOf(expected));
        return;
    }

    /**
     * Execute the received statement and assert that:
     *
     *  1. The statement fails, and
     *  1. The SQLSTATE for the failure matches the received SQL
     *     state, and
     *  2. The failure exception includes the received index/
     *     constraint name in its message.  This is intended to
     *     be used for uniqueness and foreign key violations,
     *     esp. SQLSTATE 23503 and 23505.
     *
     * @param sqlState the expected SQLState of the error
     * @param st the statement to use for execution
     * @param query the SQL text to execute
     * @param violatedConstraints the constraints or indexes that are
     *   violated by this statement; expect the error message to mention
     *   at least one of them
     */
    private void checkStatementError(String sqlState,
        Statement st, String query, String... violatedConstraints)
        throws SQLException
    {
        try {

            st.execute(query);
            fail("Expected error '" + sqlState + "' when executing a " +
                "statement, but no error was thrown.");

        } catch (SQLException se) {

            assertSQLState(sqlState, se);

            boolean foundConstraint = false;
            for (String c : violatedConstraints) {
                if (se.getMessage().contains(c)) {
                    foundConstraint = true;
                    break;
                }
            }

            if (!foundConstraint)
            {
                fail("Error " + sqlState + " should have been caused " +
                    "by one of the following indexes/constraints " +
                    Arrays.toString(violatedConstraints) +
                    ", but none of them appeared in the error message.",
                    se);
            }

        }
    }

    /**
     * Do various checks to ensure that the constraint has truly
     * been dropped.  Then do simple SELECT queries using optimizer
     * overrides to verify that all expected remaining constraints
     * still exist, and that their backing indexes all contain the
     * expected number of rows.
     */
    private void verifyConstraints(Statement st,
        PreparedStatement constraintNames, String tName,
        String constraintName, String [][] remainingConstraints,
        int numRowsExpected) throws SQLException
    {
        constraintNames.setString(1, tName);
        ResultSet constraints = constraintNames.executeQuery();
        if (remainingConstraints == null)
            JDBC.assertEmpty(constraints);
        else
            JDBC.assertUnorderedResultSet(constraints, remainingConstraints);

        String select = "select * from " +
            tName + " --DERBY-PROPERTIES constraint=";

        /* Make sure the dropped constraint is no longer visible
         * from SQL.
         */
        if (constraintName != null)
            assertStatementError("42Y48", st, select + constraintName);

        JDBC.assertDrainResults(st.executeQuery(
            "select * from " + tName), numRowsExpected);

        if (remainingConstraints == null)
            return;

        /* Run through the remaining constraints and do a simple
         * SELECT with each one (via optimizer overrides) as a
         * sanity check that we see the correct number of rows.
         */
        for (int i = 0; i < remainingConstraints.length; i++)
        {
            JDBC.assertDrainResults(
                st.executeQuery(select + remainingConstraints[i][0]),
                numRowsExpected);
        }

        return;
    }
}
