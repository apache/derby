/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.OrderByAndSortAvoidance
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;

/**
 * Tests for DERBY-3926. Optimizer is choosing to avoid sort which is
 * causing the results to be returned in wrong order.
 */
public class OrderByAndSortAvoidance extends BaseJDBCTestCase {

    public OrderByAndSortAvoidance(String name) {
        super(name);
    }

    /**
     * Construct top level suite in this JUnit test
     *
     * @return A suite containing embedded and client suites.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("OrderByAndSortAvoidance");

        suite.addTest(makeSuite());

        suite.addTest(
             TestConfiguration.clientServerDecorator(makeSuite()));

        return suite;
    }

    /**
     * Construct suite of tests
     *
     * @return A suite containing the test cases.
     */
    private static Test makeSuite()
    {
        return new CleanDatabaseTestSetup(
            new TestSuite(OrderByAndSortAvoidance.class)) {
                protected void decorateSQL(Statement st)
                        throws SQLException {
                    st.executeUpdate("create table a(col1 int, col2 int)");
                    st.executeUpdate("insert into a values(1,1),(1,1)");
                    st.executeUpdate("create table b(col1 int, col2 int)");
                    st.executeUpdate("insert into b values(2,2),(2,2)");
                    st.executeUpdate("create table c(col1 int, col2 int)");
                    st.executeUpdate("insert into c values(3,3),(3,3)");

                    st.executeUpdate(
                        "create TABLE table1 (id BIGINT NOT NULL, PRIMARY KEY(id))");

                    st.executeUpdate(
                        "CREATE INDEX key1 ON table1(id)");
        
                    st.executeUpdate(
                        "CREATE TABLE table2 (id BIGINT NOT NULL, name "
                        + "VARCHAR(40) NOT NULL, value VARCHAR(1024), PRIMARY "
                        + "KEY(id, name))");
            
                    st.executeUpdate(
                        "CREATE UNIQUE INDEX key2 ON table2(id, name)");
        
                    st.executeUpdate(
                        "CREATE INDEX key3 ON table2(value)");

                    populateTestTables(getConnection());

                    //Start of tables creation for DERBY-4240 repro
                    st.executeUpdate(
                        "CREATE TABLE test1 (id BIGINT NOT NULL, name VARCHAR(255), "+
                        "PRIMARY KEY (id))");
                    st.executeUpdate(
                        "CREATE TABLE test2 (entity_id BIGINT, rel_id BIGINT)");
                    st.executeUpdate(
                        "CREATE INDEX idx_test2 ON test2 (entity_id)");
                    st.executeUpdate(
                        "INSERT INTO test1 (id, name) VALUES (102, 'Tom')");
                    st.executeUpdate(
                        "INSERT INTO test1 (id, name) VALUES (1, null)");
                    st.executeUpdate(
                        "INSERT INTO test1 (id, name) VALUES (103, 'Jerry')");
                    st.executeUpdate(
                        "INSERT INTO test1 (id, name) VALUES (101, 'Pupy')");
                    st.executeUpdate(
                        "INSERT INTO test2 (entity_id, rel_id) VALUES (1, 102)");
                    st.executeUpdate(
                        "INSERT INTO test2 (entity_id, rel_id) VALUES (1, 101)");
                    st.executeUpdate(
                        "INSERT INTO test2 (entity_id, rel_id) VALUES (1, 103)");
                    //End of tables creation for DERBY-4240 repro

                    //Start of tables creation for DERBY-4331 repro
                    st.executeUpdate(
                        "CREATE TABLE REPOSITORIES ( ID INT CONSTRAINT "+
                        "REPOSITORIES_PRIMARY_ID PRIMARY KEY GENERATED ALWAYS "+
                        "AS IDENTITY, "+
                        "PATH VARCHAR(32672) CONSTRAINT REPOSITORIES_PATH "+
                        "UNIQUE NOT NULL)");
                    st.executeUpdate(
                        "CREATE TABLE FILES ( ID INT CONSTRAINT FILES_PRIMARY_ID "+
                        "PRIMARY KEY GENERATED ALWAYS AS IDENTITY, "+
                        "PATH VARCHAR(32672) NOT NULL, REPOSITORY INT NOT NULL "+
                        "REFERENCES REPOSITORIES ON DELETE CASCADE, "+
                        "CONSTRAINT FILES_REPOSITORY_PATH UNIQUE "+
                        "(REPOSITORY, PATH))");
                    st.executeUpdate(
                        "CREATE TABLE AUTHORS ( "+
                        "ID INT CONSTRAINT AUTHORS_PRIMARY_ID PRIMARY KEY "+
                        "GENERATED ALWAYS AS IDENTITY, REPOSITORY INT NOT NULL "+
                        "REFERENCES REPOSITORIES ON DELETE CASCADE, "+
                        "NAME VARCHAR(32672) NOT NULL, "+
                        "CONSTRAINT AUTHORS_REPOSITORY_NAME UNIQUE (REPOSITORY, NAME))");
                    st.executeUpdate(
                        "CREATE TABLE CHANGESETS ( "+
                        "ID INT CONSTRAINT CHANGESETS_PRIMARY_ID PRIMARY KEY "+
                        "GENERATED ALWAYS AS IDENTITY, " +
                        "REPOSITORY INT NOT NULL REFERENCES REPOSITORIES "+
                        "ON DELETE CASCADE, REVISION VARCHAR(1024) NOT NULL, "+
                        "AUTHOR INT NOT NULL REFERENCES AUTHORS ON DELETE CASCADE, "+
                        "TIME TIMESTAMP NOT NULL, MESSAGE VARCHAR(32672) NOT NULL, "+
                        "CONSTRAINT CHANGESETS_REPOSITORY_REVISION UNIQUE "+
                        "(REPOSITORY, REVISION))");
                    st.executeUpdate(
                        "CREATE UNIQUE INDEX IDX_CHANGESETS_ID_DESC ON "+
                        "CHANGESETS(ID DESC)");
                    st.executeUpdate(
                        "CREATE TABLE FILECHANGES ( "+
                        "ID INT CONSTRAINT FILECHANGES_PRIMARY_ID PRIMARY KEY "+
                        "GENERATED ALWAYS AS IDENTITY, FILE INT NOT NULL "+
                        "REFERENCES FILES ON DELETE CASCADE, "+
                        "CHANGESET INT NOT NULL REFERENCES CHANGESETS "+
                        "ON DELETE CASCADE, " +
                        "CONSTRAINT FILECHANGES_FILE_CHANGESET "+
                        "UNIQUE (FILE, CHANGESET))");
                    st.executeUpdate(
                        "insert into repositories(path) values "+
                        "'/var/tmp/source5923202038296723704opengrok/mercurial'");
                    st.executeUpdate(
                        "insert into files(path, repository) values "+
                        "('/mercurial/Makefile', 1), "+
                        "('/mercurial/main.c', 1), "+
                        "('/mercurial/header.h', 1), "+
                        "('/mercurial/.hgignore', 1)");
                    st.executeUpdate(
                        "insert into authors(repository, name) values "+
                        "(1, 'Trond Norbye <trond.norbye@sun.com>')");
                    st.executeUpdate(
                        "insert into changesets(repository, revision, author, "+
                        "time, message) values (1,'0:816b6279ae9c',1,"+
                        "'2008-08-12 22:00:00.0','Add .hgignore file'),"+
                        "(1,'1:f24a5fd7a85d',1,'2008-08-12 22:03:00.0',"+
                        "'Created a small dummy program'),"+
                        "(1,'2:585a1b3f2efb',1,'2008-08-12 22:13:00.0',"+
                        "'Add lint make target and fix lint warnings')");
                    st.executeUpdate(
                        "insert into filechanges(file, changeset) values "+
                        "(4,1),(1,2),(3,2),(2,2),(1,3),(2,3)");
                    //End of tables creation for DERBY-4331 repro

                }
            };
    }

    /**
     * Helper method that inserts a row into table1.
     * @param ps parameterized statement that inserts a row into table1
     * @param id the value of the id column
     */
    private static void insertTable1(PreparedStatement ps, long id)
            throws SQLException {
        ps.setLong(1, id);
        ps.executeUpdate();
    }

    /**
     * Helper method that inserts a row into table2.
     * @param ps parameterized statement that inserts a row into table2
     * @param id the value of the id column
     * @param name the value of the name column
     * @param value the value of the value column
     */
    private static void insertTable2(PreparedStatement ps, long id,
                                     String name, String value)
            throws SQLException {
        ps.setLong(1, id);
        ps.setString(2, name);
        ps.setString(3, value);
        ps.executeUpdate();
    }

    /**
     * Helper method that updates a row in table2
     * @param ps parameterized statement that updates a row in table2
     * @param id the id of the row to update
     */
    private static void updateTable2(PreparedStatement ps, long id) throws SQLException {
        ps.setLong(1, id);
        ps.executeUpdate();
    }

    /**
     * Populate table1 and table2 with the rows needed for reproducing
     * DERBY-3926.
     */
    private static void populateTestTables(Connection conn) throws SQLException {

        PreparedStatement it1 = conn.prepareStatement(
            "INSERT INTO table1 VALUES (?)");
        PreparedStatement it2 = conn.prepareStatement(
            "INSERT INTO table2 VALUES (?,?,?)");
        PreparedStatement ut2 = conn.prepareStatement(
            "UPDATE table2 SET value='true' WHERE id=? AND name='has_address'");

        insertTable1(it1, 2147483649L);
        insertTable2(it2, 2147483649L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483649L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483649L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483649L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483649L, "DocumentSortingValues", "______21855__1__1");
        insertTable2(it2, 2147483649L, "ItemSeq", "1");
        insertTable2(it2, 2147483649L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483649L, "Plex", "S");
        insertTable2(it2, 2147483649L, "SC3", "=");
        insertTable2(it2, 2147483649L, "has_address", "true");
        insertTable2(it2, 2147483649L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483649L, "Language", "FR");
        insertTable2(it2, 2147483649L, "logo", "false");
        insertTable2(it2, 2147483649L, "SC4", "=");
        insertTable2(it2, 2147483649L, "InternalAddress", "");
        insertTable2(it2, 2147483649L, "AddressG6", "");
        insertTable2(it2, 2147483649L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483649L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483649L, "SC1", "Country=BE");
        insertTable2(it2, 2147483649L, "CommunicationOrderId", "21865");
        insertTable2(it2, 2147483649L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483649L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483649L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483649L, "Enveloping", "C");
        insertTable2(it2, 2147483649L, "PostComponentId", "21855");
        insertTable2(it2, 2147483649L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483649L, "city", "Bruxelles");
        insertTable2(it2, 2147483649L, "AddressG7", "");
        insertTable2(it2, 2147483649L, "CopyMention", "");
        insertTable2(it2, 2147483649L, "AddressG8", "");
        insertTable2(it2, 2147483649L, "StapleNbr", "");
        insertTable2(it2, 2147483649L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483649L, "AddressG5", "");
        insertTable2(it2, 2147483649L, "header", "true");
        insertTable2(it2, 2147483649L, "EnvelopSortingValue", "BE1180___testOlivier001-0001___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483649L, "GroupedWith", "");
        insertTable2(it2, 2147483649L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483649L, "DiversionReason", "");
        insertTable2(it2, 2147483649L, "Pliable", "true");
        insertTable2(it2, 2147483649L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483649L, "country", "BE");
        insertTable2(it2, 2147483649L, "AddressG1", "testOlivier001-0001");
        insertTable2(it2, 2147483649L, "MentionCode", "");
        insertTable2(it2, 2147483649L, "postCode", "1180");
        insertTable2(it2, 2147483649L, "SC5", "=");
        insertTable2(it2, 2147483649L, "Branding", "1C");
        insertTable1(it1, 2147483650L);
        insertTable2(it2, 2147483650L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483650L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483650L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483650L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483650L, "DocumentSortingValues", "______21855__1__1");
        insertTable2(it2, 2147483650L, "ItemSeq", "1");
        insertTable2(it2, 2147483650L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483650L, "Plex", "S");
        insertTable2(it2, 2147483650L, "SC3", "=");
        insertTable2(it2, 2147483650L, "has_address", "true");
        insertTable2(it2, 2147483650L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483650L, "Language", "FR");
        insertTable2(it2, 2147483650L, "logo", "false");
        insertTable2(it2, 2147483650L, "SC4", "=");
        insertTable2(it2, 2147483650L, "InternalAddress", "");
        insertTable2(it2, 2147483650L, "AddressG6", "");
        insertTable2(it2, 2147483650L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483650L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483650L, "SC1", "Country=BE");
        insertTable2(it2, 2147483650L, "CommunicationOrderId", "21865");
        insertTable2(it2, 2147483650L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483650L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483650L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483650L, "Enveloping", "C");
        insertTable2(it2, 2147483650L, "PostComponentId", "21855");
        insertTable2(it2, 2147483650L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483650L, "city", "Bruxelles");
        insertTable2(it2, 2147483650L, "AddressG7", "");
        insertTable2(it2, 2147483650L, "CopyMention", "");
        insertTable2(it2, 2147483650L, "AddressG8", "");
        insertTable2(it2, 2147483650L, "StapleNbr", "");
        insertTable2(it2, 2147483650L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483650L, "AddressG5", "");
        insertTable2(it2, 2147483650L, "header", "true");
        insertTable2(it2, 2147483650L, "EnvelopSortingValue", "BE1180___testOlivier001-0001___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483650L, "GroupedWith", "");
        insertTable2(it2, 2147483650L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483650L, "DiversionReason", "");
        insertTable2(it2, 2147483650L, "Pliable", "true");
        insertTable2(it2, 2147483650L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483650L, "country", "BE");
        insertTable2(it2, 2147483650L, "AddressG1", "testOlivier001-0001");
        insertTable2(it2, 2147483650L, "MentionCode", "");
        insertTable2(it2, 2147483650L, "postCode", "1180");
        insertTable2(it2, 2147483650L, "SC5", "=");
        insertTable2(it2, 2147483650L, "Branding", "1C");
        insertTable1(it1, 2147483651L);
        insertTable2(it2, 2147483651L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483651L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483651L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483651L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483651L, "DocumentSortingValues", "______21856__1__1");
        insertTable2(it2, 2147483651L, "ItemSeq", "1");
        insertTable2(it2, 2147483651L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483651L, "Plex", "S");
        insertTable2(it2, 2147483651L, "SC3", "=");
        insertTable2(it2, 2147483651L, "has_address", "true");
        insertTable2(it2, 2147483651L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483651L, "Language", "FR");
        insertTable2(it2, 2147483651L, "logo", "false");
        insertTable2(it2, 2147483651L, "SC4", "=");
        insertTable2(it2, 2147483651L, "InternalAddress", "");
        insertTable2(it2, 2147483651L, "AddressG6", "");
        insertTable2(it2, 2147483651L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483651L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483651L, "SC1", "Country=BE");
        insertTable2(it2, 2147483651L, "CommunicationOrderId", "21866");
        insertTable2(it2, 2147483651L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483651L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483651L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483651L, "Enveloping", "C");
        insertTable2(it2, 2147483651L, "PostComponentId", "21856");
        insertTable2(it2, 2147483651L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483651L, "city", "Bruxelles");
        insertTable2(it2, 2147483651L, "AddressG7", "");
        insertTable2(it2, 2147483651L, "CopyMention", "");
        insertTable2(it2, 2147483651L, "AddressG8", "");
        insertTable2(it2, 2147483651L, "StapleNbr", "");
        insertTable2(it2, 2147483651L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483651L, "AddressG5", "");
        insertTable2(it2, 2147483651L, "header", "true");
        insertTable2(it2, 2147483651L, "EnvelopSortingValue", "BE1180___testOlivier001-0002___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483651L, "GroupedWith", "");
        insertTable2(it2, 2147483651L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483651L, "DiversionReason", "");
        insertTable2(it2, 2147483651L, "Pliable", "true");
        insertTable2(it2, 2147483651L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483651L, "country", "BE");
        insertTable2(it2, 2147483651L, "AddressG1", "testOlivier001-0002");
        insertTable2(it2, 2147483651L, "MentionCode", "");
        insertTable2(it2, 2147483651L, "postCode", "1180");
        insertTable2(it2, 2147483651L, "SC5", "=");
        insertTable2(it2, 2147483651L, "Branding", "1C");
        insertTable1(it1, 2147483652L);
        insertTable2(it2, 2147483652L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483652L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483652L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483652L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483652L, "DocumentSortingValues", "______21856__1__1");
        insertTable2(it2, 2147483652L, "ItemSeq", "1");
        insertTable2(it2, 2147483652L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483652L, "Plex", "S");
        insertTable2(it2, 2147483652L, "SC3", "=");
        insertTable2(it2, 2147483652L, "has_address", "true");
        insertTable2(it2, 2147483652L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483652L, "Language", "FR");
        insertTable2(it2, 2147483652L, "logo", "false");
        insertTable2(it2, 2147483652L, "SC4", "=");
        insertTable2(it2, 2147483652L, "InternalAddress", "");
        insertTable2(it2, 2147483652L, "AddressG6", "");
        insertTable2(it2, 2147483652L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483652L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483652L, "SC1", "Country=BE");
        insertTable2(it2, 2147483652L, "CommunicationOrderId", "21866");
        insertTable2(it2, 2147483652L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483652L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483652L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483652L, "Enveloping", "C");
        insertTable2(it2, 2147483652L, "PostComponentId", "21856");
        insertTable2(it2, 2147483652L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483652L, "city", "Bruxelles");
        insertTable2(it2, 2147483652L, "AddressG7", "");
        insertTable2(it2, 2147483652L, "CopyMention", "");
        insertTable2(it2, 2147483652L, "AddressG8", "");
        insertTable2(it2, 2147483652L, "StapleNbr", "");
        insertTable2(it2, 2147483652L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483652L, "AddressG5", "");
        insertTable2(it2, 2147483652L, "header", "true");
        insertTable2(it2, 2147483652L, "EnvelopSortingValue","BE1180___testOlivier001-0002___________Bla bla bla bla bl,a bla 99____");
        insertTable2(it2, 2147483652L, "GroupedWith", "");
        insertTable2(it2, 2147483652L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483652L, "DiversionReason", "");
        insertTable2(it2, 2147483652L, "Pliable", "true");
        insertTable2(it2, 2147483652L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483652L, "country", "BE");
        insertTable2(it2, 2147483652L, "AddressG1", "testOlivier001-0002");
        insertTable2(it2, 2147483652L, "MentionCode", "");
        insertTable2(it2, 2147483652L, "postCode", "1180");
        insertTable2(it2, 2147483652L, "SC5", "=");
        insertTable2(it2, 2147483652L, "Branding", "1C");
        insertTable1(it1, 2147483653L);
        insertTable2(it2, 2147483653L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483653L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483653L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483653L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483653L, "DocumentSortingValues", "______21857__1__1");
        insertTable2(it2, 2147483653L, "ItemSeq", "1");
        insertTable2(it2, 2147483653L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483653L, "Plex", "S");
        insertTable2(it2, 2147483653L, "SC3", "=");
        insertTable2(it2, 2147483653L, "has_address", "true");
        insertTable2(it2, 2147483653L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483653L, "Language", "FR");
        insertTable2(it2, 2147483653L, "logo", "false");
        insertTable2(it2, 2147483653L, "SC4", "=");
        insertTable2(it2, 2147483653L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483653L, "AddressG6", "");
        insertTable2(it2, 2147483653L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483653L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483653L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483653L, "CommunicationOrderId", "21867");
        insertTable2(it2, 2147483653L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483653L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483653L, "SortPlan", "");
        insertTable2(it2, 2147483653L, "Enveloping", "N");
        insertTable2(it2, 2147483653L, "PostComponentId", "21857");
        insertTable2(it2, 2147483653L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483653L, "city", "Bruxelles");
        insertTable2(it2, 2147483653L, "AddressG7", "");
        insertTable2(it2, 2147483653L, "CopyMention", "");
        insertTable2(it2, 2147483653L, "AddressG8", "");
        insertTable2(it2, 2147483653L, "StapleNbr", "");
        insertTable2(it2, 2147483653L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483653L, "AddressG5", "");
        insertTable2(it2, 2147483653L, "header", "true");
        insertTable2(it2, 2147483653L, "EnvelopSortingValue", "BE1180___testOlivier002-0003___________Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483653L, "GroupedWith", "");
        insertTable2(it2, 2147483653L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483653L, "DiversionReason", "001");
        insertTable2(it2, 2147483653L, "Pliable", "true");
        insertTable2(it2, 2147483653L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483653L, "country", "BE");
        insertTable2(it2, 2147483653L, "AddressG1", "testOlivier002-0003");
        insertTable2(it2, 2147483653L, "MentionCode", "");
        insertTable2(it2, 2147483653L, "postCode", "1180");
        insertTable2(it2, 2147483653L, "SC5", "=");
        insertTable2(it2, 2147483653L, "Branding", "1C");
        insertTable1(it1, 2147483654L);
        insertTable2(it2, 2147483654L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483654L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483654L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483654L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483654L, "DocumentSortingValues", "______21857__1__1");
        insertTable2(it2, 2147483654L, "ItemSeq", "1");
        insertTable2(it2, 2147483654L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483654L, "Plex", "S");
        insertTable2(it2, 2147483654L, "SC3", "=");
        insertTable2(it2, 2147483654L, "has_address", "true");
        insertTable2(it2, 2147483654L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483654L, "Language", "FR");
        insertTable2(it2, 2147483654L, "logo", "false");
        insertTable2(it2, 2147483654L, "SC4", "=");
        insertTable2(it2, 2147483654L, "InternalAddress","233/621");
        insertTable2(it2, 2147483654L, "AddressG6", "");
        insertTable2(it2, 2147483654L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483654L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483654L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483654L, "CommunicationOrderId", "21867");
        insertTable2(it2, 2147483654L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483654L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483654L, "SortPlan", "");
        insertTable2(it2, 2147483654L, "Enveloping", "N");
        insertTable2(it2, 2147483654L, "PostComponentId", "21857");
        insertTable2(it2, 2147483654L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483654L, "city", "Bruxelles");
        insertTable2(it2, 2147483654L, "AddressG7", "");
        insertTable2(it2, 2147483654L, "CopyMention", "");
        insertTable2(it2, 2147483654L, "AddressG8", "");
        insertTable2(it2, 2147483654L, "StapleNbr", "");
        insertTable2(it2, 2147483654L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483654L, "AddressG5", "");
        insertTable2(it2, 2147483654L, "header", "true");
        insertTable2(it2, 2147483654L, "EnvelopSortingValue", "BE1180___testOlivier002-0003___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483654L, "GroupedWith", "");
        insertTable2(it2, 2147483654L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483654L, "DiversionReason", "001");
        insertTable2(it2, 2147483654L, "Pliable", "true");
        insertTable2(it2, 2147483654L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483654L, "country", "BE");
        insertTable2(it2, 2147483654L, "AddressG1", "testOlivier002-0003");
        insertTable2(it2, 2147483654L, "MentionCode", "");
        insertTable2(it2, 2147483654L, "postCode", "1180");
        insertTable2(it2, 2147483654L, "SC5", "=");
        insertTable2(it2, 2147483654L, "Branding", "1C");
        insertTable1(it1, 2147483655L);
        insertTable2(it2, 2147483655L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483655L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483655L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483655L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483655L, "DocumentSortingValues", "______21858__1__1");
        insertTable2(it2, 2147483655L, "ItemSeq", "1");
        insertTable2(it2, 2147483655L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483655L, "Plex", "S");
        insertTable2(it2, 2147483655L, "SC3", "=");
        insertTable2(it2, 2147483655L, "has_address", "true");
        insertTable2(it2, 2147483655L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483655L, "Language", "FR");
        insertTable2(it2, 2147483655L, "logo", "false");
        insertTable2(it2, 2147483655L, "SC4", "=");
        insertTable2(it2, 2147483655L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483655L, "AddressG6", "");
        insertTable2(it2, 2147483655L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483655L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483655L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483655L, "CommunicationOrderId", "21868");
        insertTable2(it2, 2147483655L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483655L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483655L, "SortPlan", "");
        insertTable2(it2, 2147483655L, "Enveloping", "N");
        insertTable2(it2, 2147483655L, "PostComponentId", "21858");
        insertTable2(it2, 2147483655L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483655L, "city", "Bruxelles");
        insertTable2(it2, 2147483655L, "AddressG7", "");
        insertTable2(it2, 2147483655L, "CopyMention", "");
        insertTable2(it2, 2147483655L, "AddressG8", "");
        insertTable2(it2, 2147483655L, "StapleNbr", "");
        insertTable2(it2, 2147483655L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483655L, "AddressG5", "");
        insertTable2(it2, 2147483655L, "header", "true");
        insertTable2(it2, 2147483655L, "EnvelopSortingValue", "BE1180___testOlivier002-0004___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483655L, "GroupedWith", "");
        insertTable2(it2, 2147483655L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483655L, "DiversionReason", "001");
        insertTable2(it2, 2147483655L, "Pliable", "true");
        insertTable2(it2, 2147483655L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483655L, "country", "BE");
        insertTable2(it2, 2147483655L, "AddressG1", "testOlivier002-0004");
        insertTable2(it2, 2147483655L, "MentionCode", "");
        insertTable2(it2, 2147483655L, "postCode", "1180");
        insertTable2(it2, 2147483655L, "SC5", "=");
        insertTable2(it2, 2147483655L, "Branding", "1C");
        insertTable1(it1, 2147483656L);
        insertTable2(it2, 2147483656L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483656L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483656L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483656L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483656L, "DocumentSortingValues", "______21858__1__1");
        insertTable2(it2, 2147483656L, "ItemSeq", "1");
        insertTable2(it2, 2147483656L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483656L, "Plex", "S");
        insertTable2(it2, 2147483656L, "SC3", "=");
        insertTable2(it2, 2147483656L, "has_address", "true");
        insertTable2(it2, 2147483656L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483656L, "Language", "FR");
        insertTable2(it2, 2147483656L, "logo", "false");
        insertTable2(it2, 2147483656L, "SC4", "=");
        insertTable2(it2, 2147483656L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483656L, "AddressG6", "");
        insertTable2(it2, 2147483656L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483656L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483656L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483656L, "CommunicationOrderId", "21868");
        insertTable2(it2, 2147483656L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483656L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483656L, "SortPlan", "");
        insertTable2(it2, 2147483656L, "Enveloping", "N");
        insertTable2(it2, 2147483656L, "PostComponentId", "21858");
        insertTable2(it2, 2147483656L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483656L, "city", "Bruxelles");
        insertTable2(it2, 2147483656L, "AddressG7", "");
        insertTable2(it2, 2147483656L, "CopyMention", "");
        insertTable2(it2, 2147483656L, "AddressG8", "");
        insertTable2(it2, 2147483656L, "StapleNbr", "");
        insertTable2(it2, 2147483656L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483656L, "AddressG5", "");
        insertTable2(it2, 2147483656L, "header", "true");
        insertTable2(it2, 2147483656L, "EnvelopSortingValue", "BE1180___testOlivier002-0004___________Boulevard du Souverain, 23____");
        insertTable2(it2, 2147483656L, "GroupedWith", "");
        insertTable2(it2, 2147483656L, "AddressG2", "Boulevard du Souverain, 23");
        insertTable2(it2, 2147483656L, "DiversionReason", "001");
        insertTable2(it2, 2147483656L, "Pliable", "true");
        insertTable2(it2, 2147483656L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483656L, "country", "BE");
        insertTable2(it2, 2147483656L, "AddressG1", "testOlivier002-0004");
        insertTable2(it2, 2147483656L, "MentionCode", "");
        insertTable2(it2, 2147483656L, "postCode", "1180");
        insertTable2(it2, 2147483656L, "SC5", "=");
        insertTable2(it2, 2147483656L, "Branding", "1C");
        insertTable1(it1, 2147483657L);
        insertTable2(it2, 2147483657L, "SC2", "=");
        insertTable2(it2, 2147483657L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483657L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483657L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483657L, "DocumentSortingValues", "______21859__1__1");
        insertTable2(it2, 2147483657L, "ItemSeq", "1");
        insertTable2(it2, 2147483657L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483657L, "Plex", "S");
        insertTable2(it2, 2147483657L, "SC3", "=");
        insertTable2(it2, 2147483657L, "has_address", "true");
        insertTable2(it2, 2147483657L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483657L, "Language", "FR");
        insertTable2(it2, 2147483657L, "logo", "false");
        insertTable2(it2, 2147483657L, "SC4", "=");
        insertTable2(it2, 2147483657L, "InternalAddress", "");
        insertTable2(it2, 2147483657L, "AddressG6", "");
        insertTable2(it2, 2147483657L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483657L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483657L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483657L, "CommunicationOrderId", "21869");
        insertTable2(it2, 2147483657L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483657L, "location", "rue du broker");
        insertTable2(it2, 2147483657L, "SortPlan", "");
        insertTable2(it2, 2147483657L, "Enveloping", "C");
        insertTable2(it2, 2147483657L, "PostComponentId", "21859");
        insertTable2(it2, 2147483657L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483657L, "city", "Broker ville");
        insertTable2(it2, 2147483657L, "AddressG7", "");
        insertTable2(it2, 2147483657L, "CopyMention", "");
        insertTable2(it2, 2147483657L, "AddressG8", "");
        insertTable2(it2, 2147483657L, "StapleNbr", "");
        insertTable2(it2, 2147483657L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483657L, "AddressG5", "");
        insertTable2(it2, 2147483657L, "header", "true");
        insertTable2(it2, 2147483657L, "EnvelopSortingValue", "BE1000___testOlivier003-0005___________rue du broker_________________");
        insertTable2(it2, 2147483657L, "GroupedWith", "");
        insertTable2(it2, 2147483657L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483657L, "DiversionReason", "");
        insertTable2(it2, 2147483657L, "Pliable", "true");
        insertTable2(it2, 2147483657L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483657L, "country", "BE");
        insertTable2(it2, 2147483657L, "AddressG1", "testOlivier003-0005");
        insertTable2(it2, 2147483657L, "MentionCode", "");
        insertTable2(it2, 2147483657L, "postCode", "1000");
        insertTable2(it2, 2147483657L, "SC5", "=");
        insertTable2(it2, 2147483657L, "Branding", "1C");
        insertTable1(it1, 2147483658L);
        insertTable2(it2, 2147483658L, "SC2", "=");
        insertTable2(it2, 2147483658L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483658L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483658L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483658L, "DocumentSortingValues", "______21859__1__1");
        insertTable2(it2, 2147483658L, "ItemSeq", "1");
        insertTable2(it2, 2147483658L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483658L, "Plex", "S");
        insertTable2(it2, 2147483658L, "SC3", "=");
        insertTable2(it2, 2147483658L, "has_address", "true");
        insertTable2(it2, 2147483658L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483658L, "Language", "FR");
        insertTable2(it2, 2147483658L, "logo", "false");
        insertTable2(it2, 2147483658L, "SC4", "=");
        insertTable2(it2, 2147483658L, "InternalAddress", "");
        insertTable2(it2, 2147483658L, "AddressG6", "");
        insertTable2(it2, 2147483658L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483658L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483658L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483658L, "CommunicationOrderId", "21869");
        insertTable2(it2, 2147483658L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483658L, "location", "rue du broker");
        insertTable2(it2, 2147483658L, "SortPlan", "");
        insertTable2(it2, 2147483658L, "Enveloping", "C");
        insertTable2(it2, 2147483658L, "PostComponentId", "21859");
        insertTable2(it2, 2147483658L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483658L, "city", "Broker ville");
        insertTable2(it2, 2147483658L, "AddressG7", "");
        insertTable2(it2, 2147483658L, "CopyMention", "");
        insertTable2(it2, 2147483658L, "AddressG8", "");
        insertTable2(it2, 2147483658L, "StapleNbr", "");
        insertTable2(it2, 2147483658L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483658L, "AddressG5", "");
        insertTable2(it2, 2147483658L, "header", "true");
        insertTable2(it2, 2147483658L, "EnvelopSortingValue", "BE1000___testOlivier003-0005___________rue dubroker_________________");
        insertTable2(it2, 2147483658L, "GroupedWith", "");
        insertTable2(it2, 2147483658L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483658L, "DiversionReason", "");
        insertTable2(it2, 2147483658L, "Pliable", "true");
        insertTable2(it2, 2147483658L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483658L, "country", "BE");
        insertTable2(it2, 2147483658L, "AddressG1", "testOlivier003-0005");
        insertTable2(it2, 2147483658L, "MentionCode", "");
        insertTable2(it2, 2147483658L, "postCode", "1000");
        insertTable2(it2, 2147483658L, "SC5", "=");
        insertTable2(it2, 2147483658L, "Branding", "1C");
        insertTable1(it1, 2147483659L);
        insertTable2(it2, 2147483659L, "SC2", "=");
        insertTable2(it2, 2147483659L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483659L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483659L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483659L, "DocumentSortingValues", "______21860__1__1");
        insertTable2(it2, 2147483659L, "ItemSeq", "1");
        insertTable2(it2, 2147483659L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483659L, "Plex", "S");
        insertTable2(it2, 2147483659L, "SC3", "=");
        insertTable2(it2, 2147483659L, "has_address", "true");
        insertTable2(it2, 2147483659L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483659L, "Language", "FR");
        insertTable2(it2, 2147483659L, "logo", "false");
        insertTable2(it2, 2147483659L, "SC4", "=");
        insertTable2(it2, 2147483659L, "InternalAddress", "");
        insertTable2(it2, 2147483659L, "AddressG6", "");
        insertTable2(it2, 2147483659L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483659L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483659L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483659L, "CommunicationOrderId", "21870");
        insertTable2(it2, 2147483659L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483659L, "location", "rue du broker");
        insertTable2(it2, 2147483659L, "SortPlan", "");
        insertTable2(it2, 2147483659L, "Enveloping", "C");
        insertTable2(it2, 2147483659L, "PostComponentId", "21860");
        insertTable2(it2, 2147483659L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483659L, "city", "Broker ville");
        insertTable2(it2, 2147483659L, "AddressG7", "");
        insertTable2(it2, 2147483659L, "CopyMention", "");
        insertTable2(it2, 2147483659L, "AddressG8", "");
        insertTable2(it2, 2147483659L, "StapleNbr", "");
        insertTable2(it2, 2147483659L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483659L, "AddressG5", "");
        insertTable2(it2, 2147483659L, "header", "true");
        insertTable2(it2, 2147483659L, "EnvelopSortingValue", "BE1000___testOlivier003-0006___________rue du broker_________________");
        insertTable2(it2, 2147483659L, "GroupedWith", "");
        insertTable2(it2, 2147483659L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483659L, "DiversionReason", "");
        insertTable2(it2, 2147483659L, "Pliable", "true");
        insertTable2(it2, 2147483659L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483659L, "country", "BE");
        insertTable2(it2, 2147483659L, "AddressG1", "testOlivier003-0006");
        insertTable2(it2, 2147483659L, "MentionCode", "");
        insertTable2(it2, 2147483659L, "postCode", "1000");
        insertTable2(it2, 2147483659L, "SC5", "=");
        insertTable2(it2, 2147483659L, "Branding", "1C");
        insertTable1(it1, 2147483660L);
        insertTable2(it2, 2147483660L, "SC2", "=");
        insertTable2(it2, 2147483660L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483660L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483660L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483660L, "DocumentSortingValues", "______21860__1__1");
        insertTable2(it2, 2147483660L, "ItemSeq", "1");
        insertTable2(it2, 2147483660L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483660L, "Plex", "S");
        insertTable2(it2, 2147483660L, "SC3", "=");
        insertTable2(it2, 2147483660L, "has_address", "true");
        insertTable2(it2, 2147483660L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483660L, "Language", "FR");
        insertTable2(it2, 2147483660L, "logo", "false");
        insertTable2(it2, 2147483660L, "SC4", "=");
        insertTable2(it2, 2147483660L, "InternalAddress", "");
        insertTable2(it2, 2147483660L, "AddressG6", "");
        insertTable2(it2, 2147483660L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483660L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483660L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483660L, "CommunicationOrderId", "21870");
        insertTable2(it2, 2147483660L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483660L, "location", "rue du broker");
        insertTable2(it2, 2147483660L, "SortPlan", "");
        insertTable2(it2, 2147483660L, "Enveloping", "C");
        insertTable2(it2, 2147483660L, "PostComponentId", "21860");
        insertTable2(it2, 2147483660L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483660L, "city", "Broker ville");
        insertTable2(it2, 2147483660L, "AddressG7", "");
        insertTable2(it2, 2147483660L, "CopyMention", "");
        insertTable2(it2, 2147483660L, "AddressG8", "");
        insertTable2(it2, 2147483660L, "StapleNbr", "");
        insertTable2(it2, 2147483660L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483660L, "AddressG5", "");
        insertTable2(it2, 2147483660L, "header", "true");
        insertTable2(it2, 2147483660L, "EnvelopSortingValue", "BE1000___testOlivier003-0006___________rue dubroker_________________");
        insertTable2(it2, 2147483660L, "GroupedWith", "");
        insertTable2(it2, 2147483660L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483660L, "DiversionReason", "");
        insertTable2(it2, 2147483660L, "Pliable", "true");
        insertTable2(it2, 2147483660L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483660L, "country", "BE");
        insertTable2(it2, 2147483660L, "AddressG1", "testOlivier003-0006");
        insertTable2(it2, 2147483660L, "MentionCode", "");
        insertTable2(it2, 2147483660L, "postCode", "1000");
        insertTable2(it2, 2147483660L, "SC5", "=");
        insertTable2(it2, 2147483660L, "Branding", "1C");
        insertTable1(it1, 2147483661L);
        insertTable2(it2, 2147483661L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483661L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483661L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483661L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483661L, "DocumentSortingValues", "______21861__1__1");
        insertTable2(it2, 2147483661L, "ItemSeq", "1");
        insertTable2(it2, 2147483661L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483661L, "Plex", "S");
        insertTable2(it2, 2147483661L, "SC3", "=");
        insertTable2(it2, 2147483661L, "has_address", "true");
        insertTable2(it2, 2147483661L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483661L, "Language", "FR");
        insertTable2(it2, 2147483661L, "logo", "false");
        insertTable2(it2, 2147483661L, "SC4", "=");
        insertTable2(it2, 2147483661L, "InternalAddress", "");
        insertTable2(it2, 2147483661L, "AddressG6", "");
        insertTable2(it2, 2147483661L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483661L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483661L, "SC1", "Country=BE");
        insertTable2(it2, 2147483661L, "CommunicationOrderId", "21871");
        insertTable2(it2, 2147483661L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483661L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483661L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483661L, "Enveloping", "C");
        insertTable2(it2, 2147483661L, "PostComponentId", "21861");
        insertTable2(it2, 2147483661L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483661L, "city", "Bruxelles");
        insertTable2(it2, 2147483661L, "AddressG7", "");
        insertTable2(it2, 2147483661L, "CopyMention", "");
        insertTable2(it2, 2147483661L, "AddressG8", "");
        insertTable2(it2, 2147483661L, "StapleNbr", "");
        insertTable2(it2, 2147483661L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483661L, "AddressG5", "");
        insertTable2(it2, 2147483661L, "header", "true");
        insertTable2(it2, 2147483661L, "EnvelopSortingValue", "BE1180___testOlivier004-0007___________Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483661L, "GroupedWith", "");
        insertTable2(it2, 2147483661L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483661L, "DiversionReason", "");
        insertTable2(it2, 2147483661L, "Pliable", "true");
        insertTable2(it2, 2147483661L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483661L, "country", "BE");
        insertTable2(it2, 2147483661L, "AddressG1", "testOlivier004-0007");
        insertTable2(it2, 2147483661L, "MentionCode", "");
        insertTable2(it2, 2147483661L, "postCode", "1180");
        insertTable2(it2, 2147483661L, "SC5", "=");
        insertTable2(it2, 2147483661L, "Branding", "1C");
        insertTable1(it1, 2147483662L);
        insertTable2(it2, 2147483662L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483662L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483662L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483662L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483662L, "DocumentSortingValues", "______21861__1__1");
        insertTable2(it2, 2147483662L, "ItemSeq", "1");
        insertTable2(it2, 2147483662L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483662L, "Plex", "S");
        insertTable2(it2, 2147483662L, "SC3", "=");
        insertTable2(it2, 2147483662L, "has_address", "true");
        insertTable2(it2, 2147483662L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483662L, "Language", "FR");
        insertTable2(it2, 2147483662L, "logo", "false");
        insertTable2(it2, 2147483662L, "SC4", "=");
        insertTable2(it2, 2147483662L, "InternalAddress", "");
        insertTable2(it2, 2147483662L, "AddressG6", "");
        insertTable2(it2, 2147483662L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483662L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483662L, "SC1", "Country=BE");
        insertTable2(it2, 2147483662L, "CommunicationOrderId", "21871");
        insertTable2(it2, 2147483662L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483662L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483662L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483662L, "Enveloping", "C");
        insertTable2(it2, 2147483662L, "PostComponentId", "21861");
        insertTable2(it2, 2147483662L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483662L, "city", "Bruxelles");
        insertTable2(it2, 2147483662L, "AddressG7", "");
        insertTable2(it2, 2147483662L, "CopyMention", "");
        insertTable2(it2, 2147483662L, "AddressG8", "");
        insertTable2(it2, 2147483662L, "StapleNbr", "");
        insertTable2(it2, 2147483662L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483662L, "AddressG5", "");
        insertTable2(it2, 2147483662L, "header", "true");
        insertTable2(it2, 2147483662L, "EnvelopSortingValue", "BE1180___testOlivier004-0007___________Boulevard du Souverain, 23____");
        insertTable2(it2, 2147483662L, "GroupedWith", "");
        insertTable2(it2, 2147483662L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483662L, "DiversionReason", "");
        insertTable2(it2, 2147483662L, "Pliable", "true");
        insertTable2(it2, 2147483662L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483662L, "country", "BE");
        insertTable2(it2, 2147483662L, "AddressG1", "testOlivier004-0007");
        insertTable2(it2, 2147483662L, "MentionCode", "");
        insertTable2(it2, 2147483662L, "postCode", "1180");
        insertTable2(it2, 2147483662L, "SC5", "=");
        insertTable2(it2, 2147483662L, "Branding", "1C");
        insertTable1(it1, 2147483663L);
        insertTable2(it2, 2147483663L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483663L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483663L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483663L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483663L, "DocumentSortingValues", "______21862__1__1");
        insertTable2(it2, 2147483663L, "ItemSeq", "1");
        insertTable2(it2, 2147483663L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483663L, "Plex", "S");
        insertTable2(it2, 2147483663L, "SC3", "=");
        insertTable2(it2, 2147483663L, "has_address", "true");
        insertTable2(it2, 2147483663L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483663L, "Language", "FR");
        insertTable2(it2, 2147483663L, "logo", "false");
        insertTable2(it2, 2147483663L, "SC4", "=");
        insertTable2(it2, 2147483663L, "InternalAddress", "");
        insertTable2(it2, 2147483663L, "AddressG6", "");
        insertTable2(it2, 2147483663L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483663L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483663L, "SC1", "Country=BE");
        insertTable2(it2, 2147483663L, "CommunicationOrderId", "21872");
        insertTable2(it2, 2147483663L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483663L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483663L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483663L, "Enveloping", "C");
        insertTable2(it2, 2147483663L, "PostComponentId", "21862");
        insertTable2(it2, 2147483663L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483663L, "city", "Bruxelles");
        insertTable2(it2, 2147483663L, "AddressG7", "");
        insertTable2(it2, 2147483663L, "CopyMention", "");
        insertTable2(it2, 2147483663L, "AddressG8", "");
        insertTable2(it2, 2147483663L, "StapleNbr", "");
        insertTable2(it2, 2147483663L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483663L, "AddressG5", "");
        insertTable2(it2, 2147483663L, "header", "true");
        insertTable2(it2, 2147483663L, "EnvelopSortingValue", "BE1180___testOlivier004-0008___________Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483663L, "GroupedWith", "");
        insertTable2(it2, 2147483663L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483663L, "DiversionReason", "");
        insertTable2(it2, 2147483663L, "Pliable", "true");
        insertTable2(it2, 2147483663L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483663L, "country", "BE");
        insertTable2(it2, 2147483663L, "AddressG1", "testOlivier004-0008");
        insertTable2(it2, 2147483663L, "MentionCode", "");
        insertTable2(it2, 2147483663L, "postCode", "1180");
        insertTable2(it2, 2147483663L, "SC5", "=");
        insertTable2(it2, 2147483663L, "Branding", "1C");
        insertTable1(it1, 2147483664L);
        insertTable2(it2, 2147483664L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483664L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483664L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483664L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483664L, "DocumentSortingValues", "______21862__1__1");
        insertTable2(it2, 2147483664L, "ItemSeq", "1");
        insertTable2(it2, 2147483664L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483664L, "Plex", "S");
        insertTable2(it2, 2147483664L, "SC3", "=");
        insertTable2(it2, 2147483664L, "has_address", "true");
        insertTable2(it2, 2147483664L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483664L, "Language", "FR");
        insertTable2(it2, 2147483664L, "logo", "false");
        insertTable2(it2, 2147483664L, "SC4", "=");
        insertTable2(it2, 2147483664L, "InternalAddress", "");
        insertTable2(it2, 2147483664L, "AddressG6", "");
        insertTable2(it2, 2147483664L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483664L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483664L, "SC1", "Country=BE");
        insertTable2(it2, 2147483664L, "CommunicationOrderId", "21872");
        insertTable2(it2, 2147483664L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483664L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483664L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483664L, "Enveloping", "C");
        insertTable2(it2, 2147483664L, "PostComponentId", "21862");
        insertTable2(it2, 2147483664L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483664L, "city", "Bruxelles");
        insertTable2(it2, 2147483664L, "AddressG7", "");
        insertTable2(it2, 2147483664L, "CopyMention", "");
        insertTable2(it2, 2147483664L, "AddressG8", "");
        insertTable2(it2, 2147483664L, "StapleNbr", "");
        insertTable2(it2, 2147483664L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483664L, "AddressG5", "");
        insertTable2(it2, 2147483664L, "header", "true");
        insertTable2(it2, 2147483664L, "EnvelopSortingValue", "BE1180___testOlivier004-0008___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483664L, "GroupedWith", "");
        insertTable2(it2, 2147483664L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483664L, "DiversionReason", "");
        insertTable2(it2, 2147483664L, "Pliable", "true");
        insertTable2(it2, 2147483664L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483664L, "country", "BE");
        insertTable2(it2, 2147483664L, "AddressG1", "testOlivier004-0008");
        insertTable2(it2, 2147483664L, "MentionCode", "");
        insertTable2(it2, 2147483664L, "postCode", "1180");
        insertTable2(it2, 2147483664L, "SC5", "=");
        insertTable2(it2, 2147483664L, "Branding", "1C");
        insertTable1(it1, 2147483665L);
        insertTable2(it2, 2147483665L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483665L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483665L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483665L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483665L, "DocumentSortingValues", "______21863__1__1");
        insertTable2(it2, 2147483665L, "ItemSeq", "1");
        insertTable2(it2, 2147483665L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483665L, "Plex", "S");
        insertTable2(it2, 2147483665L, "SC3", "=");
        insertTable2(it2, 2147483665L, "has_address", "true");
        insertTable2(it2, 2147483665L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483665L, "Language", "FR");
        insertTable2(it2, 2147483665L, "logo", "false");
        insertTable2(it2, 2147483665L, "SC4", "=");
        insertTable2(it2, 2147483665L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483665L, "AddressG6", "");
        insertTable2(it2, 2147483665L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483665L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483665L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483665L, "CommunicationOrderId", "21873");
        insertTable2(it2, 2147483665L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483665L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483665L, "SortPlan", "");
        insertTable2(it2, 2147483665L, "Enveloping", "N");
        insertTable2(it2, 2147483665L, "PostComponentId", "21863");
        insertTable2(it2, 2147483665L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483665L, "city", "Bruxelles");
        insertTable2(it2, 2147483665L, "AddressG7", "");
        insertTable2(it2, 2147483665L, "CopyMention", "");
        insertTable2(it2, 2147483665L, "AddressG8", "");
        insertTable2(it2, 2147483665L, "StapleNbr", "");
        insertTable2(it2, 2147483665L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483665L, "AddressG5", "");
        insertTable2(it2, 2147483665L, "header", "true");
        insertTable2(it2, 2147483665L, "EnvelopSortingValue", "BE1180___testOlivier005-0009___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483665L, "GroupedWith", "");
        insertTable2(it2, 2147483665L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483665L, "DiversionReason", "001");
        insertTable2(it2, 2147483665L, "Pliable", "true");
        insertTable2(it2, 2147483665L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483665L, "country", "BE");
        insertTable2(it2, 2147483665L, "AddressG1", "testOlivier005-0009");
        insertTable2(it2, 2147483665L, "MentionCode", "");
        insertTable2(it2, 2147483665L, "postCode", "1180");
        insertTable2(it2, 2147483665L, "SC5", "=");
        insertTable2(it2, 2147483665L, "Branding", "1C");
        insertTable1(it1, 2147483666L);
        insertTable2(it2, 2147483666L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483666L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483666L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483666L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483666L, "DocumentSortingValues", "______21863__1__1");
        insertTable2(it2, 2147483666L, "ItemSeq", "1");
        insertTable2(it2, 2147483666L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483666L, "Plex", "S");
        insertTable2(it2, 2147483666L, "SC3", "=");
        insertTable2(it2, 2147483666L, "has_address", "true");
        insertTable2(it2, 2147483666L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483666L, "Language", "FR");
        insertTable2(it2, 2147483666L, "logo", "false");
        insertTable2(it2, 2147483666L, "SC4", "=");
        insertTable2(it2, 2147483666L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483666L, "AddressG6", "");
        insertTable2(it2, 2147483666L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483666L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483666L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483666L, "CommunicationOrderId", "21873");
        insertTable2(it2, 2147483666L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483666L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483666L, "SortPlan", "");
        insertTable2(it2, 2147483666L, "Enveloping", "N");
        insertTable2(it2, 2147483666L, "PostComponentId", "21863");
        insertTable2(it2, 2147483666L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483666L, "city", "Bruxelles");
        insertTable2(it2, 2147483666L, "AddressG7", "");
        insertTable2(it2, 2147483666L, "CopyMention", "");
        insertTable2(it2, 2147483666L, "AddressG8", "");
        insertTable2(it2, 2147483666L, "StapleNbr", "");
        insertTable2(it2, 2147483666L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483666L, "AddressG5", "");
        insertTable2(it2, 2147483666L, "header", "true");
        insertTable2(it2, 2147483666L, "EnvelopSortingValue", "BE1180___testOlivier005-0009___________Boulevard du Souverain, 23____");
        insertTable2(it2, 2147483666L, "GroupedWith", "");
        insertTable2(it2, 2147483666L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483666L, "DiversionReason", "001");
        insertTable2(it2, 2147483666L, "Pliable", "true");
        insertTable2(it2, 2147483666L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483666L, "country", "BE");
        insertTable2(it2, 2147483666L, "AddressG1", "testOlivier005-0009");
        insertTable2(it2, 2147483666L, "MentionCode", "");
        insertTable2(it2, 2147483666L, "postCode", "1180");
        insertTable2(it2, 2147483666L, "SC5", "=");
        insertTable2(it2, 2147483666L, "Branding", "1C");
        insertTable1(it1, 2147483667L);
        insertTable2(it2, 2147483667L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483667L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483667L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483667L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483667L, "DocumentSortingValues", "______21864__1__1");
        insertTable2(it2, 2147483667L, "ItemSeq", "1");
        insertTable2(it2, 2147483667L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483667L, "Plex", "S");
        insertTable2(it2, 2147483667L, "SC3", "=");
        insertTable2(it2, 2147483667L, "has_address", "true");
        insertTable2(it2, 2147483667L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483667L, "Language", "FR");
        insertTable2(it2, 2147483667L, "logo", "false");
        insertTable2(it2, 2147483667L, "SC4", "=");
        insertTable2(it2, 2147483667L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483667L, "AddressG6", "");
        insertTable2(it2, 2147483667L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483667L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483667L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483667L, "CommunicationOrderId", "21874");
        insertTable2(it2, 2147483667L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483667L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483667L, "SortPlan", "");
        insertTable2(it2, 2147483667L, "Enveloping", "N");
        insertTable2(it2, 2147483667L, "PostComponentId", "21864");
        insertTable2(it2, 2147483667L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483667L, "city", "Bruxelles");
        insertTable2(it2, 2147483667L, "AddressG7", "");
        insertTable2(it2, 2147483667L, "CopyMention", "");
        insertTable2(it2, 2147483667L, "AddressG8", "");
        insertTable2(it2, 2147483667L, "StapleNbr", "");
        insertTable2(it2, 2147483667L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483667L, "AddressG5", "");
        insertTable2(it2, 2147483667L, "header", "true");
        insertTable2(it2, 2147483667L, "EnvelopSortingValue", "BE1180___testOlivier005-0010___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483667L, "GroupedWith", "");
        insertTable2(it2, 2147483667L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483667L, "DiversionReason", "001");
        insertTable2(it2, 2147483667L, "Pliable", "true");
        insertTable2(it2, 2147483667L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483667L, "country", "BE");
        insertTable2(it2, 2147483667L, "AddressG1", "testOlivier005-0010");
        insertTable2(it2, 2147483667L, "MentionCode", "");
        insertTable2(it2, 2147483667L, "postCode", "1180");
        insertTable2(it2, 2147483667L, "SC5", "=");
        insertTable2(it2, 2147483667L, "Branding", "1C");
        insertTable1(it1, 2147483668L);
        insertTable2(it2, 2147483668L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483668L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483668L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483668L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483668L, "DocumentSortingValues", "______21864__1__1");
        insertTable2(it2, 2147483668L, "ItemSeq", "1");
        insertTable2(it2, 2147483668L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483668L, "Plex", "S");
        insertTable2(it2, 2147483668L, "SC3", "=");
        insertTable2(it2, 2147483668L, "has_address", "true");
        insertTable2(it2, 2147483668L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483668L, "Language", "FR");
        insertTable2(it2, 2147483668L, "logo", "false");
        insertTable2(it2, 2147483668L, "SC4", "=");
        insertTable2(it2, 2147483668L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483668L, "AddressG6", "");
        insertTable2(it2, 2147483668L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483668L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483668L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483668L, "CommunicationOrderId", "21874");
        insertTable2(it2, 2147483668L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483668L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483668L, "SortPlan", "");
        insertTable2(it2, 2147483668L, "Enveloping", "N");
        insertTable2(it2, 2147483668L, "PostComponentId", "21864");
        insertTable2(it2, 2147483668L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483668L, "city", "Bruxelles");
        insertTable2(it2, 2147483668L, "AddressG7", "");
        insertTable2(it2, 2147483668L, "CopyMention", "");
        insertTable2(it2, 2147483668L, "AddressG8", "");
        insertTable2(it2, 2147483668L, "StapleNbr", "");
        insertTable2(it2, 2147483668L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483668L, "AddressG5", "");
        insertTable2(it2, 2147483668L, "header", "true");
        insertTable2(it2, 2147483668L, "EnvelopSortingValue", "BE1180___testOlivier005-0010___________Boulevard du Souverain, 23____");
        insertTable2(it2, 2147483668L, "GroupedWith", "");
        insertTable2(it2, 2147483668L, "AddressG2", "Boulevard du Souverain, 23");
        insertTable2(it2, 2147483668L, "DiversionReason", "001");
        insertTable2(it2, 2147483668L, "Pliable", "true");
        insertTable2(it2, 2147483668L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483668L, "country", "BE");
        insertTable2(it2, 2147483668L, "AddressG1", "testOlivier005-0010");
        insertTable2(it2, 2147483668L, "MentionCode", "");
        insertTable2(it2, 2147483668L, "postCode", "1180");
        insertTable2(it2, 2147483668L, "SC5", "=");
        insertTable2(it2, 2147483668L, "Branding", "1C");
        insertTable1(it1, 2147483669L);
        insertTable2(it2, 2147483669L, "SC2", "=");
        insertTable2(it2, 2147483669L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483669L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483669L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483669L, "DocumentSortingValues", "______21865__1__1");
        insertTable2(it2, 2147483669L, "ItemSeq", "1");
        insertTable2(it2, 2147483669L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483669L, "Plex", "S");
        insertTable2(it2, 2147483669L, "SC3", "=");
        insertTable2(it2, 2147483669L, "has_address", "true");
        insertTable2(it2, 2147483669L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483669L, "Language", "FR");
        insertTable2(it2, 2147483669L, "logo", "false");
        insertTable2(it2, 2147483669L, "SC4", "=");
        insertTable2(it2, 2147483669L, "InternalAddress", "");
        insertTable2(it2, 2147483669L, "AddressG6", "");
        insertTable2(it2, 2147483669L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483669L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483669L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483669L, "CommunicationOrderId", "21875");
        insertTable2(it2, 2147483669L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483669L, "location", "rue du broker");
        insertTable2(it2, 2147483669L, "SortPlan", "");
        insertTable2(it2, 2147483669L, "Enveloping", "C");
        insertTable2(it2, 2147483669L, "PostComponentId", "21865");
        insertTable2(it2, 2147483669L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483669L, "city", "Broker ville");
        insertTable2(it2, 2147483669L, "AddressG7", "");
        insertTable2(it2, 2147483669L, "CopyMention", "");
        insertTable2(it2, 2147483669L, "AddressG8", "");
        insertTable2(it2, 2147483669L, "StapleNbr", "");
        insertTable2(it2, 2147483669L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483669L, "AddressG5", "");
        insertTable2(it2, 2147483669L, "header", "true");
        insertTable2(it2, 2147483669L, "EnvelopSortingValue", "BE1000___testOlivier006-0011___________rue du broker_________________");
        insertTable2(it2, 2147483669L, "GroupedWith", "");
        insertTable2(it2, 2147483669L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483669L, "DiversionReason", "");
        insertTable2(it2, 2147483669L, "Pliable", "true");
        insertTable2(it2, 2147483669L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483669L, "country", "BE");
        insertTable2(it2, 2147483669L, "AddressG1", "testOlivier006-0011");
        insertTable2(it2, 2147483669L, "MentionCode", "");
        insertTable2(it2, 2147483669L, "postCode", "1000");
        insertTable2(it2, 2147483669L, "SC5", "=");
        insertTable2(it2, 2147483669L, "Branding", "1C");
        insertTable1(it1, 2147483670L);
        insertTable2(it2, 2147483670L, "SC2", "=");
        insertTable2(it2, 2147483670L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483670L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483670L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483670L, "DocumentSortingValues", "______21865__1__1");
        insertTable2(it2, 2147483670L, "ItemSeq", "1");
        insertTable2(it2, 2147483670L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483670L, "Plex", "S");
        insertTable2(it2, 2147483670L, "SC3", "=");
        insertTable2(it2, 2147483670L, "has_address", "true");
        insertTable2(it2, 2147483670L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483670L, "Language", "FR");
        insertTable2(it2, 2147483670L, "logo", "false");
        insertTable2(it2, 2147483670L, "SC4", "=");
        insertTable2(it2, 2147483670L, "InternalAddress", "");
        insertTable2(it2, 2147483670L, "AddressG6", "");
        insertTable2(it2, 2147483670L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483670L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483670L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483670L, "CommunicationOrderId", "21875");
        insertTable2(it2, 2147483670L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483670L, "location", "rue du broker");
        insertTable2(it2, 2147483670L, "SortPlan", "");
        insertTable2(it2, 2147483670L, "Enveloping", "C");
        insertTable2(it2, 2147483670L, "PostComponentId", "21865");
        insertTable2(it2, 2147483670L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483670L, "city", "Broker ville");
        insertTable2(it2, 2147483670L, "AddressG7", "");
        insertTable2(it2, 2147483670L, "CopyMention", "");
        insertTable2(it2, 2147483670L, "AddressG8", "");
        insertTable2(it2, 2147483670L, "StapleNbr", "");
        insertTable2(it2, 2147483670L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483670L, "AddressG5", "");
        insertTable2(it2, 2147483670L, "header", "true");
        insertTable2(it2, 2147483670L, "EnvelopSortingValue", "BE1000___testOlivier006-0011___________rue dubroker_________________");
        insertTable2(it2, 2147483670L, "GroupedWith", "");
        insertTable2(it2, 2147483670L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483670L, "DiversionReason", "");
        insertTable2(it2, 2147483670L, "Pliable", "true");
        insertTable2(it2, 2147483670L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483670L, "country", "BE");
        insertTable2(it2, 2147483670L, "AddressG1", "testOlivier006-0011");
        insertTable2(it2, 2147483670L, "MentionCode", "");
        insertTable2(it2, 2147483670L, "postCode", "1000");
        insertTable2(it2, 2147483670L, "SC5", "=");
        insertTable2(it2, 2147483670L, "Branding", "1C");
        insertTable1(it1, 2147483671L);
        insertTable2(it2, 2147483671L, "SC2", "=");
        insertTable2(it2, 2147483671L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483671L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483671L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483671L, "DocumentSortingValues", "______21866__1__1");
        insertTable2(it2, 2147483671L, "ItemSeq", "1");
        insertTable2(it2, 2147483671L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483671L, "Plex", "S");
        insertTable2(it2, 2147483671L, "SC3", "=");
        insertTable2(it2, 2147483671L, "has_address", "true");
        insertTable2(it2, 2147483671L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483671L, "Language", "FR");
        insertTable2(it2, 2147483671L, "logo", "false");
        insertTable2(it2, 2147483671L, "SC4", "=");
        insertTable2(it2, 2147483671L, "InternalAddress", "");
        insertTable2(it2, 2147483671L, "AddressG6", "");
        insertTable2(it2, 2147483671L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483671L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483671L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483671L, "CommunicationOrderId", "21876");
        insertTable2(it2, 2147483671L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483671L, "location", "rue du broker");
        insertTable2(it2, 2147483671L, "SortPlan", "");
        insertTable2(it2, 2147483671L, "Enveloping", "C");
        insertTable2(it2, 2147483671L, "PostComponentId", "21866");
        insertTable2(it2, 2147483671L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483671L, "city", "Broker ville");
        insertTable2(it2, 2147483671L, "AddressG7", "");
        insertTable2(it2, 2147483671L, "CopyMention", "");
        insertTable2(it2, 2147483671L, "AddressG8", "");
        insertTable2(it2, 2147483671L, "StapleNbr", "");
        insertTable2(it2, 2147483671L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483671L, "AddressG5", "");
        insertTable2(it2, 2147483671L, "header", "true");
        insertTable2(it2, 2147483671L, "EnvelopSortingValue", "BE1000___testOlivier006-0012___________rue du broker_________________");
        insertTable2(it2, 2147483671L, "GroupedWith", "");
        insertTable2(it2, 2147483671L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483671L, "DiversionReason", "");
        insertTable2(it2, 2147483671L, "Pliable", "true");
        insertTable2(it2, 2147483671L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483671L, "country", "BE");
        insertTable2(it2, 2147483671L, "AddressG1", "testOlivier006-0012");
        insertTable2(it2, 2147483671L, "MentionCode", "");
        insertTable2(it2, 2147483671L, "postCode", "1000");
        insertTable2(it2, 2147483671L, "SC5", "=");
        insertTable2(it2, 2147483671L, "Branding", "1C");
        insertTable1(it1, 2147483672L);
        insertTable2(it2, 2147483672L, "SC2", "=");
        insertTable2(it2, 2147483672L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483672L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483672L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483672L, "DocumentSortingValues", "______21866__1__1");
        insertTable2(it2, 2147483672L, "ItemSeq", "1");
        insertTable2(it2, 2147483672L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483672L, "Plex", "S");
        insertTable2(it2, 2147483672L, "SC3", "=");
        insertTable2(it2, 2147483672L, "has_address", "true");
        insertTable2(it2, 2147483672L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483672L, "Language", "FR");
        insertTable2(it2, 2147483672L, "logo", "false");
        insertTable2(it2, 2147483672L, "SC4", "=");
        insertTable2(it2, 2147483672L, "InternalAddress", "");
        insertTable2(it2, 2147483672L, "AddressG6", "");
        insertTable2(it2, 2147483672L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483672L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483672L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483672L, "CommunicationOrderId", "21876");
        insertTable2(it2, 2147483672L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483672L, "location", "rue du broker");
        insertTable2(it2, 2147483672L, "SortPlan", "");
        insertTable2(it2, 2147483672L, "Enveloping", "C");
        insertTable2(it2, 2147483672L, "PostComponentId", "21866");
        insertTable2(it2, 2147483672L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483672L, "city", "Broker ville");
        insertTable2(it2, 2147483672L, "AddressG7", "");
        insertTable2(it2, 2147483672L, "CopyMention", "");
        insertTable2(it2, 2147483672L, "AddressG8", "");
        insertTable2(it2, 2147483672L, "StapleNbr", "");
        insertTable2(it2, 2147483672L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483672L, "AddressG5", "");
        insertTable2(it2, 2147483672L, "header", "true");
        insertTable2(it2, 2147483672L, "EnvelopSortingValue", "BE1000___testOlivier006-0012___________rue dubroker_________________");
        insertTable2(it2, 2147483672L, "GroupedWith", "");
        insertTable2(it2, 2147483672L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483672L, "DiversionReason", "");
        insertTable2(it2, 2147483672L, "Pliable", "true");
        insertTable2(it2, 2147483672L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483672L, "country", "BE");
        insertTable2(it2, 2147483672L, "AddressG1", "testOlivier006-0012");
        insertTable2(it2, 2147483672L, "MentionCode", "");
        insertTable2(it2, 2147483672L, "postCode", "1000");
        insertTable2(it2, 2147483672L, "SC5", "=");
        insertTable2(it2, 2147483672L, "Branding", "1C");
        insertTable1(it1, 2147483673L);
        insertTable2(it2, 2147483673L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483673L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483673L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483673L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483673L, "DocumentSortingValues", "______21867__1__1");
        insertTable2(it2, 2147483673L, "ItemSeq", "1");
        insertTable2(it2, 2147483673L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483673L, "Plex", "S");
        insertTable2(it2, 2147483673L, "SC3", "=");
        insertTable2(it2, 2147483673L, "has_address", "true");
        insertTable2(it2, 2147483673L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483673L, "Language", "FR");
        insertTable2(it2, 2147483673L, "logo", "false");
        insertTable2(it2, 2147483673L, "SC4", "=");
        insertTable2(it2, 2147483673L, "InternalAddress", "");
        insertTable2(it2, 2147483673L, "AddressG6", "");
        insertTable2(it2, 2147483673L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483673L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483673L, "SC1", "Country=BE");
        insertTable2(it2, 2147483673L, "CommunicationOrderId", "21877");
        insertTable2(it2, 2147483673L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483673L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483673L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483673L, "Enveloping", "C");
        insertTable2(it2, 2147483673L, "PostComponentId", "21867");
        insertTable2(it2, 2147483673L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483673L, "city", "Bruxelles");
        insertTable2(it2, 2147483673L, "AddressG7", "");
        insertTable2(it2, 2147483673L, "CopyMention", "");
        insertTable2(it2, 2147483673L, "AddressG8", "");
        insertTable2(it2, 2147483673L, "StapleNbr", "");
        insertTable2(it2, 2147483673L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483673L, "AddressG5", "");
        insertTable2(it2, 2147483673L, "header", "true");
        insertTable2(it2, 2147483673L, "EnvelopSortingValue", "BE1180___testOlivier007-0013___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483673L, "GroupedWith", "");
        insertTable2(it2, 2147483673L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483673L, "DiversionReason", "");
        insertTable2(it2, 2147483673L, "Pliable", "true");
        insertTable2(it2, 2147483673L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483673L, "country", "BE");
        insertTable2(it2, 2147483673L, "AddressG1", "testOlivier007-0013");
        insertTable2(it2, 2147483673L, "MentionCode", "");
        insertTable2(it2, 2147483673L, "postCode", "1180");
        insertTable2(it2, 2147483673L, "SC5", "=");
        insertTable2(it2, 2147483673L, "Branding", "1C");
        insertTable1(it1, 2147483674L);
        insertTable2(it2, 2147483674L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483674L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483674L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483674L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483674L, "DocumentSortingValues", "______21867__1__1");
        insertTable2(it2, 2147483674L, "ItemSeq", "1");
        insertTable2(it2, 2147483674L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483674L, "Plex", "S");
        insertTable2(it2, 2147483674L, "SC3", "=");
        insertTable2(it2, 2147483674L, "has_address", "true");
        insertTable2(it2, 2147483674L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483674L, "Language", "FR");
        insertTable2(it2, 2147483674L, "logo", "false");
        insertTable2(it2, 2147483674L, "SC4", "=");
        insertTable2(it2, 2147483674L, "InternalAddress", "");
        insertTable2(it2, 2147483674L, "AddressG6", "");
        insertTable2(it2, 2147483674L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483674L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483674L, "SC1", "Country=BE");
        insertTable2(it2, 2147483674L, "CommunicationOrderId", "21877");
        insertTable2(it2, 2147483674L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483674L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483674L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483674L, "Enveloping", "C");
        insertTable2(it2, 2147483674L, "PostComponentId", "21867");
        insertTable2(it2, 2147483674L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483674L, "city", "Bruxelles");
        insertTable2(it2, 2147483674L, "AddressG7", "");
        insertTable2(it2, 2147483674L, "CopyMention", "");
        insertTable2(it2, 2147483674L, "AddressG8", "");
        insertTable2(it2, 2147483674L, "StapleNbr", "");
        insertTable2(it2, 2147483674L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483674L, "AddressG5", "");
        insertTable2(it2, 2147483674L, "header", "true");
        insertTable2(it2, 2147483674L, "EnvelopSortingValue", "BE1180___testOlivier007-0013___________Boulevard du Souverain, 23____");
        insertTable2(it2, 2147483674L, "GroupedWith", "");
        insertTable2(it2, 2147483674L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483674L, "DiversionReason", "");
        insertTable2(it2, 2147483674L, "Pliable", "true");
        insertTable2(it2, 2147483674L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483674L, "country", "BE");
        insertTable2(it2, 2147483674L, "AddressG1", "testOlivier007-0013");
        insertTable2(it2, 2147483674L, "MentionCode", "");
        insertTable2(it2, 2147483674L, "postCode", "1180");
        insertTable2(it2, 2147483674L, "SC5", "=");
        insertTable2(it2, 2147483674L, "Branding", "1C");
        insertTable1(it1, 2147483675L);
        insertTable2(it2, 2147483675L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483675L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483675L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483675L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483675L, "DocumentSortingValues", "______21868__1__1");
        insertTable2(it2, 2147483675L, "ItemSeq", "1");
        insertTable2(it2, 2147483675L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483675L, "Plex", "S");
        insertTable2(it2, 2147483675L, "SC3", "=");
        insertTable2(it2, 2147483675L, "has_address", "true");
        insertTable2(it2, 2147483675L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483675L, "Language", "FR");
        insertTable2(it2, 2147483675L, "logo", "false");
        insertTable2(it2, 2147483675L, "SC4", "=");
        insertTable2(it2, 2147483675L, "InternalAddress", "");
        insertTable2(it2, 2147483675L, "AddressG6", "");
        insertTable2(it2, 2147483675L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483675L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483675L, "SC1", "Country=BE");
        insertTable2(it2, 2147483675L, "CommunicationOrderId", "21878");
        insertTable2(it2, 2147483675L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483675L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483675L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483675L, "Enveloping", "C");
        insertTable2(it2, 2147483675L, "PostComponentId", "21868");
        insertTable2(it2, 2147483675L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483675L, "city", "Bruxelles");
        insertTable2(it2, 2147483675L, "AddressG7", "");
        insertTable2(it2, 2147483675L, "CopyMention", "");
        insertTable2(it2, 2147483675L, "AddressG8", "");
        insertTable2(it2, 2147483675L, "StapleNbr", "");
        insertTable2(it2, 2147483675L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483675L, "AddressG5", "");
        insertTable2(it2, 2147483675L, "header", "true");
        insertTable2(it2, 2147483675L, "EnvelopSortingValue", "BE1180___testOlivier007-0014___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483675L, "GroupedWith", "");
        insertTable2(it2, 2147483675L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483675L, "DiversionReason", "");
        insertTable2(it2, 2147483675L, "Pliable", "true");
        insertTable2(it2, 2147483675L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483675L, "country", "BE");
        insertTable2(it2, 2147483675L, "AddressG1", "testOlivier007-0014");
        insertTable2(it2, 2147483675L, "MentionCode", "");
        insertTable2(it2, 2147483675L, "postCode", "1180");
        insertTable2(it2, 2147483675L, "SC5", "=");
        insertTable2(it2, 2147483675L, "Branding", "1C");
        insertTable1(it1, 2147483676L);
        insertTable2(it2, 2147483676L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483676L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483676L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483676L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483676L, "DocumentSortingValues", "______21868__1__1");
        insertTable2(it2, 2147483676L, "ItemSeq", "1");
        insertTable2(it2, 2147483676L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483676L, "Plex", "S");
        insertTable2(it2, 2147483676L, "SC3", "=");
        insertTable2(it2, 2147483676L, "has_address", "true");
        insertTable2(it2, 2147483676L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483676L, "Language", "FR");
        insertTable2(it2, 2147483676L, "logo", "false");
        insertTable2(it2, 2147483676L, "SC4", "=");
        insertTable2(it2, 2147483676L, "InternalAddress", "");
        insertTable2(it2, 2147483676L, "AddressG6", "");
        insertTable2(it2, 2147483676L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483676L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483676L, "SC1", "Country=BE");
        insertTable2(it2, 2147483676L, "CommunicationOrderId", "21878");
        insertTable2(it2, 2147483676L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483676L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483676L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483676L, "Enveloping", "C");
        insertTable2(it2, 2147483676L, "PostComponentId", "21868");
        insertTable2(it2, 2147483676L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483676L, "city", "Bruxelles");
        insertTable2(it2, 2147483676L, "AddressG7", "");
        insertTable2(it2, 2147483676L, "CopyMention", "");
        insertTable2(it2, 2147483676L, "AddressG8", "");
        insertTable2(it2, 2147483676L, "StapleNbr", "");
        insertTable2(it2, 2147483676L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483676L, "AddressG5", "");
        insertTable2(it2, 2147483676L, "header", "true");
        insertTable2(it2, 2147483676L, "EnvelopSortingValue", "BE1180___testOlivier007-0014___________Boulevard du Souverain, 23____");
        insertTable2(it2, 2147483676L, "GroupedWith", "");
        insertTable2(it2, 2147483676L, "AddressG2", "Boulevard du Souverain, 23");
        insertTable2(it2, 2147483676L, "DiversionReason", "");
        insertTable2(it2, 2147483676L, "Pliable", "true");
        insertTable2(it2, 2147483676L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483676L, "country", "BE");
        insertTable2(it2, 2147483676L, "AddressG1", "testOlivier007-0014");
        insertTable2(it2, 2147483676L, "MentionCode", "");
        insertTable2(it2, 2147483676L, "postCode", "1180");
        insertTable2(it2, 2147483676L, "SC5", "=");
        insertTable2(it2, 2147483676L, "Branding", "1C");
        insertTable1(it1, 2147483677L);
        insertTable2(it2, 2147483677L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483677L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483677L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483677L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483677L, "DocumentSortingValues", "______21869__1__1");
        insertTable2(it2, 2147483677L, "ItemSeq", "1");
        insertTable2(it2, 2147483677L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483677L, "Plex", "S");
        insertTable2(it2, 2147483677L, "SC3", "=");
        insertTable2(it2, 2147483677L, "has_address", "true");
        insertTable2(it2, 2147483677L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483677L, "Language", "FR");
        insertTable2(it2, 2147483677L, "logo", "false");
        insertTable2(it2, 2147483677L, "SC4", "=");
        insertTable2(it2, 2147483677L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483677L, "AddressG6", "");
        insertTable2(it2, 2147483677L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483677L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483677L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483677L, "CommunicationOrderId", "21879");
        insertTable2(it2, 2147483677L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483677L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483677L, "SortPlan", "");
        insertTable2(it2, 2147483677L, "Enveloping", "N");
        insertTable2(it2, 2147483677L, "PostComponentId", "21869");
        insertTable2(it2, 2147483677L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483677L, "city", "Bruxelles");
        insertTable2(it2, 2147483677L, "AddressG7", "");
        insertTable2(it2, 2147483677L, "CopyMention", "");
        insertTable2(it2, 2147483677L, "AddressG8", "");
        insertTable2(it2, 2147483677L, "StapleNbr", "");
        insertTable2(it2, 2147483677L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483677L, "AddressG5", "");
        insertTable2(it2, 2147483677L, "header", "true");
        insertTable2(it2, 2147483677L, "EnvelopSortingValue", "BE1180___testOlivier008-0015___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483677L, "GroupedWith", "");
        insertTable2(it2, 2147483677L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483677L, "DiversionReason", "001");
        insertTable2(it2, 2147483677L, "Pliable", "true");
        insertTable2(it2, 2147483677L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483677L, "country", "BE");
        insertTable2(it2, 2147483677L, "AddressG1", "testOlivier008-0015");
        insertTable2(it2, 2147483677L, "MentionCode", "");
        insertTable2(it2, 2147483677L, "postCode", "1180");
        insertTable2(it2, 2147483677L, "SC5", "=");
        insertTable2(it2, 2147483677L, "Branding", "1C");
        insertTable1(it1, 2147483678L);
        insertTable2(it2, 2147483678L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483678L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483678L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483678L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483678L, "DocumentSortingValues", "______21869__1__1");
        insertTable2(it2, 2147483678L, "ItemSeq", "1");
        insertTable2(it2, 2147483678L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483678L, "Plex", "S");
        insertTable2(it2, 2147483678L, "SC3", "=");
        insertTable2(it2, 2147483678L, "has_address", "true");
        insertTable2(it2, 2147483678L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483678L, "Language", "FR");
        insertTable2(it2, 2147483678L, "logo", "false");
        insertTable2(it2, 2147483678L, "SC4", "=");
        insertTable2(it2, 2147483678L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483678L, "AddressG6", "");
        insertTable2(it2, 2147483678L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483678L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483678L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483678L, "CommunicationOrderId", "21879");
        insertTable2(it2, 2147483678L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483678L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483678L, "SortPlan", "");
        insertTable2(it2, 2147483678L, "Enveloping", "N");
        insertTable2(it2, 2147483678L, "PostComponentId", "21869");
        insertTable2(it2, 2147483678L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483678L, "city", "Bruxelles");
        insertTable2(it2, 2147483678L, "AddressG7", "");
        insertTable2(it2, 2147483678L, "CopyMention", "");
        insertTable2(it2, 2147483678L, "AddressG8", "");
        insertTable2(it2, 2147483678L, "StapleNbr", "");
        insertTable2(it2, 2147483678L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483678L, "AddressG5", "");
        insertTable2(it2, 2147483678L, "header", "true");
        insertTable2(it2, 2147483678L, "EnvelopSortingValue", "BE1180___testOlivier008-0015___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483678L, "GroupedWith", "");
        insertTable2(it2, 2147483678L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483678L, "DiversionReason", "001");
        insertTable2(it2, 2147483678L, "Pliable", "true");
        insertTable2(it2, 2147483678L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483678L, "country", "BE");
        insertTable2(it2, 2147483678L, "AddressG1", "testOlivier008-0015");
        insertTable2(it2, 2147483678L, "MentionCode", "");
        insertTable2(it2, 2147483678L, "postCode", "1180");
        insertTable2(it2, 2147483678L, "SC5", "=");
        insertTable2(it2, 2147483678L, "Branding", "1C");
        insertTable1(it1, 2147483679L);
        insertTable2(it2, 2147483679L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483679L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483679L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483679L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483679L, "DocumentSortingValues", "______21870__1__1");
        insertTable2(it2, 2147483679L, "ItemSeq", "1");
        insertTable2(it2, 2147483679L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483679L, "Plex", "S");
        insertTable2(it2, 2147483679L, "SC3", "=");
        insertTable2(it2, 2147483679L, "has_address", "true");
        insertTable2(it2, 2147483679L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483679L, "Language", "FR");
        insertTable2(it2, 2147483679L, "logo", "false");
        insertTable2(it2, 2147483679L, "SC4", "=");
        insertTable2(it2, 2147483679L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483679L, "AddressG6", "");
        insertTable2(it2, 2147483679L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483679L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483679L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483679L, "CommunicationOrderId", "21880");
        insertTable2(it2, 2147483679L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483679L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483679L, "SortPlan", "");
        insertTable2(it2, 2147483679L, "Enveloping", "N");
        insertTable2(it2, 2147483679L, "PostComponentId", "21870");
        insertTable2(it2, 2147483679L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483679L, "city", "Bruxelles");
        insertTable2(it2, 2147483679L, "AddressG7", "");
        insertTable2(it2, 2147483679L, "CopyMention", "");
        insertTable2(it2, 2147483679L, "AddressG8", "");
        insertTable2(it2, 2147483679L, "StapleNbr", "");
        insertTable2(it2, 2147483679L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483679L, "AddressG5", "");
        insertTable2(it2, 2147483679L, "header", "true");
        insertTable2(it2, 2147483679L, "EnvelopSortingValue", "BE1180___testOlivier008-0016___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483679L, "GroupedWith", "");
        insertTable2(it2, 2147483679L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483679L, "DiversionReason", "001");
        insertTable2(it2, 2147483679L, "Pliable", "true");
        insertTable2(it2, 2147483679L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483679L, "country", "BE");
        insertTable2(it2, 2147483679L, "AddressG1", "testOlivier008-0016");
        insertTable2(it2, 2147483679L, "MentionCode", "");
        insertTable2(it2, 2147483679L, "postCode", "1180");
        insertTable2(it2, 2147483679L, "SC5", "=");
        insertTable2(it2, 2147483679L, "Branding", "1C");
        insertTable1(it1, 2147483680L);
        insertTable2(it2, 2147483680L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483680L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483680L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483680L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483680L, "DocumentSortingValues", "______21870__1__1");
        insertTable2(it2, 2147483680L, "ItemSeq", "1");
        insertTable2(it2, 2147483680L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483680L, "Plex", "S");
        insertTable2(it2, 2147483680L, "SC3", "=");
        insertTable2(it2, 2147483680L, "has_address", "true");
        insertTable2(it2, 2147483680L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483680L, "Language", "FR");
        insertTable2(it2, 2147483680L, "logo", "false");
        insertTable2(it2, 2147483680L, "SC4", "=");
        insertTable2(it2, 2147483680L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483680L, "AddressG6", "");
        insertTable2(it2, 2147483680L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483680L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483680L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483680L, "CommunicationOrderId", "21880");
        insertTable2(it2, 2147483680L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483680L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483680L, "SortPlan", "");
        insertTable2(it2, 2147483680L, "Enveloping", "N");
        insertTable2(it2, 2147483680L, "PostComponentId", "21870");
        insertTable2(it2, 2147483680L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483680L, "city", "Bruxelles");
        insertTable2(it2, 2147483680L, "AddressG7", "");
        insertTable2(it2, 2147483680L, "CopyMention", "");
        insertTable2(it2, 2147483680L, "AddressG8", "");
        insertTable2(it2, 2147483680L, "StapleNbr", "");
        insertTable2(it2, 2147483680L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483680L, "AddressG5", "");
        insertTable2(it2, 2147483680L, "header", "true");
        insertTable2(it2, 2147483680L, "EnvelopSortingValue", "BE1180___testOlivier008-0016___________Boulevard du Souverain, 23____");
        insertTable2(it2, 2147483680L, "GroupedWith", "");
        insertTable2(it2, 2147483680L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483680L, "DiversionReason", "001");
        insertTable2(it2, 2147483680L, "Pliable", "true");
        insertTable2(it2, 2147483680L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483680L, "country", "BE");
        insertTable2(it2, 2147483680L, "AddressG1", "testOlivier008-0016");
        insertTable2(it2, 2147483680L, "MentionCode", "");
        insertTable2(it2, 2147483680L, "postCode", "1180");
        insertTable2(it2, 2147483680L, "SC5", "=");
        insertTable2(it2, 2147483680L, "Branding", "1C");
        insertTable1(it1, 2147483681L);
        insertTable2(it2, 2147483681L, "SC2", "=");
        insertTable2(it2, 2147483681L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483681L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483681L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483681L, "DocumentSortingValues", "______21871__1__1");
        insertTable2(it2, 2147483681L, "ItemSeq", "1");
        insertTable2(it2, 2147483681L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483681L, "Plex", "S");
        insertTable2(it2, 2147483681L, "SC3", "=");
        insertTable2(it2, 2147483681L, "has_address", "true");
        insertTable2(it2, 2147483681L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483681L, "Language", "FR");
        insertTable2(it2, 2147483681L, "logo", "false");
        insertTable2(it2, 2147483681L, "SC4", "=");
        insertTable2(it2, 2147483681L, "InternalAddress", "");
        insertTable2(it2, 2147483681L, "AddressG6", "");
        insertTable2(it2, 2147483681L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483681L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483681L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483681L, "CommunicationOrderId", "21881");
        insertTable2(it2, 2147483681L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483681L, "location", "rue du broker");
        insertTable2(it2, 2147483681L, "SortPlan", "");
        insertTable2(it2, 2147483681L, "Enveloping", "C");
        insertTable2(it2, 2147483681L, "PostComponentId", "21871");
        insertTable2(it2, 2147483681L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483681L, "city", "Broker ville");
        insertTable2(it2, 2147483681L, "AddressG7", "");
        insertTable2(it2, 2147483681L, "CopyMention", "");
        insertTable2(it2, 2147483681L, "AddressG8", "");
        insertTable2(it2, 2147483681L, "StapleNbr", "");
        insertTable2(it2, 2147483681L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483681L, "AddressG5", "");
        insertTable2(it2, 2147483681L, "header", "true");
        insertTable2(it2, 2147483681L, "EnvelopSortingValue", "BE1000___testOlivier009-0017___________rue du broker_________________");
        insertTable2(it2, 2147483681L, "GroupedWith", "");
        insertTable2(it2, 2147483681L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483681L, "DiversionReason", "");
        insertTable2(it2, 2147483681L, "Pliable", "true");
        insertTable2(it2, 2147483681L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483681L, "country", "BE");
        insertTable2(it2, 2147483681L, "AddressG1", "testOlivier009-0017");
        insertTable2(it2, 2147483681L, "MentionCode", "");
        insertTable2(it2, 2147483681L, "postCode", "1000");
        insertTable2(it2, 2147483681L, "SC5", "=");
        insertTable2(it2, 2147483681L, "Branding", "1C");
        insertTable1(it1, 2147483682L);
        insertTable2(it2, 2147483682L, "SC2", "=");
        insertTable2(it2, 2147483682L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483682L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483682L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483682L, "DocumentSortingValues", "______21871__1__1");
        insertTable2(it2, 2147483682L, "ItemSeq", "1");
        insertTable2(it2, 2147483682L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483682L, "Plex", "S");
        insertTable2(it2, 2147483682L, "SC3", "=");
        insertTable2(it2, 2147483682L, "has_address", "true");
        insertTable2(it2, 2147483682L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483682L, "Language", "FR");
        insertTable2(it2, 2147483682L, "logo", "false");
        insertTable2(it2, 2147483682L, "SC4", "=");
        insertTable2(it2, 2147483682L, "InternalAddress", "");
        insertTable2(it2, 2147483682L, "AddressG6", "");
        insertTable2(it2, 2147483682L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483682L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483682L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483682L, "CommunicationOrderId", "21881");
        insertTable2(it2, 2147483682L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483682L, "location", "rue du broker");
        insertTable2(it2, 2147483682L, "SortPlan", "");
        insertTable2(it2, 2147483682L, "Enveloping", "C");
        insertTable2(it2, 2147483682L, "PostComponentId", "21871");
        insertTable2(it2, 2147483682L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483682L, "city", "Broker ville");
        insertTable2(it2, 2147483682L, "AddressG7", "");
        insertTable2(it2, 2147483682L, "CopyMention", "");
        insertTable2(it2, 2147483682L, "AddressG8", "");
        insertTable2(it2, 2147483682L, "StapleNbr", "");
        insertTable2(it2, 2147483682L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483682L, "AddressG5", "");
        insertTable2(it2, 2147483682L, "header", "true");
        insertTable2(it2, 2147483682L, "EnvelopSortingValue", "BE1000___testOlivier009-0017___________rue dubroker_________________");
        insertTable2(it2, 2147483682L, "GroupedWith", "");
        insertTable2(it2, 2147483682L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483682L, "DiversionReason", "");
        insertTable2(it2, 2147483682L, "Pliable", "true");
        insertTable2(it2, 2147483682L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483682L, "country", "BE");
        insertTable2(it2, 2147483682L, "AddressG1", "testOlivier009-0017");
        insertTable2(it2, 2147483682L, "MentionCode", "");
        insertTable2(it2, 2147483682L, "postCode", "1000");
        insertTable2(it2, 2147483682L, "SC5", "=");
        insertTable2(it2, 2147483682L, "Branding", "1C");
        insertTable1(it1, 2147483683L);
        insertTable2(it2, 2147483683L, "SC2", "=");
        insertTable2(it2, 2147483683L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483683L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483683L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483683L, "DocumentSortingValues", "______21872__1__1");
        insertTable2(it2, 2147483683L, "ItemSeq", "1");
        insertTable2(it2, 2147483683L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483683L, "Plex", "S");
        insertTable2(it2, 2147483683L, "SC3", "=");
        insertTable2(it2, 2147483683L, "has_address", "true");
        insertTable2(it2, 2147483683L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483683L, "Language", "FR");
        insertTable2(it2, 2147483683L, "logo", "false");
        insertTable2(it2, 2147483683L, "SC4", "=");
        insertTable2(it2, 2147483683L, "InternalAddress", "");
        insertTable2(it2, 2147483683L, "AddressG6", "");
        insertTable2(it2, 2147483683L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483683L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483683L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483683L, "CommunicationOrderId", "21882");
        insertTable2(it2, 2147483683L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483683L, "location", "rue du broker");
        insertTable2(it2, 2147483683L, "SortPlan", "");
        insertTable2(it2, 2147483683L, "Enveloping", "C");
        insertTable2(it2, 2147483683L, "PostComponentId", "21872");
        insertTable2(it2, 2147483683L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483683L, "city", "Broker ville");
        insertTable2(it2, 2147483683L, "AddressG7", "");
        insertTable2(it2, 2147483683L, "CopyMention", "");
        insertTable2(it2, 2147483683L, "AddressG8", "");
        insertTable2(it2, 2147483683L, "StapleNbr", "");
        insertTable2(it2, 2147483683L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483683L, "AddressG5", "");
        insertTable2(it2, 2147483683L, "header", "true");
        insertTable2(it2, 2147483683L, "EnvelopSortingValue", "BE1000___testOlivier009-0018___________rue du broker_________________");
        insertTable2(it2, 2147483683L, "GroupedWith", "");
        insertTable2(it2, 2147483683L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483683L, "DiversionReason", "");
        insertTable2(it2, 2147483683L, "Pliable", "true");
        insertTable2(it2, 2147483683L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483683L, "country", "BE");
        insertTable2(it2, 2147483683L, "AddressG1", "testOlivier009-0018");
        insertTable2(it2, 2147483683L, "MentionCode", "");
        insertTable2(it2, 2147483683L, "postCode", "1000");
        insertTable2(it2, 2147483683L, "SC5", "=");
        insertTable2(it2, 2147483683L, "Branding", "1C");
        insertTable1(it1, 2147483684L);
        insertTable2(it2, 2147483684L, "SC2", "=");
        insertTable2(it2, 2147483684L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483684L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483684L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483684L, "DocumentSortingValues", "______21872__1__1");
        insertTable2(it2, 2147483684L, "ItemSeq", "1");
        insertTable2(it2, 2147483684L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483684L, "Plex", "S");
        insertTable2(it2, 2147483684L, "SC3", "=");
        insertTable2(it2, 2147483684L, "has_address", "true");
        insertTable2(it2, 2147483684L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483684L, "Language", "FR");
        insertTable2(it2, 2147483684L, "logo", "false");
        insertTable2(it2, 2147483684L, "SC4", "=");
        insertTable2(it2, 2147483684L, "InternalAddress", "");
        insertTable2(it2, 2147483684L, "AddressG6", "");
        insertTable2(it2, 2147483684L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483684L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483684L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483684L, "CommunicationOrderId", "21882");
        insertTable2(it2, 2147483684L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483684L, "location", "rue du broker");
        insertTable2(it2, 2147483684L, "SortPlan", "");
        insertTable2(it2, 2147483684L, "Enveloping", "C");
        insertTable2(it2, 2147483684L, "PostComponentId", "21872");
        insertTable2(it2, 2147483684L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483684L, "city", "Broker ville");
        insertTable2(it2, 2147483684L, "AddressG7", "");
        insertTable2(it2, 2147483684L, "CopyMention", "");
        insertTable2(it2, 2147483684L, "AddressG8", "");
        insertTable2(it2, 2147483684L, "StapleNbr", "");
        insertTable2(it2, 2147483684L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483684L, "AddressG5", "");
        insertTable2(it2, 2147483684L, "header", "true");
        insertTable2(it2, 2147483684L, "EnvelopSortingValue", "BE1000___testOlivier009-0018___________rue du broker_________________");
        insertTable2(it2, 2147483684L, "GroupedWith", "");
        insertTable2(it2, 2147483684L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483684L, "DiversionReason", "");
        insertTable2(it2, 2147483684L, "Pliable", "true");
        insertTable2(it2, 2147483684L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483684L, "country", "BE");
        insertTable2(it2, 2147483684L, "AddressG1", "testOlivier009-0018");
        insertTable2(it2, 2147483684L, "MentionCode", "");
        insertTable2(it2, 2147483684L, "postCode", "1000");
        insertTable2(it2, 2147483684L, "SC5", "=");
        insertTable2(it2, 2147483684L, "Branding", "1C");
        insertTable1(it1, 2147483685L);
        insertTable2(it2, 2147483685L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483685L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483685L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483685L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483685L, "DocumentSortingValues", "______21873__1__1");
        insertTable2(it2, 2147483685L, "ItemSeq", "1");
        insertTable2(it2, 2147483685L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483685L, "Plex", "S");
        insertTable2(it2, 2147483685L, "SC3", "=");
        insertTable2(it2, 2147483685L, "has_address", "true");
        insertTable2(it2, 2147483685L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483685L, "Language", "FR");
        insertTable2(it2, 2147483685L, "logo", "false");
        insertTable2(it2, 2147483685L, "SC4", "=");
        insertTable2(it2, 2147483685L, "InternalAddress", "");
        insertTable2(it2, 2147483685L, "AddressG6", "");
        insertTable2(it2, 2147483685L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483685L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483685L, "SC1", "Country=BE");
        insertTable2(it2, 2147483685L, "CommunicationOrderId", "21883");
        insertTable2(it2, 2147483685L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483685L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483685L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483685L, "Enveloping", "C");
        insertTable2(it2, 2147483685L, "PostComponentId", "21873");
        insertTable2(it2, 2147483685L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483685L, "city", "Bruxelles");
        insertTable2(it2, 2147483685L, "AddressG7", "");
        insertTable2(it2, 2147483685L, "CopyMention", "");
        insertTable2(it2, 2147483685L, "AddressG8", "");
        insertTable2(it2, 2147483685L, "StapleNbr", "");
        insertTable2(it2, 2147483685L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483685L, "AddressG5", "");
        insertTable2(it2, 2147483685L, "header", "true");
        insertTable2(it2, 2147483685L, "EnvelopSortingValue", "BE1180___testOlivier010-0019___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483685L, "GroupedWith", "");
        insertTable2(it2, 2147483685L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483685L, "DiversionReason", "");
        insertTable2(it2, 2147483685L, "Pliable", "true");
        insertTable2(it2, 2147483685L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483685L, "country", "BE");
        insertTable2(it2, 2147483685L, "AddressG1", "testOlivier010-0019");
        insertTable2(it2, 2147483685L, "MentionCode", "");
        insertTable2(it2, 2147483685L, "postCode", "1180");
        insertTable2(it2, 2147483685L, "SC5", "=");
        insertTable2(it2, 2147483685L, "Branding", "1C");
        insertTable1(it1, 2147483686L);
        insertTable2(it2, 2147483686L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483686L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483686L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483686L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483686L, "DocumentSortingValues", "______21873__1__1");
        insertTable2(it2, 2147483686L, "ItemSeq", "1");
        insertTable2(it2, 2147483686L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483686L, "Plex", "S");
        insertTable2(it2, 2147483686L, "SC3", "=");
        insertTable2(it2, 2147483686L, "has_address", "true");
        insertTable2(it2, 2147483686L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483686L, "Language", "FR");
        insertTable2(it2, 2147483686L, "logo", "false");
        insertTable2(it2, 2147483686L, "SC4", "=");
        insertTable2(it2, 2147483686L, "InternalAddress", "");
        insertTable2(it2, 2147483686L, "AddressG6", "");
        insertTable2(it2, 2147483686L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483686L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483686L, "SC1", "Country=BE");
        insertTable2(it2, 2147483686L, "CommunicationOrderId", "21883");
        insertTable2(it2, 2147483686L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483686L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483686L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483686L, "Enveloping", "C");
        insertTable2(it2, 2147483686L, "PostComponentId", "21873");
        insertTable2(it2, 2147483686L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483686L, "city", "Bruxelles");
        insertTable2(it2, 2147483686L, "AddressG7", "");
        insertTable2(it2, 2147483686L, "CopyMention", "");
        insertTable2(it2, 2147483686L, "AddressG8", "");
        insertTable2(it2, 2147483686L, "StapleNbr", "");
        insertTable2(it2, 2147483686L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483686L, "AddressG5", "");
        insertTable2(it2, 2147483686L, "header", "true");
        insertTable2(it2, 2147483686L, "EnvelopSortingValue", "BE1180___testOlivier010-0019___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483686L, "GroupedWith", "");
        insertTable2(it2, 2147483686L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483686L, "DiversionReason", "");
        insertTable2(it2, 2147483686L, "Pliable", "true");
        insertTable2(it2, 2147483686L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483686L, "country", "BE");
        insertTable2(it2, 2147483686L, "AddressG1", "testOlivier010-0019");
        insertTable2(it2, 2147483686L, "MentionCode", "");
        insertTable2(it2, 2147483686L, "postCode", "1180");
        insertTable2(it2, 2147483686L, "SC5", "=");
        insertTable2(it2, 2147483686L, "Branding", "1C");
        insertTable1(it1, 2147483687L);
        insertTable2(it2, 2147483687L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483687L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483687L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483687L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483687L, "DocumentSortingValues", "______21874__1__1");
        insertTable2(it2, 2147483687L, "ItemSeq", "1");
        insertTable2(it2, 2147483687L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483687L, "Plex", "S");
        insertTable2(it2, 2147483687L, "SC3", "=");
        insertTable2(it2, 2147483687L, "has_address", "true");
        insertTable2(it2, 2147483687L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483687L, "Language", "FR");
        insertTable2(it2, 2147483687L, "logo", "false");
        insertTable2(it2, 2147483687L, "SC4", "=");
        insertTable2(it2, 2147483687L, "InternalAddress", "");
        insertTable2(it2, 2147483687L, "AddressG6", "");
        insertTable2(it2, 2147483687L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483687L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483687L, "SC1", "Country=BE");
        insertTable2(it2, 2147483687L, "CommunicationOrderId", "21884");
        insertTable2(it2, 2147483687L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483687L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483687L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483687L, "Enveloping", "C");
        insertTable2(it2, 2147483687L, "PostComponentId", "21874");
        insertTable2(it2, 2147483687L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483687L, "city", "Bruxelles");
        insertTable2(it2, 2147483687L, "AddressG7", "");
        insertTable2(it2, 2147483687L, "CopyMention", "");
        insertTable2(it2, 2147483687L, "AddressG8", "");
        insertTable2(it2, 2147483687L, "StapleNbr", "");
        insertTable2(it2, 2147483687L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483687L, "AddressG5", "");
        insertTable2(it2, 2147483687L, "header", "true");
        insertTable2(it2, 2147483687L, "EnvelopSortingValue", "BE1180___testOlivier010-0020___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483687L, "GroupedWith", "");
        insertTable2(it2, 2147483687L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483687L, "DiversionReason", "");
        insertTable2(it2, 2147483687L, "Pliable", "true");
        insertTable2(it2, 2147483687L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483687L, "country", "BE");
        insertTable2(it2, 2147483687L, "AddressG1", "testOlivier010-0020");
        insertTable2(it2, 2147483687L, "MentionCode", "");
        insertTable2(it2, 2147483687L, "postCode", "1180");
        insertTable2(it2, 2147483687L, "SC5", "=");
        insertTable2(it2, 2147483687L, "Branding", "1C");
        insertTable1(it1, 2147483688L);
        insertTable2(it2, 2147483688L, "SC2", "LaPosteSortPlan=B-W3-S2");
        insertTable2(it2, 2147483688L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483688L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483688L, "AddressG3", "1180, Bruxelles");
        insertTable2(it2, 2147483688L, "DocumentSortingValues", "______21874__1__1");
        insertTable2(it2, 2147483688L, "ItemSeq", "1");
        insertTable2(it2, 2147483688L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483688L, "Plex", "S");
        insertTable2(it2, 2147483688L, "SC3", "=");
        insertTable2(it2, 2147483688L, "has_address", "true");
        insertTable2(it2, 2147483688L, "SubBatchSortingValue", "BE_B-W3-S2___");
        insertTable2(it2, 2147483688L, "Language", "FR");
        insertTable2(it2, 2147483688L, "logo", "false");
        insertTable2(it2, 2147483688L, "SC4", "=");
        insertTable2(it2, 2147483688L, "InternalAddress", "");
        insertTable2(it2, 2147483688L, "AddressG6", "");
        insertTable2(it2, 2147483688L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483688L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483688L, "SC1", "Country=BE");
        insertTable2(it2, 2147483688L, "CommunicationOrderId", "21884");
        insertTable2(it2, 2147483688L, "BatchTypeId", "Paper");
        insertTable2(it2, 2147483688L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483688L, "SortPlan", "B-W3-S2");
        insertTable2(it2, 2147483688L, "Enveloping", "C");
        insertTable2(it2, 2147483688L, "PostComponentId", "21874");
        insertTable2(it2, 2147483688L, "BatchTypeLabel", "Paper");
        insertTable2(it2, 2147483688L, "city", "Bruxelles");
        insertTable2(it2, 2147483688L, "AddressG7", "");
        insertTable2(it2, 2147483688L, "CopyMention", "");
        insertTable2(it2, 2147483688L, "AddressG8", "");
        insertTable2(it2, 2147483688L, "StapleNbr", "");
        insertTable2(it2, 2147483688L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483688L, "AddressG5", "");
        insertTable2(it2, 2147483688L, "header", "true");
        insertTable2(it2, 2147483688L, "EnvelopSortingValue", "BE1180___testOlivier010-0020___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483688L, "GroupedWith", "");
        insertTable2(it2, 2147483688L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483688L, "DiversionReason", "");
        insertTable2(it2, 2147483688L, "Pliable", "true");
        insertTable2(it2, 2147483688L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483688L, "country", "BE");
        insertTable2(it2, 2147483688L, "AddressG1", "testOlivier010-0020");
        insertTable2(it2, 2147483688L, "MentionCode", "");
        insertTable2(it2, 2147483688L, "postCode", "1180");
        insertTable2(it2, 2147483688L, "SC5", "=");
        insertTable2(it2, 2147483688L, "Branding", "1C");
        insertTable1(it1, 2147483689L);
        insertTable2(it2, 2147483689L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483689L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483689L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483689L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483689L, "DocumentSortingValues", "______21875__1__1");
        insertTable2(it2, 2147483689L, "ItemSeq", "1");
        insertTable2(it2, 2147483689L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483689L, "Plex", "S");
        insertTable2(it2, 2147483689L, "SC3", "=");
        insertTable2(it2, 2147483689L, "has_address", "true");
        insertTable2(it2, 2147483689L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483689L, "Language", "FR");
        insertTable2(it2, 2147483689L, "logo", "false");
        insertTable2(it2, 2147483689L, "SC4", "=");
        insertTable2(it2, 2147483689L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483689L, "AddressG6", "");
        insertTable2(it2, 2147483689L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483689L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483689L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483689L, "CommunicationOrderId", "21885");
        insertTable2(it2, 2147483689L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483689L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483689L, "SortPlan", "");
        insertTable2(it2, 2147483689L, "Enveloping", "N");
        insertTable2(it2, 2147483689L, "PostComponentId", "21875");
        insertTable2(it2, 2147483689L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483689L, "city", "Bruxelles");
        insertTable2(it2, 2147483689L, "AddressG7", "");
        insertTable2(it2, 2147483689L, "CopyMention", "");
        insertTable2(it2, 2147483689L, "AddressG8", "");
        insertTable2(it2, 2147483689L, "StapleNbr", "");
        insertTable2(it2, 2147483689L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483689L, "AddressG5", "");
        insertTable2(it2, 2147483689L, "header", "true");
        insertTable2(it2, 2147483689L, "EnvelopSortingValue", "BE1180___testOlivier011-0021___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483689L, "GroupedWith", "");
        insertTable2(it2, 2147483689L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483689L, "DiversionReason", "001");
        insertTable2(it2, 2147483689L, "Pliable", "true");
        insertTable2(it2, 2147483689L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483689L, "country", "BE");
        insertTable2(it2, 2147483689L, "AddressG1", "testOlivier011-0021");
        insertTable2(it2, 2147483689L, "MentionCode", "");
        insertTable2(it2, 2147483689L, "postCode", "1180");
        insertTable2(it2, 2147483689L, "SC5", "=");
        insertTable2(it2, 2147483689L, "Branding", "1C");
        insertTable1(it1, 2147483690L);
        insertTable2(it2, 2147483690L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483690L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483690L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483690L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483690L, "DocumentSortingValues", "______21875__1__1");
        insertTable2(it2, 2147483690L, "ItemSeq", "1");
        insertTable2(it2, 2147483690L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483690L, "Plex", "S");
        insertTable2(it2, 2147483690L, "SC3", "=");
        insertTable2(it2, 2147483690L, "has_address", "true");
        insertTable2(it2, 2147483690L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483690L, "Language", "FR");
        insertTable2(it2, 2147483690L, "logo", "false");
        insertTable2(it2, 2147483690L, "SC4", "=");
        insertTable2(it2, 2147483690L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483690L, "AddressG6", "");
        insertTable2(it2, 2147483690L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483690L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483690L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483690L, "CommunicationOrderId", "21885");
        insertTable2(it2, 2147483690L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483690L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483690L, "SortPlan", "");
        insertTable2(it2, 2147483690L, "Enveloping", "N");
        insertTable2(it2, 2147483690L, "PostComponentId", "21875");
        insertTable2(it2, 2147483690L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483690L, "city", "Bruxelles");
        insertTable2(it2, 2147483690L, "AddressG7", "");
        insertTable2(it2, 2147483690L, "CopyMention", "");
        insertTable2(it2, 2147483690L, "AddressG8", "");
        insertTable2(it2, 2147483690L, "StapleNbr", "");
        insertTable2(it2, 2147483690L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483690L, "AddressG5", "");
        insertTable2(it2, 2147483690L, "header", "true");
        insertTable2(it2, 2147483690L, "EnvelopSortingValue", "BE1180___testOlivier011-0021___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483690L, "GroupedWith", "");
        insertTable2(it2, 2147483690L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483690L, "DiversionReason", "001");
        insertTable2(it2, 2147483690L, "Pliable", "true");
        insertTable2(it2, 2147483690L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483690L, "country", "BE");
        insertTable2(it2, 2147483690L, "AddressG1", "testOlivier011-0021");
        insertTable2(it2, 2147483690L, "MentionCode", "");
        insertTable2(it2, 2147483690L, "postCode", "1180");
        insertTable2(it2, 2147483690L, "SC5", "=");
        insertTable2(it2, 2147483690L, "Branding", "1C");
        insertTable1(it1, 2147483691L);
        insertTable2(it2, 2147483691L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483691L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483691L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483691L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483691L, "DocumentSortingValues", "______21876__1__1");
        insertTable2(it2, 2147483691L, "ItemSeq", "1");
        insertTable2(it2, 2147483691L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483691L, "Plex", "S");
        insertTable2(it2, 2147483691L, "SC3", "=");
        insertTable2(it2, 2147483691L, "has_address", "true");
        insertTable2(it2, 2147483691L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483691L, "Language", "FR");
        insertTable2(it2, 2147483691L, "logo", "false");
        insertTable2(it2, 2147483691L, "SC4", "=");
        insertTable2(it2, 2147483691L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483691L, "AddressG6", "");
        insertTable2(it2, 2147483691L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483691L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483691L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483691L, "CommunicationOrderId", "21886");
        insertTable2(it2, 2147483691L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483691L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483691L, "SortPlan", "");
        insertTable2(it2, 2147483691L, "Enveloping", "N");
        insertTable2(it2, 2147483691L, "PostComponentId", "21876");
        insertTable2(it2, 2147483691L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483691L, "city", "Bruxelles");
        insertTable2(it2, 2147483691L, "AddressG7", "");
        insertTable2(it2, 2147483691L, "CopyMention", "");
        insertTable2(it2, 2147483691L, "AddressG8", "");
        insertTable2(it2, 2147483691L, "StapleNbr", "");
        insertTable2(it2, 2147483691L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483691L, "AddressG5", "");
        insertTable2(it2, 2147483691L, "header", "true");
        insertTable2(it2, 2147483691L, "EnvelopSortingValue", "BE1180___testOlivier011-0022___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483691L, "GroupedWith", "");
        insertTable2(it2, 2147483691L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483691L, "DiversionReason", "001");
        insertTable2(it2, 2147483691L, "Pliable", "true");
        insertTable2(it2, 2147483691L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483691L, "country", "BE");
        insertTable2(it2, 2147483691L, "AddressG1", "testOlivier011-0022");
        insertTable2(it2, 2147483691L, "MentionCode", "");
        insertTable2(it2, 2147483691L, "postCode", "1180");
        insertTable2(it2, 2147483691L, "SC5", "=");
        insertTable2(it2, 2147483691L, "Branding", "1C");
        insertTable1(it1, 2147483692L);
        insertTable2(it2, 2147483692L, "SC2", "PostCode=1180");
        insertTable2(it2, 2147483692L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483692L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483692L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 2147483692L, "DocumentSortingValues", "______21876__1__1");
        insertTable2(it2, 2147483692L, "ItemSeq", "1");
        insertTable2(it2, 2147483692L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 2147483692L, "Plex", "S");
        insertTable2(it2, 2147483692L, "SC3", "=");
        insertTable2(it2, 2147483692L, "has_address", "true");
        insertTable2(it2, 2147483692L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 2147483692L, "Language", "FR");
        insertTable2(it2, 2147483692L, "logo", "false");
        insertTable2(it2, 2147483692L, "SC4", "=");
        insertTable2(it2, 2147483692L, "InternalAddress", "233/621");
        insertTable2(it2, 2147483692L, "AddressG6", "");
        insertTable2(it2, 2147483692L, "InternalAddressBringer", "true");
        insertTable2(it2, 2147483692L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483692L, "SC1", "Requester=ddch257");
        insertTable2(it2, 2147483692L, "CommunicationOrderId", "21886");
        insertTable2(it2, 2147483692L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 2147483692L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483692L, "SortPlan", "");
        insertTable2(it2, 2147483692L, "Enveloping", "N");
        insertTable2(it2, 2147483692L, "PostComponentId", "21876");
        insertTable2(it2, 2147483692L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 2147483692L, "city", "Bruxelles");
        insertTable2(it2, 2147483692L, "AddressG7", "");
        insertTable2(it2, 2147483692L, "CopyMention", "");
        insertTable2(it2, 2147483692L, "AddressG8", "");
        insertTable2(it2, 2147483692L, "StapleNbr", "");
        insertTable2(it2, 2147483692L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483692L, "AddressG5", "");
        insertTable2(it2, 2147483692L, "header", "true");
        insertTable2(it2, 2147483692L, "EnvelopSortingValue", "BE1180___testOlivier011-0022___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 2147483692L, "GroupedWith", "");
        insertTable2(it2, 2147483692L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 2147483692L, "DiversionReason", "001");
        insertTable2(it2, 2147483692L, "Pliable", "true");
        insertTable2(it2, 2147483692L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483692L, "country", "BE");
        insertTable2(it2, 2147483692L, "AddressG1", "testOlivier011-0022");
        insertTable2(it2, 2147483692L, "MentionCode", "");
        insertTable2(it2, 2147483692L, "postCode", "1180");
        insertTable2(it2, 2147483692L, "SC5", "=");
        insertTable2(it2, 2147483692L, "Branding", "1C");
        insertTable1(it1, 2147483693L);
        insertTable2(it2, 2147483693L, "SC2", "=");
        insertTable2(it2, 2147483693L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483693L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483693L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483693L, "DocumentSortingValues", "______21877__1__1");
        insertTable2(it2, 2147483693L, "ItemSeq", "1");
        insertTable2(it2, 2147483693L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483693L, "Plex", "S");
        insertTable2(it2, 2147483693L, "SC3", "=");
        insertTable2(it2, 2147483693L, "has_address", "true");
        insertTable2(it2, 2147483693L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483693L, "Language", "FR");
        insertTable2(it2, 2147483693L, "logo", "false");
        insertTable2(it2, 2147483693L, "SC4", "=");
        insertTable2(it2, 2147483693L, "InternalAddress", "");
        insertTable2(it2, 2147483693L, "AddressG6", "");
        insertTable2(it2, 2147483693L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483693L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483693L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483693L, "CommunicationOrderId", "21887");
        insertTable2(it2, 2147483693L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483693L, "location", "rue du broker");
        insertTable2(it2, 2147483693L, "SortPlan", "");
        insertTable2(it2, 2147483693L, "Enveloping", "C");
        insertTable2(it2, 2147483693L, "PostComponentId", "21877");
        insertTable2(it2, 2147483693L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483693L, "city", "Broker ville");
        insertTable2(it2, 2147483693L, "AddressG7", "");
        insertTable2(it2, 2147483693L, "CopyMention", "");
        insertTable2(it2, 2147483693L, "AddressG8", "");
        insertTable2(it2, 2147483693L, "StapleNbr", "");
        insertTable2(it2, 2147483693L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483693L, "AddressG5", "");
        insertTable2(it2, 2147483693L, "header", "true");
        insertTable2(it2, 2147483693L, "EnvelopSortingValue", "BE1000___testOlivier012-0023___________rue du broker_________________");
        insertTable2(it2, 2147483693L, "GroupedWith", "");
        insertTable2(it2, 2147483693L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483693L, "DiversionReason", "");
        insertTable2(it2, 2147483693L, "Pliable", "true");
        insertTable2(it2, 2147483693L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483693L, "country", "BE");
        insertTable2(it2, 2147483693L, "AddressG1", "testOlivier012-0023");
        insertTable2(it2, 2147483693L, "MentionCode", "");
        insertTable2(it2, 2147483693L, "postCode", "1000");
        insertTable2(it2, 2147483693L, "SC5", "=");
        insertTable2(it2, 2147483693L, "Branding", "1C");
        insertTable1(it1, 2147483694L);
        insertTable2(it2, 2147483694L, "SC2", "=");
        insertTable2(it2, 2147483694L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483694L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483694L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483694L, "DocumentSortingValues", "______21877__1__1");
        insertTable2(it2, 2147483694L, "ItemSeq", "1");
        insertTable2(it2, 2147483694L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483694L, "Plex", "S");
        insertTable2(it2, 2147483694L, "SC3", "=");
        insertTable2(it2, 2147483694L, "has_address", "true");
        insertTable2(it2, 2147483694L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483694L, "Language", "FR");
        insertTable2(it2, 2147483694L, "logo", "false");
        insertTable2(it2, 2147483694L, "SC4", "=");
        insertTable2(it2, 2147483694L, "InternalAddress", "");
        insertTable2(it2, 2147483694L, "AddressG6", "");
        insertTable2(it2, 2147483694L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483694L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483694L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483694L, "CommunicationOrderId", "21887");
        insertTable2(it2, 2147483694L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483694L, "location", "rue du broker");
        insertTable2(it2, 2147483694L, "SortPlan", "");
        insertTable2(it2, 2147483694L, "Enveloping", "C");
        insertTable2(it2, 2147483694L, "PostComponentId", "21877");
        insertTable2(it2, 2147483694L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483694L, "city", "Broker ville");
        insertTable2(it2, 2147483694L, "AddressG7", "");
        insertTable2(it2, 2147483694L, "CopyMention", "");
        insertTable2(it2, 2147483694L, "AddressG8", "");
        insertTable2(it2, 2147483694L, "StapleNbr", "");
        insertTable2(it2, 2147483694L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483694L, "AddressG5", "");
        insertTable2(it2, 2147483694L, "header", "true");
        insertTable2(it2, 2147483694L, "EnvelopSortingValue", "BE1000___testOlivier012-0023___________rue du broker_________________");
        insertTable2(it2, 2147483694L, "GroupedWith", "");
        insertTable2(it2, 2147483694L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483694L, "DiversionReason", "");
        insertTable2(it2, 2147483694L, "Pliable", "true");
        insertTable2(it2, 2147483694L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483694L, "country", "BE");
        insertTable2(it2, 2147483694L, "AddressG1", "testOlivier012-0023");
        insertTable2(it2, 2147483694L, "MentionCode", "");
        insertTable2(it2, 2147483694L, "postCode", "1000");
        insertTable2(it2, 2147483694L, "SC5", "=");
        insertTable2(it2, 2147483694L, "Branding", "1C");
        insertTable1(it1, 2147483695L);
        insertTable2(it2, 2147483695L, "SC2", "=");
        insertTable2(it2, 2147483695L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483695L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483695L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483695L, "DocumentSortingValues", "______21878__1__1");
        insertTable2(it2, 2147483695L, "ItemSeq", "1");
        insertTable2(it2, 2147483695L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483695L, "Plex", "S");
        insertTable2(it2, 2147483695L, "SC3", "=");
        insertTable2(it2, 2147483695L, "has_address", "true");
        insertTable2(it2, 2147483695L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483695L, "Language", "FR");
        insertTable2(it2, 2147483695L, "logo", "false");
        insertTable2(it2, 2147483695L, "SC4", "=");
        insertTable2(it2, 2147483695L, "InternalAddress", "");
        insertTable2(it2, 2147483695L, "AddressG6", "");
        insertTable2(it2, 2147483695L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483695L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483695L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483695L, "CommunicationOrderId", "21888");
        insertTable2(it2, 2147483695L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483695L, "location", "rue du broker");
        insertTable2(it2, 2147483695L, "SortPlan", "");
        insertTable2(it2, 2147483695L, "Enveloping", "C");
        insertTable2(it2, 2147483695L, "PostComponentId", "21878");
        insertTable2(it2, 2147483695L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483695L, "city", "Broker ville");
        insertTable2(it2, 2147483695L, "AddressG7", "");
        insertTable2(it2, 2147483695L, "CopyMention", "");
        insertTable2(it2, 2147483695L, "AddressG8", "");
        insertTable2(it2, 2147483695L, "StapleNbr", "");
        insertTable2(it2, 2147483695L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483695L, "AddressG5", "");
        insertTable2(it2, 2147483695L, "header", "true");
        insertTable2(it2, 2147483695L, "EnvelopSortingValue", "BE1000___testOlivier012-0024___________rue du broker_________________");
        insertTable2(it2, 2147483695L, "GroupedWith", "");
        insertTable2(it2, 2147483695L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483695L, "DiversionReason", "");
        insertTable2(it2, 2147483695L, "Pliable", "true");
        insertTable2(it2, 2147483695L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483695L, "country", "BE");
        insertTable2(it2, 2147483695L, "AddressG1", "testOlivier012-0024");
        insertTable2(it2, 2147483695L, "MentionCode", "");
        insertTable2(it2, 2147483695L, "postCode", "1000");
        insertTable2(it2, 2147483695L, "SC5", "=");
        insertTable2(it2, 2147483695L, "Branding", "1C");
        insertTable1(it1, 2147483696L);
        insertTable2(it2, 2147483696L, "SC2", "=");
        insertTable2(it2, 2147483696L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 2147483696L, "IsIdenticalToDocumentAddress", "true");
        insertTable2(it2, 2147483696L, "AddressG3", "Broker ville");
        insertTable2(it2, 2147483696L, "DocumentSortingValues", "______21878__1__1");
        insertTable2(it2, 2147483696L, "ItemSeq", "1");
        insertTable2(it2, 2147483696L, "BatchTypeInstructions", "");
        insertTable2(it2, 2147483696L, "Plex", "S");
        insertTable2(it2, 2147483696L, "SC3", "=");
        insertTable2(it2, 2147483696L, "has_address", "true");
        insertTable2(it2, 2147483696L, "SubBatchSortingValue", "Une info b");
        insertTable2(it2, 2147483696L, "Language", "FR");
        insertTable2(it2, 2147483696L, "logo", "false");
        insertTable2(it2, 2147483696L, "SC4", "=");
        insertTable2(it2, 2147483696L, "InternalAddress", "");
        insertTable2(it2, 2147483696L, "AddressG6", "");
        insertTable2(it2, 2147483696L, "InternalAddressBringer", "false");
        insertTable2(it2, 2147483696L, "AddresseeSeq", "1");
        insertTable2(it2, 2147483696L, "SC1", "DeliveryInformation=Une info bidon");
        insertTable2(it2, 2147483696L, "CommunicationOrderId", "21888");
        insertTable2(it2, 2147483696L, "BatchTypeId", "BROKER NET");
        insertTable2(it2, 2147483696L, "location", "rue du broker");
        insertTable2(it2, 2147483696L, "SortPlan", "");
        insertTable2(it2, 2147483696L, "Enveloping", "C");
        insertTable2(it2, 2147483696L, "PostComponentId", "21878");
        insertTable2(it2, 2147483696L, "BatchTypeLabel", "BROKER NET");
        insertTable2(it2, 2147483696L, "city", "Broker ville");
        insertTable2(it2, 2147483696L, "AddressG7", "");
        insertTable2(it2, 2147483696L, "CopyMention", "");
        insertTable2(it2, 2147483696L, "AddressG8", "");
        insertTable2(it2, 2147483696L, "StapleNbr", "");
        insertTable2(it2, 2147483696L, "TLEBundle", "Niveau2");
        insertTable2(it2, 2147483696L, "AddressG5", "");
        insertTable2(it2, 2147483696L, "header", "true");
        insertTable2(it2, 2147483696L, "EnvelopSortingValue", "BE1000___testOlivier012-0024___________rue du broker_________________");
        insertTable2(it2, 2147483696L, "GroupedWith", "");
        insertTable2(it2, 2147483696L, "AddressG2", "rue du broker");
        insertTable2(it2, 2147483696L, "DiversionReason", "");
        insertTable2(it2, 2147483696L, "Pliable", "true");
        insertTable2(it2, 2147483696L, "TLEBinder", "Niveau1");
        insertTable2(it2, 2147483696L, "country", "BE");
        insertTable2(it2, 2147483696L, "AddressG1", "testOlivier012-0024");
        insertTable2(it2, 2147483696L, "MentionCode", "");
        insertTable2(it2, 2147483696L, "postCode", "1000");
        insertTable2(it2, 2147483696L, "SC5", "=");
        insertTable2(it2, 2147483696L, "Branding", "1C");
        insertTable2(it2, 2147483649L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483649L, "GenerationTime", "Wed Oct 22 14:12:10 CEST 2008");
        insertTable2(it2, 2147483649L, "DocumentID", "21865/1/1//");
        insertTable2(it2, 2147483649L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483649L, "PhysicalID", "No");
        insertTable2(it2, 2147483649L, "Staple", "No");
        updateTable2(ut2, 2147483649L);
        insertTable2(it2, 2147483650L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483650L, "GenerationTime", "Wed Oct 22 14:12:10 CEST 2008");
        insertTable2(it2, 2147483650L, "DocumentID", "21865/1/1//");
        insertTable2(it2, 2147483650L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483650L, "PhysicalID", "No");
        insertTable2(it2, 2147483650L, "Staple", "No");
        updateTable2(ut2, 2147483650L);
        insertTable2(it2, 2147483651L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483651L, "GenerationTime", "Wed Oct 22 14:12:10 CEST 2008");
        insertTable2(it2, 2147483651L, "DocumentID", "21866/1/1//");
        insertTable2(it2, 2147483651L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483651L, "PhysicalID", "No");
        insertTable2(it2, 2147483651L, "Staple", "No");
        updateTable2(ut2, 2147483651L);
        insertTable2(it2, 2147483652L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483652L, "GenerationTime", "Wed Oct 22 14:12:10 CEST 2008");
        insertTable2(it2, 2147483652L, "DocumentID", "21866/1/1//");
        insertTable2(it2, 2147483652L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483652L, "PhysicalID", "No");
        insertTable2(it2, 2147483652L, "Staple", "No");
        updateTable2(ut2, 2147483652L);
        insertTable1(it1, 4294967297L);
        insertTable2(it2, 4294967297L, "SC2", "PostCode=1180");
        insertTable2(it2, 4294967297L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 4294967297L, "IsIdenticalToDocumentAddress", "");
        insertTable2(it2, 4294967297L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 4294967297L, "DocumentSortingValues", "");
        insertTable2(it2, 4294967297L, "ItemSeq", "0");
        insertTable2(it2, 4294967297L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 4294967297L, "Plex", "S");
        insertTable2(it2, 4294967297L, "SC3", "=");
        insertTable2(it2, 4294967297L, "has_address", "true");
        insertTable2(it2, 4294967297L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 4294967297L, "Language", "FR");
        insertTable2(it2, 4294967297L, "SC4", "=");
        insertTable2(it2, 4294967297L, "InternalAddress", "233/621");
        insertTable2(it2, 4294967297L, "AddressG6", "");
        insertTable2(it2, 4294967297L, "InternalAddressBringer", "true");
        insertTable2(it2, 4294967297L, "AddresseeSeq", "0");
        insertTable2(it2, 4294967297L, "SC1", "Requester=ddch257");
        insertTable2(it2, 4294967297L, "CommunicationOrderId", "21867");
        insertTable2(it2, 4294967297L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 4294967297L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 4294967297L, "SortPlan", "");
        insertTable2(it2, 4294967297L, "Enveloping", "N");
        insertTable2(it2, 4294967297L, "PostComponentId", "21857");
        insertTable2(it2, 4294967297L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 4294967297L, "city", "Bruxelles");
        insertTable2(it2, 4294967297L, "AddressG7", "");
        insertTable2(it2, 4294967297L, "CopyMention", "");
        insertTable2(it2, 4294967297L, "StapleNbr", "NO");
        insertTable2(it2, 4294967297L, "AddressG8", "");
        insertTable2(it2, 4294967297L, "AddressG5", "");
        insertTable2(it2, 4294967297L, "EnvelopSortingValue", "BE1180___testOlivier002-0003___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 4294967297L, "GroupedWith", "");
        insertTable2(it2, 4294967297L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 4294967297L, "DiversionReason", "001");
        insertTable2(it2, 4294967297L, "Pliable", "true");
        insertTable2(it2, 4294967297L, "country", "BE");
        insertTable2(it2, 4294967297L, "AddressG1", "testOlivier002-0003");
        insertTable2(it2, 4294967297L, "MentionCode", "");
        insertTable2(it2, 4294967297L, "postCode", "1180");
        insertTable2(it2, 4294967297L, "SC5", "=");
        insertTable2(it2, 4294967297L, "env_type", "I");
        insertTable2(it2, 4294967297L, "Branding", "1C");
        insertTable2(it2, 4294967297L, "PaperLayout", "ADDINT");
        insertTable2(it2, 4294967297L, "GenerationTime", "Wed Oct 22 14:12:10 CEST 2008");
        insertTable2(it2, 4294967297L, "DocumentID", "21867/0/0//");
        insertTable2(it2, 4294967297L, "PageSequenceId", "000001");
        insertTable2(it2, 4294967297L, "PhysicalID", "No");
        insertTable2(it2, 4294967297L, "cover_page", "true");
        insertTable2(it2, 4294967297L, "Staple", "No");
        updateTable2(ut2, 4294967297L);
        insertTable2(it2, 2147483653L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483653L, "GenerationTime", "Wed Oct 22 14:12:10 CEST 2008");
        insertTable2(it2, 2147483653L, "DocumentID", "21867/1/1//");
        insertTable2(it2, 2147483653L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483653L, "PhysicalID", "No");
        insertTable2(it2, 2147483653L, "Staple", "No");
        updateTable2(ut2, 2147483653L);
        insertTable2(it2, 2147483654L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483654L, "GenerationTime", "Wed Oct 22 14:12:10 CEST 2008");
        insertTable2(it2, 2147483654L, "DocumentID", "21867/1/1//");
        insertTable2(it2, 2147483654L, "PageSequenceId", "000003");
        insertTable2(it2, 2147483654L, "PhysicalID", "No");
        insertTable2(it2, 2147483654L, "Staple", "No");
        updateTable2(ut2, 2147483654L);
        insertTable1(it1, 6442450945L);
        insertTable2(it2, 6442450945L, "SC2", "PostCode=1180");
        insertTable2(it2, 6442450945L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 6442450945L, "IsIdenticalToDocumentAddress", "");
        insertTable2(it2, 6442450945L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 6442450945L, "DocumentSortingValues", "");
        insertTable2(it2, 6442450945L, "ItemSeq", "0");
        insertTable2(it2, 6442450945L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 6442450945L, "Plex", "S");
        insertTable2(it2, 6442450945L, "SC3", "=");
        insertTable2(it2, 6442450945L, "has_address", "true");
        insertTable2(it2, 6442450945L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 6442450945L, "Language", "FR");
        insertTable2(it2, 6442450945L, "SC4", "=");
        insertTable2(it2, 6442450945L, "InternalAddress", "233/621");
        insertTable2(it2, 6442450945L, "AddressG6", "");
        insertTable2(it2, 6442450945L, "InternalAddressBringer", "true");
        insertTable2(it2, 6442450945L, "AddresseeSeq", "0");
        insertTable2(it2, 6442450945L, "SC1", "Requester=ddch257");
        insertTable2(it2, 6442450945L, "CommunicationOrderId", "21868");
        insertTable2(it2, 6442450945L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 6442450945L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 6442450945L, "SortPlan", "");
        insertTable2(it2, 6442450945L, "Enveloping", "N");
        insertTable2(it2, 6442450945L, "PostComponentId", "21858");
        insertTable2(it2, 6442450945L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 6442450945L, "city", "Bruxelles");
        insertTable2(it2, 6442450945L, "AddressG7", "");
        insertTable2(it2, 6442450945L, "CopyMention", "");
        insertTable2(it2, 6442450945L, "StapleNbr", "NO");
        insertTable2(it2, 6442450945L, "AddressG8", "");
        insertTable2(it2, 6442450945L, "AddressG5", "");
        insertTable2(it2, 6442450945L, "EnvelopSortingValue", "BE1180___testOlivier002-0004___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 6442450945L, "GroupedWith", "");
        insertTable2(it2, 6442450945L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 6442450945L, "DiversionReason", "001");
        insertTable2(it2, 6442450945L, "Pliable", "true");
        insertTable2(it2, 6442450945L, "country", "BE");
        insertTable2(it2, 6442450945L, "AddressG1", "testOlivier002-0004");
        insertTable2(it2, 6442450945L, "MentionCode", "");
        insertTable2(it2, 6442450945L, "postCode", "1180");
        insertTable2(it2, 6442450945L, "SC5", "=");
        insertTable2(it2, 6442450945L, "env_type", "I");
        insertTable2(it2, 6442450945L, "Branding", "1C");
        insertTable2(it2, 6442450945L, "PaperLayout", "ADDINT");
        insertTable2(it2, 6442450945L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 6442450945L, "DocumentID", "21868/0/0//");
        insertTable2(it2, 6442450945L, "PageSequenceId", "000001");
        insertTable2(it2, 6442450945L, "PhysicalID", "No");
        insertTable2(it2, 6442450945L, "cover_page", "true");
        insertTable2(it2, 6442450945L, "Staple", "No");
        updateTable2(ut2, 6442450945L);
        insertTable2(it2, 2147483655L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483655L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483655L, "DocumentID", "21868/1/1//");
        insertTable2(it2, 2147483655L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483655L, "PhysicalID", "No");
        insertTable2(it2, 2147483655L, "Staple", "No");
        updateTable2(ut2, 2147483655L);
        insertTable2(it2, 2147483656L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483656L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483656L, "DocumentID", "21868/1/1//");
        insertTable2(it2, 2147483656L, "PageSequenceId", "000003");
        insertTable2(it2, 2147483656L, "PhysicalID", "No");
        insertTable2(it2, 2147483656L, "Staple", "No");
        updateTable2(ut2, 2147483656L);
        insertTable2(it2, 2147483657L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483657L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483657L, "DocumentID", "21869/1/1//");
        insertTable2(it2, 2147483657L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483657L, "PhysicalID", "No");
        insertTable2(it2, 2147483657L, "Staple", "No");
        updateTable2(ut2, 2147483657L);
        insertTable1(it1, 8589934593L);
        insertTable2(it2, 8589934593L, "SC2", "PostCode=1180");
        insertTable2(it2, 8589934593L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 8589934593L, "IsIdenticalToDocumentAddress", "");
        insertTable2(it2, 8589934593L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 8589934593L, "DocumentSortingValues", "");
        insertTable2(it2, 8589934593L, "ItemSeq", "0");
        insertTable2(it2, 8589934593L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 8589934593L, "Plex", "S");
        insertTable2(it2, 8589934593L, "SC3", "=");
        insertTable2(it2, 8589934593L, "has_address", "true");
        insertTable2(it2, 8589934593L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 8589934593L, "Language", "FR");
        insertTable2(it2, 8589934593L, "SC4", "=");
        insertTable2(it2, 8589934593L, "InternalAddress", "233/621");
        insertTable2(it2, 8589934593L, "AddressG6", "");
        insertTable2(it2, 8589934593L, "InternalAddressBringer", "true");
        insertTable2(it2, 8589934593L, "AddresseeSeq", "0");
        insertTable2(it2, 8589934593L, "SC1", "Requester=ddch257");
        insertTable2(it2, 8589934593L, "CommunicationOrderId", "21873");
        insertTable2(it2, 8589934593L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 8589934593L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 8589934593L, "SortPlan", "");
        insertTable2(it2, 8589934593L, "Enveloping", "N");
        insertTable2(it2, 8589934593L, "PostComponentId", "21863");
        insertTable2(it2, 8589934593L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 8589934593L, "city", "Bruxelles");
        insertTable2(it2, 8589934593L, "AddressG7", "");
        insertTable2(it2, 8589934593L, "CopyMention", "");
        insertTable2(it2, 8589934593L, "StapleNbr", "NO");
        insertTable2(it2, 8589934593L, "AddressG8", "");
        insertTable2(it2, 8589934593L, "AddressG5", "");
        insertTable2(it2, 8589934593L, "EnvelopSortingValue", "BE1180___testOlivier005-0009___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 8589934593L, "GroupedWith", "");
        insertTable2(it2, 8589934593L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 8589934593L, "DiversionReason", "001");
        insertTable2(it2, 8589934593L, "Pliable", "true");
        insertTable2(it2, 8589934593L, "country", "BE");
        insertTable2(it2, 8589934593L, "AddressG1", "testOlivier005-0009");
        insertTable2(it2, 8589934593L, "MentionCode", "");
        insertTable2(it2, 8589934593L, "postCode", "1180");
        insertTable2(it2, 8589934593L, "SC5", "=");
        insertTable2(it2, 8589934593L, "env_type", "I");
        insertTable2(it2, 8589934593L, "Branding", "1C");
        insertTable2(it2, 2147483658L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483658L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483658L, "DocumentID", "21869/1/1//");
        insertTable2(it2, 2147483658L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483658L, "PhysicalID", "No");
        insertTable2(it2, 2147483658L, "Staple", "No");
        updateTable2(ut2, 2147483658L);
        insertTable2(it2, 2147483659L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483659L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483659L, "DocumentID", "21870/1/1//");
        insertTable2(it2, 2147483659L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483659L, "PhysicalID", "No");
        insertTable2(it2, 2147483659L, "Staple", "No");
        updateTable2(ut2, 2147483659L);
        insertTable2(it2, 2147483660L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483660L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483660L, "DocumentID", "21870/1/1//");
        insertTable2(it2, 2147483660L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483660L, "PhysicalID", "No");
        insertTable2(it2, 2147483660L, "Staple", "No");
        updateTable2(ut2, 2147483660L);
        insertTable2(it2, 2147483661L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483661L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483661L, "DocumentID", "21871/1/1//");
        insertTable2(it2, 2147483661L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483661L, "PhysicalID", "No");
        insertTable2(it2, 2147483661L, "Staple", "No");
        updateTable2(ut2, 2147483661L);
        insertTable2(it2, 2147483662L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483662L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483662L, "DocumentID", "21871/1/1//");
        insertTable2(it2, 2147483662L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483662L, "PhysicalID", "No");
        insertTable2(it2, 2147483662L, "Staple", "No");
        updateTable2(ut2, 2147483662L);
        insertTable2(it2, 2147483663L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483663L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483663L, "DocumentID", "21872/1/1//");
        insertTable2(it2, 2147483663L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483663L, "PhysicalID", "No");
        insertTable2(it2, 2147483663L, "Staple", "No");
        updateTable2(ut2, 2147483663L);
        insertTable2(it2, 2147483664L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483664L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483664L, "DocumentID", "21872/1/1//");
        insertTable2(it2, 2147483664L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483664L, "PhysicalID", "No");
        insertTable2(it2, 2147483664L, "Staple", "No");
        updateTable2(ut2, 2147483664L);
        insertTable1(it1, 10737418241L);
        insertTable2(it2, 10737418241L, "SC2", "PostCode=1180");
        insertTable2(it2, 10737418241L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 10737418241L, "IsIdenticalToDocumentAddress", "");
        insertTable2(it2, 10737418241L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 10737418241L, "DocumentSortingValues", "");
        insertTable2(it2, 10737418241L, "ItemSeq", "0");
        insertTable2(it2, 10737418241L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 10737418241L, "Plex", "S");
        insertTable2(it2, 10737418241L, "SC3", "=");
        insertTable2(it2, 10737418241L, "has_address", "true");
        insertTable2(it2, 10737418241L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 10737418241L, "Language", "FR");
        insertTable2(it2, 10737418241L, "SC4", "=");
        insertTable2(it2, 10737418241L, "InternalAddress", "233/621");
        insertTable2(it2, 10737418241L, "AddressG6", "");
        insertTable2(it2, 10737418241L, "InternalAddressBringer", "true");
        insertTable2(it2, 10737418241L, "AddresseeSeq", "0");
        insertTable2(it2, 10737418241L, "SC1", "Requester=ddch257");
        insertTable2(it2, 10737418241L, "CommunicationOrderId", "21874");
        insertTable2(it2, 10737418241L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 10737418241L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 10737418241L, "SortPlan", "");
        insertTable2(it2, 10737418241L, "Enveloping", "N");
        insertTable2(it2, 10737418241L, "PostComponentId", "21864");
        insertTable2(it2, 10737418241L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 10737418241L, "city", "Bruxelles");
        insertTable2(it2, 10737418241L, "AddressG7", "");
        insertTable2(it2, 10737418241L, "CopyMention", "");
        insertTable2(it2, 10737418241L, "StapleNbr", "NO");
        insertTable2(it2, 10737418241L, "AddressG8", "");
        insertTable2(it2, 10737418241L, "AddressG5", "");
        insertTable2(it2, 10737418241L, "EnvelopSortingValue", "BE1180___testOlivier005-0010___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 10737418241L, "GroupedWith", "");
        insertTable2(it2, 10737418241L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 10737418241L, "DiversionReason", "001");
        insertTable2(it2, 10737418241L, "Pliable", "true");
        insertTable2(it2, 10737418241L, "country", "BE");
        insertTable2(it2, 10737418241L, "AddressG1", "testOlivier005-0010");
        insertTable2(it2, 10737418241L, "MentionCode", "");
        insertTable2(it2, 10737418241L, "postCode", "1180");
        insertTable2(it2, 10737418241L, "SC5", "=");
        insertTable2(it2, 10737418241L, "env_type", "I");
        insertTable2(it2, 10737418241L, "Branding", "1C");
        insertTable2(it2, 8589934593L, "PaperLayout", "ADDINT");
        insertTable2(it2, 8589934593L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 8589934593L, "DocumentID", "21873/0/0//");
        insertTable2(it2, 8589934593L, "PageSequenceId", "000001");
        insertTable2(it2, 8589934593L, "PhysicalID", "No");
        insertTable2(it2, 8589934593L, "cover_page", "true");
        insertTable2(it2, 8589934593L, "Staple", "No");
        updateTable2(ut2, 8589934593L);
        insertTable2(it2, 2147483665L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483665L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483665L, "DocumentID", "21873/1/1//");
        insertTable2(it2, 2147483665L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483665L, "PhysicalID", "No");
        insertTable2(it2, 2147483665L, "Staple", "No");
        updateTable2(ut2, 2147483665L);
        insertTable2(it2, 2147483666L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483666L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483666L, "DocumentID", "21873/1/1//");
        insertTable2(it2, 2147483666L, "PageSequenceId", "000003");
        insertTable2(it2, 2147483666L, "PhysicalID", "No");
        insertTable2(it2, 2147483666L, "Staple", "No");
        updateTable2(ut2, 2147483666L);
        insertTable2(it2, 10737418241L, "PaperLayout", "ADDINT");
        insertTable2(it2, 10737418241L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 10737418241L, "DocumentID", "21874/0/0//");
        insertTable2(it2, 10737418241L, "PageSequenceId", "000001");
        insertTable2(it2, 10737418241L, "PhysicalID", "No");
        insertTable2(it2, 10737418241L, "cover_page", "true");
        insertTable2(it2, 10737418241L, "Staple", "No");
        updateTable2(ut2, 10737418241L);
        insertTable2(it2, 2147483667L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483667L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483667L, "DocumentID", "21874/1/1//");
        insertTable2(it2, 2147483667L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483667L, "PhysicalID", "No");
        insertTable2(it2, 2147483667L, "Staple", "No");
        updateTable2(ut2, 2147483667L);
        insertTable2(it2, 2147483668L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483668L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483668L, "DocumentID", "21874/1/1//");
        insertTable2(it2, 2147483668L, "PageSequenceId", "000003");
        insertTable2(it2, 2147483668L, "PhysicalID", "No");
        insertTable2(it2, 2147483668L, "Staple", "No");
        updateTable2(ut2, 2147483668L);
        insertTable2(it2, 2147483669L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483669L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483669L, "DocumentID", "21875/1/1//");
        insertTable2(it2, 2147483669L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483669L, "PhysicalID", "No");
        insertTable2(it2, 2147483669L, "Staple", "No");
        updateTable2(ut2, 2147483669L);
        insertTable2(it2, 2147483670L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483670L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483670L, "DocumentID", "21875/1/1//");
        insertTable2(it2, 2147483670L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483670L, "PhysicalID", "No");
        insertTable2(it2, 2147483670L, "Staple", "No");
        updateTable2(ut2, 2147483670L);
        insertTable1(it1, 12884901889L);
        insertTable2(it2, 12884901889L, "SC2", "PostCode=1180");
        insertTable2(it2, 12884901889L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 12884901889L, "IsIdenticalToDocumentAddress", "");
        insertTable2(it2, 12884901889L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 12884901889L, "DocumentSortingValues", "");
        insertTable2(it2, 12884901889L, "ItemSeq", "0");
        insertTable2(it2, 12884901889L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 12884901889L, "Plex", "S");
        insertTable2(it2, 12884901889L, "SC3", "=");
        insertTable2(it2, 12884901889L, "has_address", "true");
        insertTable2(it2, 12884901889L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 12884901889L, "Language", "FR");
        insertTable2(it2, 12884901889L, "SC4", "=");
        insertTable2(it2, 12884901889L, "InternalAddress", "233/621");
        insertTable2(it2, 12884901889L, "AddressG6", "");
        insertTable2(it2, 12884901889L, "InternalAddressBringer", "true");
        insertTable2(it2, 12884901889L, "AddresseeSeq", "0");
        insertTable2(it2, 12884901889L, "SC1", "Requester=ddch257");
        insertTable2(it2, 12884901889L, "CommunicationOrderId", "21879");
        insertTable2(it2, 12884901889L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 12884901889L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 12884901889L, "SortPlan", "");
        insertTable2(it2, 12884901889L, "Enveloping", "N");
        insertTable2(it2, 12884901889L, "PostComponentId", "21869");
        insertTable2(it2, 12884901889L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 12884901889L, "city", "Bruxelles");
        insertTable2(it2, 12884901889L, "AddressG7", "");
        insertTable2(it2, 12884901889L, "CopyMention", "");
        insertTable2(it2, 12884901889L, "StapleNbr", "NO");
        insertTable2(it2, 12884901889L, "AddressG8", "");
        insertTable2(it2, 12884901889L, "AddressG5", "");
        insertTable2(it2, 12884901889L, "EnvelopSortingValue", "BE1180___testOlivier008-0015___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 12884901889L, "GroupedWith", "");
        insertTable2(it2, 12884901889L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 12884901889L, "DiversionReason", "001");
        insertTable2(it2, 12884901889L, "Pliable", "true");
        insertTable2(it2, 12884901889L, "country", "BE");
        insertTable2(it2, 12884901889L, "AddressG1", "testOlivier008-0015");
        insertTable2(it2, 12884901889L, "MentionCode", "");
        insertTable2(it2, 12884901889L, "postCode", "1180");
        insertTable2(it2, 12884901889L, "SC5", "=");
        insertTable2(it2, 12884901889L, "env_type", "I");
        insertTable2(it2, 12884901889L, "Branding", "1C");
        insertTable2(it2, 2147483671L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483671L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483671L, "DocumentID", "21876/1/1//");
        insertTable2(it2, 2147483671L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483671L, "PhysicalID", "No");
        insertTable2(it2, 2147483671L, "Staple", "No");
        updateTable2(ut2, 2147483671L);
        insertTable2(it2, 2147483672L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483672L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483672L, "DocumentID", "21876/1/1//");
        insertTable2(it2, 2147483672L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483672L, "PhysicalID", "No");
        insertTable2(it2, 2147483672L, "Staple", "No");
        updateTable2(ut2, 2147483672L);
        insertTable2(it2, 2147483673L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483673L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483673L, "DocumentID", "21877/1/1//");
        insertTable2(it2, 2147483673L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483673L, "PhysicalID", "No");
        insertTable2(it2, 2147483673L, "Staple", "No");
        updateTable2(ut2, 2147483673L);
        insertTable2(it2, 2147483674L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483674L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483674L, "DocumentID", "21877/1/1//");
        insertTable2(it2, 2147483674L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483674L, "PhysicalID", "No");
        insertTable2(it2, 2147483674L, "Staple", "No");
        updateTable2(ut2, 2147483674L);
        insertTable2(it2, 2147483675L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483675L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483675L, "DocumentID", "21878/1/1//");
        insertTable2(it2, 2147483675L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483675L, "PhysicalID", "No");
        insertTable2(it2, 2147483675L, "Staple", "No");
        updateTable2(ut2, 2147483675L);
        insertTable2(it2, 2147483676L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483676L, "GenerationTime", "Wed Oct 22 14:12:11 CEST 2008");
        insertTable2(it2, 2147483676L, "DocumentID", "21878/1/1//");
        insertTable2(it2, 2147483676L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483676L, "PhysicalID", "No");
        insertTable2(it2, 2147483676L, "Staple", "No");
        updateTable2(ut2, 2147483676L);
        insertTable1(it1, 15032385537L);
        insertTable2(it2, 15032385537L, "SC2", "PostCode=1180");
        insertTable2(it2, 15032385537L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 15032385537L, "IsIdenticalToDocumentAddress", "");
        insertTable2(it2, 15032385537L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 15032385537L, "DocumentSortingValues", "");
        insertTable2(it2, 15032385537L, "ItemSeq", "0");
        insertTable2(it2, 15032385537L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 15032385537L, "Plex", "S");
        insertTable2(it2, 15032385537L, "SC3", "=");
        insertTable2(it2, 15032385537L, "has_address", "true");
        insertTable2(it2, 15032385537L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 15032385537L, "Language", "FR");
        insertTable2(it2, 15032385537L, "SC4", "=");
        insertTable2(it2, 15032385537L, "InternalAddress", "233/621");
        insertTable2(it2, 15032385537L, "AddressG6", "");
        insertTable2(it2, 15032385537L, "InternalAddressBringer", "true");
        insertTable2(it2, 15032385537L, "AddresseeSeq", "0");
        insertTable2(it2, 15032385537L, "SC1", "Requester=ddch257");
        insertTable2(it2, 15032385537L, "CommunicationOrderId", "21880");
        insertTable2(it2, 15032385537L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 15032385537L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 15032385537L, "SortPlan", "");
        insertTable2(it2, 15032385537L, "Enveloping", "N");
        insertTable2(it2, 15032385537L, "PostComponentId", "21870");
        insertTable2(it2, 15032385537L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 15032385537L, "city", "Bruxelles");
        insertTable2(it2, 15032385537L, "AddressG7", "");
        insertTable2(it2, 15032385537L, "CopyMention", "");
        insertTable2(it2, 15032385537L, "StapleNbr", "NO");
        insertTable2(it2, 15032385537L, "AddressG8", "");
        insertTable2(it2, 15032385537L, "AddressG5", "");
        insertTable2(it2, 15032385537L, "EnvelopSortingValue", "BE1180___testOlivier008-0016___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 15032385537L, "GroupedWith", "");
        insertTable2(it2, 15032385537L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 15032385537L, "DiversionReason", "001");
        insertTable2(it2, 15032385537L, "Pliable", "true");
        insertTable2(it2, 15032385537L, "country", "BE");
        insertTable2(it2, 15032385537L, "AddressG1", "testOlivier008-0016");
        insertTable2(it2, 15032385537L, "MentionCode", "");
        insertTable2(it2, 15032385537L, "postCode", "1180");
        insertTable2(it2, 15032385537L, "SC5", "=");
        insertTable2(it2, 15032385537L, "env_type", "I");
        insertTable2(it2, 15032385537L, "Branding", "1C");
        insertTable2(it2, 12884901889L, "PaperLayout", "ADDINT");
        insertTable2(it2, 12884901889L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 12884901889L, "DocumentID", "21879/0/0//");
        insertTable2(it2, 12884901889L, "PageSequenceId", "000001");
        insertTable2(it2, 12884901889L, "PhysicalID", "No");
        insertTable2(it2, 12884901889L, "cover_page", "true");
        insertTable2(it2, 12884901889L, "Staple", "No");
        updateTable2(ut2, 12884901889L);
        insertTable2(it2, 2147483677L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483677L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483677L, "DocumentID", "21879/1/1//");
        insertTable2(it2, 2147483677L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483677L, "PhysicalID", "No");
        insertTable2(it2, 2147483677L, "Staple", "No");
        updateTable2(ut2, 2147483677L);
        insertTable2(it2, 2147483678L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483678L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483678L, "DocumentID", "21879/1/1//");
        insertTable2(it2, 2147483678L, "PageSequenceId", "000003");
        insertTable2(it2, 2147483678L, "PhysicalID", "No");
        insertTable2(it2, 2147483678L, "Staple", "No");
        updateTable2(ut2, 2147483678L);
        insertTable2(it2, 15032385537L, "PaperLayout", "ADDINT");
        insertTable2(it2, 15032385537L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 15032385537L, "DocumentID", "21880/0/0//");
        insertTable2(it2, 15032385537L, "PageSequenceId", "000001");
        insertTable2(it2, 15032385537L, "PhysicalID", "No");
        insertTable2(it2, 15032385537L, "cover_page", "true");
        insertTable2(it2, 15032385537L, "Staple", "No");
        updateTable2(ut2, 15032385537L);
        insertTable2(it2, 2147483679L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483679L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483679L, "DocumentID", "21880/1/1//");
        insertTable2(it2, 2147483679L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483679L, "PhysicalID", "No");
        insertTable2(it2, 2147483679L, "Staple", "No");
        updateTable2(ut2, 2147483679L);
        insertTable2(it2, 2147483680L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483680L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483680L, "DocumentID", "21880/1/1//");
        insertTable2(it2, 2147483680L, "PageSequenceId", "000003");
        insertTable2(it2, 2147483680L, "PhysicalID", "No");
        insertTable2(it2, 2147483680L, "Staple", "No");
        updateTable2(ut2, 2147483680L);
        insertTable1(it1, 17179869185L);
        insertTable2(it2, 17179869185L, "SC2", "PostCode=1180");
        insertTable2(it2, 17179869185L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 17179869185L, "IsIdenticalToDocumentAddress", "");
        insertTable2(it2, 17179869185L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 17179869185L, "DocumentSortingValues", "");
        insertTable2(it2, 17179869185L, "ItemSeq", "0");
        insertTable2(it2, 17179869185L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 17179869185L, "Plex", "S");
        insertTable2(it2, 17179869185L, "SC3", "=");
        insertTable2(it2, 17179869185L, "has_address", "true");
        insertTable2(it2, 17179869185L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 17179869185L, "Language", "FR");
        insertTable2(it2, 17179869185L, "SC4", "=");
        insertTable2(it2, 17179869185L, "InternalAddress", "233/621");
        insertTable2(it2, 17179869185L, "AddressG6", "");
        insertTable2(it2, 17179869185L, "InternalAddressBringer", "true");
        insertTable2(it2, 17179869185L, "AddresseeSeq", "0");
        insertTable2(it2, 17179869185L, "SC1", "Requester=ddch257");
        insertTable2(it2, 17179869185L, "CommunicationOrderId", "21885");
        insertTable2(it2, 17179869185L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 17179869185L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 17179869185L, "SortPlan", "");
        insertTable2(it2, 17179869185L, "Enveloping", "N");
        insertTable2(it2, 17179869185L, "PostComponentId", "21875");
        insertTable2(it2, 17179869185L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 17179869185L, "city", "Bruxelles");
        insertTable2(it2, 17179869185L, "AddressG7", "");
        insertTable2(it2, 17179869185L, "CopyMention", "");
        insertTable2(it2, 17179869185L, "StapleNbr", "NO");
        insertTable2(it2, 17179869185L, "AddressG8", "");
        insertTable2(it2, 17179869185L, "AddressG5", "");
        insertTable2(it2, 17179869185L, "EnvelopSortingValue", "BE1180___testOlivier011-0021___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 17179869185L, "GroupedWith", "");
        insertTable2(it2, 17179869185L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 17179869185L, "DiversionReason", "001");
        insertTable2(it2, 17179869185L, "Pliable", "true");
        insertTable2(it2, 17179869185L, "country", "BE");
        insertTable2(it2, 17179869185L, "AddressG1", "testOlivier011-0021");
        insertTable2(it2, 17179869185L, "MentionCode", "");
        insertTable2(it2, 17179869185L, "postCode", "1180");
        insertTable2(it2, 17179869185L, "SC5", "=");
        insertTable2(it2, 17179869185L, "env_type", "I");
        insertTable2(it2, 17179869185L, "Branding", "1C");
        insertTable2(it2, 2147483681L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483681L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483681L, "DocumentID", "21881/1/1//");
        insertTable2(it2, 2147483681L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483681L, "PhysicalID", "No");
        insertTable2(it2, 2147483681L, "Staple", "No");
        updateTable2(ut2, 2147483681L);
        insertTable2(it2, 2147483682L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483682L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483682L, "DocumentID", "21881/1/1//");
        insertTable2(it2, 2147483682L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483682L, "PhysicalID", "No");
        insertTable2(it2, 2147483682L, "Staple", "No");
        updateTable2(ut2, 2147483682L);
        insertTable2(it2, 2147483683L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483683L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483683L, "DocumentID", "21882/1/1//");
        insertTable2(it2, 2147483683L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483683L, "PhysicalID", "No");
        insertTable2(it2, 2147483683L, "Staple", "No");
        updateTable2(ut2, 2147483683L);
        insertTable2(it2, 2147483684L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483684L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483684L, "DocumentID", "21882/1/1//");
        insertTable2(it2, 2147483684L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483684L, "PhysicalID", "No");
        insertTable2(it2, 2147483684L, "Staple", "No");
        updateTable2(ut2, 2147483684L);
        insertTable2(it2, 2147483685L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483685L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483685L, "DocumentID", "21883/1/1//");
        insertTable2(it2, 2147483685L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483685L, "PhysicalID", "No");
        insertTable2(it2, 2147483685L, "Staple", "No");
        updateTable2(ut2, 2147483685L);
        insertTable2(it2, 2147483686L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483686L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483686L, "DocumentID", "21883/1/1//");
        insertTable2(it2, 2147483686L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483686L, "PhysicalID", "No");
        insertTable2(it2, 2147483686L, "Staple", "No");
        updateTable2(ut2, 2147483686L);
        insertTable2(it2, 2147483687L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483687L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483687L, "DocumentID", "21884/1/1//");
        insertTable2(it2, 2147483687L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483687L, "PhysicalID", "No");
        insertTable2(it2, 2147483687L, "Staple", "No");
        updateTable2(ut2, 2147483687L);
        insertTable1(it1, 19327352833L);
        insertTable2(it2, 19327352833L, "SC2", "PostCode=1180");
        insertTable2(it2, 19327352833L, "AddressG4", "BELGIQUE");
        insertTable2(it2, 19327352833L, "IsIdenticalToDocumentAddress", "");
        insertTable2(it2, 19327352833L, "AddressG3", "1180 Bruxelles");
        insertTable2(it2, 19327352833L, "DocumentSortingValues", "");
        insertTable2(it2, 19327352833L, "ItemSeq", "0");
        insertTable2(it2, 19327352833L, "BatchTypeInstructions", "Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.");
        insertTable2(it2, 19327352833L, "Plex", "S");
        insertTable2(it2, 19327352833L, "SC3", "=");
        insertTable2(it2, 19327352833L, "has_address", "true");
        insertTable2(it2, 19327352833L, "SubBatchSortingValue", "ddch257___1180______");
        insertTable2(it2, 19327352833L, "Language", "FR");
        insertTable2(it2, 19327352833L, "SC4", "=");
        insertTable2(it2, 19327352833L, "InternalAddress", "233/621");
        insertTable2(it2, 19327352833L, "AddressG6", "");
        insertTable2(it2, 19327352833L, "InternalAddressBringer", "true");
        insertTable2(it2, 19327352833L, "AddresseeSeq", "0");
        insertTable2(it2, 19327352833L, "SC1", "Requester=ddch257");
        insertTable2(it2, 19327352833L, "CommunicationOrderId", "21886");
        insertTable2(it2, 19327352833L, "BatchTypeId", "233-621-001");
        insertTable2(it2, 19327352833L, "location", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 19327352833L, "SortPlan", "");
        insertTable2(it2, 19327352833L, "Enveloping", "N");
        insertTable2(it2, 19327352833L, "PostComponentId", "21876");
        insertTable2(it2, 19327352833L, "BatchTypeLabel", "Diversion pour test");
        insertTable2(it2, 19327352833L, "city", "Bruxelles");
        insertTable2(it2, 19327352833L, "AddressG7", "");
        insertTable2(it2, 19327352833L, "CopyMention", "");
        insertTable2(it2, 19327352833L, "StapleNbr", "NO");
        insertTable2(it2, 19327352833L, "AddressG8", "");
        insertTable2(it2, 19327352833L, "AddressG5", "");
        insertTable2(it2, 19327352833L, "EnvelopSortingValue", "BE1180___testOlivier011-0022___________Bla bla bla bla bla bla 99____");
        insertTable2(it2, 19327352833L, "GroupedWith", "");
        insertTable2(it2, 19327352833L, "AddressG2", "Bla bla bla bla bla bla 99");
        insertTable2(it2, 19327352833L, "DiversionReason", "001");
        insertTable2(it2, 19327352833L, "Pliable", "true");
        insertTable2(it2, 19327352833L, "country", "BE");
        insertTable2(it2, 19327352833L, "AddressG1", "testOlivier011-0022");
        insertTable2(it2, 19327352833L, "MentionCode", "");
        insertTable2(it2, 19327352833L, "postCode", "1180");
        insertTable2(it2, 19327352833L, "SC5", "=");
        insertTable2(it2, 19327352833L, "env_type", "I");
        insertTable2(it2, 19327352833L, "Branding", "1C");
        insertTable2(it2, 2147483688L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483688L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483688L, "DocumentID", "21884/1/1//");
        insertTable2(it2, 2147483688L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483688L, "PhysicalID", "No");
        insertTable2(it2, 2147483688L, "Staple", "No");
        updateTable2(ut2, 2147483688L);
        insertTable2(it2, 17179869185L, "PaperLayout", "ADDINT");
        insertTable2(it2, 17179869185L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 17179869185L, "DocumentID", "21885/0/0//");
        insertTable2(it2, 17179869185L, "PageSequenceId", "000001");
        insertTable2(it2, 17179869185L, "PhysicalID", "No");
        insertTable2(it2, 17179869185L, "cover_page", "true");
        insertTable2(it2, 17179869185L, "Staple", "No");
        updateTable2(ut2, 17179869185L);
        insertTable2(it2, 2147483689L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483689L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483689L, "DocumentID", "21885/1/1//");
        insertTable2(it2, 2147483689L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483689L, "PhysicalID", "No");
        insertTable2(it2, 2147483689L, "Staple", "No");
        updateTable2(ut2, 2147483689L);
        insertTable2(it2, 2147483690L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483690L, "GenerationTime", "Wed Oct 22 14:12:12 CEST 2008");
        insertTable2(it2, 2147483690L, "DocumentID", "21885/1/1//");
        insertTable2(it2, 2147483690L, "PageSequenceId", "000003");
        insertTable2(it2, 2147483690L, "PhysicalID", "No");
        insertTable2(it2, 2147483690L, "Staple", "No");
        updateTable2(ut2, 2147483690L);
        insertTable2(it2, 19327352833L, "PaperLayout", "ADDINT");
        insertTable2(it2, 19327352833L, "GenerationTime", "Wed Oct 22 14:12:13 CEST 2008");
        insertTable2(it2, 19327352833L, "DocumentID", "21886/0/0//");
        insertTable2(it2, 19327352833L, "PageSequenceId", "000001");
        insertTable2(it2, 19327352833L, "PhysicalID", "No");
        insertTable2(it2, 19327352833L, "cover_page", "true");
        insertTable2(it2, 19327352833L, "Staple", "No");
        updateTable2(ut2, 19327352833L);
        insertTable2(it2, 2147483691L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483691L, "GenerationTime", "Wed Oct 22 14:12:13 CEST 2008");
        insertTable2(it2, 2147483691L, "DocumentID", "21886/1/1//");
        insertTable2(it2, 2147483691L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483691L, "PhysicalID", "No");
        insertTable2(it2, 2147483691L, "Staple", "No");
        updateTable2(ut2, 2147483691L);
        insertTable2(it2, 2147483692L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483692L, "GenerationTime", "Wed Oct 22 14:12:13 CEST 2008");
        insertTable2(it2, 2147483692L, "DocumentID", "21886/1/1//");
        insertTable2(it2, 2147483692L, "PageSequenceId", "000003");
        insertTable2(it2, 2147483692L, "PhysicalID", "No");
        insertTable2(it2, 2147483692L, "Staple", "No");
        updateTable2(ut2, 2147483692L);
        insertTable2(it2, 2147483693L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483693L, "GenerationTime", "Wed Oct 22 14:12:13 CEST 2008");
        insertTable2(it2, 2147483693L, "DocumentID", "21887/1/1//");
        insertTable2(it2, 2147483693L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483693L, "PhysicalID", "No");
        insertTable2(it2, 2147483693L, "Staple", "No");
        updateTable2(ut2, 2147483693L);
        insertTable2(it2, 2147483694L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483694L, "GenerationTime", "Wed Oct 22 14:12:13 CEST 2008");
        insertTable2(it2, 2147483694L, "DocumentID", "21887/1/1//");
        insertTable2(it2, 2147483694L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483694L, "PhysicalID", "No");
        insertTable2(it2, 2147483694L, "Staple", "No");
        updateTable2(ut2, 2147483694L);
        insertTable2(it2, 2147483695L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483695L, "GenerationTime", "Wed Oct 22 14:12:13 CEST 2008");
        insertTable2(it2, 2147483695L, "DocumentID", "21888/1/1//");
        insertTable2(it2, 2147483695L, "PageSequenceId", "000001");
        insertTable2(it2, 2147483695L, "PhysicalID", "No");
        insertTable2(it2, 2147483695L, "Staple", "No");
        updateTable2(ut2, 2147483695L);
        insertTable2(it2, 2147483696L, "PaperLayout", "LETTER");
        insertTable2(it2, 2147483696L, "GenerationTime", "Wed Oct 22 14:12:13 CEST 2008");
        insertTable2(it2, 2147483696L, "DocumentID", "21888/1/1//");
        insertTable2(it2, 2147483696L, "PageSequenceId", "000002");
        insertTable2(it2, 2147483696L, "PhysicalID", "No");
        insertTable2(it2, 2147483696L, "Staple", "No");
        updateTable2(ut2, 2147483696L);

        it1.close();
        it2.close();
        ut2.close();
    }

    /**
     * Add a test case for DERBY-4331 where the rows were not ordered correctly
     * for both ascending and descending order by clause.  
     */
    public void testDerby4331() throws SQLException {
        Statement s;
        ResultSet rs;
        RuntimeStatisticsParser rtsp;
        String [][] desc_result = new String[][] {
        		{"3"},{"3"},{"2"},{"2"},{"2"},{"1"}};
        String [][] asc_result  = new String[][] {
        		{"1"},{"2"},{"2"},{"2"},{"3"},{"3"}};
        
        String sql1 = 
        	"SELECT CS.ID FROM CHANGESETS CS, FILECHANGES FC, "+
        	"REPOSITORIES R, FILES F, AUTHORS A WHERE "+
        	"R.PATH = '/var/tmp/source5923202038296723704opengrok/mercurial' "+
        	"AND F.REPOSITORY = R.ID AND A.REPOSITORY = R.ID AND "+
        	"CS.REPOSITORY = R.ID AND CS.ID = FC.CHANGESET AND F.ID = FC.FILE "+
        	"AND A.ID = CS.AUTHOR AND EXISTS ( "+
        	"SELECT 1 FROM FILES F2 WHERE "+
        	"F2.ID = FC.FILE AND F2.REPOSITORY = R.ID AND "+
        	"F2.PATH LIKE '/%' ESCAPE '#') "+
        	"ORDER BY CS.ID DESC";
        s = createStatement();
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, desc_result);
        
        sql1 = 
        	"SELECT CS.ID FROM --DERBY-PROPERTIES joinOrder=FIXED \n" +
        	"CHANGESETS CS, FILECHANGES FC, REPOSITORIES R, FILES F, "+
        	"AUTHORS A WHERE " +
        	"R.PATH = '/var/tmp/source5923202038296723704opengrok/mercurial' "+
        	"AND F.REPOSITORY = R.ID AND A.REPOSITORY = R.ID AND "+
        	"CS.REPOSITORY = R.ID AND CS.ID = FC.CHANGESET AND "+
        	"F.ID = FC.FILE AND A.ID = CS.AUTHOR AND EXISTS ( "+
        	"SELECT 1 FROM FILES F2 WHERE "+
        	"F2.ID = FC.FILE AND F2.REPOSITORY = R.ID AND "+
        	"F2.PATH LIKE '/%' ESCAPE '#') "+
        	"ORDER BY CS.ID DESC"; 
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, desc_result);

        sql1 =
        	"SELECT CS.ID FROM --DERBY-PROPERTIES joinOrder=FIXED  \n" +
        	"REPOSITORIES R -- DERBY-PROPERTIES constraint=REPOSITORIES_PATH \n"+
        	",FILES F -- DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	",FILECHANGES FC -- DERBY-PROPERTIES constraint=FILECHANGES_FILE_CHANGESET \n"+
        	", AUTHORS A -- DERBY-PROPERTIES constraint=AUTHORS_REPOSITORY_NAME \n"+
        	", CHANGESETS CS -- DERBY-PROPERTIES constraint=CHANGESETS_PRIMARY_ID \n"+
        	"WHERE "+
        	"R.PATH = '/var/tmp/source5923202038296723704opengrok/mercurial' "+
        	"AND F.REPOSITORY = R.ID AND "+
        	"A.REPOSITORY = R.ID AND "+
        	"CS.REPOSITORY = R.ID AND "+
        	"CS.ID = FC.CHANGESET AND "+
        	"F.ID = FC.FILE AND "+
        	"A.ID = CS.AUTHOR AND "+
        	"EXISTS ( SELECT 1 FROM FILES F2 WHERE "+
        	"F2.ID = FC.FILE AND F2.REPOSITORY = R.ID AND "+
        	"F2.PATH LIKE '/%' ESCAPE '#') "+
        	"ORDER BY CS.ID DESC"; 
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, desc_result);

        sql1 =
        	"SELECT CS.ID FROM " +
        	" CHANGESETS CS, FILECHANGES FC, REPOSITORIES R, FILES F, "+
        	"AUTHORS A WHERE "+
        	"R.PATH = '/var/tmp/source5923202038296723704opengrok/mercurial' "+
        	"AND F.REPOSITORY = R.ID AND A.REPOSITORY = R.ID AND "+
        	"CS.REPOSITORY = R.ID AND CS.ID = FC.CHANGESET AND "+
        	"F.ID = FC.FILE AND A.ID = CS.AUTHOR AND EXISTS ( "+
        	"SELECT 1 FROM FILES F2 WHERE F2.REPOSITORY = 1) "+
        	"ORDER BY CS.ID DESC";
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, desc_result);
        
        sql1 = 
        	"SELECT CS.ID FROM --DERBY-PROPERTIES joinOrder=FIXED \n" +
        	"REPOSITORIES R, FILES F, FILECHANGES FC, AUTHORS A, "+
        	"CHANGESETS CS WHERE " +
        	"R.PATH = '/var/tmp/source5923202038296723704opengrok/mercurial' "+
        	"AND F.REPOSITORY = R.ID AND A.REPOSITORY = R.ID AND "+
        	"CS.REPOSITORY = R.ID AND CS.ID = FC.CHANGESET AND "+
        	"F.ID = FC.FILE AND A.ID = CS.AUTHOR AND EXISTS ( "+
        	"SELECT 1 FROM FILES F2 WHERE "+
        	"F2.ID = FC.FILE AND F2.REPOSITORY = R.ID AND "+
        	"F2.PATH LIKE '/%' ESCAPE '#') ORDER BY CS.ID DESC";
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, desc_result);
        
        sql1 =
        	"SELECT CS.ID FROM --DERBY-PROPERTIES joinOrder=FIXED \n"+
        	"REPOSITORIES R --DERBY-PROPERTIES constraint=REPOSITORIES_PATH \n"+
        	", FILES F --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	", FILECHANGES FC --DERBY-PROPERTIES constraint=FILECHANGES_FILE_CHANGESET \n"+
        	", AUTHORS A --DERBY-PROPERTIES constraint=AUTHORS_REPOSITORY_NAME \n"+
        	", CHANGESETS CS --DERBY-PROPERTIES constraint=CHANGESETS_PRIMARY_ID \n"+
        	"WHERE " +
        	"R.PATH = '/var/tmp/source5923202038296723704opengrok/mercurial' "+
        	"AND F.REPOSITORY = R.ID AND A.REPOSITORY = R.ID AND "+
        	"CS.REPOSITORY = R.ID AND CS.ID = FC.CHANGESET AND "+
        	"F.ID = FC.FILE AND A.ID = CS.AUTHOR AND EXISTS ( SELECT 1 "+
        	"FROM FILES F2 --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	"WHERE F2.ID = FC.FILE AND F2.REPOSITORY = R.ID AND "+
        	"F2.PATH LIKE '/%' ESCAPE '#') ORDER BY CS.ID DESC";
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, desc_result);
        
        sql1 = 
        	"SELECT CS.ID FROM --DERBY-PROPERTIES joinOrder=FIXED \n" +
        	"REPOSITORIES R --DERBY-PROPERTIES constraint=REPOSITORIES_PATH \n"+
        	", FILES F --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	", FILECHANGES FC --DERBY-PROPERTIES constraint=FILECHANGES_FILE_CHANGESET \n"+
        	", AUTHORS A --DERBY-PROPERTIES constraint=AUTHORS_REPOSITORY_NAME \n"+
        	", CHANGESETS CS --DERBY-PROPERTIES constraint=CHANGESETS_PRIMARY_ID \n"+
        	"WHERE "+
        	"R.PATH = '/var/tmp/source5923202038296723704opengrok/mercurial' "+
        	"AND F.REPOSITORY = R.ID AND A.REPOSITORY = R.ID AND "+
        	"CS.REPOSITORY = R.ID AND CS.ID = FC.CHANGESET AND "+
        	"F.ID = FC.FILE AND A.ID = CS.AUTHOR AND EXISTS ( "+
        	"SELECT 1 "+
        	"FROM FILES F2 --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	"WHERE F2.ID = FC.FILE )ORDER BY CS.ID DESC";
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, desc_result);
        
        sql1 = 
        	"SELECT CS.ID FROM --DERBY-PROPERTIES joinOrder=FIXED \n"+
        	"REPOSITORIES R --DERBY-PROPERTIES constraint=REPOSITORIES_PATH \n"+
        	", FILES F --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	", FILECHANGES FC --DERBY-PROPERTIES constraint=FILECHANGES_FILE_CHANGESET \n"+
        	", CHANGESETS CS --DERBY-PROPERTIES constraint=CHANGESETS_PRIMARY_ID \n"+
        	"WHERE "+
        	"R.PATH = '/var/tmp/source5923202038296723704opengrok/mercurial' "+
        	"AND F.REPOSITORY = R.ID AND CS.REPOSITORY = R.ID AND "+
        	"CS.ID = FC.CHANGESET AND F.ID = FC.FILE AND EXISTS ("+
        	"SELECT 1 " +
        	"FROM FILES F2 --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	"WHERE F2.ID = FC.FILE) ORDER BY CS.ID DESC";
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, desc_result);
        
        sql1 =
        	"SELECT CS.ID FROM --DERBY-PROPERTIES joinOrder=FIXED \n"+
        	"FILES F --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	", FILECHANGES FC --DERBY-PROPERTIES constraint=FILECHANGES_FILE_CHANGESET \n"+
        	", CHANGESETS CS --DERBY-PROPERTIES constraint=CHANGESETS_PRIMARY_ID \n"+
        	"WHERE CS.ID = FC.CHANGESET AND F.ID = FC.FILE AND EXISTS ( "+
        	"SELECT 1 "+
        	"FROM FILES F2 --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	"WHERE F2.ID = FC.FILE) ORDER BY CS.ID DESC";
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, desc_result);
        
        sql1 = 
        	"SELECT CS.ID FROM --DERBY-PROPERTIES joinOrder=FIXED \n"+
        	"FILES F --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	", FILECHANGES FC --DERBY-PROPERTIES constraint=FILECHANGES_FILE_CHANGESET \n"+
        	", CHANGESETS CS --DERBY-PROPERTIES constraint=CHANGESETS_PRIMARY_ID \n"+
        	"WHERE CS.ID = FC.CHANGESET AND F.ID = FC.FILE AND EXISTS ( "+
        	"SELECT 1 "+
        	"FROM FILES F2 --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	"WHERE F2.ID = FC.FILE) ORDER BY CS.ID";
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, asc_result);

        sql1 = 
        	"SELECT CS.ID FROM --DERBY-PROPERTIES joinOrder=FIXED \n"+
        	"FILES F --DERBY-PROPERTIES constraint=FILES_REPOSITORY_PATH \n"+
        	", FILECHANGES FC --DERBY-PROPERTIES constraint=FILECHANGES_FILE_CHANGESET \n"+
        	", CHANGESETS CS --DERBY-PROPERTIES constraint=CHANGESETS_PRIMARY_ID \n"+
        	"WHERE CS.ID = FC.CHANGESET AND F.ID = FC.FILE "+
        	"ORDER BY CS.ID DESC";
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, desc_result);
    }

    /**
     * Add a test case for DERBY-4240 where the rows were not ordered despite
     * an order by clause. The fix for DERBY-3926 took care of the bug. 
     */
    public void testDerby4240OrderByCase() throws SQLException {
        String sql1 = 
        	"SELECT t1.id, t1.name FROM test2 t2 INNER JOIN test1 t1 "+
        	"ON t2.rel_id = t1.id WHERE t2.entity_id = 1 ORDER BY t1.id ASC";
        Statement s;
        ResultSet rs;
        RuntimeStatisticsParser rtsp;
        String [][] result;

        s = createStatement();
        s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        rs = s.executeQuery(sql1);
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.whatSortingRequired());
        rs = s.executeQuery(sql1);
        result = new String[][] {
                {"101", "Pupy"},{"102", "Tom"}, {"103", "Jerry"}};
        JDBC.assertFullResultSet(rs, result);
    }

    /**
     * Some more tests for order by and sort avoidance logic
     */
    public void testAdditionalOrderByCases() throws SQLException {
        String sql1;
        Statement s;
        ResultSet rs;
        RuntimeStatisticsParser rtsp;
        String [][] result;

        s = createStatement();
        s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        sql1 = "select a.col1, b.col2, c.col2 from a, b, c where c.col1=3 " +
        "order by a.col1, c.col1";
        rs = s.executeQuery(sql1);
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.whatSortingRequired());
        rs = s.executeQuery(sql1);
        result = new String[][] {
                {"1", "2", "3"},{"1", "2", "3"}, {"1", "2", "3"},   
                {"1", "2", "3"},{"1", "2", "3"}, {"1", "2", "3"},   
                {"1", "2", "3"},{"1", "2", "3"}};
        JDBC.assertFullResultSet(rs, result);
        
        sql1 = "select a.col1, b.col2, c.col2 from a, b, c where a.col1=1 "+
        "and b.col1 = 2 and c.col1=3 order by a.col1, b.col1, c.col1";
        rs = s.executeQuery(sql1);
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.whatSortingRequired());
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, result);

        sql1 = "select c.col1, b.col1, a.col1 from a, b, c where a.col1=1 "+
        "and b.col1 = 2 and c.col1=3 order by c.col1, b.col1, a.col1";
        result = new String[][] {
                {"3", "2", "1"},{"3", "2", "1"}, {"3", "2", "1"},   
                {"3", "2", "1"},{"3", "2", "1"}, {"3", "2", "1"},   
                {"3", "2", "1"},{"3", "2", "1"}};
        rs = s.executeQuery(sql1);
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.whatSortingRequired());
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, result);

        sql1 = "select c.col1, b.col1, a.col1 from a, b, c where a.col1=1 "+
        "and b.col1 = 2 and c.col1=3 order by c.col2, b.col2, a.col2";
        rs = s.executeQuery(sql1);
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.whatSortingRequired());
        rs = s.executeQuery(sql1);
        JDBC.assertFullResultSet(rs, result);
    }

    /**
     * Test for forcing a order of tables in the FROM list user optimizer
     * overrides. This ordering of table is going to require us to do sorting.
     * This forced sorting returns the correct result order.
     */
    public void testForceSortWithOptimizerOverrides() throws SQLException {
        Statement s = createStatement();
        String sql1 = "SELECT table1.id, m0.value, m1.value " +
        "FROM  --DERBY-PROPERTIES joinOrder=FIXED \n"+
        "table2  m0, table2 m1, table1 "+
        "WHERE table1.id=m0.id AND m0.name='PageSequenceId' "+
        "AND table1.id=m1.id AND m1.name='PostComponentId' "+
        "AND m1.value='21857' ORDER BY m0.value";

        s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        ResultSet rs = s.executeQuery(sql1);
		RuntimeStatisticsParser rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedTableScan("TABLE2"));
		assertTrue(rtsp.whatSortingRequired());

        rs = s.executeQuery(sql1);
        String[][] result = {
                {"4294967297", "000001", "21857"}, 
                {"2147483653", "000002", "21857"}, 
                {"2147483654", "000003", "21857"}};
        JDBC.assertFullResultSet(rs, result);
    }

    /**
     * Following sql with no overrides also demonstrates the bug where we are 
     * returning the results in wrong order
     */
    public void testWithNoOptimizerOverrides() throws SQLException {
        Statement s = createStatement();

        String sql1 = "SELECT table1.id, m0.value, m1.value FROM table1, " +
        "table2 m0, table2 m1 WHERE table1.id=m0.id AND " +
        "m0.name='PageSequenceId' AND table1.id=m1.id AND " +
        "m1.name='PostComponentId' AND m1.value='21857' " +
        "ORDER BY m0.value";

        s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        ResultSet rs = s.executeQuery(sql1);
		RuntimeStatisticsParser rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedSpecificIndexForIndexScan("TABLE2","KEY3"));
		assertTrue(rtsp.whatSortingRequired());
  
        rs = s.executeQuery(sql1);
        String[][] result = {
                {"4294967297", "000001", "21857"}, 
                {"2147483653", "000002", "21857"}, 
                {"2147483654", "000003", "21857"}};
        JDBC.assertFullResultSet(rs, result);
    }

    /**
     * Test for forcing the index use using optimizer override. This will
     * demonstrate the bug where we are returning the results in wrong order
     */
    public void testForcedIndexUseForWrongOrder() throws SQLException {
        Statement s = createStatement();

        String sql1 = "SELECT table1.id, m0.value, m1.value \n" +
        "FROM  --DERBY-PROPERTIES joinOrder=FIXED \n" + 
        "table2  m1 -- DERBY-PROPERTIES index=key3 \n" +
        ",  table2 m0 -- DERBY-PROPERTIES index=key3 \n"+
        ", table1 " + 
        "WHERE table1.id=m0.id AND m0.name='PageSequenceId' "+
        "AND table1.id=m1.id AND m1.name='PostComponentId' AND "+ 
        " m1.value='21857' ORDER BY m0.value";

		s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        ResultSet rs = s.executeQuery(sql1);
		RuntimeStatisticsParser rtsp = SQLUtilities.getRuntimeStatisticsParser(
				s);
		assertTrue(rtsp.usedSpecificIndexForIndexScan("TABLE2","KEY3"));
		assertTrue(rtsp.whatSortingRequired());

		rs = s.executeQuery(sql1);
        String[][] result = {
                {"4294967297", "000001", "21857"}, 
                {"2147483653", "000002", "21857"}, 
                {"2147483654", "000003", "21857"}};
        JDBC.assertFullResultSet(rs, result);
    }


    /*
     * DERBY-6148. Verifying that permuted join order doesn't
     * erroneously give sort avoidance under certain index access
     * paths.
     */
    public void testDerby6148() throws SQLException {
        Statement s = createStatement();

        createTablesForDerby6148(s);
        insertDataForDerby6148();
        createIndexesForDerby6148(s);

        // This query failed prior to fixing DERBY-6148
        final String brokenQuery =
            "SELECT t.id, t.item, title " +
            "    FROM d6148_tests t" +
            "         -- DERBY-PROPERTIES joinStrategy = NESTEDLOOP, " +
            "                             constraint = d6148_tests_1\n" +
            "       , d6148_item_usage u" +
            "         -- DERBY-PROPERTIES joinStrategy = NESTEDLOOP," +
            "                             constraint = d6148_item_usage_1\n" +
            "    WHERE username = 'MICKEY' AND " +
            "          u.item = t.item " +
            "ORDER BY t.item, title";

        // These queries worked prior to fixing DERBY-6148
        final String goodQuery1 = // changed order of FROM tables here:
            "SELECT t.id, t.item, title " +
            "    FROM d6148_item_usage u" +
            "         -- DERBY-PROPERTIES joinStrategy = NESTEDLOOP," +
            "                             constraint = d6148_item_usage_1\n" +
            "       , d6148_tests t" +
            "         -- DERBY-PROPERTIES joinStrategy = NESTEDLOOP, " +
            "                             constraint = d6148_tests_1\n" +
            "    WHERE username = 'MICKEY' AND " +
            "          u.item = t.item " +
            "ORDER BY t.item, title";

        final String goodQuery2 = // changed ORDER BY column to other equijoin
                                  // predicate column
            "SELECT t.id, t.item, title " +
            "    FROM d6148_tests t" +
            "         -- DERBY-PROPERTIES joinStrategy = NESTEDLOOP, " +
            "                             constraint = d6148_tests_1\n" +
            "       , d6148_item_usage u" +
            "         -- DERBY-PROPERTIES joinStrategy = NESTEDLOOP," +
            "                             constraint = d6148_item_usage_1\n" +
            "    WHERE username = 'MICKEY' AND " +
            "          u.item = t.item " +
            "ORDER BY u.item, title";


        final String[][] expectedRows = getExpectedRowsDerby6148();
        JDBC.assertFullResultSet(s.executeQuery(brokenQuery), expectedRows);
        JDBC.assertFullResultSet(s.executeQuery(goodQuery1), expectedRows);
        JDBC.assertFullResultSet(s.executeQuery(goodQuery2), expectedRows);
    }

    private String[][] getExpectedRowsDerby6148() {
        return new String[][]{
            {"15", "60001", "Test 15         "},
            {"19", "60001", "Test 19         "},
            {"25", "60001", "Test 25         "},
            {"27", "60001", "Test 27         "},
            {"28", "60001", "Test 28         "},
            {"10", "61303", "Test 10         "},
            {"11", "61303", "Test 11         "},
            {"13", "61303", "Test 13         "},
            {"14", "61303", "Test 14         "},
            {"21", "61303", "Test 21         "},
            {"35", "61303", "Test 35         "},
            {"9", "61303", "Test 9          "},
            {"26", "7205731", "Test 26         "},
            {"32", "7205731", "Test 32         "},
            {"4", "7205731", "Test 4          "},
            {"5", "7205731", "Test 5          "},
            {"6", "7205731", "Test 6          "},
            {"7", "7205731", "Test 7          "},
            {"8", "7205731", "Test 8          "},
            {"1", "XY101", "Test 1          "},
            {"12", "XY101", "Test 12         "},
            {"16", "XY101", "Test 16         "},
            {"17", "XY101", "Test 17         "},
            {"18", "XY101", "Test 18         "},
            {"2", "XY101", "Test 2          "},
            {"22", "XY101", "Test 22         "},
            {"23", "XY101", "Test 23         "},
            {"24", "XY101", "Test 24         "},
            {"3", "XY101", "Test 3          "},
            {"31", "XY101", "Test 31         "}};
    }

    private void createTablesForDerby6148(Statement s) throws SQLException {
        s.executeUpdate(
            "create table d6148_tests (" +
            "    id integer not null generated always as identity " +
            "        (start with 1, increment by 1), " +
            "    item varchar(15) not null, " +
            "    title varchar(255) not null)");

        s.executeUpdate(
            "create table d6148_item_usage (" +
            "    username varchar(15) not null, " +
            "    item varchar(15) not null, " +
            "    value smallint default 0)");

        s.executeUpdate(
            "create table d6148_items (" +
            "    item varchar(15) not null, " +
            "    name varchar(255) not null, " +
            "    special char(1) default null)");

        s.executeUpdate(
            "create table d6148_users (" +
            "    username varchar(15) not null, " +
            "    surname varchar(255) not null)");

    }

    private void createIndexesForDerby6148(Statement s) throws SQLException {
        // Create primary/unique indexes
        s.executeUpdate(
            "alter table d6148_items add constraint " +
            "    d6148_items_pk primary key (item)");

        s.executeUpdate(
            "alter table d6148_item_usage add constraint " +
            "    d6148_item_usage_pk primary key (username, item)");

        s.executeUpdate(
            "alter table d6148_users add constraint " +
            "    users_pk primary key (username)");

        s.executeUpdate(
            "alter table d6148_tests add constraint " +
            "    d6148_tests_pk primary key (id)");

        s.executeUpdate(
            "alter table d6148_tests add constraint " +
            "    d6148_tests_1 unique (item, title)");

        // Add foreign key constraints
        s.executeUpdate(
            "alter table d6148_item_usage add constraint " +
            "    d6148_item_usage_2 foreign key (item) references " +
            "    d6148_items (item) on delete cascade on update no action");

        s.executeUpdate(
            "alter table d6148_item_usage add constraint " +
            "    d6148_item_usage_1 foreign key (username) references " +
            "    d6148_users (username) on delete cascade on update no action");

        s.executeUpdate(
            "alter table d6148_tests add constraint " +
            "    d6148_tests_2 foreign key (item) references " +
            "    d6148_items (item) on delete cascade on update no action");
    }

    private void insertDataForDerby6148() throws SQLException {
        String[][] users = {
            {"ADMIN","Administrator"},
            {"MINNIE","MOUSE"},
            {"MICKEY","MOUSE"},
            {"TEST","Test"},
            {"PIED","Piper"},
            {"WINNIE","Pooh"},
            {"DONALD","Duck"},
            {"CLARK","Kent"},
            {"VARG","Veum"},
            {"TOMMY","Tiger"},
            {"USER1","?????"},
            {"DEMO","Demo"},
            {"BRAM","Stoker"},
            {"USER2","???????"},
            {"USER3","?????"}};

        PreparedStatement ps = prepareStatement(
            "insert into d6148_users values (?,?)");

        for (String[] u : users) {
            ps.setString(1, u[0]);
            ps.setString(2, u[1]);
            ps.executeUpdate();
        }

        String[][] items = {
            {"XY101","XY101", null},
            {"61303","61303", null},
            {"7205731","7205731", null},
            {"60001","60001", null},
            {"60001B","60001B", null},
            {"61108","61108", null}};

        ps = prepareStatement(
            "insert into d6148_items values (?,?,?)");

        for (String[] i : items) {
            ps.setString(1, i[0]);
            ps.setString(2, i[1]);
            ps.setString(3, i[2]);
            ps.executeUpdate();
        }

        String[][] tests = {
            {"XY101","Test 1          "},
            {"XY101","Test 2          "},
            {"XY101","Test 3          "},
            {"7205731","Test 4          "},
            {"7205731","Test 5          "},
            {"7205731","Test 6          "},
            {"7205731","Test 7          "},
            {"7205731","Test 8          "},
            {"61303","Test 9          "},
            {"61303","Test 10         "},
            {"61303","Test 11         "},
            {"XY101","Test 12         "},
            {"61303","Test 13         "},
            {"61303","Test 14         "},
            {"60001","Test 15         "},
            {"XY101","Test 16         "},
            {"XY101","Test 17         "},
            {"XY101","Test 18         "},
            {"60001","Test 19         "},
            {"60001B","Test 20         "},
            {"61303","Test 21         "},
            {"XY101","Test 22         "},
            {"XY101","Test 23         "},
            {"XY101","Test 24         "},
            {"60001","Test 25         "},
            {"7205731","Test 26         "},
            {"60001","Test 27         "},
            {"60001","Test 28         "},
            {"60001B","Test 29         "},
            {"60001B","Test 30         "},
            {"XY101","Test 31         "},
            {"7205731","Test 32         "},
            {"60001B","Test 33         "},
            {"60001B","Test 34         "},
            {"61303","Test 35         "}};

        ps = prepareStatement(
            "insert into d6148_tests values (default,?,?)");

        for (String[] t : tests) {
            ps.setString(1, t[0]);
            ps.setString(2, t[1]);
            ps.executeUpdate();
        }

        String[][] item_usage = {
            {"MINNIE","XY101","4"},
            {"MICKEY","XY101","4"},
            {"MICKEY","61303","4"},
            {"MICKEY","7205731","4"},
            {"PIED","61303","2"},
            {"TOMMY","60001","1"},
            {"USER1","60001","0"},
            {"BRAM","60001","2"},
            {"WINNIE","7205731","1"},
            {"MICKEY","60001","4"},
            {"DONALD","60001","2"},
            {"PIED","60001","2"},
            {"VARG","60001","2"},
            {"CLARK","60001","2"},
            {"TEST","60001B","0"},
            {"DEMO","61303","0"},
            {"DONALD","61303","2"},
            {"DONALD","60001B","4"},
            {"DEMO","XY101","0"},
            {"USER2","61303","0"},
            {"USER3","61303","0"},
            {"MICKEY","61108","4"},
            {"MINNIE","60001B","0"}};

        ps = prepareStatement(
            "insert into d6148_item_usage values (?,?,?)");

        for (String[] iu : item_usage) {
            ps.setString(1, iu[0]);
            ps.setString(2, iu[1]);
            ps.setString(3, iu[2]);
            ps.executeUpdate();
        }
    }
}
