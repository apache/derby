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

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import junit.framework.Test;
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

    public static Test suite() {
        return TestConfiguration.defaultSuite(OrderByAndSortAvoidance.class);
    }

    protected void setUp() throws SQLException {
        getConnection().setAutoCommit(false);
        Statement st = createStatement();

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
            
        st.executeUpdate(
        		"INSERT INTO table1 VALUES (2147483649)");
        
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, 'SC2', "
                + "'LaPosteSortPlan=B-W3-S2')");
            
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, "+
        		"'AddressG4', 'BELGIQUE')");
        
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'IsIdenticalToDocumentAddress', 'true')");

        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, "+
        		"'AddressG3', '1180, Bruxelles')");
        
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'DocumentSortingValues', '______21855__1__1')");

        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'ItemSeq', '1')");

        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'BatchTypeInstructions', '')");

        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'Plex', 'S')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'SC3', '=')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'has_address', "+
        		"'true')");
        
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'SubBatchSortingValue', 'BE_B-W3-S2___')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'Language', 'FR')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'logo', 'false')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'SC4', '=')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'InternalAddress', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'AddressG6', '')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'InternalAddressBringer', 'false')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'AddresseeSeq', '1')");
        
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'SC1', 'Country=BE')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, "+
        		"'CommunicationOrderId', '21865')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'BatchTypeId', 'Paper')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'location', "+
        		"'Bla bla bla bla bla bla 99')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, 'SortPlan', "
                + "'B-W3-S2')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'Enveloping', 'C')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'PostComponentId', '21855')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'BatchTypeLabel', "+
        		"'Paper')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'city', 'Bruxelles')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'AddressG7', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'CopyMention', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'AddressG8', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'StapleNbr', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'TLEBundle', "+
        		"'Niveau2')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'AddressG5', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'header', 'true')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier001-0001___________Bla bla bla bla "
                + "bla bla 99____')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'GroupedWith', '')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, "+
        			"'DiversionReason', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'Pliable', 'true')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'TLEBinder', "+
        		"'Niveau1')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'country', 'BE')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'AddressG1', "+
        		"'testOlivier001-0001')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'MentionCode', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'postCode', '1180')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483649, 'SC5', '=')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483649, 'Branding', '1C')");
            
        st.executeUpdate(
        "INSERT INTO table1 VALUES (2147483650)");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'SC2', "+
        		"'LaPosteSortPlan=B-W3-S2')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, 'AddressG4', "
                + "'BELGIQUE')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, "+
        		"'IsIdenticalToDocumentAddress', 'true')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, 'AddressG3', "
                + "'1180, Bruxelles')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, "+
        		"'DocumentSortingValues', '______21855__1__1')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'ItemSeq', '1')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, "+
        		"'BatchTypeInstructions', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'Plex', 'S')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'SC3', '=')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, "
                + "'has_address', 'true')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, " +
        		"'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'Language', 'FR')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'logo', 'false')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'SC4', '=')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, "+
        		"'InternalAddress', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'AddressG6', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, "+
        		"'InternalAddressBringer', 'false')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'AddresseeSeq', '1')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'SC1', 'Country=BE')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, "
                + "'CommunicationOrderId', '21865')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'BatchTypeId', 'Paper')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, 'location', "
                + "'Bla bla bla bla bla bla 99')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'SortPlan', 'B-W3-S2')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'Enveloping', 'C')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'PostComponentId', "+
        		"'21855')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, "
                + "'BatchTypeLabel', 'Paper')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'city', 'Bruxelles')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'AddressG7', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'CopyMention', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'AddressG8', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'StapleNbr', '')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, 'TLEBundle', "
                + "'Niveau2')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'AddressG5', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'header', 'true')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, "+
        		"'EnvelopSortingValue', 'BE1180___testOlivier001-0001"+
        		"___________Bla bla bla bla bla bla 99____')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'GroupedWith', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'AddressG2', "+
        		"'Bla bla bla bla bla bla 99')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, "
                + "'DiversionReason', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'Pliable', 'true')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, 'TLEBinder', "
                + "'Niveau1')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'country', 'BE')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, 'AddressG1', "
                + "'testOlivier001-0001')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'MentionCode', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'postCode', '1180')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483650, 'SC5', '=')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483650, 'Branding', '1C')");
        
        st.executeUpdate(
        		"INSERT INTO table1 VALUES (2147483651)");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, 'SC2', "
                + "'LaPosteSortPlan=B-W3-S2')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'AddressG4', "+
        		"'BELGIQUE')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'IsIdenticalToDocumentAddress', 'true')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'AddressG3', "+
        		"'1180, Bruxelles')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'DocumentSortingValues', '______21856__1__1')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'ItemSeq', '1')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'BatchTypeInstructions', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'Plex', 'S')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'SC3', '=')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'has_address', 'true')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'SubBatchSortingValue', 'BE_B-W3-S2___')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'Language', 'FR')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'logo', 'false')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'SC4', '=')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'InternalAddress', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'AddressG6', '')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'InternalAddressBringer', 'false')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'AddresseeSeq', '1')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'SC1', 'Country=BE')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, "+
        		"'CommunicationOrderId', '21866')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'BatchTypeId', 'Paper')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'location', "+
        		"'Bla bla bla bla bla bla 99')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, 'SortPlan', "
                + "'B-W3-S2')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'Enveloping', 'C')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'PostComponentId', '21856')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'BatchTypeLabel', 'Paper')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'city', 'Bruxelles')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'AddressG7', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'CopyMention', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'AddressG8', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'StapleNbr', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'TLEBundle', 'Niveau2')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'AddressG5', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'header', 'true')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier001-0002___________Bla bla bla bla "
                + "bla bla 99____')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'GroupedWith', '')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'DiversionReason', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'Pliable', 'true')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'TLEBinder', 'Niveau1')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'country', 'BE')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'AddressG1', "+
        		"'testOlivier001-0002')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'MentionCode', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'postCode', '1180')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483651, 'SC5', '=')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483651, 'Branding', '1C')");
            
        st.executeUpdate(
        "INSERT INTO table1 VALUES (2147483652)");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'SC2', 'LaPosteSortPlan=B-W3-S2')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, 'AddressG4', "
                + "'BELGIQUE')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, "+
        		"'IsIdenticalToDocumentAddress', 'true')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, 'AddressG3', "
                + "'1180, Bruxelles')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, "+
        		"'DocumentSortingValues', '______21856__1__1')");

        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, 'ItemSeq', '1')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'BatchTypeInstructions', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483652, 'Plex', 'S')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'SC3', '=')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, "
                + "'has_address', 'true')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483652, 'Language', 'FR')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'logo', 'false')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483652, 'SC4', '=')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'InternalAddress', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483652, 'AddressG6', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'InternalAddressBringer', 'false')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483652, 'AddresseeSeq', '1')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'SC1', 'Country=BE')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, "
                + "'CommunicationOrderId', '21866')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'BatchTypeId', 'Paper')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, 'location', "
                + "'Bla bla bla bla bla bla 99')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'SortPlan', 'B-W3-S2')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483652, 'Enveloping', 'C')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'PostComponentId', '21856')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, "
                + "'BatchTypeLabel', 'Paper')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'city', 'Bruxelles')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483652, 'AddressG7', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'CopyMention', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483652, 'AddressG8', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'StapleNbr', '')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, 'TLEBundle', "
                + "'Niveau2')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'AddressG5', '')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483652, 'header', 'true')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'EnvelopSortingValue',"
        		+ "'BE1180___testOlivier001-0002___________Bla bla bla bla bl,"
        		+ "a bla 99____')");
            
        st.executeUpdate(
        "INSERT INTO table2 VALUES (2147483652, 'GroupedWith', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'AddressG2', 'Bla bla bla bla bla bla 99')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, "
                + "'DiversionReason', '')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'Pliable', 'true')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, 'TLEBinder', "
                + "'Niveau1')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'country', 'BE')");
            
        st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, 'AddressG1', "
                + "'testOlivier001-0002')");
        
        st.executeUpdate(
        		"INSERT INTO table2 VALUES (2147483652, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483652, 'SC5', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483653)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'AddressG3', '1180 Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'DocumentSortingValues', '______21857__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'ItemSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'InternalAddress', '233/621')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'SC1', "
                + "'Requester=ddch257')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, "+
            		"'CommunicationOrderId', '21867')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'location', "+
            		"'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'Enveloping', 'N')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'PostComponentId', '21857')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, "+
            		"'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'TLEBundle', "+
            		"'Niveau2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'header', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier002-0003___________Bla bla bla bla "
                + "bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'GroupedWith', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, "+
            		"'DiversionReason', '001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'TLEBinder', "+
            		"'Niveau1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'AddressG1', "+
            		"'testOlivier002-0003')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'postCode', '1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'Branding', '1C')");
            
            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483654)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'SC2', 'PostCode=1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, "+
            		"'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'AddressG3', "
                + "'1180 Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, "+
            		"'DocumentSortingValues', '______21857__1__1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, "+
            		"'BatchTypeInstructions', 'Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'SC3', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, "+
            		"'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'logo', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'InternalAddress',"+
            		"'233/621')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, "+
            		"'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'SC1', 'Requester=ddch257')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, "
                + "'CommunicationOrderId', '21867')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'SortPlan', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'PostComponentId', '21857')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'city', 'Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'StapleNbr', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'AddressG5', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, "+
            		"'EnvelopSortingValue', "+
            		"'BE1180___testOlivier002-0003___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'AddressG2', "+
            		"'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'country', 'BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'AddressG1', "
                + "'testOlivier002-0003')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'SC5', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483655)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'AddressG3', '1180 Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'DocumentSortingValues', '______21858__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'ItemSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'InternalAddress', '233/621')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'SC1', "
                + "'Requester=ddch257')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, "+
            		"'CommunicationOrderId', '21868')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'location', "+
            		"'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'Enveloping', 'N')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'PostComponentId', '21858')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, "+
            		"'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'TLEBundle', 'Niveau2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'header', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier002-0004___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'GroupedWith', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'DiversionReason', '001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'TLEBinder', 'Niveau1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'AddressG1', "+
            		"'testOlivier002-0004')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'postCode', '1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'Branding', '1C')");
            
            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483656)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'SC2', 'PostCode=1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, "+
            		"'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'AddressG3', "
                + "'1180 Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'DocumentSorting"+
            		"Values', '______21858__1__1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'BatchTypeIn"+
            		"structions', 'Ne pas jeter ces documents.  Ils ont "+
            		"\u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'SC3', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'SubBatchSortin"+
            		"gValue', 'ddch257___1180______')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'logo', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'InternalAddres"+
            		"s', '233/621')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'InternalAddressB"+
            		"ringer', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'SC1', 'Request"+
            		"er=ddch257')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, "
                + "'CommunicationOrderId', '21868')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'BatchTypeId', "+
            		"'233-621-001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'SortPlan', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'PostComponentI"+
            		"d', '21858')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'city', 'Bruxel"+
            		"les')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'StapleNbr', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'AddressG5', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'EnvelopSorting"+
            		"Value', 'BE1180___testOlivier002-0004___________Boulev"+
            		"ard du Souverain, 23____')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'AddressG2', 'B"+
            		"oulevard du Souverain, 23')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'country', 'BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'AddressG1', "
                + "'testOlivier002-0004')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'SC5', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483657)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'SC2', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'AddressG3', 'Broker ville')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'DocumentSortingValues', '______21859__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'ItemSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'SubBatchSortingValue', 'Une info b')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'SC1', "
                + "'DeliveryInformation=Une info bidon')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'CommunicationOrderId', '21869')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'BatchTypeId', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'location', 'rue du broker')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'Enveloping', 'C')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'PostComponentId', '21859')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'BatchTypeLabel', 'BROKER NET')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'city', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'TLEBundle', 'Niveau2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'header', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'EnvelopSortingValue', "
                + "'BE1000___testOlivier003-0005___________rue du "
                + "broker_________________')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'GroupedWith', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'AddressG2', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'DiversionReason', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'TLEBinder', 'Niveau1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'AddressG1', 'testOlivier003-0005')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'postCode', '1000')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'Branding', '1C')");
            
            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483658)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'SC2', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'AddressG3', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'DocumentSortingValues', '______21859__1__1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'BatchTypeInstructions', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'SC3', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'SubBatchSortingValue', 'Une info b')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'logo', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'InternalAddress', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'SC1', 'DeliveryInformation=Une info bidon')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, "
                + "'CommunicationOrderId', '21869')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'BatchTypeId', 'BROKER NET')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'location', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'SortPlan', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'PostComponentId', '21859')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, "
                + "'BatchTypeLabel', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'city', 'Broker ville')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'StapleNbr', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'AddressG5', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'EnvelopSorting"+
            		"Value', 'BE1000___testOlivier003-0005___________rue du"+
            		"broker_________________')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'AddressG2', 'rue du broker')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'country', 'BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'AddressG1', "
                + "'testOlivier003-0005')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'postCode', '1000')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'SC5', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483659)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'SC2', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'AddressG3', 'Broker ville')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'DocumentSortingValues', '______21860__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'ItemSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'SubBatchSortingValue', 'Une info b')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'SC1', "
                + "'DeliveryInformation=Une info bidon')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'CommunicationOrderId', '21870')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'BatchTypeId', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'location', 'rue du broker')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'Enveloping', 'C')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'PostComponentId', '21860')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'BatchTypeLabel', 'BROKER NET')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'city', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'TLEBundle', 'Niveau2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'header', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'EnvelopSortingValue', "
                + "'BE1000___testOlivier003-0006___________rue du "
                + "broker_________________')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'AddressG2', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'DiversionReason', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'TLEBinder', 'Niveau1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'AddressG1', 'testOlivier003-0006')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'postCode', '1000')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'Branding', '1C')");
            
            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483660)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'SC2', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'AddressG3', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'DocumentSortingValues', '______21860__1__1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'BatchTypeInstructions', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'SC3', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'SubBatchSortingValue', 'Une info b')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'logo', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'InternalAddress', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'InternalAddressBringer', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'SC1', 'DeliveryInformation=Une info bidon')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, "
                + "'CommunicationOrderId', '21870')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'BatchTypeId', 'BROKER NET')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'location', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'SortPlan', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'PostComponentId', '21860')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, "
                + "'BatchTypeLabel', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'city', 'Broker ville')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'AddressG5', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'EnvelopSorting"+
            		"Value', 'BE1000___testOlivier003-0006___________rue du"+
            		"broker_________________')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'AddressG2', 'rue du broker')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'country', 'BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'AddressG1', "
                + "'testOlivier003-0006')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'postCode', '1000')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483661)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'SC2', "
                + "'LaPosteSortPlan=B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'AddressG3', '1180, Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'DocumentSortingValues', '______21861__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'ItemSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'SC1', 'Country=BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'CommunicationOrderId', '21871')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'BatchTypeId', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'location', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'SortPlan', "
                + "'B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'Enveloping', 'C')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'PostComponentId', '21861')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'BatchTypeLabel', 'Paper')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'TLEBundle', 'Niveau2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'header', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier004-0007___________Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'GroupedWith', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'DiversionReason', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'AddressG1', 'testOlivier004-0007')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'postCode', '1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'Branding', '1C')");
            
            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483662)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'SC2', 'LaPosteSortPlan=B-W3-S2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'AddressG3', "
                + "'1180, Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'DocumentSortingValues', '______21861__1__1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'BatchTypeInstructions', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'SC3', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'logo', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'InternalAddress', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'SC1', 'Country=BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, "
                + "'CommunicationOrderId', '21871')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'BatchTypeId', 'Paper')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'SortPlan', 'B-W3-S2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'PostComponentId', '21861')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, "
                + "'BatchTypeLabel', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'city', 'Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'StapleNbr', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'AddressG5', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'EnvelopSorting"+
            		"Value', 'BE1180___testOlivier004-0007___________Boulev"+
            		"ard du Souverain, 23____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'AddressG2', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'country', 'BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'AddressG1', "
                + "'testOlivier004-0007')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'SC5', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483663)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'SC2', "
                + "'LaPosteSortPlan=B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'AddressG3', '1180, Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'DocumentSortingValues', '______21862__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'ItemSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'InternalAddress', '')");
                        
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'SC1', 'Country=BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'CommunicationOrderId', '21872')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'BatchTypeId', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'location', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'SortPlan', "
                + "'B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'Enveloping', 'C')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'PostComponentId', '21862')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'BatchTypeLabel', 'Paper')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'header', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier004-0008___________Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'GroupedWith', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'DiversionReason', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'TLEBinder', 'Niveau1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'AddressG1', 'testOlivier004-0008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'postCode', '1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483664)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'SC2', 'LaPosteSortPlan=B-W3-S2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'AddressG3', "
                + "'1180, Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'DocumentSortingValues', '______21862__1__1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'BatchTypeInstructions', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'SC3', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, "
                + "'has_address', 'true')");
            
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'SubBatchSortingValue', 'BE_B-W3-S2___')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'InternalAddress', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'SC1', 'Country=BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, "
                + "'CommunicationOrderId', '21872')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'BatchTypeId', 'Paper')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'SortPlan', 'B-W3-S2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'PostComponentId', '21862')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, "
                + "'BatchTypeLabel', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'EnvelopSortingValue', 'BE1180___testOlivier004-0008___________Bla bla bla bla bla bla 99____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'AddressG2', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'AddressG1', "
                + "'testOlivier004-0008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483665)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'AddressG3', '1180 Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'DocumentSortingValues', '______21863__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'InternalAddress', '233/621')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'SC1', "
                + "'Requester=ddch257')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'CommunicationOrderId', '21873')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'location', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'Enveloping', 'N')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'PostComponentId', '21863')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'BatchTypeLabel', 'Diversion pour test')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier005-0009___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'DiversionReason', '001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'AddressG1', 'testOlivier005-0009')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'postCode', '1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483666)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'SC2', 'PostCode=1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'IsIdenticalToD"+
            		"ocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'AddressG3', "
                + "'1180 Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'DocumentSortin"+
            		"gValues', '______21863__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'BatchTypeInstr"+
            		"uctions', 'Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 fait"+
            		"s pour quelque chose.')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'SubBatchSortingValue', 'ddch257___1180______')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'InternalAddress', '233/621')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'InternalAddressBringer', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'SC1', 'Requester=ddch257')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, "
                + "'CommunicationOrderId', '21873')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'BatchTypeId', '233-621-001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'PostComponentId', '21863')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'EnvelopSorting"+
            		"Value', 'BE1180___testOlivier005-0009___________Boulev"+
            		"ard du Souverain, 23____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'AddressG2', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'AddressG1', "
                + "'testOlivier005-0009')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483667)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'AddressG3', '1180 Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'DocumentSortingValues', '______21864__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'InternalAddress', '233/621')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'SC1', "
                + "'Requester=ddch257')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'CommunicationOrderId', '21874')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'location', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'Enveloping', 'N')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'PostComponentId', '21864')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'BatchTypeLabel', 'Diversion pour test')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier005-0010___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'DiversionReason', '001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'AddressG1', 'testOlivier005-0010')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'postCode', '1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483668)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'SC2', 'PostCode=1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'AddressG3', "
                + "'1180 Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'DocumentSortin"+
            		"gValues', '______21864__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'BatchTypeInstr"+
            		"uctions', 'Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 fa"+
            		"its pour quelque chose.')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'SubBatchSortin"+
            		"gValue', 'ddch257___1180______')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'InternalAddress', '233/621')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'InternalAddressBringer', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'SC1', 'Requester=ddch257')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, "
                + "'CommunicationOrderId', '21874')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'BatchTypeId', '233-621-001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'PostComponentId', '21864')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'EnvelopSorting"+
            		"Value', 'BE1180___testOlivier005-0010___________Boulev"+
            		"ard du Souverain, 23____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'AddressG2', 'B"+
            		"oulevard du Souverain, 23')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'AddressG1', "
                + "'testOlivier005-0010')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483669)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'SC2', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'AddressG3', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'DocumentSortingValues', '______21865__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'SubBatchSortingValue', 'Une info b')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'SC1', "
                + "'DeliveryInformation=Une info bidon')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'CommunicationOrderId', '21875')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'BatchTypeId', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'location', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'Enveloping', 'C')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'PostComponentId', '21865')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'BatchTypeLabel', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'city', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'EnvelopSortingValue', "
                + "'BE1000___testOlivier006-0011___________rue du "
                + "broker_________________')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'AddressG2', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'DiversionReason', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'AddressG1', 'testOlivier006-0011')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'postCode', '1000')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483670)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'SC2', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'AddressG3', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'DocumentSortingValues', '______21865__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'BatchTypeInstructions', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'SubBatchSortingValue', 'Une info b')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'InternalAddress', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'InternalAddressBringer', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'SC1', 'DeliveryInformation=Une info bidon')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, "
                + "'CommunicationOrderId', '21875')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'BatchTypeId', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'location', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'PostComponentId', '21865')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, "
                + "'BatchTypeLabel', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'city', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'EnvelopSorting"+
            		"Value', 'BE1000___testOlivier006-0011___________rue du"+
            		"broker_________________')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'AddressG2', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'AddressG1', "
                + "'testOlivier006-0011')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'postCode', '1000')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483671)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'SC2', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'AddressG3', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'DocumentSortingValues', '______21866__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'SubBatchSortingValue', 'Une info b')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'SC1', "
                + "'DeliveryInformation=Une info bidon')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'CommunicationOrderId', '21876')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'BatchTypeId', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'location', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'Enveloping', 'C')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'PostComponentId', '21866')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'BatchTypeLabel', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'city', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'EnvelopSortingValue', "
                + "'BE1000___testOlivier006-0012___________rue du "
                + "broker_________________')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'AddressG2', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'DiversionReason', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'AddressG1', 'testOlivier006-0012')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'postCode', '1000')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483672)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'SC2', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'AddressG3', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'DocumentSortingValues', '______21866__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'BatchTypeInstructions', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'SubBatchSortingValue', 'Une info b')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'InternalAddress', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'InternalAddressBringer', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'SC1', 'DeliveryInformation=Une info bidon')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, "
                + "'CommunicationOrderId', '21876')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'BatchTypeId', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'location', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'PostComponentId', '21866')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, "
                + "'BatchTypeLabel', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'city', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'EnvelopSorting"+
            		"Value', 'BE1000___testOlivier006-0012___________rue du"+
            		"broker_________________')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'AddressG2', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'AddressG1', "
                + "'testOlivier006-0012')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'postCode', '1000')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483673)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'SC2', "
                + "'LaPosteSortPlan=B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'AddressG3', '1180, Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'DocumentSortingValues', '______21867__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'SC1', 'Country=BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'CommunicationOrderId', '21877')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'BatchTypeId', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'location', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'SortPlan', "
                + "'B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'Enveloping', 'C')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'PostComponentId', '21867')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'BatchTypeLabel', 'Paper')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier007-0013___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'DiversionReason', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'AddressG1', 'testOlivier007-0013')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'postCode', '1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483674)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'SC2', 'LaPosteSortPlan=B-W3-S2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'AddressG3', "
                + "'1180, Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'DocumentSortingValues', '______21867__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'BatchTypeInstructions', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'SubBatchSortingValue', 'BE_B-W3-S2___')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'InternalAddress', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'InternalAddressBringer', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'SC1', 'Country=BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, "
                + "'CommunicationOrderId', '21877')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'BatchTypeId', 'Paper')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'SortPlan', 'B-W3-S2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'PostComponentId', '21867')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, "
                + "'BatchTypeLabel', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'EnvelopSorting"+
            		"Value', 'BE1180___testOlivier007-0013___________Boulev"+
            		"ard du Souverain, 23____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'AddressG2', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'AddressG1', "
                + "'testOlivier007-0013')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483675)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'SC2', "
                + "'LaPosteSortPlan=B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'AddressG3', '1180, Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'DocumentSortingValues', '______21868__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'ItemSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'SC1', 'Country=BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'CommunicationOrderId', '21878')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'BatchTypeId', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'location', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'SortPlan', "
                + "'B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'Enveloping', 'C')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'PostComponentId', '21868')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'BatchTypeLabel', 'Paper')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'TLEBundle', 'Niveau2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'header', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier007-0014___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'GroupedWith', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'DiversionReason', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'TLEBinder', 'Niveau1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'AddressG1', 'testOlivier007-0014')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'postCode', '1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'Branding', '1C')");
            
            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483676)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'SC2', 'LaPosteSortPlan=B-W3-S2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'AddressG3', "
                + "'1180, Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'DocumentSortingValues', '______21868__1__1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'BatchTypeInstructions', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'SC3', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'logo', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'InternalAddress', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'SC1', 'Country=BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, "
                + "'CommunicationOrderId', '21878')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'BatchTypeId', 'Paper')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'SortPlan', 'B-W3-S2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'PostComponentId', '21868')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, "
                + "'BatchTypeLabel', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'city', 'Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'StapleNbr', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'AddressG5', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'EnvelopSorting"+
            		"Value', 'BE1180___testOlivier007-0014___________Boulev"+
            		"ard du Souverain, 23____')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'AddressG2', 'B"+
            		"oulevard du Souverain, 23')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'country', 'BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'AddressG1', "
                + "'testOlivier007-0014')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'SC5', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483677)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'AddressG3', '1180 Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'DocumentSortingValues', '______21869__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'InternalAddress', '233/621')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'SC1', "
                + "'Requester=ddch257')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'CommunicationOrderId', '21879')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'location', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'Enveloping', 'N')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'PostComponentId', '21869')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'BatchTypeLabel', 'Diversion pour test')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier008-0015___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'DiversionReason', '001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'AddressG1', 'testOlivier008-0015')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'postCode', '1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483678)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'SC2', 'PostCode=1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'AddressG3', "
                + "'1180 Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'DocumentSortingValues', '______21869__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'BatchTypeInstructions', 'Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'SubBatchSortingValue', 'ddch257___1180______')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'InternalAddress', '233/621')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'InternalAddressBringer', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'SC1', 'Requester=ddch257')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, "
                + "'CommunicationOrderId', '21879')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'BatchTypeId', '233-621-001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'PostComponentId', '21869')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'EnvelopSortingValue', 'BE1180___testOlivier008-0015___________Bla bla bla bla bla bla 99____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'AddressG2', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'AddressG1', "
                + "'testOlivier008-0015')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483679)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'AddressG3', '1180 Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'DocumentSortingValues', '______21870__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'InternalAddress', '233/621')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'SC1', "
                + "'Requester=ddch257')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'CommunicationOrderId', '21880')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'location', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'Enveloping', 'N')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'PostComponentId', '21870')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'BatchTypeLabel', 'Diversion pour test')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier008-0016___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'DiversionReason', '001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'AddressG1', 'testOlivier008-0016')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'postCode', '1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483680)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'SC2', 'PostCode=1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'AddressG3', "
                + "'1180 Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'DocumentSortingValues', '______21870__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'BatchTypeInstr"+
            		"uctions', 'Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 fai"+
            		"ts pour quelque chose.')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'SubBatchSortingValue', 'ddch257___1180______')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'InternalAddress', '233/621')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'InternalAddressBringer', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'SC1', 'Requester=ddch257')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, "
                + "'CommunicationOrderId', '21880')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'BatchTypeId', '233-621-001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'PostComponentId', '21870')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'EnvelopSorting"+
            		"Value', 'BE1180___testOlivier008-0016___________Boulev"+
            		"ard du Souverain, 23____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'AddressG2', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'AddressG1', "
                + "'testOlivier008-0016')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483681)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'SC2', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'AddressG3', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'DocumentSortingValues', '______21871__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'SubBatchSortingValue', 'Une info b')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'SC1', "
                + "'DeliveryInformation=Une info bidon')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'CommunicationOrderId', '21881')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'BatchTypeId', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'location', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'Enveloping', 'C')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'PostComponentId', '21871')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'BatchTypeLabel', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'city', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'EnvelopSortingValue', "
                + "'BE1000___testOlivier009-0017___________rue du "
                + "broker_________________')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'AddressG2', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'DiversionReason', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'AddressG1', 'testOlivier009-0017')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'postCode', '1000')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483682)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'SC2', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'AddressG3', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'DocumentSortingValues', '______21871__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'BatchTypeInstructions', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'SubBatchSortingValue', 'Une info b')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'InternalAddress', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'InternalAddressBringer', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'SC1', 'DeliveryInformation=Une info bidon')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, "
                + "'CommunicationOrderId', '21881')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'BatchTypeId', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'location', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'PostComponentId', '21871')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, "
                + "'BatchTypeLabel', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'city', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'EnvelopSorting"+
            		"Value', 'BE1000___testOlivier009-0017___________rue du"+
            		"broker_________________')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'AddressG2', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'AddressG1', "
                + "'testOlivier009-0017')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'postCode', '1000')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483683)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'SC2', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'AddressG3', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'DocumentSortingValues', '______21872__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'SubBatchSortingValue', 'Une info b')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'SC1', "
                + "'DeliveryInformation=Une info bidon')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'CommunicationOrderId', '21882')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'BatchTypeId', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'location', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'Enveloping', 'C')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'PostComponentId', '21872')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'BatchTypeLabel', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'city', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'EnvelopSortingValue', "
                + "'BE1000___testOlivier009-0018___________rue du "
                + "broker_________________')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'AddressG2', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'DiversionReason', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'AddressG1', 'testOlivier009-0018')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'postCode', '1000')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483684)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'SC2', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'AddressG3', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'DocumentSortingValues', '______21872__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'BatchTypeInstructions', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'SubBatchSortingValue', 'Une info b')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'InternalAddress', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'InternalAddressBringer', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'SC1', 'DeliveryInformation=Une info bidon')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, "
                + "'CommunicationOrderId', '21882')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'BatchTypeId', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'location', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'PostComponentId', '21872')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, "
                + "'BatchTypeLabel', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'city', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'EnvelopSortingValue', 'BE1000___testOlivier009-0018___________rue du broker_________________')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'AddressG2', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'AddressG1', "
                + "'testOlivier009-0018')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'postCode', '1000')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483685)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'SC2', "
                + "'LaPosteSortPlan=B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'AddressG3', '1180, Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'DocumentSortingValues', '______21873__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'SC1', 'Country=BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'CommunicationOrderId', '21883')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'BatchTypeId', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'location', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'SortPlan', "
                + "'B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'Enveloping', 'C')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'PostComponentId', '21873')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'BatchTypeLabel', 'Paper')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier010-0019___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'DiversionReason', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'AddressG1', 'testOlivier010-0019')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'postCode', '1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483686)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'SC2', 'LaPosteSortPlan=B-W3-S2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'AddressG3', "
                + "'1180, Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'DocumentSortingValues', '______21873__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'BatchTypeInstructions', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'SubBatchSortingValue', 'BE_B-W3-S2___')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'InternalAddress', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'InternalAddressBringer', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'SC1', 'Country=BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, "
                + "'CommunicationOrderId', '21883')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'BatchTypeId', 'Paper')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'SortPlan', 'B-W3-S2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'PostComponentId', '21873')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, "
                + "'BatchTypeLabel', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'AddressG5', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'EnvelopSortingValue', 'BE1180___testOlivier010-0019___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'AddressG2', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'country', 'BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'AddressG1', "
                + "'testOlivier010-0019')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'SC5', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483687)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'SC2', "
                + "'LaPosteSortPlan=B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'AddressG3', '1180, Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'DocumentSortingValues', '______21874__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'ItemSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'SC1', 'Country=BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'CommunicationOrderId', '21884')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'BatchTypeId', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'location', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'SortPlan', "
                + "'B-W3-S2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'Enveloping', 'C')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'PostComponentId', '21874')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'BatchTypeLabel', 'Paper')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'TLEBundle', 'Niveau2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'header', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier010-0020___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'GroupedWith', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'DiversionReason', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'TLEBinder', 'Niveau1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'AddressG1', 'testOlivier010-0020')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'postCode', '1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'Branding', '1C')");
            
            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483688)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'SC2', 'LaPosteSortPlan=B-W3-S2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'AddressG3', "
                + "'1180, Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'DocumentSortingValues', '______21874__1__1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'BatchTypeInstructions', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'SC3', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'SubBatchSortingValue', 'BE_B-W3-S2___')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'logo', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'InternalAddress', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'SC1', 'Country=BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, "
                + "'CommunicationOrderId', '21884')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'BatchTypeId', 'Paper')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'SortPlan', 'B-W3-S2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'PostComponentId', '21874')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, "
                + "'BatchTypeLabel', 'Paper')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'city', 'Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'StapleNbr', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'AddressG5', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'EnvelopSortingValue', 'BE1180___testOlivier010-0020___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'AddressG2', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'country', 'BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'AddressG1', "
                + "'testOlivier010-0020')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'SC5', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483689)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'AddressG3', '1180 Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'DocumentSortingValues', '______21875__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'ItemSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'InternalAddress', '233/621')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'SC1', "
                + "'Requester=ddch257')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'CommunicationOrderId', '21885')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'location', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'Enveloping', 'N')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'PostComponentId', '21875')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'TLEBundle', 'Niveau2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'header', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier011-0021___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'GroupedWith', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'DiversionReason', '001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'TLEBinder', 'Niveau1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'AddressG1', 'testOlivier011-0021')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'postCode', '1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'Branding', '1C')");
            
            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483690)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'SC2', 'PostCode=1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'AddressG3', "
                + "'1180 Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'DocumentSortingValues', '______21875__1__1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'BatchTypeInstructions', 'Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'SC3', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'logo', 'false')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'InternalAddress', '233/621')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'SC1', 'Requester=ddch257')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, "
                + "'CommunicationOrderId', '21885')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'SortPlan', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'PostComponentId', '21875')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'city', 'Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'StapleNbr', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'AddressG5', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'EnvelopSortingValue', 'BE1180___testOlivier011-0021___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'AddressG2', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'country', 'BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'AddressG1', "
                + "'testOlivier011-0021')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'SC5', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483691)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'AddressG3', '1180 Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'DocumentSortingValues', '______21876__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'ItemSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'InternalAddress', '233/621')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'SC1', "
                + "'Requester=ddch257')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'CommunicationOrderId', '21886')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'location', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'Enveloping', 'N')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'PostComponentId', '21876')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'TLEBundle', 'Niveau2')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'header', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier011-0022___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'GroupedWith', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'DiversionReason', '001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'TLEBinder', 'Niveau1')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'AddressG1', 'testOlivier011-0022')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'postCode', '1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'Branding', '1C')");
            
            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483692)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'SC2', 'PostCode=1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'AddressG3', "
                + "'1180 Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'DocumentSortingValues', '______21876__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'BatchTypeInstructions', 'Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'SubBatchSortingValue', 'ddch257___1180______')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'InternalAddress', '233/621')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'InternalAddressBringer', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'SC1', 'Requester=ddch257')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, "
                + "'CommunicationOrderId', '21886')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'BatchTypeId', '233-621-001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'PostComponentId', '21876')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'EnvelopSortingValue', 'BE1180___testOlivier011-0022___________Bla bla bla bla bla bla 99____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'AddressG2', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'AddressG1', "
                + "'testOlivier011-0022')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483693)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'SC2', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'AddressG3', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'DocumentSortingValues', '______21877__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'SubBatchSortingValue', 'Une info b')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'SC1', "
                + "'DeliveryInformation=Une info bidon')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'CommunicationOrderId', '21887')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'BatchTypeId', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'location', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'Enveloping', 'C')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'PostComponentId', '21877')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'BatchTypeLabel', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'city', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'EnvelopSortingValue', "
                + "'BE1000___testOlivier012-0023___________rue du "
                + "broker_________________')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'AddressG2', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'DiversionReason', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'AddressG1', 'testOlivier012-0023')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'postCode', '1000')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483694)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'SC2', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'AddressG3', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'DocumentSortingValues', '______21877__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'BatchTypeInstructions', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'SubBatchSortingValue', 'Une info b')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'InternalAddress', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'InternalAddressBringer', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'SC1', 'DeliveryInformation=Une info bidon')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, "
                + "'CommunicationOrderId', '21887')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'BatchTypeId', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'location', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'PostComponentId', '21877')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, "
                + "'BatchTypeLabel', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'city', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'EnvelopSortingValue', 'BE1000___testOlivier012-0023___________rue du broker_________________')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'AddressG2', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'AddressG1', "
                + "'testOlivier012-0023')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'postCode', '1000')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (2147483695)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'SC2', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'IsIdenticalToDocumentAddress', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'AddressG3', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'DocumentSortingValues', '______21878__1__1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'ItemSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'BatchTypeInstructions', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'SubBatchSortingValue', 'Une info b')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'logo', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'InternalAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'InternalAddressBringer', 'false')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'AddresseeSeq', '1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'SC1', "
                + "'DeliveryInformation=Une info bidon')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'CommunicationOrderId', '21888')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'BatchTypeId', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'location', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'Enveloping', 'C')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'PostComponentId', '21878')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'BatchTypeLabel', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'city', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'StapleNbr', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'TLEBundle', 'Niveau2')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'header', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'EnvelopSortingValue', "
                + "'BE1000___testOlivier012-0024___________rue du "
                + "broker_________________')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'AddressG2', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'DiversionReason', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'TLEBinder', 'Niveau1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'AddressG1', 'testOlivier012-0024')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'postCode', '1000')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (2147483696)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'SC2', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'IsIdenticalToDocumentAddress', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'AddressG3', "
                + "'Broker ville')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'DocumentSortingValues', '______21878__1__1')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'ItemSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'BatchTypeInstructions', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'SubBatchSortingValue', 'Une info b')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'logo', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'InternalAddress', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'InternalAddressBringer', 'false')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'AddresseeSeq', '1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'SC1', 'DeliveryInformation=Une info bidon')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, "
                + "'CommunicationOrderId', '21888')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'BatchTypeId', 'BROKER NET')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'location', "
                + "'rue du broker')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'Enveloping', 'C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'PostComponentId', '21878')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, "
                + "'BatchTypeLabel', 'BROKER NET')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'city', 'Broker ville')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'StapleNbr', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'TLEBundle', "
                + "'Niveau2')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'header', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'EnvelopSortingValue', 'BE1000___testOlivier012-0024___________rue du broker_________________')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'AddressG2', 'rue du broker')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, "
                + "'DiversionReason', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'TLEBinder', "
                + "'Niveau1')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'AddressG1', "
                + "'testOlivier012-0024')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'postCode', '1000')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483649, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'GenerationTime', 'Wed Oct 22 14:12:10 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483649, 'DocumentID', '21865/1/1//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483649, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483649, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483649 AND name='has_address'");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483650, 'GenerationTime', 'Wed Oct 22 14:12:10 CEST 2008')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, "
                + "'DocumentID', '21865/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483650, 'PageSequenceId', '000002')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483650, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483650, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483650 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483651, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'GenerationTime', 'Wed Oct 22 14:12:10 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483651, 'DocumentID', '21866/1/1//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483651, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483651, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483651 AND name='has_address'");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483652, 'GenerationTime', 'Wed Oct 22 14:12:10 CEST 2008')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, "
                + "'DocumentID', '21866/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483652, 'PageSequenceId', '000002')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483652, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483652, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483652 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (4294967297)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, "
                + "'IsIdenticalToDocumentAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'AddressG3', '1180 Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, "
                + "'DocumentSortingValues', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'ItemSeq', '0')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'InternalAddress', '233/621')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'InternalAddressBringer', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'AddresseeSeq', '0')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'SC1', 'Requester=ddch257')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, "
                + "'CommunicationOrderId', '21867')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'BatchTypeId', '233-621-001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'PostComponentId', '21857')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'StapleNbr', 'NO')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'EnvelopSortingValue', 'BE1180___testOlivier002-0003___________Bla bla bla bla bla bla 99____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'AddressG2', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'AddressG1', 'testOlivier002-0003')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'postCode', '1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'env_type', 'I')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'PaperLayout', 'ADDINT')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, "
                + "'GenerationTime', 'Wed Oct 22 14:12:10 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'DocumentID', '21867/0/0//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (4294967297, 'cover_page', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (4294967297, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=4294967297 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'GenerationTime', 'Wed Oct 22 14:12:10 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'DocumentID', '21867/1/1//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, "
                + "'PageSequenceId', '000002')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483653, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483653, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483653 AND name='has_address'");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'GenerationTime', 'Wed Oct 22 14:12:10 CEST 2008')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, "
                + "'DocumentID', '21867/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'PageSequenceId', '000003')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483654, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483654, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483654 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (6442450945)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, "
                + "'IsIdenticalToDocumentAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'AddressG3', '1180 Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, "
                + "'DocumentSortingValues', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'ItemSeq', '0')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'InternalAddress', '233/621')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'InternalAddressBringer', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'AddresseeSeq', '0')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'SC1', 'Requester=ddch257')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, "
                + "'CommunicationOrderId', '21868')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'BatchTypeId', '233-621-001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'PostComponentId', '21858')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'StapleNbr', 'NO')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'EnvelopSortingValue', 'BE1180___testOlivier002-0004___________Bla bla bla bla bla bla 99____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'AddressG2', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'AddressG1', 'testOlivier002-0004')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'postCode', '1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'env_type', 'I')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'PaperLayout', 'ADDINT')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'DocumentID', '21868/0/0//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (6442450945, 'cover_page', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (6442450945, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=6442450945 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'DocumentID', '21868/1/1//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, "
                + "'PageSequenceId', '000002')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483655, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483655, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483655 AND name='has_address'");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, "
                + "'DocumentID', '21868/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'PageSequenceId', '000003')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483656, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483656, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483656 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'DocumentID', '21869/1/1//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483657, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483657, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483657 AND name='has_address'");

            st.executeUpdate(
                "INSERT INTO table1 VALUES (8589934593)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'SC2', 'PostCode=1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'AddressG4', "
                + "'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'IsIdenticalToDocumentAddress', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'AddressG3', "
                + "'1180 Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'DocumentSortingValues', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'ItemSeq', '0')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'BatchTypeInstructions', 'Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'SC3', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'SubBatchSortingValue', 'ddch257___1180______')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'SC4', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, "
                + "'InternalAddress', '233/621')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'AddressG6', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, "
                + "'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'AddresseeSeq', '0')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'SC1', "
                + "'Requester=ddch257')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'CommunicationOrderId', '21873')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, "
                + "'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'location', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'Enveloping', 'N')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, "
                + "'PostComponentId', '21863')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'BatchTypeLabel', 'Diversion pour test')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'AddressG7', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'StapleNbr', 'NO')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'AddressG5', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier005-0009___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'GroupedWith', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'AddressG2', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'DiversionReason', '001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'country', 'BE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'AddressG1', "
                + "'testOlivier005-0009')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'MentionCode', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'SC5', '=')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'env_type', 'I')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'Branding', '1C')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, "
                + "'DocumentID', '21869/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'PageSequenceId', '000002')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483658, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483658, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483658 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'DocumentID', '21870/1/1//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483659, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483659, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483659 AND name='has_address'");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, "
                + "'DocumentID', '21870/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'PageSequenceId', '000002')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483660, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483660, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483660 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'DocumentID', '21871/1/1//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483661, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483661, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483661 AND name='has_address'");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, "
                + "'DocumentID', '21871/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'PageSequenceId', '000002')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483662, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483662, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483662 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'DocumentID', '21872/1/1//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483663, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483663, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483663 AND name='has_address'");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, "
                + "'DocumentID', '21872/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'PageSequenceId', '000002')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483664, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483664, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483664 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (10737418241)");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'AddressG4', 'BELGIQUE')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, "
                + "'IsIdenticalToDocumentAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'AddressG3', '1180 Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, "
                + "'DocumentSortingValues', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'ItemSeq', '0')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'Plex', 'S')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'has_address', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'Language', 'FR')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'InternalAddress', '233/621')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'InternalAddressBringer', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'AddresseeSeq', '0')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'SC1', 'Requester=ddch257')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, "
                + "'CommunicationOrderId', '21874')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'BatchTypeId', '233-621-001')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'SortPlan', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'PostComponentId', '21864')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'city', 'Bruxelles')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'CopyMention', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'StapleNbr', 'NO')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'AddressG8', '')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'EnvelopSortingValue', 'BE1180___testOlivier005-0010___________Bla bla bla bla bla bla 99____')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'AddressG2', 'Bla bla bla bla bla bla 99')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'Pliable', 'true')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'AddressG1', 'testOlivier005-0010')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'postCode', '1180')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'env_type', 'I')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'PaperLayout', 'ADDINT')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'DocumentID', '21873/0/0//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (8589934593, 'cover_page', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (8589934593, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=8589934593 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'DocumentID', '21873/1/1//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, "
                + "'PageSequenceId', '000002')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483665, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483665, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483665 AND name='has_address'");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, "
                + "'DocumentID', '21873/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'PageSequenceId', '000003')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483666, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483666, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483666 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'PaperLayout', 'ADDINT')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'DocumentID', '21874/0/0//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (10737418241, "
                + "'cover_page', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (10737418241, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=10737418241 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'DocumentID', '21874/1/1//')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, "
                + "'PageSequenceId', '000002')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483667, 'PhysicalID', 'No')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483667, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483667 AND name='has_address'");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, "
                + "'DocumentID', '21874/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'PageSequenceId', '000003')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483668, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483668, 'Staple', 'No')");

            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483668 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'PaperLayout', 'LETTER')");

            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'DocumentID', '21875/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483669, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483669, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483669 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, "
                + "'DocumentID', '21875/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'PageSequenceId', '000002')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483670, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483670, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483670 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (12884901889)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, "
                + "'IsIdenticalToDocumentAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'AddressG3', '1180 Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, "
                + "'DocumentSortingValues', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'ItemSeq', '0')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'InternalAddress', '233/621')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'AddresseeSeq', '0')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'SC1', 'Requester=ddch257')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, "
                + "'CommunicationOrderId', '21879')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'SortPlan', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'PostComponentId', '21869')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'city', 'Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'StapleNbr', 'NO')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'EnvelopSortingValue', 'BE1180___testOlivier008-0015___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'AddressG2', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'AddressG1', 'testOlivier008-0015')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'postCode', '1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'env_type', 'I')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'DocumentID', '21876/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483671, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483671, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483671 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, "
                + "'DocumentID', '21876/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'PageSequenceId', '000002')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483672, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483672, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483672 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'DocumentID', '21877/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483673, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483673, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483673 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, "
                + "'DocumentID', '21877/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'PageSequenceId', '000002')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483674, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483674, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483674 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'DocumentID', '21878/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483675, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483675, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483675 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'GenerationTime', 'Wed Oct 22 14:12:11 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, "
                + "'DocumentID', '21878/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'PageSequenceId', '000002')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483676, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483676, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483676 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (15032385537)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, "
                + "'IsIdenticalToDocumentAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'AddressG3', '1180 Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, "
                + "'DocumentSortingValues', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'ItemSeq', '0')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'InternalAddress', '233/621')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'AddresseeSeq', '0')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'SC1', 'Requester=ddch257')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, "
                + "'CommunicationOrderId', '21880')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'SortPlan', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'PostComponentId', '21870')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'city', 'Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'StapleNbr', 'NO')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'EnvelopSortingValue', 'BE1180___testOlivier008-0016___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'AddressG2', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'AddressG1', 'testOlivier008-0016')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'postCode', '1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'env_type', 'I')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'PaperLayout', 'ADDINT')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, "
                + "'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'DocumentID', '21879/0/0//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (12884901889, "
                + "'cover_page', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (12884901889, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=12884901889 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'DocumentID', '21879/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, "
                + "'PageSequenceId', '000002')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483677, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483677, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483677 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, "
                + "'DocumentID', '21879/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'PageSequenceId', '000003')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483678, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483678, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483678 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'PaperLayout', 'ADDINT')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, "
                + "'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'DocumentID', '21880/0/0//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (15032385537, "
                + "'cover_page', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (15032385537, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=15032385537 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'DocumentID', '21880/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, "
                + "'PageSequenceId', '000002')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483679, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483679, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483679 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, "
                + "'DocumentID', '21880/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'PageSequenceId', '000003')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483680, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483680, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483680 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table1 VALUES (17179869185)");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'SC2', "
                + "'PostCode=1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, "
                + "'IsIdenticalToDocumentAddress', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'AddressG3', '1180 Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, "
                + "'DocumentSortingValues', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'ItemSeq', '0')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, "
                + "'BatchTypeInstructions', 'Ne pas jeter ces "
                + "documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'Plex', 'S')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'SC3', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'has_address', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, "
                + "'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'Language', 'FR')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'SC4', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'InternalAddress', '233/621')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'AddressG6', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'AddresseeSeq', '0')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'SC1', 'Requester=ddch257')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, "
                + "'CommunicationOrderId', '21885')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'location', "
                + "'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'SortPlan', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'Enveloping', 'N')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'PostComponentId', '21875')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, "
                + "'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'city', 'Bruxelles')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'AddressG7', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'CopyMention', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'StapleNbr', 'NO')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'AddressG8', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'AddressG5', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'EnvelopSortingValue', 'BE1180___testOlivier011-0021___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'GroupedWith', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'AddressG2', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, "
                + "'DiversionReason', '001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'Pliable', 'true')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'country', 'BE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'AddressG1', 'testOlivier011-0021')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'MentionCode', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'postCode', '1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'SC5', '=')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'env_type', 'I')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, 'Branding', '1C')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'DocumentID', '21881/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483681, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483681, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483681 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, "
                + "'DocumentID', '21881/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'PageSequenceId', '000002')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483682, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483682, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483682 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'DocumentID', '21882/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483683, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483683, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483683 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, "
                + "'DocumentID', '21882/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'PageSequenceId', '000002')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483684, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483684, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483684 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'DocumentID', '21883/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483685, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483685, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483685 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, "
                + "'DocumentID', '21883/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'PageSequenceId', '000002')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483686, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483686, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483686 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'DocumentID', '21884/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483687, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483687, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483687 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table1 VALUES (19327352833)");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'SC2', 'PostCode=1180')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'AddressG4', 'BELGIQUE')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'IsIdenticalToDocumentAddress', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'AddressG3', '1180 Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'DocumentSortingValues', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'ItemSeq', '0')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'BatchTypeInstructions', 'Ne pas jeter ces documents.  Ils ont \u00e9t\u00e9 faits pour quelque chose.')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'Plex', 'S')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'SC3', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'has_address', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'SubBatchSortingValue', 'ddch257___1180______')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'Language', 'FR')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'SC4', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'InternalAddress', '233/621')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'AddressG6', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'InternalAddressBringer', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'AddresseeSeq', '0')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'SC1', "
                + "'Requester=ddch257')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'CommunicationOrderId', '21886')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'BatchTypeId', '233-621-001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'location', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'SortPlan', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'Enveloping', 'N')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'PostComponentId', '21876')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'BatchTypeLabel', 'Diversion pour test')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'city', 'Bruxelles')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'AddressG7', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'CopyMention', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'StapleNbr', 'NO')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'AddressG8', '')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'AddressG5', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'EnvelopSortingValue', "
                + "'BE1180___testOlivier011-0022___________Bla bla bla bla bla bla 99____')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'GroupedWith', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'AddressG2', 'Bla bla bla bla bla bla 99')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'DiversionReason', '001')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'Pliable', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'country', 'BE')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'AddressG1', 'testOlivier011-0022')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'MentionCode', '')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'postCode', '1180')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'SC5', '=')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, 'env_type', 'I')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'Branding', '1C')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, "
                + "'DocumentID', '21884/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'PageSequenceId', '000002')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483688, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483688, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483688 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'PaperLayout', 'ADDINT')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, "
                + "'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'DocumentID', '21885/0/0//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (17179869185, "
                + "'cover_page', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (17179869185, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=17179869185 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'DocumentID', '21885/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, "
                + "'PageSequenceId', '000002')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483689, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483689, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483689 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'GenerationTime', 'Wed Oct 22 14:12:12 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, "
                + "'DocumentID', '21885/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'PageSequenceId', '000003')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483690, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483690, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483690 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'PaperLayout', 'ADDINT')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'GenerationTime', 'Wed Oct 22 14:12:13 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'DocumentID', '21886/0/0//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (19327352833, "
                + "'cover_page', 'true')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (19327352833, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=19327352833 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'GenerationTime', 'Wed Oct 22 14:12:13 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'DocumentID', '21886/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, "
                + "'PageSequenceId', '000002')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483691, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483691, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483691 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'GenerationTime', 'Wed Oct 22 14:12:13 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, "
                + "'DocumentID', '21886/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'PageSequenceId', '000003')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483692, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483692, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483692 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'GenerationTime', 'Wed Oct 22 14:12:13 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'DocumentID', '21887/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483693, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483693, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483693 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'GenerationTime', 'Wed Oct 22 14:12:13 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, "
                + "'DocumentID', '21887/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'PageSequenceId', '000002')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483694, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483694, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483694 "
                + "AND name='has_address'");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'GenerationTime', 'Wed Oct 22 14:12:13 CEST 2008')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'DocumentID', '21888/1/1//')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, "
                + "'PageSequenceId', '000001')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483695, 'PhysicalID', 'No')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483695, 'Staple', 'No')");
            
            st.executeUpdate(
            		"UPDATE table2 SET value='true' WHERE id=2147483695 AND name='has_address'");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, "
                + "'PaperLayout', 'LETTER')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'GenerationTime', 'Wed Oct 22 14:12:13 CEST 2008')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, "
                + "'DocumentID', '21888/1/1//')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'PageSequenceId', '000002')");
            
            st.executeUpdate(
                "INSERT INTO table2 VALUES (2147483696, 'PhysicalID', 'No')");
            
            st.executeUpdate(
            		"INSERT INTO table2 VALUES (2147483696, 'Staple', 'No')");
            
            st.executeUpdate(
                "UPDATE table2 SET value='true' WHERE id=2147483696 "
                + "AND name='has_address'");
            
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

            getConnection().commit();
            st.close();
    }

    protected void tearDown() throws Exception {
        Statement stmt = createStatement();
        rollback();
        stmt.executeUpdate("drop table table2");
        stmt.executeUpdate("drop table table1");
        stmt.executeUpdate("drop table a");
        stmt.executeUpdate("drop table b");
        stmt.executeUpdate("drop table c");
        //drop tables needed for DERBY-4240
        stmt.executeUpdate("drop table test1");
        stmt.executeUpdate("drop table test2");
        stmt.close();
        commit();
        super.tearDown();
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
		assertTrue(rtsp.usedTableScan("TABLE1"));
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
		assertTrue(rtsp.usedTableScan("TABLE1"));
		assertTrue(rtsp.whatSortingRequired());

		rs = s.executeQuery(sql1);
        String[][] result = {
                {"4294967297", "000001", "21857"}, 
                {"2147483653", "000002", "21857"}, 
                {"2147483654", "000003", "21857"}};
        JDBC.assertFullResultSet(rs, result);
    }
}
