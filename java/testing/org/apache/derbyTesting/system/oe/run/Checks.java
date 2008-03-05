/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.test.Checks
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.system.oe.run;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBCPerfTestCase;
import org.apache.derbyTesting.system.oe.util.HandleCheckError;
import org.apache.derbyTesting.system.oe.util.OEChecks;

/**
 * Do some checks on the Order Entry database.
 */
public class Checks extends JDBCPerfTestCase {

    /**
     * Warehouse scaling factor
     */
    private short scale = 1;
    
    /**
     * Run checks on OE database
     */
    private OEChecks check = null;
    
    public Checks(String name)
    {
        super(name);
    }
    /**
     * @param name -
     *            test name
     * @param scale
     *            -warehouse scale factor
     */
    public Checks(String name, short scale) {
        super(name);
        this.scale = scale;

    }

    public void setUp() throws Exception
    {
        this.check = new OEChecks();
        check.initialize(new HandleCheckError() {
            public void handleCheckError(String error) {
                fail(error);
            }
        }, getConnection(),scale);
    }
    /**
     * Return suite of tests that checks the row counts for all the tables in
     * the Order Entry bechmark.
     * 
     * @param scale
     */
    public static Test checkAllRowCounts(short scale) {
        TestSuite suite = new TestSuite("Order Entry -Check Row Counts");
        suite.addTest(new Checks("testWarehouseRows", scale));
        suite.addTest(new Checks("testStockRows", scale));
        suite.addTest(new Checks("testItemRows", scale));
        suite.addTest(new Checks("testCustomerRows", scale));
        suite.addTest(new Checks("testDistrictRows", scale));
        suite.addTest(new Checks("testOrdersRows", scale));
        suite.addTest(new Checks("testNewOrdersRows", scale));
        suite.addTest(new Checks("testOrderLineRows", scale));
        suite.addTest(new Checks("testHistoryRows", scale));

        return suite;

    }
    
    /**
     * Consistency checks per Section 3.3.2 of TPC-C spec
     */
    public static Test consistencyChecks()
    {
        TestSuite suite = new TestSuite("Order Entry -Consistency checks");
        suite.addTest(new Checks("testCondition1"));
        suite.addTest(new Checks("testCondition2"));
        suite.addTest(new Checks("testCondition3"));
        suite.addTest(new Checks("testCondition4"));
        
        return suite;
    }

    /**
     * @return suite of tests that perform certain consistency checks on the OE
     *         database
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("OrderEntry - checks");
        suite.addTest(checkAllRowCounts((short) 1));
        suite.addTest(consistencyChecks());
        
        return suite;
    }

    /**
     * Section 3.3.2.1 of TPC-C specification. Entries in the WAREHOUSE and
     * DISTRICT tables must satisfy the relationship: W_YTD = sum(D_YTD) for
     * each warehouse defined by (W_ID = D_W_ID).
     */
    public void testCondition1() throws SQLException {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("SELECT W.W_ID,W.W_YTD, SUM(D.D_YTD) " +
                        "from WAREHOUSE W , DISTRICT D where W.W_ID=D.D_W_ID" +
                        " GROUP BY W.W_ID,W.W_YTD");
        while (rs.next()) {
            println("W_ID"+ rs.getShort(1)+ "W_YTD)="
                    + rs.getString(2)
                    + "SUM(D_YTD)=" + rs.getString(3));
            // retrieve decimal values as string.
            // to allow Order Entry to be
            // run on J2ME/CDC/Foundation which does not support BigDecimal.
            Assert.assertEquals(
                    "Condition#1: W_YTD = sum(D_YTD) not " +
                    "satisfied for W_ID="+ rs.getShort(1),
                    rs.getString(2),rs.getString(3));
        }
        commit();
        rs.close();
        s.close();
    }

    /**
     * Section 3.3.2.2 Consistency Condition 2 (TPC-C spec) Entries in the
     * DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
     * D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID) for each district defined by
     * (D_W_ID = O_W_ID = NO_W_ID) and (D_ID = O_D_ID = NO_D_ID). This condition
     * does not apply to the NEW-ORDER table for any districts which have no
     * outstanding new orders (i.e., the number of rows is zero).
     */
    public void testCondition2() throws SQLException {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("SELECT D.D_ID, D.D_W_ID,D.D_NEXT_O_ID, "+
                        "MAX(O.O_ID),MAX(N.NO_O_ID) FROM NEWORDERS N, " +
                        "DISTRICT D, ORDERS O WHERE D.D_W_ID=O.O_W_ID " +
                        "AND D.D_W_ID = N.NO_W_ID AND D.D_ID = O.O_D_ID " +
                        "AND D.D_ID = N.NO_D_ID GROUP BY " +
                        "D.D_ID,D.D_W_ID,D.D_NEXT_O_ID");
        
        while ( rs.next() )
        {
            println("D_ID="+rs.getShort(1)+"D_W_ID="+rs.getShort(2) +
                    "D_NEXT_O_ID="+ rs.getInt(3) +"MAX(O.O_ID)="+rs.getInt(4) +
                    "MAX(N.NO_O_ID)="+rs.getInt(5));
            Assert.assertEquals("Condition#2 (a), (D_NEXT_O_ID -1) = MAX(O_ID) " +
                    "not satisfied for D_ID="+rs.getShort(1)+
                    "D_W_ID="+rs.getShort(2)
                    , (rs.getInt(3)-1),rs.getInt(4));
            Assert.assertEquals("Condition#2 (b), (D_NEXT_O_ID -1 = MAX(NO_O_ID) " +
                    "not satisfied for D_ID="+rs.getShort(1)+
                    " D_W_ID="+rs.getShort(2)
                    , (rs.getInt(3)-1),rs.getInt(5));
            Assert.assertEquals("Condition#2 (c), MAX(O_ID) = MAX(NO_O_ID) " +
                    " not satisfied for D_ID="+rs.getShort(1)+
                    " D_W_ID="+rs.getShort(2)
                    , rs.getInt(4),rs.getInt(5));
        }
        commit();
        rs.close();
        s.close();
            
    }

    /**
     * 3.3.2.3 Consistency Condition 3 
     * Entries in the NEW-ORDER table must
     * satisfy the relationship: max(NO_O_ID) - min(NO_O_ID) + 1 = [number of
     * rows in the NEW-ORDER table for this district] for each district defined
     * by NO_W_ID and NO_D_ID. This condition does not apply to any districts
     * which have no outstanding new orders (i.e., the number of rows is zero).
     */
    public void testCondition3() throws SQLException {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("SELECT NO_W_ID,NO_D_ID," +
                        "MAX(NO_O_ID) - MIN(NO_O_ID) +1, COUNT(*)" +
                        " FROM NEWORDERS GROUP BY NO_W_ID,NO_D_ID");
        while (rs.next()) {
            println("NO_W_ID=" + rs.getShort(1) + ",NO_D_ID="
                    + rs.getShort(2) + "Max(NO_O_ID)-MIN(NO_O_ID)+1 ="
                    + rs.getInt(3) + " Num_rows_NO for this district="
                    + rs.getInt(4));
            Assert.assertEquals(
                    "Condition#3 (max(NO_O_ID) - min(NO_O_ID) + 1) = [number of"+
                    " rows in the NEW-ORDER table for district] not satisfied " +
                    "for NO_W_ID="+rs.getShort(1)+" NO_D_ID="+rs.getShort(2),
                    rs.getInt(3),rs.getInt(4));
        }
        commit();
        rs.close();
        s.close();

    }
    
   /**
     * 3.3.2.4 Consistency Condition 4 
     * Entries in the ORDER and ORDER-LINE
     * tables must satisfy the relationship: sum(O_OL_CNT) = [number of rows in
     * the ORDER-LINE table for this district] for each district defined by
     * (O_W_ID = OL_W_ID) and (O_D_ID = OL_D_ID).
     */
    public void testCondition4() throws SQLException {
        
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("SELECT " +
                        "T1.SUM_OL_CNT,T2.NUM_OL_ROWS, T1.O_W_ID," +
                        "T1.O_D_ID FROM " +
                        "(SELECT O.O_W_ID, O.O_D_ID, " +
                        "SUM(CAST(O.O_OL_CNT AS BIGINT)) AS " +
                        "SUM_OL_CNT FROM ORDERS " +
                        "O GROUP BY O.O_W_ID,O.O_D_ID) T1 ," +
                        "(SELECT OL_W_ID,OL_D_ID,COUNT(*) AS " +
                        "NUM_OL_ROWS FROM ORDERLINE " +
                        "OL GROUP BY OL.OL_W_ID,OL.OL_D_ID) T2" +
                        " WHERE (T1.O_W_ID = T2.OL_W_ID) AND" +
                        " (T1.O_D_ID = T2.OL_D_ID)");    
        while(rs.next())
        {
            println("Sum of ol_cnt"+ rs.getLong(1) 
                    +" Num_rows_OL="+rs.getInt(1));
            Assert.assertEquals("Condition#4 sum(O_OL_CNT) = [number of rows"
                    +" in the ORDER-LINE table for this district]not satisfied"
                    +"for O_W_ID="+rs.getShort(3)+",O_D_ID="+rs.getShort(4),
                    rs.getLong(1),rs.getLong(2));
        }
        commit();
        rs.close();
        s.close();

    }
    
    /**
     * Test cardinality of WAREHOUSE table
     * 
     * @throws Exception
     */
    public void testWarehouseRows() throws Exception {
        check.testWarehouseRows();
    }

    /**
     * Test cardinality of STOCK table
     * 
     * @throws Exception
     */
    public void testStockRows() throws Exception {
        check.testStockRows();
    }

    /**
     * Test cardinality of ORDERS table
     * 
     * @throws Exception
     */
    public void testOrdersRows() throws Exception {
        check.testOrdersRows();
    }

    /**
     * Test cardinality of DISTRICT table
     * 
     * @throws Exception
     */
    public void testDistrictRows() throws Exception {
        check.testDistrictRows();
    }

    /**
     * Test cardinality of CUSTOMER table
     * 
     * @throws Exception
     */
    public void testCustomerRows() throws Exception {
        check.testCustomerRows();
    }

    /**
     * Test cardinality of ITEM table
     * 
     * @throws Exception
     */
    public void testItemRows() throws Exception {
        check.testItemRows();
    }

    /**
     * Test cardinality of NEWORDERS table
     * 
     * @throws Exception
     */
    public void testNewOrdersRows() throws Exception {
        check.testNewOrdersRows();
    }

    /**
     * Test cardinality of HISTORY table
     * 
     * @throws Exception
     */
    public void testHistoryRows() throws Exception {
        check.testHistoryRows();
    }

    /**
     * Test cardinality of ORDERLINE table
     * 
     * @throws Exception
     */
    public void testOrderLineRows() throws Exception {
        check.testOrderLineRows();
    }
}
