/*
 *
 * Derby - Class SURQueryMixTest
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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import junit.extensions.TestSetup;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for Scrollable Updatable ResultSet (SUR). This TestCase tests
 * scrolling (navigation), updates (using updateXXX() and updateRow() or
 * positioned updates), deletion of records (using deleteRow() or positioned 
 * deletes) of ResultSets.
 */
public class SURQueryMixTest extends SURBaseTest
{
    /**
     * Constructor
     * @param model name of data model for this TestCase
     * @param query to use for producing the resultset
     * @param cursorName name of cursor
     * @param positioned flag to determine if the Test should use positioned
     *        updates/deletes instead of updateRow() and deleteRow()
     */
    public SURQueryMixTest(final String model, final String query, 
                           final String cursorName, final boolean positioned) 
    {
        super("SURQueryMixTest{Model=" + model + ",Query=" +query + ",Cursor=" 
                + cursorName + ",Positioned=" + positioned + "}");
        this.query = query;
        this.cursorName = cursorName;
        this.positioned = positioned;
        this.checkRowUpdated = false;
        this.checkRowDeleted = false;
    }

    /**
     * Test SUR properties of the query
     */
    public void runTest() 
        throws SQLException
    {
        println(query);
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        DatabaseMetaData dbMeta = getConnection().getMetaData();
                
        if (dbMeta.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)) {
            checkRowDeleted = true;
        }
        
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_UPDATABLE);
        
        s.setCursorName(cursorName);
        ResultSet rs = s.executeQuery(query);

        checkRowUpdated = dbMeta.ownUpdatesAreVisible(rs.getType());        
        checkRowDeleted = dbMeta.ownDeletesAreVisible(rs.getType());
        
        // Create map with rows
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        Map<Integer, String> rows = createRowMap(rs);
        
        // Set of rows which are updated (contains Integer with position in RS)
        final Set<Integer> updatedRows = new HashSet<Integer>();
        
        // Set of rows which are deleted (contains Integer with position in RS)
        final Set<Integer> deletedRows = new HashSet<Integer>();
                
        // Test navigation
        testNavigation(rs, rows, updatedRows, deletedRows);
        
        // Only test updatability if the ResultSet is updatable:
        // (Note: this enables the test do run successfully even if
        // scrollable updatable resultsets are not implemented. 
        // If SUR is not implemented, a well behaved JDBC driver will 
        // downgrade the concurrency mode to READ_ONLY).
        // SUR may be implemented incrementally, i.e first in embedded mode
        // then in the network driver.)
        if (rs.getConcurrency()==ResultSet.CONCUR_UPDATABLE) {
        
            // update a random sample of 2 records
            updateRandomSampleOfNRecords(rs, rows, updatedRows, 2); 
            testNavigation(rs, rows, updatedRows, deletedRows); 
            
            // update a random sample of 5 records
            updateRandomSampleOfNRecords(rs, rows, updatedRows, 5); 
            testNavigation(rs, rows, updatedRows, deletedRows); 
            
            // update a random sample of 10 records
            updateRandomSampleOfNRecords(rs, rows, updatedRows, 10); 
            testNavigation(rs, rows, updatedRows, deletedRows); 
            
            // delete a random sample of 2 records
            deleteRandomSampleOfNRecords(rs, rows, deletedRows, 2);
            testNavigation(rs, rows, updatedRows, deletedRows); 
            
            // delete a random sample of 5 records
            deleteRandomSampleOfNRecords(rs, rows, deletedRows, 5);
            testNavigation(rs, rows, updatedRows, deletedRows); 
            
            // delete a random sample of 10 records
            deleteRandomSampleOfNRecords(rs, rows, deletedRows, 10);
            testNavigation(rs, rows, updatedRows, deletedRows); 
        } else {
            assertTrue("ResultSet concurrency downgraded to CONCUR_READ_ONLY",
                       false);
        }
        
        rs.close();
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
//IC see: https://issues.apache.org/jira/browse/DERBY-1701
        s.close();
    }
    
    /**
     * Creates a Map of the values in the ResultSet. 
     * The key object in the map, is the postion in the 
     * ResultSet (Integer 1..n), while the value is a
     * concatenation of the strings for all columns in the row.
     */
    private Map<Integer, String> createRowMap(final ResultSet rs)
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        final Map<Integer, String> rows = new HashMap<Integer, String>();
        rs.beforeFirst();
        assertTrue("Unexpected return from isBeforeFirst()",
                   rs.isBeforeFirst());
        
        int i = 0;
        int sum = 0;
        int expectedSum = 0;
        boolean checkSum = true;
        while (rs.next()) {
            expectedSum += i;
            i++;
            String row = getRowString(rs);
            println(row);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            rows.put(i, row);
            sum += rs.getInt(1);
            if (rs.getInt(1) < 0) {
                checkSum = false;
            }
        }
        if (i<SURDataModelSetup.recordCount) {
            checkSum = false;
        }
        
        assertTrue("Unexpected return from isAfterLast()", rs.isAfterLast());
        
        if (checkSum) {
            assertEquals("Sum for column 1 is not correct", expectedSum, sum);
        }
        
        return rows;
    }

    /**
     * Create a random sample of rows
     * @param rows Map to create sample from
     * @param k number of rows in the sample
     * @return a list containing k elements of rows
     **/
    private List createRandomSample(final Map<Integer, String> rows, int k) {
        Random r = new Random();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        ArrayList<Integer> sampledKeys = new ArrayList<Integer>();
        int n = 0;        
        for (Integer key : rows.keySet()) {
            n++;            
            if (n<=k) {
                sampledKeys.add(key);
            } else {
                // sampledKeys now has a size of k
                double d = r.nextDouble();
                // p = probability of going into the sample
                double p = (double) k / (double) n; 
                if (d<p) {
                    // Replace a random value from the sample with the new value
                    int keyToReplace = r.nextInt(k);
                    sampledKeys.set(keyToReplace, key);
                }
            }
        }
        return sampledKeys;
    }
    
    /**
     * Delete a random sample of n records in the resultset
     * @param rs result set to be updated
     * @param rows map of rows, will also be updated
     * @param deletedRows set of rows being deleted (position in RS)
     * @param k number of records to be deleted
     */
    private void deleteRandomSampleOfNRecords(final ResultSet rs, 
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                                              final Map<Integer, String> rows,
                                              final Set<Integer> deletedRows,
                                              final int k) 
        throws SQLException
    {
        List sampledKeys = createRandomSample(rows, k);
        println("Sampled keys:" + sampledKeys);
        ResultSetMetaData meta = rs.getMetaData();
        for (Iterator i = sampledKeys.iterator(); i.hasNext();) {
            Integer key = (Integer) i.next();
            rs.absolute(key.intValue());            
            if (rs.rowDeleted()) continue; // skip deleting row if already deleted
            if (positioned) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
//IC see: https://issues.apache.org/jira/browse/DERBY-1701
                createStatement().executeUpdate
                        ("DELETE FROM T1 WHERE CURRENT OF \"" + cursorName + 
                         "\"");
            } else {
                rs.deleteRow();
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-690
            rs.relative(0);
            println("Deleted row " + key);
            // Update the rows table
            rows.put(key, getRowString(rs));
            
            // Update the updatedRows set
            deletedRows.add(key);
        }
    }
    
    /**
     * Update a random sample of n records in the resultset
     * @param rs result set to be updated
     * @param rows map of rows, will also be updated
     * @param updatedRows set of being updated (position in RS)
     * @param k number of records to be updated
     */
    private void updateRandomSampleOfNRecords(final ResultSet rs, 
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                                              final Map<Integer, String> rows,
                                              final Set<Integer> updatedRows,
                                              final int k) 
        throws SQLException
    {
        List sampledKeys = createRandomSample(rows, k);
        println("Sampled keys:" + sampledKeys);
        ResultSetMetaData meta = rs.getMetaData();
        for (Iterator i = sampledKeys.iterator(); i.hasNext();) {
            Integer key = (Integer) i.next();
            rs.absolute(key.intValue());            
            
            if (positioned) {
                updatePositioned(rs, meta);
                rs.relative(0); // If this call is not here, the old values are
                                // returned in rs.getXXX calls
            } else {
                updateRow(rs, meta);
            }
            // Update the rows table
            rows.put(key, getRowString(rs));
            
            // Update the updatedRows set
            updatedRows.add(key);
        }
    }

    /**
     * Updates the current row in the ResultSet using updateRow()
     * @param rs ResultSet to be updated
     * @param meta meta for the ResultSet
     **/
    private void updateRow(final ResultSet rs, final ResultSetMetaData meta) 
        throws SQLException
    {
        for (int column = 1; column<=meta.getColumnCount(); column++) {
            if (meta.getColumnType(column)==Types.INTEGER) {
                // Set to negative value
                rs.updateInt(column, -rs.getInt(column));
            } else {
                rs.updateString(column, "UPDATED_" + rs.getString(column));
            }
        }
        rs.updateRow();
    }
    
    /**
     * Updates the current row in the ResultSet using updateRow()
     * @param rs ResultSet to be updated
     * @param meta meta for the ResultSet
     **/
    private void updatePositioned(final ResultSet rs, 
                                  final ResultSetMetaData meta) 
        throws SQLException                          
    {
        StringBuffer sb = new StringBuffer();
        sb.append("UPDATE T1 SET ");
        for (int column = 1; column<=meta.getColumnCount(); column++) {
            sb.append(meta.getColumnName(column));
            sb.append("=?");
            if (column<meta.getColumnCount()) {
                sb.append(",");
            }
        }
        sb.append(" WHERE CURRENT OF \"");
        sb.append(cursorName);
        sb.append("\"");
        println(sb.toString());
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
//IC see: https://issues.apache.org/jira/browse/DERBY-1701
        PreparedStatement ps = prepareStatement(sb.toString());
        
        for (int column = 1; column<=meta.getColumnCount(); column++) {
           if (meta.getColumnType(column)==Types.INTEGER) {
                // Set to negative value
                ps.setInt(column, -rs.getInt(column));
            } else {
                ps.setString(column, "UPDATED_" + rs.getString(column));
            }
        }
        assertEquals("Expected one row to be updated", 1, ps.executeUpdate());        
    }
    
    
    /**
     * Tests navigation in ResultSet.
     * @param rs ResultSet to test navigation of. 
     *                     Needs to be scrollable
     * @param rows a sample of the rows which are in the ResultSet. Maps
     *                   position to a concatenation of the string values
     * @param updatedRows a integer set of which rows that have been 
     *            updated. Used to test rowUpdated()
     * @param deletedRows a integer set of which rows that have been
     *            deleted. Used to test rowDeleted()
     */
    private void testNavigation(final ResultSet rs, final Map rows, 
                                final Set updatedRows, final Set deletedRows) 
        throws SQLException
    {        
        rs.afterLast();
        {
            int i = rows.size();
            while (rs.previous()) {
                String rowString = getRowString(rs);
                assertEquals("Navigating with rs.previous(). The row is " +
                             "different compared to the value when navigating " +
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                             "forward.", rows.get(i), rowString);
                
                
                if (checkRowUpdated && updatedRows.contains(i)) {
                    assertTrue("Expected rs.rowUpdated() to return true on " + 
                               "updated row " + rowString, rs.rowUpdated());
                } 
                if (checkRowDeleted && deletedRows.contains(i)) {
                    assertTrue("Expected rs.rowDeleted() to return true on " + 
                               "deleted row " + rowString, rs.rowDeleted());
                } 
                i--;
            }
        }
        // Test absolute
        for (int i = 1; i <= rows.size(); i++) {
            assertTrue("Unexpected return from absolute()", rs.absolute(i));
            String rowString = getRowString(rs);
            assertEquals("Navigating with rs.absolute(). The row is " +
                         "different compared to the value" +
                         " when navigating forward.", 
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                         rows.get(i),
                         rowString);
            if (checkRowUpdated && updatedRows.contains(i)) {
                assertTrue("Expected rs.rowUpdated() to return true on " +
                           "updated row " + rowString, rs.rowUpdated());
            }
            if (checkRowDeleted && deletedRows.contains(i)) {
                assertTrue("Expected rs.rowDeleted() to return true on " +
                           "deleted row " + rowString, rs.rowDeleted());
            }
        }
        assertFalse("Unexpected return from absolute()", rs.absolute(0));
        assertTrue("Unexpected return from isBeforeFirst()", 
                   rs.isBeforeFirst());
        assertFalse("Unexpected return from absolute()", 
                    rs.absolute(rows.size() + 1));
        assertTrue("Unexpected return from isAfterLast()", rs.isAfterLast());
        assertTrue("Unexpected return from absolute()", rs.absolute(-1));
        assertTrue("Unexpected return from isLast()", rs.isLast());
        assertTrue("Unexpected return from absolute()", rs.absolute(1));
        assertTrue("Unexpected return from isFirst()", rs.isFirst());
        
        // Test relative
        {
            rs.beforeFirst();
            assertTrue("Unexptected return from isBeforeFirst()", 
                       rs.isBeforeFirst());
            
            int relativePos = rows.size();
            assertTrue("Unexpected return from relative()", 
                       rs.relative(relativePos)); 
            
            // Should now be on the last row
            assertTrue("Unexptected return from isLast()", rs.isLast());
            assertEquals("Navigating with rs.relative(+). " +
                         "A tuple was different compared to the value" +
                         " when navigating forward.", 
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                         rows.get(relativePos),
                         getRowString(rs));
            
            assertTrue("Unexpected return from relative()", 
                       rs.relative((-relativePos + 1))); 
            
            // Should now be on the first row
            assertTrue("Unexptected return from isFirst()", rs.isFirst());
            
            assertEquals("Navigating with rs.relative(-). " + 
                         "A tuple was different compared to the value" +
                         " when navigating forward.", 
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                         rows.get(1),
                         getRowString(rs));
            
        }
        // Test navigation in the end of the ResultSet
        rs.afterLast();
        assertTrue("Unexpected return from isAfterLast()", rs.isAfterLast());
        assertTrue("Unexpected return from previous()", rs.previous());
        assertTrue("Unexpected return from isLast()", rs.isLast());
        assertFalse("Unexpected return from next()", rs.next());
        assertTrue("Unexpected return from isAfterLast()", rs.isAfterLast());
        rs.last();
        assertTrue("Unexpected return from isLast()", rs.isLast());
        assertFalse("Unexpected return from next()", rs.next());
        assertTrue("Unexpected return from isAfterLast()", rs.isAfterLast());


        // Test navigation in the beginning of the ResultSet
        rs.beforeFirst();
        assertTrue("Unexpected return from isBeforeFirst()", 
                   rs.isBeforeFirst());
        assertTrue("Unexpected return from next()", rs.next());
        assertTrue("Unexpected return from isFirst", rs.isFirst());
        assertFalse("Unexpected return from previous()", rs.previous());
        assertTrue("Unexpected return from isBeforeFirst()", 
                   rs.isBeforeFirst());
        
        rs.first();
        assertTrue("Unexpected return from isFirst", rs.isFirst());
        assertFalse("Unexpected return from previous()", rs.previous());
        assertTrue("Unexpected return from isBeforeFirst()", 
                   rs.isBeforeFirst());
    }

    /**
     * Get a concatenation of the values of the 
     * current Row in the ResultSet
     */
    private String getRowString(final ResultSet rs) 
        throws SQLException
    {
        int numberOfColumns = rs.getMetaData().getColumnCount();
        StringBuffer sb = new StringBuffer();
        if (rs.rowDeleted()) return "";
        for (int i = 1; i <= numberOfColumns; i++) {
            sb.append(rs.getString(i));
            if (i < numberOfColumns) { 
                sb.append(','); 
            }
        }
        return sb.toString();
    }
    
    private final String query;
    private final String cursorName;
    private final boolean positioned;
    private boolean checkRowUpdated;
    private boolean checkRowDeleted;
    
    private final static String[] selectConditions = new String[] {
        "WHERE c like 'T%'",
        " ",        
        "WHERE b > 5",
        "WHERE id >= a",
        "WHERE id > 1 and id < 900",
        "WHERE id = 1",
        "WHERE id in (1,3,4,600,900,955,966,977,978)",
        "WHERE a in (1,3,4,600,9200,955,966,977,978)",
        "WHERE a>2 and a<9000"
    };
    
    private final static String[] projectConditions = new String[] {
        "id,c,a,b",
        "id,c",
        "a,b",
        "*",
        "id,a,b,c",        
        "id,a",           
        "a,b,c",        
        "a,c"
    };
    
    private static BaseTestSuite createTestCases(final String modelName) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite();
        for (int doPos = 0; doPos<2; doPos++) {
            boolean positioned = doPos>0; // true if to use positioned updates

            for (int i = 0; i < selectConditions.length; i++) {
                for (int j = 0; j < projectConditions.length; j++) {
                    final String cursorName = "cursor_" + i + "_" + j;
                    
                    final String stmtString = "SELECT " + projectConditions[j] +
                            " FROM T1 " + selectConditions[i];
                    suite.addTest(new SURQueryMixTest(modelName, stmtString, cursorName, 
                                              positioned));
                }
            }
        }
        return suite;
    }
    
    /**
     * Run in client and embedded.
     */
    public static Test suite() 
    {   
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite mainSuite = new BaseTestSuite("SURQueryMixTest suite");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2021
        mainSuite.addTest(baseSuite("SURQueryMixTest:embedded"));
        mainSuite.addTest(
                TestConfiguration.clientServerDecorator(
                        baseSuite("SURQueryMixTest:client")));
        
        return mainSuite;
        
    }
  
    /**
     * The suite contains all testcases in this class running on different data models
     */
    private static Test baseSuite(String name) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite mainSuite = new BaseTestSuite(name);
      
        // Iterate over all data models and decorate the tests:
        for (Iterator i = SURDataModelSetup.SURDataModel.values().iterator();
             i.hasNext();) {
            
            SURDataModelSetup.SURDataModel model =
                (SURDataModelSetup.SURDataModel) i.next();
            
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            BaseTestSuite suite = createTestCases(model.toString());
            TestSetup decorator = new SURDataModelSetup(suite, model);
            mainSuite.addTest(decorator);
        }
        return mainSuite;
    }
}
