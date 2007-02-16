/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.TriggerTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test triggers.
 *
 */
public class TriggerTest extends BaseJDBCTestCase {

    public TriggerTest(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }
    
    /**
     * Run only in embedded as TRIGGERs are server side logic.
     * @return
     */
    public static Test suite() {
        return new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(TriggerTest.class));
        
    }
    
    protected void initializeConnection(Connection conn) throws SQLException
    {
        conn.setAutoCommit(false);
    }
    
    protected void tearDown() throws Exception
    {
        JDBC.dropSchema(getConnection().getMetaData(),
                getTestConfiguration().getUserName());
        super.tearDown();
    }
    
    /**
     * Test that the action statement of a trigger
     * can work with all datatypes.
     * @throws SQLException
     * @throws IOException 
     */
    public void testTypesInActionStatement() throws SQLException, IOException
    {
        List types = DatabaseMetaDataTest.getSQLTypes(getConnection());
        
        for (Iterator i = types.iterator(); i.hasNext(); )
        {
            actionTypeTest(i.next().toString());
        }
    }
    
    /**
     * Test that the action statement of a trigger
     * can work with a specific datatype.
     * @param type SQL type to be tested
     * @throws SQLException 
     * @throws IOException 
     */
    private void actionTypeTest(String type) throws SQLException, IOException
    {
        System.out.println("type="+type);
        Statement s = createStatement(); 
        
        actionTypesSetup(type);
        
        actionTypesInsertTest(type); 
               
        s.executeUpdate("DROP TABLE T_MAIN");
        s.executeUpdate("DROP TABLE T_ACTION_ROW");
        s.executeUpdate("DROP TABLE T_ACTION_STATEMENT");
        s.close();
        
        commit();
        

    }
   
    /**
     * Setup the tables and triggers for a single type for actionTypeTest
     */
    private void actionTypesSetup(String type) throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE T_MAIN(" +
                "ID INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
                "V " + type + " )");
        s.executeUpdate("CREATE TABLE T_ACTION_ROW(ID INT, A CHAR(1), " +
                "V1 " + type + ", V2 " + type + " )"); 
        s.executeUpdate("CREATE TABLE T_ACTION_STATEMENT(ID INT, A CHAR(1), " +
                "V1 " + type + ", V2 " + type + " )"); 
       
        // ON INSERT copy the typed value V into the action table.
        // Use V twice to ensure there are no issues with values
        // that can be streamed.
        // Two identical actions,  per row and per statement.
        s.executeUpdate("CREATE TRIGGER AIR " +
                "AFTER INSERT ON T_MAIN " +
                "REFERENCING NEW AS N " +
                "FOR EACH ROW " +      
                "INSERT INTO T_ACTION_ROW(A, V1, ID, V2) VALUES ('I', N.V, N.ID, N.V)");
        
        s.executeUpdate("CREATE TRIGGER AIS " +
                "AFTER INSERT ON T_MAIN " +
                "REFERENCING NEW_TABLE AS N " +
                "FOR EACH STATEMENT " +      
                "INSERT INTO T_ACTION_STATEMENT(A, V1, ID, V2) " +
                "SELECT 'I', V, ID, V FROM N");

        s.close();
        commit();
    }
    
    /**
     * Execute three insert statements.
     * NULL as the value for the type
     * one row insert with random value for the type
     * three row insert with random values for the type
     * 
     * Check that the data in the action table matches the main
     * table (see the after insert trigger definitions).
     * @param type
     * @throws SQLException
     * @throws IOException
     * 
     */
    private void actionTypesInsertTest(String type)
        throws SQLException, IOException
    {  
        Statement s = createStatement();
        s.executeUpdate("INSERT INTO T_MAIN(V) VALUES NULL");
        s.close();
        actionTypesCompareMainToAction(1, type);

        int jdbcType = DatabaseMetaDataTest.getJDBCType(type);

        // Can't directly insert into XML columns from JDBC.
        if (jdbcType == JDBC.SQLXML)
            return; // temp
        
        Random r = new Random();
        
        PreparedStatement ps;
        ps = prepareStatement("INSERT INTO T_MAIN(V) VALUES (?)");
        ps.setObject(1, getRandomValue(r, jdbcType), jdbcType);
        ps.executeUpdate();
        ps.close();

        actionTypesCompareMainToAction(2, type);

        ps = prepareStatement("INSERT INTO T_MAIN(V) VALUES (?), (?), (?)");
        ps.setObject(1, getRandomValue(r, jdbcType), jdbcType);
        ps.setObject(2, getRandomValue(r, jdbcType), jdbcType);
        ps.setObject(3, getRandomValue(r, jdbcType), jdbcType);
        ps.executeUpdate();
        ps.close();

        actionTypesCompareMainToAction(5, type);    
    }
    
    /**
     * Compare the contents of the main table to the action table.
     * See the trigger defintions for details.
     * @param actionCount
     * @param type
     * @throws SQLException
     * @throws IOException
     */
    private void actionTypesCompareMainToAction(int actionCount,
            String type) throws SQLException, IOException {
        
        Statement s1 = createStatement();
        Statement s2 = createStatement();
        
        String sqlMain = "SELECT ID, V, V FROM T_MAIN ORDER BY 1";
        String sqlActionRow = "SELECT ID, V1, V2 FROM T_ACTION_ROW ORDER BY 1";
        String sqlActionStatement = "SELECT ID, V1, V2 FROM T_ACTION_STATEMENT ORDER BY 1";
        
        // Derby does not (yet) allow a XML column in select list 
        if ("XML".equals(type)) {
            sqlMain = "SELECT ID, XMLSERIALIZE(V AS CLOB), " +
                    "XMLSERIALIZE(V AS CLOB) FROM T_MAIN ORDER BY 1";
            sqlActionRow = "SELECT ID, XMLSERIALIZE(V1 AS CLOB), " +
                    "XMLSERIALIZE(V2 AS CLOB) FROM T_ACTION_ROW ORDER BY 1";
            sqlActionStatement = "SELECT ID, XMLSERIALIZE(V1 AS CLOB), " +
                "XMLSERIALIZE(V2 AS CLOB) FROM T_ACTION_STATEMENT ORDER BY 1";
        }
        
        ResultSet rsMain = s1.executeQuery(sqlMain);
        ResultSet rsAction = s2.executeQuery(sqlActionRow);        
        JDBC.assertSameContents(rsMain, rsAction);
        
        rsMain = s1.executeQuery(sqlMain);
        rsAction = s2.executeQuery(sqlActionStatement);        
        JDBC.assertSameContents(rsMain, rsAction);
        
        
        assertTableRowCount("T_ACTION_ROW", actionCount);
        assertTableRowCount("T_ACTION_STATEMENT", actionCount);
        
        s1.close();
        s2.close();
    }
    
    /**
     * Generate a random object (never null) for
     * a given JDBC type. Object is suitable for
     * PreparedStatement.setObject() either
     * with or without passing in jdbcType to setObject.
     * 
     * For character types a String object is returned.
     * For binary types a byte[] is returned.
     * (work in progress)
     */
    public static Object getRandomValue(Random r, int jdbcType)
    {
        switch (jdbcType)
        {
        case Types.SMALLINT:
            return new Integer((short) r.nextInt());
        case Types.INTEGER:
            return new Integer(r.nextInt());
            
        case Types.BIGINT:
            return new Long(r.nextLong());
            
        case Types.FLOAT:
        case Types.REAL:
            return new Float(r.nextFloat());
            
        case Types.DOUBLE:
            return new Double(r.nextDouble());

        case Types.DATE:
            long d = r.nextLong();
            if (d < 0)
                d = -d;
            d = d / (24L * 60L * 60L * 1000L);
            d = d % (4000L * 365L); // limit year to a reasonable value.
            d = d * (24L * 60L * 60L * 1000L);
            return new Date(d);
            
        case Types.TIME:
            long t = r.nextLong();
            if (t < 0)
                t = -t;
             return new Time(t % (24L * 60L * 60L * 1000L));
             
        case Types.TIMESTAMP:
            // limit year to a reasonable value
            long ts = r.nextLong();
            if (ts < 0)
                ts = -ts;
            ts = ts % (4000L * 365L * 24L * 60L * 60L * 1000L);
            return new Timestamp(ts);
            
        case Types.LONGVARCHAR:
            return randomString(r, r.nextInt(32701));

        case Types.LONGVARBINARY:
            return randomBinary(r, r.nextInt(32701));
             
       }
        
        // fail("unexpected JDBC Type " + jdbcType);
        return null;
    }
    
    private static byte[] randomBinary(Random r, int len)
    {
        byte[] bb = new byte[len];
        for (int i = 0; i < bb.length; i++)
            bb[i] = (byte) r.nextInt();
        return bb;
 
    }
    private static String randomString(Random r, int len)
    {
        char[] cb = new char[len];
        for (int i = 0; i < cb.length; i++)
            cb[i] = (char) r.nextInt(Character.MAX_VALUE);
              
        return new String(cb);
                
    }


}
