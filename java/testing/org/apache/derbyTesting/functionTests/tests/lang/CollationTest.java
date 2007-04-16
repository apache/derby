/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CollationTest
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

public class CollationTest extends BaseJDBCTestCase {

    public CollationTest(String name) {
        super(name);
    }
    
    private static final String[] NAMES =
    {
            // Just Smith, Zebra, Acorn with alternate A,S and Z
            "Smith",
            "Zebra",
            "\u0104corn",
            "\u017Bebra",
            "Acorn",
            "\u015Amith",
            "aacorn",
    };
    
  /**
   * Test order by with default collation
   * 
   * @throws SQLException
   */
public void testDefaultCollation() throws SQLException {
      DataSource ds = JDBCDataSource.getDataSourceLogical("defaultdb");
      JDBCDataSource.setBeanProperty(ds, "connectionAttributes", 
                  "create=true");
      checkLangBasedQuery(ds, new String[][] {{"4","Acorn"},{"0","Smith"},{"1","Zebra"},
              {"6","aacorn"}, {"2","\u0104corn"},{"5","\u015Amith"},{"3","\u017Bebra"}});      
      }
      
  /**
   * Test order by with polish collation
   * @throws SQLException
   */
public void xtestPolishCollation() throws SQLException {
      DataSource ds = JDBCDataSource.getDataSourceLogical("poldb");
      JDBCDataSource.setBeanProperty(ds, "connectionAttributes", 
                  "create=true;territory=pl;collation=TERRITORY_BASED");
      checkLangBasedQuery(ds, new String[][] {{"6","aacorn"}, {"4","Acorn"}, {"2","\u0104corn"},
              {"0","Smith"},{"5","\u015Amith"}, {"1","Zebra"},{"3","\u017Bebra"}});
      }    
  

  /**
   * Test order by with Norwegian collation
   * 
   * @throws SQLException
   */
public void xtestNorwayCollation() throws SQLException {
      DataSource ds = JDBCDataSource.getDataSourceLogical("nordb");
      JDBCDataSource.setBeanProperty(ds, "connectionAttributes", 
                  "create=true;territory=no;collation=TERRITORY_BASED");
      checkLangBasedQuery(ds, new String[][] {{"4","Acorn"}, {"2","\u0104corn"},{"0","Smith"},
              {"5","\u015Amith"}, {"1","Zebra"},{"3","\u017Bebra"}, {"6","aacorn"}});
      }
  

  /**
   * Test order by with English collation
   * 
  * @throws SQLException
  */
public void xtestEnglishCollation() throws SQLException {
      DataSource ds = JDBCDataSource.getDataSourceLogical("endb");
      JDBCDataSource.setBeanProperty(ds, "connectionAttributes", 
                  "create=true;territory=en;collation=TERRITORY_BASED");
      checkLangBasedQuery(ds, new String[][] {{"6","aacorn"},{"4","Acorn"},{"2","\u0104corn"},{"0","Smith"},
               {"5","\u015Amith"},{"1","Zebra"},{"3","\u017Bebra"}});      
      }
  
/**
 * sort customers by 
 * @param ds
 * @param expectedResult
 * @throws SQLException
 */
private void checkLangBasedQuery(DataSource ds, String[][] expectedResult) throws SQLException {
    Connection conn = ds.getConnection();
      Statement s = conn.createStatement();
      
      s.execute("CREATE TABLE CUSTOMER(ID INT, NAME VARCHAR(40))");

      conn.setAutoCommit(false);
      PreparedStatement ps = conn.prepareStatement("INSERT INTO CUSTOMER VALUES(?,?)");

      for (int i = 0; i < NAMES.length; i++)
      {
              ps.setInt(1, i);
              ps.setString(2, NAMES[i]);
              ps.executeUpdate();
      }
      
      ResultSet rs = s.executeQuery("SELECT ID, NAME FROM CUSTOMER ORDER BY NAME");
      JDBC.assertFullResultSet(rs,expectedResult);
      s.execute("DROP TABLE CUSTOMER");     
      conn.commit();
      ps.close();
}
    
  public static Test suite() {

        Test test =  TestConfiguration.defaultSuite(CollationTest.class);
        test = TestConfiguration.additionalDatabaseDecorator(test, "defaultdb");
        test = TestConfiguration.additionalDatabaseDecorator(test, "endb");
        test = TestConfiguration.additionalDatabaseDecorator(test, "nordb");
        test = TestConfiguration.additionalDatabaseDecorator(test, "poldb");
        return test;
    }

}
