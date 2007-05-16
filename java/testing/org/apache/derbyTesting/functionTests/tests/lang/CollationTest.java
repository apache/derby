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
      setUpTable(ds);
      
      Connection conn = ds.getConnection();
      conn.setAutoCommit(false);
      Statement s = conn.createStatement();

      //The collation should be UCS_BASIC for this database
      checkLangBasedQuery(s, 
      		"VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collation')",
			new String[][] {{"UCS_BASIC"}});

      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME",
      		new String[][] {{"4","Acorn"},{"0","Smith"},{"1","Zebra"},
      		{"6","aacorn"}, {"2","\u0104corn"},{"5","\u015Amith"},{"3","\u017Bebra"} });   

      //COMPARISONS INVOLVING CONSTANTS
      //In default JVM territory, 'aacorn' is != 'Acorn'
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER where 'aacorn' = 'Acorn' ",
      		null);
      //In default JVM territory, 'aacorn' is not < 'Acorn'
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER where 'aacorn' < 'Acorn' ",
      		null);

      //COMPARISONS INVOLVING CONSTANT and PERSISTENT COLUMN
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER WHERE NAME <= 'Smith' ",
      		new String[][] {{"0","Smith"}, {"4","Acorn"} });   
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER WHERE NAME between 'Acorn' and 'Zebra' ",
      		new String[][] {{"0","Smith"}, {"1","Zebra"}, {"4","Acorn"} });
      //After index creation, the query above will return same data but in 
      //different order
      /*s.executeUpdate("CREATE INDEX CUSTOMER_INDEX1 ON CUSTOMER(NAME)");
      s.executeUpdate("INSERT INTO CUSTOMER VALUES (NULL, NULL)");
      checkLangBasedQuery(s, 
      		"SELECT ID, NAME FROM CUSTOMER WHERE NAME between 'Acorn' and " +
			" 'Zebra' ORDER BY NAME",
      		new String[][] {{"4","Acorn"}, {"0","Smith"}, {"1","Zebra"} });
*/
      //For non-collated databases, COMPARISONS OF USER PERSISTENT CHARACTER 
      //COLUMN AND CHARACTER CONSTANT WILL not FAIL IN SYSTEM SCHEMA.
      s.executeUpdate("set schema SYS");
      checkLangBasedQuery(s, "SELECT ID, NAME FROM APP.CUSTOMER WHERE NAME <= 'Smith' ",
      		new String[][] {{"0","Smith"}, {"4","Acorn"} });   

      s.close();
      conn.commit();

      dropTable(ds);
      }
      
  /**
   * Test order by with polish collation
   * @throws SQLException
   */
public void testPolishCollation() throws SQLException {
      DataSource ds = JDBCDataSource.getDataSourceLogical("poldb");
      JDBCDataSource.setBeanProperty(ds, "connectionAttributes", 
                  "create=true;territory=pl;collation=TERRITORY_BASED");
      setUpTable(ds);
      
      Connection conn = ds.getConnection();
      conn.setAutoCommit(false);
      Statement s = conn.createStatement();

      //The collation should be TERRITORY_BASED for this database
      checkLangBasedQuery(s, 
      		"VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collation')",
			new String[][] {{"TERRITORY_BASED"}});

      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME",
      		new String[][] {{"6","aacorn"}, {"4","Acorn"}, {"2","\u0104corn"},
      		{"0","Smith"},{"5","\u015Amith"}, {"1","Zebra"},{"3","\u017Bebra"} });
      
      //COMPARISONS INVOLVING CONSTANTS
      //In Polish, 'aacorn' is != 'Acorn'
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER where 'aacorn' = 'Acorn' ",
      		null);
      //In Polish, 'aacorn' is < 'Acorn'
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER where 'aacorn' < 'Acorn'",
      		new String[][] {{"0","Smith"}, {"1","Zebra"}, {"2","\u0104corn"},
      		{"3","\u017Bebra"}, {"4","Acorn"}, {"5","\u015Amith"}, 
			{"6","aacorn"} });

      //COMPARISONS INVOLVING CONSTANT and PERSISTENT COLUMN
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER WHERE NAME <= 'Smith' ",
      		new String[][] {{"0","Smith"}, {"2","\u0104corn"}, {"4","Acorn"}, 
      		{"6","aacorn"} });
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER WHERE NAME between 'Acorn' and 'Zebra' ",
      		new String[][] {{"0","Smith"}, {"1","Zebra"}, {"2","\u0104corn"}, 
      		{"4","Acorn"}, {"5","\u015Amith"} });
      //After index creation, the query above will return same data but in 
      //different order
      /*s.executeUpdate("CREATE INDEX CUSTOMER_INDEX1 ON CUSTOMER(NAME)");
      s.executeUpdate("INSERT INTO CUSTOMER VALUES (NULL, NULL)");
      checkLangBasedQuery(s, 
      		"SELECT ID, NAME FROM CUSTOMER -- derby-properties index=customer_index1 \r WHERE NAME between 'Acorn' and " +
			" 'Zebra'", //ORDER BY NAME",
      		new String[][] {{"4","Acorn"}, {"2","\u0104corn"}, {"0","Smith"}, 
		      		{"5","\u015Amith"}, {"1","Zebra"} });
      */
      //For collated databases, COMPARISONS OF USER PERSISTENT CHARACTER 
      //COLUMN AND CHARACTER CONSTANT WILL FAIL IN SYSTEM SCHEMA.
      s.executeUpdate("set schema SYS");
      assertStatementError("42818", s, "SELECT ID, NAME FROM APP.CUSTOMER WHERE NAME <= 'Smith' ");

      s.close();
      conn.commit();

      dropTable(ds);
      }    
  

  /**
   * Test order by with Norwegian collation
   * 
   * @throws SQLException
   */
public void testNorwayCollation() throws SQLException {
      DataSource ds = JDBCDataSource.getDataSourceLogical("nordb");
      JDBCDataSource.setBeanProperty(ds, "connectionAttributes", 
                  "create=true;territory=no;collation=TERRITORY_BASED");
      setUpTable(ds);
      
      Connection conn = ds.getConnection();
      conn.setAutoCommit(false);
      Statement s = conn.createStatement();

      //The collation should be TERRITORY_BASED for this database
      checkLangBasedQuery(s, 
      		"VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collation')",
			new String[][] {{"TERRITORY_BASED"}});

      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME",
      		new String[][] {{"4","Acorn"}, {"2","\u0104corn"},{"0","Smith"},
      		{"5","\u015Amith"}, {"1","Zebra"},{"3","\u017Bebra"}, {"6","aacorn"} });
      
      //COMPARISONS INVOLVING CONSTANTS
      //In Norway, 'aacorn' is != 'Acorn'
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER where 'aacorn' = 'Acorn' ",
      		null);
      //In Norway, 'aacorn' is not < 'Acorn'
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER where 'aacorn' < 'Acorn' ",
      		null);

      //COMPARISONS INVOLVING CONSTANT and PERSISTENT COLUMN
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER WHERE NAME <= 'Smith' ",
      		new String[][] {{"0","Smith"}, {"2","\u0104corn"}, {"4","Acorn"} });
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER WHERE NAME between 'Acorn' and 'Zebra' ",
      		new String[][] {{"0","Smith"}, {"1","Zebra"}, {"2","\u0104corn"}, 
      		{"4","Acorn"}, {"5","\u015Amith"} });
      //After index creation, the query above will return same data but in 
      //different order
      /*s.executeUpdate("CREATE INDEX CUSTOMER_INDEX1 ON CUSTOMER(NAME)");
      s.executeUpdate("INSERT INTO CUSTOMER VALUES (NULL, NULL)");
      checkLangBasedQuery(s, 
      		"SELECT ID, NAME FROM CUSTOMER  -- derby-properties index=customer_index1 \r WHERE NAME between 'Acorn' and " +
			" 'Zebra'", //ORDER BY NAME",
      		new String[][] {{"4","Acorn"}, {"2","\u0104corn"}, {"0","Smith"}, 
		      		{"5","\u015Amith"}, {"1","Zebra"} });
      */
      //For collated databases, COMPARISONS OF USER PERSISTENT CHARACTER 
      //COLUMN AND CHARACTER CONSTANT WILL FAIL IN SYSTEM SCHEMA.
      s.executeUpdate("set schema SYS");
      assertStatementError("42818", s, "SELECT ID, NAME FROM APP.CUSTOMER WHERE NAME <= 'Smith' ");

      s.close();
      conn.commit();

      dropTable(ds);
      }
  

  /**
   * Test order by with English collation
   * 
  * @throws SQLException
  */
public void testEnglishCollation() throws SQLException {
      DataSource ds = JDBCDataSource.getDataSourceLogical("endb");
      JDBCDataSource.setBeanProperty(ds, "connectionAttributes", 
                  "create=true;territory=en;collation=TERRITORY_BASED");
      setUpTable(ds);
      
      Connection conn = ds.getConnection();
      conn.setAutoCommit(false);
      Statement s = conn.createStatement();

      //The collation should be TERRITORY_BASED for this database
      checkLangBasedQuery(s, 
      		"VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collation')",
			new String[][] {{"TERRITORY_BASED"}});

      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME",
      		new String[][] {{"6","aacorn"},{"4","Acorn"},{"2","\u0104corn"},{"0","Smith"},
      		{"5","\u015Amith"},{"1","Zebra"},{"3","\u017Bebra"} });      

      //COMPARISONS INVOLVING CONSTANTS
      //In English, 'aacorn' != 'Acorn'
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER where 'aacorn' = 'Acorn' ",
      		null);
      //In English, 'aacorn' is < 'Acorn'
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER where 'aacorn' < 'Acorn'",
      		new String[][] {{"0","Smith"}, {"1","Zebra"}, {"2","\u0104corn"},
      		{"3","\u017Bebra"}, {"4","Acorn"}, {"5","\u015Amith"}, 
			{"6","aacorn"} });

      //COMPARISONS INVOLVING CONSTANT and PERSISTENT COLUMN
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER WHERE NAME <= 'Smith' ",
      		new String[][] {{"0","Smith"}, {"2","\u0104corn"}, {"4","Acorn"},
      		{"6","aacorn"} });
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER WHERE NAME between 'Acorn' and 'Zebra' ",
      		new String[][] {{"0","Smith"}, {"1","Zebra"}, {"2","\u0104corn"}, 
      		{"4","Acorn"}, {"5","\u015Amith"} });
      //After index creation, the query above will return same data but in 
      //different order
      /*s.executeUpdate("CREATE INDEX CUSTOMER_INDEX1 ON CUSTOMER(NAME)");
      s.executeUpdate("INSERT INTO CUSTOMER VALUES (NULL, NULL)");
      checkLangBasedQuery(s, 
      		"SELECT ID, NAME FROM CUSTOMER -- derby-properties index=customer_index1 \r WHERE NAME between 'Acorn' and " + 
			" 'Zebra'", //ORDER BY NAME",
      		new String[][] {{"4","Acorn"}, {"2","\u0104corn"}, {"0","Smith"}, 
      		{"5","\u015Amith"}, {"1","Zebra"} });
      */
      //For collated databases, COMPARISONS OF USER PERSISTENT CHARACTER 
      //COLUMN AND CHARACTER CONSTANT WILL FAIL IN SYSTEM SCHEMA.
      s.executeUpdate("set schema SYS");
      assertStatementError("42818", s, "SELECT ID, NAME FROM APP.CUSTOMER WHERE NAME <= 'Smith' ");
      
      s.close();
      conn.commit();
      
      dropTable(ds);
      }

private void setUpTable(DataSource ds) throws SQLException {
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

    conn.commit();
    ps.close();
    s.close();
}

private void dropTable(DataSource ds) throws SQLException {
	Connection conn = ds.getConnection();
	Statement s = conn.createStatement();
	
    s.execute("DROP TABLE CUSTOMER");     
    s.close();
}
/**
 * sort customers by 
 * @param ds
 * @param expectedResult Null for this means that the passed query is 
 * expected to return an empty resultset. If not empty, then the resultset
 * from the query should match this paramter
 * @throws SQLException
 */
private void checkLangBasedQuery(Statement s, String query, String[][] expectedResult) throws SQLException {
    ResultSet rs = s.executeQuery(query);
    if (expectedResult == null) //expecting empty resultset from the query
    	JDBC.assertEmpty(rs);
    else
    	JDBC.assertFullResultSet(rs,expectedResult);
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
