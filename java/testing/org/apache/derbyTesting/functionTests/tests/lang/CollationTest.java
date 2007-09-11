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
import java.sql.Types;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest;
import org.apache.derbyTesting.junit.XML;
//import org.apache.derby.iapi.types.XML;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

public class CollationTest extends BaseJDBCTestCase {

	/*
	 * ToDo test cases
	 * 1)Use a parameter as cast operand and cast that to character type. The
	 * resultant type should get it's collation from the compilation schema
	 * 2)Test conditional if (NULLIF and CASE) with different datatypes to see
	 * how casting works. The compile node for this SQL construct seems to be
	 * dealing with lot of casting code (ConditionalNode)
	 * 3)When doing concatenation testing, check what happens if concatantion
	 * is between non-char types. This is because ConcatenationOperatorNode
	 * in compile package has following comment "If either the left or right 
	 * operands are non-string, non-bit types, then we generate an implicit 
	 * cast to VARCHAR."
	 * 4)Do testing with upper and lower
	 * 5)It looks like node for LIKE ESCAPE which is LikeEscapeOperatorNode
	 * also uses quite a bit of casting. Should include test for LIKE ESCAPE
	 * which will trigger the casting.
	 * 6)Binary arithmetic operators do casting if one of the operands is
	 * string and other is numeric. Test that combination
	 * 7)Looks like import utility does casting (in ColumnInfo class). See
	 * if any testing is required for that.
	 * 8)Do testing with UNION and use the results of UNION in collation
	 * comparison (if there is something like that possible. I didn't put too
	 * much thought into it but wanted to list here so we can do the required
	 * testing if needed).
	 */
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

      getConnection().setAutoCommit(false);
      Statement s = createStatement();
      PreparedStatement ps;
      ResultSet rs;
      
      setUpTable(s);

      //The collation should be UCS_BASIC for this database
      checkLangBasedQuery(s, 
      		"VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collation')",
			new String[][] {{"UCS_BASIC"}});

      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME",
      		new String[][] {{"4","Acorn"},{"0","Smith"},{"1","Zebra"},
      		{"6","aacorn"}, {"2","\u0104corn"},{"5","\u015Amith"},{"3","\u017Bebra"} });   

      // Order by expresssion
      s.executeUpdate("CREATE FUNCTION mimic(val VARCHAR(32000)) RETURNS VARCHAR(32000) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.CollationTest.mimic' LANGUAGE JAVA PARAMETER STYLE JAVA");
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER ORDER BY MIMIC(NAME)",
                new String[][] {{"4","Acorn"},{"0","Smith"},{"1","Zebra"},
                {"6","aacorn"}, {"2","\u0104corn"},{"5","\u015Amith"},{"3","\u017Bebra"} });   

      s.executeUpdate("DROP FUNCTION mimic");
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

      s.executeUpdate("set schema APP");
      //Following sql will not fail in a database which uses UCS_BASIC for
      //user schemas. Since the collation of user schemas match that of system
      //schema, the following comparison will not fail. It will fail in a 
      //database with territory based collation for user schemas. 
      checkLangBasedQuery(s, "SELECT 1 FROM SYS.SYSTABLES WHERE " +
      		" TABLENAME = 'CUSTOMER' ",
      		new String[][] {{"1"} });    
      //Using cast for persistent character column from system table in the
      //query above won't affect the above sql in any ways. 
      checkLangBasedQuery(s, "SELECT 1 FROM SYS.SYSTABLES WHERE CAST " +
      		" (TABLENAME AS CHAR(15)) = 'CUSTOMER' ",
      		new String[][] {{"1"} });   

      //Do some testing using CASE WHEN THEN ELSE
      //following will work with no problem for a database with UCS_BASIC
      //collation for system and user schemas
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE CASE " +
      		" WHEN 1=1 THEN TABLENAME ELSE 'c' END = 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   
      //Using cast for result of CASE expression in the query above would not
      //affect the sql in any ways. 
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE CAST " +
      		" ((CASE WHEN 1=1 THEN TABLENAME ELSE 'c' END) AS CHAR(12)) = " +
			" 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing using CONCATENATION
      //following will work with no problem for a database with UCS_BASIC
      //collation for system and user schemas
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" TABLENAME || ' ' = 'SYSCOLUMNS '",
      		new String[][] {{"SYSCOLUMNS"} });   
      //Using cast for result of CAST expression in the query above would not
      //affect the sql in any ways. 
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" CAST (TABLENAME || ' ' AS CHAR(12)) = " +
			" 'SYSCOLUMNS '",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing using COALESCE
      //following will work with no problem for a database with UCS_BASIC
      //collation for system and user schemas
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" COALESCE(TABLENAME, 'c') = 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   
      //Using cast for result of COALESCE expression in the query above would not
      //affect the sql in any ways. 
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" CAST (COALESCE (TABLENAME, 'c') AS CHAR(12)) = " +
			" 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing using NULLIF
      //following will work with no problem for a database with UCS_BASIC
      //collation for system and user schemas
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" NULLIF(TABLENAME, 'c') = 'SYSCOLUMNS'",
  		new String[][] {{"SYSCOLUMNS"} });   
      //Using cast for result of NULLIF expression in the query above would not
      //affect the sql in any ways. 
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" CAST (NULLIF (TABLENAME, 'c') AS CHAR(12)) = " +
			" 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Test USER/CURRENT_USER/SESSION_USER
      checkLangBasedQuery(s, "SELECT count(*) FROM CUSTOMER WHERE "+ 
      		"CURRENT_USER = 'APP'",
      		new String[][] {{"7"}});   
      
      //Do some testing with MAX/MIN operators
      checkLangBasedQuery(s, "SELECT MAX(NAME) maxName FROM CUSTOMER ORDER BY maxName ",
      		new String[][] {{"\u017Bebra"}});   
      checkLangBasedQuery(s, "SELECT MIN(NAME) minName FROM CUSTOMER ORDER BY minName ",
      		new String[][] {{"Acorn"}});   

      //Do some testing with CHAR/VARCHAR functions
      s.executeUpdate("set schema SYS");
      checkLangBasedQuery(s, "SELECT CHAR(ID) FROM APP.CUSTOMER WHERE " +
      		" CHAR(ID)='0'", new String[] [] {{"0"}});
      
      s.executeUpdate("set schema APP");
      if (XML.classpathMeetsXMLReqs())
      	checkLangBasedQuery(s, "SELECT XMLSERIALIZE(x as CHAR(10)) " +
      			" FROM xmlTable, SYS.SYSTABLES WHERE " +
				" XMLSERIALIZE(x as CHAR(10)) = TABLENAME",
				null);
      //Start of parameter testing
      //Start with simple ? param in a string comparison
      //Since all schemas (ie user and system) have the same collation, the 
      //following test won't fail.
      s.executeUpdate("set schema APP");
      ps = prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" ? = TABLENAME");
      ps.setString(1, "SYSCOLUMNS");
      rs = ps.executeQuery();
      JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});

      //Since all schemas (ie user and system) have the same collation, the 
      //following test won't fail.
      ps = prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" SUBSTR(?,2) = TABLENAME");
      ps.setString(1, " SYSCOLUMNS");
      rs = ps.executeQuery();
      JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});

      //Since all schemas (ie user and system) have the same collation, the 
      //following test won't fail.
      ps = prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" LTRIM(?) = TABLENAME");
      ps.setString(1, " SYSCOLUMNS");
      rs = ps.executeQuery();
      JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});
      ps = prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" RTRIM(?) = TABLENAME");
      ps.setString(1, "SYSCOLUMNS  ");
      rs = ps.executeQuery();
      JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});

      //Since all schemas (ie user and system) have the same collation, the 
      //following test won't fail.
      ps = prepareStatement("SELECT COUNT(*) FROM CUSTOMER WHERE " + 
      		" ? IN (SELECT TABLENAME FROM SYS.SYSTABLES)");
      ps.setString(1, "SYSCOLUMNS");
      rs = ps.executeQuery();
      JDBC.assertFullResultSet(rs,new String[][] {{"7"}});
      //End of parameter testing
      
      s.close();
      }
      
  /**
   * Test order by with polish collation
   * @throws SQLException
   */
public void testPolishCollation() throws SQLException {

      getConnection().setAutoCommit(false);
      Statement s = createStatement();
      
      setUpTable(s);

      //The collation should be TERRITORY_BASED for this database
      checkLangBasedQuery(s, 
      		"VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collation')",
			new String[][] {{"TERRITORY_BASED"}});

      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME",
      		new String[][] {{"6","aacorn"}, {"4","Acorn"}, {"2","\u0104corn"},
      		{"0","Smith"},{"5","\u015Amith"}, {"1","Zebra"},{"3","\u017Bebra"} });
      
      // Order by expresssion
      s.executeUpdate("CREATE FUNCTION mimic(val VARCHAR(32000)) RETURNS VARCHAR(32000) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.CollationTest.mimic' LANGUAGE JAVA PARAMETER STYLE JAVA");
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER ORDER BY MIMIC(NAME)",
              new String[][] {{"6","aacorn"}, {"4","Acorn"}, {"2","\u0104corn"},
                {"0","Smith"},{"5","\u015Amith"}, {"1","Zebra"},{"3","\u017Bebra"} });
                
      s.executeUpdate("DROP FUNCTION mimic");
      
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

      //Do some testing with MAX/MIN operators
      s.executeUpdate("set schema APP");
      checkLangBasedQuery(s, "SELECT MAX(NAME) maxName FROM CUSTOMER ORDER BY maxName ",
      		new String[][] {{"\u017Bebra"}});   
      checkLangBasedQuery(s, "SELECT MIN(NAME) minName FROM CUSTOMER ORDER BY minName ",
      		new String[][] {{"aacorn"}});   

      commonTestingForTerritoryBasedDB(s);
    
      }    
  

  /**
   * Test order by with Norwegian collation
   * 
   * @throws SQLException
   */
public void testNorwayCollation() throws SQLException {

      getConnection().setAutoCommit(false);
      Statement s = createStatement();

      setUpTable(s);

      //The collation should be TERRITORY_BASED for this database
      checkLangBasedQuery(s, 
      		"VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collation')",
			new String[][] {{"TERRITORY_BASED"}});

      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME",
      		new String[][] {{"4","Acorn"}, {"2","\u0104corn"},{"0","Smith"},
      		{"5","\u015Amith"}, {"1","Zebra"},{"3","\u017Bebra"}, {"6","aacorn"} });
      
      // Order by expresssion
      s.executeUpdate("CREATE FUNCTION mimic(val VARCHAR(32000)) RETURNS VARCHAR(32000) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.CollationTest.mimic' LANGUAGE JAVA PARAMETER STYLE JAVA");
      checkLangBasedQuery(s, "SELECT ID, NAME FROM CUSTOMER ORDER BY MIMIC(NAME)",
                new String[][] {{"4","Acorn"}, {"2","\u0104corn"},{"0","Smith"},
                {"5","\u015Amith"}, {"1","Zebra"},{"3","\u017Bebra"}, {"6","aacorn"} });
              
      s.executeUpdate("DROP FUNCTION mimic");
  
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

      //Do some testing with MAX/MIN operators
      s.executeUpdate("set schema APP");
      checkLangBasedQuery(s, "SELECT MAX(NAME) maxName FROM CUSTOMER ORDER BY maxName ",
      		new String[][] {{"aacorn"}});   
      checkLangBasedQuery(s, "SELECT MIN(NAME) minName FROM CUSTOMER ORDER BY minName ",
      		new String[][] {{"Acorn"}});   

      commonTestingForTerritoryBasedDB(s);

      s.close();

      }
  

  /**
   * Test order by with English collation
   * 
  * @throws SQLException
  */
public void testEnglishCollation() throws SQLException {

      getConnection().setAutoCommit(false);
      Statement s = createStatement();
      setUpTable(s);

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

      //Do some testing with MAX/MIN operators
      s.executeUpdate("set schema APP");
      checkLangBasedQuery(s, "SELECT MAX(NAME) maxName FROM CUSTOMER ORDER BY maxName ",
      		new String[][] {{"\u017Bebra"}});   
      checkLangBasedQuery(s, "SELECT MIN(NAME) minName FROM CUSTOMER ORDER BY minName ",
      		new String[][] {{"aacorn"}});   

      commonTestingForTerritoryBasedDB(s);

      s.close();
      }

private void commonTestingForTerritoryBasedDB(Statement s) throws SQLException{
	PreparedStatement ps;
	ResultSet rs;
    Connection conn = s.getConnection();		

    s.executeUpdate("set schema APP");
    //Following sql will fail because the compilation schema is user schema
    //and hence the character constant "CUSTOMER" will pickup the collation
    //of user schema, which is territory based for this database. But the
    //persistent character columns from sys schema, which is TABLENAME in
    //following query will have the UCS_BASIC collation. Since the 2 
    //collation types don't match, the following comparison will fail
    assertStatementError("42818", s, "SELECT 1 FROM SYS.SYSTABLES WHERE " +
    		" TABLENAME = 'CUSTOMER' ");   
    //To get around the problem in the query above, use cast for persistent 
    //character column from system table and then compare it against a 
    //character constant. Do this when the compilation schema is a user 
    //schema and not system schema. This will ensure that the result 
    //of the casting will pick up the collation of the user schema. And 
    //constant character string will also pick up the collation of user 
    //schema and hence the comparison between the 2 will not fail
    checkLangBasedQuery(s, "SELECT 1 FROM SYS.SYSTABLES WHERE CAST " +
    		" (TABLENAME AS CHAR(15)) = 'CUSTOMER' ",
    		new String[][] {{"1"} });   

    //Do some testing using CASE WHEN THEN ELSE
    //following sql will not work for a database with territory based
    //collation for user schemas. This is because the resultant string type 
    //from the CASE expression below will have collation derivation of NONE.
    //The reason for collation derivation of NONE is that the CASE's 2 
    //operands have different collation types and as per SQL standards, if an
    //aggregate method has operands with different collations, then the 
    //result will have collation derivation of NONE. The right side of =
    //operation has collation type of territory based and hence the following
    //sql fails. DERBY-2678 This query should not fail because even though 
    //left hand side of = has collation derivation of NONE, the right hand
    //side has collation derivation of IMPLICIT, and so we should just pick the
    //collation of the rhs as per SQL standard. Once DERBY-2678 is fixed, we
    //don't need to use the CAST on this query to make it work (we are doing
    //that in the next test).
    assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE CASE " +
    		" WHEN 1=1 THEN TABLENAME ELSE 'c' END = 'SYSCOLUMNS'");
    //CASTing the result of the CASE expression will solve the problem in the
    //query above. Now both the operands around = operation will have 
    //collation type of territory based and hence the sql won't fail
    checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE CAST " +
    		" ((CASE WHEN 1=1 THEN TABLENAME ELSE 'c' END) AS CHAR(12)) = " +
			" 'SYSCOLUMNS'",
    		new String[][] {{"SYSCOLUMNS"} });   
    //Another test for CASE WHEN THEN ELSE DERBY-2776
    //The data type for THEN is not same as the data type for ELSE.
    //THEN is of type CHAR and ELSE is of type VARCHAR. VARCHAR has higher
    //precedence hence the type associated with the return type of CASE will
    //be VARCHAR. Also, since the collation type of THEN and ELSE match,
    //which is TERRITORY BASED, the return type of CASE will have the collation
    //of TERRITORY BASED. This collation is same as the rhs of the = operation
    //and hence following sql will pass. 
    checkLangBasedQuery(s, "SELECT count(*) FROM CUSTOMER WHERE CASE WHEN " +
    		" 1=1 THEN NAMECHAR ELSE NAME END = NAMECHAR",
    		new String[][] {{"7"} });   
    //The query below will work for the same reason. 
    checkLangBasedQuery(s, "SELECT count(*) FROM SYS.SYSTABLES WHERE CASE " +
    		" WHEN 1=1 THEN TABLENAME ELSE TABLEID END = TABLENAME",
    		new String[][] {{"22"} });   

    //Do some testing using CONCATENATION
    //following will fail because result string of concatenation has 
    //collation derivation of NONE. That is because it's 2 operands have
    //different collation types. TABLENAME has collation type of UCS_BASIC
    //but constant character string ' ' has collation type of territory based
    //So the left hand side of = operator has collation derivation of NONE
    //and right hand side has collation derivation of territory based and
    //that causes the = comparison to fail. DERBY-2678 This query should not 
    //fail because even though left hand side of = has collation derivation of 
    //NONE, the right hand side has collation derivation of IMPLICIT, and so we 
    //should just pick the collation of the rhs as per SQL standard. Once 
    //DERBY-2678 is fixed, we don't need to use the CAST on this query to make 
    //it work (we are doing that in the next test).
    assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" TABLENAME || ' ' = 'SYSCOLUMNS '");   
    //CASTing the result of the concat expression will solve the problem in 
    //the query above. Now both the operands around = operation will have 
    //collation type of territory based and hence the sql won't fail
    checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" CAST (TABLENAME || ' ' AS CHAR(12)) = " +
			" 'SYSCOLUMNS '",
    		new String[][] {{"SYSCOLUMNS"} });
    //Following will fail because both sides of the = operator have collation
    //derivation of NONE. DERBY-2725
    assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" TABLENAME || ' ' = TABLENAME || 'SYSCOLUMNS '");   

    //Do some testing using COALESCE
    //following will fail because result string of COALESCE has 
    //collation derivation of NONE. That is because it's 2 operands have
    //different collation types. TABLENAME has collation type of UCS_BASIC
    //but constant character string 'c' has collation type of territory based
    //So the left hand side of = operator has collation derivation of NONE
    //and right hand side has collation derivation of territory based and
    //that causes the = comparison to fail. DERBY-2678 This query should not 
    //fail because even though left hand side of = has collation derivation of 
    //NONE, the right hand side has collation derivation of IMPLICIT, and so we 
    //should just pick the collation of the rhs as per SQL standard. Once 
    //DERBY-2678 is fixed, we don't need to use the CAST on this query to make 
    //it work (we are doing that in the next test).
    assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" COALESCE(TABLENAME, 'c') = 'SYSCOLUMNS'");   
    //CASTing the result of the COALESCE expression will solve the problem in 
    //the query above. Now both the operands around = operation will have 
    //collation type of territory based and hence the sql won't fail
    checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" CAST (COALESCE (TABLENAME, 'c') AS CHAR(12)) = " +
			" 'SYSCOLUMNS'",
    		new String[][] {{"SYSCOLUMNS"} });   

    //Do some testing using NULLIF
    //following will fail because result string of NULLIF has 
    //collation derivation of NONE. That is because it's 2 operands have
    //different collation types. TABLENAME has collation type of UCS_BASIC
    //but constant character string 'c' has collation type of territory based
    //So the left hand side of = operator has collation derivation of NONE
    //and right hand side has collation derivation of territory based and
    //that causes the = comparison to fail. DERBY-2678 This query should not 
    //fail because even though left hand side of = has collation derivation of 
    //NONE, the right hand side has collation derivation of IMPLICIT, and so 
    //we should just pick the collation of the rhs as per SQL standard. Once 
    //DERBY-2678 is fixed, we don't need to use the CAST on this query to make 
    //it work (we are doing that in the next test).
    assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" NULLIF(TABLENAME, 'c') = 'SYSCOLUMNS'");   
    //CASTing the result of the NULLIF expression will solve the problem in 
    //the query above. Now both the operands around = operation will have 
    //collation type of territory based and hence the sql won't fail
    checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" NULLIF (CAST (TABLENAME AS CHAR(12)), 'c' ) = " +
			" 'SYSCOLUMNS'",
    		new String[][] {{"SYSCOLUMNS"} });   

    //Do some testing with CHAR/VARCHAR functions
    s.executeUpdate("set schema SYS");
    //Following will work because both operands are = have the collation type
    //of UCS_BASIC
    checkLangBasedQuery(s, "SELECT CHAR(ID) FROM APP.CUSTOMER WHERE " +
    		" CHAR(ID)='0'", new String[] [] {{"0"}});
    //Derby does not allow VARCHAR function on numeric columns and hence 
    //this VARCHAR test looks little different than the CHAR test above.
    checkLangBasedQuery(s, "SELECT ID FROM APP.CUSTOMER WHERE " +
    		" VARCHAR(NAME)='Smith'", new String[] [] {{"0"}});
    //Now try a negative test
    s.executeUpdate("set schema APP");
    //following will fail because CHAR(TABLENAME)= TABLENAME is causing compare
    //between 2 character string types with different collation types. The lhs
    //operand has collation of territory based but rhs operand has collation of
    //UCS_BASIC
    assertStatementError("42818", s, "SELECT CHAR(TABLENAME) FROM " +
    		" SYS.SYSTABLES WHERE CHAR(TABLENAME)= TABLENAME AND " + 
			" VARCHAR(TABLENAME) = 'SYSCOLUMNS'");
    //To resolve the problem above, we need to use CAST around TABLENAME
    checkLangBasedQuery(s, "SELECT CHAR(TABLENAME) FROM SYS.SYSTABLES WHERE " +
    		" CHAR(TABLENAME)= (CAST (TABLENAME AS CHAR(12))) AND " + 
			" VARCHAR(TABLENAME) = 'SYSCOLUMNS'",
    		new String[][] {{"SYSCOLUMNS"} });  

    //Test USER/CURRENT_USER/SESSION_USER/CURRENT SCHMEA/ CURRENT ISOLATION
    //following will fail because we are trying to compare UCS_BASIC 
    //(CURRENT_USER) with territory based ("APP" taking it's collation from
    //compilation schema which is user schema at this time). 
    assertStatementError("42818", s, "SELECT count(*) FROM CUSTOMER WHERE "+
    		"CURRENT_USER = 'APP'");  
    //The problem above can be fixed by CASTing CURRENT_USER so that the 
    //collation type will be picked up from compilation schema which is user
    //schema at this point.
    checkLangBasedQuery(s, "SELECT count(*) FROM CUSTOMER WHERE "+ 
    		"CAST(CURRENT_USER AS CHAR(12)) = 'APP'",
    		new String[][] {{"7"}});   
    //following comparison will not cause compilation error because both the
    //operands around = have collation type of UCS_BASIC
    checkLangBasedQuery(s, "SELECT count(*) FROM CUSTOMER WHERE "+ 
    		"SESSION_USER = USER", new String[][] {{"7"}});
    //following will fail because we are trying to compare UCS_BASIC 
    //(CURRENT ISOLATION) with territory based ("CS" taking it's collation from
    //compilation schema which is user schema at this time). 
    assertStatementError("42818", s, "SELECT count(*) FROM CUSTOMER WHERE "+
	"CURRENT ISOLATION = 'CS'");  
    //Following will not give compilation error because both sides in = have 
    //the same collation type 
    checkLangBasedQuery(s, "SELECT count(*) FROM CUSTOMER WHERE "+ 
    		"CAST(CURRENT ISOLATION AS CHAR(12)) = 'CS'",
    		new String[][] {{"7"}});   
    //Following will not cause compilation error because both the operands
    //around the = have collation type of UCS_BASIC. We are in the SYS
    //schema and hence character string constant 'APP' has picked the collation
    //type of SYS schema which is UCS_BASIC
    s.executeUpdate("set schema SYS");
    checkLangBasedQuery(s, "SELECT count(*) FROM APP.CUSTOMER WHERE "+ 
    		"CURRENT SCHEMA = 'SYS'", new String[][] {{"7"}});   
    
    s.executeUpdate("set schema APP");
    if (XML.classpathMeetsXMLReqs()) {
        assertStatementError("42818", s, "SELECT XMLSERIALIZE(x as CHAR(10)) " +
        		" FROM xmlTable, SYS.SYSTABLES WHERE " + 
    			" XMLSERIALIZE(x as CHAR(10)) = TABLENAME");
        checkLangBasedQuery(s, "SELECT XMLSERIALIZE(x as CHAR(10)) FROM " +
        		" xmlTable, SYS.SYSTABLES WHERE XMLSERIALIZE(x as CHAR(10)) = " + 
    			" CAST(TABLENAME AS CHAR(10))",
        		null);
        //Do some parameter testing for XMLSERIALIZE. ? is not supported inside
        //the XMLSERIALIZE function and hence following will result in errors.
        checkPreparedStatementError(conn, "SELECT XMLSERIALIZE(x as CHAR(10)) " +
        		" FROM xmlTable, SYS.SYSTABLES WHERE " +
				" XMLSERIALIZE(? as CHAR(10)) = TABLENAME", "42Z70");
        checkPreparedStatementError(conn, "SELECT XMLSERIALIZE(x as CHAR(10)) FROM " +
        		" xmlTable, SYS.SYSTABLES WHERE XMLSERIALIZE(? as CHAR(10)) = " + 
    			" CAST(TABLENAME AS CHAR(10))", "42Z70");
    }
    
    //Start of user defined function testing
    //At this point, just create a function which involves character strings
    //in it's definition. In subsequent checkin, there will be collation 
    //related testing using this function's return value
    s.executeUpdate("set schema APP");
    s.executeUpdate("CREATE FUNCTION CONCAT_NOCALL(VARCHAR(10), VARCHAR(10)) "+
    		" RETURNS VARCHAR(20) RETURNS NULL ON NULL INPUT EXTERNAL NAME " + 
			"'org.apache.derbyTesting.functionTests.tests.lang.RoutineTest.concat' "+
			" LANGUAGE JAVA PARAMETER STYLE JAVA");
    //DERBY-2831 Creating a function inside a non-existent schema should not
    //fail when it's return type is of character string type. Following is a
    //simple test case copied from DERBY-2831
    s.executeUpdate("CREATE FUNCTION AA.B() RETURNS VARCHAR(10) NO SQL " +
    		"PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME 'aaa.bbb.ccc' ");
    //following fails as expected because aaa.bbb.ccc doesn't exist 
    assertStatementError("XJ001", s, "SELECT AA.B() FROM CUSTOMER ");

    //Start of parameter testing
    //Start with simple ? param in a string comparison
    //Following will work fine because ? is supposed to take it's collation 
    //from the context which in this case is from TABLENAME and TABLENAME
    //has collation type of UCS_BASIC
    s.executeUpdate("set schema APP");
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" ? = TABLENAME");
    ps.setString(1, "SYSCOLUMNS");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});      

    //Do parameter testing with SUBSTR
    //Won't work in territory based database because in 
    //SUBSTR(?, int) = TABLENAME
    //? will get the collation of the current schema which is a user
    //schema and hence the collation type of result of SUBSTR will also be 
    //territory based since the result of SUBSTR always picks up the 
    //collation of it's first operand. So the comparison between left hand
    //side with terriotry based and right hand side with UCS_BASIC will fail.
    checkPreparedStatementError(conn, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" SUBSTR(?,2) = TABLENAME", "42818");
    //To fix the problem above, we need to CAST TABLENAME so that the result 
    //of CAST will pick up the collation of the current schema and this will
    //cause both the operands of SUBSTR(?,2) = CAST(TABLENAME AS CHAR(10)) 
    //to have same collation
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" SUBSTR(?,2) = CAST(TABLENAME AS CHAR(10))");
    ps.setString(1, "aSYSCOLUMNS");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});      

    //Do parameter testing with CONCATENATION operator
    //Following will fail because the result of concatenation will have 
    //collation type of UCS_BASIC whereas the right hand side of = operator
    //will have collation type current schema which is territory based.
    //The reason CONCAT will have collation type of UCS_BASIC is because ? will
    //take collation from context which here will be TABLENAME and hence the
    //result of concatenation will have collation type of it's 2 operands,
    //namely UCS_BASIC
    checkPreparedStatementError(conn, "SELECT TABLENAME FROM SYS.SYSTABLES " +
    		" WHERE TABLENAME || ? LIKE 'SYSCOLUMNS '", "42ZA2");   
    //The query above can be made to work if we are in SYS schema or if we use
    //CAST while we are trying to run the query is user schema
    //Let's try CAST first
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" CAST((TABLENAME || ?) AS CHAR(20)) LIKE 'SYSCOLUMNS'");   
    //try switching to SYS schema and then run the original query without CAST
    s.executeUpdate("set schema SYS");
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES " +
    		" WHERE TABLENAME || ? LIKE 'SYSCOLUMNS'");   
    s.executeUpdate("set schema APP");
    //The following will fail because the left hand side of LIKE has collation
    //derivation of NONE where as the right hand side has collation derivation
    //of IMPLICIT
    assertStatementError("42ZA2", s, "SELECT TABLENAME FROM SYS.SYSTABLES " +
    		" WHERE TABLENAME || 'AA' LIKE 'SYSCOLUMNS '");   
    //To fix the problem, we can use CAST on the left hand side so it's 
    //collation will be picked up from the compilation schema which is same as
    //what happens for the right hand operand.
    checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" CAST ((TABLENAME || 'AA') AS CHAR(12)) LIKE 'SYSCOLUMNS '",
    		null );   

    //Do parameter testing for IS NULL
    //Following query will pass because it doesn't matter what the collation of
    //? is when doing a NULL check
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" ? IS NULL");   
    ps.setString(1, " ");
    rs = ps.executeQuery();
	JDBC.assertEmpty(rs);
	//Now do the testing for IS NOT NULL
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" ? IS NOT NULL");
    ps.setNull(1, java.sql.Types.VARCHAR);
    rs = ps.executeQuery();
    JDBC.assertEmpty(rs);

    //Do parameter testing for LENGTH
    //Following query will fail because LENGTH operator is not allowed to take
    //a parameter. I just wanted to have a test case out for the changes that
    //are going into engine code (ie LengthOperatorNode)
    checkPreparedStatementError(conn, "SELECT COUNT(*) FROM CUSTOMER WHERE " +
    		" LENGTH(?) != 0", "42X36");   

    //Do parameter testing for BETWEEN
    //Following should pass for ? will take the collation from the context and
    //hence, it will be UCS_BASIC
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
	" TABLENAME NOT BETWEEN ? AND TABLENAME");   
    ps.setString(1, " ");
    rs = ps.executeQuery();
	JDBC.assertEmpty(rs);
	//Following will fail because ? will take collation of territory based but
	//the left hand side has collation of UCS_BASIC
    checkPreparedStatementError(conn, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" TABLENAME NOT BETWEEN ? AND 'SYSCOLUMNS'", "42818");   
    
    //Do parameter testing with COALESCE
    //following will pass because the ? inside the COALESCE will take the 
    //collation type of the other operand which is TABLENAME. The result of
    //COALESCE will have collation type of UCS_BASIC and that is the same
    //collation that the ? on rhs of = will get.
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
	" COALESCE(TABLENAME, ?) = ?");   
    ps.setString(1, " ");
    ps.setString(2, "SYSCOLUMNS ");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});

    //Do parameter testing with LTRIM
    //Won't work in territory based database because in 
    //LTRIM(?) = TABLENAME
    //? will get the collation of the current schema which is a user
    //schema and hence the collation type of result of LTRIM will also be 
    //territory based since the result of LTRIM always picks up the 
    //collation of it's operand. So the comparison between left hand
    //side with terriotry based and right hand side with UCS_BASIC will fail.
    checkPreparedStatementError(conn, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" LTRIM(?) = TABLENAME", "42818");
    //To fix the problem above, we need to CAST TABLENAME so that the result 
    //of CAST will pick up the collation of the current schema and this will
    //cause both the operands of LTRIM(?) = CAST(TABLENAME AS CHAR(10)) 
    //to have same collation
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" LTRIM(?) = CAST(TABLENAME AS CHAR(10))");
    ps.setString(1, " SYSCOLUMNS");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});

    //Similar testing for RTRIM
    checkPreparedStatementError(conn, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" RTRIM(?) = TABLENAME", "42818");
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" RTRIM(?) = CAST(TABLENAME AS CHAR(10))");
    ps.setString(1, "SYSCOLUMNS  ");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});

    //Similar testing for TRIM
    //Following won't work because the character string constant 'a' is 
    //picking up the collation of the current schema which is territory based.
    //And the ? in TRIM will pick up it's collation from 'a' and hence the
    //comparison between territory based character string returned from TRIM
    //function will fail against UCS_BASIC based TABLENAME on the right
    checkPreparedStatementError(conn, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" TRIM('a' FROM ?) = TABLENAME", "42818");
    //The problem can be fixed by using CAST on TABLENAME so the resultant of
    //CAST string will compare fine with the output of TRIM. Note CAST always
    //picks up the collation of the compilation schema.
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" TRIM('a' FROM ?) = CAST(TABLENAME AS CHAR(10))");
    ps.setString(1, "aSYSCOLUMNS");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});
    //Another test for TRIM
    //Following will not fail because the ? in TRIM will pick up collation
    //from it's first parameter which is a SUBSTR on TABLENAME and hence the 
    //result of TRIM will have UCS_BASIC collation which matches the collation
    //on the right.
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" TRIM(LEADING SUBSTR(TABLENAME, LENGTH(TABLENAME)) FROM ?) = TABLENAME");
    ps.setString(1, "SYSCOLUMNS");
    rs = ps.executeQuery();
    //No rows returned because the result of TRIM is going to be 'YSCOLUMNS'
    JDBC.assertEmpty(rs);
    
    //Do parameter testing for LOCATE
    //Following will fail because 'LOOKFORME' has collation of territory based
    //but TABLENAME has collation of UCS_BASIC and hence LOCATE will fail 
    //because the collation types of it's two operands do not match
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" LOCATE(?, TABLENAME) != 0");
    ps.setString(1, "ABC");
    rs = ps.executeQuery();
    JDBC.assertEmpty(rs);
    //Just switch the parameter position and try the sql again
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
    		" LOCATE(TABLENAME, ?) != 0");
    ps.setString(1, "ABC");
    rs = ps.executeQuery();
    JDBC.assertEmpty(rs);
    
    //Do parameter testing with IN and subquery
    //Following will work just fine because ? will take it's collation from the
    //context which in this case will be collation of TABLENAME which has 
    //collation type of UCS_BASIC. 
    ps = conn.prepareStatement("SELECT COUNT(*) FROM CUSTOMER WHERE ? IN " +
    		" (SELECT TABLENAME FROM SYS.SYSTABLES)");
    ps.setString(1, "SYSCOLUMNS");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs,new String[][] {{"7"}});

    //Testing for NOT IN. Following won't work becuase ? is taking the 
    //collation type from context which will be from the character string
    //literal 'SYSCOLUMNS'. That literal will have the collation type of the
    //current schema which is the user schema and hence it's collation type
    //will be territory based. But that collation does not match the left hand
    //side on IN clause and hence it results in compliation error.
    checkPreparedStatementError(conn, "SELECT TABLENAME FROM SYS.SYSTABLES " +
    		" WHERE TABLENAME NOT IN (?, ' SYSCOLUMNS ') AND " +
			" CAST(TABLENAME AS CHAR(10)) = 'SYSCOLUMNS' ", "42818");
    //We can make the query work in 2 ways
    //1)Be in the SYS schema and then ? will take the collation of UCS_BASIC
    //because that is what the character string literal ' SYSCOLUMNS ' has.
    s.executeUpdate("set schema SYS");
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES " +
    		" WHERE TABLENAME NOT IN (?, ' SYSCOLUMNS ') AND " +
			" CAST(TABLENAME AS CHAR(10)) = 'SYSCOLUMNS' ");
    ps.setString(1, "aSYSCOLUMNS");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});
    //2)The other way to fix the query would be to do a CAST on TABLENAME so
    //it will have the collation of current schema which is APP 
    s.executeUpdate("set schema APP");
    ps = conn.prepareStatement("SELECT TABLENAME FROM SYS.SYSTABLES WHERE " + 
	" CAST(TABLENAME AS CHAR(10)) NOT IN (?, ' SYSCOLUMNS ') AND " +
	" CAST(TABLENAME AS CHAR(10)) = 'SYSCOLUMNS' ");
    ps.setString(1, "aSYSCOLUMNS");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs,new String[][] {{"SYSCOLUMNS"}});

    //Following will not fail because collation of ? here does not matter 
    //since we are not doing a collation related method 
    s.executeUpdate("set schema SYS");
    ps = conn.prepareStatement("INSERT INTO APP.CUSTOMER(NAME) VALUES(?)");
    ps.setString(1, "SYSCOLUMNS");
    ps.executeUpdate();
    ps.close();
    s.executeUpdate("INSERT INTO APP.CUSTOMER(NAME) VALUES('abc')");
    rs = s.executeQuery("SELECT COUNT(*) FROM APP.CUSTOMER ");
    JDBC.assertFullResultSet(rs,new String[][] {{"9"}});
    //following will fail because NAME has collation type of territory based
    //but 'abc' has collation type of UCS_BASIC
    assertStatementError("42818", s, "DELETE FROM APP.CUSTOMER WHERE NAME = 'abc'");
    //changing to APP schema will fix the problem
    s.executeUpdate("set schema APP");
    s.executeUpdate("DELETE FROM APP.CUSTOMER WHERE NAME = 'abc'");
    rs = s.executeQuery("SELECT COUNT(*) FROM APP.CUSTOMER ");
    JDBC.assertFullResultSet(rs,new String[][] {{"8"}});
    //End of parameter testing
    
    //The user table has to adhere to the collation type of the schema in which
    //it resides. If the table creation breaks that rule, then an exception 
    //will be thrown. DERBY-2879
    s.executeUpdate("set schema APP");
    //following fails as expected because otherwise character types in T will
    //have collation type of UCS_BASIC but the APP schema has collation of
    //territory based
    assertStatementError("42ZA3", s, "CREATE TABLE T AS SELECT TABLENAME " +
    		" FROM SYS.SYSTABLES WITH NO DATA");
    //But following will work because there is no character string type
    //involved. (DERBY-2959)
    s.executeUpdate("CREATE TABLE T AS SELECT COLUMNNUMBER FROM " +
    		" SYS.SYSCOLUMNS WITH NO DATA");
    
    //DERBY-2951
    //Following was giving Assert failure in store code because we were not
    //writing and reading the collation information from the disk.
    s.execute("create table assoc (x char(10) not null primary key, "+
    		" y char(100))");
    s.execute("create table assocout(x char(10))");
    ps = conn.prepareStatement("insert into assoc values (?, 'hello')");
    ps.setString(1, new Integer(10).toString());
    ps.executeUpdate();     
    
    //DERBY-2955
    //We should set the collation type in the bind phase of create table rather
    //than in code generation phase. Otherwise, following sql will give 
    //incorrect exception about collation mismatch for the LIKE clause
    s.execute("CREATE TABLE DERBY_2955 (EMPNAME CHAR(20), CONSTRAINT " +
    		" STAFF9_EMPNAME CHECK (EMPNAME NOT LIKE 'T%'))");
    
    //DERBY-2960
    //Following group by was failing earlier because we were generating
    //SQLVarchar rather than CollatorSQLVarchar in territory based db 
    s.execute("CREATE TABLE DERBY_2960 (C CHAR(10), V VARCHAR(50))");
    s.execute("INSERT INTO DERBY_2960 VALUES ('duplicate', 'is duplicated')");
    rs = s.executeQuery("SELECT SUBSTR(c||v, 1, 4), COUNT(*) FROM DERBY_2960" +
    		" GROUP BY SUBSTR(c||v, 1, 4)");
    JDBC.assertFullResultSet(rs,new String[][] {{"dupl","1"}});
    
    //DERBY-2966
    //Moving to insert row in a territory based db should not cause exception
    ps = conn.prepareStatement("SELECT * FROM CUSTOMER FOR UPDATE",
    		ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
    rs = ps.executeQuery();
    rs.moveToInsertRow();
    rs.close();
    ps.close();
    
    //DERBY-2973
    //alter table modify column should not give an error
    s.execute("CREATE TABLE DERBY_2973 (V VARCHAR(40))");
    s.execute("CREATE INDEX DERBY_2973_I1 ON DERBY_2973 (V)");
    s.execute("ALTER TABLE DERBY_2973 ALTER V SET DATA TYPE VARCHAR(4096)");
    s.execute("INSERT INTO DERBY_2973 VALUES('hello')");
    
    //DERBY-2961
    //Should generate collation sensitive data type when working with something
    //like V AS CLOB insdie XMLSERIALIZE as shown below 
    //SELECT ID, XMLSERIALIZE(V AS CLOB), XMLSERIALIZE(V AS CLOB) FROM 
    //    DERBY_2961 ORDER BY 1
    s.executeUpdate("set schema APP");
    if (XML.classpathMeetsXMLReqs()) {
        checkLangBasedQuery(s, "SELECT ID, XMLSERIALIZE(V AS CLOB) " +
        		" FROM DERBY_2961 ORDER BY 1",
        		new String[][] {{"1",null}});
    }
    
    // Test Collation for functions DERBY-2972
    s.executeUpdate("CREATE FUNCTION HELLO () RETURNS VARCHAR(32000) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.CollationTest.hello' LANGUAGE JAVA PARAMETER STYLE JAVA");
    s.executeUpdate("create table testing (a varchar(2024))");
    s.executeUpdate("insert into testing values('hello')");
    rs = s.executeQuery("select * from testing where a = HELLO()");
    JDBC.assertSingleValueResultSet(rs, "hello");
    s.executeUpdate("DROP FUNCTION hello");
    s.executeUpdate("DROP TABLE  testing");
    
    // Test system functions. Should have UCS_BASIC collation
    // so a statement like this won't work, we need to cast the function.
    assertStatementError("42818",s,"VALUES case WHEN SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.stream.error.logSeverityLevel') = '50000'  THEN 'LOGSHUTDOWN  ERRORS' ELSE 'DONT KNOW' END");
    // cast function output and we it will match the compilation schema and run
    rs = s.executeQuery("VALUES case WHEN CAST(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.stream.error.logSeverityLevel') AS VARCHAR(30000))   = '50000'  THEN 'LOGSHUTDOWN  ERRORS' ELSE 'DONT KNOW' END");
    JDBC.assertSingleValueResultSet(rs,"DONT KNOW");
    
    // Test system table function.  Should have UCS_BASIC collation
    s.executeUpdate("create table lockfunctesttable (i int)");
    conn.setAutoCommit(false);
    s.executeUpdate("insert into lockfunctesttable values(1)");
    // This statement should error because of collation mismatch
    assertStatementError("42818",s,"select * from SYSCS_DIAG.LOCK_TABLE where tablename = 'LOCKFUNCTESTTABLE'");
    // we have to cast for it to work.
    rs = s.executeQuery("select * from SYSCS_DIAG.LOCK_TABLE where CAST(tablename as VARCHAR(128))= 'LOCKFUNCTESTTABLE'");
    JDBC.assertDrainResults(rs,2);
    s.executeUpdate("drop table lockfunctesttable");
    
    
    //DERBY-2910 
    // Test proper collation is set for  implicit cast with 
    // UPPER(CURRENT_DATE) and concatonation.
    
    s.executeUpdate("create table a (vc varchar(30))");
    s.executeUpdate("insert into a values(CURRENT_DATE)");
    rs = s.executeQuery("select vc from a where vc = CURRENT_DATE");
    assertEquals(1,JDBC.assertDrainResults(rs));
    rs = s.executeQuery("select vc from a where vc = UPPER(CURRENT_DATE)");
    JDBC.assertDrainResults(rs,1);
    rs = s.executeQuery("select vc from a where vc =  '' || CURRENT_DATE");
    JDBC.assertDrainResults(rs,1);
    rs = s.executeQuery("select vc from a where '' || CURRENT_DATE = vc");
    assertEquals(1,JDBC.assertDrainResults(rs));
    assertStatementError("42818",s,"select TABLENAME FROM SYS.SYSTABLES WHERE UPPER(CURRENT_DATE) = TABLENAME");
    s.close();

}

// methods used for function testing.

/**
 * Name says it all
 * @return hello
 */
public static String hello() {
        return "hello";
}

/**
 * Just return the value as passed in.  Used to make sure 
 * order by works properly with collation with order by expression
 * @param val value to return
 * @return
 */
public static String mimic(String val) {
    return val;
}


private void setUpTable(Statement s) throws SQLException {

    s.execute("CREATE TABLE CUSTOMER(ID INT, NAME VARCHAR(40), NAMECHAR CHAR(40))");
    
    Connection conn = s.getConnection();

    PreparedStatement ps = conn.prepareStatement("INSERT INTO CUSTOMER VALUES(?,?,?)");
    for (int i = 0; i < NAMES.length; i++)
    {
            ps.setInt(1, i);
            ps.setString(2, NAMES[i]);
            ps.setString(3, NAMES[i]);
            ps.executeUpdate();
    }

    s.execute("create table xmlTable (x xml)");
    s.executeUpdate("insert into xmlTable values(null)");

    s.execute("create table DERBY_2961 (ID INT  GENERATED ALWAYS AS " +
    		" IDENTITY PRIMARY KEY, V XML)");
    s.executeUpdate("insert into DERBY_2961(V) values(null)");
    
    conn.commit();
    ps.close();
}

private void dropTable(Statement s) throws SQLException {
	
    s.execute("DROP TABLE APP.CUSTOMER");     
    s.getConnection().commit();
}

/**
 * Make sure that attempt to prepare the statement will give the passed error
 * 
 * @param con Connection on which query should be prepared
 * @param query Query to be prepared
 * @param error Prepared statement will give this error for the passed query
 */
private void checkPreparedStatementError(Connection con, String query, 
		String error)
{
	try{
	    con.prepareStatement(query);
        fail("Expected error '" + error  + "' but no error was thrown.");
	} catch (SQLException sqle) {
        assertSQLState(error, sqle);		
	}
	
}
/**
 * Execute the passed statement and compare the results against the
 * expectedResult 
 *
 * @param s              statement object to use to execute the query
 * @param query          string with the query to execute.
 * @param expectedResult Null for this means that the passed query is 
 * expected to return an empty resultset. If not empty, then the resultset
 * from the query should match this paramter
 *
 * @throws SQLException
 */
private void checkLangBasedQuery(Statement s, String query, String[][] expectedResult) throws SQLException {
    ResultSet rs = s.executeQuery(query);
    if (expectedResult == null) //expecting empty resultset from the query
    	JDBC.assertEmpty(rs);
    else
    	JDBC.assertFullResultSet(rs,expectedResult);
}
    
  /**
   * Tests only need to run in embedded since collation
   * is a server side operation.
   */
  public static Test suite() {
      
      TestSuite suite = new TestSuite("CollationTest");

        suite.addTest(new CleanDatabaseTestSetup(
                new CollationTest("testDefaultCollation")));
        suite.addTest(collatedSuite("en", "testEnglishCollation"));
        suite.addTest(collatedSuite("no", "testNorwayCollation"));
        suite.addTest(collatedSuite("pl", "testPolishCollation"));
        return suite;
    }
  
  /**
   * Return a suite that uses a single use database with
   * a primary fixture from this test plus potentially other
   * fixtures.
   * @param locale Locale to use for the database
   * @param baseFixture Base fixture from this test.
   * @return suite of tests to run for the given locale
   */
  private static Test collatedSuite(String locale, String baseFixture)
  {
      TestSuite suite = new TestSuite("CollationTest:territory="+locale);
      suite.addTest(new CollationTest(baseFixture));
      
      // DMD.getTables() should not fail after the fix to DERBY-2896
      suite.addTest(DatabaseMetaDataTest.suite());
      return Decorator.territoryCollatedDatabase(suite, locale);
  }

}
