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

      //Do some testing with MAX/MIN operators
      checkLangBasedQuery(s, "SELECT MAX(NAME) maxName FROM CUSTOMER ORDER BY maxName ",
      		new String[][] {{"\u017Bebra"}});   
      checkLangBasedQuery(s, "SELECT MIN(NAME) minName FROM CUSTOMER ORDER BY minName ",
      		new String[][] {{"Acorn"}});   

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
      //sql fails.
      assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE CASE " +
      		" WHEN 1=1 THEN TABLENAME ELSE 'c' END = 'SYSCOLUMNS'");
      //CASTing the result of the CASE expression will solve the problem in the
      //query above. Now both the operands around = operation will have 
      //collation type of territory based and hence the sql won't fail
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE CAST " +
      		" ((CASE WHEN 1=1 THEN TABLENAME ELSE 'c' END) AS CHAR(12)) = " +
			" 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing using CONCATENATION
      //following will fail because result string of concatenation has 
      //collation derivation of NONE. That is because it's 2 operands have
      //different collation types. TABLENAME has collation type of UCS_BASIC
      //but constant character string ' ' has collation type of territory based
      //So the left hand side of = operator has collation derivation of NONE
      //and right hand side has collation derivation of territory based and
      //that causes the = comparison to fail
      assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" TABLENAME || ' ' = 'SYSCOLUMNS '");   
      //CASTing the result of the concat expression will solve the problem in 
      //the query above. Now both the operands around = operation will have 
      //collation type of territory based and hence the sql won't fail
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" CAST (TABLENAME || ' ' AS CHAR(12)) = " +
			" 'SYSCOLUMNS '",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing using COALESCE
      //following will fail because result string of COALESCE has 
      //collation derivation of NONE. That is because it's 2 operands have
      //different collation types. TABLENAME has collation type of UCS_BASIC
      //but constant character string 'c' has collation type of territory based
      //So the left hand side of = operator has collation derivation of NONE
      //and right hand side has collation derivation of territory based and
      //that causes the = comparison to fail
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
      //that causes the = comparison to fail
      assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" NULLIF(TABLENAME, 'c') = 'SYSCOLUMNS'");   
      //CASTing the result of the NULLIF expression will solve the problem in 
      //the query above. Now both the operands around = operation will have 
      //collation type of territory based and hence the sql won't fail
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" NULLIF (CAST (TABLENAME AS CHAR(12)), 'c' ) = " +
			" 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing with MAX/MIN operators
      checkLangBasedQuery(s, "SELECT MAX(NAME) maxName FROM CUSTOMER ORDER BY maxName ",
      		new String[][] {{"\u017Bebra"}});   
      checkLangBasedQuery(s, "SELECT MIN(NAME) minName FROM CUSTOMER ORDER BY minName ",
      		new String[][] {{"aacorn"}});   

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
      //sql fails.
      assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE CASE " +
      		" WHEN 1=1 THEN TABLENAME ELSE 'c' END = 'SYSCOLUMNS'");
      //CASTing the result of the CASE expression will solve the problem in the
      //query above. Now both the operands around = operation will have 
      //collation type of territory based and hence the sql won't fail
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE CAST " +
      		" ((CASE WHEN 1=1 THEN TABLENAME ELSE 'c' END) AS CHAR(12)) = " +
			" 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing using CONCATENATION
      //following will fail because result string of concatenation has 
      //collation derivation of NONE. That is because it's 2 operands have
      //different collation types. TABLENAME has collation type of UCS_BASIC
      //but constant character string ' ' has collation type of territory based
      //So the left hand side of = operator has collation derivation of NONE
      //and right hand side has collation derivation of territory based and
      //that causes the = comparison to fail
      assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" TABLENAME || ' ' = 'SYSCOLUMNS '");   
      //CASTing the result of the concat expression will solve the problem in 
      //the query above. Now both the operands around = operation will have 
      //collation type of territory based and hence the sql won't fail
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" CAST (TABLENAME || ' ' AS CHAR(12)) = " +
			" 'SYSCOLUMNS '",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing using COALESCE
      //following will fail because result string of COALESCE has 
      //collation derivation of NONE. That is because it's 2 operands have
      //different collation types. TABLENAME has collation type of UCS_BASIC
      //but constant character string 'c' has collation type of territory based
      //So the left hand side of = operator has collation derivation of NONE
      //and right hand side has collation derivation of territory based and
      //that causes the = comparison to fail
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
      //that causes the = comparison to fail
      assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" NULLIF(TABLENAME, 'c') = 'SYSCOLUMNS'");   
      //CASTing the result of the NULLIF expression will solve the problem in 
      //the query above. Now both the operands around = operation will have 
      //collation type of territory based and hence the sql won't fail
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" NULLIF (CAST (TABLENAME AS CHAR(12)), 'c' ) = " +
			" 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing with MAX/MIN operators
      checkLangBasedQuery(s, "SELECT MAX(NAME) maxName FROM CUSTOMER ORDER BY maxName ",
      		new String[][] {{"aacorn"}});   
      checkLangBasedQuery(s, "SELECT MIN(NAME) minName FROM CUSTOMER ORDER BY minName ",
      		new String[][] {{"Acorn"}});   

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
      //sql fails.
      assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE CASE " +
      		" WHEN 1=1 THEN TABLENAME ELSE 'c' END = 'SYSCOLUMNS'");
      //CASTing the result of the CASE expression will solve the problem in the
      //query above. Now both the operands around = operation will have 
      //collation type of territory based and hence the sql won't fail
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE CAST " +
      		" ((CASE WHEN 1=1 THEN TABLENAME ELSE 'c' END) AS CHAR(12)) = " +
			" 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing using CONCATENATION
      //following will fail because result string of concatenation has 
      //collation derivation of NONE. That is because it's 2 operands have
      //different collation types. TABLENAME has collation type of UCS_BASIC
      //but constant character string ' ' has collation type of territory based
      //So the left hand side of = operator has collation derivation of NONE
      //and right hand side has collation derivation of territory based and
      //that causes the = comparison to fail
      assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" TABLENAME || ' ' = 'SYSCOLUMNS '");   
      //CASTing the result of the concat expression will solve the problem in 
      //the query above. Now both the operands around = operation will have 
      //collation type of territory based and hence the sql won't fail
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" CAST (TABLENAME || ' ' AS CHAR(12)) = " +
			" 'SYSCOLUMNS '",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing using COALESCE
      //following will fail because result string of COALESCE has 
      //collation derivation of NONE. That is because it's 2 operands have
      //different collation types. TABLENAME has collation type of UCS_BASIC
      //but constant character string 'c' has collation type of territory based
      //So the left hand side of = operator has collation derivation of NONE
      //and right hand side has collation derivation of territory based and
      //that causes the = comparison to fail
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
      //that causes the = comparison to fail
      assertStatementError("42818", s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
		" NULLIF(TABLENAME, 'c') = 'SYSCOLUMNS'");   
      //CASTing the result of the NULLIF expression will solve the problem in 
      //the query above. Now both the operands around = operation will have 
      //collation type of territory based and hence the sql won't fail
      checkLangBasedQuery(s, "SELECT TABLENAME FROM SYS.SYSTABLES WHERE " +
      		" NULLIF (CAST (TABLENAME AS CHAR(12)), 'c' ) = " +
			" 'SYSCOLUMNS'",
      		new String[][] {{"SYSCOLUMNS"} });   

      //Do some testing with MAX/MIN operators
      checkLangBasedQuery(s, "SELECT MAX(NAME) maxName FROM CUSTOMER ORDER BY maxName ",
      		new String[][] {{"\u017Bebra"}});   
      checkLangBasedQuery(s, "SELECT MIN(NAME) minName FROM CUSTOMER ORDER BY minName ",
      		new String[][] {{"aacorn"}});   
      
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
    
  public static Test suite() {

        Test test =  TestConfiguration.defaultSuite(CollationTest.class);
        test = TestConfiguration.additionalDatabaseDecorator(test, "defaultdb");
        test = TestConfiguration.additionalDatabaseDecorator(test, "endb");
        test = TestConfiguration.additionalDatabaseDecorator(test, "nordb");
        test = TestConfiguration.additionalDatabaseDecorator(test, "poldb");
        return test;
    }

}
