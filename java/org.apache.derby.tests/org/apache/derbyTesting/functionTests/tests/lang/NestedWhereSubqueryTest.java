/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.NestedWhereSubqueryTest
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Nested WHERE subquery tests. Tests nested WHERE EXISTS | ANY | IN functionality.
 *
 * Please refer to DERBY-3301 for more details.
 */
public class NestedWhereSubqueryTest extends BaseJDBCTestCase {

	public NestedWhereSubqueryTest(String name) {
		super(name);
	}

	/**
	 * Main test body
	 * 
	 * @throws SQLException
	 */
	public void testBasicOperations()
		throws SQLException {
		Statement s = createStatement();

		/*
		 * Create tables needed for DERBY-3301 regression test
		 */
		StringBuffer sb = new StringBuffer();
		sb.append("CREATE TABLE departments ( ");
		sb.append("ID INTEGER NOT NULL, ");
		sb.append("NAME VARCHAR(32) NOT NULL, ");
		sb.append("COMPANYID INTEGER, ");
		sb.append("CONSTRAINT DEPTS_PK PRIMARY KEY (ID) ");
		sb.append(")");
		s.executeUpdate(sb.toString());

		sb = new StringBuffer();
		sb.append("CREATE TABLE employees ( ");
		sb.append("EMPID INTEGER NOT NULL, ");
		sb.append("FIRSTNAME VARCHAR(32) NOT NULL, ");
		sb.append("DEPARTMENT INTEGER, ");
		sb.append("CONSTRAINT PERS_DEPT_FK FOREIGN KEY (DEPARTMENT) REFERENCES departments, ");
		sb.append("CONSTRAINT EMPS_PK PRIMARY KEY (EMPID) ");
		sb.append(")");
		s.executeUpdate(sb.toString());

		sb = new StringBuffer();
		sb.append("CREATE TABLE projects ( ");
		sb.append("PROJID INTEGER NOT NULL, ");
		sb.append("NAME VARCHAR(32) NOT NULL, ");
		sb.append("CONSTRAINT PROJS_PK PRIMARY KEY (PROJID) ");
		sb.append(")");
		s.executeUpdate(sb.toString());

		sb = new StringBuffer();
		sb.append("CREATE TABLE project_employees ( ");
		sb.append("PROJID INTEGER REFERENCES projects NOT NULL, ");
		sb.append("EMPID INTEGER REFERENCES employees NOT NULL ");
		sb.append(")");
		s.executeUpdate(sb.toString());

		/*
		 * Fill some data into the tables
		 */
		s.executeUpdate("INSERT INTO departments VALUES (1, 'Research', 1)");
		s.executeUpdate("INSERT INTO departments VALUES (2, 'Marketing', 1)");

		s.executeUpdate("INSERT INTO employees VALUES (11, 'Alex', 1)");
		s.executeUpdate("INSERT INTO employees VALUES (12, 'Bill', 1)");
		s.executeUpdate("INSERT INTO employees VALUES (13, 'Charles', 1)");
		s.executeUpdate("INSERT INTO employees VALUES (14, 'David', 2)");
		s.executeUpdate("INSERT INTO employees VALUES (15, 'Earl', 2)");

		s.executeUpdate("INSERT INTO projects VALUES (101, 'red')");
		s.executeUpdate("INSERT INTO projects VALUES (102, 'orange')");
		s.executeUpdate("INSERT INTO projects VALUES (103, 'yellow')");

		s.executeUpdate("INSERT INTO project_employees VALUES (102, 13)");
		s.executeUpdate("INSERT INTO project_employees VALUES (101, 13)");
		s.executeUpdate("INSERT INTO project_employees VALUES (102, 12)");
		s.executeUpdate("INSERT INTO project_employees VALUES (103, 15)");
		s.executeUpdate("INSERT INTO project_employees VALUES (103, 14)");
		s.executeUpdate("INSERT INTO project_employees VALUES (101, 12)");
		s.executeUpdate("INSERT INTO project_employees VALUES (101, 11)");

		/*
		 * Preliminary data check
		 */
		ResultSet rs = s.executeQuery("select * from employees");
		String[][] expectedRows = {{"11", "Alex", "1"},
									{"12", "Bill", "1"},
									{"13", "Charles", "1"},
									{"14", "David", "2"},
									{"15", "Earl", "2"}};		
		JDBC.assertUnorderedResultSet(rs, expectedRows);

		rs = s.executeQuery("select * from departments");
		expectedRows = new String [][] {{"1", "Research", "1"},
										{"2","Marketing","1"}};		
		JDBC.assertUnorderedResultSet(rs, expectedRows);

		rs = s.executeQuery("select * from projects");
		expectedRows = new String [][] {{"101","red"},
										{"102","orange"},
										{"103","yellow"}};		
		JDBC.assertUnorderedResultSet(rs, expectedRows);

		rs = s.executeQuery("select * from project_employees");
		expectedRows = new String [][] {{"102","13"},
										{"101","13"},
										{"102","12"},
										{"103","15"},
										{"103","14"},
										{"101","12"},
										{"101","11"}};		
		JDBC.assertUnorderedResultSet(rs, expectedRows);

		/*
		 * DERBY-3301: This query should return 7 rows
		 */
		sb = new StringBuffer();
		sb.append("select unbound_e.empid, unbound_p.projid ");
		sb.append("from departments this, ");
		sb.append("     employees unbound_e, ");
		sb.append("     projects unbound_p ");
		sb.append("where exists ( ");
		sb.append("  select 1 from employees this_employees_e ");
		sb.append("  where exists ( ");
		sb.append("    select 1 from project_employees this_employees_e_projects_p ");
		sb.append("    where this_employees_e_projects_p.empid = this_employees_e.empid ");
		sb.append("    and this_employees_e.department = this.id ");
		sb.append("    and unbound_p.projid = this_employees_e_projects_p.projid ");
		sb.append("    and unbound_e.empid = this_employees_e.empid) ");
		sb.append(" )");

		rs = s.executeQuery(sb.toString());
		expectedRows = new String [][] {{"13", "101"},
										{"12", "101"},
										{"11", "101"},
										{"13", "102"},
										{"12", "102"},
										{"15", "103"},
										{"14", "103"}};
		JDBC.assertUnorderedResultSet(rs, expectedRows);
		
		/* A variation of the above WHERE EXISTS but using IN should return the same rows */
		sb = new StringBuffer();
		sb.append("select unbound_e.empid, unbound_p.projid ");
		sb.append("from departments this, ");
		sb.append("     employees unbound_e, ");
		sb.append("     projects unbound_p ");
		sb.append("where exists ( "); 
		sb.append(" select 1 from employees this_employees_e ");
		sb.append("     where this_employees_e.empid in ( ");
		sb.append("         select this_employees_e_projects_p.empid ");
		sb.append("           from project_employees this_employees_e_projects_p ");
		sb.append("         where this_employees_e_projects_p.empid = this_employees_e.empid ");
		sb.append("         and this_employees_e.department = this.id ");
		sb.append("         and unbound_p.projid = this_employees_e_projects_p.projid ");
		sb.append("         and unbound_e.empid = this_employees_e.empid) ");
		sb.append("     )");

		rs = s.executeQuery(sb.toString());
		JDBC.assertUnorderedResultSet(rs, expectedRows);

		/* A variation of the above WHERE EXISTS but using ANY should return the same rows */
		sb = new StringBuffer();
		sb.append("select unbound_e.empid, unbound_p.projid ");
		sb.append("from departments this, ");
		sb.append("     employees unbound_e, ");
		sb.append("     projects unbound_p ");
		sb.append("where exists ( "); 
		sb.append(" select 1 from employees this_employees_e ");
		sb.append("     where this_employees_e.empid = any ( ");
		sb.append("         select this_employees_e_projects_p.empid ");
		sb.append("           from project_employees this_employees_e_projects_p ");
		sb.append("         where this_employees_e_projects_p.empid = this_employees_e.empid ");
		sb.append("         and this_employees_e.department = this.id ");
		sb.append("         and unbound_p.projid = this_employees_e_projects_p.projid ");
		sb.append("         and unbound_e.empid = this_employees_e.empid) ");
		sb.append("     )");

		rs = s.executeQuery(sb.toString());
		JDBC.assertUnorderedResultSet(rs, expectedRows);

		/* 
		 * The next 5 queries were also found problematic as part DERBY-3301 
		 */
		sb = new StringBuffer();
		sb.append("select unbound_e.empid from departments this, employees unbound_e ");
		sb.append("where exists ( ");
		sb.append("   select 1 from employees this_employees_e ");
		sb.append("      where this_employees_e.department = this.id and ");
		sb.append("            unbound_e.empid = this_employees_e.empid and this.id = 2)");

		rs = s.executeQuery(sb.toString());		
		expectedRows = new String [][] {{"14"},{"15"}};		
		JDBC.assertUnorderedResultSet(rs, expectedRows);
		
		sb = new StringBuffer();
		sb.append("select this.id,unbound_e.empid,unbound_p.projid from departments this, ");
		sb.append("        employees unbound_e, projects unbound_p ");
		sb.append("where exists ( ");
		sb.append("   select 1 from employees this_employees_e ");
		sb.append("   where exists ( ");
		sb.append("      select 1 from project_employees this_employees_e_projects_p ");
		sb.append("      where this_employees_e_projects_p.\"EMPID\" = this_employees_e.empid and ");
		sb.append("         unbound_p.projid = this_employees_e_projects_p.projid and ");
		sb.append("         this_employees_e.department = this.id and ");
		sb.append("         unbound_e.empid = this_employees_e.empid ");
		sb.append(" )) ");		
		
		rs = s.executeQuery(sb.toString());
		expectedRows = new String [][] {{"1","11","101"},
										{"1","12","101"},
										{"1","13","101"},
										{"1","12","102"},
										{"1","13","102"},
										{"2","14","103"},
										{"2","15","103"}};
		JDBC.assertUnorderedResultSet(rs, expectedRows);
		
		sb = new StringBuffer();
		sb.append("select unbound_e.empid,unbound_p.projid from departments this, ");
		sb.append("       employees unbound_e, projects unbound_p ");
		sb.append("where exists ( ");
		sb.append("   select 1 from employees this_employees_e ");
		sb.append("   where exists ( ");
		sb.append("      select 1 from project_employees this_employees_e_projects_p ");
		sb.append("      where this_employees_e_projects_p.\"EMPID\" = this_employees_e.empid ");
		sb.append("            and unbound_p.projid = this_employees_e_projects_p.projid ");
		sb.append("            and this_employees_e.department = this.id ");
		sb.append("            and unbound_e.empid = this_employees_e.empid ");
		sb.append("            and this.id = 1)) ");
		
		rs = s.executeQuery(sb.toString());
		expectedRows = new String [][] {{"11","101"},
										{"12","101"},
										{"13","101"},
										{"12","102"},
										{"13","102"}};
		JDBC.assertUnorderedResultSet(rs, expectedRows);
		
		sb = new StringBuffer();
		sb.append("select unbound_e.empid,unbound_p.projid from departments this, ");
		sb.append("       employees unbound_e, projects unbound_p ");
		sb.append("where exists ( ");
		sb.append("   select 1 from employees this_employees_e ");
		sb.append("   where exists ( ");
		sb.append("      select 1 from project_employees this_employees_e_projects_p ");
		sb.append("      where this_employees_e_projects_p.\"EMPID\" = this_employees_e.empid ");
		sb.append("            and unbound_p.projid = this_employees_e_projects_p.projid ");
		sb.append("            and this_employees_e.department = this.id ");
		sb.append("            and unbound_e.empid = this_employees_e.empid ");
		sb.append("            and this.companyid = 1))");
		
		rs = s.executeQuery(sb.toString());
		expectedRows = new String [][] {{"11","101"},
										{"12","101"},
										{"13","101"},
										{"12","102"},
										{"13","102"},
										{"14","103"},
										{"15","103"}};
		JDBC.assertUnorderedResultSet(rs, expectedRows);
		
		sb = new StringBuffer();
		sb.append("select unbound_e.empid, unbound_p.projid ");
		sb.append("from departments this, ");
		sb.append("     employees unbound_e, ");
		sb.append("     projects unbound_p ");
		sb.append("where exists ( ");
		sb.append("   select 1 from employees this_employees_e ");
		sb.append("   where 1 = 1 and exists ( ");
		sb.append("      select 1 from project_employees this_employees_e_projects_p ");
		sb.append("      where this_employees_e_projects_p.empid = this_employees_e.empid ");
		sb.append("            and this_employees_e.department = this.id ");
		sb.append("            and unbound_p.projid = this_employees_e_projects_p.projid ");
		sb.append("            and unbound_e.empid = this_employees_e.empid) ");
		sb.append(")");

		rs = s.executeQuery(sb.toString());
		expectedRows = new String [][] {{"11","101"},
										{"12","101"},
										{"13","101"},
										{"12","102"},
										{"13","102"},
										{"14","103"},
										{"15","103"}};  
		JDBC.assertUnorderedResultSet(rs, expectedRows);
		
		/* Variation of the above using WHERE IN ... WHERE IN */
		sb = new StringBuffer();
		sb.append("select unbound_e.empid, unbound_p.projid ");
		sb.append("from departments this, employees unbound_e, projects unbound_p ");
		sb.append("where this.id in ( ");
		sb.append("   select this_employees_e.department from employees this_employees_e ");
		sb.append("   where this_employees_e.empid in ( ");
		sb.append("      select this_employees_e_projects_p.empid ");
		sb.append("      from project_employees this_employees_e_projects_p ");
		sb.append("      where this_employees_e_projects_p.empid = this_employees_e.empid ");
		sb.append("            and this_employees_e.department = this.id ");
		sb.append("            and unbound_p.projid = this_employees_e_projects_p.projid ");
		sb.append("            and unbound_e.empid = this_employees_e.empid)");
		sb.append(")");
		
		rs = s.executeQuery(sb.toString());
		/* Expected rows are as above */
		JDBC.assertUnorderedResultSet(rs, expectedRows);

		/* Variation of the above using WHERE ANY ... WHERE ANY */
		sb = new StringBuffer();
		sb.append("select unbound_e.empid, unbound_p.projid ");
		sb.append("from departments this, employees unbound_e, projects unbound_p ");
		sb.append("where this.id = any ( ");
		sb.append("   select this_employees_e.department from employees this_employees_e ");
		sb.append("   where this_employees_e.empid = any ( ");
		sb.append("      select this_employees_e_projects_p.empid ");
		sb.append("      from project_employees this_employees_e_projects_p ");
		sb.append("      where this_employees_e_projects_p.empid = this_employees_e.empid ");
		sb.append("            and this_employees_e.department = this.id ");
		sb.append("            and unbound_p.projid = this_employees_e_projects_p.projid ");
		sb.append("            and unbound_e.empid = this_employees_e.empid)");
		sb.append(")");
		
		rs = s.executeQuery(sb.toString());
		/* Expected rows are as above */
		JDBC.assertUnorderedResultSet(rs, expectedRows);
		
		/*
		 * DERBY-3321 revealed an NPE with a subquery in the [NOT] EXIST subuery FromList.
		 */
		s.executeUpdate("create table a (aa int, bb int)");
		s.executeUpdate("create table b (bb int)");
		s.executeUpdate("insert into a values (1,1),(1,2),(2,2)");
		s.executeUpdate("insert into b values (1)");
		
		/* NOT EXISTS */
		sb = new StringBuffer();
		sb.append("select * from a ");
		sb.append("where not exists ");
		sb.append("(select bb from (select bb from b) p where a.bb=p.bb)");
		rs = s.executeQuery(sb.toString());
		expectedRows = new String [][] {{"1","2"},		
										{"2","2"}};
		JDBC.assertUnorderedResultSet(rs, expectedRows);		
		
		/* EXISTS */
		sb = new StringBuffer();
		sb.append("select * from a ");
		sb.append("where exists ");
		sb.append("(select bb from (select bb from b) p where a.bb=p.bb)");
		rs = s.executeQuery(sb.toString());
		expectedRows = new String [][] {{"1","1"}};										
		JDBC.assertUnorderedResultSet(rs, expectedRows);	
		
		/*
		 * Clean up the tables used.
		 */				
		s.executeUpdate("drop table project_employees");	
		s.executeUpdate("drop table projects");
		s.executeUpdate("drop table employees");
		s.executeUpdate("drop table departments");			
		
		s.executeUpdate("drop table a");	
		s.executeUpdate("drop table b");	
		
		s.close();
	}

    /**
     * Allow multiple columns in EXISTS subquery. SQL feature T501 "Enhanced
     * EXISTS predicate".
     * <p/>
     * Strictly speaking, this test belongs in a general subquery test class,
     * but pending conversion of subquery.sql to JUnit, testDerby5501 resides
     * here (FIXME).
     */
    public void testDerby5501 () throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();

        s.executeUpdate("create table t5501a(i int, j int, primary key(i,j))");
        s.executeUpdate("create table t5501b(i int)");

        s.executeUpdate("insert into t5501a values (1,1),(2,2),(3,3),(4,4)");
        s.executeUpdate("insert into t5501b values 1,3,5");


        // works before DERBY-5501
        ResultSet rs = s.executeQuery(
            "select i from t5501b t1 where not exists " +
            "    (select i from t5501a t2 where t1.i=t2.i)");
        JDBC.assertUnorderedResultSet(rs, new String [][] {{"5"}});

        rs = s.executeQuery(
            "select i+3.14 from t5501b t1 where not exists " +
            "    (select i+3.14 from t5501a t2 where t1.i=t2.i)");
        JDBC.assertUnorderedResultSet(rs, new String [][] {{"8.14"}});

        // works before DERBY-5501: "*" is specially handled already
        rs = s.executeQuery(
            "select i from t5501b t1 where not exists " +
            "    (select * from t5501a t2 where t1.i=t2.i)");
        JDBC.assertUnorderedResultSet(rs, new String [][] {{"5"}});


        // fails before DERBY-5501
        rs = s.executeQuery(
            "select i from t5501b t1 where not exists " +
            "    (select i,j from t5501a t2 where t1.i=t2.i)");
        JDBC.assertUnorderedResultSet(rs, new String [][] {{"5"}});

        rs = s.executeQuery(
            "select i from t5501b t1 where not exists " +
            "    (select true,j from t5501a t2 where t1.i=t2.i)");
        JDBC.assertUnorderedResultSet(rs, new String [][] {{"5"}});

        s.executeUpdate("delete from t5501a where i=1");
        rs = s.executeQuery(
            "select i from t5501b t1 where not exists " +
            "    (select i,j from t5501a t2 where t1.i=t2.i)");
        JDBC.assertUnorderedResultSet(rs, new String [][] {{"1"}, {"5"}});

        // should still fail: no column "k" exists
        assertCompileError(
            "42X04",
            "select i from t5501b t1 where not exists " +
            "    (select i,k from t5501a t2 where t1.i=t2.i)");

        // should still fail: no table "foo" exists
        assertCompileError(
            "42X10",
            "select i from t5501b t1 where not exists " +
            "    (select t2.*,foo.* from t5501a t2 where t1.i=t2.i)");

        // should still fail: illegal integer format in cast
        assertCompileError(
            "22018",
            "select i from t5501b t1 where not exists " +
            "   (select t2.*,cast('a' as int) from t5501a t2 where t1.i=t2.i)");
    }


    public static Test suite() {
		return TestConfiguration.defaultSuite(NestedWhereSubqueryTest.class);
	}
}