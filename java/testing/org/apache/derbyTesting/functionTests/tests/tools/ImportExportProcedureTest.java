/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.ImportExportProcedureTest

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

 */
package org.apache.derbyTesting.functionTests.tests.tools;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Converted from ieptests.sql
 *
 */
public class ImportExportProcedureTest extends BaseJDBCTestCase {

    private static final String INVALID_DELIMITER = "XIE0J";

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public ImportExportProcedureTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        BaseTestSuite suite = new BaseTestSuite("ImportExportProcedureTest");
        suite.addTest(TestConfiguration.defaultSuite(ImportExportProcedureTest.class));
        return new SupportFilesSetup(suite, new String[] { 
        		"functionTests/testData/ImportExport/db2ttypes.del",
        		"functionTests/testData/ImportExport/mixednl.del",
        		"functionTests/testData/ImportExport/position_info.del"
        });
    }

    public void testImportExportProcedures() throws Exception
    {
        ResultSet rs = null;

        CallableStatement cSt;
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;
        
        Connection conn = getConnection();
        
        st.executeUpdate(
            "create table ex_emp(id int , name char(7) , skills "
            + "varchar(200), salary decimal(10,2)) ");
        
        //table used for import
        
        st.executeUpdate(
            "create table imp_emp(id int , name char(7), skills "
            + "varchar(200), salary decimal(10,2)) ");
        
        //After an export from ex_emp and import to imp_emp both 
        // tables should havesame data.double delimter cases with 
        // default character delimter "field seperator character 
        // inside a double delimited string as first line
        
        st.executeUpdate(
            "insert into ex_emp values(99, 'smith' , "
            + "'tennis\"p,l,ayer\"', 190.55) ");
        
        // Perform Export:

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/emp.dat");        

        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'EX_EMP' "
            + ", 'extinout/emp.dat' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        // Perform Import
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'IMP_EMP' "
            + ", 'extinout/emp.dat' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        st.executeUpdate(
            " insert into ex_emp values(100, 'smith' , "
            + "'tennis\"player\"', 190.55) ");
        
        st.executeUpdate(
            " insert into ex_emp values(101, 'smith' , "
            + "'tennis\"player', 190.55) ");
        
        st.executeUpdate(
            " insert into ex_emp values(102, 'smith' , "
            + "'\"tennis\"player', 190.55) ");
        
        st.executeUpdate(
            " insert into ex_emp values(103, 'smith' , "
            + "'\"tennis\"player\"', 190.55) ");
        
        st.executeUpdate(
            " insert into ex_emp values(104, 'smith' , "
            + "'\"tennis\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\""
            + "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"player\"', null) ");
        
        //empty string
        
        st.executeUpdate(
            "insert into ex_emp values(105, 'smith' , '\"\"', 190.55) ");
        
        //just delimeter inside
        
        st.executeUpdate(
            "insert into ex_emp values(106, 'smith' , "
            + "'\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"', 190.55)");
        
        //null value
        
        st.executeUpdate(
            "insert into ex_emp values(107, 'smith\"' , null, 190.55) ");
        
        //all values are nulls
        
        st.executeUpdate(
            "insert into ex_emp values(108, null , null, null) ");
        
        // Perform Export:
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/emp.dat");

        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'EX_EMP' "
            + ", 'extinout/emp.dat' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        // Perform Import
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'IMP_EMP' "
            + ", 'extinout/emp.dat' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from ex_emp");
        
        expColNames = new String [] {"ID", "NAME", "SKILLS", "SALARY"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99", "smith", "tennis\"p,l,ayer\"", "190.55"},
            {"100", "smith", "tennis\"player\"", "190.55"},
            {"101", "smith", "tennis\"player", "190.55"},
            {"102", "smith", "\"tennis\"player", "190.55"},
            {"103", "smith", "\"tennis\"player\"", "190.55"},
            {"104", "smith", "\"tennis\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\""
                + "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"player\"", null},
            {"105", "smith", "\"\"", "190.55"},
            {"106", "smith", "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", "190.55"},
            {"107", "smith\"", null, "190.55"},
            {"108", null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from imp_emp");
        
        expColNames = new String [] {"ID", "NAME", "SKILLS", "SALARY"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99", "smith", "tennis\"p,l,ayer\"", "190.55"},
            {"99", "smith", "tennis\"p,l,ayer\"", "190.55"},
            {"100", "smith", "tennis\"player\"", "190.55"},
            {"101", "smith", "tennis\"player", "190.55"},
            {"102", "smith", "\"tennis\"player", "190.55"},
            {"103", "smith", "\"tennis\"player\"", "190.55"},
            {"104", "smith", "\"tennis\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\""
                + "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"player\"", null},
            {"105", "smith", "\"\"", "190.55"},
            {"106", "smith", "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", "190.55"},
            {"107", "smith\"", null, "190.55"},
            {"108", null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //checking query
        
        rs = st.executeQuery(
            "select count(*) from imp_emp, ex_emp where "
            + "ex_emp.id = imp_emp.id and "
            + "(ex_emp.skills=imp_emp.skills or (ex_emp.skills is "
            + "NULL and imp_emp.skills is NULL))");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"11"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 7,
            " delete from imp_emp where id < 105");

        //DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/emp.dat");

        //export from ex_emp using the a query only rows that got 
        // deleted in imp_emp
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select * from "
            + "ex_emp where id < 105', 'extinout/emp.dat' , null, "
            + "null, null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, "
            + "'IMP_EMP' , 'extinout/emp.dat' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        //checking query
        
        rs = st.executeQuery(
            "select count(*) from imp_emp, ex_emp where "
            + "ex_emp.id = imp_emp.id and "
            + "(ex_emp.skills=imp_emp.skills or (ex_emp.skills is "
            + "NULL and imp_emp.skills is NULL))");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"10"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/emp.dat");

        //export the columns in different column order than in the 
        // table.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select name , "
            + "salary , skills, id from ex_emp where id < 105', "
            + "'extinout/emp.dat' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        // import them in to a with order different than in the table
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'IMP_EMP' "
            + ",'NAME, SALARY, SKILLS, ID', null, "
            + "'extinout/emp.dat', null, null, null, 1) ");
        assertUpdateCount(cSt, 0);
        
        //check query
        
        rs = st.executeQuery(
            "select count(*) from imp_emp, ex_emp where "
            + "ex_emp.id = imp_emp.id and "
            + "(ex_emp.skills=imp_emp.skills or (ex_emp.skills is "
            + "NULL and imp_emp.skills is NULL))");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // do import replace into the table with table order but 
        // using column indexes
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'IMP_EMP' "
            + ",null, '4, 1, 3, 2', 'extinout/emp.dat', null, "
            + "null, null, 1) ");
        assertUpdateCount(cSt, 0);
        
        //check query
        
        rs = st.executeQuery(
            "select count(*) from imp_emp, ex_emp where "
            + "ex_emp.id = imp_emp.id and "
            + "(ex_emp.skills=imp_emp.skills or (ex_emp.skills is "
            + "NULL and imp_emp.skills is NULL))");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //replace using insert column names and column indexes
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'IMP_EMP' "
            + ",'SALARY, ID, SKILLS, NAME', '2, 4, 3, 1', "
            + "'extinout/emp.dat', null, null, null, 1) ");
        assertUpdateCount(cSt, 0);
        
        //check query
        
        rs = st.executeQuery(
            "select count(*) from imp_emp, ex_emp where "
            + "ex_emp.id = imp_emp.id and "
            + "(ex_emp.skills=imp_emp.skills or (ex_emp.skills is "
            + "NULL and imp_emp.skills is NULL))");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/emp.dat");

        //-testing with different delimiters single quote(') as 
        // character delimiter
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'EX_EMP' "
            + ", 'extinout/emp.dat' , null, '''', null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, "
            + "'IMP_EMP' , 'extinout/emp.dat' , null, '''', null, 1) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from imp_emp ");
        
        expColNames = new String [] {"ID", "NAME", "SKILLS", "SALARY"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99", "smith", "tennis\"p,l,ayer\"", "190.55"},
            {"100", "smith", "tennis\"player\"", "190.55"},
            {"101", "smith", "tennis\"player", "190.55"},
            {"102", "smith", "\"tennis\"player", "190.55"},
            {"103", "smith", "\"tennis\"player\"", "190.55"},
            {"104", "smith", "\"tennis\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\""
                + "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"player\"", null},
            {"105", "smith", "\"\"", "190.55"},
            {"106", "smith", "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", "190.55"},
            {"107", "smith\"", null, "190.55"},
            {"108", null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/emp.dat");

        // single quote(') as column delimiter
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'EX_EMP' "
            + ", 'extinout/emp.dat' , '''',null, null) ");
        assertUpdateCount(cSt, 0);
        
        assertUpdateCount(st, 10,
            " delete from imp_emp ");
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, "
            + "'IMP_EMP' , 'extinout/emp.dat' , '''', null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from imp_emp");
        
        expColNames = new String [] {"ID", "NAME", "SKILLS", "SALARY"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99", "smith", "tennis\"p,l,ayer\"", "190.55"},
            {"100", "smith", "tennis\"player\"", "190.55"},
            {"101", "smith", "tennis\"player", "190.55"},
            {"102", "smith", "\"tennis\"player", "190.55"},
            {"103", "smith", "\"tennis\"player\"", "190.55"},
            {"104", "smith", "\"tennis\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\""
                + "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"player\"", null},
            {"105", "smith", "\"\"", "190.55"},
            {"106", "smith", "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", "190.55"},
            {"107", "smith\"", null, "190.55"},
            {"108", null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/emp.dat");
 
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'EX_EMP' "
            + ", 'extinout/emp.dat' , '*', '%', null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'EX_EMP' "
            + ", 'extinout/emp.dat' , '*', '%', null, 1) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from imp_emp ");
        
        expColNames = new String [] {"ID", "NAME", "SKILLS", "SALARY"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99", "smith", "tennis\"p,l,ayer\"", "190.55"},
            {"100", "smith", "tennis\"player\"", "190.55"},
            {"101", "smith", "tennis\"player", "190.55"},
            {"102", "smith", "\"tennis\"player", "190.55"},
            {"103", "smith", "\"tennis\"player\"", "190.55"},
            {"104", "smith", "\"tennis\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\""
                + "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"player\"", null},
            {"105", "smith", "\"\"", "190.55"},
            {"106", "smith", "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"", "190.55"},
            {"107", "smith\"", null, "190.55"},
            {"108", null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //cases for identity columnscreate table emp1(id int 
        // generated always as identity (start with 100), name 
        // char(7),              skills varchar(200), salary 
        // decimal(10,2),skills varchar(200))check import export 
        // with real and double that can not be explictitlycasted 
        // from VARCHAR type .
        
        st.executeUpdate(
            "create table noncast(c1 double , c2 real ) ");
        
        st.executeUpdate(
            " insert into noncast values(1.5 , 6.7 ) ");
        
        st.executeUpdate(
            " insert into noncast values(2.5 , 8.999) ");
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/noncast.dat");
 
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('APP' , "
            + "'NONCAST' , 'extinout/noncast.dat'  , null , null , null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, "
            + "'NONCAST' , 'extinout/noncast.dat'  , null , null , "
            + "null , 0) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'NONCAST', "
            + "'C2 , C1' , '2, 1' , 'extinout/noncast.dat'  , null "
            + ", null , null , 0) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from noncast ");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1.5", "6.7"},
            {"2.5", "8.999"},
            {"1.5", "6.7"},
            {"2.5", "8.999"},
            {"1.5", "6.7"},
            {"2.5", "8.999"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //check import/export of time types
        
        st.executeUpdate(
            "CREATE TABLE   TTYPES(DATETYPE DATE, TIMETYPE TIME, "
            + "TSTAMPTYPE TIMESTAMP )");
        
        st.executeUpdate(
            " insert into ttypes values('1999-09-09' , "
            + "'12:15:19' , '1999-09-09 11:11:11')");
        
        st.executeUpdate(
            " insert into ttypes values('2999-12-01' , "
            + "'13:16:10' , '2999-09-09 11:12:11')");
        
        st.executeUpdate(
            " insert into ttypes values('3000-11-02' , "
            + "'14:17:21' , '4999-09-09 11:13:11')");
        
        st.executeUpdate(
            " insert into ttypes values('2004-04-03' , "
            + "'15:18:31' , '2004-09-09 11:14:11')");
        
        st.executeUpdate(
            " insert into ttypes values(null , null , null)");
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/ttypes.del");

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'TTYPES' "
            + ", 'extinout/ttypes.del' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'TTYPES' "
            + ", 'extinout/ttypes.del' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from ttypes");
        
        expColNames = new String [] {"DATETYPE", "TIMETYPE", "TSTAMPTYPE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1999-09-09", "12:15:19", "1999-09-09 11:11:11.0"},
            {"2999-12-01", "13:16:10", "2999-09-09 11:12:11.0"},
            {"3000-11-02", "14:17:21", "4999-09-09 11:13:11.0"},
            {"2004-04-03", "15:18:31", "2004-09-09 11:14:11.0"},
            {null, null, null},
            {"1999-09-09", "12:15:19", "1999-09-09 11:11:11.0"},
            {"2999-12-01", "13:16:10", "2999-09-09 11:12:11.0"},
            {"3000-11-02", "14:17:21", "4999-09-09 11:13:11.0"},
            {"2004-04-03", "15:18:31", "2004-09-09 11:14:11.0"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //-Import should commit on success and rollback on any 
        // failures
        
        conn.setAutoCommit(false);
        
        st.executeUpdate(
            " create table t1(a int ) ");
        
        st.executeUpdate(
            " insert into t1 values(1) ");
        
        st.executeUpdate(
            " insert into t1 values(2) ");
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.del");

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'T1' , "
            + "'extinout/t1.del' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T1' , "
            + "'extinout/t1.del' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        //above import should have committed , following rollback 
        // should be a noop.
        
        conn.rollback();
        
        rs = st.executeQuery(
            " select * from t1");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into t1 values(3) ");
        
        st.executeUpdate(
            " insert into t1 values(4) ");
        
        //file not found error should rollback
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T1' , "
            + "'extinout/nofile.del' , null, null, null, 0) ");
        assertStatementError("38000", cSt);
        
        conn.commit();
        rs = st.executeQuery(
            " select * from t1 ");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into t1 values(3) ");
        
        st.executeUpdate(
            " insert into t1 values(4) ");
        
        //table not found error should issue a implicit rollback
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'NOTABLE' "
            + ", 'extinout/t1.del' , null, null, null, 0) ");
        assertStatementError("XIE0M", cSt);
        
        conn.commit();
        rs = st.executeQuery(
            " select * from t1 ");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 4,
            " delete from t1");
        
        //-check commit/rollback with replace options using
        
        st.executeUpdate(
            "insert into t1 values(1) ");
        
        st.executeUpdate(
            " insert into t1 values(2) ");
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.del");

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'T1' , "
            + "'extinout/t1.del' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        //above export should have a commit.rollback below should 
        // be a noop
        
        conn.rollback();
        
        rs = st.executeQuery(
            " select * from t1");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T1' , "
            + "'extinout/t1.del' , null, null, null, 1) ");
        assertUpdateCount(cSt, 0);
        
        //above import should have committed , following rollback 
        // should be a noop.
        
        conn.rollback();
        
        rs = st.executeQuery(
            " select * from t1");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into t1 values(3) ");
        
        st.executeUpdate(
            " insert into t1 values(4) ");
        
        //file not found error should rollback
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T1' , "
            + "'extinout/nofile.del' , null, null, null, 1) ");
        assertStatementError("38000", cSt);
        
        conn.commit();
        rs = st.executeQuery(
            " select * from t1 ");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into t1 values(3) ");
        
        st.executeUpdate(
            " insert into t1 values(4) ");
        
        //table not found error should issue a implicit rollback
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'NOTABLE' "
            + ", 'extinout/t1.del' , null, null, null, 1) ");
        assertStatementError("XIE0M", cSt);
        
        conn.commit();
        //-check IMPORT_DATA calls commit/rollback
        
        rs = st.executeQuery(
            "select * from t1 ");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 2,
            " delete from t1");
        
        //-check commit/rollback with replace options using
        
        st.executeUpdate(
            "insert into t1 values(1) ");
        
        st.executeUpdate(
            " insert into t1 values(2) ");
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.del");

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'T1' , "
            + "'extinout/t1.del' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'T1' , 'A' "
            + ", '1' , 'extinout/t1.del' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        //above import should have committed , following rollback 
        // should be a noop.
        
        conn.rollback();
        
        rs = st.executeQuery(
            " select * from t1");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into t1 values(3) ");
        
        st.executeUpdate(
            " insert into t1 values(4) ");
        
        //file not found error should rollback
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'T1', 'A' , "
            + "'1'  , 'extinout/nofile.del' , null, null, null, 0) ");
        assertStatementError("38000", cSt);
        
        conn.commit();
        rs = st.executeQuery(
            " select * from t1 ");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into t1 values(3) ");
        
        st.executeUpdate(
            " insert into t1 values(4) ");
        
        //table not found error should issue a implicit rollback
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'NOTABLE' , "
            + "'A' , '1', 'extinout/t1.del' , null, null, null, 1) ");
        assertStatementError("XIE0M", cSt);
        
        conn.commit();
        rs = st.executeQuery(
            " select * from t1 ");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        conn.setAutoCommit(true);
        
        //make sure commit import code is ok in autcommit mode.
        
        st.executeUpdate(
            "insert into t1 values(3) ");
        
        st.executeUpdate(
            " insert into t1 values(4) ");
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'T1' , 'A' "
            + ", '1' , 'extinout/t1.del' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from t1 ");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into t1 values(5) ");
        
        st.executeUpdate(
            " insert into t1 values(6) ");
        
        //following import will back , but should not have any 
        // impact on inserts
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'T1', 'A' , "
            + "'1'  , 'extinout/nofile.del' , null, null, null, 0) ");
        assertStatementError("38000", cSt);
        
        rs = st.executeQuery(
            " select * from t1 ");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"1"},
            {"2"},
            {"5"},
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //END IMPORT COMMIT/ROLLBACK TESTSING-all types supported 
        // by Derby import/export
        
        st.executeUpdate(
            "create table alltypes(chartype char(20) , "
            + "biginttype bigint , datetype date , decimaltype "
            + "decimal(10,5) , doubletype double , inttype integer "
            + ", lvartype long varchar , realtype real , sminttype "
            + "smallint , timetype time , tstamptype timestamp , "
            + "vartype varchar(50))");
        
        st.executeUpdate(
            " insert into  alltypes values('chartype string' , "
            + "9223372036854775807, '1993-10-29' , 12345.54321, "
            + "10E307, 2147483647, 'long varchar testing', 10E3, "
            + "32767, '09.39.43', '2004-09-09 11:14:11', "
            + "'varchar testing')");
        
        st.executeUpdate(
            " insert into  alltypes values('chartype string' , "
            + "-9223372036854775808, '1993-10-29' , 0.0, -10E307, "
            + "-2147483647, 'long varchar testing', -10E3, 32767, "
            + "'09.39.43', '2004-09-09 11:14:11', "
            + "'varchar testing')");
        
        st.executeUpdate(
            " insert into  alltypes values('\"chartype\" string' "
            + ", 9223372036854775807, '1993-10-29' , -12345.54321, "
            + "10E307, 2147483647, 'long \"varchar\" testing', "
            + "10E3, 32767, '09.39.43', "
            + "'2004-09-09 11:14:11', '\"varchar\" testing')");
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/alltypes.del");

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, "
            + "'ALLTYPES' , 'extinout/alltypes.del' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, "
            + "'ALLTYPES' , 'extinout/alltypes.del' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from alltypes ");
        
        expColNames = new String [] {"CHARTYPE", "BIGINTTYPE", "DATETYPE", "DECIMALTYPE", "DOUBLETYPE", "INTTYPE", "LVARTYPE", "REALTYPE", "SMINTTYPE", "TIMETYPE", "TSTAMPTYPE", "VARTYPE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"chartype string", "9223372036854775807", "1993-10-29", "12345.54321", "1.0E308", "2147483647", "long varchar testing", "10000.0", "32767", "09:39:43", "2004-09-09 11:14:11.0", "varchar testing"},
            {"chartype string", "-9223372036854775808", "1993-10-29", "0.00000", "-1.0E308", "-2147483647", "long varchar testing", "-10000.0", "32767", "09:39:43", "2004-09-09 11:14:11.0", "varchar testing"},
            {"\"chartype\" string", "9223372036854775807", "1993-10-29", "-12345.54321", "1.0E308", "2147483647", "long \"varchar\" testing", "10000.0", "32767", "09:39:43", "2004-09-09 11:14:11.0", "\"varchar\" testing"},
            {"chartype string", "9223372036854775807", "1993-10-29", "12345.54321", "1.0E308", "2147483647", "long varchar testing", "10000.0", "32767", "09:39:43", "2004-09-09 11:14:11.0", "varchar testing"},
            {"chartype string", "-9223372036854775808", "1993-10-29", "0.00000", "-1.0E308", "-2147483647", "long varchar testing", "-10000.0", "32767", "09:39:43", "2004-09-09 11:14:11.0", "varchar testing"},
            {"\"chartype\" string", "9223372036854775807", "1993-10-29", "-12345.54321", "1.0E308", "2147483647", "long \"varchar\" testing", "10000.0", "32767", "09:39:43", "2004-09-09 11:14:11.0", "\"varchar\" testing"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 6,
            " delete from alltypes");
        
        //import should work with trigger enabled on append and 
        // should not work on replace
        
        st.executeUpdate(
            "create table test1(a char(20)) ");
        
        st.executeUpdate(
            " create trigger trig_import after INSERT on "
            + "alltypes referencing new as newrow for each  row "
            + "insert into test1 values(newrow.chartype)");
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, "
            + "'ALLTYPES' , 'extinout/alltypes.del' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select count(*) from alltypes ");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from test1");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"chartype string"},
            {"chartype string"},
            {"\"chartype\" string"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 3,
            " delete from alltypes");
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, "
            + "'ALLTYPES' , 'extinout/alltypes.del' , null, null, null, 1) ");
        assertStatementError("38000", cSt);
        
        rs = st.executeQuery(
            " select count(*) from alltypes");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " drop trigger trig_import");
        
        st.executeUpdate(
            " drop table test1");
        
        //test importing to identity columns
        
        st.executeUpdate(
            "create table table1(c1 char(30), c2 int generated "
            + "always as identity, c3 real, c4 char(1))");
        
        st.executeUpdate(
            " create table table2(c1 char(30), c2 int, c3 real, "
            + "c4 char(1))");
        
        st.executeUpdate(
            " insert into table2 values('Robert',100, 45.2, 'J')");
        
        st.executeUpdate(
            " insert into table2 values('Mike',101, 76.9, 'K')");
        
        st.executeUpdate(
            " insert into table2 values('Leo',102, 23.4, 'I')");
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/import.del");

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select "
            + "c1,c3,c4 from table2' , 'extinout/import.del' , "
            + "null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " CALL SYSCS_UTIL.SYSCS_IMPORT_DATA(NULL,'TABLE1', "
            + "'C1,C3,C4' , null, 'extinout/import.del',null, null,null,0)");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from table1");
        
        expColNames = new String [] {"C1", "C2", "C3", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"Robert", "1", "45.2", "J"},
            {"Mike", "2", "76.9", "K"},
            {"Leo", "3", "23.4", "I"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 3,
            " delete from table1");
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/import.del");
 
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE(null , 'TABLE2' "
            + ", 'extinout/import.del',  null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        //following import should fail becuase of inserting into 
        // identity column.
        
        cSt = prepareCall(
            "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE(NULL, 'TABLE1', "
            + "'extinout/import.del',null, null, null,1)");
        assertStatementError("38000", cSt);
        
        //following import should be succesful
        
        cSt = prepareCall(
            "CALL SYSCS_UTIL.SYSCS_IMPORT_DATA(NULL, 'TABLE1', "
            + "'C1,C3,C4' , '1,3,4', 'extinout/import.del',null, "
            + "null, null,1)");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from table1");
        
        expColNames = new String [] {"C1", "C2", "C3", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"Robert", "1", "45.2", "J"},
            {"Mike", "2", "76.9", "K"},
            {"Leo", "3", "23.4", "I"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 3,
            " update table2 set c2=null");
        
        //check null values import to identity columns should also 
        // fail
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/import.del");
 
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE(null , 'TABLE2' "
            + ", 'extinout/import.del' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE(NULL, 'TABLE1', "
            + "'extinout/import.del',null, null, null,1)");
        assertStatementError("38000", cSt);
        
        rs = st.executeQuery(
            " select * from table1");
        
        expColNames = new String [] {"C1", "C2", "C3", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"Robert", "1", "45.2", "J"},
            {"Mike", "2", "76.9", "K"},
            {"Leo", "3", "23.4", "I"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //check that replace fails when there dependents and 
        // replaced datadoes not violate foreign key constraints.
        
        st.executeUpdate(
            "create table parent(a int not null primary key)");
        
        st.executeUpdate(
            " insert into parent values (1) , (2) , (3) , (4) ");
        
        st.executeUpdate(
            " create table child(b int references parent(a))");
        
        st.executeUpdate(
            " insert into child values (1) , (2) , (3) , (4) ");
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/parent.del");

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select * from "
            + "parent where a < 3' , 'extinout/parent.del' , null, "
            + "null, null) ");
        assertUpdateCount(cSt, 0);
        
        //replace should fail because of dependent table
        
        cSt = prepareCall(
            "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE(NULL, 'PARENT', "
            + "'extinout/parent.del',null, null, null,1)");
        assertStatementError("XIE0R", cSt);
        
        rs = st.executeQuery(
            " select * from parent");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //-test with a file which has a differen records 
        // seperators (\n, \r , \r\n)
        
        st.executeUpdate(
            "create table nt1( a int , b char(30))");
        
        cSt = prepareCall(
            " CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE(NULL, 'NT1', "
            + "'extin/mixednl.del',null, null, 'UTF-8',0)");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from nt1");
        
        expColNames = new String [] {"A", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "XXXXXX0"},
            {"1", "XXXXXX1"},
            {"2", "XXXXXX2"},
            {"3", "XXXXXX3"},
            {"4", "XXXXXX4"},
            {"5", "YYYYY5"},
            {"6", "YYYYY6"},
            {"7", "YYYYY7"},
            {"8", "YYYYY8"},
            {"9", "YYYYY9"},
            {"10", "ZZZZZZ10"},
            {"11", "ZZZZZZ11"},
            {"12", "ZZZZZZ12"},
            {"13", "ZZZZZZ13"},
            {"14", "ZZZZZZ14"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " drop table nt1 ");
        
        //test case for bug 5977;(with lot of text data)
        
        st.executeUpdate(
            "create table position_info ( position_code "
            + "varchar(10) not null , literal_no int not null , "
            + "job_category_code varchar(10), summary_description "
            + "long varchar, detail_description long varchar, "
            + "web_flag varchar(1) )");
        
        cSt = prepareCall(
            " CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE ('APP', "
            + "'POSITION_INFO', 'extin/position_info.del', null, "
            + "null, 'US-ASCII', 1)");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select count(*) from position_info ");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"680"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select detail_description from position_info where "
            + "position_code='AG1000'");
        
        expColNames = new String [] {"DETAIL_DESCRIPTION"};
        JDBC.assertColumnNames(rs, expColNames);
        
        rs.next();
        String expected = rs.getString(1);
        assertTrue(expected.startsWith("Essential Duties and Responsibilities (include"));
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/pinfo.del");

        cSt = prepareCall(
            " CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE ('APP', "
            + "'POSITION_INFO', 'extinout/pinfo.del', null, null, null)");
        assertUpdateCount(cSt, 0);
        
        assertUpdateCount(st, 680,
            " delete from position_info");
        
        cSt = prepareCall(
            " CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE ('APP', "
            + "'POSITION_INFO', 'extinout/pinfo.del', null, null, null, 1)");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select count(*) from position_info ");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"680"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select detail_description from position_info where "
            + "position_code='AG1000'");
        
        expColNames = new String [] {"DETAIL_DESCRIPTION"};
        JDBC.assertColumnNames(rs, expColNames);
        
        rs.next();
        expected = rs.getString(1);
        assertTrue(expected.startsWith("Essential Duties and Responsibilities (include"));
      
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/autoinc.dat");
	
        //test for autoincrement values
        
        cSt = prepareCall(
            "CALL "
            + "SYSCS_UTIL.SYSCS_EXPORT_QUERY('values(1),(2),(3)','e"
            + "xtinout/autoinc.dat',null,null,null)");
        assertUpdateCount(cSt, 0);
        
        st.executeUpdate(
            " create table dest_always(i int generated always as "
            + "identity)");
        
        st.executeUpdate(
            " create table dest_by_default(i int generated by "
            + "default as identity)");
        
        cSt = prepareCall(
            " CALL "
            + "SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','DEST_ALWAYS','e"
            + "xtinout/autoinc.dat',null,null,null,0)");
        assertStatementError("38000", cSt);
        
        rs = st.executeQuery(
            " select * from dest_always");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        cSt = prepareCall(
            " CALL "
            + "SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','DEST_BY_DEFAULT"
            + "','extinout/autoinc.dat',null,null,null,0)");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from dest_by_default");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " drop table dest_always");
        
        st.executeUpdate(
            " drop table dest_by_default");
        
        st.executeUpdate(
            " create table dest_always(i int generated always as "
            + "identity)");
        
        st.executeUpdate(
            " create table dest_by_default(i int generated by "
            + "default as identity)");
        
        cSt = prepareCall(
            " CALL "
            + "SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','DEST_ALWAYS','e"
            + "xtinout/autoinc.dat',null,null,null,1)");
        assertStatementError("38000", cSt);
        
        rs = st.executeQuery(
            " select * from dest_always");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        cSt = prepareCall(
            " CALL "
            + "SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','DEST_BY_DEFAULT"
            + "','extinout/autoinc.dat',null,null,null,1)");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from dest_by_default");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " drop table dest_always");
        
        st.executeUpdate(
            " drop table dest_by_default");
        
        //test case for bug (DERBY-390)test import/export with 
        // reserved words as table Name, column Names ..etc.
        
        st.executeUpdate(
            "create schema \"Group\"");
        
        st.executeUpdate(
            " create table \"Group\".\"Order\"(\"select\" int, "
            + "\"delete\" int, itemName char(20)) ");
        
        st.executeUpdate(
            " insert into \"Group\".\"Order\" values(1, 2, 'memory') ");
        
        st.executeUpdate(
            " insert into \"Group\".\"Order\" values(3, 4, 'disk') ");
        
        st.executeUpdate(
            " insert into \"Group\".\"Order\" values(5, 6, 'mouse') ");
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/order.dat");
 
        //following export should fail because schema name is not 
        // matching the way it is defined using delimited quotes.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('GROUP', "
            + "'Order' , 'extinout/order.dat', null, null, null) ");
        assertStatementError("38000", cSt);
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/order.dat");
 
        //following export should fail because table name is not 
        // matching the way it is defined in the quotes.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('Group', "
            + "'ORDER' , 'extinout/order.dat', null, null, null) ");
        assertStatementError("38000", cSt);
        
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/order.dat");

        //following export should fail because of unquoted table 
        // name that is a reserved word.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select * from "
            + "\"Group\".Order' , 'extinout/order.dat' ,    null , "
            + "null , null ) ");
        assertStatementError("38000", cSt);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/order.dat");
        
        //following exports should pass.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('Group', "
            + "'Order' , 'extinout/order.dat', null, null, null) ");
        assertUpdateCount(cSt, 0);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/order.dat");
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select * from "
            + "\"Group\".\"Order\"' , 'extinout/order.dat' ,    "
            + "null , null , null ) ");
        assertUpdateCount(cSt, 0);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/order.dat");
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select "
            + "\"select\" , \"delete\" , itemName from "
            + "\"Group\".\"Order\"' , 'extinout/order.dat' ,    "
            + "null , null , null ) ");
        assertUpdateCount(cSt, 0);

        //following import should fail because schema name is not 
        // matching the way it is defined using delimited quotes.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('GROUP', "
            + "'Order' , 'extinout/order.dat', null, null, null, 0) ");
        assertStatementError("XIE0M", cSt);
        
        //following import should fail because table name is not 
        // matching the way it is defined in the quotes.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('Group', "
            + "'ORDER' , 'extinout/order.dat', null, null, null, 0) ");
        assertStatementError("XIE0M", cSt);
        
        //following import should fail because table name is not 
        // matching the way it is defined in the quotes.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('Group', 'ORDER' "
            + ", null , null ,   'extinout/order.dat'   , null , "
            + "null , null, 1) ");
        assertStatementError("XIE0M", cSt);
        
        //following import should fail because column name is not 
        // matching the way it is defined in the quotes.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('Group', 'Order' "
            + ", 'DELETE, ITEMNAME' , '2, 3' ,   "
            + "'extinout/order.dat'   , null , null , null, 1) ");
        assertStatementError("XIE08", cSt);
        
        //following import should fail because undelimited column 
        // name is not in upper case.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('Group', 'Order' "
            + ", 'delete, itemName' , '2, 3' ,   "
            + "'extinout/order.dat'   , null , null , null, 1) ");
        assertStatementError("XIE08", cSt);
        
        //following imports should pass
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('Group', "
            + "'Order' , 'extinout/order.dat', null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
        " select * from \"Group\".\"Order\"");
        
        expColNames = new String [] {"select", "delete", "ITEMNAME"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2", "memory"},
            {"3", "4", "disk"},
            {"5", "6", "mouse"},
            {"1", "2", "memory"},
            {"3", "4", "disk"},
            {"5", "6", "mouse"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA('Group', 'Order' "
            + ", null , null ,   'extinout/order.dat'   , null , "
            + "null , null, 1) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from \"Group\".\"Order\"");
        
        expColNames = new String [] {"select", "delete", "ITEMNAME"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2", "memory"},
            {"3", "4", "disk"},
            {"5", "6", "mouse"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA('Group', 'Order' "
            + ", 'delete' , '2' ,   'extinout/order.dat'   , null "
            + ", null , null, 1) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from \"Group\".\"Order\"");
        
        expColNames = new String [] {"select", "delete", "ITEMNAME"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null, "2", null},
            {null, "4", null},
            {null, "6", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA('Group', 'Order' "
            + ", 'ITEMNAME, select, delete' , '3,2,1' ,   "
            + "'extinout/order.dat'   , null , null , null, 1) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from \"Group\".\"Order\"");
        
        expColNames = new String [] {"select", "delete", "ITEMNAME"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "1", "memory"},
            {"4", "3", "disk"},
            {"6", "5", "mouse"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
            
        //-test undelimited names( All unquoted SQL identfiers 
        // should be passed in upper case).
        
        st.executeUpdate(
            "create schema inventory");
        
        st.executeUpdate(
            " create table inventory.orderTable(id int, amount "
            + "int, itemName char(20)) ");
        
        st.executeUpdate(
            " insert into inventory.orderTable values(101, 5, 'pizza') ");
        
        st.executeUpdate(
            " insert into inventory.orderTable values(102, 6, 'coke') ");
        
        st.executeUpdate(
            " insert into inventory.orderTable values(103, 7, "
            + "'break sticks') ");
        
        st.executeUpdate(
            " insert into inventory.orderTable values(104, 8, "
            + "'buffolo wings') ");
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/order.dat");
 
        //following export should fail because schema name is not 
        // in upper case.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('inventory', "
            + "'ORDERTABLE' , 'extinout/order.dat', null, null, null) ");
        assertStatementError("38000", cSt);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/order.dat");
        
        //following export should fail because table name is not 
        // in upper case.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('INVENTORY', "
            + "'ordertable' , 'extinout/order.dat', null, null, null) ");
        assertStatementError("38000", cSt);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/order.dat");
        
        //following export should pass.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('INVENTORY', "
            + "'ORDERTABLE' , 'extinout/order.dat', null, null, null) ");
        assertUpdateCount(cSt, 0);

        //following import should fail because schema name is not 
        // in upper case
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('inventory', "
            + "'ORDERTABLE' , 'extinout/order.dat', null, null, null, 0) ");
        assertStatementError("XIE0M", cSt);
        
        //following import should fail because table name is not 
        // in upper case.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('INVENTORY', "
            + "'ordertable' , 'extinout/order.dat', null, null, null, 0) ");
        assertStatementError("XIE0M", cSt);
        
        //following import should fail because table name is not 
        // in upper case .
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('INVENTORY', "
            + "'ordertable' , null , null ,   'extinout/order.dat' "
            + "  , null , null , null, 1) ");
        assertStatementError("XIE0M", cSt);
        
        //following import should fail because column name is not 
        // in upper case.
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('INVENTORY', "
            + "'ORDERTABLE' , 'amount, ITEMNAME' , '2, 3' ,   "
            + "'extinout/order.dat'   , null , null , null, 1) ");
        assertStatementError("XIE08", cSt);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA('INVENTORY', "
            + "'ORDERTABLE' , null , null ,   'extinout/order.dat' "
            + "  , null , null , null, 1) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from inventory.orderTable");
        
        expColNames = new String [] {"ID", "AMOUNT", "ITEMNAME"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"101", "5", "pizza"},
            {"102", "6", "coke"},
            {"103", "7", "break sticks"},
            {"104", "8", "buffolo wings"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA('INVENTORY', "
            + "'ORDERTABLE' , 'ITEMNAME, ID, AMOUNT' , '3,2,1' ,   "
            + "'extinout/order.dat'   , null , null , null, 1) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from inventory.orderTable");
        
        expColNames = new String [] {"ID", "AMOUNT", "ITEMNAME"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5", "101", "pizza"},
            {"6", "102", "coke"},
            {"7", "103", "break sticks"},
            {"8", "104", "buffolo wings"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " drop table inventory.orderTable");
        
        //end derby-390 related test cases.
  
        getConnection().rollback();
        st.close();
    }

    /* 
    *tests for Derby-4555
    */
    public void test4555ColumnIndexesParsing() throws Exception{	

  	CallableStatement cSt;
        Statement st = createStatement();

	st.executeUpdate(" create table pet(C1 varchar(50), C2 varchar(50) , C3 varchar(50))");

	st.executeUpdate("insert into pet values('Pet', 'Kind' , 'Age')");

	st.executeUpdate("insert into pet values('Name', 'of' , null)");

	st.executeUpdate("insert into pet values(null, 'Animal' , null)");

	st.executeUpdate("insert into pet values('Rover', 'Dog' , '4')");

	st.executeUpdate("insert into pet values('Spot', 'Cat' , '2')");
	
	st.executeUpdate("insert into pet values('Squawky','Parrot','37')");
	
	st.executeUpdate(" create table pet1(C1 varchar(50), C2 varchar(50) , C3 int NOT NULL)");
	
	SupportFilesSetup.deleteFile("extinout/pet.dat");
	st.executeUpdate("call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, "
            + "'PET' , 'extinout/pet.dat', null, null, null) ");
        
	st.executeUpdate("delete from pet");
	
	// With both indexes and names of the columns for COLUMNINDEXES argument
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,2,\"Age\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");
        assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from pet"),
                        new String[][]
                        {
			      {"Rover","Dog","4"},
                              { "Spot", "Cat", "2"},
                              { "Squawky", "Parrot", "37"},
                        });

	st.executeUpdate("delete from pet");

	//Only with the names of the columns and multiline headernames for COLUMNINDEXES argument
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '\"Pet Name\",\"Kind of Animal\",\"Age\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");
        assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from pet"),
                        new String[][]
                        {
			      {"Rover","Dog","4"},
                              { "Spot", "Cat", "2"},
                              { "Squawky", "Parrot", "37"},
                        });

	st.executeUpdate("delete from pet");

	//Changing the order of the header names.	
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , 'C3,C1,C2' , '\"Age\",1,2' ,   'extinout/pet.dat' "
            + "  , null , null ,null , 0, 3) ");
        assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from Pet"),
                        new String[][]
                        {
			      {"Rover","Dog","4"},
                              { "Spot", "Cat", "2"},
                              { "Squawky", "Parrot", "37"},
                        });
	st.executeUpdate("delete from pet");

	//invalid column name 
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '\"Pet\",2,3' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");

	assertStatementError("42XAU", cSt);
	
	//Skip argument is 0 and non-existent input file
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '\"Pet Name\",2,3' ,   'extinout/petlist.dat' "
            + "  , null , null , null, 0, 0) ");

	assertStatementError("42XAV", cSt);

	//Skip argument is 2 and non-existent input file
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '\"Pet Name\",2,3' ,   'extinout/petlist.dat' "
            + "  , null , null , null, 0, 2) ");

	assertStatementError("XIE04", cSt);

	//Skip argument is 0
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,2,\"Age\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 0) ");

	assertStatementError("42XAV", cSt);

	//Invalid number of header lines of the input file causes NULL value error
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET1' , null , '1,2,\"Age\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 1) ");

	assertStatementError("XIE0R", cSt);

	//Invalid number of header lines of the input file causes NULL value error
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET1' , null , '\"Pet\"\"Kind\"\"Age\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 1) ");

	assertStatementError("42XAU", cSt);

	//Skip argument is 7 that is greater than number of rows in the file.
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,2,\"Age\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 7) ");

	assertStatementError("42XAU", cSt);

	//Column name in capital letters.
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,2,\"AGE\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");
        
	assertStatementError("42XAU", cSt);
	
	//Multiple header lines in the input file
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,\"Kind of Animal\",\"Age\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");
        assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from pet"),
                        new String[][]
                        {
			      {"Rover","Dog","4"},
                              { "Spot", "Cat", "2"},
                              { "Squawky", "Parrot", "37"},
                        });

	st.executeUpdate("delete from pet");

	//Invalid ' " ' marks
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '\"Pet\"\"Name\",\"Kind\"\"of\"\"Animal\",\"Age\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");
        

	assertStatementError("42XAU", cSt);

	//Invalid number of header lines of the input file causes NULL value error
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET1' , null , '\"Pet Name\",\"Kind of\",\"Age\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 2) ");

        try {
            cSt.execute();
        }
        catch ( SQLException se )
        {
            assertSQLState( "XIE0R", se );
            assertTrue("not column C3:"+se.getMessage(),
                       se.getMessage().indexOf("C3") >= 0);
        }

        //Skip=4
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '\"Pet Name Rover\",2,3' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 4) ");
        assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from pet"),
                        new String[][]
                        {
			      { "Spot", "Cat", "2"},
                              { "Squawky", "Parrot", "37"},
                        });

	st.executeUpdate("delete from pet");

	//Skip=4
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,2,\"Age 4\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 4) ");
        assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from pet"),
                        new String[][]
                        {
			      { "Spot", "Cat", "2"},
                              { "Squawky", "Parrot", "37"},
                        });

	st.executeUpdate("delete from pet");

	//Skip=4
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,\"Kind of Animal Dog\",3' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 4) ");
        assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from pet"),
                        new String[][]
                        {
			      { "Spot", "Cat", "2"},
                              { "Squawky", "Parrot", "37"},
                        });

	st.executeUpdate("delete from pet");

	//The number of values in COLUMNINDEXES and INSERTCOLUMNS does not match
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,2' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");

	assertStatementError("42802", cSt);

	//The number of values in COLUMNINDEXES and INSERTCOLUMNS does not match
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,\"Kind of Animal\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");

	assertStatementError("42802", cSt);

	//The number of values in COLUMNINDEXES and INSERTCOLUMNS does not match
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,\"Age\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");

	assertStatementError("42802", cSt);

	//Skip argument is -2
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,2,3' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, -2) ");

	assertStatementError("42XAV", cSt);

	//Skip argument is -1
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , null , '1,2,3' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, -1) ");

	assertStatementError("42XAV", cSt);
	
	//Insert only two columns
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , 'C1,C2' , '1,2' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");
	
	assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select C1,C2 from pet"),
                        new String[][]
                        {
			      {"Rover","Dog"},
                              { "Spot", "Cat"},
                              { "Squawky", "Parrot"},
                        });

	st.executeUpdate("delete from pet");
	
	//Wrong values for INSERTCOLUMNS argument causes NULL value error  
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET1' , 'C1,C2' , '1,3' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");
	
	assertStatementError("XIE0R", cSt);

	//Insert only two columns, order is different
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , 'C1,C3' , '1,3' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");
	
	assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select C1,C3 from pet"),
                        new String[][]
                        {
			      {"Rover","4"},
                              { "Spot", "2"},
                              { "Squawky", "37"},
                        });

	st.executeUpdate("delete from pet");

	//Insert only two columns, order is different, with column names
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_BULK(null, "
            + "'PET' , 'C1,C2' , '\"Pet Name\",\"Kind of Animal\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0, 3) ");
	
	assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select C1,C2 from pet"),
                        new String[][]
                        {
			      {"Rover","Dog"},
                              { "Spot", "Cat"},
                              { "Squawky", "Parrot"},
                        });

	st.executeUpdate("delete from pet");

	/*
         *Tests for SYSCS_IMPORT_TABLE_BULK procedure
	 */
	//Skip=0 and non-existent input file
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE_BULK(null, 'PET', 'extinout/petlist.dat', null, null, null, 0, 0) ");
	
	assertStatementError("38000", cSt);
	
	
	//Skip=2 and non-existent input file
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE_BULK(null, 'PET', 'extinout/petlist.dat', null, null, null, 0, 2) ");
	
	assertStatementError("XJ001", cSt);
	
	//Skip=-1
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE_BULK(null, 'PET', 'extinout/pet.dat', null, null, null, 0, -1) ");
	
	assertStatementError("42XAV", cSt);
	
	//Skip argument is 7 that is greater than number of rows in the file.
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE_BULK(null, 'PET', 'extinout/pet.dat', null, null, null, 0, 7) ");

        try {
            cSt.execute();
        }
        catch ( SQLException se )
        {
            // Check line number in message:
            assertSQLState( "XIE0E", se );
            assertTrue("not line 7:"+se.getMessage(),
                       se.getMessage().indexOf("7") >= 0);
        }

	//End of tests for SYSCS_IMPORT_TABLE_BULK procedure

	/**
         *Tests for SYSCS_IMPORT_DATA procedure
	 */
	
	//COLUMNINDEXES 1,2,"Age". This should fail because no lines are skipped by this procedure
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, "
            + "'PET1' , null , '1,2,\"AGE\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0) ");

	assertStatementError("42XAV", cSt);

	//COLUMNINDEXES 1,2,Age. This should fail because no ' " ' in the column name.
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, "
            + "'PET1' , null , '1,2,AGE' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0) ");
	
	assertStatementError("38000", cSt);

	//COLUMNINDEXES 1,2,4. This should fail because no column 4 in the column name.
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, "
            + "'PET1' , null , '1,2,4' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0) ");

	assertStatementError("38000", cSt);

	//File name is wrong.
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, "
            + "'PET' , null , '1,2,3' ,   'extinout/petlist.dat' "
            + "  , null , null , null, 0) ");
        

	assertStatementError("38000", cSt);

	//End of tests for SYSCS_IMPORT_DATA procedure

	/**
         *Tests for SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE procedure
	 */

	//COLUMNINDEXES 1,2,"Age". This should fail because no lines are skipped by this procedure
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE(null, "
            + "'PET1' , null , '1,2,\"AGE\"' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0) ");

	assertStatementError("42XAV", cSt);

	//COLUMNINDEXES 1,2,Age. This should fail because no ' " ' in the column name.
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE(null, "
            + "'PET1' , null , '1,2,AGE' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0) ");
	
	assertStatementError("38000", cSt);

	//COLUMNINDEXES 1,2,4. This should fail because no column 4 in the column name.
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE(null, "
            + "'PET1' , null , '1,2,4' ,   'extinout/pet.dat' "
            + "  , null , null , null, 0) ");

	assertStatementError("38000", cSt);

	//File name is wrong.
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE(null, "
            + "'PET' , null , '1,2,3' ,   'extinout/petlist.dat' "
            + "  , null , null , null, 0) ");
        

	assertStatementError("38000", cSt);

	//End of tests for SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE procedure


	/**
         *Tests for SYSCS_IMPORT_TABLE procedure
	 */	
	//Non-existent input file
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE(null, 'PET', 'extinout/petlist.dat', null, null, null, 0) ");
	
	assertStatementError("38000", cSt);

	st.executeUpdate(" create table pet2(C1 varchar(50), C2 varchar(50))");
	
	st.executeUpdate("insert into pet2 values('Rover', 'Dog')");

	st.executeUpdate("insert into pet2 values('Spot', 'Cat')");
	
	st.executeUpdate("insert into pet2 values('Squawky','Parrot')");

	SupportFilesSetup.deleteFile("extinout/pet2.dat");
	st.executeUpdate("call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, "
            + "'PET2' , 'extinout/pet2.dat', null, null, null) ");

	st.executeUpdate("delete from pet2");

	//Table with only two columns
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE(null, 'PET2', 'extinout/pet2.dat', null, null, null, 0) ");

	assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from pet2"),
                        new String[][]
                        {
			      {"Rover","Dog"},
                              { "Spot", "Cat"},
                              { "Squawky", "Parrot"},
                        });

	st.executeUpdate("delete from pet2");

	st.executeUpdate(" create table pet3(C1 varchar(50),C2 int, C3 varchar(50))");
	
	st.executeUpdate("insert into pet3 values('Rover', 4, 'Dog')");

	st.executeUpdate("insert into pet3 values('Spot', 2,'Cat')");
	
	st.executeUpdate("insert into pet3 values('Squawky',37,'Parrot')");

	SupportFilesSetup.deleteFile("extinout/pet3.dat");
	st.executeUpdate("call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, "
            + "'PET3' , 'extinout/pet3.dat', null, null, null) ");

	st.executeUpdate("delete from pet3");
	
	
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE(null, 'PET3', 'extinout/pet3.dat', null, null, null, 0) ");

	assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from pet3"),
                        new String[][]
                        {
			      {"Rover","4","Dog"},
                              { "Spot","2", "Cat"},
                              { "Squawky", "37","Parrot"},
                        });
	st.executeUpdate("delete from pet3");
	//End of tests for SYSCS_IMPORT_TABLE procedure


	/**
         *Tests for SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE procedure
	 */	
	//Non-existent input file
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE(null, 'PET', 'extinout/petlist.dat', null, null, null, 0) ");
	
	assertStatementError("38000", cSt);

	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE(null, 'PET2', 'extinout/pet2.dat', null, null, null, 0) ");

	assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from pet2"),
                        new String[][]
                        {
			      {"Rover","Dog"},
                              { "Spot", "Cat"},
                              { "Squawky", "Parrot"},
                        });


	st.executeUpdate("delete from pet2");
	cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE(null, 'PET3', 'extinout/pet3.dat', null, null, null, 0) ");

	assertUpdateCount(cSt, 0);

	JDBC.assertFullResultSet(
                	st.executeQuery("select * from pet3"),
                        new String[][]
                        {
			      {"Rover","4","Dog"},
                              { "Spot","2", "Cat"},
                              { "Squawky", "37","Parrot"},
                        });
	st.executeUpdate("delete from pet3");
	//End of tests for SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE procedure

	
    }
    
    /**
     * Converted from iepnegativetests.sql
     */
    public void testImportExportProcedureNegative() throws Exception
    {
        ResultSet rs = null;

        CallableStatement cSt;
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;

        st.executeUpdate(
            "create schema iep");
        
        st.executeUpdate(
            " create table iep.t1(a int)");
        
        st.executeUpdate(
            " insert into iep.t1 values(100) , (101) , (102) , "
            + "(103) , (104) , (105) , (106)");

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extout/nodir/t1.dat");
        
        //export error casesexport can not create file
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T1' , "
            + "'extout/nodir/t1.dat' , null, null, null) ");
        assertStatementError("XIE0I", cSt);
	

        //export table not found
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', "
            + "'NOTABLE' , 'extinout/t1.dat' , null, null, null) ");
        assertStatementError("38000", cSt);
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
 
        //-export schema is not valid
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('XXXX', 'T1' , "
            + "'extinout/t1.dat' , null, null, null) ");
        assertStatementError("38000", cSt);
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
 
        //export query is invalid (syntax error)
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select from "
            + "t1', 'extinout/t1.dat' , null, null, null) ");
        assertStatementError("38000", cSt);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
        
        //export codeset is invalid
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select * from "
            + "iep.t1', 'extinout/t1.dat' , null, null, 'NOSUCHCODESET') ");
        assertStatementError("XIE0I", cSt);
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
 
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('XXXX', 'T1' , "
            + "'extinout/t1.dat' , null, null, null) ");
        assertStatementError("38000", cSt);
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
 
        //export delimiter errror casesperiod can not be used as 
        // character ot column delimiter
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , null, '.', null) ");
        assertStatementError("XIE0K", cSt);
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
 
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , '.', null, null) ");
        assertStatementError("XIE0J", cSt);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
        
        //same delimter can not be used as character and column 
        // delimters
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , ';', ';', null) ");
        assertStatementError("XIE0J", cSt);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
        
        //space character can not be a delimiter
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , ' ', ';', null) ");
        assertStatementError("XIE0J", cSt);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , null, ' ', null) ");
        assertStatementError("XIE0J", cSt);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
        
        //if emtry strinng is passed actual value delimiter should 
        // be spaceand the that should become a invalid delimiter
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , '', ';', null) ");
        assertStatementError("XIE0J", cSt);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , null, '', null) ");
        assertStatementError("XIE0J", cSt);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
        
        //more than one character passed to the delimiters get 
        // truncated to onefollowing one should give error because 
        // eventually '\' delimiteris used a both for char and col
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , '\\', '\\', null) ");
        assertStatementError("XIE0J", cSt);
                
        //DO A VALID EXPORT AND  IMPORT
        
        st.executeUpdate(
            "set schema iep");

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t1.dat");
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , null, null, 'utf-8') ");
        assertUpdateCount(cSt, 0);
        
        assertUpdateCount(st, 7,
            " delete from t1 ");

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE('IEP', 'T1' , "
            + "'extinout/t1.dat' , null, null, 'utf-8', 0) ");
        assertUpdateCount(cSt, 0);
        
        //  DERBY-2925: need to delete existing files 
        SupportFilesSetup.deleteFile("extinout/t1.dat");
        rs = st.executeQuery(
            " select * from t1");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"100"},
            {"101"},
            {"102"},
            {"103"},
            {"104"},
            {"105"},
            {"106"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extin/nodir/t1.dat");
        
        //import error casesimport can not find input file
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE('IEP', 'T1' , "
            + "'extin/nodir/t1.dat' , null, null, null, 0) ");
        assertStatementError("38000", cSt);
       
        //import table not found
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('IEP', "
            + "'NOTABLE' , 'extinout/t1.dat' , null, null, null, 0) ");
        assertStatementError("XIE0M", cSt);
       
        //import schema is not valid
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('XXXX', 'T1' , "
            + "'extinout/t1.dat' , null, null, null, 0) ");
        assertStatementError("XIE0M", cSt);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , null, null, 'INCORRECTCODESET', 0) ");
        assertStatementError("38000", cSt);
        
        //check import with invalid delimiter usageif emtry 
        // strinng is passed actual value delimiter should be 
        // spaceand the that should become a invalid delimiter
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , '', ';', null, 0) ");
        assertStatementError(INVALID_DELIMITER, cSt);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , null, '', null, 0) ");
        assertStatementError(INVALID_DELIMITER, cSt);
        
        //same delimter can not be used as character and column 
        // delimters
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('IEP', 'T1' , "
            + "'extinout/t1.dat' , ';', ';', null, 1) ");
        assertStatementError(INVALID_DELIMITER, cSt);
        
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        
        st.executeUpdate(
            " create table v1(a int) ");
        
        assertUpdateCount(st, 0,
            " declare global temporary table session.temp1(c1 "
            + "int) on commit preserve rows not logged");
        
        st.executeUpdate(
            " insert into session.temp1 values(1) , (2) , (3) , "
            + "(4) , (5) , (6)");
        
        rs = st.executeQuery(
            " select * from session.temp1");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"5"},
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/temp1.dat");
 
        //export to from a temporary table
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('SESSION', "
            + "'TEMP1' , 'extinout/temp1.dat' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        // because temporary table has on commit preserve rows, 
        // commit issued by export will not delete data from the 
        // temp table.
        
        rs = st.executeQuery(
            "select * from session.temp1");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"5"},
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //import back to a regualr table
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('IEP', 'V1' , "
            + "'extinout/temp1.dat' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from v1");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"5"},
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        conn.commit();
        //import to a temp table should fail with a table not 
        // found errror
        
        assertUpdateCount(st, 0,
            "declare global temporary table session.temp2(c1 "
            + "int) not logged");
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('SESSION', "
            + "'TEMP2' , 'extinout/temp1.dat' , null, null, null, 0) ");
        assertStatementError("XIE0M", cSt);
        
        assertStatementError("42X05", st,
            " select * from session.temp2 ");
        
        conn.commit();
        st.executeUpdate(
            " drop table v1");
        
        conn.setAutoCommit(true);
        
        st.executeUpdate(
            " create table t3(c1 int , c2 double , c3 decimal , "
            + "c4 varchar(20) )");
        
        st.executeUpdate(
            " insert into t3 values(1 , 3.5 , 8.6 , 'test strings')");
        
        st.executeUpdate(
            " insert into t3 values(2 , 3.5 , 8.6 , 'test strings')");
        
        st.executeUpdate(
            " insert into t3 values(3 , 3.5 , 8.6 , 'test strings')");
        
        st.executeUpdate(
            " insert into t3 values(4 , 3.5 , 8.6 , 'test strings')");

	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t3.dat");

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T3' , "
            + "'extinout/t3.dat' , null, null, null) ");
        assertUpdateCount(cSt, 0);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('IEP', 'T3' , "
            + "'extinout/t3.dat' , null, null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        rs = st.executeQuery(
            " select * from t3");
        
        expColNames = new String [] {"C1", "C2", "C3", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "3.5", "8", "test strings"},
            {"2", "3.5", "8", "test strings"},
            {"3", "3.5", "8", "test strings"},
            {"4", "3.5", "8", "test strings"},
            {"1", "3.5", "8", "test strings"},
            {"2", "3.5", "8", "test strings"},
            {"3", "3.5", "8", "test strings"},
            {"4", "3.5", "8", "test strings"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //import data column names are incorrect
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('IEP', 'T3' , "
            + "'X1, X2, X3, X4', null, 'extinout/t3.dat' , null, "
            + "null, null, 0) ");
        assertStatementError("XIE08", cSt);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA('IEP', 'T3' , "
            + "'X1, X2, X3', '1,2,3,4', 'extinout/t3.dat' , null, "
            + "null, null, 0) ");
        assertStatementError("XIE08", cSt);
        
        //import data insert column names count < column indexes 
        // does not match
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('IEP', 'T3' , "
            + "'C1, C2, C3', '1,2,3,4', 'extinout/t3.dat' , null, "
            + "null, null, 0) ");
        assertUpdateCount(cSt, 0);
        
        //import data column indexes count > insert columns count
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('IEP', 'T3' , "
            + "'C1, C2, C3,C4', '1,2', 'extinout/t3.dat' , null, "
            + "null, null, 0) ");
        assertStatementError("38000", cSt);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA('IEP', 'T3' , "
            + "null, '11,22,12,24', 'extinout/t3.dat' , null, "
            + "null, null, 0) ");
        assertStatementError("38000", cSt);
        
        //repeat the above type cases with empty file and minor 
        // variation to paramters
        
        assertUpdateCount(st, 12,
            "delete from t3 ");
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/t3.dat");
 
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_TABLE ('IEP', 'T3' , "
            + "'extinout/t3.dat' , ';', '^', 'utf-16') ");
        assertUpdateCount(cSt, 0);
        
        //import data column names are incorrect
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('IEP', 'T3' , "
            + "'X1, X2, X3, X4', null, 'extinout/t3.dat' , ';', "
            + "'^', 'utf-16', 1) ");
        assertStatementError("XIE08", cSt);
        
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_IMPORT_DATA('IEP', 'T3' , "
            + "'X1, X2, X3', '1,2,3,4', 'extinout/t3.dat' , ';', "
            + "'^', 'utf-16', 1) ");
        assertStatementError("XIE08", cSt);
        
        //import data insert column names count < column indexes 
        // does not match
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('IEP', 'T3' , "
            + "'C1, C2, C3', null, 'extinout/t3.dat' , ';', '^', "
            + "'utf-16', 1) ");
        assertUpdateCount(cSt, 0);
        
        //import data column indexes count > insert columns count
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('IEP', 'T3' , "
            + "null, '1,2', 'extinout/t3.dat' , ';', '^', 'utf-16', 1) ");
        assertStatementError("38000", cSt);
        
        //specify column indexes that are not there in the file 
        // that is being  imported
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA('IEP', 'T3' , "
            + "null, '11,22,12,24', 'extinout/t3.dat' , ';', '^', "
            + "'utf-16', 1) ");
        assertUpdateCount(cSt, 0);
        
        //import to a system table shoud fail
        
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE ('SYS', "
            + "'SYSTABLES' , 'extinout/t3.dat' , ';', '^', 'utf-16', 1) ");
        assertStatementError("38000", cSt);
        
        //import should aquire a lock on the table
        
        st.executeUpdate(
            "create table parent(a int not null primary key)");
        
        st.executeUpdate(
            " insert into parent values (1) , (2) , (3) , (4) ");
       
	//DERBY-2925: need to delete existing files first.
        SupportFilesSetup.deleteFile("extinout/parent.del");
 
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select * from "
            + "parent where a < 3' , 'extinout/parent.del' , null, "
            + "null, null) ");
        assertUpdateCount(cSt, 0);
        
        Connection c1 = openDefaultConnection();
        Statement st_c1 = c1.createStatement();
        c1.setAutoCommit(false);
        st_c1.executeUpdate("lock table iep.parent in share mode");

        conn.setAutoCommit(false);
        
        cSt = prepareCall(
            "call "
            + "SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.locks."
            + "waitTimeout', '5')");
        assertUpdateCount(cSt, 0);
        
        assertStatementError("38000", st, "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('IEP', 'PARENT', 'extinout/parent.del',null, null, null,1)");

        c1.rollback();
        c1.close();
        conn.setAutoCommit(true);
        
        getConnection().rollback();
        st.close();
    }
}
