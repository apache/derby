/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.TriggerGeneralTest
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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import org.apache.derby.iapi.db.Factory;
import org.apache.derby.iapi.db.TriggerExecutionContext;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * General trigger test
 */
public final class TriggerGeneralTest extends BaseJDBCTestCase {

    /**
     * Pattern for a hexadecimal digit,
     * cf. http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html
     */
    public static final String HEX_DIGIT = "\\p{XDigit}";
    public static final String LANG_DUPLICATE_KEY_CONSTRAINT = "23505";
    public static final String LANG_SYNTAX_ERROR = "42X01";
    public static final String LANG_COLUMN_NOT_FOUND_IN_TABLE = "42X14";
    public static final String LANG_NO_USER_DDL_IN_SYSTEM_SCHEMA = "42X62";
    public static final String LANG_OBJECT_NOT_FOUND = "42X94";
    public static final String LANG_NO_PARAMS_IN_TRIGGER_ACTION = "42Y27";
    public static final String LANG_DUPLICATE_COLUMN_IN_TRIGGER_UPDATE = "42Y40";
    public static final String LANG_OBJECT_DOES_NOT_EXIST = "42Y55";
    public static final String LANG_INVALID_OPERATION_ON_VIEW = "42Y62";
    public static final String LANG_TRIGGER_RECURSION_EXCEEDED = "54038";
    public static final String LANG_PROVIDER_HAS_DEPENDENT_OBJECT = "X0Y25";
    public static final String LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT = "X0Y32";
    public static final String LANG_SCHEMA_NOT_EMPTY = "X0Y54";
    public static final String LANG_INVALID_OPERATION_ON_SYSTEM_TABLE = "X0Y56";

    private static PrintStream out;
    private static ByteArrayOutputStream outs;

    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name test fixture name
     */
    public TriggerGeneralTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        Test t = TestConfiguration.defaultSuite(TriggerGeneralTest.class);
        t = new SupportFilesSetup(t,
                   new String[] {
                   "functionTests/tests/lang/dcl_emc1.jar",
                   });
        return SecurityManagerSetup.noSecurityManager(t);
    }

    public void testTriggersInGeneral() throws Exception
    {
        ResultSet rs;
        ResultSetMetaData rsmd;

        final Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;
        outs = new ByteArrayOutputStream();
        out = new PrintStream(outs);

        st.executeUpdate("create function triggerFiresMin(s varchar(128)) "
            + "returns varchar(1) PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
            + "  EXTERNAL NAME "
            + "'org.apache.derbyTesting.functionTests.tests.lang."
            + "TriggerGeneralTest.triggerFiresMinimal'");

        st.executeUpdate("create function triggerFires(s varchar(128)) "
            + "returns varchar(1) PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
            + "  EXTERNAL NAME "
            + "'org.apache.derbyTesting.functionTests.tests.lang."
            + "TriggerGeneralTest.triggerFires'");

        assertStatementError(LANG_OBJECT_DOES_NOT_EXIST, st,
            "drop table x");

        st.executeUpdate("create table x (x int, y int, z int, constraint ck1 "
            + "check (x > 0))");

        st.executeUpdate("create view v as select * from x");

        // ok

        st.executeUpdate("create trigger t1 NO CASCADE before update of x,y "
            + "on x for each row values 1");

        // trigger already exists

        assertStatementError(LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT, st,
            "create trigger t1 NO CASCADE before update of x,y "
            + "on x for each row values 1");

        // trigger already exists

        assertStatementError(LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT, st,
            "create trigger app.t1 NO CASCADE before update of "
            + "x,y on x for each row values 1");

        // make sure system tables look as we expect

        rs = st.executeQuery("select cast(triggername as char(10)), event, "
            + "firingtime, type, state, referencedcolumns from "
            + "sys.systriggers");

        expColNames = new String [] {
            "1", "EVENT", "FIRINGTIME", "TYPE", "STATE", "REFERENCEDCOLUMNS"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"T1", "U", "B", "R", "E", "(1,2)"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select cast(triggername as char(10)), CAST "
            + "(TRIGGERDEFINITION AS VARCHAR(180)), STMTNAME from "
            + "sys.systriggers t, sys.sysstatements s "
            + "     where s.stmtid = t.actionstmtid");

        expColNames = new String [] {"1", "2", "STMTNAME"};
        JDBC.assertColumnNames(rs, expColNames);
        rs.next();
        assertEquals("T1        ", rs.getString(1));
        assertEquals("values 1", rs.getString(2));
        assertTrue(matchUUIDs(rs.getString(3)));
        assertFalse(rs.next());

        rs = st.executeQuery("select cast(triggername as char(10)), tablename "
            + "from sys.systriggers t, sys.systables tb"
            + "     where t.tableid = tb.tableid");

        expColNames = new String [] {"1", "TABLENAME"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"T1", "X"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "values SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', 'SYSTRIGGERS')");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{{"1"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop trigger t1");

        // not in sys schema

        assertStatementError(LANG_NO_USER_DDL_IN_SYSTEM_SCHEMA, st,
            "create trigger sys.tr NO CASCADE before insert on x "
            + "for each row values 1");

        // not on table in sys schema

        assertStatementError(LANG_INVALID_OPERATION_ON_SYSTEM_TABLE, st,
            "create trigger tr NO CASCADE before insert on "
            + "sys.systables for each row values 1");

        // duplicate columns, not allowed

        assertStatementError(LANG_DUPLICATE_COLUMN_IN_TRIGGER_UPDATE, st,
            "create trigger tr NO CASCADE before update of x, x "
            + "on x for each row values 1");

        // no params in column list

        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tr NO CASCADE before update of x, ? "
            + "on x for each row values 1");

        // invalid column

        assertStatementError(LANG_COLUMN_NOT_FOUND_IN_TABLE, st,
            "create trigger tr NO CASCADE before update of "
            + "doesnotexist on x for each row values 1");

        // not on view

        assertStatementError(LANG_INVALID_OPERATION_ON_VIEW, st,
            "create trigger tr NO CASCADE before insert on v for "
            + "each row values 1");

        // error to use table qualifier

        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tr NO CASCADE before update of x.x "
            + "on x for each row values 1");

        // error to use schema.table qualifier

        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tr NO CASCADE before update of "
            + "app.x.x on x for each row values 1");

        // no params in trigger action bad

        assertStatementError(LANG_NO_PARAMS_IN_TRIGGER_ACTION, st,
            "create trigger tr NO CASCADE before delete on x for "
            + "each row select * from x where x = ?");

        st.executeUpdate("create trigger stmttrigger NO CASCADE before delete "
            + "on x for each statement values 1");

        rs = st.executeQuery(
            "select triggername, type from sys.systriggers where "
            + "triggername = 'STMTTRIGGER'");

        expColNames = new String [] {"TRIGGERNAME", "TYPE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"STMTTRIGGER", "S"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop trigger stmttrigger");

        st.executeUpdate("create trigger rowtrigger NO CASCADE before delete "
            + "on x for each row values 1");

        rs = st.executeQuery(
            "select triggername, type from sys.systriggers where "
            + "triggername = 'ROWTRIGGER'");

        expColNames = new String [] {"TRIGGERNAME", "TYPE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"ROWTRIGGER", "R"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop trigger rowtrigger");

        // fool around with depedencies CREATE TRIGGER

        st.executeUpdate("create trigger t2 NO CASCADE before update of x,y "
            + "on x for each row values 1");

        st.executeUpdate("alter table x add constraint ck2 check(x > 0)");
        st.executeUpdate("drop view v");
        st.executeUpdate("create view v as select * from x");
        st.executeUpdate("create index ix on x(x)");
        st.executeUpdate("drop trigger t2");
        st.executeUpdate("drop index ix");
        st.executeUpdate("alter table x drop constraint ck2");

        // MAKE SURE TRIGGER SPS IS RECOMPILED IF TABLE IS ALTERED.

        st.executeUpdate("create table y (x int, y int, z int)");

        st.executeUpdate("create trigger tins after insert on x referencing "
            + "new_table as newtab for each statement insert into "
            + "y select x, y, z from newtab");

        st.executeUpdate("insert into x values (1, 1, 1)");

        st.executeUpdate("alter table x add column w int default 100");

        st.executeUpdate(
            "alter table x add constraint nonulls check (w is not null)");

        st.executeUpdate("insert into x values (2, 2, 2, 2)");

        rs = st.executeQuery("select * from y");

        expColNames = new String [] {"X", "Y", "Z"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop trigger tins");
        st.executeUpdate("drop table y");

        // prove that by dropping the underlying table, we have
        // dropped the trigger first, lets create a few other triggers

        st.executeUpdate("create trigger t2 NO CASCADE before update of x,y "
            + "on x for each row values 1");

        st.executeUpdate("create trigger t3 after update of x,y on x for each "
            + "statement values 1");

        st.executeUpdate("create trigger t4 after delete on x for each "
            + "statement values 1");

        rs = st.executeQuery("select cast(triggername as char(10)), tablename "
            + "from sys.systriggers t, sys.systables  tb"
            + "     where t.tableid = tb.tableid order by 1");

        expColNames = new String [] {"1", "TABLENAME"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"T2", "X"},
            {"T3", "X"},
            {"T4", "X"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop view v");
        st.executeUpdate("drop table x");

        rs = st.executeQuery("select cast(triggername as char(10)), tablename "
            + "from sys.systriggers t, sys.systables  tb"
            + "     where t.tableid = tb.tableid order by 1");

        expColNames = new String [] {"1", "TABLENAME"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // schema testing

        st.executeUpdate("create table x (x int, y int, z int)");
        st.executeUpdate("create schema test");
        st.executeUpdate("create trigger test.t1 NO CASCADE before delete on "
            + "x for each row values 1");
        st.executeUpdate("set schema test");
        st.executeUpdate("create trigger t2 NO CASCADE before delete on app.x "
            + "for each row values 1");

        rs = st.executeQuery(
            "select schemaname, triggername from sys.systriggers "
            + "t, sys.sysschemas s"
            + " where s.schemaid = t.schemaid");

        expColNames = new String [] {"SCHEMANAME", "TRIGGERNAME"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"TEST", "T1"},
            {"TEST", "T2"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("set schema app");

        assertStatementError(LANG_SCHEMA_NOT_EMPTY, st,
            "drop schema test restrict");

        st.executeUpdate("drop trigger test.t2");

        assertStatementError(LANG_SCHEMA_NOT_EMPTY, st,
            "drop schema test restrict");

        st.executeUpdate("set schema test");
        st.executeUpdate("drop trigger t1");
        st.executeUpdate("set schema app");

        // ok this time

        st.executeUpdate("drop schema test restrict");
        st.executeUpdate("create table t (x int, y int, c char(1))");

        // try multiple values, make sure result sets don't get
        // screwed up this time we'll print out result sets

        st.executeUpdate("create trigger t1 after insert on t for each row"
            + " values app.triggerFires('3rd')");

        st.executeUpdate("create trigger t2 no cascade before insert on t for "
            + "each statement"
            + " values app.triggerFires('1st')");

        st.executeUpdate("create trigger t3 after insert on t for each row"
            + " values app.triggerFires('4th')");

        st.executeUpdate(
            "create trigger t4 no cascade before insert on t for each row"
            + " values app.triggerFires('2nd')");

        st.executeUpdate(
            "create trigger t5 after insert on t for each statement"
            + " values app.triggerFires('5th')");

        st.executeUpdate("insert into t values " +
                "(2,2,'2')," +
                "(3,3,'3')," +
                "(4,4,'4')");

        assertTriggerOutput(
             "TRIGGER: <1st> on statement insert into t values " +
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" + 
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{2,2,2}\n" +
             "\t{3,3,3}\n" +
             "\t{4,4,4}\n" +
             "\n" +
             "TRIGGER: <2nd> on statement insert into t values " +
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" +
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{2,2,2}\n" +
             "\n" +
             "TRIGGER: <2nd> on statement insert into t values " +
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" +
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{3,3,3}\n" +
             "\n" +
             "TRIGGER: <2nd> on statement insert into t values " +
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" +
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{4,4,4}\n" +
             "\n" +
             "TRIGGER: <3rd> on statement insert into t values " +
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" +
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{2,2,2}\n" +
             "\n" +
             "TRIGGER: <3rd> on statement insert into t values " +
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" +
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{3,3,3}\n" +
             "\n" +
             "TRIGGER: <3rd> on statement insert into t values " +
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" +
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{4,4,4}\n" +
             "\n" +
             "TRIGGER: <4th> on statement insert into t values " +
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" +
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{2,2,2}\n" +
             "\n" +
             "TRIGGER: <4th> on statement insert into t values " +
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" +
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{3,3,3}\n" +
             "\n" +
             "TRIGGER: <4th> on statement insert into t values " + 
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" +
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{4,4,4}\n" +
             "\n" +
             "TRIGGER: <5th> on statement insert into t values " +
             "(2,2,'2'),(3,3,'3'),(4,4,'4')\n" +
             "BEFORE RESULT SET\n" +
             "<NULL>\n" +
             "\n" +
             "AFTER RESULT SET\n" +
             "\t X,Y,C\n" +
             "\t - - -\n" +
             "\t{2,2,2}\n" +
             "\t{3,3,3}\n" +
             "\t{4,4,4}\n\n");
   
        assertUpdateCount(st, 3, "delete from t");

        st.executeUpdate("drop trigger t1");
        st.executeUpdate("drop trigger t2");
        st.executeUpdate("drop trigger t3");
        st.executeUpdate("drop trigger t4");
        st.executeUpdate("drop trigger t5");
        st.executeUpdate("drop table x");
        st.executeUpdate("drop table t");

        // Prove that we are firing the proper triggers based on
        // the columns we are changing;

        st.executeUpdate("create table t (c1 int, c2 int)");

        st.executeUpdate("create trigger tins after insert on t for each row"
            + " values app.triggerFiresMin('insert')");

        st.executeUpdate("create trigger tdel after delete on t for each row"
            + " values app.triggerFiresMin('delete')");

        st.executeUpdate(
            "create trigger tupc1 after update of c1 on t for each row"
            + " values app.triggerFiresMin('update c1')");

        st.executeUpdate(
            "create trigger tupc2 after update of c2 on t for each row"
            + " values app.triggerFiresMin('update c2')");

        st.executeUpdate("create trigger tupc1c2 after update of c1,c2 on t "
            + "for each row"
            + " values app.triggerFiresMin('update c1,c2')");

        st.executeUpdate("create trigger tupc2c1 after update of c2,c1 on t "
            + "for each row"
            + " values app.triggerFiresMin('update c2,c1')");

        st.executeUpdate("insert into t values (1,1)");
        assertTriggerOutput("TRIGGER: <insert>\n");

        st.executeUpdate("update t set c1 = 1");
        assertTriggerOutput(
            "TRIGGER: <update c1>\n" +
            "TRIGGER: <update c1,c2>\n" +
            "TRIGGER: <update c2,c1>\n");

        st.executeUpdate("update t set c2 = 1");
        assertTriggerOutput(
            "TRIGGER: <update c2>\n" +
            "TRIGGER: <update c1,c2>\n" +
            "TRIGGER: <update c2,c1>\n");

        st.executeUpdate("update t set c2 = 1, c1 = 1");
        assertTriggerOutput(
            "TRIGGER: <update c1>\n" +
            "TRIGGER: <update c2>\n" +
            "TRIGGER: <update c1,c2>\n" +
            "TRIGGER: <update c2,c1>\n");

        st.executeUpdate("update t set c1 = 1, c2 = 1");
        assertTriggerOutput(
            "TRIGGER: <update c1>\n" +
            "TRIGGER: <update c2>\n" +
            "TRIGGER: <update c1,c2>\n" +
            "TRIGGER: <update c2,c1>\n");

        st.executeUpdate("delete from t");
        assertTriggerOutput("TRIGGER: <delete>\n");

        // Make sure that triggers work with delimited identifiers
        // Make sure that text munging works correctly

        st.executeUpdate("create table trigtable(\"cOlUmN1\" int, \"cOlUmN2  "
            + "\" int, \"cOlUmN3\"\"\"\"  \" int)");

        st.executeUpdate("create table trighistory(\"cOlUmN1\" int, \"cOlUmN2 "
            + " \" int, \"cOlUmN3\"\"\"\"  \" int)");

        st.executeUpdate("insert into trigtable values (1, 2, 3)");

        st.executeUpdate("create trigger \"tt1\" after insert on trigtable "
            + "referencing NEW as NEW for each row "
            + "insert into trighistory (\"cOlUmN1\", \"cOlUmN2  "
            + "\", \"cOlUmN3\"\"\"\"  \") values (new.\"cOlUmN1\" "
            + "+ 5, \"NEW\".\"cOlUmN2  \" * new.\"cOlUmN3\"\"\"\"  \", 5)");

        rs = st.executeQuery("select cast(triggername as char(10)), CAST "
            + "(TRIGGERDEFINITION AS VARCHAR(180)), STMTNAME from "
            + "sys.systriggers t, sys.sysstatements s "
            + "     where s.stmtid = t.actionstmtid and triggername = 'tt1'");

        expColNames = new String [] {"1", "2", "STMTNAME"};
        JDBC.assertColumnNames(rs, expColNames);

        rs.next();
        assertEquals("tt1       ", rs.getString(1));
        assertEquals(
            "insert into \"APP\".\"TRIGHISTORY\" (\"cOlUmN1\", \"cOlUmN2  \", " +
                "\"cOlUmN3\"\"\"\"  \") " +
            "values (new.\"cOlUmN1\" + 5, \"NEW\".\"cOlUmN2  \" * "
                + "new.\"cOlUmN3\"\"\"\"  \", 5)",
            rs.getString(2));
        assertTrue(matchUUIDs(rs.getString(3)));
        assertFalse(rs.next());

        st.executeUpdate("insert into trigtable values (1, 2, 3)");

        rs = st.executeQuery("select * from trighistory");

        expColNames = new String [] {"cOlUmN1", "cOlUmN2  ", "cOlUmN3\"\"  "};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "6", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop trigger \"tt1\"");

        st.executeUpdate("create trigger \"tt1\" after insert on trigtable "
            + "referencing new as new for each row "
            + "insert into trighistory (\"cOlUmN1\", \"cOlUmN2  "
            + "\", \"cOlUmN3\"\"\"\"  \") values (new.\"cOlUmN1\" "
            + "+ new.\"cOlUmN1\", \"NEW\".\"cOlUmN2  \" * "
            + "new.\"cOlUmN3\"\"\"\"  \", new.\"cOlUmN2  \" * 3)");

        rs = st.executeQuery("select cast(triggername as char(10)), CAST "
            + "(TRIGGERDEFINITION AS VARCHAR(180)), STMTNAME from "
            + "sys.systriggers t, sys.sysstatements s "
            + "     where s.stmtid = t.actionstmtid and triggername = 'tt1'");

        expColNames = new String [] {"1", "2", "STMTNAME"};
        JDBC.assertColumnNames(rs, expColNames);

        rs.next();
        assertEquals("tt1       ", rs.getString(1));
        assertEquals("insert into \"APP\".\"TRIGHISTORY\" (\"cOlUmN1\", \"cOlUmN2 "
                + " \", \"cOlUmN3\"\"\"\"  \") values "
                + "(new.\"cOlUmN1\" + new.\"cOlUmN1\", "
                + "\"NEW\".\"cOlUmN2  \" * new.\"cOlUmN3\"\"\"\"  "
                + "\", new.\"cOlUmN2  \" * 3)",
            rs.getString(2));
        assertTrue(matchUUIDs(rs.getString(3)));
        assertFalse(rs.next());

        st.executeUpdate("insert into trigtable values (1, 2, 3)");

        rs = st.executeQuery("select * from trighistory");

        expColNames = new String [] {"cOlUmN1", "cOlUmN2  ", "cOlUmN3\"\"  "};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "6", "5"},
            {"2", "6", "6"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table trigtable");
        st.executeUpdate("drop table trighistory");

        // trigger bug that got fixed mysteriously between xena
        // and buffy

        st.executeUpdate("create table trigtable1(c1 int, c2 int)");
        st.executeUpdate("create table trighistory(trigtable char(30), " + 
                         "c1 int, c2 int)");

        st.executeUpdate(
            "create trigger trigtable1 after update on trigtable1 "
            + "referencing OLD as oldtable "
            + "for each row "
            + "insert into trighistory values ('trigtable1', "
            + "oldtable.c1, oldtable.c2)");

        st.executeUpdate("insert into trigtable1 values (1, 1)");

        assertUpdateCount(st, 1,
            "update trigtable1 set c1 = 11, c2 = 11");

        rs = st.executeQuery("select * from trighistory");

        expColNames = new String [] {"TRIGTABLE", "C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"trigtable1", "1", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table trigtable1");
        st.executeUpdate("drop table trighistory");

        // Test for bug 3495 - triggers were causing deferred
        // insert, which caused the insert to use a
        // TemporaryRowHolderImpl. This was not being
        // re-initialized properly when closed, and it was trying
        // to re-insert the row from the first insert.

        setAutoCommit(false);

        st.executeUpdate("drop table t");

        st.executeUpdate("create table t (x int)");
        st.executeUpdate("create trigger tr after insert on t for each "
                         + "statement values 1");
        final PreparedStatement pSt = prepareStatement(
            "insert into t values (?)");

        rs = st.executeQuery("values (1)");

        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));

        BaseJDBCTestCase.assertUpdateCount(pSt, 1);

        rs = st.executeQuery("values (2)");

        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));

        BaseJDBCTestCase.assertUpdateCount(pSt, 1);

        rs = st.executeQuery("select * from t");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Test MODE DB2SQL not as reserved keyword. beetle 4546

        assertStatementError(LANG_OBJECT_DOES_NOT_EXIST, st,
            "drop table db2sql");

        assertStatementError(LANG_OBJECT_DOES_NOT_EXIST, st,
            "drop table db2sql2");

        st.executeUpdate(
            "create table db2sql  (db2sql int, mode int, yipng int)");

        st.executeUpdate("create table db2sql2 (db2sql2 int)");

        // Test MODE DB2SQL on trigger.  beetle 4546

        assertStatementError(LANG_OBJECT_NOT_FOUND, st,
            "drop trigger db2sqltr1");

        st.executeUpdate("create trigger db2sqltr1 after insert on db2sql "
            + "for each row "
            + "MODE DB2SQL "
            + "insert into db2sql2 values (1)");

        // Test optimizer plan of trigger action. Beetle 4826

        setAutoCommit(true);

        assertStatementError(LANG_OBJECT_DOES_NOT_EXIST, st,
            "drop table parent");

        st.executeUpdate("create table t1(a int not null primary key, b int)");

        st.executeUpdate(
            "create table parent (a int not null primary key, b int)");

        st.executeUpdate("create trigger trig1 AFTER DELETE on t1 "
            + "referencing OLD as OLD for each row "
            + "delete from parent where a = OLD.a");

        st.executeUpdate("insert into t1 values (0, 1)");
        st.executeUpdate("insert into t1 values (1, 1)");
        st.executeUpdate("insert into t1 values (2, 1)");
        st.executeUpdate("insert into t1 values (3, 1)");

        st.executeUpdate("insert into parent values (0, 1)");
        st.executeUpdate("insert into parent values (1, 1)");
        st.executeUpdate("insert into parent values (2, 1)");
        st.executeUpdate("insert into parent values (3, 1)");
        st.executeUpdate("insert into parent values (4, 1)");

        setAutoCommit(false);

        assertUpdateCount(st, 1,
            "delete from t1 where a = 3");

        // Check the locks, but retry the correctness of the result set for
        // a while since we have seen extraneous locks here on several system
        // tables, which should be released (DERBY-6628)
        long millis = 60000;
        boolean ok = false;
        expColNames = new String [] {"TYPE", "MODE", "TABLENAME"};
        expRS = new String [][]
        {
            {"ROW", "X", "PARENT"},
            {"TABLE", "IX", "PARENT"},
            {"ROW", "X", "T1"},
            {"TABLE", "IX", "T1"}
        };


        while (millis >= 0) {
            rs = st.executeQuery("select type, mode, tablename from " +
                    "syscs_diag.lock_table " +
                    "order by tablename, type");

            JDBC.assertColumnNames(rs, expColNames);

            try {
                JDBC.assertFullResultSet(rs, expRS, true);
                ok = true;
                break;
            } catch (AssertionFailedError t) {
                millis -= 2000;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {}
            }
        }

        if (!ok) {
            // DERBY-6628: get more information
            dumpRs(st.executeQuery(
                    "select * from syscs_diag.lock_table " + 
                    "    order by tablename, type"));
            fail("Unexpected set of locks found");
        }

        rollback();

        setAutoCommit(true);

        st.executeUpdate("drop table t1");
        st.executeUpdate("drop table parent");

        // Test use of old AND new referencing names within the
        // same trigger (beetle 5725).

        st.executeUpdate("create table x(x int)");
        st.executeUpdate("insert into x values (2), (8), (78)");

        st.executeUpdate("create table removed (x int)");

        // statement trigger

        st.executeUpdate("create trigger t1 after update of x on x referencing"
            + " old_table as old new_table as new for each "
            + "statement insert into"
            + " removed select x from old where x not in (select x from "
            + " new where x < 10)");

        rs = st.executeQuery("select * from x");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2"},
            {"8"},
            {"78"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from removed");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        assertUpdateCount(st, 1,
            "update x set x=18 where x=8");

        rs = st.executeQuery("select * from x");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2"},
            {"18"},
            {"78"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from removed");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"8"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // row trigger

        st.executeUpdate("create trigger t2 after update of x on x referencing"
            + " old as oldrow new as newrow for each row insert into"
            + " removed values (newrow.x + oldrow.x)");

        assertUpdateCount(st, 1,
            "update x set x=28 where x=18");

        rs = st.executeQuery("select * from x");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2"},
            {"28"},
            {"78"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from removed");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"8"},
            {"18"},
            {"46"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // do an alter table, then make sure triggers recompile
        // correctly.

        st.executeUpdate("alter table x add column y int");

        assertUpdateCount(st, 1,
            "update x set x=88 where x > 44");

        rs = st.executeQuery("select * from x");

        expColNames = new String [] {"X", "Y"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", null},
            {"28", null},
            {"88", null}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from removed");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"8"},
            {"18"},
            {"46"},
            {"78"},
            {"166"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table x");
        st.executeUpdate("drop table removed");

        // after
        st.executeUpdate("create table x (x int, constraint ck check (x > 0))");
        st.executeUpdate("create trigger tgood after insert on x for each "
                         + "statement insert into x values 666");

        assertStatementError(LANG_TRIGGER_RECURSION_EXCEEDED, st,
            "insert into x values 1");

        rs = st.executeQuery("select * from x");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        st.executeUpdate("drop trigger tgood");

        st.executeUpdate("create trigger tgood after insert on x for each "
            + "statement delete from x");

        st.executeUpdate("insert into x values 1");

        rs = st.executeQuery("select * from x");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        st.executeUpdate("drop trigger tgood");

        st.executeUpdate("create trigger tgood after insert on x for each "
            + "statement update x set x = x+100");

        st.executeUpdate("insert into x values 1");

        rs = st.executeQuery("select * from x");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"101"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop trigger tgood");

        assertUpdateCount(st, 1,
            "delete from x");

        st.executeUpdate("create trigger tgood after insert on x "
            + "for each statement insert into x values (666), (999), (333)");

        assertStatementError(LANG_TRIGGER_RECURSION_EXCEEDED, st,
            "insert into x values 1");

        rs = st.executeQuery("select * from x order by 1");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        st.executeUpdate("drop trigger tgood");

        assertUpdateCount(st, 0,
            "delete from x");

        st.executeUpdate("create trigger tgood after insert on x "
            + "referencing new as n "
            + "for each row insert into x values (n.x)");

        assertStatementError(LANG_TRIGGER_RECURSION_EXCEEDED, st,
            "insert into x values 7");

        rs = st.executeQuery("select * from x order by 1");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        st.executeUpdate("drop trigger tgood");

        assertUpdateCount(st, 0,
            "delete from x");

        st.executeUpdate("create trigger tgood after insert on x "
            + "referencing new as n "
            + "for each row insert into x values (333), (999), (333)");

        assertStatementError(LANG_TRIGGER_RECURSION_EXCEEDED, st,
            "insert into x values 1");

        rs = st.executeQuery("select * from x order by 1");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        st.executeUpdate("drop trigger tgood");

        st.executeUpdate("drop table x");

        // Derby-388: When a set of inserts/updates is performed
        // on a table and each update fires a trigger that in turn
        // performs other updates, Derby will sometimes try to
        // recompile the trigger in the middle of the update
        // process and will throw an NPE when doing so.

        st.executeUpdate("create procedure d388 () language java parameter "
            + "style java modifies sql data"
            + " external name "
            + "'org.apache.derbyTesting.functionTests.tests.lang."
            + "TriggerGeneralTest.derby388'");

        // Just call the procedure; it will do the rest.

        final CallableStatement cSt = prepareCall("call d388()");
        BaseJDBCTestCase.assertUpdateCount(cSt, 0);


        // Derby-85: It turns out that if a table t1 exists in a
        // non-default schema and the default schema (e.g.,
        // "SOMEUSER") doesn't exist yet (because no  objects have
        // been created in that schema), then attempts to create a
        // trigger on t1 using its qualified name will lead to a
        // null pointer  exception in the Derby engine.

        Connection c1 = openDefaultConnection("someuser", "pw");

        c1.setAutoCommit(false);
        Statement c1_st = c1.createStatement();
        c1_st.executeUpdate("create table myschema.mytable (i int)");

        c1_st.executeUpdate("create trigger mytrigger after update on "
            + "myschema.mytable for each row select * from sys.systables");

        c1.rollback();
        c1.close();

        // DERBY-438 - Working triggers with BLOB columns

        setAutoCommit(true);

        st.executeUpdate(
            "create table t438 (id int,  cost decimal(6,2), bl blob)");

        st.executeUpdate("create table t438_t (id int, bl blob, l int, nc "
            + "decimal(6,2), oc decimal(6,2))");

        st.executeUpdate("create trigger tr_438 after update on t438 "
            + "referencing new as n old as o "
            + "for each row "
            + "insert into t438_t(id, bl, l, nc, oc) values (n.id, "
            + "n.bl, length(n.bl), n.cost, o.cost)");

        // initially just some small BLOB values.

        st.executeUpdate("insert into t438 values (1, 34.53, cast "
            + "(X'124594322143423214ab35f2e34c' as blob))");

        st.executeUpdate("insert into t438 values (0, 95.32, null)");

        st.executeUpdate(
            "insert into t438 values (2, 22.21, cast (X'aa' as blob))");

        rs = st.executeQuery(
            "select id, cost, length(bl) from t438 order by 1");

        expColNames = new String [] {"ID", "COST", "3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", "95.32", null},
            {"1", "34.53", "14"},
            {"2", "22.21", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        assertUpdateCount(st, 3,
            "update t438 set cost = cost + 1.23");

        rs = st.executeQuery(
            "select id, length(bl), l, nc, oc from t438_t order by 1,5,4");

        expColNames = new String [] {"ID", "2", "L", "NC", "OC"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", null, null, "96.55", "95.32"},
            {"1", "14", "14", "35.76", "34.53"},
            {"2", "1", "1", "23.44", "22.21"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select id, cast (bl as blob(20)) from t438 order by 1");

        expColNames = new String [] {"ID", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", null},
            {"1", "124594322143423214ab35f2e34c"},
            {"2", "aa"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select id, cast (bl as blob(20)) from t438_t order by 1");

        expColNames = new String [] {"ID", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", null},
            {"1", "124594322143423214ab35f2e34c"},
            {"2", "aa"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table t438");

        st.executeUpdate("drop table t438_t");

        // now re-start with CLOB types

        st.executeUpdate(
            "create table t438 (id int,  cost decimal(6,2), cl clob)");

        st.executeUpdate("create table t438_t (id int, cl clob, l int, nc "
            + "decimal(6,2), oc decimal(6,2))");

        st.executeUpdate("create trigger tr_438 after update on t438 "
            + "referencing new as n old as o "
            + "for each row "
            + "insert into t438_t(id, cl, l, nc, oc) values (n.id, "
            + "n.cl, length(n.cl), n.cost, o.cost)");

        // initially just some small CLOB values.

        st.executeUpdate("insert into t438 values (1, 34.53, cast ('Italy''s "
            + "centre-left leader Romano Prodi insists his poll "
            + "victory is valid as contested ballots are checked.' "
            + "as clob))");

        st.executeUpdate(
            "insert into t438 values (0, 95.32, null)");

        st.executeUpdate(
            "insert into t438 values (2, 22.21, cast ('free' as clob))");

        rs = st.executeQuery(
            "select id, cost, length(cl) from t438 order by 1");

        expColNames = new String [] {"ID", "COST", "3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", "95.32", null},
            {"1", "34.53", "107"},
            {"2", "22.21", "4"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        assertUpdateCount(st, 3,
            "update t438 set cost = cost + 1.23");

        rs = st.executeQuery(
            "select id, length(cl), l, nc, oc from t438_t order by 1,5,4");

        expColNames = new String [] {"ID", "2", "L", "NC", "OC"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", null, null, "96.55", "95.32"},
            {"1", "107", "107", "35.76", "34.53"},
            {"2", "4", "4", "23.44", "22.21"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select id, cast (cl as clob(60)) from t438 order by 1");

        expColNames = new String [] {"ID", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", null},
            {"1", "Italy's centre-left leader Romano Prodi insists his poll " +
                  "vic"},
            {"2", "free"}
        };

        JDBC.assertFullResultSet(
            rs, expRS, new String[]{"01004", "01004", "01004"});

        rs = st.executeQuery(
            "select id, cast (cl as clob(60)) from t438_t order by 1");
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", null},
            {"1", "Italy's centre-left leader Romano Prodi insists his poll " +
                  "vic"},
            {"2", "free"}
        };

        JDBC.assertFullResultSet(
            rs, expRS, new String[]{"01004", "01004", "01004"});

        st.executeUpdate("drop table t438");
        st.executeUpdate("drop table t438_t");

        // Testcase showing DERBY-1258

        st.executeUpdate("create table tsn (I integer, \"i\" integer)");
        st.executeUpdate("create table tsn_t (a integer, b integer)");
        st.executeUpdate("create trigger tr_sn after insert on tsn "
                         + "referencing new as n "
                         + "for each row "
                         + "insert into tsn_t(a, b) values (n.I, n.\"i\")");

        st.executeUpdate("insert into tsn values (1, 234)");

        rs = st.executeQuery("select * from tsn");

        expColNames = new String [] {"I", "i"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "234"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Should have 1,234 as data in tsn_t

        rs = st.executeQuery("select * from tsn_t");

        expColNames = new String [] {"A", "B"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "234"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table tsn");
        st.executeUpdate("drop table tsn_t");

        // Testcase showing DERBY-1064

        st.executeUpdate("CREATE TABLE T10641 ( X INT PRIMARY KEY )");
        st.executeUpdate("CREATE TABLE T10641_DELETIONS ( X INT )");
        st.executeUpdate("CREATE TABLE T10642 ("
            + "    Y INT,"
            + "    CONSTRAINT Y_AND_X FOREIGN KEY(Y) REFERENCES "
            + "T10641(X) ON DELETE CASCADE)");

        st.executeUpdate("CREATE TABLE T10642_DELETIONS ( Y INT )");

        st.executeUpdate("CREATE TRIGGER TRIGGER_T10641"
            + "    AFTER DELETE ON T10641"
            + "    REFERENCING OLD AS OLD_ROW"
            + "    FOR EACH ROW"
            + "    INSERT INTO T10641_DELETIONS VALUES (OLD_ROW.X)");

        st.executeUpdate("CREATE TRIGGER TRIGGER_T10642"
            + "    AFTER DELETE ON T10642"
            + "    REFERENCING OLD AS OLD_ROW"
            + "    FOR EACH ROW"
            + "    INSERT INTO T10642_DELETIONS VALUES (OLD_ROW.Y)");

        st.executeUpdate("INSERT INTO T10641 VALUES (0)");
        st.executeUpdate("INSERT INTO T10642 VALUES (0)");
        st.executeUpdate("INSERT INTO T10641 VALUES (1)");
        st.executeUpdate("INSERT INTO T10642 VALUES (1)");

        rs = st.executeQuery("SELECT * FROM T10641");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("SELECT * FROM T10642");

        expColNames = new String [] {"Y"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        assertUpdateCount(st, 2,
            "DELETE FROM T10641");

        rs = st.executeQuery("SELECT * FROM T10641");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery("SELECT * FROM T10642");

        expColNames = new String [] {"Y"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery("SELECT * FROM T10641_DELETIONS");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("SELECT * FROM T10642_DELETIONS");

        expColNames = new String [] {"Y"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // DERBY-1652

        st.executeUpdate("create table test (testid integer not null "
            + "    generated always as identity (start with 1, "
            + "increment by 1), "
            + "    info integer not null, ts timestamp not null "
            + "default '1980-01-01-00.00.00.000000')");

        st.executeUpdate("create trigger update_test "
            + "    after update on test "
            + "    referencing old as old "
            + "    for each row "
            + "    update test set ts=current_timestamp where "
            + "testid=old.testid");

        st.executeUpdate("insert into test(info) values (1),(2),(3)");

        assertStatementError(LANG_TRIGGER_RECURSION_EXCEEDED, st,
            "UPDATE TEST SET INFO = 1 WHERE TESTID = 2");

        st.executeUpdate("drop table test");

        // DERBY-1621 creating and dropping index on the table in
        // the trigger action

        st.executeUpdate("create table t1 (i int)");
        st.executeUpdate("create table t2 (i int)");
        st.executeUpdate("create trigger tt after insert on t1 for each "
                         + "statement insert into t2 values 1");

        st.executeUpdate("insert into t1 values 1");

        st.executeUpdate("create unique index tu on t2(i)");

        assertStatementError(LANG_DUPLICATE_KEY_CONSTRAINT, st,
            "insert into t1 values 1");

        rs = st.executeQuery("select * from t2");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{{"1"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        assertStatementError(LANG_DUPLICATE_KEY_CONSTRAINT, st,
            "insert into t1 values 1");

        rs = st.executeQuery("select * from t2");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{{"1"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop index tu");

        rs = st.executeQuery("select * from t2");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{{"1"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("insert into t1 values 1");

        rs = st.executeQuery("select * from t2");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop trigger tt");

        // dropping a table which the trigger references, should
        // fail (DERBY-2041)

        st.executeUpdate("create table t3 (i int)");

        st.executeUpdate("create table t4 (i int)");

        st.executeUpdate("create trigger tt2 after insert on t3 for each "
            + "statement insert into t4 values 1");

        st.executeUpdate("insert into t3 values 1");

        rs = st.executeQuery("select * from t4");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{{"1"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        assertStatementError(LANG_PROVIDER_HAS_DEPENDENT_OBJECT, st,
            "drop table t4");

        st.executeUpdate("insert into t3 values 1");

        rs = st.executeQuery("select * from t4");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // dropping a function which the trigger references

        st.executeUpdate("create function max_value(x int, y int) returns int "
            + "language java parameter style java external name "
            + "'java.lang.Math.max'");

        st.executeUpdate("create table test(a integer)");

        st.executeUpdate("create trigger test_trigger AFTER insert on test "
            + "FOR EACH ROW values max_value(2,4)");

        st.executeUpdate("insert into test values(1)");

        //- drop function should fail (DERBY-2041)

        assertStatementError(LANG_PROVIDER_HAS_DEPENDENT_OBJECT, st,
            "drop function max_value");

        st.executeUpdate("insert into test values(2)");
        st.executeUpdate("insert into test values(1)");

        // dropping a view which the trigger references

        st.executeUpdate("create table t11TriggerTest (c111 int not null "
            + "primary key, c112 int)");

        st.executeUpdate("insert into t11TriggerTest values(1,1)");
        st.executeUpdate("insert into t11TriggerTest values(2,2)");

        // create a view based on table t11TriggerTest

        st.executeUpdate(
            "create view v21ViewTest as select * from t11TriggerTest");

        // get ready to create a trigger. Trigger is created on
        // t31TriggerTest and it inserts into t32TriggerTest

        st.executeUpdate("create table t31TriggerTest (c311 int)");
        st.executeUpdate("create table t32TriggerTest (c321 int)");

        st.executeUpdate("create trigger tr31t31TriggerTest after insert on "
            + "t31TriggerTest for each statement"
            + "   insert into t32TriggerTest values (select c111 "
            + "from v21ViewTest where c112=1)");

        // try an insert which will fire the trigger

        st.executeUpdate("insert into t31TriggerTest values(1)");

        rs = st.executeQuery("select * from t31TriggerTest");

        expColNames = new String [] {"C311"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{{"1"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        // we know the trigger got fired if there is one row in
        // t32TriggerTest

        rs = st.executeQuery("select * from t32TriggerTest");

        expColNames = new String [] {"C321"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{{"1"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        // drop the view used by the trigger. should fail after
        // DERBY-2041.

        assertStatementError(LANG_PROVIDER_HAS_DEPENDENT_OBJECT, st,
            "drop view v21ViewTest");

        // try an insert which would cause insert trigger to fire.

        st.executeUpdate("insert into t31TriggerTest values(1)");

        rs = st.executeQuery("select * from t31TriggerTest");

        expColNames = new String [] {"C311"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from t32TriggerTest");

        expColNames = new String [] {"C321"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // DERBY-630 NPE in CREATE TRIGGER when compilation schema
        // is other than APP.

        Connection user1_c = openDefaultConnection("user1", "pw");

        Statement u1_st = user1_c.createStatement();

        u1_st.executeUpdate("create table ippo.t1 (i int)");
        u1_st.executeUpdate("create table ippo.t2 (i int)");
        u1_st.executeUpdate("create index ippo.idx2 on t2(i)");

        u1_st.executeUpdate(
            "create trigger ippo.tr1 after insert on ippo.t1 for "
            + "each row mode db2sql "
            + "   insert into ippo.t2 values 1");

        u1_st.executeUpdate("insert into ippo.t1 values 1");

        u1_st.executeUpdate("insert into ippo.t1 values 1");

        ResultSet u1_rs = u1_st.executeQuery("select * from ippo.t2");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(u1_rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"1"}
        };

        JDBC.assertFullResultSet(u1_rs, expRS, true);

        u1_st.executeUpdate("drop index ippo.idx2");
        u1_st.executeUpdate("insert into ippo.t1 values 1");

        u1_rs = u1_st.executeQuery("select * from ippo.t2");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(u1_rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"1"},
            {"1"}
        };

        JDBC.assertFullResultSet(u1_rs, expRS, true);

        u1_st.executeUpdate("drop trigger ippo.tr1");
        u1_st.executeUpdate("drop table ippo.t2");
        u1_st.executeUpdate("drop table ippo.t1");
        u1_st.executeUpdate("drop schema ippo restrict");

        // DERBY-1953 if neither FOR EACH STATEMENT or FOR EACH
        // ROW is specified, FOR EACH STATEMENT is implicit.

        u1_st.executeUpdate("create table topt1 (i int)");
        u1_st.executeUpdate("insert into topt1 values 1,2,3");

        u1_st.executeUpdate("create table topt2 (i int)");

        // expect error

        assertStatementError(LANG_SYNTAX_ERROR, u1_st,
            "create trigger tropt after insert on topt1 for each "
            + "mode db2sql insert into topt2 values 1");

        // ok

        u1_st.executeUpdate(
            "create trigger tropt after insert on topt1 insert "
            + "into topt2 values 1");

        u1_st.executeUpdate("insert into topt1 values 4,5,6");

        // expect 1 row

        rs = u1_st.executeQuery("select * from topt2");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{{"1"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        u1_st.executeUpdate("drop trigger tropt");

        assertUpdateCount(u1_st, 1,
            "delete from topt2");

        u1_st.executeUpdate("create trigger tropt after insert on topt1 "
            + "referencing new_table as new_opt1 insert into topt2 "
            + "select * from new_opt1");

        u1_st.executeUpdate("insert into topt1 values 7,8,9");

        // expect 3 rows

        rs = u1_st.executeQuery("select * from topt2");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"7"},
            {"8"},
            {"9"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        u1_st.executeUpdate("drop trigger tropt");

        assertUpdateCount(u1_st, 3,
            "delete from topt2");

        u1_st.executeUpdate("create trigger tropt after insert on topt1 "
            + "referencing new_table as new_opt1 for each "
            + "statement insert into topt2 select * from new_opt1");

        u1_st.executeUpdate("insert into topt1 values 10,11,12");

        // expect 3 rows

        rs = u1_st.executeQuery("select * from topt2");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10"},
            {"11"},
            {"12"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        u1_st.executeUpdate("drop trigger tropt");

        assertUpdateCount(u1_st, 3,
            "delete from topt2");

        u1_st.executeUpdate("create trigger tropt after update on topt1 "
            + "referencing old as oldrow for each row insert into "
            + "topt2 values oldrow.i");

        assertUpdateCount(u1_st, 12,
            "update topt1 set i=100");

        // expect 12 rows

        rs = u1_st.executeQuery("select * from topt2");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"5"},
            {"6"},
            {"7"},
            {"8"},
            {"9"},
            {"10"},
            {"11"},
            {"12"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        u1_st.executeUpdate("drop trigger tropt");
        u1_st.executeUpdate("drop table topt2");
        u1_st.executeUpdate("drop table topt1");

        // DERBY-1204 trigger causes
        // StringIndexOutOfBoundsException which half closes
        // connection and causes rest of test to fail. Enable this
        // trigger test case to resolve 1204.

        u1_st.executeUpdate("create table x (x int)");

        // ok

        u1_st.executeUpdate("create trigger tgood after insert on x "
            + "referencing new as n "
            + "for each row insert into x values (n.x), (999), (333)");

        assertStatementError(LANG_TRIGGER_RECURSION_EXCEEDED, u1_st,
            "insert into x values 1");

        rs = u1_st.executeQuery("select * from x order by 1");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        u1_st.executeUpdate("drop trigger tgood");
        u1_st.executeUpdate("drop table x");

        u1_st.executeUpdate("create table x (i int)");
        u1_st.executeUpdate("create table y (i int)");

        // ok

        u1_st.executeUpdate("create trigger tgood after insert on x "
            + "for each statement insert into y values (666), (999), (333)");

        u1_st.executeUpdate("drop trigger tgood");

        // ok

        u1_st.executeUpdate("create trigger tgood after insert on x "
            + "referencing new as n "
            + "for each row insert into y values (n.i)");

        u1_st.executeUpdate("drop trigger tgood");

        // ok

        u1_st.executeUpdate("create trigger tgood after insert on x "
            + "referencing new as n "
            + "for each row insert into y values (333), (999), (333)");

        u1_st.executeUpdate("drop trigger tgood");

        // ok.  This used to throw StringIndexOutOfBoundsException

        u1_st.executeUpdate("create trigger tgood after insert on x "
            + "referencing new as n "
            + "for each row insert into y values (n.i), (999), (333)");

        u1_st.executeUpdate("insert into x values (888)");

        rs = u1_st.executeQuery("select * from y");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"888"},
            {"999"},
            {"333"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        u1_st.executeUpdate("drop trigger tgood");

        assertUpdateCount(u1_st, 1,
            "delete from x");

        assertUpdateCount(u1_st, 3,
            "delete from y");

        u1_st.executeUpdate("create trigger tgood after insert on x "
            + "referencing new as n "
            + "for each row insert into y values (n.i), (n.i+1), (n.i+2)");

        u1_st.executeUpdate("insert into x values (1), (4), (7)");

        rs = u1_st.executeQuery("select * from y");

        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"5"},
            {"6"},
            {"7"},
            {"8"},
            {"9"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        u1_st.executeUpdate("drop trigger tgood");
        u1_st.executeUpdate("drop table x");
        u1_st.executeUpdate("drop table y");

        u1_st.executeUpdate("create table x (i int, j varchar(10))");
        u1_st.executeUpdate("create table y (i int, j varchar(10))");

        u1_st.executeUpdate("create trigger tgood after insert on x "
            + "referencing new as n "
            + "for each row insert into y values (0, 'X'), (n.i, "
            + "'Y'), (0, n.j), (n.i,n.j)");

        u1_st.executeUpdate("insert into x values (1,'A'), (2,'B'), (3, 'C')");

        rs = u1_st.executeQuery("select * from y");

        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", "X"},
            {"1", "Y"},
            {"0", "A"},
            {"1", "A"},
            {"0", "X"},
            {"2", "Y"},
            {"0", "B"},
            {"2", "B"},
            {"0", "X"},
            {"3", "Y"},
            {"0", "C"},
            {"3", "C"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        u1_st.executeUpdate("drop trigger tgood");
        u1_st.executeUpdate("drop table x");
        u1_st.executeUpdate("drop table y");
        user1_c.close();

        // DERBY-2183 trigger recompilation test
        Connection user1b_c = openDefaultConnection("user1", "pw");
        Statement u1b_st = user1b_c.createStatement();
        CallableStatement u1b_cSt;

        u1b_st.executeUpdate("set schema app");

        assertStatementError(LANG_OBJECT_NOT_FOUND, u1b_st,
            "drop trigger app.tr1");

        u1b_st.executeUpdate("drop table app.t1");
        u1b_st.executeUpdate("create table app.t1 (i int, j int)");
        u1b_st.executeUpdate("insert into app.t1 values (1,10)");

        u1b_st.executeUpdate(
            "create trigger app.tr1 after update of i on app.t1 "
            + "referencing old as old for each row update t1 set j "
            + "= old.j+1");

        assertUpdateCount(u1b_st, 1,
            "update app.t1 set i=i+1");

        ResultSet u1b_rs = u1b_st.executeQuery("select * from app.t1");

        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(u1b_rs, expColNames);

        expRS = new String [][]
        {
            {"2", "11"}
        };

        JDBC.assertFullResultSet(u1b_rs, expRS, true);

        u1b_cSt = prepareCall(
            "call sqlj.install_jar('file:extin/dcl_emc1.jar', "
            + "'APP.dcl_emc1', 0)");assertUpdateCount(u1b_cSt, 0);

        Connection user2_c = openDefaultConnection("user2", "pw");
        Statement u2_st = user2_c.createStatement();

        // ok

        assertUpdateCount(u2_st, 1,
            "update app.t1 set i=i+1");

        ResultSet u2_rs = u2_st.executeQuery("select * from app.t1");

        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(u2_rs, expColNames);

        expRS = new String [][]
        {
            {"3", "12"}
        };

        JDBC.assertFullResultSet(u2_rs, expRS, true);

        CallableStatement u2_cSt = prepareCall(
            "call sqlj.replace_jar('file:extin/dcl_emc1.jar', 'APP.dcl_emc1')");
        assertUpdateCount(u2_cSt, 0);

        assertUpdateCount(u2_st, 1,
            "update app.t1 set i=i+1");

        u2_rs = u2_st.executeQuery("select * from app.t1");

        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(u2_rs, expColNames);

        expRS = new String [][]
        {
            {"4", "13"}
        };

        JDBC.assertFullResultSet(u2_rs, expRS, true);

        u2_cSt = prepareCall(
            "call sqlj.remove_jar('APP.dcl_emc1', 0)");
        assertUpdateCount(u2_cSt, 0);

        assertUpdateCount(u2_st, 1,
            "update app.t1 set i=i+1");

        u2_rs = u2_st.executeQuery("select * from app.t1");

        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(u2_rs, expColNames);

        expRS = new String [][]
        {
            {"5", "14"}
        };

        JDBC.assertFullResultSet(u2_rs, expRS, true);

        u2_st.executeUpdate("drop trigger app.tr1");
        u2_st.executeUpdate("drop table app.t1");

        u1b_st.executeUpdate("set schema app");

        u1b_st.executeUpdate("create table app.t1 (id int, i int, j int)");
        u1b_st.executeUpdate("insert into app.t1 values (1,10, 100)");
        u1b_st.executeUpdate("insert into app.t1 values (2,20, 200)");
        u1b_st.executeUpdate("insert into app.t1 values (3,30, 300)");

        u1b_st.executeUpdate("create trigger app.tr1 after update on app.t1 "
            + "referencing old as oldt  new as newt "
            + "for each row update t1 set t1.j = CASE WHEN (oldt.j "
            + "< 100) THEN (oldt.j + 1) ELSE 1 END WHERE"
            + "((newt.j is null) OR (oldt.j = newt.j)) AND newt.id = t1.id");

        assertUpdateCount(u1b_st, 3,
            "update app.t1 set i=i+1");

        u1b_rs = u1b_st.executeQuery("select * from app.t1");

        expColNames = new String [] {"ID", "I", "J"};
        JDBC.assertColumnNames(u1b_rs, expColNames);

        expRS = new String [][]
        {
            {"1", "11", "1"},
            {"2", "21", "1"},
            {"3", "31", "1"}
        };

        JDBC.assertFullResultSet(u1b_rs, expRS, true);

        u1b_cSt = prepareCall(
            "call sqlj.install_jar('file:extin/dcl_emc1.jar', "
            + "'APP.dcl_emc1', 0)");assertUpdateCount(u1b_cSt, 0);

        // switch to user 2

        // ok

        assertUpdateCount(u2_st, 3,
            "update app.t1 set i=i+1");

        u2_rs = u2_st.executeQuery("select * from app.t1");

        expColNames = new String [] {"ID", "I", "J"};
        JDBC.assertColumnNames(u2_rs, expColNames);

        expRS = new String [][]
        {
            {"1", "12", "2"},
            {"2", "22", "2"},
            {"3", "32", "2"}
        };

        JDBC.assertFullResultSet(u2_rs, expRS, true);

        u2_cSt = prepareCall(
            "call sqlj.replace_jar('file:extin/dcl_emc1.jar', 'APP.dcl_emc1')");
        assertUpdateCount(u2_cSt, 0);

        assertUpdateCount(u2_st, 3,
            "update app.t1 set i=i+1");

        u2_rs = u2_st.executeQuery("select * from app.t1");

        expColNames = new String [] {"ID", "I", "J"};
        JDBC.assertColumnNames(u2_rs, expColNames);

        expRS = new String [][]
        {
            {"1", "13", "3"},
            {"2", "23", "3"},
            {"3", "33", "3"}
        };

        JDBC.assertFullResultSet(u2_rs, expRS, true);

        u2_cSt = prepareCall(
            "call sqlj.remove_jar('APP.dcl_emc1', 0)");
        assertUpdateCount(u2_cSt, 0);

        assertUpdateCount(u2_st, 3,
            "update app.t1 set i=i+1");

        u2_rs = u2_st.executeQuery("select * from app.t1");

        expColNames = new String [] {"ID", "I", "J"};
        JDBC.assertColumnNames(u2_rs, expColNames);

        expRS = new String [][]
        {
            {"1", "14", "4"},
            {"2", "24", "4"},
            {"3", "34", "4"}
        };

        JDBC.assertFullResultSet(u2_rs, expRS, true);

        u2_st.executeUpdate("drop trigger app.tr1");
        u2_st.executeUpdate("drop table app.t1");
        user2_c.close();
        user1b_c.close();
    }

    /**
     * Derby-388: When a set of inserts and updates is performed on a table
     * and each update fires a trigger that in turn performs other updates,
     * Derby will sometimes try to recompile the trigger in the middle
     * of the update process and will throw an NPE when doing so.
     *
     * @throws java.sql.SQLException
     */
    public static void derby388() throws SQLException
    {
        final Connection conn =
            DriverManager.getConnection("jdbc:default:connection");
        final boolean needCommit = !conn.getAutoCommit();
        final Statement s = conn.createStatement();

        // Create our objects.
        s.execute("CREATE TABLE D388_T1 (ID INT)");
        s.execute("CREATE TABLE D388_T2 (ID_2 INT)");
        s.execute(
            "CREATE TRIGGER D388_TRIG1 AFTER UPDATE OF ID ON D388_T1" +
            "   REFERENCING NEW AS N_ROW FOR EACH ROW" +
            "   UPDATE D388_T2" +
            "   SET ID_2 = " +
            "     CASE WHEN (N_ROW.ID <= 0) THEN N_ROW.ID" +
            "     ELSE 6 END " +
            "   WHERE N_ROW.ID < ID_2"
        );

        if (needCommit)
            conn.commit();

        // Statement to insert into D388_T1.
        final PreparedStatement ps1 = conn.prepareStatement(
            "INSERT INTO D388_T1 VALUES (?)");

        // Statement to insert into D388_T2.
        final PreparedStatement ps2 = conn.prepareStatement(
            "INSERT INTO D388_T2(ID_2) VALUES (?)");

        // Statement that will cause the trigger to fire.
        final Statement st = conn.createStatement();
        for (int i = 0; i < 20; i++) {

            for (int id = 0; id < 10; id++) {

                ps2.setInt(1, id);
                ps2.executeUpdate();
                ps1.setInt(1, 2*id);
                ps1.executeUpdate();

                if (needCommit)
                    conn.commit();

            }

            // Execute an update, which will fire the trigger.
            // Note that having the update here is important
            // for the reproduction.  If we try to remove the
            // outer loop and just insert lots of rows followed
            // by a single UPDATE, the problem won't reproduce.
            st.execute("UPDATE D388_T1 SET ID=5");
            if (needCommit)
                conn.commit();

        }

        // Clean up.
        s.execute("DROP TABLE D388_T1");
        s.execute("DROP TABLE D388_T2");

        if (needCommit)
            conn.commit();

        st.close();
        ps1.close();
        ps2.close();
    }

    public static String triggerFiresMinimal(String string) throws Throwable
    {
        out.println("TRIGGER: " + "<"+string+">");
        return "";
    }

    public static String triggerFires(String string) throws Throwable
    {
        final TriggerExecutionContext tec = 
                Factory.getTriggerExecutionContext();
        out.println(
            "TRIGGER: " + "<" + string + "> on statement " +
            tec.getEventStatementText());
        printTriggerChanges();
        return "";
    }

    private static void printTriggerChanges() throws Throwable
    {
        final TriggerExecutionContext tec = 
                Factory.getTriggerExecutionContext();
        out.println("BEFORE RESULT SET");
        BaseJDBCTestCase.dumpRs(tec.getOldRowSet(), out);
        out.println("\nAFTER RESULT SET");
        BaseJDBCTestCase.dumpRs(tec.getNewRowSet(), out);
    }   


    /** 
     * Compare the expected output with the fired triggers' output
     * and reset the print stream used for this purpose.
     */
    private static void assertTriggerOutput(String expected) {
        // Windows: get rid of any carriage returns added by println in output 
        // before we compare since our expected output contains only newlines.
        String got = outs.toString().replaceAll("\r", "");
        
        assertEquals(expected, got);
        outs.reset();
    }

    
    // Match TRIGGERACTN_uuid_uuid
    private boolean matchUUIDs(String raw) {
        final StringBuilder sb = new StringBuilder();
        // UUID 1
        addPat(sb, HEX_DIGIT, 8);

        for (int i = 0; i < 3; i++) {
            sb.append("-");
            addPat(sb, HEX_DIGIT, 4);
        }

        sb.append("-");
        addPat(sb, HEX_DIGIT, 12);
        sb.append('_');

        // UUID 2
        addPat(sb, HEX_DIGIT, 8);

        for (int i = 0; i < 3; i++) {
            sb.append("-");
            addPat(sb, HEX_DIGIT, 4);
        }

        sb.append("-");
        addPat(sb, HEX_DIGIT, 12);

        final Pattern p = Pattern.compile("TRIGGERACTN_" + sb.toString());
        final Matcher m = p.matcher(raw);
        return m.matches();
    }
    
    private void addPat(StringBuilder sb, String pat, int cnt) {
        for (int i = 0; i < cnt; i++) {
            sb.append(pat);
        }
    }
}
