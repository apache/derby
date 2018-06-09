/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.TriggerBeforeTrigTest
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
 * Consolidated BEFORE trigger tests from all trigger tests. 
 *  
 * The following tests moved from TriggerValidate.sql to
 * "here" (i.e. to the earlier the harness version
 * ("triggerBeforeTrig.sql) of this JUnit test.
 */
       
public final class TriggerBeforeTrigTest extends BaseJDBCTestCase {

    public static final String LANG_COL_NOT_FOUND = "42X04";
    public static final String LANG_TRIGGER_BAD_REF_MISMATCH = "42Y92";
    public static final String LANG_TRIGGER_BAD_REF_CLAUSE_DUPS = "42Y93";
    public static final String LANG_UNSUPPORTED_TRIGGER_STMT = "42Z9D";
    public static final String LANG_SYNTAX_ERROR = "42X01";
    
    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name test name
     */
    public TriggerBeforeTrigTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        return TestConfiguration.defaultSuite(TriggerBeforeTrigTest.class);
    }

    public void testBeforeTriggers() throws Exception
    {
        ResultSet rs;
        Statement st = createStatement();
        
        try {
        st.executeUpdate("create table x (x int, constraint ck check (x > 0))");
        
        st.executeUpdate("create table unrelated (x int, constraint "
            + "ckunrelated check (x > 0))");
        
        st.executeUpdate("create index x on x(x)");
        
        //---------------------------------- 
        // DDL
        //----------------------------------
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement drop table x");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement drop index x");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement alter table x add column y int");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement alter table x add constraint ck2 "
            + "check(x > 0)");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement alter table x drop constraint ck");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement create index x2 on x (x)");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement create index xunrelated on unrelated(x)");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement drop index xunrelated");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement drop trigger tbad");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement "
            + "	create trigger tbad2 NO CASCADE before insert on x "
            + "for each statement values 1");
        
        st.executeUpdate("create trigger tokv1 NO CASCADE before insert on x "
            + "for each statement values 1");
        
        st.executeUpdate("insert into x values 1");
        
        rs = st.executeQuery("select * from x");
        JDBC.assertFullResultSet(rs, new String [][]{{"1"}}, true);
        
        st.executeUpdate("drop trigger tokv1");
        
        //---------------------------------- 
        // MISC
        //----------------------------------
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement set isolation to rr");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger tbad NO CASCADE before insert on x "
            + "for each statement lock table x in share mode");
        
        //---------------------------------- 
        // DML, cannot perform 
        // dml on same table for before trigger, of for 
        // after
        // ---------------------------------- 
        
        // before
        
        assertStatementError(LANG_UNSUPPORTED_TRIGGER_STMT, st,
            "create trigger tbadX NO CASCADE before insert on x "
            + "for each statement insert into x values 1");
        
        assertStatementError(LANG_UNSUPPORTED_TRIGGER_STMT, st,
            "create trigger tbadX NO CASCADE before insert on x "
            + "for each statement delete from x");
        
        assertStatementError(LANG_UNSUPPORTED_TRIGGER_STMT, st,
            "create trigger tbadX NO CASCADE before insert on x "
            + "for each statement update x set x = x");
        
        // Following tests moved here from triggerRefClause, since 
        // these use BEFORE triggers syntax
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger t1 NO CASCADE before update on x "
            + "referencing badtoken as oldtable for each row values 1");
        
        assertStatementError(LANG_SYNTAX_ERROR, st,
            "create trigger t1 NO CASCADE before update on x "
            + "referencing old as oldrow new for each row values 1");
        
        // dup names
        
        assertStatementError(LANG_TRIGGER_BAD_REF_CLAUSE_DUPS, st,
            "create trigger t1 NO CASCADE before update on x "
            + "referencing old as oldrow new as newrow old as oldrow2 "
            + "	for each row values 1");
        
        assertStatementError(LANG_TRIGGER_BAD_REF_CLAUSE_DUPS, st,
            "create trigger t1 NO CASCADE before update on x "
            + "referencing new as newrow new as newrow2 old as oldrow2 "
            + "	for each row values 1");
        
        // mismatch: row->for each statement, table->for each row
        
        assertStatementError(LANG_TRIGGER_BAD_REF_MISMATCH, st,
            "create trigger t1 NO CASCADE before update on x "
            + "referencing new_table as newtab for each row values 1");
        
        assertStatementError(LANG_TRIGGER_BAD_REF_MISMATCH, st,
            "create trigger t1 NO CASCADE before update on x "
            + "referencing new as newrow for each statement values 1");
        
        // same as above, but using old
        
        assertStatementError(LANG_TRIGGER_BAD_REF_MISMATCH, st,
            "create trigger t1 NO CASCADE before update on x "
            + "referencing old_table as old for each row select * from old");
        
        assertStatementError(LANG_TRIGGER_BAD_REF_MISMATCH, st,
            "create trigger t1 NO CASCADE before update on x "
            + "referencing old_table as old for each statement values old.x");
        
        // old and new cannot be used once they have been redefined
        
        assertStatementError(LANG_TRIGGER_BAD_REF_MISMATCH, st,
            "create trigger t1 NO CASCADE before update on x "
            + "referencing old_table as oldtable for each "
            + "statement select * from old");
        
        assertStatementError(LANG_COL_NOT_FOUND, st,
            "create trigger t1 NO CASCADE before update on x "
            + "referencing old as oldtable for each row values old.x");
        
        // try some other likely uses
        
        st.executeUpdate("create table y (x int)");
        
        assertStatementError(LANG_UNSUPPORTED_TRIGGER_STMT, st,
            "create trigger t1 NO CASCADE before insert on x "
            + "referencing new_table as newrowtab for each "
            + "statement insert into y select x from newrowtab");
        
        } finally {
            // cleanup
            dontThrow(st, "drop table x");
            dontThrow(st, "drop table y");
            dontThrow(st, "drop table unrelated");
            commit();
        }
    }



    private void dontThrow(Statement st, String stm) {
        try {
            st.executeUpdate(stm);
        } catch (SQLException e) {
            // ignore, best effort here
            println("\"" + stm+ "\" failed");
        }
    }
}
