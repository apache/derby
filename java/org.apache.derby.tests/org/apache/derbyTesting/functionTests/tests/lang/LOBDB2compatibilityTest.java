/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LOBDB2compatibilityTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
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

import java.sql.ResultSet;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test to run in db2 compatibility mode
 */
public final class LOBDB2compatibilityTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name test name
     */
    public LOBDB2compatibilityTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        return TestConfiguration.defaultSuite(LOBDB2compatibilityTest.class);
    }

    public void test_LOBDB2compatibility() throws Exception
    {
        ResultSet rs;
        final Statement st = createStatement();

        String [][] expRS;

        st.executeUpdate("create table t1(c11 int)");
        st.executeUpdate("insert into t1 values(1)");
        
        // Equal tests are allowed only for BLOB==BLOB
        assertStatementError("42818", st,
            "select c11 from t1 where cast(x'1111' as "
            + "blob(5))=cast(x'1111' as blob(5))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'1111' as "
            + "blob(5))=cast(x'1111' as blob(7))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'1110' as "
            + "blob(5))=cast(x'1110' as blob(7))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'1111' as "
            + "blob(5))=cast(x'11100000' as blob(7))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'1111' as "
            + "blob(5))=cast(x'1110000000' as blob(7))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where x'11' = cast(x'11' as blob(1))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'11' as blob(1)) = x'11'");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'11' as blob(1)) = "
            + "cast(x'11' as blob(1))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where '1' = cast('1' as clob(1))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast('1' as clob(1)) = '1'");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast('1' as clob(1)) = "
            + "cast('1' as clob(1))");
        
        assertStatementError("0A000", st,
            " select c11 from t1 where '1' = cast('1' as nclob(1))");
        
        assertStatementError("0A000", st,
            " select c11 from t1 where cast('1' as nclob(1)) = '1'");
        
        assertStatementError("0A000", st,
            " select c11 from t1 where cast('1' as nclob(1)) = "
            + "cast('1' as nclob(1))");
        
        //
        // NCLOB is comparable with CLOB
        //
        assertStatementError("0A000", st,
            "select c11 from t1 where cast('1' as nclob(10)) = "
            + "cast('1' as clob(10))");
        
        assertStatementError("0A000", st,
            " select c11 from t1 where cast('1' as clob(10)) = "
            + "cast('1' as nclob(10))");
        
        assertStatementError("42Y55", st, "drop table b");
        assertStatementError("42Y55", st, "drop table c");
        assertStatementError("42Y55", st, "drop table n");
        
        st.executeUpdate("create table b(blob blob(3K))");
        st.executeUpdate("create table c(clob clob(2M))");
        
        assertStatementError("0A000", st, "create table n(nclob nclob(1G))");
        
        st.executeUpdate(
            " insert into b values(cast(X'0031' as blob(3K)))");
        
        st.executeUpdate(
            " insert into c values(cast('2' as clob(2M)))");
        
        assertStatementError("0A000", st,
            " insert into n values(cast('3' as nclob(1G)))");
        
        st.executeUpdate(
            " insert into b values(cast(X'0031' as blob(3K)))");
        
        st.executeUpdate(
            " insert into c values(cast('2' as clob(2M)))");
        
        assertStatementError("0A000", st,
            " insert into n values(cast('3' as nclob(1G)))");
        
        st.executeUpdate(
            " insert into b values(cast(X'0031' as blob(3K)))");
        
        st.executeUpdate(
            " insert into c values(cast('2' as clob(2M)))");
        
        assertStatementError("0A000", st,
            " insert into n values(cast('3' as nclob(1G)))");
        
        rs = st.executeQuery("select blob from b");
        
        expRS = new String [][]
        {
            {"0031"},
            {"0031"},
            {"0031"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery("select clob from c");
        
        expRS = new String [][]
        {
            {"2"},
            {"2"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertStatementError("42X05", st,
            " select nclob from n");
        
        //
        // Comparsion using tables
        //
        assertStatementError("42818", st,
            "select * from b as b1, b as b2 where b1.blob=b2.blob");
        
        assertStatementError("42818", st,
            " select * from b as b1, b as b2 where b1.blob!=b2.blob");
        
        assertStatementError("42818", st,
            " select * from b as b1, b as b2 where b1.blob=x'0001'");
        
        assertStatementError("42818", st,
            " select * from b as b1, b as b2 where x'0001'=b1.blob");
        
        assertStatementError("42818", st,
            " select * from b as b1, b as b2 where x'0001'!=b1.blob");
        
        assertStatementError("42818", st,
            " select * from b as b1, b as b2 where b1.blob=X'7575'");
        
        assertStatementError("42818", st,
            " select * from b as b1, b as b2 where X'7575'=b1.blob");
        
        assertStatementError("42818", st,
            " select c.clob from c where c.clob = '2'");
        
        assertStatementError("42X05", st,
            " select n.nclob from n where n.nclob = '3'");
        
        //
        // ORDER tests on LOB types (not allowed)
        //
        assertStatementError("42818", st,
            "select c11 from t1 where cast(x'1111' as "
            + "blob(5))=cast(x'1111' as blob(5))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'1111' as "
            + "blob(5))!=cast(x'1111' as blob(5))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'1111' as "
            + "blob(5))<cast(x'1111' as blob(5))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'1111' as "
            + "blob(5))>cast(x'1111' as blob(7))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'1111' as "
            + "blob(5))<=cast(x'1110' as blob(7))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast(x'1111' as "
            + "blob(5))>=cast(x'11100000' as blob(7))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast('fish' as "
            + "clob(5))=cast('fish' as clob(5))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast('fish' as "
            + "clob(5))!=cast('fish' as clob(5))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast('fish' as "
            + "clob(5))<cast('fish' as clob(5))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast('fish' as "
            + "clob(5))>cast('fish' as clob(7))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast('fish' as "
            + "clob(5))<=cast('fish' as clob(7))");
        
        assertStatementError("42818", st,
            " select c11 from t1 where cast('fish' as "
            + "clob(5))>=cast('fish' as clob(7))");
        
        assertStatementError("0A000", st,
            " select c11 from t1 where cast('fish' as "
            + "nclob(5))=cast('fish' as nclob(5))");
        
        assertStatementError("0A000", st,
            " select c11 from t1 where cast('fish' as "
            + "nclob(5))!=cast('fish' as nclob(5))");
        
        assertStatementError("0A000", st,
            " select c11 from t1 where cast('fish' as "
            + "nclob(5))<cast('fish' as nclob(5))");
        
        assertStatementError("0A000", st,
            " select c11 from t1 where cast('fish' as "
            + "nclob(5))>cast('fish' as nclob(7))");
        
        assertStatementError("0A000", st,
            " select c11 from t1 where cast('fish' as "
            + "nclob(5))<=cast('fish' as nclob(7))");
        
        assertStatementError("0A000", st,
            " select c11 from t1 where cast('fish' as "
            + "nclob(5))>=cast('fish' as nclob(7))");
        
        //
        // BIT STRING literal is not allowed in DB2
        //
        assertStatementError("42X01", st,
            "values cast(B'1' as blob(10))");
        
        rollback();
        st.close();
    }
}
