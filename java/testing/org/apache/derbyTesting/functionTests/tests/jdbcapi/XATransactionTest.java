/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.XATransactionTest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.client.ClientXid;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/** The test of the properties of the XA transaction interface implementation.
  */
public class XATransactionTest extends BaseJDBCTestCase {

    public void testGlobalXIDinTransactionTable() throws Exception {
        Statement stm = getConnection().createStatement();
        stm.execute("create table XATT2 (i int, text char(10))");

        XADataSource xaDataSource = J2EEDataSource.getXADataSource();
        XAConnection xaConn = xaDataSource.getXAConnection();
        XAResource xaRes = xaConn.getXAResource();
        Connection conn = xaConn.getConnection();

        // create large enough xid
        byte[] gid = new byte[64];
        byte[] bid = new byte[64];
        for (int i=0; i < 64; i++) {
            gid[i] = (byte) i;
            bid[i] = (byte) (64 - i);
        }
        Xid xid = new ClientXid(0x1234, gid, bid);

        // get the stuff required to execute the global transaction
        xaConn = xaDataSource.getXAConnection();
        xaRes = xaConn.getXAResource();
        conn = xaConn.getConnection();

        // start the transaction with that xid
        xaRes.start(xid, XAResource.TMNOFLAGS);

        // do some work
        stm = conn.createStatement();
        stm.execute("insert into XATT2 values (1234, 'Test_Entry')");
        stm.close();

        // end the wotk on the transaction branch
        xaRes.end(xid, XAResource.TMSUCCESS);

        ResultSet rs = null;
        stm = null;

        try {

            // check the output of the global xid in syscs_diag.transaction_table
            stm = getConnection().createStatement();

            String query = "select global_xid from syscs_diag.transaction_table"
                         + " where global_xid is not null";

            // execute the query to obtain the xid of the global transaction
            rs = stm.executeQuery(query);

            // there should be at least one globaltransaction in progress
            Assert.assertTrue(rs.next());

            // check whether the xid obtained matches the original xid
            Xid rXid = parseXid(rs.getString(1));
            Assert.assertEquals(xid, rXid);

            // there should be at most one global transaction in progress
            Assert.assertFalse(rs.next());

        } catch (Exception ex) {
            try {
                // close all the stuff
                if (rs != null)
                    rs.close();
                if (stm != null)
                    stm.close();

                // rollback the global transaction
                xaRes.rollback(xid);
                // close the connection
                xaConn.close();
            } catch (Exception e) {
                // ignore the exception because it
                // would hide the original exception
            }
            // throw the stuff further
            throw ex;
        }

        // close all the stuff
        rs.close();
        stm.close();

        // rollback the global transaction
        xaRes.rollback(xid);

        // close the connection
        xaConn.close();
    }

    /* ------------------- end helper methods  -------------------------- */

    /** Parses the xid value from the string. The format of the input string is
      * the same as the global_xid column in syscs_diag.transaction_table table -
      * '(formatid_in_dec,global_transaction_id_in_hex,branch_qualifier_in_hex)'
      * @param str Global Transaction ID converted to a string.
      * @return The xid object corresponding to the xid specified in a string.
      */
    private static Xid parseXid(String str) {
        Assert.assertNotNull(str);
        Assert.assertTrue(str.matches("\\(\\p{Digit}+,\\p{XDigit}+,\\p{XDigit}+\\)"));

        String formatIdS = str.substring(1, str.indexOf(','));
        String gtidS = str.substring(str.indexOf(',')+1, str.lastIndexOf(','));
        String bqualS = str.substring(str.lastIndexOf(',')+1, str.length()-1);

        Assert.assertTrue(gtidS.length() % 2 == 0);
        Assert.assertTrue(bqualS.length() % 2 == 0);

        int fmtid = Integer.parseInt(formatIdS);
        byte[] gtid = new byte[gtidS.length()/2];
        byte[] bqual = new byte[bqualS.length()/2];

        for (int i=0; i < gtid.length; i++) {
            gtid[i] = (byte) Integer.parseInt(gtidS.substring(2*i, 2*i + 2), 16);
        }

        for (int i=0; i < bqual.length; i++) {
            bqual[i] = (byte) Integer.parseInt(bqualS.substring(2*i, 2*i + 2), 16);
        }

        // Using ClientXid is ok also for embedded driver
        // since it does not contain any related code
        // and there is no implementation of Xid iterface
        // for embedded driver
        return new ClientXid(fmtid, gtid, bqual);
    }

    public XATransactionTest(String name) {
        super(name);
    }

    public static Test suite() {
        Test test;
        TestSuite suite = new TestSuite("XATransactionTest");

        test = TestConfiguration.defaultSuite(XATransactionTest.class);
        suite.addTest(test);

        return suite;
    }
}
