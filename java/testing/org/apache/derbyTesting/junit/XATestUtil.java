/*

   Derby - Class org.apache.derby.impl.services.bytecode.CodeChunk

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

package org.apache.derbyTesting.junit;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

public class XATestUtil {
    
    /**
     * Return a new Xid for testing.
    */
    public static Xid getXid(int xid, int b1, int b2) {
        return new utilXid(xid, b1, b2);
    }
    
    /**
     * Dump an unexpected XAException.
     * @param tag Useful info to print
     * @param xae The exception
     */
    public static void dumpXAException(String tag, XAException xae) {

        System.out.println(tag + " : XAException - " + xae.getMessage()
                + " errorCode " + errorCode(xae));
        xae.printStackTrace(System.out);
    }

    /**
     * Create a view that allows useful inspection of the active
     * global transactions.
    */
    public static void createXATransactionView(Statement s) throws SQLException
    {
        s.execute(
                "create view XATESTUTIL.global_xactTable as " +
                "select  cast(global_xid as char(2)) as gxid," +
                " status, " +
                " CAST (case when first_instant is NULL then 'NULL' else 'false' end AS VARCHAR(8)) as readOnly, " +
                " cast (username as char(10)) as username, type " +
                " from syscs_diag.transaction_table");
    }
    
    /**
     * Display the active global transactions.
     * @param conn
     * @throws SQLException
     */
    public static void checkXATransactionView(Connection conn,String[][] expectedRows) throws SQLException
    {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(
                "select * from XATESTUTIL.global_xactTable where gxid is not null order by gxid");
        if (expectedRows == null)
            JDBC.assertEmpty(rs);
        else
            JDBC.assertFullResultSet(rs, expectedRows);
        rs.close();
    }
    
    /**
     * Return a string for the error code of the XAException.
    */
    public static String errorCode(XAException e)
    {
        String error;
        switch(e.errorCode)
        {
        case XAException.XA_HEURCOM : error = "XA_HEURCOM "; break;
        case XAException.XA_HEURHAZ : error = "XA_HEURHAZ"; break;
        case XAException.XA_HEURMIX : error = "XA_HEURMIX"; break;
        case XAException.XA_HEURRB : error = "XA_HEURRB "; break;
        case XAException.XA_NOMIGRATE : error = "XA_NOMIGRATE "; break;
        case XAException.XA_RBCOMMFAIL : error = "XA_RBCOMMFAIL "; break;
        case XAException.XA_RBDEADLOCK : error = "XA_RBDEADLOCK "; break;
        case XAException.XA_RBINTEGRITY : error = "XA_RBINTEGRITY "; break;
        case XAException.XA_RBOTHER : error = "XA_RBOTHER "; break;
        case XAException.XA_RBPROTO : error = "XA_RBPROTO "; break;
        case XAException.XA_RBROLLBACK : error = "XA_RBROLLBACK "; break;
        case XAException.XA_RBTIMEOUT : error = "XA_RBTIMEOUT "; break;
        case XAException.XA_RBTRANSIENT : error = "XA_RBTRANSIENT "; break;
        case XAException.XA_RDONLY : error = "XA_RDONLY "; break;
        case XAException.XA_RETRY : error = "XA_RETRY "; break;
        case XAException.XAER_ASYNC : error = "XAER_ASYNC "; break;
        case XAException.XAER_DUPID : error = "XAER_DUPID "; break;
        case XAException.XAER_INVAL : error = "XAER_INVAL "; break;
        case XAException.XAER_NOTA : error = "XAER_NOTA "; break;
        case XAException.XAER_OUTSIDE : error = "XAER_OUTSIDE "; break;
        case XAException.XAER_PROTO : error = "XAER_PROTO "; break;
        case XAException.XAER_RMERR : error = "XAER_RMERR "; break;
        case XAException.XAER_RMFAIL : error = "XAER_RMFAIL "; break;
        default: error = Integer.toString(e.errorCode); break;
        }        
        return error;
    }
    
}
/**
 * Simple utility class implementation of Xid for tests.
 *
 */
class utilXid implements Xid, Serializable {
    private static final long serialVersionUID = 64467338100036L;

    private final int format_id;

    private byte[] global_id;

    private byte[] branch_id;

    utilXid(int xid, int b1, int b2) {
        format_id = xid;
        global_id = new byte[Xid.MAXGTRIDSIZE];
        branch_id = new byte[Xid.MAXBQUALSIZE];

        for (int i = 0; i < global_id.length; i++) {
            global_id[i] = (byte) (b1 + i);
        }

        for (int i = 0; i < branch_id.length; i++) {
            branch_id[i] = (byte) (b2 + i);
        }
    }

    /**
     * Obtain the format id part of the Xid.
     * <p>
     *
     * @return Format identifier. O means the OSI CCR format.
     **/
    public int getFormatId() {
        return (format_id);
    }

    /**
     * Obtain the global transaction identifier part of XID as an array of 
     * bytes.
     * <p>
     *
     * @return A byte array containing the global transaction identifier.
     **/
    public byte[] getGlobalTransactionId() {
        return (global_id);
    }

    /**
     * Obtain the transaction branch qualifier part of the Xid in a byte array.
     * <p>
     *
     * @return A byte array containing the branch qualifier of the transaction.
     **/
    public byte[] getBranchQualifier() {
        return (branch_id);
    }
}
