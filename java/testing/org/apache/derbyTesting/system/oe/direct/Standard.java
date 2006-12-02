/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.direct.Standard
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.system.oe.direct;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derbyTesting.system.oe.client.Display;
import org.apache.derbyTesting.system.oe.client.Operations;

/**
 * Implement the transactions following the TPC-C specification
 * using client side prepared statements. Thus all the logic
 * is contained within this class. The client, through this
 * object, holds onto PreparedStatements for all the SQL
 * for its lifetime.
 * <P>
 * This standard implementation is based upon the sample
 * programs in the appendix of the TPC-C specification.
 * <P>
 * More specific direct (client side) implementations
 * could extend this class overriding methods as needed.
 */
public class Standard implements Operations {
    
    private final Connection conn;

    /**
     * Create an instance of this implementation.
     * Connection will be set to non auto commit
     * mode and SERIZIALZABLE isolation.
     */
    public Standard(Connection conn) throws SQLException
    {
        this.conn = conn;
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }
    
    /**
     * Prepare all statements as forward-only, read-only, close at commit.
     */
    PreparedStatement prepare(String sql) throws SQLException
    {
        return conn.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }
    
    /*
     *  Stock Level transaction.
     *  Described in section 2.8.2.
     *  SQL based upon sample prgram in appendix A.5.
     */
    
    private PreparedStatement sl1;
    private PreparedStatement sl2;
    
    public void setupStockLevel() throws Exception {
        sl1 = prepare(
            "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?");
        
        sl2 = prepare(
            "SELECT COUNT(DISTINCT(S_I_ID)) FROM ORDERLINE, STOCK " +
            "WHERE OL_W_ID = ? AND OL_D_ID = ? " +
            "AND OL_O_ID < ? AND OL_O_ID >= ? " +
            "AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?");
    }
    
    public void stockLevel(Display display, Object displayData, short w,
            short d, int threshold) throws Exception {
        
        int isolation = conn.getTransactionIsolation();

        int level;
        try {

            try {

                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                         
                sl1.setShort(1, w);
                sl1.setShort(2, d);

                sl2.setShort(1, w);
                sl2.setShort(2, d);

                sl2.setShort(5, w);
                sl2.setInt(6, threshold);

                ResultSet rs = sl1.executeQuery();

                rs.next();
                int nextOrder = rs.getInt(1);
                rs.close();

                sl2.setInt(3, nextOrder);
                sl2.setInt(4, nextOrder - 20);

                rs = sl2.executeQuery();
                rs.next();
                level = rs.getInt(1);
                rs.close();

                conn.commit();
            } finally {
                conn.setTransactionIsolation(isolation);
            }

        } catch (SQLException sqle) {

            conn.rollback();
            throw sqle;
        }

        if (display != null)
            display.displayStockLevel(displayData, w, d, threshold, level);
    }
    
    /*
     * Order Status transaction.
     */

    public void setupOrderStatus() throws Exception {
        // TODO Auto-generated method stub

    }
    public void orderStatus(Display display, Object displayData, short w,
            short d, String customerLast) throws Exception {
        // TODO Auto-generated method stub

    }

    public void orderStatus(Display display, Object displayData, short w,
            short d, int c) throws Exception {
        // TODO Auto-generated method stub

    }

    public void setupPayment() throws Exception {
        // TODO Auto-generated method stub

    }

    public void payment(Display display, Object displayData, short w, short d,
            short cw, short cd, String customerLast, String amount)
            throws Exception {
        // TODO Auto-generated method stub

    }

    public void payment(Display display, Object displayData, short w, short d,
            short cw, short cd, int c, String amount) throws Exception {
        // TODO Auto-generated method stub

    }
    
    public void setupNewOrder() throws Exception {
        // TODO Auto-generated method stub

    }
    public void newOrder(Display display, Object displayData, short w, short d,
            int c, int[] items, short[] quantities, short[] supplyW)
            throws Exception {
        // TODO Auto-generated method stub

    }
    public void setupScheduleDelivery() throws Exception {
        // TODO Auto-generated method stub

    }
    public void scheduleDelivery(Display display, Object displayData, short w,
            short carrier) throws Exception {
        // TODO Auto-generated method stub

    }
    
    public void setupDelivery() throws Exception {
        // TODO Auto-generated method stub

    }
    public int delivery() throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }
}
