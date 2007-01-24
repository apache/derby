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
import java.util.ArrayList;
import java.util.List;

import org.apache.derbyTesting.system.oe.client.Display;
import org.apache.derbyTesting.system.oe.client.Operations;
import org.apache.derbyTesting.system.oe.model.Customer;
import org.apache.derbyTesting.system.oe.model.Order;
import org.apache.derbyTesting.system.oe.model.OrderLine;

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
            "SELECT COUNT(DISTINCT(S_I_ID)) AS LOW_STOCK FROM ORDERLINE, STOCK " +
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
                int nextOrder = rs.getInt("D_NEXT_O_ID");
                rs.close();

                sl2.setInt(3, nextOrder);
                sl2.setInt(4, nextOrder - 20);

                rs = sl2.executeQuery();
                rs.next();
                level = rs.getInt("LOW_STOCK");
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
    
    private PreparedStatement osCustomerById;
    private PreparedStatement osLastOrderNumber;
    private PreparedStatement osOrderDetails;
    private PreparedStatement osOrderLineItems;
    
    private PreparedStatement osCustomerByName;

    public void setupOrderStatus() throws Exception {
        osCustomerById = prepare(
                "SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_LAST " +
                "FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
        osLastOrderNumber = prepare(
                "SELECT MAX(O_ID) AS LAST_ORDER FROM ORDERS " +
                "WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ?");
        osOrderDetails = prepare(
                "SELECT O_ENTRY_D, O_CARRIER_ID, O_OL_CNT " +
                "FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_ID = ?");
        osOrderLineItems = prepare(
                "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, " +
                "OL_DELIVERY_D FROM ORDERLINE " +
                "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?");

        osCustomerByName = prepare(
                "SELECT C_ID, C_BALANCE, C_FIRST, C_MIDDLE " +
                "FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? " +
                "ORDER BY C_FIRST");

    }
    
    /**
     * Order status by customer last name.
     * Based up the example SQL queries in appendix A.3
     */
    public void orderStatus(Display display, Object displayData, short w,
            short d, String customerLast) throws Exception {
        
       
        try {
            osCustomerByName.setShort(1, w);
            osCustomerByName.setShort(2, d);
            osCustomerByName.setString(3, customerLast);
            ResultSet rs = osCustomerByName.executeQuery();
            int n = 0;
            List list = new ArrayList();
            while (rs.next())
            {
                Customer customer = new Customer();
                customer.setWarehouse(w);
                customer.setDistrict(d);
                customer.setLast(customerLast);
                
                customer.setId(rs.getInt("C_ID"));
                customer.setBalance(rs.getString("C_BALANCE"));
                customer.setFirst(rs.getString("C_FIRST"));
                customer.setMiddle(rs.getString("C_MIDDLE"));
                
                list.add(customer);
            }
            rs.close();
            if (list.isEmpty())
                throw new SQLException("Order Status by name - no matching customer "
                        + customerLast);
            
            // Customer to use is midpoint (with round up) (see 2.6.2.2)
            int mid = n/2;
            if (n%2 == 1)
                mid++;


            Customer customer = (Customer) list.get(mid);
            list = null;
            
            getOrderStatusForCustomer(display, displayData, true, customer);
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    /**
     * Order status by customer identifier.
     * Based up the example SQL queries in appendix A.3
     */
    public void orderStatus(Display display, Object displayData, short w,
            short d, int c) throws Exception {
        
        Customer customer = new Customer();
        customer.setWarehouse(w);
        customer.setDistrict(d);
        customer.setId(c);
        
        try {
            // Get the customer information
            osCustomerById.setShort(1, w);
            osCustomerById.setShort(2, d);
            osCustomerById.setInt(3, c);
            ResultSet rs = osCustomerById.executeQuery();
            rs.next();
            customer.setBalance(rs.getString("C_BALANCE"));
            customer.setFirst(rs.getString("C_FIRST"));
            customer.setMiddle(rs.getString("C_MIDDLE"));
            customer.setLast(rs.getString("C_LAST"));    
            rs.close();

            getOrderStatusForCustomer(display, displayData, false, customer);
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }
    
    /**
     * Fetch the order details having obtained the customer information
     * and display it.
     */
    private void getOrderStatusForCustomer(Display display, Object displayData,
            boolean byName, Customer customer) throws Exception
    {
        Order order = new Order();
        order.setWarehouse(customer.getWarehouse());
        order.setDistrict(customer.getDistrict());
        
        // Find the most recent order number for this customer
        osLastOrderNumber.setShort(1, customer.getWarehouse());
        osLastOrderNumber.setShort(2, customer.getDistrict());
        osLastOrderNumber.setInt(3, customer.getId());
        ResultSet rs = osLastOrderNumber.executeQuery();
        rs.next();
        order.setId(rs.getInt("LAST_ORDER"));
        rs.close();
        
        // Details for the order.
        osOrderDetails.setShort(1, customer.getWarehouse());
        osOrderDetails.setShort(2, customer.getDistrict());
        osOrderDetails.setInt(3, order.getId());
        rs = osOrderDetails.executeQuery();
        rs.next();
        order.setEntry_d(rs.getTimestamp("O_ENTRY_D"));
        order.setCarrier_id((Integer) rs.getObject("O_CARRIER_ID"));
        order.setOl_cnt(rs.getInt("O_OL_CNT"));
        rs.close();

        OrderLine[] lineItems = new OrderLine[order.getOl_cnt()];
        
        osOrderLineItems.setShort(1, order.getWarehouse());
        osOrderLineItems.setShort(2, order.getDistrict());
        osOrderLineItems.setInt(3, order.getId());
        rs = osOrderLineItems.executeQuery();
        int oli = 0;
        while (rs.next())
        {
            OrderLine ol = new OrderLine();
            ol.setI_id(rs.getInt("OL_I_ID"));
            ol.setSupply_w_id(rs.getShort("OL_SUPPLY_W_ID"));
            ol.setQuantity(rs.getShort("OL_QUANTITY"));
            ol.setAmount(rs.getString("OL_AMOUNT"));
            ol.setDelivery_d( rs.getTimestamp("OL_DELIVERY_D"));
            
            lineItems[oli++] = ol;
        }
        rs.close();
        conn.commit();
        
        if (display != null)
            display.displayOrderStatus(displayData,
                    byName, customer, order, lineItems);
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

    public void close() throws SQLException {
        
        close(sl1);
        close(sl2);
        
        close(osCustomerById);
        close(osCustomerByName);
        close(osLastOrderNumber);
        close(osOrderDetails);
        close(osOrderLineItems);
        
    }
    private static void close(PreparedStatement ps)
       throws SQLException
    {
        if (ps != null)
            ps.close();
    }
}
