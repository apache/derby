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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.derbyTesting.system.oe.client.Display;
import org.apache.derbyTesting.system.oe.client.Operations;
import org.apache.derbyTesting.system.oe.model.Customer;
import org.apache.derbyTesting.system.oe.model.District;
import org.apache.derbyTesting.system.oe.model.Order;
import org.apache.derbyTesting.system.oe.model.OrderLine;
import org.apache.derbyTesting.system.oe.model.Warehouse;

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
 * <P>
 * Object is single threaded so it re-uses objects
 * where possible to avoid the garbage collection
 * due to the application affecting the results
 * too much since the purpose of the framework
 * is to test Derby's performance.
 */
public class Standard extends StatementHelper implements Operations {
    
    /*
     * Objects for re-use within the transactions
     */
    private final Customer customer = new Customer();
    
    private final Warehouse warehouse = new Warehouse();
    
    private final District district = new District();
    
    private final Order order = new Order();
    
    private final List<Object> nameList = new ArrayList<Object>();


    /**
     * Create an instance of this implementation.
     * Connection will be set to non auto commit
     * mode and SERIZIALZABLE isolation.
     */
    public Standard(Connection conn) throws SQLException
    {
        super(conn, false, Connection.TRANSACTION_SERIALIZABLE);
    }
    
    /**
     * Return an Operations implementation based upon
     * Standard with a single difference. In this implementation
     * the reset() executed after each PreparedStatement execute
     * does nothing. Sees if there is any performance impact
     * of explicitly closing each ResultSet and clearing the
     * parameters.
     * <P>
     * Each ResultSet will be closed implicitly either at commit
     * time or at the next execution of the same PreparedStatement object.
     */
    public static Operations noReset(final Connection conn)
        throws SQLException
    {
        return new Standard(conn) {
            protected void reset(PreparedStatement ps) {}
        };
    }
    
    /**
     *  Stock Level transaction.
     *  Described in section 2.8.2.
     *  SQL based upon sample prgram in appendix A.5.
     */
    public void stockLevel(Display display, Object displayData, short w,
            short d, int threshold) throws Exception {
        
        int isolation = conn.getTransactionIsolation();

        int lowStock;
        try {

            try {

                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                
                PreparedStatement sl1 = prepareStatement(
                        "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?");

                PreparedStatement sl2 = prepareStatement(
                        "SELECT COUNT(DISTINCT(S_I_ID)) AS LOW_STOCK FROM ORDERLINE, STOCK " +
                        "WHERE OL_W_ID = ? AND OL_D_ID = ? " +
                        "AND OL_O_ID < ? AND OL_O_ID >= ? " +
                        "AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?");
                         
                sl1.setShort(1, w);
                sl1.setShort(2, d);

                sl2.setShort(1, w);
                sl2.setShort(2, d);

                sl2.setShort(5, w);
                sl2.setInt(6, threshold);

                ResultSet rs = sl1.executeQuery();

                rs.next();
                int nextOrder = rs.getInt("D_NEXT_O_ID");
                reset(sl1);

                sl2.setInt(3, nextOrder);
                sl2.setInt(4, nextOrder - 20);

                rs = sl2.executeQuery();
                rs.next();
                lowStock = rs.getInt("LOW_STOCK");
                reset(sl2);

                conn.commit();
            } finally {
                conn.setTransactionIsolation(isolation);
            }

        } catch (SQLException sqle) {

            conn.rollback();
            conn.setTransactionIsolation(isolation);
            throw sqle;
        }

        if (display != null)
            display.displayStockLevel(displayData, w, d, threshold, lowStock);
    }
       
    /**
     * Order status by customer last name.
     * Based up the example SQL queries in appendix A.3
     */
    public void orderStatus(Display display, Object displayData, short w,
            short d, String customerLast) throws Exception {
        
        PreparedStatement osCustomerByName = prepareStatement(
                "SELECT C_ID, C_BALANCE, C_FIRST, C_MIDDLE " +
                "FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? " +
                "ORDER BY C_FIRST");
        
       
        try {
            osCustomerByName.setShort(1, w);
            osCustomerByName.setShort(2, d);
            osCustomerByName.setString(3, customerLast);
            ResultSet rs = osCustomerByName.executeQuery();

            nameList.clear();
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
                
                nameList.add(customer);
            }
            reset(osCustomerByName);
            if (nameList.isEmpty())
                throw new SQLException("Order Status by name - no matching customer "
                        + customerLast);
            
            // Customer to use is midpoint (with round up) (see 2.6.2.2)
            int mid = nameList.size()/2;
            if (mid != 0) {
                if (nameList.size()%2 == 1)
                    mid++;
            }


            Customer customer = (Customer) nameList.get(mid);
            nameList.clear();
            
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
        
        PreparedStatement osCustomerById = prepareStatement(
                "SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_LAST " +
                "FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
        
        customer.clear();
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
            reset(osCustomerById);

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
        PreparedStatement osLastOrderNumber = prepareStatement(
                "SELECT MAX(O_ID) AS LAST_ORDER FROM ORDERS " +
                "WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ?");
        PreparedStatement osOrderDetails = prepareStatement(
                "SELECT O_ENTRY_D, O_CARRIER_ID, O_OL_CNT " +
                "FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_ID = ?");
        PreparedStatement osOrderLineItems = prepareStatement(
                "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, " +
                "OL_DELIVERY_D FROM ORDERLINE " +
                "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?");
        
        order.clear();
        order.setWarehouse(customer.getWarehouse());
        order.setDistrict(customer.getDistrict());
        
        // Find the most recent order number for this customer
        osLastOrderNumber.setShort(1, customer.getWarehouse());
        osLastOrderNumber.setShort(2, customer.getDistrict());
        osLastOrderNumber.setInt(3, customer.getId());
        ResultSet rs = osLastOrderNumber.executeQuery();
        rs.next();
        order.setId(rs.getInt("LAST_ORDER"));
        reset(osLastOrderNumber);
        
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
    
    /**
     * Payment by customer last name.
     * Section 2.5.2
     * The CUSTOMER row will be fetched and then updated.
     * This is due to the need to select the specific customer
     * first based upon last name (which will actually fetch and
     * hence lock a number of customers).
     */
    public void payment(Display display, Object displayData, short w, short d,
            short cw, short cd, String customerLast, String amount)
            throws Exception {
            
        PreparedStatement pyCustomerByName = prepareStatement(
                    "SELECT C_ID " +
                    "FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? " +
                    "ORDER BY C_FIRST");
        
        // Since so much data is needed for the payment transaction
        // from the customer we don't fill it in as we select the
        // correct customer. Instead we just fetch the identifier
        // and then execute a payment by identifier.
        try {
            pyCustomerByName.setShort(1, cw);
            pyCustomerByName.setShort(2, cd);
            pyCustomerByName.setString(3, customerLast);
            ResultSet rs = pyCustomerByName.executeQuery();

            nameList.clear();
            while (rs.next())
            {           
                nameList.add(rs.getObject("C_ID"));            
            }
            reset(pyCustomerByName);
            if (nameList.isEmpty())
                throw new SQLException("Payment by name - no matching customer "
                        + customerLast);
            
            // Customer to use is midpoint (with round up) (see 2.5.2.2)
            int mid = nameList.size()/2;
            if (mid != 0) {
                if (nameList.size()%2 == 1)
                    mid++;
            }
            
            int c = ((Integer) nameList.get(mid)).intValue();

            paymentById(display, displayData, w, d, cw, cd, c, amount);
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
        
        if (display != null)
            ;
    }

    /**
     * Payment by customer identifier.
     * Section 2.5.2.
     * The CUSTOMER row is update and then fetched.
     * 
     */
    public void payment(Display display, Object displayData, short w, short d,
            short cw, short cd, int c, final String amount) throws Exception {
        
        try {
            paymentById(display, displayData, w, d, cw, cd, c, amount);
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
        
        if (display != null)
            ;
    }
    
    private void paymentById(Display display, Object displayData, short w, short d,
            short cw, short cd, int c, final String amount) throws Exception {

        PreparedStatement pyCustomerPayment = prepareStatement(
                "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE - ?, " +
                "C_YTD_PAYMENT = C_YTD_PAYMENT + ?, " +
                "C_PAYMENT_CNT = C_PAYMENT_CNT + 1 " +
                "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
            
        PreparedStatement pyCustomerInfoId = prepareStatement(
                "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, " +
                "C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, " +
                "C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT " +
                "FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
        
        PreparedStatement pyCustomerUpdateBadCredit = prepareStatement(
                "UPDATE CUSTOMER SET C_DATA = " +
                " BAD_CREDIT_DATA(C_DATA, ?, ?, C_W_ID, C_W_ID, C_ID, ?) " +
                "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
        PreparedStatement pyCustomerGetData = prepareStatement(
                "SELECT SUBSTR(C_DATA, 1, 200) AS C_DATA_200 " +
                "FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
            
        PreparedStatement pyDistrictUpdate = prepareStatement(
                "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?");
        PreparedStatement pyDistrictInfo = prepareStatement(
                "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ? ");
        PreparedStatement pyWarehouseUpdate = prepareStatement(
                "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?");
        PreparedStatement pyWarehouseInfo = prepareStatement(
                    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP " +
                    "FROM WAREHOUSE WHERE W_ID = ?");
            
        PreparedStatement pyHistory = prepareStatement(
                "INSERT INTO HISTORY(H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, " +
                "H_DATE, H_AMOUNT, H_DATA) " +
                "VALUES (?, ?, ?, ?, ?, CURRENT TIMESTAMP, ?, ?)");
        
        Customer customer = new Customer();
        customer.setWarehouse(cw);
        customer.setDistrict(cd);
        customer.setId(c);
        
        // Update the customer assuming that they have good credit
        pyCustomerPayment.setString(1, amount);
        pyCustomerPayment.setString(2, amount);
        pyCustomerPayment.setShort(3, cw);
        pyCustomerPayment.setShort(4, cd);
        pyCustomerPayment.setInt(5, c);
        pyCustomerPayment.executeUpdate();
        
        // Get the customer information
        pyCustomerInfoId.setShort(1, cw);
        pyCustomerInfoId.setShort(2, cd);
        pyCustomerInfoId.setInt(3, c);
        ResultSet rs = pyCustomerInfoId.executeQuery();
        rs.next();
        
        customer.setFirst(rs.getString("C_FIRST"));
        customer.setMiddle(rs.getString("C_MIDDLE"));
        customer.setLast(rs.getString("C_LAST"));
        customer.setBalance(rs.getString("C_BALANCE"));
        
        customer.setAddress(getAddress(rs, "C_STREET_1"));

        customer.setPhone(rs.getString("C_PHONE"));
        customer.setSince(rs.getTimestamp("C_SINCE"));
        customer.setCredit(rs.getString("C_CREDIT"));
        customer.setCredit_lim(rs.getString("C_CREDIT_LIM"));
        customer.setDiscount(rs.getString("C_DISCOUNT"));
        reset(pyCustomerInfoId);
        
        // additional work for bad credit customers.
        if ("BC".equals(customer.getCredit()))
        {
            pyCustomerUpdateBadCredit.setShort(1, w);
            pyCustomerUpdateBadCredit.setShort(2, d);
            pyCustomerUpdateBadCredit.setString(3, amount);
            pyCustomerUpdateBadCredit.setShort(4, cw);
            pyCustomerUpdateBadCredit.setShort(5, cd);
            pyCustomerUpdateBadCredit.setInt(6, c);         
            pyCustomerUpdateBadCredit.executeUpdate();
            reset(pyCustomerUpdateBadCredit);
            
            // Need to display the first 200 characters
            // of C_DATA information if the customer has
            // bad credit.
            pyCustomerGetData.setShort(1, cw);
            pyCustomerGetData.setShort(2, cd);
            pyCustomerGetData.setInt(3, c);                     
            rs = pyCustomerGetData.executeQuery();
            rs.next();
            customer.setData(rs.getString("C_DATA_200"));
            reset(pyCustomerGetData);
        }

        district.clear();
        district.setWarehouse(w);
        district.setId(d);

        // Update DISTRICT
        pyDistrictUpdate.setString(1, amount);
        pyDistrictUpdate.setShort(2, w);
        pyDistrictUpdate.setShort(3, d);
        pyDistrictUpdate.executeUpdate();
        reset(pyDistrictUpdate);

        // Get the required information from DISTRICT
        pyDistrictInfo.setShort(1, w);
        pyDistrictInfo.setShort(2, d);
        rs = pyDistrictInfo.executeQuery();
        rs.next();
        district.setName(rs.getString("D_NAME"));
        district.setAddress(getAddress(rs, "D_STREET_1"));
        reset(pyDistrictInfo);        
        
        warehouse.clear();
        warehouse.setId(w);
        
        // Update WAREHOUSE
        pyWarehouseUpdate.setString(1, amount);
        pyWarehouseUpdate.setShort(2, w);
        pyWarehouseUpdate.executeUpdate();
        reset(pyWarehouseUpdate);
        
        // Get the required information from WAREHOUSE
        pyWarehouseInfo.setShort(1, w);
        rs = pyWarehouseInfo.executeQuery();
        rs.next();
        warehouse.setName(rs.getString("W_NAME"));
        warehouse.setAddress(getAddress(rs, "W_STREET_1"));
        reset(pyWarehouseInfo);
         
        // Insert HISTORY row
        pyHistory.setInt(1, c);
        pyHistory.setShort(2, cd);
        pyHistory.setShort(3, cw);
        pyHistory.setShort(4, d);
        pyHistory.setShort(5, w);
        pyHistory.setString(6, amount);
        StringBuffer hData = new StringBuffer(24);
        hData.append(warehouse.getName());
        hData.append("    ");
        hData.append(district.getName());
        pyHistory.setString(7, hData.toString());
        pyHistory.executeUpdate();
        reset(pyHistory);
        
        conn.commit();
  
    }
    
    private static final String[] STOCK_INFO = {
    "SELECT S_QUANTITY, S_DIST_01, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
    "SELECT S_QUANTITY, S_DIST_02, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
    "SELECT S_QUANTITY, S_DIST_03, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
    "SELECT S_QUANTITY, S_DIST_04, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
    "SELECT S_QUANTITY, S_DIST_05, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
    "SELECT S_QUANTITY, S_DIST_06, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
    "SELECT S_QUANTITY, S_DIST_07, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
    "SELECT S_QUANTITY, S_DIST_08, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
    "SELECT S_QUANTITY, S_DIST_09, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
    "SELECT S_QUANTITY, S_DIST_10, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?",
       };
    
    public void newOrder(Display display, Object displayData, short w, short d,
            int c, int[] items, short[] quantities, short[] supplyW)
            throws Exception {
        
        // This transaction is subject to deadlocks since the
        // stock table is read and then updated, and multiple
        // stock items are read and updated in a random order.
        // to avoid the deadlocks, the items are sorted here.
        // If some engine did not require sorting then it could
        // provide a different implementation of this class with
        // the sort method a no-op.
        sortOrderItems(items, quantities, supplyW);
        
        try {
            // Get the warehouse tax
            PreparedStatement psWarehouseTax = prepareStatement(
                "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?");
            psWarehouseTax.setShort(1, w);
            ResultSet rs = psWarehouseTax.executeQuery();
            rs.next();
            BigDecimal warehouseTax = (BigDecimal) rs.getObject(1);
            reset(psWarehouseTax);
            
            // Get the district tax and order number including the update.            
            PreparedStatement psDistrictUpdate = prepareStatement(
                "UPDATE DISTRICT SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
                "WHERE D_W_ID = ? AND D_ID = ?");
            psDistrictUpdate.setShort(1, w);
            psDistrictUpdate.setShort(2, d);
            psDistrictUpdate.executeUpdate();
            reset(psDistrictUpdate);

            PreparedStatement psDistrict = prepareStatement(
                "SELECT D_NEXT_O_ID - 1, D_TAX " +
                "FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?");
            psDistrict.setShort(1, w);
            psDistrict.setShort(2, d);
            rs = psDistrict.executeQuery();
            rs.next();
            int orderNumber = rs.getInt(1);
            BigDecimal districtTax = (BigDecimal) rs.getObject(2);
            reset(psDistrict);
            
            PreparedStatement psCustomer = prepareStatement(
                "SELECT C_LAST, C_DISCOUNT, C_CREDIT " +
                "FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
            psCustomer.setShort(1, w);
            psCustomer.setShort(2, d);
            psCustomer.setInt(3, c);
            rs = psCustomer.executeQuery();
            rs.next();
            // TODO fetch data
            reset(psCustomer);
            
            // See if all the items are from the local warehouse.
            short allLocal = 1;
            for (int i = 0; i < supplyW.length; i++)
            {
                if (supplyW[i] != w)
                {
                    allLocal = 0;
                    break;
                }
            }
            
            PreparedStatement psOrder = prepareStatement(
                "INSERT INTO ORDERS VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, ?, ?)");
            psOrder.setInt(1, orderNumber);
            psOrder.setShort(2, d);
            psOrder.setShort(3, w);
            psOrder.setInt(4, c);
            psOrder.setShort(5, (short) items.length);
            psOrder.setShort(6, allLocal);
            psOrder.executeUpdate();
            reset(psOrder);

            PreparedStatement psNewOrder = prepareStatement(
                "INSERT INTO NEWORDERS VALUES(?, ?, ?)"); 
            psNewOrder.setInt(1, orderNumber);
            psNewOrder.setShort(2, d);
            psNewOrder.setShort(3, w);
            psNewOrder.executeUpdate();
            reset(psNewOrder);
            
            /*
             * Now all the processing for the order line items.
             */
            PreparedStatement psOrderLine = prepareStatement(
                "INSERT INTO ORDERLINE(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, " +
                "OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO, " +
                "OL_DELIVERY_D) VALUES (?, ?, ?, ?, ?, ?, ?, " +
                "CAST (? AS DECIMAL(5,2)) * CAST (? AS SMALLINT), ?, NULL)");
            
            // These are constant across the order items
            psOrderLine.setShort(1, w);
            psOrderLine.setShort(2, d);
            psOrderLine.setInt(3, orderNumber);
            
            PreparedStatement psItemPrice = prepareStatement(
                    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?");
            
            PreparedStatement psStockUpdate = prepareStatement(
                    "UPDATE STOCK SET S_ORDER_CNT = S_ORDER_CNT + 1, " +
                    "S_YTD = S_YTD + ?, S_REMOTE_CNT = S_REMOTE_CNT + ?, " +
                    "S_QUANTITY = ? WHERE S_I_ID = ? AND S_W_ID = ?");

            for (int i = 0; i < items.length; i++)
            {
                // Item details
                psItemPrice.setInt(1, items[i]);
                rs = psItemPrice.executeQuery();
                rs.next();
                BigDecimal itemPrice = (BigDecimal) rs.getObject(1);
                String itemName = rs.getString(2);
                String itemData = rs.getString(3);
                rs.close();
                
                // SELECT S_QUANTITY, S_DIST_XX, S_DATA FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?
                PreparedStatement psStockInfo = prepareStatement(STOCK_INFO[d-1]);
                psStockInfo.setInt(1, items[i]);
                psStockInfo.setShort(2, w);
                rs = psStockInfo.executeQuery();
                rs.next();
                int stockQuantity = rs.getInt(1);
                String stockDistInfo = rs.getString(2);
                String stockData = rs.getString(3);
                reset(psStockInfo);

                psStockUpdate.setInt(1, quantities[i]);
                psStockUpdate.setInt(2, w == supplyW[i] ? 0 : 1);
                
                if ((stockQuantity - quantities[i]) > 10)
                    stockQuantity -= quantities[i];
                else
                    stockQuantity = (stockQuantity - quantities[i] + 91);
                psStockUpdate.setInt(3, stockQuantity);
                psStockUpdate.setInt(4, items[i]);
                psStockUpdate.setShort(5, w);
                psStockUpdate.executeUpdate();


                psOrderLine.setShort(4, (short) (i + 1));
                psOrderLine.setInt(5, items[i]);
                psOrderLine.setShort(6, supplyW[i]);
                psOrderLine.setShort(7, quantities[i]);
                psOrderLine.setObject(8, itemPrice, Types.DECIMAL);
                psOrderLine.setShort(9, quantities[i]);
                psOrderLine.setString(10, stockDistInfo);
                psOrderLine.executeUpdate();
            }
            
            reset(psOrderLine);
            reset(psItemPrice);
            reset(psOrderLine);
            reset(psStockUpdate);
            
            // get the sum of the order. This is done as a select rather than
            // directly in this code so that all the DECIMAL arithmetic is made
            // using the SQL engine (since this is a test of Derby).
            //

            PreparedStatement psTotal = prepareStatement(
                "SELECT SUM(OL_AMOUNT) FROM ORDERLINE " +
                "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?");

            psTotal.setShort(1, w);
            psTotal.setShort(2, d);
            psTotal.setInt(3, orderNumber);
            rs = psTotal.executeQuery();
            rs.next();
            BigDecimal orderTotal = (BigDecimal) rs.getObject(1);
            reset(psTotal);
 
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
        }
    }
    
    /**
     * Schedule a delivery using the database as the queuing
     * mechanism and the results file.
     * See delivery.sql.
     */
    public void scheduleDelivery(Display display, Object displayData, short w,
            short carrier) throws Exception {
        
        PreparedStatement sdSchedule = prepareStatement(
                "INSERT INTO DELIVERY_REQUEST(DR_W_ID, DR_CARRIER_ID, DR_STATE) " +
                "VALUES(?, ?, 'Q')");
        
        int isolation = conn.getTransactionIsolation(); 
        try {

            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            
            sdSchedule.setShort(1, w);
            sdSchedule.setShort(2, carrier);
            sdSchedule.executeUpdate();
            reset(sdSchedule);
            conn.commit();
        } finally {
            conn.setTransactionIsolation(isolation);
        }
        
        if (display != null)
            display.displayScheduleDelivery(displayData, w, carrier);
    }
    
    public void delivery() throws Exception {
        
        PreparedStatement dlFindOldestRequest = prepareStatement(
                "SELECT DR_ID, DR_W_ID, DR_CARRIER_ID FROM DELIVERY_REQUEST " +
                "WHERE DR_STATE = 'Q' ORDER BY DR_QUEUED");
            dlFindOldestRequest.setMaxRows(1);
            
            PreparedStatement dlSetRequestState = prepareStatement(
                "UPDATE DELIVERY_REQUEST SET DR_STATE = ? " +
                "WHERE DR_ID = ?");
            PreparedStatement dlCompleteDelivery = prepareStatement(
                "UPDATE DELIVERY_REQUEST SET DR_STATE = 'C', DR_COMPLETED = CURRENT TIMESTAMP " +
                "WHERE DR_ID = ?");
            
            PreparedStatement dlFindOrderToDeliver = prepareStatement(
                "SELECT MIN(NO_O_ID) AS ORDER_TO_DELIVER FROM NEWORDERS " +
                "WHERE NO_W_ID = ? AND NO_D_ID = ?");
            
            PreparedStatement dlDeleteNewOrder = prepareStatement(
                "DELETE FROM NEWORDERS WHERE NO_W_ID = ? AND NO_D_ID = ? AND NO_O_ID = ?");
            
            PreparedStatement dlSetOrderCarrier = prepareStatement(
                "UPDATE ORDERS SET O_CARRIER_ID = ? " +
                "WHERE O_W_ID = ? AND O_D_ID = ? AND O_ID = ?");
            
            PreparedStatement dlSetOrderlineDate = prepareStatement(
                "UPDATE ORDERLINE SET OL_DELIVERY_D = CURRENT TIMESTAMP " +
                "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?");
            
            
            PreparedStatement dlUpdateCustomer = prepareStatement(
                "UPDATE CUSTOMER SET " +
                "C_BALANCE = (SELECT SUM(OL_AMOUNT) FROM ORDERLINE " +
                              "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?), " +
                "C_DELIVERY_CNT = C_DELIVERY_CNT + 1 " +
                "WHERE C_W_ID = ? AND C_D_ID = ? AND " +
                "C_ID = (SELECT O_C_ID FROM ORDERS " +
                        "WHERE O_W_ID = ? AND O_D_ID = ? AND O_ID = ?)");
            
            PreparedStatement dlRecordDelivery = prepareStatement(
                "INSERT INTO DELIVERY_ORDERS(DO_DR_ID, DO_D_ID, DO_O_ID) " +
                "VALUES (?, ?, ?)");

        
        // Find the most oldest queued order (FIFO)
        ResultSet rs = dlFindOldestRequest.executeQuery();
        rs.next();
        int request = rs.getInt("DR_ID");
        short w = rs.getShort("DR_W_ID");
        short carrier = rs.getShort("DR_CARRIER_ID");
        reset(dlFindOldestRequest);
        
        // Mark it as in progress
        dlSetRequestState.setString(1, "I");
        dlSetRequestState.setInt(2, request);
        dlSetRequestState.executeUpdate();
        reset(dlSetRequestState);
        
        conn.commit();
        
        // This parameter remains invariant over
        // the batch we will insert.
        dlRecordDelivery.setInt(1, request);
        
        // Process one row per-district for this warehouse
        for (short d = 1; d <= 10; d++)
        {
            dlRecordDelivery.setShort(2, d);

            // Get the oldest NEWORDERS for this district
            dlFindOrderToDeliver.setShort(1, w);
            dlFindOrderToDeliver.setShort(2, d);
            rs = dlFindOrderToDeliver.executeQuery();
            rs.next();
            int order = rs.getInt("ORDER_TO_DELIVER");
            if (rs.wasNull()) {
                // No orders to deliver
                dlRecordDelivery.setNull(3, Types.INTEGER);
                dlRecordDelivery.addBatch();
            }
            reset(dlFindOrderToDeliver);
            
            // Delete the NEWORDERS row
            dlDeleteNewOrder.setShort(1, w);
            dlDeleteNewOrder.setShort(2, d);
            dlDeleteNewOrder.setInt(3, order);
            dlDeleteNewOrder.executeUpdate();
            reset(dlDeleteNewOrder);
            
            // Set the carrier in ORDERS
            dlSetOrderCarrier.setShort(1, carrier);
            dlSetOrderCarrier.setShort(2, w);
            dlSetOrderCarrier.setShort(3, d);
            dlSetOrderCarrier.setInt(4, order);
            dlSetOrderCarrier.executeUpdate();
            reset(dlSetOrderCarrier);
            
            // Update ORDERLINE with the delivery date
            dlSetOrderlineDate.setShort(1, w);
            dlSetOrderlineDate.setShort(2, d);
            dlSetOrderlineDate.setInt(3, order);
            dlSetOrderlineDate.executeUpdate();
            reset(dlSetOrderlineDate);
            
            dlUpdateCustomer.setShort(1, w);
            dlUpdateCustomer.setShort(2, d);
            dlUpdateCustomer.setInt(3, order);
            dlUpdateCustomer.setShort(4, w);
            dlUpdateCustomer.setShort(5, d);
            dlUpdateCustomer.setShort(6, w);
            dlUpdateCustomer.setShort(7, d);
            dlUpdateCustomer.setInt(8, order);
            dlUpdateCustomer.executeUpdate();
            reset(dlUpdateCustomer);
                      
            conn.commit();
            
            dlRecordDelivery.setInt(3, order);
            dlRecordDelivery.addBatch();
        }
        
        // Record the delivery including the timestamp
        // 90% are meant to complete within 80 seconds
        // of them being queued.
        dlRecordDelivery.executeBatch();
        reset(dlRecordDelivery);
        dlCompleteDelivery.setInt(1, request);
        dlCompleteDelivery.executeUpdate();
        reset(dlCompleteDelivery);
        conn.commit();
        
    }

    public void sortOrderItems(int[] items, short[] quantities, short[] supplyW) {

        OrderItem4Sort[] list = new OrderItem4Sort[items.length];

        for (int i = 0; i < items.length; i++)
        {
            list[i] = new OrderItem4Sort(items[i], quantities[i], supplyW[i]);
        }

        java.util.Arrays.sort(list);

        for (int i = 0; i < items.length; i++)
        {
            items[i] = list[i].i;
            quantities[i] = list[i].q;
            supplyW[i] = list[i].w;
        }
    }
}

class OrderItem4Sort implements Comparable {

    final int i;
    final short q;
    final short w;

    OrderItem4Sort(int i, short q, short w)
    {
        this.i = i;
        this.q = q;
        this.w = w;
    }


    public int compareTo(Object o) {

        OrderItem4Sort oo = (OrderItem4Sort) o;

        if (w < oo.w)
            return -1;
        if (w > oo.w)
            return 1;
        if (i < oo.i)
            return -1;
        if (i > oo.i)
            return 1;
        return 0;
    }
}
