/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.test.OperationsTester
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
package org.apache.derbyTesting.system.oe.test;

import java.util.HashMap;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.system.oe.client.Display;
import org.apache.derbyTesting.system.oe.client.Operations;
import org.apache.derbyTesting.system.oe.direct.Standard;
import org.apache.derbyTesting.system.oe.model.Customer;
import org.apache.derbyTesting.system.oe.model.District;
import org.apache.derbyTesting.system.oe.model.Order;
import org.apache.derbyTesting.system.oe.model.OrderLine;
import org.apache.derbyTesting.system.oe.model.Warehouse;
import org.apache.derbyTesting.system.oe.util.OERandom;

/**
 * Test an implementation of Operations.
 * Currently just tests the setup but as more
 * code is added the implemetations of the transactions
 * will be added.
 */
public class OperationsTester extends BaseJDBCTestCase implements Display {

    private Operations ops;
    private final OERandom rand;
    private final short w = 1;
    
    public OperationsTester(String name) {
        super(name);
        this.rand = new OERandom(-1, -1, -1);
    }
    
    protected void setUp() throws Exception 
    {
        ops = new Standard(getConnection());
    }
    
    protected void tearDown() throws Exception
    {
        ops.close();
        super.tearDown();
    }
    
    public void testStockLevel() throws Exception
    {
        ops.setupStockLevel();
        
        // Check a null display is handled
        ops.stockLevel(null, null,
                w, rand.district(), rand.threshold());
        
        for (int i = 0; i < 20; i++) {
           
            short d = rand.district();
            int threshold = rand.threshold();
            
            HashMap inputData = new HashMap();
            inputData.put("d", new Short(d));
            inputData.put("threshold", new Integer(threshold));
            
            ops.stockLevel(this, inputData,
                    w, d, threshold);
        }
    }
    
    /**
     * Execute a number of order-status transactions
     * by name and identifier. Also check the implementation
     * accepts a null display.
     * @throws Exception
     */
    public void testOrderStatus() throws Exception
    {
        ops.setupOrderStatus();
        
        // By identifier
        ops.orderStatus(null, null,
                w, rand.district(), rand.NURand1023());
        for (int i = 0; i < 50; i++) {
            
            short d = rand.district();
            int c = rand.NURand1023();
            
            HashMap inputData = new HashMap();
            inputData.put("d", new Short(d));
            inputData.put("c", new Integer(c));

            ops.orderStatus(this, inputData, w, d, c);
        }
        
        // By name 
        ops.orderStatus(null, null,
                w, rand.district(), rand.randomCLast());
        for (int i = 0; i < 50; i++)
        {
            short d = rand.district();
            String customerLast = rand.randomCLast();
            
            HashMap inputData = new HashMap();
            inputData.put("d", new Short(d));
            inputData.put("customerLast", customerLast);

            ops.orderStatus(this, inputData, w, d, customerLast);
            
        }
    }
    public void testPayment() throws Exception
    {
        ops.setupPayment();
        
        //  With no display
        ops.payment(null, null, w, rand.district(),
                w, rand.district(), rand.randomCLast(), rand.payment().toString());
        
        for (int i = 0; i < 50; i++) {
            ops.payment(this, null, w, rand.district(),
                    w, rand.district(), rand.randomCLast(), rand.payment().toString());
        }  
        
        // With no display
        ops.payment(null, null, w, rand.district(),
                w, rand.district(), rand.NURand1023(), rand.payment().toString());

        for (int i = 0; i < 50; i++) {
            
            ops.payment(this, null, w, rand.district(),
                    w, rand.district(), rand.NURand1023(), rand.payment().toString());
        }
    }
    public void testNewOrder() throws Exception
    {
        ops.setupNewOrder();
    }
    public void testScheduleDelivery() throws Exception
    {
        ops.setupScheduleDelivery();
        for (int i = 0; i < 50; i++)
            ops.scheduleDelivery(this, null, w, rand.carrier());
    }
    public void testDelivery() throws Exception
    {
        ops.setupDelivery();
        // Ensure there are some schedule deliveries
        testScheduleDelivery();
        for (int i = 0; i < 50; i++)
            ops.delivery();
    }

    public void displayStockLevel(Object displayData, short w, short d, int threshold, int lowStock) throws Exception {
        HashMap inputData = (HashMap) displayData;
        assertEquals("sl:w", this.w, w);
        assertEquals("sl:d", ((Short) inputData.get("d")).shortValue(), d);
        assertEquals("sl:threshold", ((Integer) inputData.get("threshold")).intValue(), threshold);
        assertTrue("sl:low stock", lowStock >= 0); 
    }

    public void displayOrderStatus(Object displayData, boolean byName, Customer customer, Order order, OrderLine[] lineItems) throws Exception {
        HashMap inputData = (HashMap) displayData;
        assertEquals("os:w", this.w, customer.getWarehouse());
        assertEquals("os:d", ((Short) inputData.get("d")).shortValue(), customer.getDistrict());
        
        if (byName)
        {
            assertNotNull(inputData.get("customerLast"));
        }
        else
        {
            assertNull(inputData.get("customerLast"));
        }
    }

    public void displayPayment(Object displayData, String amount, boolean byName, Warehouse warehouse, District district, Customer customer) throws Exception {
        // TODO Auto-generated method stub
        
    }

    public void displayNewOrder(Object displayData, Warehouse warehouse, District district, Customer customer, Order order) throws Exception {
        // TODO Auto-generated method stub
        
    }

    public void displayScheduleDelivery(Object displayData, short w, short carrier) throws Exception {
        // TODO Auto-generated method stub
        
    }
}
