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

import org.apache.derbyTesting.system.oe.client.Operations;

/**
 * Test an implementation of Operations.
 * Currently just tests the setup but as more
 * code is added the implemetations of the transactions
 * will be added.
 */
class OperationsTester  {

    private final Operations ops;
    
    OperationsTester(Operations ops) {
        this.ops = ops;
    }
    
    void test() throws Exception
    {
        testStockLevel();
        testOrderStatus();
        testPayment();
        testNewOrder();
        testScheduleDelivery();
        testDelivery();
    }
    
    private void testStockLevel() throws Exception
    {
        ops.setupStockLevel();
    }
    private void testOrderStatus() throws Exception
    {
        ops.setupOrderStatus();
    }
    private void testPayment() throws Exception
    {
        ops.setupPayment();
    }
    private void testNewOrder() throws Exception
    {
        ops.setupNewOrder();
    }
    private void testScheduleDelivery() throws Exception
    {
        ops.setupScheduleDelivery();
    }
    private void testDelivery() throws Exception
    {
        ops.setupDelivery();
    }
}
