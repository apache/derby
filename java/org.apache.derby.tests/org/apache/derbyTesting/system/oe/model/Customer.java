/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.model.Customer
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
package org.apache.derbyTesting.system.oe.model;

import java.sql.Timestamp;

/**
 * An Order Entry customer.
 * <P>
 * Fields map to definition in TPC-C for the CUSTOMER table.
 * The Java names of fields do not include the C_ prefix
 * and are in lower case.
 * <BR>
 * For clarity these fields are renamed in Java
 * <UL>
 * <LI>w_id =&gt; warehouse (SQL column C_W_ID)
 * <LI>d_id =&gt; district (SQL column C_D_ID)
 * </UL>
 * <BR>
 * The columns that map to an address are extracted out as
 * a Address object with the corresponding Java field address.
 * <BR>
 * All fields have Java bean setters and getters.
 * <BR>
 * Fields that are DECIMAL in the database map to String in Java
 * (rather than BigDecimal) to allow running on J2ME/CDC/Foundation.
 * <P>
 * Primary key maps to {warehouse,district,id}.
 * <P>
 * A Customer object may sparsely populated, when returned from a
 * business transaction it is only guaranteed to contain  the information
 * required to display the result of that transaction.
 * 
 */
public class Customer {
    
    private short warehouse;  
    private short district;
    private int id;
    
    private String first;
    private String middle;
    private String last;   
    private Address address;
    private String phone;
    private Timestamp since;
    private String credit;
    private String credit_lim;
    private String discount;
    private String balance;
    private String ytd_payment;
    private int payment_cnt;
    private int delivery_cnt;
    private String data;
    
    /**
     * Clear all information to allow object re-use.
     */
    public void clear()
    {
        warehouse = district = 0;
        id = 0;     
        first = middle = last = null;
        address = null;     
        phone = null;       
        since = null;       
        credit = credit_lim = discount = null;      
        ytd_payment = null;     
        payment_cnt = delivery_cnt = 0; 
        data = null;
    }
    
    public Address getAddress() {
        return address;
    }
    public void setAddress(Address address) {
        this.address = address;
    }
    public String getBalance() {
        return balance;
    }
    public void setBalance(String balance) {
        this.balance = balance;
    }
    public String getCredit() {
        return credit;
    }
    public void setCredit(String credit) {
        this.credit = credit;
    }
    public String getCredit_lim() {
        return credit_lim;
    }
    public void setCredit_lim(String credit_lim) {
        this.credit_lim = credit_lim;
    }
    public String getData() {
        return data;
    }
    public void setData(String data) {
        this.data = data;
    }
    public int getDelivery_cnt() {
        return delivery_cnt;
    }
    public void setDelivery_cnt(int delivery_cnt) {
        this.delivery_cnt = delivery_cnt;
    }
    public String getDiscount() {
        return discount;
    }
    public void setDiscount(String discount) {
        this.discount = discount;
    }
    public short getDistrict() {
        return district;
    }
    public void setDistrict(short district) {
        this.district = district;
    }
    public String getFirst() {
        return first;
    }
    public void setFirst(String first) {
        this.first = first;
    }
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getLast() {
        return last;
    }
    public void setLast(String last) {
        this.last = last;
    }
    public String getMiddle() {
        return middle;
    }
    public void setMiddle(String middle) {
        this.middle = middle;
    }
    public int getPayment_cnt() {
        return payment_cnt;
    }
    public void setPayment_cnt(int payment_cnt) {
        this.payment_cnt = payment_cnt;
    }
    public String getPhone() {
        return phone;
    }
    public void setPhone(String phone) {
        this.phone = phone;
    }
    public Timestamp getSince() {
        return since;
    }
    public void setSince(Timestamp since) {
        this.since = since;
    }
    public short getWarehouse() {
        return warehouse;
    }
    public void setWarehouse(short warehouse) {
        this.warehouse = warehouse;
    }
    public String getYtd_payment() {
        return ytd_payment;
    }
    public void setYtd_payment(String ytd_payment) {
        this.ytd_payment = ytd_payment;
    }
    
}

