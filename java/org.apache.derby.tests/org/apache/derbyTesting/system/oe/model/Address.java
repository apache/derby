/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.model.Address
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

/**
 * Address of a customer, warehouse or district.
 * <P>
 * Fields map to definition in TPC-C for the CUSTOMER,
 * WAREHOUSE and DISTRICT tables.
 * The Java names of fields do not include the C_,W_ or D_ prefixes
 * and are in lower case.
 * <BR>
 * All fields have Java bean setters and getters.
 */
public class Address {
    
    private String street1;
    private String street2;
    private String city;
    private String state;
    private String zip;
    
    /**
     * Reset the fields to allow object re-use.
     *
     */
    public void clear()
    {
        street1 = street2 = city = state = zip = null;
    }

    public String getCity() {
        return city;
    }
    public void setCity(String city) {
        this.city = city;
    }
    public String getState() {
        return state;
    }
    public void setState(String state) {
        this.state = state;
    }
    public String getStreet1() {
        return street1;
    }
    public void setStreet1(String street1) {
        this.street1 = street1;
    }
    public String getStreet2() {
        return street2;
    }
    public void setStreet2(String street2) {
        this.street2 = street2;
    }
    public String getZip() {
        return zip;
    }
    public void setZip(String zip) {
        this.zip = zip;
    }
}

