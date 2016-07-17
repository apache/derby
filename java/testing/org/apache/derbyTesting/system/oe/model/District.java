/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.model.District
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
 * An Order Entry district.
 * <P>
 * Fields map to definition in TPC-C for the DISTRICT table.
 * The Java names of fields do not include the D_ prefix
 * and are in lower case.
 * For clarity this field are renamed in Java
 * <UL>
 * <LI>w_id =&gt; warehouse (SQL column D_W_ID)
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
 * Primary key maps to {warehouse, id}.
 * <P>
 * Implemented by extending Warehouse as they share the same basic format.
 * 
 * <P>
 * A District object may sparsely populated, when returned from a
 * business transaction it is only guaranteed to contain  the information
 * required to display the result of that transaction.
 */
public class District extends Warehouse {
    private short warehouse;
    
    /**
     * Clear all information to allow object re-use.
     */
    public void clear()
    {
        super.clear();
        warehouse = 0;
    }


    public short getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(short warehouse) {
        this.warehouse = warehouse;
    }
}
