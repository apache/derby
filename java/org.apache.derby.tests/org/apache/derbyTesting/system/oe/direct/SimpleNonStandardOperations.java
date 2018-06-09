/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.direct.SimpleNonStandardOperations
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

import org.apache.derbyTesting.system.oe.client.Operations;
import org.apache.derbyTesting.system.oe.client.Submitter;
import org.apache.derbyTesting.system.oe.model.Address;
import org.apache.derbyTesting.system.oe.model.Customer;
import org.apache.derbyTesting.system.oe.util.OERandom;

/**
 * Collection of simple transactions that can be executed against
 * an order-entry database. These are not part of any standard
 * TPC-C specification but are useful for running specific
 * performance tests against Derby.
 * 
 * Since they are not standard operations there is no
 * ability to display the information. Any data selected
 * by a query is always fetched by processing all the
 * rows and all the columns using getXXX.
 *
 */
public class SimpleNonStandardOperations extends StatementHelper {
    
    /*
     * Reusable objects
     */
    private final Customer customer = new Customer();
    
    private final Address address = new Address();
    
    private final OERandom rand;

    public SimpleNonStandardOperations(Connection conn,
            int isolation) throws SQLException
    {
        super(conn, false, isolation);
        rand = Submitter.getRuntimeRandom(conn);
    }
    
    /**
     * Return an SimpleNonStandardOperations implementation based upon
     * SimpleNonStandardOperations with a single difference.
     * In this implementation the reset() executed after each
     * PreparedStatement execute
     * does nothing. Sees if there is any performance impact
     * of explicitly closing each ResultSet and clearing the
     * parameters.
     * <P>
     * Each ResultSet will be closed implicitly either at commit
     * time or at the next execution of the same PreparedStatement object.
     */
    public static SimpleNonStandardOperations noReset(final Connection conn,
            final int isolation)
        throws SQLException
    {
        return new SimpleNonStandardOperations(conn, isolation) {
            protected void reset(PreparedStatement ps) {}
        };
    }
    
    /**
     * Execute customerInquiry() with random parameters.
     * @throws SQLException 
     *
     */
    public void customerInquiry(int scale) throws SQLException
    {
        customerInquiry((short) rand.randomInt(1, scale),
                (short) rand.district(), rand.NURand1023());
    }
    
    /**
     * Lookup a customer's information (name, address, balance)
     * fetching it by the identifier.
     * <BR>
     * Primary key lookup against the CUSTOMER table (which
     * of course can be arbitrarily large depending on the
     * scale of the database. The cardinality of the CUSTOMER
     * is 30,000 rows per warehouse, for example with a 20
     * warehouse system this test would perform a primary
     * key lookup against 600,000 rows.
     * 
     * @param w Warehouse for customer
     * @param d District for customer
     * @param c Customer identifier
     */
    public void customerInquiry(short w, short d, int c)
       throws SQLException
    {
        
        PreparedStatement customerInquiry = prepareStatement(
                "SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_LAST, " +
                "C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, " +
                "C_PHONE " +
                "FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
        
            customerInquiry.setShort(1, w);
            customerInquiry.setShort(2, d);
            customerInquiry.setInt(3, c);
            ResultSet rs = customerInquiry.executeQuery();
            rs.next();
            
            customer.clear();
            customer.setBalance(rs.getString("C_BALANCE"));
            customer.setFirst(rs.getString("C_FIRST"));
            customer.setMiddle(rs.getString("C_MIDDLE"));
            customer.setLast(rs.getString("C_LAST"));
            
            customer.setAddress(getAddress(address, rs, "C_STREET_1"));
            
            customer.setPhone(rs.getString("C_PHONE"));
            
            reset(customerInquiry);
            conn.commit();
    }
    
    /**
     * Execute customerAddressChange() with random parameters.
     * @throws SQLException 
     *
     */
    public void customerAddressChange(int scale) throws SQLException
    {
        customerAddressChange((short) rand.randomInt(1, scale),
                (short) rand.district(), rand.NURand1023());
    }
    
    /**
     * Update a customers address with a new random value.
     * Update of a single row through a primary key.
     * <BR>
     * Primary key update against the CUSTOMER table (which
     * of course can be arbitrarily large depending on the
     * scale of the database. The cardinality of the CUSTOMER
     * is 30,000 rows per warehouse, for example with a 20
     * warehouse system this test would perform a primary
     * key lookup against 600,000 rows.
     * 
     * @param w Warehouse for customer
     * @param d District for customer
     * @param c Customer identifier
 
     */
    public void customerAddressChange(short w, short d, int c)
    throws SQLException
    {
        PreparedStatement customerAddressChange = prepareStatement(
                "UPDATE CUSTOMER " +
                "SET C_STREET_1 = ?, C_STREET_2 = ?, " +
                "C_CITY = ?, C_STATE = ?, C_ZIP = ? " +
                "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
        
        customerAddressChange.setString(1, rand.randomAString10_20()); // c_street_1
        customerAddressChange.setString(2, rand.randomAString10_20()); // c_street_2
        customerAddressChange.setString(3, rand.randomAString10_20()); // c_city
        customerAddressChange.setString(4, rand.randomState()); // c_state
        customerAddressChange.setString(5, rand.randomZIP()); // c_zip
        
        customerAddressChange.setShort(6, w);
        customerAddressChange.setShort(7, d);
        customerAddressChange.setInt(8, c);
        
        customerAddressChange.executeUpdate();
        
        reset(customerAddressChange);
        
        conn.commit();
          
    }
}
