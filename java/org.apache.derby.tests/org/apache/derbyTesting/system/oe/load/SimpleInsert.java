/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.load.SimpleInsert
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
package org.apache.derbyTesting.system.oe.load;

import java.sql.*;

import org.apache.derbyTesting.system.oe.client.Load;
import org.apache.derbyTesting.system.oe.util.OERandom;

/**
 * Implement the initial database population according to the TPC-C database
 * population requirements in Clause 4.3 using simple Insert sql statements
 */
public class SimpleInsert implements Load {

    /**
     * database connection
     */
    Connection conn = null;

    /**
     * warehouse scale factor, default to 1.
     */
    short scale = 1;
    
    /**
     * Seed value for random number generator.
     */
    long seed = System.currentTimeMillis();

    /**
     * Utility to generate random data per the TPC-C requirements
     */
    OERandom random;

    /**
     * Perform the necessary setup before database population.
     * 
     * @param conn -
     *            database connection
     * @param scale -
     *            scale of the database. The WAREHOUSE table is used as the base
     *            unit of scaling.
     * @throws Exception
     */
    public void setupLoad(Connection conn, short scale) throws SQLException {

        setupConnection(conn, scale);

        Statement s = conn.createStatement();
        try {
            s.execute("DROP TABLE C");
        } catch (SQLException sqle) {
            // ignore
        }
        conn.commit();
        s.execute("CREATE TABLE C(CLOAD INT)");
        conn.commit();

        random = new OERandom(-1, seed);

        // Section 2.1.6.1 of TPC-C spec
        int loadRandomFactor = random.randomInt(0, 255);
        s.execute("INSERT INTO C VALUES(" + loadRandomFactor + ")");
        s.close();
        conn.commit();  
        
        setRandomGenerator();
    }
    
    /**
     * Set the connection up to the intended state.
     * Intended for use by sub-classes.
     */
    void setupConnection(Connection conn, short scale) throws SQLException
    {
        this.conn = conn;
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        this.scale = scale;
    }
    
    /**
     * Setup the random number generator to be used for the load.
     * @throws SQLException
     */
    void setRandomGenerator() throws SQLException
    {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT CLOAD FROM C");
        rs.next();
        int loadRandomFactor = rs.getInt(1);
        rs.close();
        random = new OERandom(loadRandomFactor, seed);
        s.close();
        conn.commit();
    }

    /**
     * Follow the initial database population requirements in Section 4.3.3 and
     * populate all the required tables.
     * 
     * @throws SQLException
     */
    public void populateAllTables() throws Exception {
        // load item table
        itemTable(1, Load.ITEM_COUNT);

        for (short w = 1; w <= scale; w++) {
            populateForOneWarehouse(w);
        }

    }
    
    /**
     * Populate all the tables needed for a specific warehouse.
     * for each row in warehouse table, load the stock,
     * district table. For each row in district table, load
     * the customer table. for each row in customer table, load
     * the customer table. for each row in customer table, load
     * @param w Warehouse to be populated.
     * @throws SQLException
     */
    void populateForOneWarehouse(short w) throws SQLException
    {
        warehouseTable(w);
        // for each warehouse: load the stock table
        stockTable(1, Load.STOCK_COUNT_W, w);
        for (short d = 1; d <= Load.DISTRICT_COUNT_W; d++) {
            districtTable(w, d);
            customerTable(w, d);
            orderTable(w, d);
        }
    }

    /**
     * Populate the ITEM table. See population requirements in section 4.3.3.1
     * <BR>
     * 
     * @param itemStart
     *            insert item information starting from this Item id (ITEM.I_ID)
     * @param itemEnd
     *            last Item id (ITEM.I_ID) for inserting information for
     * @throws SQLException
     */
    public void itemTable(int itemStart, int itemEnd) throws SQLException {
        PreparedStatement ps = conn
                .prepareStatement("INSERT INTO ITEM(I_ID,I_IM_ID,I_NAME,I_PRICE,I_DATA)"
                        + " VALUES (?, ?, ?, ?, ?)");

        String price;

        for (int i = itemStart; i <= itemEnd; i++) {
            ps.setInt(1, i);
            ps.setInt(2, random.randomInt(1, 10000));
            ps.setString(3, random.randomAString14_24());
            price = random.randomDecimalString(100, 9999, 2);
            ps.setString(4, price);
            ps.setString(5, random.randomData());
            ps.executeUpdate();

            if ((i % (1000)) == 0) {
                conn.commit();
            }
        }
        conn.commit();
        ps.close();
    }

    /**
     * Populate the WAREHOUSE table for a given warehouse. See population
     * requirements in section 4.3.3.1
     * 
     * @param w
     *            WAREHOUSE ID (W_ID) to insert data for.
     * @throws SQLException
     */
    public void warehouseTable(short w) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("INSERT INTO WAREHOUSE "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 300000.00)");

        ps.setShort(1, w);
        ps.setString(2, random.randomAString(6, 10));
        ps.setString(3, random.randomAString10_20());
        ps.setString(4, random.randomAString10_20());
        ps.setString(5, random.randomAString10_20());
        ps.setString(6, random.randomState());
        ps.setString(7, random.randomZIP());
        ps.setString(8, random.randomDecimalString(0, 2000, 4));

        ps.executeUpdate();
        conn.commit();
        ps.close();
    }

    /**
     * Populate the STOCK table for a given warehouse. See population
     * requirements in section 4.3.3.1 <BR>
     * 
     * @param itemStart
     *            insert stocks of items from this Item id (ITEM.I_ID)
     * @param itemEnd
     *            last Item id (ITEM.I_ID) to insert stocks of times for.
     * @param w
     *            WAREHOUSE id (W_ID) for which the stock is populated.
     * @throws SQLException
     */
    public void stockTable(int itemStart, int itemEnd, short w)
            throws SQLException {
        PreparedStatement ps = conn.prepareStatement("INSERT INTO STOCK "
                + "(S_I_ID, S_W_ID, S_QUANTITY,S_DIST_01, S_DIST_02,"
                + " S_DIST_03,S_DIST_04,S_DIST_05,"
                + "S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,"
                + "S_ORDER_CNT, S_REMOTE_CNT, S_YTD, S_DATA ) VALUES "
                + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?)");

        ps.setShort(2, w);

        for (int i = itemStart; i <= itemEnd; i++) {
            ps.setInt(1, i);
            ps.setInt(3, random.randomInt(10, 100));
            ps.setString(4, random.randomAString24());
            ps.setString(5, random.randomAString24());
            ps.setString(6, random.randomAString24());
            ps.setString(7, random.randomAString24());
            ps.setString(8, random.randomAString24());
            ps.setString(9, random.randomAString24());
            ps.setString(10, random.randomAString24());
            ps.setString(11, random.randomAString24());
            ps.setString(12, random.randomAString24());
            ps.setString(13, random.randomAString24());

            ps.setString(14, random.randomData());
            ps.executeUpdate();

            if ((i % 1000) == 0) {
                conn.commit();
            }
        }
        conn.commit();
        ps.close();
    }

    /**
     * Populate the DISTRICT table for a given warehouse. See population
     * requirements in section 4.3.3.1 <BR>
     * 
     * @param w -
     *            WAREHOUSE id (W_ID)
     * @param d -
     *            DISTRICT id (D_ID)
     * @throws SQLException
     */
    public void districtTable(short w, short d) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("INSERT INTO DISTRICT"
                + " (D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2,"
                + " D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) "
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 30000.00, 3001)");

        ps.setShort(1, d);
        ps.setShort(2, w);
        ps.setString(3, random.randomAString(6, 10));
        ps.setString(4, random.randomAString10_20());
        ps.setString(5, random.randomAString10_20());
        ps.setString(6, random.randomAString10_20());
        ps.setString(7, random.randomState());
        ps.setString(8, random.randomZIP());
        ps.setString(9, random.randomDecimalString(0, 2000, 4));
        ps.executeUpdate();
        conn.commit();
        ps.close();

    }

    /**
     * Populate the CUSTOMER table for a given district for a specific
     * warehouse. See population requirements in section 4.3.3.1 <BR>
     * 
     * @param w -
     *            WAREHOUSE id (W_ID)
     * @param d -
     *            DISTRICT id (D_ID)
     * @throws SQLException
     */
    public void customerTable(short w, short d) throws SQLException {
        PreparedStatement psC = conn.prepareStatement("INSERT INTO CUSTOMER"
                + " (C_ID, C_D_ID, C_W_ID, C_FIRST, C_MIDDLE, C_LAST,"
                + " C_STREET_1, C_STREET_2,  C_CITY, C_STATE, C_ZIP, "
                + "C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,"
                + " C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, "
                + "C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA)  "
                + "VALUES (?, ?, ?, ?, 'OE', ?, ?, ?, ?, ?, ?, ?, "
                + " CURRENT TIMESTAMP ,?, 50000.00, ?, -10.0, 10.0,"
                + " 1, 0, ?)");

        PreparedStatement psH = conn
                .prepareStatement("INSERT INTO HISTORY (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) VALUES (?, ?, ?, ?, ?, CURRENT TIMESTAMP, 10.00, ?)");

        psC.setShort(2, d); // c_d_id
        psC.setShort(3, w); // c_w_id
        psH.setShort(2, d);
        psH.setShort(3, w);
        psH.setShort(4, d);
        psH.setShort(5, w);
        for (int c = 1; c <= Load.CUSTOMER_COUNT_W / Load.DISTRICT_COUNT_W; c++) {
            psC.setInt(1, c); // c_id
            psC.setString(4, random.randomAString8_16()); // c_first

            psC.setString(5, random.randomCLastPopulate(c)); // c_last
            psC.setString(6, random.randomAString10_20()); // c_street_1
            psC.setString(7, random.randomAString10_20()); // c_street_2
            psC.setString(8, random.randomAString10_20()); // c_city
            psC.setString(9, random.randomState()); // c_state
            psC.setString(10, random.randomZIP()); // c_zip
            psC.setString(11, random.randomNString(16, 16)); // c_phone

            psC.setString(12, Math.random() < 0.10 ? "BC" : "GC"); // c_credit

            psC.setString(13, random.randomDecimalString(0, 5000, 4)); // c_discount

            String str = random.randomAString300_500();
            if (str.length() > 255)
                str = str.substring(255);
            psC.setString(14, str); // c_data

            psC.executeUpdate();

            psH.setInt(1, c);
            psH.setString(6, random.randomAString(12, 24));
            psH.executeUpdate();

            if ((c % 1000) == 0) {
                conn.commit();
            }

        }
        conn.commit();

        psC.close();
        psH.close();
    }

    /**
     * Populate the ORDER table See population requirements in section 4.3.3.1
     * 
     * @param w -
     *            WAREHOUSE id (W_ID)
     * @param d -
     *            DISTRICT id (D_ID)
     * @throws SQLException
     */
    public void orderTable(short w, short d) throws SQLException {

        PreparedStatement psO = conn
                .prepareStatement("INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, 1)");

        PreparedStatement psOL = conn
                .prepareStatement("INSERT INTO ORDERLINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        PreparedStatement psNO = conn
                .prepareStatement("INSERT INTO NEWORDERS (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)");

        psO.setShort(2, d);
        psO.setShort(3, w);
        
        // Section 4.3.3.1 of TPC-C specification revision 5.8.0
        // O_C_ID selected sequentially from a random permutation 
        // of [1 .. 3,000]
        int[] cid = random.randomIntPerm(Load.CUSTOMER_COUNT_W
                / Load.DISTRICT_COUNT_W);
        
        for (int o_id = 1; o_id <= cid.length; o_id++) {
            psO.setInt(1, o_id);
            psO.setInt(4, cid[o_id-1]);

            Timestamp o_entry_d = new Timestamp(System.currentTimeMillis());

            psO.setTimestamp(5, o_entry_d);

            if (o_id <= Load.NEWORDERS_BREAKPOINT)
                psO.setShort(6, (short) random.randomInt(1, 10));
            else
                psO.setNull(6, Types.SMALLINT);

            int o_ol_cnt = random.randomInt(5, 15);
            psO.setInt(7, o_ol_cnt);

            psO.executeUpdate();

            psOL.setShort(2, d);
            psOL.setShort(3, w);
            psNO.setShort(2, d);
            psNO.setShort(3, w);
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {

                psOL.setInt(1, o_id);
                psOL.setInt(4, ol_number);

                // OL_I_ID random within [1 .. 100,000]
                psOL.setInt(5, random.randomInt(1, Load.ITEM_COUNT));
                psOL.setShort(6, w);
                if (o_id <= Load.NEWORDERS_BREAKPOINT) {
                    psOL.setTimestamp(7, o_entry_d);
                    psOL.setString(9, "0.00");
                } else {
                    psOL.setNull(7, Types.TIMESTAMP);
                    psOL.setString(9, random.randomDecimalString(1, 999999, 2));
                }
                psOL.setInt(8, 5);
                psOL.setString(10, random.randomAString24());
                psOL.executeUpdate();
            }
            if (o_id > Load.NEWORDERS_BREAKPOINT) {
                psNO.setInt(1, o_id);
                psNO.executeUpdate();
            }
            if ((o_id % 1000) == 0) {
                conn.commit();
            }

        }
        conn.commit();

        psOL.close();
        psO.close();
        psNO.close();
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    /**
     * Ignore, this is a single threaded load.
     */
    public void setThreadCount(int threadCount) {
    }

}
