/*

Derby - Class org.apache.derbyTesting.perf.clients.BankAccountFiller

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derbyTesting.perf.clients;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.Arrays;

/**
 * This class creates and populates tables that can be used by the
 * bank transactions test clients. It attempts to create tables that
 * follow the rules defined by the TPC-B benchmark specification.
 */
public class BankAccountFiller implements DBFiller {

    /** Name of the account table. */
    private static final String ACCOUNT_TABLE = "ACCOUNTS";
    /** Name of the branch table. */
    private static final String BRANCH_TABLE = "BRANCHES";
    /** Name of the teller table. */
    private static final String TELLER_TABLE = "TELLERS";
    /** Name of the history table. */
    private static final String HISTORY_TABLE = "HISTORY";

    /**
     * Number of extra bytes needed to make the rows in the account
     * table at least 100 bytes, as required by the TPC-B spec. The
     * table has two INT columns (4 bytes each) and one BIGINT column
     * (8 bytes).
     */
    private static final int ACCOUNT_EXTRA = 100 - 4 - 4 - 8;

    /**
     * Number of extra bytes needed to make the rows in the branch
     * table at least 100 bytes, as required by the TPC-B spec. The
     * table has one INT column (4 bytes) and one BIGINT column (8
     * bytes).
     */
    private static final int BRANCH_EXTRA = 100 - 4 - 8;

    /**
     * Number of extra bytes needed to make the rows in the teller
     * table at least 100 bytes, as required by the TPC-B spec. The
     * table has two INT columns (4 bytes each) and one BIGINT column
     * (8 bytes).
     */
    private static final int TELLER_EXTRA = 100 - 4 - 4 - 8;

    /**
     * Number of extra bytes needed to make the rows in the history
     * table at least 50 bytes, as required by the TPC-B spec. The
     * table has three INT columns (4 bytes each), one BIGINT column
     * (8 bytes) and one TIMESTAMP column (12 bytes).
     */
    private static final int HISTORY_EXTRA = 50 - 4 - 4 - 4 - 8 - 12;

    /** Number of records in the account table. */
    private final int accountRecords;
    /** Number of records in the teller table. */
    private final int tellerRecords;
    /** Number of records in the branch table. */
    private final int branchRecords;

    /**
     * Create a filler that generates tables with the given sizes.
     *
     * @param accounts number of records in the account table
     * @param tellers number of records in the teller table
     * @param branches number of records in the branch table
     */
    public BankAccountFiller(int accounts, int tellers, int branches) {
        if (accounts <= 0 || tellers <= 0 || branches <= 0) {
            throw new IllegalArgumentException(
                "all arguments must be greater than 0");
        }
        accountRecords = accounts;
        tellerRecords = tellers;
        branchRecords = branches;
    }

    /**
     * Create a filler that generate tables which have correct sizes
     * relative to each other. With scale factor 1, the account table
     * has 100000 rows, the teller table has 10 rows and the branch
     * table has 1 row. If the scale factor is different from 1, the
     * number of rows is multiplied with the scale factor.
     *
     * @param tps the scale factor for this database
     */
    public BankAccountFiller(int tps) {
        this(tps * 100000, tps * 10, tps * 1);
    }

    /**
     * Populate the database.
     */
    public void fill(Connection c) throws SQLException {
        c.setAutoCommit(false);
        dropTables(c);
        createTables(c);
        fillTables(c);
    }

    /**
     * Drop the tables if they exits.
     */
    private static void dropTables(Connection c) throws SQLException {
        WisconsinFiller.dropTable(c, ACCOUNT_TABLE);
        WisconsinFiller.dropTable(c, BRANCH_TABLE);
        WisconsinFiller.dropTable(c, TELLER_TABLE);
        WisconsinFiller.dropTable(c, HISTORY_TABLE);
        c.commit();
    }

    /**
     * Create the tables.
     */
    private static void createTables(Connection c) throws SQLException {
        Statement s = c.createStatement();

        s.executeUpdate("CREATE TABLE " + ACCOUNT_TABLE +
                        "(ACCOUNT_ID INT PRIMARY KEY, " +
                        "BRANCH_ID INT NOT NULL, " +
                        // The balance column must be able to hold 10
                        // digits and sign per TPC-B spec, so BIGINT
                        // is needed.
                        "ACCOUNT_BALANCE BIGINT NOT NULL, " +
                        "EXTRA_DATA CHAR(" + ACCOUNT_EXTRA + ") NOT NULL)");

        s.executeUpdate("CREATE TABLE " + BRANCH_TABLE +
                        "(BRANCH_ID INT PRIMARY KEY, " +
                        // The balance column must be able to hold 10
                        // digits and sign per TPC-B spec, so BIGINT
                        // is needed.
                        "BRANCH_BALANCE BIGINT NOT NULL, " +
                        "EXTRA_DATA CHAR(" + BRANCH_EXTRA + ") NOT NULL)");

        s.executeUpdate("CREATE TABLE " + TELLER_TABLE +
                        "(TELLER_ID INT PRIMARY KEY, " +
                        "BRANCH_ID INT NOT NULL, " +
                        // The balance column must be able to hold 10
                        // digits and sign per TPC-B spec, so BIGINT
                        // is needed.
                        "TELLER_BALANCE INT NOT NULL, " +
                        "EXTRA_DATA CHAR(" + TELLER_EXTRA + ") NOT NULL)");

        s.executeUpdate("CREATE TABLE " + HISTORY_TABLE +
                        "(ACCOUNT_ID INT NOT NULL, " +
                        "TELLER_ID INT NOT NULL, " +
                        "BRANCH_ID INT NOT NULL, " +
                        // The amount column must be able to hold 10
                        // digits and sign per TPC-B spec, so BIGINT
                        // is needed.
                        "AMOUNT BIGINT NOT NULL, " +
                        "TIME_STAMP TIMESTAMP NOT NULL, " +
                        "EXTRA_DATA CHAR(" + HISTORY_EXTRA + ") NOT NULL)");

        s.close();
        c.commit();
    }

    /**
     * Fill the tables with rows.
     */
    private void fillTables(Connection c) throws SQLException {

        PreparedStatement atIns =
            c.prepareStatement("INSERT INTO " + ACCOUNT_TABLE +
                               "(ACCOUNT_ID, BRANCH_ID, ACCOUNT_BALANCE, " +
                               "EXTRA_DATA) VALUES (?, ?, 0, ?)");
        atIns.setString(3, createJunk(ACCOUNT_EXTRA)); // same for all rows
        for (int id = 0; id < accountRecords; id++) {
            atIns.setInt(1, id);
            atIns.setInt(2, id % branchRecords);
            atIns.executeUpdate();
        }
        atIns.close();
        c.commit();

        PreparedStatement btIns =
            c.prepareStatement("INSERT INTO " + BRANCH_TABLE +
                               "(BRANCH_ID, BRANCH_BALANCE, EXTRA_DATA) " +
                               "VALUES (?, 0, ?)");
        btIns.setString(2, createJunk(BRANCH_EXTRA)); // same for all rows
        for (int id = 0; id < branchRecords; id++) {
            btIns.setInt(1, id);
            btIns.executeUpdate();
        }
        btIns.close();
        c.commit();

        PreparedStatement ttIns =
            c.prepareStatement("INSERT INTO " + TELLER_TABLE +
                               "(TELLER_ID, BRANCH_ID, TELLER_BALANCE, " +
                               "EXTRA_DATA) VALUES (?, ?, 0, ?)");
        ttIns.setString(3, createJunk(TELLER_EXTRA)); // same for all rows
        for (int id = 0; id < tellerRecords; id++) {
            ttIns.setInt(1, id);
            ttIns.setInt(2, id % branchRecords);
            ttIns.executeUpdate();
        }
        ttIns.close();
        c.commit();
    }

    /**
     * Return a string of the specified length that can be used to
     * increase the size of the rows. The string only contains
     * x's. The rows have a defined minimum size in bytes, whereas the
     * string length is in characters. For now, we assume that one
     * character maps to one byte on the disk as long as the string
     * only contains ASCII characters.
     *
     * @param length the length of the string
     * @return a string of the specified length
     */
    private static String createJunk(int length) {
        char[] junk = new char[length];
        Arrays.fill(junk, 'x');
        return new String(junk);
    }

    // For testing until the test client that uses the database has
    // been written.
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection c = java.sql.DriverManager.getConnection(
            "jdbc:derby:wombat;create=true");
        DBFiller f = new BankAccountFiller(4000, 20, 3);
        System.out.print("filling...");
        f.fill(c);
        System.out.println("done!");
    }
}
