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
    static final String ACCOUNT_TABLE = "ACCOUNTS";
    /** Name of the branch table. */
    static final String BRANCH_TABLE = "BRANCHES";
    /** Name of the teller table. */
    static final String TELLER_TABLE = "TELLERS";
    /** Name of the history table. */
    static final String HISTORY_TABLE = "HISTORY";

    /** The number of tellers per branch, if not specified. */
    static final int DEFAULT_TELLERS_PER_BRANCH = 10;
    /** The number of accounts per branch, if not specified. */
    static final int DEFAULT_ACCOUNTS_PER_BRANCH = 100000;

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
    static final int HISTORY_EXTRA = 50 - 4 - 4 - 4 - 8 - 12;

    /** Number of records in the branch table. */
    private final int branches;
    /** Number of tellers per branch. */
    private final int tellersPerBranch;
    /** Number of accounts per branch. */
    private final int accountsPerBranch;

    /**
     * Create a filler that generates tables with the given sizes.
     *
     * @param branches number of branches
     * @param tellersPerBranch number of tellers per branch
     * @param accountsPerBranch number of accounts per branch
     */
    public BankAccountFiller(int branches, int tellersPerBranch,
                             int accountsPerBranch) {
        if (branches <= 0 || tellersPerBranch <= 0 || accountsPerBranch <= 0) {
            throw new IllegalArgumentException(
                "all arguments must be greater than 0");
        }
        this.branches = branches;
        this.tellersPerBranch = tellersPerBranch;
        this.accountsPerBranch = accountsPerBranch;
    }

    /**
     * Create a filler that generate tables which have correct sizes
     * relative to each other. With scale factor 1, the account table
     * has 100000 rows, the teller table has 10 rows and the branch
     * table has 1 row. If the scale factor is different from 1, the
     * number of rows is multiplied with the scale factor.
     *
     * @param scale the scale factor for this database
     */
    public BankAccountFiller(int scale) {
        this(scale, DEFAULT_TELLERS_PER_BRANCH, DEFAULT_ACCOUNTS_PER_BRANCH);
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
                        "(ACCOUNT_ID INT NOT NULL, " +
                        "BRANCH_ID INT NOT NULL, " +
                        // The balance column must be able to hold 10
                        // digits and sign per TPC-B spec, so BIGINT
                        // is needed.
                        "ACCOUNT_BALANCE BIGINT NOT NULL, " +
                        "EXTRA_DATA CHAR(" + ACCOUNT_EXTRA + ") NOT NULL)");

        s.executeUpdate("CREATE TABLE " + BRANCH_TABLE +
                        "(BRANCH_ID INT NOT NULL, " +
                        // The balance column must be able to hold 10
                        // digits and sign per TPC-B spec, so BIGINT
                        // is needed.
                        "BRANCH_BALANCE BIGINT NOT NULL, " +
                        "EXTRA_DATA CHAR(" + BRANCH_EXTRA + ") NOT NULL)");

        s.executeUpdate("CREATE TABLE " + TELLER_TABLE +
                        "(TELLER_ID INT NOT NULL, " +
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

        Statement s = c.createStatement();

        PreparedStatement atIns =
            c.prepareStatement("INSERT INTO " + ACCOUNT_TABLE +
                               "(ACCOUNT_ID, BRANCH_ID, ACCOUNT_BALANCE, " +
                               "EXTRA_DATA) VALUES (?, ?, 0, ?)");
        atIns.setString(3, createJunk(ACCOUNT_EXTRA)); // same for all rows
        for (int id = 0; id < accountsPerBranch * branches; id++) {
            atIns.setInt(1, id);
            atIns.setInt(2, id / accountsPerBranch);
            atIns.executeUpdate();
        }
        atIns.close();

        s.executeUpdate("ALTER TABLE " + ACCOUNT_TABLE + " ADD CONSTRAINT " +
                ACCOUNT_TABLE + "_PK PRIMARY KEY (ACCOUNT_ID)");

        c.commit();

        PreparedStatement btIns =
            c.prepareStatement("INSERT INTO " + BRANCH_TABLE +
                               "(BRANCH_ID, BRANCH_BALANCE, EXTRA_DATA) " +
                               "VALUES (?, 0, ?)");
        btIns.setString(2, createJunk(BRANCH_EXTRA)); // same for all rows
        for (int id = 0; id < branches; id++) {
            btIns.setInt(1, id);
            btIns.executeUpdate();
        }
        btIns.close();

        s.executeUpdate("ALTER TABLE " + BRANCH_TABLE + " ADD CONSTRAINT " +
                BRANCH_TABLE + "_PK PRIMARY KEY (BRANCH_ID)");

        c.commit();

        PreparedStatement ttIns =
            c.prepareStatement("INSERT INTO " + TELLER_TABLE +
                               "(TELLER_ID, BRANCH_ID, TELLER_BALANCE, " +
                               "EXTRA_DATA) VALUES (?, ?, 0, ?)");
        ttIns.setString(3, createJunk(TELLER_EXTRA)); // same for all rows
        for (int id = 0; id < tellersPerBranch * branches; id++) {
            ttIns.setInt(1, id);
            ttIns.setInt(2, id / tellersPerBranch);
            ttIns.executeUpdate();
        }
        ttIns.close();

        s.executeUpdate("ALTER TABLE " + TELLER_TABLE + " ADD CONSTRAINT " +
                TELLER_TABLE + "_PK PRIMARY KEY (TELLER_ID)");

        c.commit();

        s.close();
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
    static String createJunk(int length) {
        char[] junk = new char[length];
        Arrays.fill(junk, 'x');
        return new String(junk);
    }
}
