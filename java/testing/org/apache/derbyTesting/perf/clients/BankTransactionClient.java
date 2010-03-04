/*

Derby - Class org.apache.derbyTesting.perf.clients.BankTransactionClient

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

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

/**
 * This class implements a client thread which performs bank transactions. The
 * transactions are intended to perform the same operations as the transactions
 * specified by the TPC-B benchmark.
 */
public class BankTransactionClient implements Client {

    /** Random number generator. */
    private final Random random = new Random();

    /** The number of branches in the database. */
    private final int branches;
    /** The number of tellers per branch. */
    private final int tellersPerBranch;
    /** The number of accounts per branch. */
    private final int accountsPerBranch;

    /** The connection on which the operations are performed. */
    private Connection conn;
    /** Statement that updates the balance of the account. */
    private PreparedStatement updateAccount;
    /** Statement that updated the history table. */
    private PreparedStatement updateHistory;
    /** Statement that updates the balance of the teller. */
    private PreparedStatement updateTeller;
    /** Statement that updated the balance of the branch. */
    private PreparedStatement updateBranch;
    /** Statement that retrieves the current account balance. */
    private PreparedStatement retrieveAccountBalance;

    /**
     * Create a client that works on a database with the given number of
     * branches, tellers and accounts.
     *
     * @param branches the number of branches in the database
     * @param tellersPerBranch the number of tellers per branch
     * @param accountsPerBranch the number of accounts per branch
     */
    public BankTransactionClient(int branches, int tellersPerBranch,
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
     * Create a client that works on a database with the default number of
     * tellers and accounts per branch.
     *
     * @param scale the scale factor for the database (equal to the number of
     * branches)
     *
     * @see BankAccountFiller#BankAccountFiller(int)
     */
    public BankTransactionClient(int scale) {
        this(scale,
             BankAccountFiller.DEFAULT_TELLERS_PER_BRANCH,
             BankAccountFiller.DEFAULT_ACCOUNTS_PER_BRANCH);
    }

    /**
     * Initialize the connection and the statements used by the test.
     */
    public void init(Connection c) throws SQLException {
        conn = c;
        c.setAutoCommit(false);

        updateAccount = c.prepareStatement(
            "UPDATE " + BankAccountFiller.ACCOUNT_TABLE +
            " SET ACCOUNT_BALANCE = ACCOUNT_BALANCE + ? WHERE ACCOUNT_ID = ?");

        updateHistory = c.prepareStatement(
            "INSERT INTO " + BankAccountFiller.HISTORY_TABLE +
            "(ACCOUNT_ID, TELLER_ID, BRANCH_ID, AMOUNT, TIME_STAMP, " +
            "EXTRA_DATA) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, '" +
            BankAccountFiller.createJunk(BankAccountFiller.HISTORY_EXTRA) +
            "')");

        updateTeller = c.prepareStatement(
            "UPDATE " + BankAccountFiller.TELLER_TABLE +
            " SET TELLER_BALANCE = TELLER_BALANCE + ? WHERE TELLER_ID = ?");

        updateBranch = c.prepareStatement(
            "UPDATE " + BankAccountFiller.BRANCH_TABLE +
            " SET BRANCH_BALANCE = BRANCH_BALANCE + ? WHERE BRANCH_ID = ?");

        retrieveAccountBalance = c.prepareStatement(
            "SELECT ACCOUNT_BALANCE FROM " + BankAccountFiller.ACCOUNT_TABLE +
            " WHERE ACCOUNT_ID = ?");
    }

    /**
     * Perform a single transaction with a profile like the one specified in
     * Clause 1.2 of the TPC-B specification.
     */
    public void doWork() throws SQLException {

        // Get the transaction input
        final int tellerId = fetchTellerId();
        final int branchId = fetchBranchId(tellerId);
        final int accountId = fetchAccountId(branchId);
        final int delta = fetchDelta();

        // Update the account balance
        updateAccount.setInt(1, delta);
        updateAccount.setInt(2, accountId);
        updateAccount.executeUpdate();

        // Add a transaction log entry
        updateHistory.setInt(1, accountId);
        updateHistory.setInt(2, tellerId);
        updateHistory.setInt(3, branchId);
        updateHistory.setInt(4, delta);
        updateHistory.executeUpdate();

        // Update the teller balance
        updateTeller.setInt(1, delta);
        updateTeller.setInt(2, tellerId);
        updateTeller.executeUpdate();

        // Update the branch balance
        updateBranch.setInt(1, delta);
        updateBranch.setInt(2, branchId);
        updateBranch.executeUpdate();

        // Retrieve the balance
        retrieveAccountBalance.setInt(1, accountId);
        ResultSet rs = retrieveAccountBalance.executeQuery();
        rs.next();
        rs.getString(1);
        rs.close();
        conn.commit();
    }

    public void printReport(PrintStream out) {}
    
    /**
     * Generate a random teller id.
     */
    private int fetchTellerId() {
        return random.nextInt(tellersPerBranch * branches);
    }

    /**
     * Find the branch the specified teller belongs to.
     *
     * @param tellerId the id of the teller
     * @return the id of the branch for this teller
     */
    private int fetchBranchId(int tellerId) {
        return tellerId / tellersPerBranch;
    }

    /**
     * Generate a random account id based on the specified branch. Per Clause
     * 5.3.5 of the TPC-B specification, the accounts should be fetched from
     * the selected branch 85% of the time (or always if that's the only
     * branch), and from another branch the rest of the time.
     *
     * @param branchId the id of the selected branch
     * @return the id of a random account
     */
    private int fetchAccountId(int branchId) {
        int branch;
        if (branches == 1 || random.nextFloat() < 0.85f) {
            // pick an account in the same branch
            branch = branchId;
        } else {
            // pick an account in one of the other branches
            branch = random.nextInt(branches - 1);
            if (branch >= branchId) {
                branch++;
            }
        }
        // select a random account in the selected branch
        return branch * accountsPerBranch + random.nextInt(accountsPerBranch);
    }

    /**
     * Generate a random delta value between -99999 and +99999, both inclusive
     * (TPC-B specification, Clause 5.3.6). The delta value specifies how much
     * the balance should increase or decrease.
     *
     * @return a random value in the range [-99999,+99999]
     */
    private int fetchDelta() {
        return random.nextInt(199999) - 99999; // [-99999,+99999]
    }
}
