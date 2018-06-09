/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DBOperations

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

class DBOperations implements Runnable {
    private Connection con;
    private int keyVal;
    private SQLException exception;
    private Throwable unexpectedException;
    
    /**
     * Instantiates DBOperation object.
     * @param con Connection to be used within this object.
     * @param keyValue key value while executing dmls.
     */
    DBOperations(Connection con, int keyValue) throws SQLException {
        this.con = con;
        this.keyVal = keyValue;
        con.setAutoCommit(false);
    }
    
    /**
     * Deletes the record with key value passed in constroctor.
     */
    void delete () throws SQLException {
        Statement stmt = con.createStatement();
        stmt.execute("delete from tab1 where i = " + keyVal);
        stmt.close();
    }
    
    /**
     * Inserts a record with key value passed in constroctor.
     */
    void insert () throws SQLException {
        Statement stmt = con.createStatement();
        try {
            
            stmt.executeUpdate("insert into tab1 values ("+keyVal+")");
        }
        catch (SQLException e) {
            exception = e;
        }
        stmt.close();
    }
    
    /**
     * Rollbacks the transaction.
     */
    void rollback () throws SQLException {
        con.rollback();
    }
    
    /**
     * Returns the SQLException received while executing insert.
     * Null if no transaction was received.
     * @return SQLException
     */
    SQLException getException () {
        return exception;
    }
    
    /**
     * commits the trasnaction.
     */
    void commit () throws SQLException {
        con.commit();
    } 
    
    /**
     * Returns if any unexpected trasnaction was thrown during any 
     * of the operation.
     * @return Throwable
     */
    public Throwable getUnexpectedException() {
        return unexpectedException;
    } 

    public void run() {
        try {
            insert();
        }
        catch (Throwable e) {
            unexpectedException = e;
        }
    }
}
