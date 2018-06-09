/*
 *
 * Derby - Class SURDataModelSetup
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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;

/**
 * This class is a decorator for the Scrollable Updatable Resultset
 * tests.  It sets up a datamodel and populates it with data.
 */
public class SURDataModelSetup extends BaseJDBCTestSetup
{  
    /**
     * Constructor.
     * @param test test to decorate with this setup
     * @param model enumerator for which model to use.
     * (Alternatively we could use a subclass for each model)
     */
    public SURDataModelSetup(Test test, SURDataModel model) {
        super(test);
        this.model = model;       
    }

     /**
     * Creates a datamodel for testing Scrollable Updatable ResultSets
     * and populates the database model with data.
     * @param model enumerator for which model to use
     * @param con connection to database
     * @param records number of records in the data model
     */
    public static void createDataModel(SURDataModel model, Connection con,
                                       int records) 
        throws SQLException
    {
        
        BaseJDBCTestCase.dropTable(con, "T1");
        
        Statement statement = con.createStatement();     
        
        /** Create the table */
        statement.execute(model.getCreateTableStatement());
        BaseTestCase.println(model.getCreateTableStatement());
        
        /** Create secondary index */
        if (model.hasSecondaryKey()) {
            statement.execute("create index a_on_t on t1(a)");
            BaseTestCase.println("create index a_on_t on t1(a)");
        }
        
        /** Populate with data */
        PreparedStatement ps = con.
            prepareStatement("insert into t1 values (?,?,?,?,?)");
        
        for (int i=0; i<records; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i);
            ps.setInt(3, i*2 + 17);
            ps.setString(4, "Tuple " +i);
            ps.setString(5, "C-tuple "+i);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
        statement.close();
        con.commit();
    }
    
    /**
     * Creates a datamodel for testing Scrollable Updatable ResultSets
     * and populates the database model with data.
     * The model will be set up with the number of records as defined by
     * the recordCount attribute.
     * @param model enumerator for which model to use
     * @param con connection to database
     */
    public static void createDataModel(SURDataModel model, Connection con) 
        throws SQLException
    {
        createDataModel(model, con, recordCount);
    }
    
    /**
     * Creates a datamodel for testing Scrollable Updatable ResultSets
     * and populates the database model with data.
     */
    protected void setUp() throws  Exception {       
        println("Setting up datamodel: " + model);

        try {
            Connection con = getConnection();
            con.setAutoCommit(false);
            createDataModel(model, con);
        } catch (SQLException e) {
            printStackTrace(e); // Print the entire stack
            throw e;
        }
    }
    
    /**
     * Delete the datamodel
     */
    protected void tearDown() 
        throws Exception
    {
        try {
            Connection con = getConnection();
            con.rollback();
            con.createStatement().execute("drop table t1");
            con.commit();
        } catch (SQLException e) {
            printStackTrace(e);
        }
        super.tearDown();
    }
    
    public String toString() {
        return "SURDataModel tests with model: " + model;
    }

    private final SURDataModel model;
    final static int recordCount = 10;  // Number of records in data model.  
        
    /**
     * Enum for the layout of the data model
     */
    public final static class SURDataModel {

        /** Model with no keys */
        public final static SURDataModel MODEL_WITH_NO_KEYS = 
            new SURDataModel("NO_KEYS");
        
        /** Model with primary key */
        public final static SURDataModel MODEL_WITH_PK = 
            new SURDataModel("PK");
        
        /** Model with secondary index */
        public final static SURDataModel MODEL_WITH_SECONDARY_KEY = 
            new SURDataModel("SECONDARY_KEY");
        
        /** Model with primary key and secondary index */
        public final static SURDataModel MODEL_WITH_PK_AND_SECONDARY_KEY = 
            new SURDataModel("PK_AND_SECONDARY_KEY");

        /** Array with all values */
        private final static Set values = Collections.unmodifiableSet
            (new HashSet<SURDataModel>(Arrays.asList(
                MODEL_WITH_NO_KEYS, 
                MODEL_WITH_PK, 
                MODEL_WITH_SECONDARY_KEY,
                MODEL_WITH_PK_AND_SECONDARY_KEY
            )));
        
        /**
         * Returns an unmodifyable set of all valid data models
         */ 
        public final static Set values() {
            return values;
        }
       

        /** Returns true if this model has primary key */
        public boolean hasPrimaryKey() {
            return (this==MODEL_WITH_PK || 
                    this==MODEL_WITH_PK_AND_SECONDARY_KEY);
        }
        
        /** Returns true if this model has a secondary key */
        public boolean hasSecondaryKey() {
            return (this==MODEL_WITH_SECONDARY_KEY || 
                    this==MODEL_WITH_PK_AND_SECONDARY_KEY);
        }

        /**
         * Returns the string for creating the table
         */
        public String getCreateTableStatement() {
            return hasPrimaryKey() 
                ? "create table t1 (id int primary key, a int, b int, c varchar(5000), d clob)"
                : "create table t1 (id int, a int, b int, c varchar(5000), d clob)";
        }

        /**
         * Returns a string representation of the model 
         * @return string representation of this object
         */
        public String toString() {
            return name;
        }
        
        /**
         * Constructor
         */
        private SURDataModel(String name) {
            this.name = name;
        }
        
        
        
        private final String name;
    }

    /**
     * Prints the stack trace. If run in the harness, the
     * harness will mark the test as failed if this method
     * has been called.
     */
    static void printStackTrace(Throwable t) {
        BaseJDBCTestCase.printStackTrace(t);
    }
}
