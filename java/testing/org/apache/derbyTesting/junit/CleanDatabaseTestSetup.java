/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.CleanDatabase
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
package org.apache.derbyTesting.junit;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import junit.framework.Test;

/**
 * Test decorator that cleans a database on setUp and
 * tearDown to provide a test with a consistent empty
 * database as a starting point.
 * 
 */
public class CleanDatabaseTestSetup extends BaseJDBCTestSetup {

    /**
     * Decorator this test with the cleaner
     */
    public CleanDatabaseTestSetup(Test test) {
        super(test);
    }

    /**
     * Clean the default database using the default connection.
     */
    protected void setUp() throws Exception {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        CleanDatabaseTestSetup.cleanDatabase(conn);
        conn.close();
    }

    /**
     * Clean the default database using the default connection.
     */
    protected void tearDown() throws Exception {
        setUp();
        super.tearDown();
    }


    /**
     * Clean a complete database
     * @param conn Connection to be used, must not be in auto-commit mode.
     * @throws SQLException database error
     */
     public static void cleanDatabase(Connection conn) throws SQLException {
        DatabaseMetaData dmd = conn.getMetaData();

        // Fetch all the user schemas into a list
        List schemas = new ArrayList();
        ResultSet rs = dmd.getSchemas();
        while (rs.next()) {

            String schema = rs.getString("TABLE_SCHEM");
            if (schema.startsWith("SYS"))
                continue;
            if (schema.equals("SQLJ"))
                continue;
            if (schema.equals("NULLID"))
                continue;

            schemas.add(schema);
        }
        rs.close();

        // DROP all the user schemas.
        for (Iterator i = schemas.iterator(); i.hasNext();) {
            String schema = (String) i.next();
            JDBC.dropSchema(dmd, schema);
        }
    }

}
