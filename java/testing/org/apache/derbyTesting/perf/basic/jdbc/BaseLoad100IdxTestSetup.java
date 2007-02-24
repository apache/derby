/*

Derby - Class org.apache.derbyTesting.perf.basic.jdbc.BaseLoad100IdxTestSetup

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
package org.apache.derbyTesting.perf.basic.jdbc;

import java.sql.Statement;
import java.sql.SQLException;
import junit.framework.Test;

/**
 * Extend the BaseLoad100TestSetup to add indexes
 * Base table is similar to BaseLoad100TestSetup and the algorithm to load data is the same
 * @see BaseLoad100TestSetup
 */
public class BaseLoad100IdxTestSetup extends BaseLoad100TestSetup {

    /**
     * constructor
     * @param test name of the test
     */
    public BaseLoad100IdxTestSetup(Test test) {
        super(test);
    }

    /**
     * @param test name of test
     * @param rowsToLoad number of rows to insert
     */
    public BaseLoad100IdxTestSetup(Test test, int rowsToLoad)
    {
        super(test);
        this.rowsToLoad=rowsToLoad;
    }

    /**
     * @param test name of the test
     * @param tableName name of the table to insert the rows into
     */
    public BaseLoad100IdxTestSetup(Test test, String tableName)
    {
        super(test);
        this.tableName = tableName;
    }

    /**
     * @param test name of test
     * @param rowcount number of rows to insert
     * @param tableName name of the table to insert the rows into
     */
     public BaseLoad100IdxTestSetup(Test test,int rowcount, String tableName)
    {
        super(test,rowcount,tableName);
    }

    /**
     * Override the decorateSQL and create the necessary schema.
     * @see org.apache.derbyTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
     */
    protected void decorateSQL(Statement s)
        throws SQLException
    {
        s.execute("CREATE TABLE " +tableName+" ("
                + "i1 INT, i2 INT, i3 INT, i4 INT, i5 INT, "
                + "c6 CHAR(20), c7 CHAR(20), c8 CHAR(20), c9 CHAR(20))");
        s.execute("CREATE UNIQUE INDEX " +tableName +"x on "+ tableName+"(i1,i3)");

    }


}
