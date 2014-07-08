/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.lang.reflect.Method;
import java.sql.SQLException;
import javax.sql.DataSource;
import junit.extensions.TestSetup;
import junit.framework.Assert;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

public class DSCreateShutdownDBTest extends BaseJDBCTestCase {

    static final String[] ADDITIONAL_DBS = {
        "dscreateconatdb1",
        "dscreateshutdowndb1", 
        "dscreateshutdowndb2",
        "conflict1",
        "conflict2",
        "conflict3",
        "conflict4",
        "conflict5",
        "conflict6",
        "conflict7"
    };
    
    static String DBNotFoundState;
    
    public DSCreateShutdownDBTest(String name) {
        super(name);
    }

    public static Test suite() 
    {
        BaseTestSuite suite = new BaseTestSuite("DSCreateShutdownDBTest");
        Test test = TestConfiguration.defaultSuite(DSCreateShutdownDBTest.class);        
        //Test test = TestConfiguration.clientServerSuite(DSCreateShutdownDBTest.class);
        suite.addTest(test);
        
        TestSetup setup = TestConfiguration.singleUseDatabaseDecorator(suite);
        // we need a couple extra databases to test they get created
        for (int i = 0; i < ADDITIONAL_DBS.length; i++)
        {
            setup = TestConfiguration.additionalDatabaseDecorator(setup,
                "emb" + ADDITIONAL_DBS[i]);
            setup = TestConfiguration.additionalDatabaseDecorator(setup,
                "srv" + ADDITIONAL_DBS[i]);
        }
    
        return suite;
    }
    
    public void tearDown() throws Exception {
        // attempt to get rid of any databases. 
        // only 5 dbs (in addition to defaultdb) should actually get
        // created, but just in case, try all...
        TestConfiguration conf = TestConfiguration.getCurrent();
        for (int i = 0; i < ADDITIONAL_DBS.length; i++) {
            removeDirectory(conf.getDatabasePath("emb" + ADDITIONAL_DBS[i]));
            removeDirectory(conf.getDatabasePath("srv" + ADDITIONAL_DBS[i]));
        }
        super.tearDown();
    }

    public void testCreateAndShutdown() throws SQLException {
        
        if (usingEmbedded())
            DBNotFoundState = "XJ004";
        else
            DBNotFoundState = "08004";
        
        // first play with default db, which is already created.
        String dbName = 
            TestConfiguration.getCurrent().getDefaultDatabaseName();
        // just check that we really access the database
        assertUpdateCount(createStatement(), 0, "set schema APP");
   
        // check that first the value is null
        assertGetNull(dbName);
        // check that we can set & that when set we can get
        // doesn't actually open connections so a little silly.
        assertSetAndGet(dbName, "shutdownDatabase", "shutdown");
        assertSetAndGet(dbName, "createDatabase", "create");
        // set to an invalid value, should get ignored
        assertNotSetAndGet(dbName, "shutdownDatabase", "boo");
        assertNotSetAndGet(dbName, "createDatabase", "boo");
        assertNotSetAndGet(dbName, "shutdownDatabase", "false");
        assertNotSetAndGet(dbName, "createDatabase", "false");
        
        assertReset(dbName);
        
        // check that create using ConnAttributes works
        assertCreateUsingConnAttrsOK(composeDatabaseName(ADDITIONAL_DBS[0]));
        
        // check that shutting down using ConnAttributes works
        assertShutdownUsingConnAttrsOK(dbName);
        // re-vive db
        getConnection();
        
        // now, actually create, and shutdown a database
        // first ensure it's not there yet
        dbName = composeDatabaseName(ADDITIONAL_DBS[1]);
        assertNoDB(dbName);
        // straightforward create and shutdown
        assertPositive(dbName);
        
        // what happens when you combine set*Database and 
        // matching connection attribute? (should work)
        dbName = composeDatabaseName(ADDITIONAL_DBS[2]);
        assertNoDB(dbName);
        assertTwiceOK(dbName);
        
        // the rest of the testing is on conflicted settings
        // the result is not defined, so a change in behavior does not 
        // necessarily indicate a bug, but may be relevant for existing apps
        // what happens when you combine create and shutdown connattr?
        // database does not get created.
        assertShutdownAndCreateConnAttr(DBNotFoundState, 
            composeDatabaseName(ADDITIONAL_DBS[3]), 
            "shutdown=true;create=true");
        assertShutdownAndCreateConnAttr(DBNotFoundState, 
            composeDatabaseName(ADDITIONAL_DBS[4]), 
            "create=true;shutdown=true");

        // and when you set both setShutdownDatabase and setCreateDatabase?
        // database does not get created
        assertConflictedSettersOK(composeDatabaseName(ADDITIONAL_DBS[5]));
        
        // what happens when you combine set*Database and
        // opposing connection attributes? database does not get created. 
        assertConflictedSetterConnAttrOK();
    }
    
    protected String composeDatabaseName(String dbName) {
        if (usingEmbedded())
            return "emb" + dbName;
        else 
            return "srv" + dbName;
    }
    
    protected void assertGetNull(String dbName) throws SQLException {
        DataSource ds = JDBCDataSource.getDataSourceLogical(dbName);
        assertNull(getBeanProperty(ds, "shutdownDatabase"));
        assertNull(getBeanProperty(ds, "createDatabase"));
    }
    
    protected void assertSetAndGet(
        String dbName, String propertyString, String setValue)
    throws SQLException {
        DataSource ds = JDBCDataSource.getDataSourceLogical(dbName);
        JDBCDataSource.setBeanProperty(ds, propertyString, setValue);
        assertEquals(setValue,getBeanProperty(ds, propertyString).toString());
    }
    
    protected void assertNotSetAndGet(
        String dbName, String propertyString, String setValue)
    throws SQLException {
        DataSource ds = JDBCDataSource.getDataSourceLogical(dbName);
        JDBCDataSource.setBeanProperty(ds, propertyString, setValue);
        assertNull(getBeanProperty(ds, propertyString));
    }
    
    protected void assertReset(String dbName) 
    throws SQLException {
        DataSource ds = JDBCDataSource.getDataSourceLogical(dbName);

        JDBCDataSource.setBeanProperty(ds, "createDatabase", "");
        assertNull(getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        assertEquals("create", getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "boo");
        assertNull(getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        assertEquals("create", getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "false");
        assertNull(getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        assertEquals("create", getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "");
        assertNull(getBeanProperty(ds, "createDatabase"));
        try { 
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "");
        assertNull(getBeanProperty(ds, "shutdownDatabase"));
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        assertEquals("shutdown", getBeanProperty(ds, "shutdownDatabase"));
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "boo");
        assertNull(getBeanProperty(ds, "shutdownDatabase"));
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "false");
        assertNull(getBeanProperty(ds, "shutdownDatabase"));
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        assertEquals("shutdown", getBeanProperty(ds, "shutdownDatabase"));
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "");
        assertNull(getBeanProperty(ds, "shutdownDatabase"));
        try { 
            JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static Object getBeanProperty(Object ds, String propertyString)
    {
        String getterName = getGetterName(propertyString);

        // Base the type of the setter method from the value's class.
        Object retObject=null;
        try {
            Method getter = ds.getClass().getMethod(getterName, null);
            retObject = getter.invoke(ds, null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        return retObject;
    }

    private static String getGetterName(String attribute) {
        return "get" + Character.toUpperCase(attribute.charAt(0))
        + attribute.substring(1);
    }
    
    // if the connattr parameter is true, we set both setShutdownDatabase
    // and ConnectionAttribute shutdown=true.
    protected void assertShutdownUsingSetOK(String dbName, boolean connAttr)
    throws SQLException {

        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        if (connAttr)
            JDBCDataSource.setBeanProperty(
                ds, "ConnectionAttributes", "shutdown=true");
        assertDSConnectionFailed("08006", ds);
    }
    
    // for completeness' sake, test create=true conn attr.
    protected void assertCreateUsingConnAttrsOK(String dbName)
    throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(
                ds, "ConnectionAttributes", "create=true");
        assertUpdateCount(
            ds.getConnection().createStatement(), 0, "set schema APP");
        JDBCDataSource.clearStringBeanProperty(ds, "ConnectionAttributes");
        assertShutdownUsingSetOK(dbName, false);
    }
    
    protected void assertShutdownUsingConnAttrsOK(String dbName)
    throws SQLException {
        DataSource ds = JDBCDataSource.getDataSourceLogical(dbName);
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", "shutdown=true");
        assertDSConnectionFailed("08006", ds);
    }

    protected void assertShutdownAndCreateConnAttr(
        String expectedSQLState, String dbName, String twoPropertyString)
    throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", twoPropertyString);
        assertDSConnectionFailed(expectedSQLState, ds);
    }
    
    protected void assertDSConnectionFailed(
        String expectedSQLState, DataSource ds) throws SQLException {
        try {
            ds.getConnection();
            fail("expected an sqlexception " + expectedSQLState);
        } catch (SQLException sqle) {
            assertSQLState(expectedSQLState, sqle);
        }
    }    
    
    protected void assertNoDB(String dbName) throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        assertDSConnectionFailed(DBNotFoundState, ds);
    }
    
    protected void assertPositive(String dbName) throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "CreateDatabase", "create");
        // check that the db exists; execute an unnecessary, but harmless, stmt
        assertUpdateCount(
            ds.getConnection().createStatement(), 0, "set schema APP");
        JDBCDataSource.clearStringBeanProperty(ds, "CreateDatabase");
        assertShutdownUsingSetOK(dbName, false);
    }

    protected void assertTwiceOK(String dbName) throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "CreateDatabase", "create");
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", "create=true");
        // check that the db exists; execute an unnecessary, but harmless, stmt
        assertUpdateCount(
            ds.getConnection().createStatement(), 0, "set schema APP");
        JDBCDataSource.clearStringBeanProperty(ds, "CreateDatabase");
        JDBCDataSource.clearStringBeanProperty(ds, "ConnectionAttributes");
        assertShutdownUsingSetOK(dbName, true);
    }
    
    protected void assertConflictedSettersOK(String dbName) throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "CreateDatabase", "create");
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            ds.getConnection();
        } catch (SQLException se) {
            assertSQLState(DBNotFoundState, se);
        }
    }

    protected void assertConflictedSetterConnAttrOK() 
    throws SQLException {
        assertConSetOK(DBNotFoundState, composeDatabaseName(ADDITIONAL_DBS[6]), 
            "shutdown=true", "CreateDatabase", "create");
        // with the new networkserver methods, this actually works...
        assertConSetOK(DBNotFoundState, composeDatabaseName(ADDITIONAL_DBS[7]),
            "create=true", "ShutdownDatabase", "shutdown");
        assertSetConOK(DBNotFoundState, composeDatabaseName(ADDITIONAL_DBS[8]), 
            "shutdown=true", "CreateDatabase", "create");
        // with the new networkserver methods, this actually works...
        assertSetConOK(DBNotFoundState, composeDatabaseName(ADDITIONAL_DBS[9]),
            "create=true", "ShutdownDatabase", "shutdown");
    }
    
    // first sets setCreate/ShutdownDB, then sets ConnectionAttributes
    protected void assertConSetOK(String expectedSQLState, String dbName, 
        String connAttrValue, String setter, String setValue) 
    throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, setter, setValue);
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", connAttrValue);
        // check that the db exists; execute an unnecessary, but harmless, stmt
        try {
            ds.getConnection();
        } catch (SQLException se) {
            assertSQLState(expectedSQLState, se);
        }
        JDBCDataSource.clearStringBeanProperty(ds, setter);
        JDBCDataSource.clearStringBeanProperty(ds, "ConnectionAttributes");
    }

    // sets ConnectionAttributes first, then SetCreate/ShutdownDB
    protected void assertSetConOK(String expectedSQLState, String dbName, 
        String connAttrValue, String setter, String setValue) 
    throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", connAttrValue);
        JDBCDataSource.setBeanProperty(ds, setter, setValue);
        // check that the db exists; execute an unnecessary, but harmless, stmt
        try {
            ds.getConnection();
        } catch (SQLException se) {
            assertSQLState(expectedSQLState, se);
        }
        JDBCDataSource.clearStringBeanProperty(ds, "ConnectionAttributes");
        JDBCDataSource.clearStringBeanProperty(ds, setter);
    }
}
