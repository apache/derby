/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_4

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SupportFilesSetup;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Upgrade test cases for 10.4.
 * If the old version is 10.4 or later then these tests
 * will not be run.
 * <BR>
    10.4 Upgrade issues

    <UL>
    <LI> testMetaDataQueryRunInSYScompilationSchema - DERBY-2946 
    Make sure that metadata queries get run with SYS schema as the current 
    compilation schema rather than a user schema as the current compilation 
    schema. This is because if the user is inside a user schema in a collated 
    database and if the meta data query gets run inside the user schema, then 
    we will run into collation mismatch errors for a subclause like following 
    in the WHERE clause.
    P.SELECTPRIV = 'Y' 
    The reason for error is that the left hand side of the = operation will 
    have collation type of UCS_BASIC because that column belongs to a table 
    from system schema. But the collation type of the right hand will be 
    territory based if the current compilation schema is user schema. But if 
    the current compilation schema is set to SYS schema, then right hand side 
    will also have collation of UCS_BASIC and hence there won't be any 
    collation mismatch. 
    Background info : character string constants pick up the collation of the
    current compilation schema. 
    </UL>

 */
public class Changes10_4 extends UpgradeChange {

    public Changes10_4(String name) {
        super(name);
    }
    
    /**
     * Return the suite of tests to test the changes made in 10.4.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */   
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.4");
        
        suite.addTestSuite(Changes10_4.class);
        return new SupportFilesSetup((Test) suite);
    }
    
    /**
     * Check that even though we have set schema to a user schema, the 
     * metadata queries get run with compilation schema as SYS.
     * DERBY-2946
     * Test added for 10.4.
     * @throws SQLException 
     *
     */
    public void testMetaDataQueryRunInSYScompilationSchema() throws SQLException
    {
    	//This test is for databases with territory based collation. That
    	//feature was added in 10.3 codeline and hence there is no point in
    	//doing any testing with pre-10.3 databases.
        if (!oldAtLeast(10, 3))
        	return;

        DataSource ds = JDBCDataSource.getDataSourceLogical("COLLATED_DB_10_3");
        
        switch (getPhase())
        {
        case PH_CREATE:
            // create the database if it was not already created. Note the
        	// JDBC url attributes.
            JDBCDataSource.setBeanProperty(
                    ds, "ConnectionAttributes", "create=true;territory=no;collation=TERRITORY_BASED");
            ds.getConnection().close();
            break;
            
        case PH_SOFT_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
        case PH_HARD_UPGRADE:
        case PH_POST_HARD_UPGRADE:
            Connection con = ds.getConnection();
        	//First make the current schema as a user schema. And then run a 
        	//metadata query to make sure that it runs fine. If it does (which
        	//is the expected behavior), then it will mean that the metadata
        	//query is getting run with SYS as the compilation schema rather
        	//than the current schema which is APP.
            Statement s = con.createStatement();
            s.execute("SET SCHEMA APP");

            DatabaseMetaData dmd = con.getMetaData();
            ResultSet rs = dmd.getTables(null,"APP",null,null);
            JDBC.assertDrainResults(rs);
            s.close();
            break;
        }
    }
}
