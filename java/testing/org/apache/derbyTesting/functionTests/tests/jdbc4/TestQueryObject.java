/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestQueryObject
 
   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import javax.sql.DataSource;
import java.sql.*;
import org.apache.derby.jdbc.ClientDataSource40;
import org.apache.derby.jdbc.EmbeddedDataSource40;

/**
 * This class tests QueryObjectGenerator feature introduced in jdbc 4.0
 */

public class TestQueryObject {      

    private static final int RECORD_COUNT = 10;
    /**
     * create table and insert data 
     */
    
    public static void initDB (Connection con) throws Exception {
        Statement stmt = con.createStatement ();
        stmt.execute ("create table querytable (id integer, data varchar (20))");
        stmt.close ();
        PreparedStatement pstmt = con.prepareStatement ("insert into querytable"
                        + "(id, data) values (?,?)");
        for (int i = 0; i < RECORD_COUNT; i++) {
            pstmt.setInt (1, i);
            pstmt.setString (2, "data" + i);
            pstmt.execute();
        }
        pstmt.close ();
    }    
    
    /**
     * Tests Connection.createQueryObject
     * @param con 
     */
    public static void testConnectionQuery (Connection con) throws Exception {
        TestQuery query = con.createQueryObject (TestQuery.class);
        if (query.getAllData().size() != RECORD_COUNT)
            System.out.println ("expected result size 10 actual " 
                    + query.getAllData().size());
        query.close();
    }
    
    /**
     * Tests DataSource.createQueryObject
     * @param ds 
     */
     public static void testDSQuery (DataSource ds) throws Exception {
        TestQuery query = ds.createQueryObject (TestQuery.class);
        if (query.getAllData().size() != RECORD_COUNT)
            System.out.println ("expected result size 10 actual size:" 
                    + query.getAllData().size());
        query.close();
    }
     
     public static void doTest (DataSource ds) {
         try {
            //this part needs to be removed while migrating
            //this test to junit                         
            Connection con = ds.getConnection();            
            con.setAutoCommit (true);
            initDB (con);
            testConnectionQuery (con);
            con.close ();
            testDSQuery (ds);
        }
        catch (Exception e) {
            e.printStackTrace ();
        }
     }         

	/**
	 * <p>
	 * Return true if we're running under the embedded client.
	 * </p>
	 */
	private	static	boolean	usingEmbeddedClient()
	{
		return "embedded".equals( System.getProperty( "framework" ) );
	}

    public static void main (String [] args) {
        //this part needs to be removed while migrating
        //this test to junit

		DataSource	ds;

		if ( usingEmbeddedClient() )
		{
			EmbeddedDataSource40 eds = new EmbeddedDataSource40 ();
			eds = new EmbeddedDataSource40 ();
			eds.setDatabaseName ("embedquerydb");
			eds.setCreateDatabase ("create");

			ds = eds;
		}
		else // DerbyNetClient
		{
			ClientDataSource40 clds = new ClientDataSource40 ();
			clds = new ClientDataSource40 ();
			clds.setDatabaseName ("netquerydb;create=true");
        	clds.setServerName ("localhost");
			clds.setPortNumber (1527);

			ds = clds;
		}
		
        doTest (ds);
    }
}
