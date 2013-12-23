/*
  Class org.apache.derbyTesting.functionTests.tests.store.Derby5234Test

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to you under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

*/

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Regression test for DERBY-5234.
 */
public class Derby5234Test extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // this number of rows will force Derby to grab a second allocation page for the table
    private static  final   long    ITERATIONS = 12500;

    // highest row count which does NOT trip the bug
    private static  final   long    MAX_KEY_PER_FIRST_EXTENT = 10217L;

    private static  final   int     VARCHAR_LENGTH = 2000;
    private static  final   String  SEED = "0123456789";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public Derby5234Test(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Create a suite of tests. */
    public static Test suite()
    {
        return TestConfiguration.defaultSuite( Derby5234Test.class );
    }

    // do this for each test case
    protected void setUp() throws Exception
    {
        super.setUp();
        
        goodStatement
            (
             getConnection(),
             "create table t5234( a bigint, b varchar( " + VARCHAR_LENGTH + " ) )"
             );
    }
    protected void tearDown() throws Exception
    {
        goodStatement
            (
             getConnection(),
             "drop table t5234"
             );
        
        super.tearDown();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Basic test case for DERBY-5234. Test that the last allocation page
     * remembers which pages have been released to the operating system.
     * </p>
     */
    public void test_01_basic() throws Exception
    {
        vetBasic( ITERATIONS );
    }
    private void vetBasic( long rowCount ) throws Exception
    {
        Connection  conn = getConnection();

        // this makes the test run faster
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit( false );

        insertRows( conn, rowCount );
        deleteRows( conn );
        compressTable( conn );

        // the bug would cause the second round of insertions to write
        // beyond the end of the file
        insertRows( conn, rowCount );
        
        conn.setAutoCommit( oldAutoCommit );
    }
    /** Fill the table with enough rows to force Derby to grab a second allocation page */
    private void    insertRows( Connection conn, long iterations )
        throws Exception
    {
        PreparedStatement insert = chattyPrepare
            (
             conn,
             "insert into t5234( a, b ) values ( ?, ? )"
             );
        String          varcharValue = makeVarcharValue();

        long    percent = 0L;
        for ( long i = 0; i < iterations; i++)
        {
            if ( (i * 10) / iterations  > percent)
            {
                conn.commit();
                percent++;
            }

            insert.setLong( 1, i );
            insert.setString( 2, varcharValue );
            insert.executeUpdate();
        }
        
        conn.commit();
    }
    private String    makeVarcharValue()
    {
        StringBuffer    buffer = new StringBuffer();
        int                 count = VARCHAR_LENGTH / SEED.length();

        for ( int i = 0; i < count; i++ ) { buffer.append( SEED ); }

        return buffer.toString();
    }
    private void deleteRows( Connection conn )
        throws Exception
    {
        goodStatement
            (
             conn,
             "delete from t5234"
             );
        
        conn.commit();
    }
    private void compressTable( Connection conn )
        throws Exception
    {
        goodStatement
            (
             conn,
             "call syscs_util.syscs_inplace_compress_table( 'APP', 'T5234', 0, 0, 1 )"
             );

        conn.commit();
    }

    /**
     * <p>
     * Test with the highest row count which did NOT trip the bug.
     * </p>
     */
    public void test_02_maxOK() throws Exception
    {
        vetBasic( MAX_KEY_PER_FIRST_EXTENT );
    }
    
    /**
     * <p>
     * Test with one more than the highest good value.
     * </p>
     */
    public void test_03_triggeringEdge() throws Exception
    {
        vetBasic( MAX_KEY_PER_FIRST_EXTENT + 1L );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // HELPER METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

}
