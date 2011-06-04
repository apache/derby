/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.BasicSetup

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

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Basic fixtures and setup for the upgrade test, not
 * tied to any specific release.
 */
public class BasicSetup extends UpgradeChange {
    
    public static Test suite() {
        TestSuite suite = new TestSuite("Upgrade basic setup");
        
        suite.addTestSuite(BasicSetup.class);
        
        return suite;
    }

    public BasicSetup(String name) {
        super(name);
    }
    
    /**
     * Simple test of the triggers. Added for DERBY-4835
     */
    public void testTriggerBasic() throws SQLException
    {
        Statement s = createStatement();
        switch (getPhase())
        {
        case PH_CREATE:
            s.executeUpdate("CREATE TABLE Trigger_t1 " +
            		"(c1 INTEGER NOT NULL GENERATED ALWAYS " +
            		"AS IDENTITY (START WITH 1, INCREMENT BY 1), " +
            		"max_size INTEGER NOT NULL, "+
            		"CONSTRAINT c1_pk PRIMARY KEY (c1))");
            s.executeUpdate("CREATE TABLE Trigger_t2 "+
            		"(c1 INTEGER DEFAULT 0 NOT NULL)");
            s.executeUpdate("CREATE TRIGGER gls_blt_trg "+
            		"AFTER INSERT ON Trigger_t1 FOR EACH ROW MODE DB2SQL "+
            		"INSERT INTO Trigger_t2(c1) "+
            		"VALUES ( (select max(c1) from Trigger_t1))");
            s.executeUpdate("INSERT INTO Trigger_t1(max_size) "+
            		"VALUES(20)");
            break;
        case PH_SOFT_UPGRADE:
            s.executeUpdate("INSERT INTO Trigger_t1(max_size) "+
    		"VALUES(20)");
            break;
        case PH_POST_SOFT_UPGRADE:
            s.executeUpdate("INSERT INTO Trigger_t1(max_size) "+
    		"VALUES(20)");
            break;
        case PH_HARD_UPGRADE:
            s.executeUpdate("INSERT INTO Trigger_t1(max_size) "+
    		"VALUES(20)");
            break;
        }
        s.close();
    }

    /**
     * Simple test of the old version from the meta data.
     */
    public void testOldVersion() throws SQLException
    {              
        switch (getPhase())
        {
        case PH_CREATE:
        case PH_POST_SOFT_UPGRADE:
            DatabaseMetaData dmd = getConnection().getMetaData();
            assertEquals("Old major (driver): ",
                    getOldMajor(), dmd.getDriverMajorVersion());
            assertEquals("Old minor (driver): ",
                    getOldMinor(), dmd.getDriverMinorVersion());
            assertEquals("Old major (database): ",
                    getOldMajor(), dmd.getDatabaseMajorVersion());
            assertEquals("Old minor (database): ",
                    getOldMinor(), dmd.getDatabaseMinorVersion());
            break;
        }
    }
    
    /**
     * Test general DML. Just execute some INSERT/UPDATE/DELETE
     * statements in all phases to see that generally the database works.
     * @throws SQLException
     */
    public void testDML() throws SQLException {
        
        final int phase = getPhase();
        
        Statement s = createStatement();
        
        switch (phase) {
        case PH_CREATE:
            s.executeUpdate("CREATE TABLE PHASE" +
                                                "(id INT NOT NULL, ok INT)");
            s.executeUpdate("CREATE TABLE TABLE1" +
                        "(id INT NOT NULL PRIMARY KEY, name varchar(200))");
            break;
        case PH_SOFT_UPGRADE:
            break;
        case PH_POST_SOFT_UPGRADE:
            break;
        case PH_HARD_UPGRADE:
            break;
        }
        s.close();
    
        PreparedStatement ps = prepareStatement(
                "INSERT INTO PHASE(id) VALUES (?)");
        ps.setInt(1, phase);
        ps.executeUpdate();
        ps.close();
        
        ps = prepareStatement("INSERT INTO TABLE1 VALUES (?, ?)");
        for (int i = 1; i < 20; i++)
        {
            ps.setInt(1, i + (phase * 100));
            ps.setString(2, "p" + phase + "i" + i);
            ps.executeUpdate();
        }
        ps.close();
        ps = prepareStatement("UPDATE TABLE1 set name = name || 'U' " +
                                    " where id = ?");
        for (int i = 1; i < 20; i+=3)
        {
            ps.setInt(1, i + (phase * 100));
            ps.executeUpdate();
        }
        ps.close();
        ps = prepareStatement("DELETE FROM TABLE1 where id = ?");
        for (int i = 1; i < 20; i+=4)
        {
            ps.setInt(1, i + (phase * 100));
            ps.executeUpdate();
        }
        ps.close();
        commit();
    }

    /**
     * Make sure table created in soft upgrade mode can be 
     * accessed after shutdown.  DERBY-2931
     * @throws SQLException
     */
    public void testCreateTable() throws SQLException
    {
        
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("DROP table t");
        } catch (SQLException se) {
            // ignore table does not exist error on
            // on drop table.
            assertSQLState("42Y55",se ); 
        }
        stmt.executeUpdate("CREATE TABLE T (I INT)");
        TestConfiguration.getCurrent().shutdownDatabase();
        stmt = createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * from t");
        JDBC.assertEmpty(rs);  
        rs.close();
    }
    

    /**
     * Test table with index can be read after
     * shutdown DERBY-2931
     * @throws SQLException
     */
    public void testIndex() throws SQLException 
    {
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("DROP table ti");
        } catch (SQLException se) {
            // ignore table does not exist error on
            // on drop table.
            assertSQLState("42Y55",se ); 
        }
        stmt.executeUpdate("CREATE TABLE TI (I INT primary key not null)");
        stmt.executeUpdate("INSERT INTO  TI values(1)");
        stmt.executeUpdate("INSERT INTO  TI values(2)");
        stmt.executeUpdate("INSERT INTO  TI values(3)");
        TestConfiguration.getCurrent().shutdownDatabase();
        stmt = createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * from TI ORDER BY I");
        JDBC.assertFullResultSet(rs, new String[][] {{"1"},{"2"},{"3"}});
        rs.close();        
    }

    
    /**
     * Ensure that after hard upgrade (with the old version)
     * we can no longer connect to the database.
     */
    public void noConnectionAfterHardUpgrade()
    {              
        switch (getPhase())
        {
        case PH_POST_HARD_UPGRADE:
            try {
                    getConnection();
                } catch (SQLException e) {
                    // Check the innermost of the nested exceptions
                    SQLException sqle = getLastSQLException(e);
                    String sqlState = sqle.getSQLState();
                	// while beta, XSLAP is expected, if not beta, XSLAN
                	if (!(sqlState.equals("XSLAP")) && !(sqlState.equals("XSLAN")))
                		fail("expected an error indicating no connection");
                }
            break;
        }
    }
    final   int TEST_COUNT = 0;
    final   int FAILURES = TEST_COUNT + 1;
    final   String  A_COL = "a";
    final   String  B_COL = "b";

    //This test has been contributed by Rick Hillegas for DERBY-5121
    // The test exhaustively walks through all subsets and permutations 
    // of columns for a trigger which inserts into a side table based on 
    // updates to a master table.
    public void testExhuastivePermutationOfTriggerColumns() throws Exception
    {
        final   int STATUS_COUNTERS = FAILURES + 1;
        int columnCount = 3;
        int[][]   powerSet = constructPowerSet( columnCount );
        int[][] permutations = permute( powerSet );
        int[]   statusCounters = new int[ STATUS_COUNTERS ];

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            for ( int triggerCols = 0; triggerCols < powerSet.length; triggerCols++ )
            {
                for ( int perm = 0; perm < permutations.length; perm++ )
                {
                    createT1( powerSet[ triggerCols ], permutations[ perm ] );
                    createT2( columnCount, powerSet[ triggerCols ], permutations[ perm ]  );
                    createTrigger( powerSet[ triggerCols ], permutations[ perm ] );
                }
            }
        	break;

        case PH_SOFT_UPGRADE:
            for ( int triggerCols = 0; triggerCols < powerSet.length; triggerCols++ )
            {
                for ( int perm = 0; perm < permutations.length; perm++ )
                {
                    for ( int i = 0; i < permutations.length; i++ )
                    {
                        runTrigger( statusCounters, columnCount, powerSet[ triggerCols ], permutations[ perm ], permutations[ i ] );
                    }
                }
            }
        	break;
        }
        summarize( statusCounters );
    }
    
    //Start of helper methods for testExhuastivePermutationOfTriggerColumns

    ////////////////////////
    //
    // make power set of N
    //
    ////////////////////////

    private int[][] constructPowerSet( int count )
    {
    	java.util.ArrayList list = new java.util.ArrayList();
        boolean[]           inclusions = new boolean[ count ];

        include( list, 0, inclusions );
        
        int[][] result = new int[ list.size() ][];
        list.toArray( result );

        return result;
    }

    private void    include( ArrayList list, int idx, boolean[] inclusions )
    {
        if ( idx >= inclusions.length )
        {
            int totalLength = inclusions.length;
            int count = 0;
            for ( int i = 0; i < totalLength; i++ )
            {
                if ( inclusions[ i ] ) { count++; }
            }

            if ( count > 0 )
            {
                int[]   result = new int[ count ];
                int     index = 0;
                for ( int i = 0; i < totalLength; i++ )
                {
                    if ( inclusions[ i ] ) { result[ index++ ] = i; }
                }
                
                list.add( result );
            }

            return;
        }

        include( list, idx, inclusions, false );
        include( list, idx, inclusions, true );
    }

    private void    include( ArrayList list, int idx, boolean[] inclusions, boolean currentCell )
    {
        inclusions[ idx++ ] = currentCell;

        // this is where the recursion happens
        include( list, idx, inclusions );
    }

    ////////////////////////////////////////////////
    //
    // create all permutations of an array of numbers
    //
    ////////////////////////////////////////////////
    private int[][] permute( int[][] original )
    {
        ArrayList list = new ArrayList();

        for ( int i = 0; i < original.length; i++ )
        {
            permute( list, new int[0], original[ i ] );
        }
        
        int[][] result = new int[ list.size() ][];
        list.toArray( result );

        return result;
    }

    private void   permute( ArrayList list, int[] start, int[] remainder )
    {
        int startLength = start.length;
        int remainderLength = remainder.length;
        
        for ( int idx = 0; idx < remainder.length; idx++ )
        {
            int[] newStart = new int[ startLength + 1 ];
            for ( int i = 0; i < startLength; i++ ) { newStart[ i ] = start[ i ]; }
            newStart[ startLength ] = remainder[ idx ];

            if ( remainderLength <= 1 ) { list.add( newStart ); }
            else
            {
                int[]   newRemainder = new int[ remainderLength - 1 ];
                int     index = 0;
                for ( int i = 0; i < remainderLength; i++ )
                {
                    if ( i != idx ) { newRemainder[ index++ ] = remainder[ i ]; }
                }

                // this is where the recursion happens
                permute( list, newStart, newRemainder );
            }
        }   // end loop through all remainder elements
    }

    private String  columnName( String stub, int idx ) { return (stub + '_' + idx ); }

    private void createT1(int[] triggerCols, int[] permutation )
    throws Exception
    {
        StringBuffer   buffer = new StringBuffer();
        buffer.append( "create table " + makeTableName( "t1", triggerCols, permutation ) + "( " );
        for ( int i = 0; i < permutation.length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName( B_COL, i ) );
            buffer.append( " int" );
        }
        buffer.append( " )" );
        Statement s = createStatement();
        s.execute(buffer.toString());
    }    
    
    private void    createT2(int columnCount, int[] triggerCols, int[] permutation  )
    throws Exception
    {
        StringBuffer   buffer = new StringBuffer();
        buffer.append( "create table " + makeTableName( "t2", triggerCols, permutation ) + "( " );
        for ( int i = 0; i < columnCount; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName( A_COL, i ) );
            buffer.append( " int" );
        }
        buffer.append( " )" );
        Statement s = createStatement();
        s.execute(buffer.toString());
    }

    private String  makeTableName( String stub, int[] triggerCols, int[] permutation )
    {
        StringBuffer   buffer = new StringBuffer();

        buffer.append( stub );
        for ( int i = 0; i < triggerCols.length; i++ )
        {
            buffer.append( "_" );
            buffer.append( triggerCols[ i ] );
        }
       buffer.append( "__" );
        for ( int i = 0; i < permutation.length; i++ )
        {
            buffer.append( "_" );
            buffer.append( permutation[ i ] );
        }

        return buffer.toString();
    }

    private void    createTrigger(int[] triggerCols, int[] permutation )
    throws Exception
    {
        boolean modeDb2SqlOptional = oldAtLeast(10, 3);
        StringBuffer   buffer = new StringBuffer();
        buffer.append( "create trigger " + makeTriggerName( "UTrg", triggerCols, permutation ) + " after update of " );
        for ( int i = 0; i < triggerCols.length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName( A_COL, triggerCols[ i ] ) );
        }
        		
        buffer.append( "\n\ton " + makeTableName( "t2", triggerCols, permutation ) + " referencing new as nr for each row " );
        buffer.append( modeDb2SqlOptional?"":"\n\tMODE DB2SQL ");
        buffer.append( "\n\tinsert into " + makeTableName( "t1", triggerCols, permutation ) + " values ( " );
        for ( int i = 0; i < permutation.length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( "nr." + columnName( A_COL, permutation[ i ] ) );
        }
        buffer.append( " )" );

        Statement s = createStatement();
        s.execute(buffer.toString());
    }

    private String  makeTriggerName( String stub, int[] triggerCols, int[] permutation )
    {
        StringBuffer   buffer = new StringBuffer();

        buffer.append( stub );
        for ( int i = 0; i < triggerCols.length; i++ )
        {
            buffer.append( "_" );
            buffer.append( triggerCols[ i ] );
        }
        buffer.append( "__" );
        for ( int i = 0; i < permutation.length; i++ )
        {
            buffer.append( "_" );
            buffer.append( permutation[ i ] );
        }
        
        return buffer.toString();
    }

    private int[]   getResults( int rowLength, String text )
        throws Exception
    {
        PreparedStatement   ps = prepareStatement(text );
        ResultSet               rs = ps.executeQuery();

        if ( !rs.next() ) { return new int[0]; }

        int[]                       result = new int[ rowLength ];
        for ( int i = 0; i < rowLength; i++ )
        {
            result[ i ] = rs.getInt( i + 1 );
        }

        rs.close();
        ps.close();

        return result;
    }

    private boolean overlap( int[] left, int[] right )
    {
        for ( int i = 0; i < left.length; i++ )
        {
            for ( int j = 0; j < right.length; j++ )
            {
                if ( left[ i ] == right[ j ] )
                {
                    //println( true, stringify( left ) + " overlaps " + stringify( right ) );
                    return true;
                }
            }
        }

        //println( true, stringify( left ) + " DOES NOT overlap " + stringify( right ) );
        return false;
    }

    private void    vetData
    ( int[] statusCounters, int[] triggerCols, int[] permutation, int[] updateColumns, String updateStatement )
    throws Exception
    {
        String  t1Name = makeTableName( "t1", triggerCols, permutation );
        String  t2Name = makeTableName( "t2", triggerCols, permutation );
        int     rowLength = permutation.length;
        int[]   t1Row = getResults( rowLength, "select * from " + t1Name );

        if ( !overlap( triggerCols, updateColumns ) )
        {
            if ( t1Row.length != 0 )
            {
                fail
                    (
                     statusCounters,
                     triggerCols,
                     permutation,
                     updateColumns,
                     "No row should have been inserted into t1! updateStatement = '" + updateStatement + "' and t1Row = " + stringify( t1Row )
                     );
            }

            return;
        }
        
        StringBuffer   buffer = new StringBuffer();
        buffer.append( "select " );
        for ( int i = 0; i < permutation.length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName( A_COL, permutation[ i ] ) );
        }
        buffer.append( " from " + t2Name );
        int[]   t2Row = getResults( rowLength, buffer.toString() );

        if ( !stringify( t1Row ).equals( stringify( t2Row ) ) )
        {
            String  detail = "Wrong data inserted into t1! " +
                "updateStatement = '" + updateStatement + "'. " +
                "Expected " + stringify( t2Row ) +
                " but found " + stringify( t1Row );
                
            fail( statusCounters, triggerCols, permutation, updateColumns, detail );
        }
    }

    private void    runTrigger( int[] statusCounters, int columnCount, int[] triggerCols, int[] permutation, int[] updateColumns )
    throws Exception
    {
        statusCounters[ TEST_COUNT ]++;

        loadData( columnCount, triggerCols, permutation );
        String  updateStatement = updateData( statusCounters, triggerCols, permutation, updateColumns );
        vetData( statusCounters, triggerCols, permutation, updateColumns, updateStatement );
    }

    private void    loadData( int columnCount, int[] triggerCols, int[] permutation )
    throws Exception
    {
        String  t1Name = makeTableName( "t1", triggerCols, permutation );
        String  t2Name = makeTableName( "t2", triggerCols, permutation );
        Statement s = createStatement();
        s.execute("delete from " + t1Name);
        s.execute("delete from " + t2Name);
        
        StringBuffer   buffer = new StringBuffer();
        buffer.append( "insert into " + t2Name + " values ( " );
        for ( int i = 0; i < columnCount; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( i );
        }
        buffer.append( " )" );
        s.execute(buffer.toString());
    }
    
    private String    updateData( int[] statusCounters, int[] triggerCols, int[] permutation, int[] updateColumns )
    throws Exception
    {
        String  t2Name = makeTableName( "t2", triggerCols, permutation );

        StringBuffer   buffer = new StringBuffer();
        buffer.append( "update " + t2Name + " set " );
        for ( int i = 0; i < updateColumns.length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName( A_COL, updateColumns[ i ] ) );
            buffer.append( " = " );
            buffer.append( (100 + i) );
        }

        String  updateStatement = buffer.toString();

        try {
            Statement s = createStatement();
            s.execute(updateStatement);
        }
        catch (SQLException se)
        {
            fail
                (
                 statusCounters,
                 triggerCols,
                 permutation,
                 updateColumns,
                 "Update statement failed! updateStatement = '" + updateStatement
                 );
        }

        return updateStatement;
    }

    private void    fail( int[] statusCounters, int[] triggerCols, int[] permutation, int[] updateColumns, String detail )
    {
        statusCounters[ FAILURES ]++;
        
        String  message = "FAILED for triggerCols = " +
            stringify( triggerCols ) +
            " and permutation = " + stringify( permutation ) +
            " and updateColumns = " + stringify( updateColumns ) +
            ". " + detail;

        System.out.println( message );
    }
    
    private void    summarize( int[] statusCounters )
    {
        int testCount = statusCounters[ TEST_COUNT ];
        int failures = statusCounters[ FAILURES ];

        if ( failures != 0 )
        {
        	System.out.println( "FAILURE! " + testCount + " test cases run, of which " + failures + " failed." );
        }
    }

    private String    stringify( int[][] array )
    {
        StringBuffer   buffer = new StringBuffer();

        buffer.append( "[" );
        for ( int i = 0; i < array.length; i++ )
        {
            buffer.append( "\n\t" );
            buffer.append( stringify( array[ i ] ) );
        }
        buffer.append( "\n]\n" );

        return buffer.toString();
    }

    private String  stringify( int[] array )
    {
        StringBuffer   buffer = new StringBuffer();

        buffer.append( "[" );
        for ( int j = 0; j < array.length; j++ )
        {
            if ( j > 0 ) { buffer.append( ", " ); }
            buffer.append( array[ j ] );
        }
        buffer.append( "]" );

        return buffer.toString();
    }
    //End of helper methods for testExhuastivePermutationOfTriggerColumns
}
