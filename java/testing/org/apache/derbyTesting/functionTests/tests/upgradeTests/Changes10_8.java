/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_8

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

import org.apache.derbyTesting.junit.SupportFilesSetup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import junit.framework.Test;
import junit.framework.TestSuite;
import java.util.ArrayList;

import org.apache.derbyTesting.junit.JDBC;


/**
 * Upgrade test cases for 10.8.
 * If the old version is 10.8 or later then these tests
 * will not be run.
 * <BR>
    10.8 Upgrade issues

    <UL>
    <LI>BOOLEAN data type support expanded.</LI>
    </UL>

 */
public class Changes10_8 extends UpgradeChange
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

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

    public Changes10_8(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Return the suite of tests to test the changes made in 10.8.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.8");

        suite.addTestSuite(Changes10_8.class);
        return new SupportFilesSetup((Test) suite);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////
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

    public void testDERBY5121TriggerTest2() throws Exception
    {
        Statement s = createStatement();
        boolean modeDb2SqlOptional = oldAtLeast(10, 3);
    	String updateSQL = "update media "+
    	"set name = 'Mon Liza', description = 'Something snarky.' " +
    	"where mediaID = 1";
        
        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        	s.execute("create table folder ( "+
        			"folderType	int	not null, folderID	int	not null, "+
        			"folderParent int, folderName varchar(50) not null)");
        	s.execute("create table media ( " +
        			"mediaID int not null, name varchar(50)	not null, "+
        			"description clob not null, mediaType varchar(50), "+
        			"mediaContents	blob, folderID int not null	default 7)");
        	s.execute("create trigger mediaInsrtDupTrgr " +
        			"after INSERT on media referencing new as nr "+
        			"for each ROW "+
        			(modeDb2SqlOptional?"":"MODE DB2SQL ") +
        			"values( nr.folderID, 7, nr.name)");
        	s.execute("create trigger mediaUpdtDupTrgr " +
        			"after UPDATE of folderID, name on media " +
        			"referencing new as nr "+
        			"for each ROW "+
        			(modeDb2SqlOptional?"":"MODE DB2SQL ") +
        			"values( nr.folderID, 7, nr.name)");
        	s.executeUpdate("insert into folder(folderType, folderID, "+
        			"folderParent, folderName ) "+
        			"values ( 7, 7, null, 'media' )");
        	s.executeUpdate("insert into media(mediaID, name, description)"+
        			"values (1, 'Mona Lisa', 'A photo of the Mona Lisa')");
        	if (oldIs(10,7,1,1))
                assertStatementError(  "XCL12", s, updateSQL );
        	else
        		s.executeUpdate(updateSQL);
        	break;

        case PH_SOFT_UPGRADE:
    		s.executeUpdate(updateSQL);
        	break;
        	
        case PH_POST_SOFT_UPGRADE:
        	//Derby 10.7.1.1 is not going to work because UPDATE sql should
        	// have read all the columns from the trigger table but it did
        	// not and hence trigger can't find the column it needs from the
        	// trigger table
        	if (oldIs(10,7,1,1))
                assertStatementError(  "S0022", s, updateSQL );
        	else
        		s.executeUpdate(updateSQL);
        	break;
        case PH_HARD_UPGRADE:
    		s.executeUpdate(updateSQL);
        	break;
        case PH_POST_HARD_UPGRADE:
    		s.executeUpdate(updateSQL);
        	s.executeUpdate("drop table media");
        	s.executeUpdate("drop table folder");
        	break;
        }
    }

    /**
     * Changes made for DERBY-1482 caused corruption which is being logged 
     *  under DERBY-5121. The issue is that the generated trigger action
     *  sql could be looking for columns (by positions, not names) in
     *  incorrect positions. With DERBY-1482, trigger assumed that the
     *  runtime resultset that they will get will only have trigger columns
     *  and trigger action columns used through the REFERENCING column.
     *  That is an incorrect assumption because the resultset could have
     *  more columns if the triggering sql requires more columns. DERBY-1482
     *  changes are in 10.7 and higher codelines. Because of this bug, the
     *  changes for DERBY-1482 have been backed out from 10.7 and 10.8
     *  codelines so they now match 10.6 and earlier releases. This in 
     *  other words means that the resultset presented to the trigger
     *  will have all the columns from the trigger table and the trigger
     *  action generated sql should look for the columns in the trigger
     *  table by their absolution column position in the trigger table.
     *  This disabling of code will make sure that all the future triggers
     *  get created correctly. The existing triggers at the time of 
     *  upgrade (to the releases with DERBY-1482 backout changes in them)
     *  will get marked invalid and when they fire next time around,
     *  the regenerated sql for them will be generated again and they
     *  will start behaving correctly. So, it is highly recommended that
     *  we upgrade 10.7.1.1 to next point release of 10.7 or to 10.8
     * @throws Exception
     */
    public void testDERBY5121TriggerDataCorruption() throws Exception
    {
        Statement s = createStatement();
        ResultSet rs;
        boolean modeDb2SqlOptional = oldAtLeast(10, 3);
        
        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        	//The following test case is for testing in different upgrade
        	// phases what happens to buggy trigger created with 10.7.1.1. 
        	// Such triggers will get fixed
        	// 1)in hard upgrade when they get fired next time around.
        	// 2)in soft upgrade if they get fired during soft upgrade session.
        	//For all the other releases, we do not generate buggy triggers
        	// and hence everything should work just fine during all phases
        	// of upgrade including the CREATE time
            s.execute("CREATE TABLE UPGRADE_tab1(id int, name varchar(20))");
            s.execute("CREATE TABLE UPGRADE_tab2(" +
            		"name varchar(20) not null, " +
            		"description int not null, id int)");
            s.execute("create trigger UPGRADE_Trg1 " +
            		"after UPDATE of name on UPGRADE_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into UPGRADE_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
            		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
        			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
        	//If we are testing 10.7.1.1, which is where DERBY-5121 was
        	// detected, we will find that the trigger did not insert
        	// the correct data thus causing the corruption. For all the
        	// earlier releases, we do not have DERBY-5121 and hence
        	// trigger will insert the correct data.
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");

        	//Following test is to test that the buggy triggers created in 
        	// 10.7.1.1 will continue to exhibit incorrect behavior if they 
        	// do not get fired during soft upgrade and the database is taken
        	// back to 10.7.1.1
            s.execute("CREATE TABLE POSTSFT_UPGRD_tab1(id int, name varchar(20))");
            s.execute("CREATE TABLE POSTSFT_UPGRD_tab2(" +
            		"name varchar(20) not null, " +
            		"description int not null, id int)");
            //We want this trigger to fire only for post hard upgrade
            s.execute("create trigger POSTSFT_UPGRD_Trg1 " +
            		"after UPDATE of name on POSTSFT_UPGRD_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into POSTSFT_UPGRD_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into POSTSFT_UPGRD_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update POSTSFT_UPGRD_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from POSTSFT_UPGRD_tab1");
        	//If we are testing 10.7.1.1, which is where DERBY-5121 was
        	// detected, we will find that the trigger did not insert
        	// the correct data thus causing the corruption. For all the
        	// earlier releases, we do not have DERBY-5121 and hence
        	// trigger will insert the correct data.
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from POSTSFT_UPGRD_tab1");
        	s.execute("delete from POSTSFT_UPGRD_tab2");

        	//Following test is to test that the buggy triggers created in
        	// 10.7.1.1 will get fixed when they get upgraded to 10.8 and 
        	// higher
            s.execute("CREATE TABLE HARD_UPGRADE_tab1(id int, name varchar(20))");
            s.execute("CREATE TABLE HARD_UPGRADE_tab2(" +
            		"name varchar(20) not null, " +
            		"description int not null, id int)");
            s.execute("create trigger HARD_UPGRADE_Trg1 " +
            		"after UPDATE of name on HARD_UPGRADE_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into HARD_UPGRADE_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into HARD_UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update HARD_UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from HARD_UPGRADE_tab1");
        	//If we are testing 10.7.1.1, which is where DERBY-5121 was
        	// detected, we will find that the trigger did not insert
        	// the correct data thus causing the corruption. For all the
        	// earlier releases, we do not have DERBY-5121 and hence
        	// trigger will insert the correct data.
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from HARD_UPGRADE_tab1");
        	s.execute("delete from HARD_UPGRADE_tab2");

        	//Following test is to test that the buggy triggers created in
        	// 10.7.1.1 will get fixed when they get upgraded to 10.8 and 
        	// higher even if they did not get fired during the session which
        	// did the upgrade
            s.execute("CREATE TABLE POSTHRD_UPGRD_tab1(id int, name varchar(20))");
            s.execute("CREATE TABLE POSTHRD_UPGRD_tab2(" +
            		"name varchar(20) not null, " +
            		"description int not null, id int)");
            //We want this trigger to fire only for post hard upgrade
            s.execute("create trigger POSTHRD_UPGRD_Trg1 " +
            		"after UPDATE of name on POSTHRD_UPGRD_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into POSTHRD_UPGRD_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into POSTHRD_UPGRD_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update POSTHRD_UPGRD_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from POSTHRD_UPGRD_tab1");
        	//If we are testing 10.7.1.1, which is where DERBY-5121 was
        	// detected, we will find that the trigger did not insert
        	// the correct data thus causing the corruption. For all the
        	// earlier releases, we do not have DERBY-5121 and hence
        	// trigger will insert the correct data.
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from POSTHRD_UPGRD_tab1");
        	s.execute("delete from POSTHRD_UPGRD_tab2");
            break;
            
        case PH_SOFT_UPGRADE:
        	//Following test case shows that the buggy trigger created in
        	// 10.7.1.1 got fixed when it got fired in soft upgrade mode
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");
        	s.execute("drop trigger UPGRADE_Trg1");

        	//Following test case shows that the trigger created during
        	// soft upgrade mode behave correctly and will not exhibit
        	// the buggy behavior of 10.7.1.1
        	s.execute("create trigger UPGRADE_Trg1 " +
            		"after UPDATE of name on UPGRADE_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into UPGRADE_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
            		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");
            break;

        case PH_POST_SOFT_UPGRADE: 
        	//Following test shows that because the buggy trigger created in
        	// 10.7.1.1 was fired during the soft upgrade mode, it has gotten
        	// fixed and it will work correctly in all the releaes
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");
        	s.execute("drop trigger UPGRADE_Trg1");

        	//Following test case says that if we are back to 10.7.1.1 after
        	// soft upgrade, we will continue to create buggy triggers. The
        	// only solution to this problem is to upgrade to a release that
        	// fixes DERBY-5121
        	s.execute("create trigger UPGRADE_Trg1 " +
            		"after UPDATE of name on UPGRADE_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into UPGRADE_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
            		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
        	//If we are testing 10.7.1.1, which is where DERBY-5121 was
        	// detected, we will find that the trigger did not insert
        	// the correct data thus causing the corruption. For all the
        	// earlier releases, we do not have DERBY-5121 and hence
        	// trigger will insert the correct data.
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");

        	//Following shows that the triggers that didn't get fired during
        	// soft upgrade will continue to exhibit incorrect behavior in
        	// 10.7.1.1. The only solution to this problem is to upgrade to a 
        	// release that fixes DERBY-5121
        	//load data into trigger table
            s.execute("insert into POSTSFT_UPGRD_tab2(name,description) "+
            		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update POSTSFT_UPGRD_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from POSTSFT_UPGRD_tab1");
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from POSTSFT_UPGRD_tab1");
        	s.execute("delete from POSTSFT_UPGRD_tab2");

        	//Following shows that the triggers that didn't get fired during
        	// soft upgrade will continue to exhibit incorrect behavior in
        	// 10.7.1.1. The only solution to this problem is to upgrade to a 
        	// release that fixes DERBY-5121
            //load data into trigger table
            s.execute("insert into HARD_UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update HARD_UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from HARD_UPGRADE_tab1");
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from HARD_UPGRADE_tab1");
        	s.execute("delete from HARD_UPGRADE_tab2");

        	//Following shows that the triggers that didn't get fired during
        	// soft upgrade will continue to exhibit incorrect behavior in
        	// 10.7.1.1. The only solution to this problem is to upgrade to a 
        	// release that fixes DERBY-5121
            //load data into trigger table
            s.execute("insert into POSTHRD_UPGRD_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update POSTHRD_UPGRD_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from POSTHRD_UPGRD_tab1");
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from POSTHRD_UPGRD_tab1");
        	s.execute("delete from POSTHRD_UPGRD_tab2");
            break;
            
        case PH_HARD_UPGRADE:
        	//Following test shows that the buggy trigger created with 10.7.1.1
        	// will get fixed after hard upgrade. Following trigger was fired
        	// during soft upgrade and post soft upgrade
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");

        	//Following test shows that the buggy trigger created with 10.7.1.1
        	// will get fixed after hard upgrade. Following trigger was never
        	// fired in soft upgrade mode
            //load data into trigger table
            s.execute("insert into HARD_UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update HARD_UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from HARD_UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from HARD_UPGRADE_tab1");
        	s.execute("delete from HARD_UPGRADE_tab2");
            break;
            
        case PH_POST_HARD_UPGRADE:
        	//Following test shows that the buggy trigger created with 10.7.1.1
        	// will get fixed after hard upgrade. Following trigger was fired
        	// during soft upgrade and post soft upgrade & during hard upgrade
            //load data into trigger table
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");

        	//Following test shows that the buggy trigger created with 10.7.1.1
        	// will get fixed after hard upgrade. Following trigger was never
        	// fired in soft upgrade mode but was fired during hard upgrade
            //load data into trigger table
            //load data into trigger table
            s.execute("insert into HARD_UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update HARD_UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from HARD_UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from HARD_UPGRADE_tab1");
        	s.execute("delete from HARD_UPGRADE_tab2");

        	//Following test shows that the buggy trigger created with 10.7.1.1
        	// will get fixed after hard upgrade. This is the first time this
        	// trigger got fired after it's creation in 10.7.1.1 CREATE mode
            //load data into trigger table
            //load data into trigger table
            s.execute("insert into POSTHRD_UPGRD_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update POSTHRD_UPGRD_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from POSTHRD_UPGRD_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from POSTHRD_UPGRD_tab1");
        	s.execute("delete from POSTHRD_UPGRD_tab2");
            break;
        }
    }
}
