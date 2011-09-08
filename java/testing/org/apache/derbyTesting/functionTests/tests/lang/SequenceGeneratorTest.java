/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SequenceGeneratorTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.math.BigDecimal;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

import org.apache.derby.impl.sql.catalog.SequenceGenerator;
import org.apache.derby.impl.sql.catalog.SequenceRange;
import org.apache.derby.impl.sql.catalog.SequenceUpdater;
import org.apache.derby.catalog.SequencePreallocator;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * <p>
 * Test sequence generators. See DERBY-712.
 * </p>
 */
public class SequenceGeneratorTest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // number of pre-allocated values in a sequence generator
    private static final long ALLOCATION_COUNT = 20L;
    private static final int TWEAKED_ALLOCATION_COUNT = 7;

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      RUTH = "RUTH";
    private static  final   String      ALICE = "ALICE";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, ALICE, RUTH  };

    private static  final   String      MISSING_ALLOCATOR = "X0Y85";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static boolean _fullDebug = false;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Create a new instance.
     */

    public SequenceGeneratorTest(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite()
    {
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(SequenceGeneratorTest.class);
        Test        cleanTest = new CleanDatabaseTestSetup( suite );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( cleanTest, LEGAL_USERS, "sequenceGenerator" );
        Test        authorizedTest = TestConfiguration.sqlAuthorizationDecorator( authenticatedTest );

        return authorizedTest;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test basic incrementing and pre-allocating of sequence values on disk.
     * </p>
     */
    public void test_01_basic() throws Exception
    {
        Connection  conn = openUserConnection( TEST_DBO );

        int initialValue = Integer.MIN_VALUE;

        goodStatement( conn, "create sequence seq_01\n" );

        assertEquals( (long) initialValue, getCurrentValue( TEST_DBO, "SEQ_01" ) );

        // first run is allocated
        int seq_01_value = initialValue;
        long seq_01_upperBound = seq_01_value + ALLOCATION_COUNT;
        for ( int i = 0; i < ALLOCATION_COUNT; i++ )
        {
            vetBumping( conn, TEST_DBO, "SEQ_01", seq_01_value++, seq_01_upperBound );
        }

        // another run is allocated
        seq_01_upperBound += ALLOCATION_COUNT;
        vetBumping( conn, TEST_DBO, "SEQ_01", seq_01_value++, seq_01_upperBound );

        // DDL flushes the metadata cache
        goodStatement( conn, "create sequence seq_01_a\n" );

        int seq_01_a_value = initialValue;
        long seq_01_a_upperBound = seq_01_a_value + ALLOCATION_COUNT;
        // check the other sequence
        for ( int i = 0; i < 2; i++ )
        {
            vetBumping( conn, TEST_DBO, "SEQ_01_A", seq_01_a_value++, seq_01_a_upperBound );
        }

        //
        // The cache was flushed when seq_01_a was created. This
        // restarts the generator for that sequence and allocates a new range.
        //
        seq_01_upperBound = seq_01_value + ALLOCATION_COUNT;
        vetBumping( conn, TEST_DBO, "SEQ_01", seq_01_value++, seq_01_upperBound );
    }
    private void vetBumping( Connection conn, String schemaName, String sequenceName, int expectedValue, long expectedValueOnDisk )
        throws Exception
    {
        PreparedStatement ps = chattyPrepare( conn, "values( next value for " + schemaName + '.' + sequenceName + " )\n" );

        assertEquals( expectedValue, getScalarInteger( ps ) );
        assertEquals( expectedValueOnDisk, getCurrentValue( schemaName, sequenceName ) );
    }

    /**
     * <p>
     * Test boundary conditions in sequence generators.
     * </p>
     */
    public void test_02_boundary() throws Exception
    {
        T_SequenceUpdater updater;

        updater = new T_SequenceUpdater
            ( new Long(  Integer.MIN_VALUE ), true, 1L, (long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE, (long) Integer.MIN_VALUE );

        assertEquals( -2147483648L, updater.getValueOnDisk().longValue() );

        long        initialValue = (long) Integer.MIN_VALUE;
        long        expectedValueOnDisk = initialValue + ALLOCATION_COUNT;

        for ( long i = 0; i < ALLOCATION_COUNT; i++ )
        {
            vetBumping( updater, initialValue + i, expectedValueOnDisk );
        }
        expectedValueOnDisk += ALLOCATION_COUNT;

        vetBumping( updater, initialValue + ALLOCATION_COUNT, expectedValueOnDisk );

        vetBoundaries( Short.MAX_VALUE, Short.MIN_VALUE );
        vetBoundaries( Integer.MAX_VALUE, Integer.MIN_VALUE );
        vetBoundaries( Long.MAX_VALUE, Long.MIN_VALUE );

        vetBoundaries( Short.MAX_VALUE/2, Short.MIN_VALUE/2 );
        vetBoundaries( Integer.MAX_VALUE/2, Integer.MIN_VALUE/2 );
        vetBoundaries( Long.MAX_VALUE/2, Long.MIN_VALUE/2 );
    }
    private void vetBoundaries
        (
         long maxValue,
         long minValue
         )
        throws Exception
    {
        long biggestStep = 2 * ALLOCATION_COUNT;
        
        for ( long i = 1L; i <= biggestStep; i++ )
        {
            vetUpperBoundary( maxValue, minValue, i );
            vetLowerBoundary( maxValue, minValue, i );
        }
    }
    private void vetUpperBoundary
        (
         long maxValue,
         long minValue,
         long stepSize
         )
        throws Exception
    {
        long restartValue = minValue;
        long firstValue;

        long initValue = maxValue - (ALLOCATION_COUNT * stepSize);

        for ( long i = initValue; (i > 0) && (i <= maxValue); i++ )
        {
            firstValue = i;
            vetBoundaries( maxValue, minValue, stepSize, firstValue, restartValue );
        }
    }
    private void vetLowerBoundary
        (
         long maxValue,
         long minValue,
         long stepSize
         )
        throws Exception
    {
        long restartValue = maxValue;
        long firstValue;

        long initValue = minValue + (ALLOCATION_COUNT * stepSize);

        for ( long i = initValue; (i < 0) && (i >= minValue); i-- )
        {
            firstValue = i;
            vetBoundaries( maxValue, minValue, -stepSize, firstValue, restartValue );
        }
    }
    private void vetBoundaries
        (
         long maxValue,
         long minValue,
         long stepSize,
         long firstValue,
         long restartValue
         )
        throws Exception
    {
        long bumps = (2 * ALLOCATION_COUNT) + 1;
        
        vetBumping( firstValue, true, stepSize, maxValue, minValue, restartValue, bumps );
        vetBumping( firstValue, false, stepSize, maxValue, minValue, restartValue, bumps );
    }
    private void vetBumping
        (
         long firstValue,
         boolean canCycle,
         long stepSize,
         long maxValue,
         long minValue,
         long restartValue,
         long bumps
         )
        throws Exception
    {
        if ( _fullDebug) { println( "stepSize = " + stepSize + " and firstValue = " + firstValue + " and canCycle = " + canCycle ); }
        
        SGVetter vetter = new SGVetter
            ( new Long(  firstValue ), canCycle, stepSize, maxValue, minValue, restartValue, ALLOCATION_COUNT );           
        T_SequenceUpdater updater = new T_SequenceUpdater
            ( new Long(  firstValue ), canCycle, stepSize, maxValue, minValue, restartValue );

        if ( _fullDebug) { println( "" ); }
        for ( long i = 0; i < bumps; i++ ) { vetBump( vetter, updater ); }
    }
    private void vetBump( SGVetter vetter, T_SequenceUpdater updater ) throws Exception
    {
        assertLongEquals( vetter.getUpperBound(), updater.getValueOnDisk() );

        Long vetterValue = vetter.getNextValue();

        if ( _fullDebug ) { println( "Expecting value = " + vetterValue + " and expecting ValueOnDisk = " + vetter.getUpperBound() ); }
        
        if ( vetterValue != null )
        {
            long updaterValue = updater.getCurrentValueAndAdvance();

            assertEquals( vetterValue.longValue(), updaterValue );
        }
        else
        {
            try {
                updater.getCurrentValueAndAdvance();
                fail( "Expected to catch cycle exception." );
            }
            catch (Exception e)
            {}
        }

        assertLongEquals( vetter.getUpperBound(), updater.getValueOnDisk() );
    }
    private void assertLongEquals( Long left, Long right )
    {
        if ( left == null ) { assertNull( right ); }
        else
        {
            assertNotNull( right );
            assertEquals( left.longValue(), right.longValue() );
        }
    }
    
    private void vetBumping( T_SequenceUpdater updater, long expectedValue, long expectedValueOnDisk )
        throws Exception
    {
        long actualValue = updater.getCurrentValueAndAdvance();
        long actualValueOnDisk = updater.getValueOnDisk().longValue();
        
        println( "Expected value = " + expectedValue + " vs actual value = " + actualValue );
        println( "    Expected value on disk = " + expectedValueOnDisk + " vs actual value on disk = " + actualValueOnDisk );
        
        assertEquals( expectedValue, actualValue );
        assertEquals( expectedValueOnDisk, actualValueOnDisk );
    }
    
    /**
     * <p>
     * Test non cycling sequence generators.
     * </p>
     */
    public void test_03_nonCycling() throws Exception
    {
        vetNonCycling( Short.MAX_VALUE, Short.MIN_VALUE );
        vetNonCycling( Integer.MAX_VALUE, Integer.MIN_VALUE );
        vetNonCycling( Long.MAX_VALUE, Long.MIN_VALUE );
    }
    private void vetNonCycling
        (
         long maxValue,
         long minValue
         )
        throws Exception
    {
        vetNonCycling( maxValue, minValue, 1L );
        vetNonCycling( maxValue, minValue, -1L );
    }
    private void vetNonCycling
        (
         long maxValue,
         long minValue,
         long stepSize
         )
        throws Exception
    {
        long bumps = 3;
        long firstValue;
        long restartValue;

        if ( stepSize > 0 )
        {
            firstValue = maxValue - bumps;
            restartValue = minValue;
        }
        else
        {
            firstValue = minValue + bumps;
            restartValue = maxValue;
        }

        SGVetter vetter = new SGVetter
            ( new Long(  firstValue ), false, stepSize, maxValue, minValue, restartValue, ALLOCATION_COUNT );           
        T_SequenceUpdater updater = new T_SequenceUpdater
            ( new Long(  firstValue ), false, stepSize, maxValue, minValue, restartValue );

        // make sure we can survive trying to bust the cycle more than once
        long extraBumps = bumps + 2;
        for ( long i = 0; i <= extraBumps; i++ )
        {
            vetBump( vetter, updater );
        }
    }
    
    /**
     * <p>
     * Test that when you reboot the database, you pick up the sequence
     * number on disk, not the last version in memory.
     * </p>
     */
    public void test_04_reboot() throws Exception
    {
        Connection  conn = openUserConnection( TEST_DBO );
        
        int initialValue = Integer.MIN_VALUE;

        goodStatement( conn, "create sequence seq_04\n" );

        int seq_04_value = initialValue;
        long seq_04_upperBound = seq_04_value + ALLOCATION_COUNT;
        vetBumping( conn, TEST_DBO, "SEQ_04", seq_04_value++, seq_04_upperBound );
        vetBumping( conn, TEST_DBO, "SEQ_04", seq_04_value++, seq_04_upperBound );

        getTestConfiguration().shutdownDatabase();
        conn = openUserConnection( TEST_DBO );
        seq_04_upperBound = seq_04_value + ALLOCATION_COUNT;
        vetBumping( conn, TEST_DBO, "SEQ_04", seq_04_value++, seq_04_upperBound );

        getTestConfiguration().shutdownDatabase();
        conn = openUserConnection( TEST_DBO );
        seq_04_upperBound = seq_04_value + ALLOCATION_COUNT;
        vetBumping( conn, TEST_DBO, "SEQ_04", seq_04_value++, seq_04_upperBound );
    }
    
    /**
     * <p>
     * Test that multiple transactions can access the same sequence generator
     * and not block.
     * </p>
     */
    public void test_05_concurrency() throws Exception
    {
        Connection  conn = openUserConnection( TEST_DBO );
        
        int initialValue = Integer.MIN_VALUE;

        goodStatement( conn, "create sequence seq_05\n" );
        goodStatement( conn, "grant usage on sequence seq_05 to public\n" );

        int seq_05_value = initialValue;
        long seq_05_upperBound = seq_05_value;

        Connection  ruthConnection = openUserConnection( "RUTH" );
        Connection  aliceConnection = openUserConnection( "ALICE" );

        ruthConnection.setAutoCommit( false );
        aliceConnection.setAutoCommit( false );

        long loopCount = 2 * ALLOCATION_COUNT;
        for ( long i = 0; i < loopCount; i++ )
        {
            Connection loopConn = ( i % 2 == 0 ) ? ruthConnection : aliceConnection;

            if ( (i % ALLOCATION_COUNT) == 0 ) { seq_05_upperBound += ALLOCATION_COUNT; }

            vetBumping( loopConn, TEST_DBO, "SEQ_05", seq_05_value++, seq_05_upperBound );
        }

        ruthConnection.commit();
        aliceConnection.commit();
    }
    
    /**
     * <p>
     * Test big step sizes.
     * </p>
     */
    public void test_06_bigStepSize() throws Exception
    {
        T_SequenceUpdater updater;
        long stepSize = (Long.MAX_VALUE / ALLOCATION_COUNT) * 3;

        updater = new T_SequenceUpdater
            ( new Long(  Long.MIN_VALUE ), true, stepSize, (long) Long.MAX_VALUE, (long) Long.MIN_VALUE, (long) Long.MIN_VALUE );

        long nextValue = updater.getCurrentValueAndAdvance();
        long currentValueOnDisk = updater.getValueOnDisk().longValue();
        long rangeSize = currentValueOnDisk - nextValue;

        // allocation count truncated to 1 because the step size is so large
        assertEquals( stepSize, rangeSize );

        vetBigStep( Short.MAX_VALUE, Short.MIN_VALUE );
        vetBigStep( Integer.MAX_VALUE, Integer.MIN_VALUE );
        vetBigStep( Long.MAX_VALUE, Long.MIN_VALUE );

        vetBigStep( ALLOCATION_COUNT, 0L );
    }
    private void vetBigStep( long maxValue, long minValue )
        throws Exception
    {
        Long firstValue = new Long( minValue );
        long restartValue = minValue;
        long stepSize = maxValue - 1;
        boolean canCycle = true;
        long truncatedAllocationCount = 1L;
        
        SGVetter vetter = new SGVetter
            ( firstValue, canCycle, stepSize, maxValue, minValue, restartValue, truncatedAllocationCount );           
        T_SequenceUpdater updater = new T_SequenceUpdater
            ( firstValue, canCycle, stepSize, maxValue, minValue, restartValue );

        for ( long i = 0; i < ALLOCATION_COUNT; i++ )
        {
            vetBump( vetter, updater );
        }
    }
    
    /**
     * <p>
     * Test that cache flushing doesn't prevent us from dropping
     * a sequence generator.
     * </p>
     */
    public void test_07_dropSequence() throws Exception
    {
        Connection  conn = getConnection();

        goodStatement( conn, "create sequence seq_07\n" );
        
        int initialValue = Integer.MIN_VALUE;
        int seq_07_value = initialValue;
        long seq_07_upperBound = seq_07_value + ALLOCATION_COUNT;
        for ( int i = 0; i < ALLOCATION_COUNT; i++ )
        {
            vetBumping( conn, TEST_DBO, "SEQ_07", seq_07_value++, seq_07_upperBound );
        }

        goodStatement( conn, "values( next value for seq_07 )\n" );
        goodStatement( conn, "drop sequence seq_07 restrict\n" );
        
        expectCompilationError( OBJECT_DOES_NOT_EXIST, "values ( next value for seq_07 )\n" );
    }
    
    /**
     * <p>
     * Test user-written range allocators.
     * </p>
     */
    public void test_08_userWrittenAllocators() throws Exception
    {
        Connection  conn = openUserConnection( TEST_DBO );
        String  className;

        goodStatement( conn, "create sequence seq_08\n" );

        className = getClass().getName() + "$" + "UnknownClass";
        goodStatement
            (
             conn,
             "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', '" + className + "')"
             );
        expectExecutionError( conn, MISSING_ALLOCATOR, "values ( next value for seq_08 )" );

        className = getClass().getName() + "$" + "BadAllocator";
        goodStatement
            (
             conn,
             "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', '" + className + "')"
             );
        expectExecutionError( conn, MISSING_ALLOCATOR, "values ( next value for seq_08 )" );

        className = getClass().getName() + "$" + "LegalAllocator";
        goodStatement
            (
             conn,
             "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', '" + className + "')"
             );
        vetBumping( conn, TEST_DBO, "SEQ_08", Integer.MIN_VALUE, Integer.MIN_VALUE + TWEAKED_ALLOCATION_COUNT );

        goodStatement
            (
             conn,
             "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', null )"
             );
    }
        
    /**
     * <p>
     * Test overriding the default length of sequence/identity ranges.
     * </p>
     */
    public void test_09_defaultRangeSize() throws Exception
    {
        Connection  conn = openUserConnection( TEST_DBO );
        long    number;

        goodStatement( conn, "create sequence seq_09_01\n" );
        number = 30L;
        goodStatement
            (
             conn,
             "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', '" + number + "')"
             );
        vetBumping( conn, TEST_DBO, "SEQ_09_01", Integer.MIN_VALUE, Integer.MIN_VALUE + number );

        // 0 results in the usual default
        goodStatement( conn, "create sequence seq_09_02\n" );
        number = 0L;
        goodStatement
            (
             conn,
             "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', '" + number + "')"
             );
        vetBumping( conn, TEST_DBO, "SEQ_09_02", Integer.MIN_VALUE, Integer.MIN_VALUE + ALLOCATION_COUNT );

        // negative numbers result in Missing Allocator exception
        goodStatement( conn, "create sequence seq_09_03\n" );
        number = -1L;
        goodStatement
            (
             conn,
             "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', '" + number + "')"
             );
        expectExecutionError( conn, MISSING_ALLOCATOR, "values ( next value for seq_09_03 )" );

        // If the value doesn't fit in an int, we also get a Missing Allocator exception
        goodStatement( conn, "create sequence seq_09_04\n" );
        number = Long.MAX_VALUE - 1L;
        goodStatement
            (
             conn,
             "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', '" + number + "')"
             );
        expectExecutionError( conn, MISSING_ALLOCATOR, "values ( next value for seq_09_04 )" );
        
        // out of range values will stifle preallocation
        goodStatement( conn, "create sequence seq_09_05 as smallint\n" );
        number = ((long) 3 * Short.MAX_VALUE);
        goodStatement
            (
             conn,
             "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', '" + number + "')"
             );
        vetBumping( conn, TEST_DBO, "SEQ_09_05", Short.MIN_VALUE, Short.MIN_VALUE + 1 );

        goodStatement
            (
             conn,
             "call syscs_util.syscs_set_database_property( 'derby.language.sequence.preallocator', null )"
             );
    }
    
    /**
     * <p>
     * Test that sequence values are not leaked during an orderly system shutdown.
     * See DERBY-5398.
     * </p>
     */
    public void test_10_5398() throws Exception
    {
        Connection  conn = openUserConnection( TEST_DBO );

        goodStatement( conn, "create sequence seq_10\n" );

        int seq_10_value = Integer.MIN_VALUE;
        long seq_10_upperBound;

        seq_10_upperBound = seq_10_value + ALLOCATION_COUNT;
        vetBumping( conn, TEST_DBO, "SEQ_10", seq_10_value++, seq_10_upperBound );

        // bring down the engine, then reboot the database
        getTestConfiguration().shutdownEngine();
        conn = openUserConnection( TEST_DBO );

        // verify that we did not leak any values
        seq_10_upperBound = seq_10_value + ALLOCATION_COUNT;
        vetBumping( conn, TEST_DBO, "SEQ_10", seq_10_value++, seq_10_upperBound );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the current value from a sequence */
    private long getCurrentValue( String schemaName, String sequenceName )
        throws Exception
    {
        Connection  conn = openUserConnection( TEST_DBO );
        
        PreparedStatement ps = chattyPrepare
            ( conn,
              "select currentvalue from sys.syssequences seq, sys.sysschemas s where s.schemaname = ? and seq.sequencename = ? and s.schemaid = seq.schemaid" );
        ps.setString( 1, schemaName );
        ps.setString( 2, sequenceName );

        long retval = getScalarLong( ps );

        conn.commit();
        
        return retval;
    }
    
    /** Get a scalar integer result from a query */
    private int getScalarInteger( PreparedStatement ps ) throws Exception
    {
        ResultSet rs = ps.executeQuery();
        rs.next();
        int retval = rs.getInt( 1 );

        rs.close();
        ps.close();

        return retval;
    }

    /** Get a scalar long result from a query */
    private long getScalarLong( PreparedStatement ps ) throws Exception
    {
        ResultSet rs = ps.executeQuery();
        rs.next();
        long retval = rs.getLong( 1 );

        rs.close();
        ps.close();

        return retval;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Machine for testing sequence generators standalone.
     * </p>
     */
    public static final class T_SequenceUpdater extends SequenceUpdater
    {
        private Long _valueOnDisk;
        
        public T_SequenceUpdater
            (
             Long currentValue,
             boolean canCycle,
             long increment,
             long maxValue,
             long minValue,
             long restartValue
             )
        {
            _valueOnDisk = currentValue;
            
            _sequenceGenerator = new SequenceGenerator
                (
                 currentValue,
                 canCycle,
                 increment,
                 maxValue,
                 minValue,
                 restartValue,
                 "DUMMY_SCHEMA",
                 "DUMMY_SEQUENCE",
                 new SequenceRange()
                 );
        }
        
        public Long getValueOnDisk() { return _valueOnDisk; }
        
        public long getCurrentValueAndAdvance() throws Exception
        {
            SQLLongint nextValue = new SQLLongint();
            
            getCurrentValueAndAdvance( nextValue );
            
            return nextValue.getLong();
        }
        
        // SequenceUpdater BEHAVIOR
        protected SequenceGenerator createSequenceGenerator( TransactionController readOnlyTC ) { return _sequenceGenerator; }
        
        protected boolean updateCurrentValueOnDisk( TransactionController tc, Long oldValue, Long newValue, boolean wait )
        {
            _valueOnDisk = newValue;

            return true;
        }
        
        // override so the tests don't get a null pointer exception looking up the lcc
        public boolean updateCurrentValueOnDisk( Long oldValue, Long newValue )
        {
            return updateCurrentValueOnDisk( null, oldValue, newValue, false );
        }
        
        // overridden to avoid a null pointer exception when we don't have a language context
        protected int getLockTimeout() { return 1000; }
    
    }

    // Illegal preallocator, which does not implement the correct interface
    public  static  final   class   BadAllocator {}

    // Legal preallocator, which preallocates a fixed size range
    public  static final   class   LegalAllocator  implements  SequencePreallocator
    {
        public  LegalAllocator() {}
        
        public  int nextRangeSize( String s, String n ) { return TWEAKED_ALLOCATION_COUNT; }
    }

}

