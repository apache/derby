/*

Derby - Class org.apache.derbyTesting.perf.clients.SequenceGeneratorConcurrency

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

package org.apache.derbyTesting.perf.clients;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

/**
 * <p>
 * Machinery to test the concurrency of sequence generators.
 * </p>
 */
public class SequenceGeneratorConcurrency
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // LOAD OPTIONS FOR THIS TEST
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Describes the load options specific to a run of the SequenceGeneratorConcurrency test.
     * </p>
     */
    public static final class LoadOptions
    {
        private int _numberOfGenerators;
        private int _tablesPerGenerator;
        private int _insertsPerTransaction;
        private boolean _debugging;

        public LoadOptions()
        {
            _numberOfGenerators = Runner.getLoadOpt( "numberOfGenerators", 1 );
            _tablesPerGenerator = Runner.getLoadOpt( "tablesPerGenerator", 1 );
            _insertsPerTransaction = Runner.getLoadOpt( "insertsPerTransaction", 1 );
            _debugging = ( Runner.getLoadOpt( "debugging", 0 ) == 1 );
        }

        /** Get the number of generators created by this test run */
        public int getNumberOfGenerators() { return _numberOfGenerators; }

        /** Get the number of tables created for each generator */
        public int getTablesPerGenerator() { return _tablesPerGenerator; }

        /** Get the number of inserts performed per transaction */
        public int getInsertsPerTransaction() { return _insertsPerTransaction; }

        /** Return whether we are in debugging mode */
        public boolean debugging() { return _debugging; }

        public String toString()
        {
            StringBuffer buffer = new StringBuffer();

            buffer.append( "LoadOptions( " );
            buffer.append( " generators = " + _numberOfGenerators );
            buffer.append( ", tablesPerGenerator = " + _tablesPerGenerator );
            buffer.append( ", insertsPerTransaction = " + _insertsPerTransaction );
            buffer.append( ", debugging = " + _debugging );
            buffer.append( " )" );

            return buffer.toString();
        }
        
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // DBFiller IMPLEMENTATION
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Create the schema necessary to support this test run.
     * </p>
     */
    public static final class Filler implements DBFiller
    {
        private LoadOptions _loadOptions;

        public Filler()
        {
            _loadOptions = new LoadOptions();
        }

        public void fill( Connection conn ) throws SQLException
        {
            int numberOfGenerators = _loadOptions.getNumberOfGenerators();
            int tablesPerGenerator = _loadOptions.getTablesPerGenerator();

            for ( int sequence = 0; sequence < numberOfGenerators; sequence++ )
            {
                runDDL( conn, "create sequence " + makeSequenceName( sequence ) );

                for ( int table = 0; table < tablesPerGenerator; table++ )
                {
                    runDDL( conn, "create table " + makeTableName( sequence, table ) + "( a int )" );
                }
            }
        }

        /** Run a DDL statement */
        private void runDDL( Connection conn, String ddl ) throws SQLException
        {
            PreparedStatement ps = prepareStatement( conn, _loadOptions.debugging(), ddl );
            ps.execute();
            ps.close();
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Client IMPLEMENTATION
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static final class SGClient implements Client
    {
        private LoadOptions _loadOptions;
        private Connection _conn;
        private PreparedStatement _psArray[][];
        private Random _randomNumberGenerator;
        private int _clientNumber;
        private int _transactionCount;
        private int _errorCount = 0;
        private HashMap _errorLog;

        private static int _clientCount = 0;

        // filled in at reporting time
        private static int _totalErrorCount = 0;
        private static int _totalTransactionCount = 0;

        public SGClient()
        {
            _clientNumber = _clientCount++;
            _transactionCount = 0;
            _errorLog = new HashMap();
            _loadOptions = new LoadOptions();

            _psArray = new PreparedStatement[ _loadOptions.getNumberOfGenerators() ] [ _loadOptions.getTablesPerGenerator() ];
            _randomNumberGenerator = new Random();

            if ( _loadOptions.debugging() )
            {
                debugPrint( "Creating client " + _clientNumber + " with " + _loadOptions.toString() );
            }
        }

        /** Create the PreparedStatements needed by the test run. */
        public void init( Connection conn ) throws SQLException
        {
            _conn = conn;

            int numberOfGenerators = _loadOptions.getNumberOfGenerators();
            int tablesPerGenerator = _loadOptions.getTablesPerGenerator();
            boolean debugging = _loadOptions.debugging();

            for ( int sequence = 0; sequence < numberOfGenerators; sequence++ )
            {
                String sequenceName = makeSequenceName( sequence );

                for ( int table = 0; table < tablesPerGenerator; table++ )
                {
                    String tableName = makeTableName( sequence, table );
                    
                    _psArray[ sequence ][ table ] = prepareStatement
                        ( _conn, debugging, "insert into " + tableName + "( a ) values ( next value for " + sequenceName + " )" );
                }
            }

            _conn.setAutoCommit( false );
        }

        /** A transaction performed by this thread */
        public void doWork() throws SQLException
        {
            int sequence = getPositiveRandomNumber() % _loadOptions.getNumberOfGenerators();
            int table = getPositiveRandomNumber() % _loadOptions.getTablesPerGenerator();
            int insertsPerTransaction = _loadOptions.getInsertsPerTransaction();
            boolean debugging = _loadOptions.debugging();

            int rowNumber = 0;

            try {
                for ( ; rowNumber < insertsPerTransaction; rowNumber++ )
                {
                    _psArray[ sequence ][ table ].executeUpdate();
                }
            }
            catch (SQLException t)
            {
                debugPrint
                    (
                     "Error on client " + _clientNumber +
                     " on sequence " + sequence +
                     " in transaction " + _transactionCount +
                     " on row " + rowNumber
                     );

                addError( t );
                _conn.rollback();

                return;
            }

            _conn.commit();
            
            _transactionCount++;
        }

        private int getPositiveRandomNumber()
        {
            int raw = _randomNumberGenerator.nextInt();

            if ( raw < 0 ) { return -raw; }
            else { return raw; }
        }

        public void printReport(PrintStream out)
        {
            Iterator keyIterator = _errorLog.keySet().iterator();

            while ( keyIterator.hasNext() )
            {
                String key = (String) keyIterator.next();
                int[] value = (int[]) _errorLog.get( key );

                String message = "    Client " + _clientNumber + " saw " + value[0] + " instances of this error: " + key;

                out.println( message );
            }

            _totalErrorCount += _errorCount;
            _totalTransactionCount += _transactionCount;

            // last client reports the totals
            if ( _clientNumber == ( _clientCount - 1 ) )
            {
                out.println( "\n" );
                out.println( _loadOptions.toString() );
                out.println( _totalErrorCount + " errors, including warmup phase." );
                out.println( _totalTransactionCount + " successful transactions, including warmup phase." );
            }
        }
        
        // error management

        /** Bump the error count for this problem */
        private void addError( Throwable t )
        {
            _errorCount++;
            
            String key = t.getClass().getName() + ": " + t.getMessage();
            int[] value = (int[]) _errorLog.get( key );

            if ( value != null ) { value[ 0 ] = value[ 0 ] + 1; }
            else
            {
                _errorLog.put( key, new int[] { 1 } );
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // UTILITY METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** make the name of a sequence */
    public static String makeSequenceName( int sequence )
    { return "seq_" + sequence; }
    
    /** make the name of a table */
    public static String makeTableName( int sequence, int table )
    { return "t_" + sequence + "_" + table; }

    public static PreparedStatement prepareStatement
        ( Connection conn, boolean debugging, String text ) throws SQLException
    {
        if ( debugging ) { debugPrint( text ); }

        return conn.prepareStatement( text );
    }
    
    public static void debugPrint( String text )
    {
        print( "DEBUG: " + text );
    }

    public static void print( String text )
    {
        System.out.println( text );
    }

}


