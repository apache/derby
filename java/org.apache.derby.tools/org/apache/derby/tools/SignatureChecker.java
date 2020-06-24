/*

   Derby - Class org.apache.derby.tools.SignatureChecker

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

package org.apache.derby.tools;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;

/**
   <p>
   This class shows which user declared SQL functions and procedures
   cannot be matched with Java methods.
   </p>

   <p>
   To run from the command-line, enter the following if running on J2SE:
   </p>
   
    <p>
	<code>java org.apache.derby.tools.SignatureChecker CONNECTION_URL_TO_DATABASE</code>
	<p>

   <p>
   And enter the following if running on J2ME:
   </p>
   
    <p>
	<code>java org.apache.derby.tools.SignatureChecker DATABASE_NAME</code>
	<p>

*/

public class SignatureChecker
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  WILDCARD = "%";

    private static  final   String[] SYSTEM_SCHEMAS =
    {
        "SQLJ",
        "SYSCS_UTIL",
        "SYSIBM",
    };


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private final ParsedArgs _parsedArgs;

    private final ArrayList<SQLRoutine>   _procedures = new ArrayList<SQLRoutine>();
    private final ArrayList<SQLRoutine>   _functions = new ArrayList<SQLRoutine>();

    private final boolean     _debugging = false;
    
	private static          LocalizedResource _messageFormatter;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private SignatureChecker( ParsedArgs parsedArgs )
    {
        _parsedArgs = parsedArgs;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ENTRY POINT
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  void    main( String[] args )
    {
        ParsedArgs  parsedArgs = new ParsedArgs( args );
        
        if ( !parsedArgs.isValid() )
        {
            printUsage();
            System.exit( 1 );
        }
        else
        {
            SignatureChecker    me = new SignatureChecker(  parsedArgs  );

            me.execute();
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MACHINERY
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get a connection to a database and then match the signatures of routines
     * in that database.
     * </p>
     */
    private void    execute()
    {
        try {
            Connection conn = getJ2SEConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

            if ( conn == null )
            {
                println(  formatMessage( "SC_NO_CONN" )  );
            }
            else
            {
                matchSignatures( conn );

                conn.close();
            }
            
//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639
        } catch (SQLException t) { printThrowable( t ); }
    }

    /**
     * <p>
     * Match the signatures of routines in the database attached to this connection.
     * </p>
     * @param conn This connection
     * @throws java.sql.SQLException
     */
    private void matchSignatures( Connection conn )
        throws SQLException
    {
        matchProcedures( conn );
        matchFunctions( conn );
    }
    
    /**
     * <p>
     * Match the signatures of procedures in this database.
     * </p>
     * @param conn The connection to use to access the database
     * @throws java.sql.SQLException
     */
    private void matchProcedures( Connection conn )
        throws SQLException
    {
        DatabaseMetaData    dbmd = conn.getMetaData();

        // find all of the user-declared procedures
        findProcedures( dbmd );

        // for each procedure, count its arguments
        countProcedureArgs( dbmd );

        //
        // Try to prepare an invocation of each procedure. This will generate an error if a
        // matching Java signature can not be found.
        //
        int     count = _procedures.size();
        for ( int i = 0; i < count; i++ )
        {
            SQLRoutine  procedure = getProcedure( i );
//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639
            StringBuilder buffer = new StringBuilder();
            int             argCount = procedure.getArgCount();

            buffer.append( "call " );
            buffer.append( procedure.getQualifiedName() );
            buffer.append( "( " );
            for ( int k = 0; k < argCount; k++ )
            {
                if ( k > 0 ) { buffer.append( ", " ); }
                buffer.append( " ? " );
            }
            buffer.append( " )" );

//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639
            checkSignature( conn, buffer.toString(), makeReadableSignature( procedure ) );
        }
    }

    /**
     * <p>
     * Match the signatures of functions in this database.
     * </p>
     * @param conn The connection to use to access the database
     * @throws java.sql.SQLException
     */
    private void matchFunctions( Connection conn )
        throws SQLException
    {
        DatabaseMetaData    dbmd = conn.getMetaData();

        // find all of the user-declared functions
        findFunctions( dbmd );

        // for each function, count its arguments
        countFunctionArgs( dbmd );

        //
        // Try to prepare an invocation of each function. This will generate an error if a
        // matching Java signature can not be found.
        //
        int     count = _functions.size();
        for ( int i = 0; i < count; i++ )
        {
            SQLRoutine  function = getFunction( i );
//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639
            StringBuilder query = new StringBuilder();
            int             argCount = function.getArgCount();

            if ( function.isTableFunction() ) { query.append( "select * from table( " ); }
            else { query.append( "values(  " ); }
            
            query.append( function.getQualifiedName() );
            query.append( "( " );
            for ( int k = 0; k < argCount; k++ )
            {
                if ( k > 0 ) { query.append( ", " ); }
                query.append( " ? " );
            }
            query.append( " ) )" );
            if ( function.isTableFunction() ) { query.append( " s" ); }

//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639
            checkSignature( conn, query.toString(), makeReadableSignature( function ) );
        }
    }

    /**
     * <p>
     * Make a human readable signature for a routine. This can be
     * used in error messages.
     * </p>
     * @param routine the routine for which we want a signature
     * @return human readable string
     */
    private String  makeReadableSignature( SQLRoutine routine )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639
        StringBuilder signature = new StringBuilder();
        int             argCount = routine.getArgCount();
        
        signature.append( routine.getQualifiedName() );
        signature.append( "( " );
        for ( int k = 0; k < argCount; k++ )
        {
            if ( k > 0 ) { signature.append( ", " ); }
//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639
            signature.append( " " );
            signature.append( routine.getArgType( k ) );
            signature.append( " " );
        }
        signature.append( " )" );

        return signature.toString();
    }

    /**
     * <p>
     * Find all of the user-declared procedures.
     * </p>
     * @param dbmd the database metadata of the database
     * @throws java.sql.SQLException
     */
    private void    findProcedures( DatabaseMetaData dbmd )
        throws SQLException
    {
        ResultSet               rs = dbmd.getProcedures( null, null, WILDCARD );

        while( rs.next() )
        {
            String  schema = rs.getString( 2 );
            String  name = rs.getString( 3 );

            if ( isSystemSchema( schema ) ) { continue; }

            putProcedure( schema, name );
        }
        rs.close();
    }
    
    /**
     * <p>
     * Count up the arguments to the user-coded procedures in
     * {@link #_procedures} and update that data structure accordingly
     * </p>
     * @param dbmd the database metadata of the database
     * @throws java.sql.SQLException
     */
    private void    countProcedureArgs( DatabaseMetaData dbmd )
        throws SQLException
    {
        int     count = _procedures.size();
        for ( int i = 0; i < count; i++ )
        {
            SQLRoutine  procedure = getProcedure( i );

            ResultSet   rs = dbmd.getProcedureColumns( null, procedure.getSchema(), procedure.getName(), WILDCARD );

            while( rs.next() )
            {
                procedure.addArg( rs.getString( 7 ) );
            }
            rs.close();
        }
    }
    
    /**
     * <p>
     * Find all of the user-declared functions. We use reflection to get our
     * hands on getFunctions() because that method does not appear in
     * the JSR169 api for DatabaseMetaData. Update {@link #_functions}.
     * </p>
     * @param dbmd the database metadata of the database
     * @throws java.sql.SQLException
     */
    private void    findFunctions( DatabaseMetaData dbmd )
        throws SQLException
    {
        try {
            ResultSet   rs = dbmd.getFunctions(null, null, WILDCARD);
//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639

            while( rs.next() )
            {
                String  schema = rs.getString( 2 );
                String  name = rs.getString( 3 );
                short   functionType = rs.getShort( 5 );

                if ( isSystemSchema( schema ) ) { continue; }

                boolean isTableFunction =
                    functionType == DatabaseMetaData.functionReturnsTable;
//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639

                putFunction( schema, name, isTableFunction );
            }
            rs.close();

            
        } catch (SQLException e) { throw new SQLException( e.getMessage() ); }
    }

    /**
     * <p>
     * Count up the arguments to the user-coded procedures. We use
     * reflection to look up the getFunctionColumns() method because that
     * method does not appear in the JSR169 api for DatabaseMetaData.
     * Update {@link #_functions}.
     * </p>
     * @param dbmd the database metadata of the database
     * @throws java.sql.SQLException
     */
    private void    countFunctionArgs( DatabaseMetaData dbmd )
        throws SQLException
    {
        int     count = _functions.size();
//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639
        for ( int i = 0; i < count; i++ )
        {
            SQLRoutine  function = getFunction( i );
            ResultSet   rs = dbmd.getFunctionColumns(
                null, function.getSchema(), function.getName(), WILDCARD);

            while( rs.next() )
            {
                short   columnType = rs.getShort( 5 );

                //
                // Skip the return value if this is a table function.
                // Skip all columns in the returned result set if this is a
                // table function.
                //
//IC see: https://issues.apache.org/jira/browse/DERBY-2438
                if ( columnType == DatabaseMetaData.functionReturn ) { continue; }
                if ( columnType == DatabaseMetaData.functionColumnResult ) { continue; }

                function.addArg( rs.getString( 7 ) );
            }
            rs.close();
        }
    }
    
    /**
     * <p>
     * Prepared a routine invocation in order to check whether it matches a Java method.
     * </p>
     * @param conn The connection to the database
     * @param query The SQL to prepare
     * @param readableSignature the signature: printed if prepare fails
     */
    private void    checkSignature( Connection conn, String query, String readableSignature )
    {
        try {
            PreparedStatement   ps = prepareStatement( conn, query );
            ps.close();

            println( formatMessage( "SC_FOUND_MATCH", readableSignature ) );

                     } catch (SQLException se)
        {
            println( formatMessage( "SC_UNRESOLVABLE", readableSignature, se.getMessage() ) );
        }
    }
    
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * We use reflection to get the J2SE connection so that references to
     * DriverManager will not generate linkage errors on old J2ME platforms
     * which may resolve references eagerly.
     *
     * @return a connection to the database
     * @throws java.sql.SQLException
     */
    private Connection  getJ2SEConnection()
        throws SQLException
    {
        try {
            Class.forName( "org.apache.derby.jdbc.EmbeddedDriver" );
            Class.forName( "org.apache.derby.jdbc.ClientDriver" );
//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639
            Class.forName( "java.sql.DriverManager" );
        } catch (ClassNotFoundException t) {}

        try {

            return DriverManager.getConnection(
                _parsedArgs.getJ2seConnectionUrl()  );
            
        } catch (SQLException t)
        {
            printThrowable( t );
            return null;
        }
    }
    
    private PreparedStatement prepareStatement( Connection conn, String text )
        throws SQLException
    {
        if ( _debugging ) { println( "Preparing: " + text ); }

        return conn.prepareStatement( text );
    }

    private static  void printUsage()
    {
        println(  formatMessage( "SC_USAGE" )  );
    }

    @SuppressWarnings("CallToPrintStackTrace")
    private static void printThrowable( Throwable t )
    {
        t.printStackTrace();
    }
    
    private static  void println( String text )
    {
        System.out.println( text );
    }

    /**
     * Return true if the schema is a system schema.
     * @param schema the schema to check
     * @return {@code true} if the schema is a system schema
     */
    private boolean isSystemSchema( String schema )
    {
        int count = SYSTEM_SCHEMAS.length;

        for ( int i = 0; i < count; i++ )
        {
            if ( SYSTEM_SCHEMAS[ i ].equals( schema ) ) { return true; }
        }

        return false;
    }
    
    /**
     * Store a procedure descriptor. Updates {@link #_procedures}.
     * @param schema schema of the procedure
     * @param name of a procedure
     */
    private void putProcedure( String schema, String name )
    {
        _procedures.add( new SQLRoutine( schema, name, false )  );
    }
        
    /**
     * Get a procedure descriptor from {@link #_procedures}.
     * @param idx The index of the procedure in {@link #_procedures}.
     * @return a procedure descriptor
     */
    private SQLRoutine getProcedure( int idx )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        return _procedures.get( idx );
    }

    /**
     * Store a function descriptor. Updates {@link #_functions}.
     *
     * @param schema The schema of the function
     * @param name The name of the function
     * @param isTableFunction {@code true} iff the function is a table function
     */
    private void putFunction( String schema, String name, boolean isTableFunction )
    {
        _functions.add( new SQLRoutine( schema, name, isTableFunction )  );
    }
        
    /**
     * Get a function descriptor from {@link #_functions}
     * .
     * @param idx The index of the procedure in {@link #_functions}.
     * @return a function descriptor
     */
    private SQLRoutine getFunction( int idx )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        return _functions.get( idx );
    }

    /**
     * Format a localizable message.
     *
     * @param key The message key by which we located the localized text
     * @param args Any arguments to the localized text to be filled in
     * @return A localized message
     */
    private static String formatMessage(String key, Object... args)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6262
        return getMessageFormatter().getTextMessage(key, args);
    }
    
    /**
     * Get the message resource.
     *
     * @return localized resource
     */
    private static  LocalizedResource   getMessageFormatter()
    {
        if ( _messageFormatter == null )
        {
            _messageFormatter = LocalizedResource.getInstance();
        }
        return _messageFormatter;
    }


        
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    static class ParsedArgs
    {
        private boolean _isValid;
        private String _j2seConnectionUrl;

        public  ParsedArgs( String[] args )
        {
            _isValid = false;
            parseArgs( args );
        }

        public boolean isValid() { return _isValid; }

        public String getJ2seConnectionUrl() { return _j2seConnectionUrl; }

        private void parseArgs( String[] args )
        {
            if ( args == null ) { return; }
            if ( args.length != 1 ) { return; }
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

            _j2seConnectionUrl = args[ 0 ];
            _isValid = true;
        }
    }

    class SQLRoutine
    {
        private final String _schema;
        private final String _name;
        private final boolean _isTableFunction;
        private final ArrayList<String>   _argList = new ArrayList<String>();

        public SQLRoutine( String schema, String name, boolean isTableFunction )
        {
            _schema = schema;
            _name = name;
            _isTableFunction = isTableFunction;
        }

        public void addArg( String typeName ) { _argList.add( typeName ); }

        public String getSchema() { return _schema; }
        public String getName() { return _name; }
        public int      getArgCount() { return _argList.size(); }
        public String getArgType( int idx ) { return _argList.get( idx ); }
        public boolean isTableFunction() { return _isTableFunction; }

        @Override
        public  String  toString()
        {
            StringBuilder    buffer = new StringBuilder();
//IC see: https://issues.apache.org/jira/browse/DERBY-6638
//IC see: https://issues.apache.org/jira/browse/DERBY-6639

            buffer.append( "SQLRoutine( " );
            buffer.append( _schema );
            buffer.append( ", " );
            buffer.append( _name );
            buffer.append( ", " );
            buffer.append(  "isTableFunction = " );
            buffer.append( _isTableFunction );
            buffer.append( ", " );
            buffer.append( " argCount = " );
            buffer.append( getArgCount() );
            buffer.append( " )" );

            return buffer.toString();
        }

        private String doubleQuote( String raw )
        {
            return '\"' + raw + '\"';
        }
        public  String  getQualifiedName()
        {
            return doubleQuote( _schema ) + '.' + doubleQuote( _name );
        }

    }
    
}
