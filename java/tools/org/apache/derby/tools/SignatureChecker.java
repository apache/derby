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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.jdbc.EmbeddedSimpleDataSource;
import org.apache.derby.shared.common.reference.JDBC40Translation;
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

    private ParsedArgs _parsedArgs;

    private ArrayList   _procedures = new ArrayList();
    private ArrayList   _functions = new ArrayList();

    private boolean     _debugging = false;
    
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
            Connection  conn;
        
            if ( _parsedArgs.isJ2ME() ) { conn = getJ2MEConnection(); }
            else { conn = getJ2SEConnection(); }

            if ( conn == null )
            {
                println(  formatMessage( "SC_NO_CONN" )  );
                return;
            }
            else
            {
                matchSignatures( conn );

                conn.close();
            }
            
        } catch (Throwable t) { printThrowable( t ); }
    }

    /**
     * <p>
     * Match the signatures of routines in the database attached to this connection.
     * </p>
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
            StringBuffer buffer = new StringBuffer();
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

            checkSignature( conn, procedure, buffer.toString(), makeReadableSignature( procedure ) );
        }
    }

    /**
     * <p>
     * Match the signatures of functions in this database.
     * </p>
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
            StringBuffer query = new StringBuffer();
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

            checkSignature( conn, function, query.toString(), makeReadableSignature( function ) );
        }
    }

    /**
     * <p>
     * Make a human readable signature for a routine. This can be
     * used in error messages.
     * </p>
     */
    private String  makeReadableSignature( SQLRoutine routine )
    {
        StringBuffer signature = new StringBuffer();
        int             argCount = routine.getArgCount();
        
        signature.append( routine.getQualifiedName() );
        signature.append( "( " );
        for ( int k = 0; k < argCount; k++ )
        {
            if ( k > 0 ) { signature.append( ", " ); }
            signature.append( " " + routine.getArgType( k ) + " " );
        }
        signature.append( " )" );

        return signature.toString();
    }

    /**
     * <p>
     * Find all of the user-declared procedures.
     * </p>
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
     * Count up the arguments to the user-coded procedures.
     * </p>
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
     * the JSR169 api for DatabaseMetaData.
     * </p>
     */
    private void    findFunctions( DatabaseMetaData dbmd )
        throws SQLException
    {
        try {
            Method      getFunctionsMethod = dbmd.getClass().getMethod
                ( "getFunctions", new Class[] { String.class, String.class, String.class } );
            ResultSet   rs = (ResultSet) getFunctionsMethod.invoke
                ( dbmd, new Object[] { null, null, WILDCARD } );

            while( rs.next() )
            {
                String  schema = rs.getString( 2 );
                String  name = rs.getString( 3 );
                short   functionType = rs.getShort( 5 );

                if ( isSystemSchema( schema ) ) { continue; }

                boolean isTableFunction;
                if ( functionType == JDBC40Translation.FUNCTION_RETURNS_TABLE ) { isTableFunction = true; }
                else { isTableFunction = false; }

                putFunction( schema, name, isTableFunction );
            }
            rs.close();

            
        } catch (Exception e) { throw new SQLException( e.getMessage() ); }
    }

    /**
     * <p>
     * Count up the arguments to the user-coded procedures. We use
     * reflection to look up the getFunctionColumns() method because that
     * method does not appear in the JSR169 api for DatabaseMetaData.
     * </p>
     */
    private void    countFunctionArgs( DatabaseMetaData dbmd )
        throws SQLException
    {
        try {
            Method      getFunctionColumnsMethod = dbmd.getClass().getMethod
                ( "getFunctionColumns", new Class[] { String.class, String.class, String.class, String.class } );

            int     count = _functions.size();
            for ( int i = 0; i < count; i++ )
            {
                SQLRoutine  function = getFunction( i );

                ResultSet   rs = (ResultSet) getFunctionColumnsMethod.invoke
                    ( dbmd, new Object[] { null, function.getSchema(), function.getName(), WILDCARD } );

                while( rs.next() )
                {
                    short   columnType = rs.getShort( 5 );

                    //
                    // Skip the return value if this is a table function.
                    // Skip all columns in the returned result set if this is a
                    // table function.
                    //
                    if ( columnType == JDBC40Translation.FUNCTION_RETURN ) { continue; }
                    if ( columnType == JDBC40Translation.FUNCTION_COLUMN_RESULT ) { continue; }
                    
                    function.addArg( rs.getString( 7 ) );
                }
                rs.close();
            }
        } catch (Exception e) { throw new SQLException( e.getMessage() ); }
        
    }
    
    /**
     * <p>
     * Prepared a routine invocation in order to check whether it matches a Java method.
     * </p>
     */
    private void    checkSignature( Connection conn, SQLRoutine routine, String query, String readableSignature )
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

    private Connection  getJ2MEConnection()
        throws SQLException
    {
        EmbeddedSimpleDataSource    dataSource = new EmbeddedSimpleDataSource();

        dataSource.setDatabaseName( _parsedArgs.getJ2meDatabaseName() );

        return dataSource.getConnection();
    }

    /**
     * We use reflection to get the J2SE connection so that references to
     * DriverManager will not generate linkage errors on old J2ME platforms
     * which may resolve references eagerly.
     */
    private Connection  getJ2SEConnection()
        throws SQLException
    {
        try {
            Class.forName( "org.apache.derby.jdbc.EmbeddedDriver" );
            Class.forName( "org.apache.derby.jdbc.ClientDriver" );
        } catch (Throwable t) {}

        try {
            Class   driverManagerClass = Class.forName( "java.sql.DriverManager" );
            Method  getConnectionMethod = driverManagerClass.getDeclaredMethod
                ( "getConnection", new Class[] { String.class } );

            return (Connection) getConnectionMethod.invoke
                ( null, new Object[] { _parsedArgs.getJ2seConnectionUrl() } );
            
        } catch (Throwable t)
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
     * Store a procedure descriptor.
     */
    private void putProcedure( String schema, String name )
    {
        _procedures.add( new SQLRoutine( schema, name, false )  );
    }
        
    /**
     * Get a procedure descriptor.
     */
    private SQLRoutine getProcedure( int idx )
    {
        return (SQLRoutine) _procedures.get( idx );
    }

    /**
     * Store a function descriptor.
     */
    private void putFunction( String schema, String name, boolean isTableFunction )
    {
        _functions.add( new SQLRoutine( schema, name, isTableFunction )  );
    }
        
    /**
     * Get a functon descriptor.
     */
    private SQLRoutine getFunction( int idx )
    {
        return (SQLRoutine) _functions.get( idx );
    }

    /**
     * Format a localizable message with 0 args.
     */
    private static  String  formatMessage( String key )
    {
        return getMessageFormatter().getTextMessage( key );
    }
    
    /**
     * Format a localizable message with 1 arg.
     */
    private static  String  formatMessage( String key, String arg0 )
    {
        return getMessageFormatter().getTextMessage( key, arg0 );
    }
    
    /**
     * Format a localizable message with 2 args.
     */
    private static  String  formatMessage( String key, String arg0, String arg1 )
    {
        return getMessageFormatter().getTextMessage( key, arg0, arg1 );
    }
    
    /**
     * Get the message resource.
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
        private boolean _isJ2ME;
        private String _j2seConnectionUrl;
        private String _j2meDatabaseName;

        public  ParsedArgs( String[] args )
        {
            _isValid = false;
            
            _isJ2ME = !classExists( "java.sql.DriverManager" );

            parseArgs( args );
        }

        public boolean isValid() { return _isValid; }
        public boolean isJ2ME() { return _isJ2ME; }

        public String getJ2seConnectionUrl() { return _j2seConnectionUrl; }
        public String getJ2meDatabaseName() { return _j2meDatabaseName; }

        private void parseArgs( String[] args )
        {
            if ( args == null ) { return; }
            if ( args.length == 0 ) { return; }
            
            if ( isJ2ME() )
            {
                if ( args.length != 1 ) { return; }

                _j2meDatabaseName = args[ 0 ];
                _isValid = true;
            }
            else
            {
                if ( args.length != 1 ) { return; }

                _j2seConnectionUrl = args[ 0 ];
                _isValid = true;
            }
        }

        private boolean classExists( String className )
        {
            try {
                Class.forName( className );

                return true;
            } catch (Throwable t) { return false; }
        }
    }

    class SQLRoutine
    {
        private String _schema;
        private String _name;
        private boolean _isTableFunction;
        private ArrayList   _argList = new ArrayList();

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
        public String getArgType( int idx ) { return (String) _argList.get( idx ); }
        public boolean isTableFunction() { return _isTableFunction; }

        public  String  toString()
        {
            StringBuffer    buffer = new StringBuffer();

            buffer.append( "SQLRoutine( " );
            buffer.append( _schema + ", " );
            buffer.append( _name + ", " );
            buffer.append(  "isTableFunction = " + _isTableFunction + ", " );
            buffer.append( " argCount = " + getArgCount() );
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
