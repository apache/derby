/*

   Class org.apache.derby.optional.dump.DataFileVTI

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

package org.apache.derby.optional.dump;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.security.AccessController;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

import org.apache.derby.authentication.UserAuthenticator;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.crypto.CipherFactory;
import org.apache.derby.iapi.services.crypto.CipherProvider;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.services.io.FormatIdInputStream;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.sql.dictionary.PasswordHasher;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.impl.jdbc.authentication.JNDIAuthenticationService;
import org.apache.derby.impl.services.jce.JCECipherFactoryBuilder;
import org.apache.derby.impl.store.raw.data.MemByteHolder;
import org.apache.derby.impl.store.raw.data.StoredRecordHeader;
import org.apache.derby.impl.store.raw.data.StoredFieldHeader;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.vti.VTITemplate;


/**
 * Table function for reading a data file in the seg0 directory of a
 * Derby database. This is based on the 10.11 version of
 * data records.
 */
public  class   DataFileVTI extends VTITemplate
{
    ////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    public  static  final   String  SYSSCHEMAS_SIGNATURE = "( schemaID char(36), schemaname varchar(128), authorizationid varchar(128) )";
    public  static  final   String  SYSSCHEMAS_CONGLOMERATE_NAME = "cc0.dat";
    public  static  final   String  SYSTABLES_SIGNATURE = "( tableid char(36), tablename varchar(128), tabletype char(1), schemaid char(36), lockgranularity char(1) )";
    public  static  final   String  SYSTABLES_CONGLOMERATE_NAME = "c60.dat";
    public  static  final   String  SYS_SCHEMA_ID = "8000000d-00d0-fd77-3ed8-000a0a0b1900";
    public  static  final   String  SYSUSERS_SIGNATURE = "( username  varchar( 128 ), hashingscheme  varchar( 32672 ), password  varchar( 32672 ), lastmodified timestamp )";
    public  static  final   String  SYSUSERS_CONGLOMERATE_NAME = "c470.dat";
    public  static  final   String  PROPERTIES_SIGNATURE = "( keyname serializable, payload serializable )";
    public  static  final   String  PROPERTIES_CONGLOMERATE_NAME = "c10.dat";
    
    private static  final   String  COMPILATION_DB = "dfv_compilation_db";
    private static  final   String  DUMMY_TABLE_NAME = "dfv_dummy";

    private static  final   long READ_ALL_PAGES = -1L;

    public  static final int CHECKSUM_SIZE    = 8;
    public  static final int SMALL_SLOT_SIZE  = 2;
    public  static final int LARGE_SLOT_SIZE  = 4;

    // record status
    public static final byte RECORD_HAS_FIRST_FIELD = 0x04;

    ////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    ////////////////////////////////////////////////////////////////////////

    //
    // Filled in from the constructor args.
    //
    private DataFile    _dataFile;

    //
    // For controlling the iteration through rows.
    //
    private boolean _opened = false;
    private ArrayList<DataValueDescriptor[]>    _rows;
    private int                 _rowIdx;
    private boolean     _lastColumnWasNull;

    // transient state
    private Calendar    _defaultCalendar = Calendar.getInstance();
    
    ////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Private constructor which performs decryption but not authentication.
     */
    private  DataFileVTI
        (
         String databaseDirectoryName,
         String dataFileName,
         String tableSignature,
         String encryptionProperties
         )
        throws Exception
    {
        Connection  conn = getCompilerConnection();
        DataTypeDescriptor[]   rowSignature = getTypeSignature( conn, tableSignature );

        _dataFile = new DataFile
            (
             new File( databaseDirectoryName ),
             dataFileName,
             encryptionProperties,
             rowSignature
             );
    }

    /** Public constructor */
    public  DataFileVTI
        (
         String databaseDirectoryName,
         String dataFileName,
         String tableSignature,
         String encryptionProperties,
         String user,
         String password
         )
        throws Exception
    {
        this( databaseDirectoryName, dataFileName, tableSignature, encryptionProperties );

        authenticate( databaseDirectoryName, encryptionProperties, user, password );
    }

    /**
     * Get a connection to a transient database which is only used to
     * compile table signatures.
     */
    private Connection  getCompilerConnection() throws SQLException
    {
        Connection  conn = DriverManager.getConnection( "jdbc:derby:memory:" + COMPILATION_DB + ";create=true" );

        // create the serializable type if it doesn't exist
        PreparedStatement   ps = conn.prepareStatement( "select count(*) from sys.sysaliases where alias = 'SERIALIZABLE'" );
        ResultSet   rs = ps.executeQuery();
        rs.next();
        boolean     alreadyExists = (rs.getInt( 1 ) > 0);
        rs.close();
        ps.close();

        if ( !alreadyExists )
        {
            conn.prepareStatement( "create type serializable external name 'java.io.Serializable' language java" ).execute();
        }

        return conn;
    }

    private DataTypeDescriptor[] getTypeSignature( Connection conn, String tableSignature )
        throws Exception
    {
        String  createTable = "create table " + DUMMY_TABLE_NAME + tableSignature;
        String  dropTable = "drop table " + DUMMY_TABLE_NAME;

        try {
            conn.prepareStatement( createTable ).execute();
        }
        catch (SQLException se)
        {
            throw new Exception( "Illegal table signature: " + tableSignature, se );
        }

        String  select = "select c.columndatatype, c.columnnumber\n" +
            "from sys.syscolumns c, sys.systables t\n" +
            "where c.referenceid = t.tableid\n" +
            "and t.tablename = ?\n" +
            "order by c.columnnumber";
        PreparedStatement   ps = conn.prepareStatement( select );
        ps.setString( 1, DUMMY_TABLE_NAME.toUpperCase() );
        ResultSet   rs = ps.executeQuery();

        ArrayList<DataTypeDescriptor>   list = new ArrayList<DataTypeDescriptor>();

        while ( rs.next() )
        {
            list.add( DataTypeDescriptor.getType( (TypeDescriptor) rs.getObject( 1 ) ) );
        }

        rs.close();
        ps.close();

        DataTypeDescriptor[]    result = new DataTypeDescriptor[ list.size() ];
        list.toArray( result );

        conn.prepareStatement( dropTable ).execute();

        return result;
    }
    
    private static  void    skipBytes( DataInputStream dais, int bytesToSkip ) throws IOException
    {
        int     actualBytesSkipped = dais.skipBytes( bytesToSkip );

        if ( actualBytesSkipped != bytesToSkip )
        {
            throw new IOException( "Expected to skip " + bytesToSkip + " bytes but only skipped " + actualBytesSkipped + " bytes." );
        }
    }

    // See FileContainer.decryptPage()
    private static  byte[] decryptPage( CipherProvider decryptionEngine, byte[] cipherText ) throws IOException
    {
        try {
            int     pageSize = cipherText.length;
            byte[]  clearText = new byte[ pageSize ];
        
            int len = decryptionEngine.decrypt( cipherText, 0, pageSize, clearText, 0 );
        
            if ( len != pageSize )
            {
                throw new IOException( "Expected to decrypt " + pageSize + " bytes but actually decrypted " + len + " bytes." );
            }
        
            // need to put the checksum where it belongs
            System.arraycopy( clearText, 8, cipherText, 0, pageSize-8);
            System.arraycopy( clearText, 0, cipherText, pageSize-8, 8);
        
            return cipherText;
        } catch( Exception e) { throw new IOException( e ); }
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //  AUTHENTICATION
    //
    ////////////////////////////////////////////////////////////////////////
    
    /**
     * Authenticate the user's permission to access the raw database.
     * The following hurdles must be passed. Otherwise, an exception is raised.
     *
     * <ul>
     * <li>The user must be the DBO of the raw database.</li>
     * <li>If there are any tuples in the SYSUSERS catalog of the raw database, then the supplied credentials must match
     * what's in SYSUSERS.</li>
     * <li>If the database properties of the raw database specify an authentication scheme,
     * then that scheme must be applied to the supplied credentials.</li>
     * </ul>
     */
    private void    authenticate
        (
         String databaseDirectoryName,
         String encryptionProperties,
         String user,
         String password
         )
        throws Exception
    {
        vetDBO( databaseDirectoryName, encryptionProperties, user );
        if ( vetNative( databaseDirectoryName, encryptionProperties, user, password ) ) { return; }
        vetRawAuthentication( databaseDirectoryName, encryptionProperties, user, password );
    }

    /**
     * Verify that the user is the DBO of the raw database.
     */
    private void    vetDBO
        (
         String databaseDirectoryName,
         String encryptionProperties,
         String user
         )
        throws Exception
    {
        boolean isDBO = false;
        boolean foundSYSIBM = false;

        if ( user != null )
        {
            String          authorizationID = StringUtil.normalizeSQLIdentifier( user );
            DataFileVTI    sysschemas = new DataFileVTI
                (
                 databaseDirectoryName,
                 SYSSCHEMAS_CONGLOMERATE_NAME,
                 SYSSCHEMAS_SIGNATURE,
                 encryptionProperties
                 );

            while( sysschemas.next() )
            {
                if ( "SYSIBM".equals( sysschemas.getString( 2 ) ) )
                {
                    foundSYSIBM = true;
                    if ( sysschemas.getString( 3 ).equals( authorizationID ) ) { isDBO = true; }
                    break;
                }
            }
            sysschemas.close();
        }
        
        if ( !foundSYSIBM )
        {
            throw new Exception
                (
                 "Could not read database at " + databaseDirectoryName +
                 ". Maybe it is encrypted and the wrong encryption key was supplied."
                 );
        }

        if ( !isDBO )
        {
            throw new Exception( user + " is not the owner of the database at " + databaseDirectoryName );
        }
    }

    /**
     * If NATIVE authentication is on, then the user's credentials must be stored in SYSUSERS.
     * Returns false if NATIVE authentication is not on. Raises an exception if NATIVE
     * authentication is on and the credentials don't match.
     */
    private boolean    vetNative
        (
         String databaseDirectoryName,
         String encryptionProperties,
         String user,
         String password
         )
        throws Exception
    {
        String          authorizationID = StringUtil.normalizeSQLIdentifier( user );
        DataFileVTI    systables = new DataFileVTI
            (
             databaseDirectoryName,
             SYSTABLES_CONGLOMERATE_NAME,
             SYSTABLES_SIGNATURE,
             encryptionProperties
             );
        boolean sysusersExists = false;

        while( systables.next() )
        {
            if ( SYS_SCHEMA_ID.equals( systables.getString( 4 ) ) )
            {
                if ( "SYSUSERS".equals( systables.getString( 2 ) ) )
                {
                    sysusersExists = true;
                    break;
                }
            }
        }
        systables.close();

        // nothing more to check if the database doesn't even have a SYSUSERS catalog
        if ( !sysusersExists ) { return false; }

        //
        // See whether the user's credentials should be and are in SYSUSERS.
        //
        
        DataFileVTI    sysusers = new DataFileVTI
            (
             databaseDirectoryName,
             SYSUSERS_CONGLOMERATE_NAME,
             SYSUSERS_SIGNATURE,
             encryptionProperties
             );
        boolean credentialsShouldBePresent = false;
        boolean credentialsMatch = false;
            
        while( sysusers.next() )
        {
            credentialsShouldBePresent = true;

            if ( sysusers.getString( 1 ).equals( authorizationID ) )
            {
                String  hashingScheme = sysusers.getString( 2 );
                String  actualPassword = sysusers.getString( 3 );
                PasswordHasher  hasher = new PasswordHasher( hashingScheme );
                String  candidatePassword = hasher.hashPasswordIntoString( authorizationID, password );

                credentialsMatch = actualPassword.equals( candidatePassword );

                break;
            }
        }
        sysusers.close();

        if ( !credentialsShouldBePresent ) { return false; }
        if ( credentialsMatch ) { return true; }
        else
        {
            throw new Exception( "Bad NATIVE credentials." );
        }
    }

    /**
     * If an authentication scheme is specified by database properties in the raw
     * database, then use that scheme to validate the credentials.
     */
    private void    vetRawAuthentication
        (
         String databaseDirectoryName,
         String encryptionProperties,
         String user,
         String password
         )
        throws Exception
    {
        Properties  props = readDatabaseProperties( databaseDirectoryName, encryptionProperties );

		String requireAuthentication = props.getProperty( Property.REQUIRE_AUTHENTICATION_PARAMETER );
		if ( !Boolean.valueOf( requireAuthentication ).booleanValue() ) { return; }
        
		String provider = props.getProperty( Property.AUTHENTICATION_PROVIDER_PARAMETER );
        if ( provider == null ) { return; } // no provider specified

        // the provider cannot be NATIVE. that case was handled above by vetNative()

        boolean authenticated;
        
		if ( StringUtil.SQLEqualsIgnoreCase( provider, Property.AUTHENTICATION_PROVIDER_LDAP ) )
        { authenticated = vetLDAP( props, databaseDirectoryName, user, password ); }
		else if ( StringUtil.SQLEqualsIgnoreCase( provider, Property.AUTHENTICATION_PROVIDER_BUILTIN ) )
        { authenticated = vetBuiltin( props, user, password ); }
        else { authenticated = vetCustom( provider, databaseDirectoryName, user, password ); }

        if ( !authenticated )
        {
            throw new Exception( "Authentication failed using provider " + provider );
        }
    }

    /**
     * Match credentials using an LDAP server.
     */
    private boolean vetLDAP
        (
         Properties dbProps,
         String databaseDirectoryName,
         String user,
         String password
         )
        throws Exception
    {
        LDAPService   authenticator = new LDAPService( dbProps );

        authenticator.boot( false, dbProps );

        Properties  userInfo = new Properties();
        userInfo.setProperty( Attribute.USERNAME_ATTR, user );
        userInfo.setProperty( Attribute.PASSWORD_ATTR, password );

        return authenticator.authenticate( databaseDirectoryName, userInfo );
    }
    
    /**
     * Match credentials against the BUILTIN credentials stored in the properties
     * conglomerate of the raw database. All of those properties have been read
     * into the props object already.
     */
    private boolean vetBuiltin
        (
         Properties props,
         String user,
         String password
         )
        throws Exception
    {
        String  passwordProperty = Property.USER_PROPERTY_PREFIX.concat( user );
        String  realPassword = props.getProperty( passwordProperty );

        if ( realPassword != null )
        {
            PasswordHasher  hasher = new PasswordHasher( realPassword );

            password = hasher.hashAndEncode( user, password );
        }
        else
        {
            realPassword = getSystemProperty( passwordProperty );
        }

        return ( (realPassword != null) && realPassword.equals( password ) );
    }
    
    /**
     * Validate credentials using a custom authenticator.
     */
    private boolean vetCustom
        (
         String customProvider,
         String databaseDirectoryName,
         String user,
         String password
         )
        throws Exception
    {
        Class<?> clazz = Class.forName( customProvider );
        UserAuthenticator   authenticator = (UserAuthenticator) clazz.getConstructor().newInstance();

        return authenticator.authenticateUser( user, password, databaseDirectoryName, new Properties() );
    }
    
    /**
     * Get system property.
     */
    private static String getSystemProperty( final String name)
	{
        return AccessController.doPrivileged
            ( new java.security.PrivilegedAction<String>()
                {
                    public String run() { return System.getProperty( name ); }
                }
            );
    }

    /**
     * Read the properties conglomerate of the raw database.
     */
    private Properties  readDatabaseProperties
        (
         String databaseDirectoryName,
         String encryptionProperties
         )
        throws Exception
    {
        Properties  retval = new Properties();
        
        DataFileVTI    pc = new DataFileVTI
            (
             databaseDirectoryName,
             PROPERTIES_CONGLOMERATE_NAME,
             PROPERTIES_SIGNATURE,
             encryptionProperties
             );

        while( pc.next() )
        {
            int col = 1;
            Object  key = pc.getObject( col++ );
            Object  value = pc.getObject( col++ );

            retval.put( key, value );
        }
        pc.close();

        return retval;
    }
    
    ////////////////////////////////////////////////////////////////////////
    //
    //  TABLE FUNCTION
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Entry point declared in the external name clause of a CREATE FUNCTION statement.
     */
    public  static  DataFileVTI dataFileVTI
        (
         String databaseDirectoryName,
         String dataFileName,
         String tableSignature,
         String encryptionProperties,
         String user,
         String password
         )
        throws Exception
    {
        return new DataFileVTI( databaseDirectoryName, dataFileName, tableSignature, encryptionProperties, user, password );
    }

    private SQLException    wrap( Throwable t )
    {
        return new SQLException( t.getMessage(), t );
    }
    
    private SQLException    wrap( String errorMessage )
    {
        String  sqlState = SQLState.JAVA_EXCEPTION.substring( 0, 5 );

        return new SQLException( errorMessage, sqlState );
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //  VTITemplate IMPLEMENTATION
    //
    ////////////////////////////////////////////////////////////////////////

    public  boolean next()  throws SQLException
    {
        if ( _dataFile == null ) { return false; }
        
        try {
            if ( !_opened )
            {
                _dataFile.openFile();
                _opened = true;
                readNextPage();
            }

            while ( _rows != null )
            {
                _rowIdx++;

                if ( _rowIdx < _rows.size() ) { return true; }
                else { readNextPage(); }
            }

            close();
            return false;
        }
        catch (Throwable t)
        {
            if ( t instanceof SQLException) { throw (SQLException) t; }
            else    { throw wrap( t ); }
        }
    }
    /**
     * Read the next page of rows.
     */
    private void    readNextPage()  throws Exception
    {
        _rows = _dataFile.readNextPage();
        _rowIdx = -1;
    }
    
    /** Get the warnings */
    public SQLWarning  getWarnings()
    {
        SQLWarning  firstWarning = null;
        ArrayList<SQLWarning>   warnings = _dataFile.getWarnings();
        if ( (warnings != null) && (warnings.size() > 0) )
        {
            firstWarning = warnings.get( 0 );
            
            // chain the warnings together
            SQLWarning  previous = null;
            for ( SQLWarning warning : warnings )
            {
                if ( previous != null ) { previous.setNextWarning( warning ); }
                previous = warning;
            }
        }

        return firstWarning;
    }

    public  void    close() throws SQLException
    {
        try {
            if ( _dataFile != null ) { _dataFile.closeFile(); }
        }
        catch (Throwable t) { throw wrap( t ); }
        finally
        {
            _dataFile = null;
            _rows = null;
        }
    }

    public  ResultSetMetaData   getMetaData()   { return null; }

    /** Get a column value (1-based indexing) and check if it's null */
    private DataValueDescriptor getRawColumn( int idx )
    {
        DataValueDescriptor dvd = _rows.get( _rowIdx )[ idx - 1 ];

        _lastColumnWasNull = dvd.isNull();

        return dvd;
    }

    
    public boolean wasNull() { return _lastColumnWasNull; }
    
    public String getString(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getString(); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public boolean getBoolean(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getBoolean(); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public byte getByte(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getByte(); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public short getShort(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getShort(); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public int getInt(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getInt(); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public long getLong(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getLong(); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public float getFloat(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getFloat(); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public double getDouble(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getDouble(); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public byte[] getBytes(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getBytes(); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public Date getDate(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getDate( _defaultCalendar ); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public Time getTime(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getTime( _defaultCalendar ); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public Timestamp getTimestamp(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getTimestamp( _defaultCalendar ); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public Object getObject(int columnIndex) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getObject(); }
        catch (Throwable t) { throw wrap( t ); }
    }
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        try { return (BigDecimal) getRawColumn( columnIndex ).getObject(); }
        catch (Throwable t) { throw wrap( t ); }
    }
	public Date getDate(int columnIndex, Calendar cal) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getDate( cal ); }
        catch (Throwable t) { throw wrap( t ); }
    }
	public Time getTime(int columnIndex, Calendar cal) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getTime( cal ); }
        catch (Throwable t) { throw wrap( t ); }
    }
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
    {
        try { return getRawColumn( columnIndex ).getTimestamp( cal ); }
        catch (Throwable t) { throw wrap( t ); }
    }
	public Blob getBlob(int columnIndex) throws SQLException
    {
        try { return (Blob) getRawColumn( columnIndex ).getObject(); }
        catch (Throwable t) { throw wrap( t ); }
    }
	public Clob getClob(int columnIndex) throws SQLException
    {
        try { return (Clob) getRawColumn( columnIndex ).getObject(); }
        catch (Throwable t) { throw wrap( t ); }
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //  NESTED CLASSES
    //
    ////////////////////////////////////////////////////////////////////////
    
    ////////////////////////////////////////////////
    //
    //  LDAP SERVICE
    //
    ////////////////////////////////////////////////

    public  static  final   class   LDAPService extends JNDIAuthenticationService
    {
        private Properties  _dbProperties;

        public  LDAPService( Properties dbProperties )
        {
            super();
            _dbProperties = dbProperties;
        }

        // override to use properties in raw database
        public String getProperty(String key)
        {
            return _dbProperties.getProperty( key );
        }
    }
    
    ////////////////////////////////////////////////
    //
    //  THIS IS THE WORKHORSE FOR THIS PROGRAM
    //
    ////////////////////////////////////////////////
    
    public  static  final   class   DataFile
    {
        // constructor args
        
        private File        _dbDirectory;
        private File            _file;
        private DataTypeDescriptor[]   _rowSignature;
        private CipherProvider  _decryptionEngine;

        // control info

        private FileInputStream _fis;
        private DataInputStream _dais;
        private long            _pageCount;
        private byte[]          _pageData;
        private int             _pageSize;
        private SlotReader  _slotReader;
        private OverflowStream  _overflowStream;
        private ArrayList<SQLWarning>  _warnings;
    
        public  DataFile
            ( File dbDirectory, String fileName, String encryptionProperties, DataTypeDescriptor[] rowSignature )
            throws Exception
        {
            _dbDirectory = dbDirectory;
            _file = new File( new File( _dbDirectory, "seg0" ), fileName );
            _rowSignature = rowSignature;
            _decryptionEngine = makeDecryptionEngine( encryptionProperties );
        }

        private CipherProvider  makeDecryptionEngine( String encryptionProperties ) throws Exception
        {
            if ( encryptionProperties == null ) { return null; }
            
            File    serviceProperties = new File( _dbDirectory, "service.properties" );
            Properties  properties = unpackEncryptionProperties( encryptionProperties );
            try (FileInputStream in = new FileInputStream(serviceProperties)) {
                properties.load(in);
            }

            CipherFactory    cipherFactory =
                new JCECipherFactoryBuilder()
                .createCipherFactory( false, properties, false );

            return cipherFactory.createNewCipher( CipherFactory.DECRYPT );
        }
        private Properties  unpackEncryptionProperties( String encryptionProperties )
        {
            Properties  retval = new Properties();
            String[]    items = encryptionProperties.split( ";" );

            for ( String item : items )
            {
                int     equalsSignIdx = item.indexOf( "=" );
                if ( equalsSignIdx > 0 )
                {
                    String  key = item.substring( 0, equalsSignIdx );
                    String  value = item.substring( equalsSignIdx + 1, item.length() );

                    retval.setProperty( key, value );
                }
            }

            return retval;
        }
        
        //
        // Order of operations is:
        //
        //   openFile()
        //   readNextPage() until returns null
        //   closeFile()
        //

        public  void    openFile()  throws Exception
        {
            _pageCount = 0L;

            _fis = new FileInputStream( _file );
            WrapperInputStream  wis = new WrapperInputStream( _fis );
            _dais = new DataInputStream( wis );
            
            readFileHeader();
            skipToPageEnd( wis );
        }

        /** Closes the file and returns a list of errors or null if no errors. */
        public  void    closeFile() throws Exception
        {
            _dais.close();
            _fis.close();
        }

        /** Get the errors which accumulated since the last time this method was called. Returns null if no errors. */
        public  ArrayList<SQLWarning>   getWarnings()
        {
            try { return _warnings; }
            finally { _warnings = null; }
        }
        /** Add a warning */
        private void    addWarning( String message, Throwable t )
        {
            if ( _warnings == null ) { _warnings = new ArrayList<SQLWarning>(); }

            _warnings.add( new SQLWarning( message, t ) );
        }

        //////////////////
        //
        // FILE HEADER
        //
        //////////////////

        private void    readFileHeader() throws Exception
        {
            readAllocPage( _dais, PageHeader.readPageFormatableID( _dais ) );

            _pageCount++;
        }

        private void    skipToPageEnd( WrapperInputStream wis )   throws Exception
        {
            int          remainingBytesOnPage = getRemainingBytesOnPage( wis );

            DataInputStream dais = new DataInputStream( wis );
            if ( remainingBytesOnPage != 0 ) { skipBytes( dais, remainingBytesOnPage ); }
        }

        private int getRemainingBytesOnPage( WrapperInputStream wis )
        {
            long        offsetIntoCurrentPage = currentOffsetIntoPage( wis );

            int         remainingBytes = ( offsetIntoCurrentPage == 0L ) ? 0 :  (int) (_pageSize - offsetIntoCurrentPage);

            return remainingBytes;
        }

        private int currentOffsetIntoPage( WrapperInputStream wis )
        {
            return (int) (wis.getBytesRead() % (long)_pageSize);
        }

        //////////////////
        //
        // PAGE DISPATCH
        //
        //////////////////

        /**
         * Returns rows on the next page. Returns null if at EOF.
         */
        public    ArrayList<DataValueDescriptor[]>  readNextPage()
        {
            _pageData = new byte[ _pageSize ];
            
            try {
                _dais.readFully( _pageData );
            }
            catch (Throwable t)
            {
                // errors caught here are fatal file stream errors
                if ( !( t instanceof EOFException ) )   { addWarning( formatThrowable( t, true ), t ); }
                
                return null;
            }

            try {
                return readNonHeaderPage();
            }
            catch (Throwable t)
            {
                // errors caught here are localized corruptions or mistakes in the coding of this class
                addWarning( formatThrowable( t, false ), t );
                
                return makeEmptyRowList();
            }
        }

        // returns null if at EOF
        private ArrayList<DataValueDescriptor[]> readNonHeaderPage() throws Exception
        {
            if ( _decryptionEngine != null )    { _pageData = decryptPage( _decryptionEngine, _pageData ); }

            WrapperInputStream  wis = new WrapperInputStream( new ByteArrayInputStream( _pageData ) );
            DataInputStream dais = new DataInputStream( wis );
            int pageFormatableID = PageHeader.readPageFormatableID( dais );

            ArrayList<DataValueDescriptor[]>    result;
            
            switch( pageFormatableID )
            {
            case StoredFormatIds.RAW_STORE_ALLOC_PAGE:
                result = readAllocPage( dais, pageFormatableID );
                break;
            case StoredFormatIds.RAW_STORE_STORED_PAGE:
                result = formatRows();
                break;
            default:
                throw new IOException( "Unknown page formatable ID: " + pageFormatableID );
            }

            _pageCount++;

            return result;
        }

        //////////////////////
        //
        // ALLOCATION PAGES
        //
        //////////////////////

        private ArrayList<DataValueDescriptor[]>    readAllocPage( DataInputStream dais, int formatableID ) throws Exception
        {
            if ( formatableID != StoredFormatIds.RAW_STORE_ALLOC_PAGE )
            {
                throw new IOException
                    (
                     "File header should start with formatable id " + StoredFormatIds.RAW_STORE_ALLOC_PAGE +
                     " but instead starts with formatable id " + formatableID
                     );
            }

            PageHeader.readPageHeader( dais );
            
            // documented in AllocPage.readAllocPageHeader()
            long    nextAllocPageNumber = dais.readLong();
            long    nextAllocPageOffset = dais.readLong();
            
            skipBytes( dais, 8 + 8 + 8 + 8 );
            
            byte    containerInfoLength = dais.readByte();
            
            if ( containerInfoLength > (byte) 0 ) { readContainerInfo( dais, containerInfoLength ); }

            // return empty set of rows
            return new ArrayList<DataValueDescriptor[]>();
        }

        // see FileContainer.writeHeaderToArray() for the format of the container descriptor
        private void    readContainerInfo( DataInputStream dais, byte containerInfoLength )
            throws Exception
        {
            int     formatableID = dais.readInt();
            if ( formatableID != StoredFormatIds.RAW_STORE_SINGLE_CONTAINER_FILE )
            {
                throw new IOException
                    (
                     "Container info should start with formatable id " + StoredFormatIds.RAW_STORE_SINGLE_CONTAINER_FILE +
                     " but instead starts with formatable id " + formatableID
                     );

            }
            
            int containerStatus = dais.readInt();
            
            _pageSize = dais.readInt();
            _slotReader = new SlotReader( _pageSize );
        }

        //////////////////
        //
        // DATA PAGES
        //
        //////////////////

        /**
         * Returns the set of rows on a page. If there are no rows on the page,
         * returns a 0-length list of rows.
         */
        private ArrayList<DataValueDescriptor[]>    formatRows() throws Exception
        {
            PageHeader ph = PageHeader.readPageHeader( _pageData );

            ArrayInputStream    ais = new ArrayInputStream( _pageData );
                    
            int     recordCount = ph.getSlotsInUse();

            ArrayList<DataValueDescriptor[]>    rows = makeEmptyRowList();

            // nothing to do if this is an overflow page
            if ( ph.isOverFlowPage() ) { return rows; }

            // largely cribbed from StoredPage.recordToString()
            for ( int slot = 0; slot < recordCount; slot++ )
            {
                // we need this in order to look at the RECORD_HAS_FIRST_FIELD status,
                // which is not exposed by StoredRecordHeader
                byte                        recordStatusByte = _pageData[ _slotReader.getRecordOffset( slot, _pageData ) ];
                StoredRecordHeader  recordHeader = _slotReader.getRecordHeader( slot, _pageData );
                int     offset = _slotReader.getRecordOffset( slot, _pageData );

                // skip deleted records
                if ( recordHeader.isDeleted() ) { continue; }

                int     fieldCount = recordHeader.getNumberFields();
                
                // move offset past record header to beginning of first field.
                offset += recordHeader.size();
                ais.setPosition( offset );
                        
                if ( fieldCount > 0 )
                {                            
                    DataValueDescriptor[]   row = makeEmptyRow();
                    boolean     rowIsValid = true;
                        
                    // field layout is described in StoredFieldHeader.write()
                    for ( int fieldIdx = 0; fieldIdx < fieldCount; fieldIdx++ )
                    {
                        int fieldStatus = StoredFieldHeader.readStatus( ais );
                        int fieldDataLength = StoredFieldHeader.readFieldDataLength( ais, fieldStatus, _slotReader.slotFieldSize() );
                        int fieldDataOffset = ais.getPosition();
                        
                        if ( fieldDataLength >= 0 ) // not null?
                        {
                            if ( StoredFieldHeader.isOverflow( fieldStatus ) )
                            {
                                long overflowPage;
                                int overflowId;
                                
                                // not likely to be a real pointer, this is most
                                // likely an old column chain where the first field
                                // is set to overflow even though the second field
                                // is the overflow pointer
                                if ( fieldIdx == 0 && fieldDataLength != 3 ) 
                                {
                                    // figure out where we should go next
                                    offset = ais.getPosition() + fieldDataLength;
                                    overflowPage = CompressedNumber.readLong( (DataInput) ais );
                                    overflowId = CompressedNumber.readInt( (DataInput) ais );
                                    
                                    printIrregularity( "questionable long column" );
                                    
                                    ais.setPosition( offset );
                                }
                                else
                                {
                                    overflowPage = CompressedNumber.readLong( (DataInput) ais );
                                    overflowId = CompressedNumber.readInt( (DataInput) ais );

                                    readOverflowField
                                        ( slot, fieldIdx, fieldDataOffset, fieldDataLength, row[ fieldIdx ], overflowPage, overflowId );
                                }
                                
                            }   // end if overflow
                            else    // not overflow
                            {
                                if ( fieldDataLength > 0 )
                                {
                                    //
                                    // Ignore records which aren't really rows.
                                    //
                                    if (
                                        recordHasFirstField( recordStatusByte )  &&
                                        ( (fieldCount == 1) || (fieldCount != _rowSignature.length) )
                                       )
                                    {
                                        rowIsValid = false;
                                    }
                                    else    // looks like a real row
                                    {
                                        try {
                                            readField( slot, fieldIdx, fieldDataOffset, fieldDataLength, row[ fieldIdx ] );
                                        } catch (Throwable t)
                                        {
                                            rowIsValid = false;
                                        }
                                    }
                                }
                                
                                // go to next field
                                offset = ais.getPosition() + fieldDataLength;
                                ais.setPosition(offset);
                            }   // end if not overflow
                        }   // end if not null
                            
                    }   // end loop through fields
                        
                    if ( rowIsValid )
                    {
                        rows.add( row );
                    }
                }   // end if there are fields
            }   // end of loop through records

            return rows;
        }

        private DataValueDescriptor[]   makeEmptyRow()  throws Exception
        {
            int columnCount = _rowSignature.length;
            DataValueDescriptor[]   row = new DataValueDescriptor[ columnCount ];

            for ( int i = 0; i < columnCount; i++ )
            {
                row[ i ] = _rowSignature[ i ].getNull();
            }

            return row;
        }

        /** Make an empty list of rows */
        private ArrayList<DataValueDescriptor[]> makeEmptyRowList() { return new ArrayList<DataValueDescriptor[]>(); }

        private boolean recordHasFirstField( byte recordStatus )
        {
            return ( (recordStatus & RECORD_HAS_FIRST_FIELD) != 0 );
        }
        
        private void  readField
            ( int recordNumber, int fieldNumber, int offset, int length, DataValueDescriptor dvd )
            throws Exception
        {
            try {
                byte[]  bytes = new byte[ length ];
                System.arraycopy( _pageData, offset, bytes, 0, length );
                ArrayInputStream    ais = new ArrayInputStream( bytes );
            
                dvd.readExternalFromArray( ais );
            }
            catch (Exception e) { formatFieldWarning( recordNumber, fieldNumber, offset, length, dvd, e ); }
        }

        private void  readOverflowField
            (
             int recordNumber, int fieldNumber, int offset, int length, DataValueDescriptor dvd,
             long overflowPage, int overflowID
             )
            throws Exception
        {
            try {
                OverflowStream  os = getOverflowStream().init( overflowPage, overflowID );
                FormatIdInputStream fiis = new FormatIdInputStream( os );

                if ( dvd instanceof StreamStorable ) 
                {
                    ( (StreamStorable) dvd).setStream( fiis );
                } 
                else 
                {
                    dvd.readExternal( fiis );
                }
            }
            catch (Exception e) { formatFieldWarning( recordNumber, fieldNumber, offset, length, dvd, e ); }
        }

        private OverflowStream  getOverflowStream()
            throws IOException
        {
            if ( _overflowStream == null )
            {
                _overflowStream = new OverflowStream
                    (
                     new RandomAccessFile( _file, "r" ),
                     _decryptionEngine,
                     _slotReader
                     );
            }

            return _overflowStream;
        }

        ////////////////////
        //
        // ERROR HANDLING
        //
        ////////////////////

        private void  formatFieldWarning
            ( int recordNumber, int fieldNumber, int offset, int length, DataValueDescriptor dvd, Throwable e )
        {
            String  errorMessage =
                "Error reading field data. Offset = " + offset + ", length = " + length +
                ", datatype = " + _rowSignature[ fieldNumber ].getSQLstring() +
                ": " + getFieldCoordinates( recordNumber, fieldNumber ) +
                ": " + formatThrowable( e, false );

            addWarning( errorMessage, e );
        }
        
        /** Coordinates of field which confused us */
        private String  getFieldCoordinates( int recordNumber, int fieldNumber )
        {
            return
                "Field " + fieldNumber +
                " in record " + recordNumber +
                getPageCoordinates();
        }

        /** Coordinates of page which confused us */
        private String  getPageCoordinates()
        {
            return
                " on page " + _pageCount +
                " in file " + _file.getName();
        }

        /** Format an error for printing */
        private String  formatThrowable( Throwable e, boolean includeStackTrace )
        {
            StringBuilder   buffer = new StringBuilder();
            buffer.append( e.getClass().getName() + ": " + e.getMessage() );

            if ( includeStackTrace )
            {
                StringWriter    sw = new StringWriter();
                PrintWriter     pw = new PrintWriter( sw );
                e.printStackTrace( pw );
                pw.flush();
                buffer.append( sw.toString() );
            }
            
            return buffer.toString();
        }
        
        /** Print a string */
        private void    println( String text ) { System.out.println( text ); }

        /* Print an irregularity */
        private void    printIrregularity( String text ) {}
    }

    ////////////////////////////////////////////////////////////////////////
    //
    // PAGE HEADER
    //
    ////////////////////////////////////////////////////////////////////////

    public  static  final   class   PageHeader
    {
        private boolean _isOverFlowPage;
        private byte        _pageStatus;
        private long        _pageVersion;
        private int         _slotsInUse;
        private int         _nextRecordID;
        private int         _pageGeneration;
        private int         _previousGeneration;
        private long        _beforeImagePageLocation;
        private int       _deletedRowCount;

        private  PageHeader( DataInputStream dais )  throws IOException
        {
            _isOverFlowPage = dais.readBoolean();
            _pageStatus =dais.readByte();
            _pageVersion = dais.readLong();
            _slotsInUse = dais.readUnsignedShort();
            _nextRecordID = dais.readInt();
            _pageGeneration = dais.readInt();
            _previousGeneration = dais.readInt();
            _beforeImagePageLocation = dais.readLong();
            _deletedRowCount = dais.readUnsignedShort();

            skipBytes( dais, 2 + 4 + 8 + 8 );
        }
        
        public boolean isOverFlowPage() { return _isOverFlowPage; }
        public byte        getPageStatus() { return _pageStatus; }
        public long        getPageVersion() { return _pageVersion; }
        public int         getSlotsInUse() { return _slotsInUse; }
        public int         getNextRecordID() { return _nextRecordID; }
        public int         getPageGeneration() { return _pageGeneration; }
        public int         getPreviousGeneration() { return _previousGeneration; }
        public long        getBeforeImagePageLocation() { return _beforeImagePageLocation; }
        public int       getDeletedRowCount() { return _deletedRowCount; }
        
        // format is documented in StoredPage.readPageHeader()
        public  static PageHeader    readPageHeader( byte[] pageData ) throws IOException
        {
            DataInputStream dais = new DataInputStream( new ByteArrayInputStream( pageData ) );

            // skip the formatableid
            int pageFormatableID = readPageFormatableID( dais );
            
            return readPageHeader( dais );
        }
        public  static PageHeader    readPageHeader( DataInputStream dais ) throws IOException
        {
            PageHeader  ph = new PageHeader( dais );
            
            return ph;
        }

        public  static int readPageFormatableID( DataInputStream dais ) throws IOException
        {
            int     formatableID = FormatIdUtil.readFormatIdInteger( dais );
            
            // Even though the formatableID only takes up the leading two bytes of
            // the AllocPage header, 4 bytes are allocated to it. Flush the next 2 bytes.
            skipBytes( dais, 2 );

            return formatableID;
        }

    }
    
    ////////////////////////////////////////////////////////////////////////
    //
    //  SLOT READER
    //
    ////////////////////////////////////////////////////////////////////////

    public  static  class   SlotReader
    {
        private int             _pageSize;
        private int             _slotTableOffsetToFirstEntry;
        private int             _slotTableOffsetToFirstRecordLengthField;
        private int             _slotTableOffsetToFirstReservedSpaceField;
        private int             _slotFieldSize;
        private int             _slotEntrySize;

        public  SlotReader( int pageSize )
        {
            _pageSize = pageSize;
            _slotFieldSize = calculateSlotFieldSize( _pageSize );
            _slotEntrySize = 3 * _slotFieldSize;
            _slotTableOffsetToFirstEntry = _pageSize - CHECKSUM_SIZE - _slotEntrySize;
            _slotTableOffsetToFirstRecordLengthField = _slotTableOffsetToFirstEntry + _slotFieldSize;
            _slotTableOffsetToFirstReservedSpaceField = _slotTableOffsetToFirstEntry + (2 * _slotFieldSize);
        }

        public  int slotFieldSize() { return _slotFieldSize; }
        public  int pageSize() { return _pageSize; }
        
        // copied from StoredPage
        public int getRecordOffset(int slot, byte[] pageData) 
        {
            byte[] data   = pageData;
            int    offset = _slotTableOffsetToFirstEntry - (slot * _slotEntrySize);

            // offset on the page of the record is stored in the first 2 or 4 bytes
            // of the slot table entry.  Code has been inlined for performance
            // critical low level routine.
            //
            // return( 
            //  (slotFieldSize == SMALL_SLOT_SIZE) ?
            //       readUnsignedShort() : readInt());

            return(
                   (_slotFieldSize == SMALL_SLOT_SIZE)  ?

                   ((data[offset++]  & 0xff) <<  8) | 
                   (data[offset]    & 0xff)          :

                   (((data[offset++] & 0xff) << 24) |
                    ((data[offset++] & 0xff) << 16) |
                    ((data[offset++] & 0xff) <<  8) |
                    ((data[offset]   & 0xff)      )));
        }

        /** Get a record header */
        private StoredRecordHeader  getRecordHeader( int slot, byte[] pageData )
        {
            return new StoredRecordHeader( pageData, getRecordOffset( slot, pageData ) );
        }
        
        // copied from StoredPage
        private int calculateSlotFieldSize( int pageSize )
        {
            if ( pageSize < 65536 )
            {
                // slots are 2 bytes (unsigned short data type) for pages <64KB
                return SMALL_SLOT_SIZE;
            } else
            {
                // slots are 4 bytes (int data type) for pages >=64KB
                return LARGE_SLOT_SIZE;
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //  STREAM FOR READING OVERFLOW COLUMNS
    //
    ////////////////////////////////////////////////////////////////////////

    public  static  class   OverflowStream  extends InputStream
    {
        RandomAccessFile        _raf;
        private CipherProvider  _decryptionEngine;
        private SlotReader      _slotReader;
        private byte[]              _currentPageData;
        private long                _overflowPage;
        private int                 _overflowID;
        private ArrayInputStream    _pageStream;
        private MemByteHolder   _bytes;

        /** Create once */
        public  OverflowStream
            (
             RandomAccessFile   raf,
             CipherProvider decryptionEngine,
             SlotReader     slotReader
             )
        {
            _raf = raf;
            _decryptionEngine = decryptionEngine;
            _slotReader = slotReader;
            _pageStream = new ArrayInputStream();
            _bytes = new MemByteHolder( _slotReader.pageSize() );
            _currentPageData = new byte[ _slotReader.pageSize() ];
        }

        /** Reinitialize for every long column */
        public  OverflowStream    init
            (
             long   overflowPage,
             int        overflowID
             )
            throws IOException
        {
            _overflowPage = overflowPage;
            _overflowID = overflowID;
            _bytes.clear();
            
            readExtents();

            return this;
        }

        public  int read()  throws IOException
        {
            int retval = _bytes.read();
            if ( retval < 0 )
            {
                _bytes.clear();
            }

            return retval;
        }

        // see StoredPage.restorePortionLongColumn()
        private void    readExtents()   throws IOException
        {
            while( _overflowPage >= 0 ) { readNextExtent(); }

            // switch to reading mode
            _bytes.startReading();
        }
        private void    readNextExtent()    throws IOException
        {
            // read and decrypt the next page in the overflow chain
            _raf.seek( _overflowPage * _slotReader.pageSize() );
            _raf.readFully( _currentPageData );
            if ( _decryptionEngine != null )    { _currentPageData = decryptPage( _decryptionEngine, _currentPageData ); }
            _pageStream.setData( _currentPageData );

            PageHeader  ph = PageHeader.readPageHeader( _currentPageData );
            int     recordCount = ph.getSlotsInUse();

            int                 slot = findRecordById( _overflowID, Page.FIRST_SLOT_NUMBER, recordCount );
            StoredRecordHeader  recordHeader = _slotReader.getRecordHeader( slot, _currentPageData );
            int                 offset = _slotReader.getRecordOffset( slot, _currentPageData );
            int                 numberFields = recordHeader.getNumberFields();

            _pageStream.setPosition( offset + recordHeader.size() );

            int                 fieldStatus = StoredFieldHeader.readStatus( _pageStream );
            int                 fieldDataLength = StoredFieldHeader.readFieldDataLength
                ( _pageStream, fieldStatus, _slotReader.slotFieldSize () );

            _bytes.write( _pageStream, fieldDataLength );
            
            // set the next overflow pointer in the stream...
            if ( numberFields == 1 ) 
            {
                // this is the last bit of the long column
                _overflowPage = -1;
                _overflowID = -1;
            } 
            else 
            {
                int firstFieldStatus = fieldStatus; // for DEBUG check

                // get the field status and data length of the overflow pointer.
                fieldStatus = StoredFieldHeader.readStatus( _pageStream );
                fieldDataLength = StoredFieldHeader.readFieldDataLength( _pageStream, fieldStatus, _slotReader.slotFieldSize() );

                if ( !StoredFieldHeader.isOverflow( fieldStatus ) )
                {
                    throw new IOException( "Corrupt overflow chain on page " + _overflowPage );
                }

                _overflowPage = CompressedNumber.readLong( (InputStream) _pageStream );
                _overflowID = CompressedNumber.readInt( (InputStream) _pageStream );
            }
        }

        // cribbed from BasePage
        private int findRecordById( int recordId, int slotHint, int maxSlot )
        {
            if ( slotHint == Page.FIRST_SLOT_NUMBER )
            {
                slotHint = recordId - RecordHandle.FIRST_RECORD_ID;
            }

            if (
                (slotHint > Page.FIRST_SLOT_NUMBER) &&
                (slotHint < maxSlot) && 
                ( recordId == _slotReader.getRecordHeader( slotHint, _currentPageData ).getId() )
               )
            {
                return(slotHint);
            }
            else
            {
                for ( int slot = Page.FIRST_SLOT_NUMBER; slot < maxSlot; slot++ )
                {
                    if ( recordId == _slotReader.getRecordHeader( slot, _currentPageData ).getId() )
                    {
                        return slot;
                    }
                }
            }

            return -1;
        }

    }

    ////////////////////////////////////////////////////////////////////////
    //
    // Other helper classes.
    //
    ////////////////////////////////////////////////////////////////////////

    //
    // Used to keep track of where we are in the input stream.
    //
    public  static  final   class   WrapperInputStream   extends InputStream
    {
        private InputStream _wrapped;
        private long            _bytesRead;

        public  WrapperInputStream( InputStream is )
        {
            _wrapped = is;
            _bytesRead = 0L;
        }

        public  int read()  throws IOException
        {
            int     retval = _wrapped.read();

            if ( retval >= 0 ) { _bytesRead++; }

            return retval;
        }

        public  long    getBytesRead() { return _bytesRead; }
    }
    
}
