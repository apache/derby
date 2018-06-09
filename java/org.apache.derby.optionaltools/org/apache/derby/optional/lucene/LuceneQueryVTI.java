/*

   Class org.apache.derby.optional.lucene.LuceneQueryVTI

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

package org.apache.derby.optional.lucene;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.io.StorageFile;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.optional.api.LuceneIndexDescriptor;
import org.apache.derby.optional.api.LuceneUtils;
import org.apache.derby.vti.StringColumnVTI;
import org.apache.derby.vti.VTIContext;
import org.apache.derby.vti.VTITemplate;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

import org.apache.derby.optional.utils.ToolUtilities;

/**
 * A VTI that provides the results of Lucene queries and
 * associated Lucene assigned document ids. 
 * 
 * This is intended for use through the provided query function
 * LuceneSupport.luceneQuery.
 * 
 */
class LuceneQueryVTI extends StringColumnVTI
{
    /////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////

    // constructor args
    private Connection  _connection;
    private String  _queryText;
    private int         _windowSize;
    private Float   _scoreCeiling;

    private String      _schema;
    private String      _table;
    private String      _column;
    
	private ScoreDoc[] _hits;
	private IndexReader _indexReader;
	private IndexSearcher _searcher;
	private int _hitIndex = -1;

    // ids (1-based positions) of the columns
    private int _minKeyID;
    private int _maxKeyID;
    private int _docIDColumnID;
    private int _scoreColumnID;

    // true if last column read was null
    private boolean _wasNull;
	
    /////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTOR
    //
    /////////////////////////////////////////////////////////////////////

	/**
	 * Return a LuceneQueryVTI based on the given Lucene query text.
	 */
	LuceneQueryVTI
        (
         String queryText,
         int    windowSize,
         Float scoreCeiling
         )
        throws SQLException
    {
        super( null );

        LuceneSupport.checkNotNull( "QUERY", queryText );
        
        _connection = LuceneSupport.getDefaultConnection();
        _queryText = queryText;
        _windowSize = windowSize;
        _scoreCeiling = scoreCeiling;
	}

    /////////////////////////////////////////////////////////////////////
    //
    //  StringColumnVTI BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////

	/**
	 * columns:
	 * 1 ... $_maxKeyID == key columns
	 * $_maxKeyID + 1 == lucene docId
	 * $_maxKeyID + 2 == lucene score
	 */
	public String getRawColumn( int columnid ) throws SQLException
    {
        _wasNull = false;
        
		try {
            ScoreDoc    scoreDoc = getScoreDoc();
            int     docID = scoreDoc.doc;
            
			if ( isKeyID( columnid ) ) { return _searcher.doc( docID ).get( getColumnName( columnid ) ); }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw ToolUtilities.wrap( e ); }
	}

    /** Handle boolean columns */
    public  boolean getBoolean( int columnid ) throws SQLException
    {
        String  stringValue = getRawColumn( columnid );

        if ( stringValue == null )
        {
            _wasNull = true;
            return false;
        }
        else
        {
            return Boolean.valueOf( stringValue );
        }
    }

    /** Handle float columns */
    public  float   getFloat( int columnid )    throws SQLException
    {
		try {
            if ( columnid == _scoreColumnID ) { return getScoreDoc().score; }
			else if ( isKeyID( columnid ) )
            {
                Number  number = getNumberValue( columnid );

                if ( number == null ) { return 0; }
                else { return number.floatValue(); }
            }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw ToolUtilities.wrap( e ); }
    }

    /** Handle double columns */
    public  double   getDouble( int columnid )    throws SQLException
    {
		try {
			if ( isKeyID( columnid ) )
            {
                Number  number = getNumberValue( columnid );

                if ( number == null ) { return 0; }
                else { return number.doubleValue(); }
            }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw ToolUtilities.wrap( e ); }
    }

    /** Handle bytecolumns */
    public  byte    getByte( int columnid )  throws SQLException
    {
        return (byte) getInt( columnid );
    }

    /** Handle short columns */
    public  short getShort( int columnid )  throws SQLException
    {
        return (short) getInt( columnid );
    }

    /** Handle long columns */
    public  long    getLong( int columnid )  throws SQLException
    {
		try {
            ScoreDoc    scoreDoc = getScoreDoc();
            int     docID = scoreDoc.doc;
			if ( isKeyID( columnid ) )
            {
                Number  number = getNumberValue( columnid );

                if ( number == null ) { return 0; }
                else { return number.longValue(); }
            }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw ToolUtilities.wrap( e ); }
    }
    
    /** Handle Date columns */
    public  Date    getDate( int columnid )  throws SQLException
    {
		try {
            ScoreDoc    scoreDoc = getScoreDoc();
            int     docID = scoreDoc.doc;
			if ( isKeyID( columnid ) )
            {
                Number  number = getNumberValue( columnid );

                if ( number == null ) { return null; }
                else { return new Date( number.longValue() ); }
            }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw ToolUtilities.wrap( e ); }
    }
    
    /** Handle Time columns */
    public  Time    getTime( int columnid )  throws SQLException
    {
		try {
            ScoreDoc    scoreDoc = getScoreDoc();
            int     docID = scoreDoc.doc;
			if ( isKeyID( columnid ) )
            {
                Number  number = getNumberValue( columnid );

                if ( number == null ) { return null; }
                else { return new Time( number.longValue() ); }
            }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw ToolUtilities.wrap( e ); }
    }
    
    /** Handle Timestamp columns */
    public  Timestamp    getTimestamp( int columnid )  throws SQLException
    {
		try {
            ScoreDoc    scoreDoc = getScoreDoc();
            int     docID = scoreDoc.doc;
			if ( isKeyID( columnid ) )
            {
                Number  number = getNumberValue( columnid );

                if ( number == null ) { return null; }
                else { return new Timestamp( number.longValue() ); }
            }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw ToolUtilities.wrap( e ); }
    }
    
    /** Handle integer columns */
    public  int getInt( int columnid )  throws SQLException
    {
        _wasNull = false;
        
		try {
            ScoreDoc    scoreDoc = getScoreDoc();
            int     docID = scoreDoc.doc;
			if ( columnid == _docIDColumnID ) { return docID; }
			else if ( isKeyID( columnid ) )
            {
                Number  number = getNumberValue( columnid );

                if ( number == null ) { return 0; }
                else { return number.intValue(); }
            }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw ToolUtilities.wrap( e ); }
    }
    private Number getNumberValue( int columnid ) throws IOException
    {
        IndexableField  field = _searcher.doc( getScoreDoc().doc ).getField( getColumnName( columnid ) );

        if ( field == null )
        {
            _wasNull = true;
            return null;
        }
        else
        {
            _wasNull = false;
            Number  number = field.numericValue();

            return number;
        }
    }

    /** Handle byte columns */
    public  byte[]  getBytes( int columnid ) throws SQLException
    {
		try {
            ScoreDoc    scoreDoc = getScoreDoc();
            int     docID = scoreDoc.doc;
            
			if ( isKeyID( columnid ) )
            {
                Document    doc = _searcher.doc( docID );
                String          columnName = getColumnName( columnid );

                if ( columnName != null )
                {
                    BytesRef        ref = doc.getBinaryValue( columnName );

                    if ( ref != null )  { return ref.bytes; }
                }

                _wasNull = true;
                return null;
            }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw ToolUtilities.wrap( e ); }
    }

    private SQLException    invalidColumnPosition( int columnid )
    {
        return ToolUtilities.newSQLException
            (
             SQLState.LANG_INVALID_COLUMN_POSITION,
             columnid,
             getColumnCount()
             );
    }

    private ScoreDoc    getScoreDoc()   throws IOException
    {
        return _hits[ _hitIndex ];
    }
	
	public boolean next()
        throws SQLException
    {
        _wasNull = false;
        if ( _schema == null ) { initScan(); }
        
		_hitIndex++;
		if (_hitIndex < _hits.length) {
			return true;
		}

        closeReader();
		return false;
	}
	
	public void close()
        throws SQLException
    {
		_hits = null;
		_hitIndex = 0;

        closeReader();
	}

    public  boolean wasNull() throws SQLException
    {
        return _wasNull;
    }
    
	/**
	 * Be sure to close the Lucene IndexReader
	 */
    //
    // This method in java.lang.Object was deprecated as of build 167
    // of JDK 9. See DERBY-6932.
    //
    @SuppressWarnings("deprecation")
	protected void finalize()
    {
		try {
			if ( _indexReader != null ) { _indexReader.close(); }
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void closeReader()
        throws SQLException
    {
		if ( _indexReader == null ) { return; }
        
		try {
			_indexReader.close();
		}
        catch (IOException e) { throw ToolUtilities.wrap( e ); }
        finally
        {
            _indexReader = null;
        }
	}
	
    /////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////

    /** Initialize the metadata and scan */
    private void    initScan()  throws SQLException
    {
        try {
            // read the execution context for this AwareVTI
            VTIContext  context = getContext();
            _schema = context.vtiSchema();
            String[]    nameParts = LuceneSupport.decodeFunctionName( context.vtiTable() );
            _table = nameParts[ LuceneSupport.TABLE_PART ];
            _column = nameParts[ LuceneSupport.COLUMN_PART ];

            // divine the column names
            VTITemplate.ColumnDescriptor[]  returnColumns = getReturnTableSignature( _connection );
            String[]    columnNames = new String[ returnColumns.length ];
            for ( int i = 0; i < returnColumns.length; i++ ) { columnNames[ i ] = returnColumns[ i ].columnName; }
            setColumnNames( columnNames );

            _scoreColumnID = getColumnCount();
            _docIDColumnID = _scoreColumnID - 1;
            _maxKeyID = _docIDColumnID - 1;
            _minKeyID = 1;
            
            // make sure the user has SELECT privilege on all relevant columns of the underlying table
            vetPrivileges();

            String      delimitedColumnName = LuceneSupport.delimitID( _column );
            DerbyLuceneDir  derbyLuceneDir = LuceneSupport.getDerbyLuceneDir( _connection, _schema, _table, delimitedColumnName );
            StorageFile propertiesFile = LuceneSupport.getIndexPropertiesFile( derbyLuceneDir );
            Properties  indexProperties = readIndexProperties( propertiesFile );
            String          indexDescriptorMaker = indexProperties.getProperty( LuceneSupport.INDEX_DESCRIPTOR_MAKER );
            LuceneIndexDescriptor   indexDescriptor = getIndexDescriptor( indexDescriptorMaker );
            Analyzer    analyzer = indexDescriptor.getAnalyzer( );
            QueryParser qp = indexDescriptor.getQueryParser();

            vetLuceneVersion( indexProperties.getProperty( LuceneSupport.LUCENE_VERSION ) );

            _indexReader = getIndexReader( derbyLuceneDir );
            _searcher = new IndexSearcher( _indexReader );

            Query luceneQuery = qp.parse( _queryText );
            TopScoreDocCollector tsdc = TopScoreDocCollector.create( _windowSize, true);
            if ( _scoreCeiling != null ) {
                tsdc = TopScoreDocCollector.create( _windowSize, new ScoreDoc( 0, _scoreCeiling ), true );
            }

            searchAndScore( luceneQuery, tsdc );
        }
        catch (IOException ioe) { throw ToolUtilities.wrap( ioe ); }
        catch (ParseException pe) { throw ToolUtilities.wrap( pe ); }
        catch (PrivilegedActionException pae) { throw ToolUtilities.wrap( pae ); }
    }

    /**
     * <p>
     * Make sure that the index wasn't created with a Lucene version from
     * the future.
     * </p>
     */
    private void    vetLuceneVersion( String indexVersionString )
        throws SQLException
    {
        Version     currentVersion = LuceneUtils.currentVersion();
        Version     indexVersion = null;

        try {
            indexVersion = Version.parseLeniently(indexVersionString);
        }
        catch (Exception e) {}

        if ( (indexVersion == null) || !currentVersion.onOrAfter( indexVersion ) )
        {
            throw ToolUtilities.newSQLException
                ( SQLState.LUCENE_BAD_VERSION, currentVersion.toString(), indexVersionString );
        }
    }

    /**
     * <p>
     * Make sure that the user has SELECT privilege on the text column and on all
     * the key columns of the underlying table.
     * </p>
     */
    private void    vetPrivileges() throws SQLException
    {
        StringBuilder   buffer = new StringBuilder();
        int _maxKeyID = getColumnCount() - 2;

        buffer.append( "select " );
        for ( int i = 0; i < _maxKeyID; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( LuceneSupport.delimitID( getColumnName( i + 1 ) ) );
        }
        buffer.append( ", " + LuceneSupport.delimitID( _column ) );
        buffer.append( " from " + LuceneSupport.makeTableName( _schema, _table ) );
        buffer.append( " where 1=2" );

        _connection.prepareStatement( buffer.toString() ).executeQuery().close();
    }

    /** Return true if the 1-based column ID is the ID of a key column */
    private boolean isKeyID( int columnid )
    {
        return ( (columnid > 0) && (columnid <= _maxKeyID) );
    }
    
	/**
	 * Returns a Lucene IndexReader, which reads from the indicated Lucene index.
	 * 
	 * @param indexHome The directory holding the Lucene index.
	 */
	private static IndexReader getIndexReader( final DerbyLuceneDir dir )
        throws IOException, PrivilegedActionException
    {
        try {
            return AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<IndexReader>()
             {
                 public IndexReader run() throws IOException
                 {
                     return DirectoryReader.open( dir );
                 }
             }
             );
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
	}
	
    /** Read the index properties file */
    private static  Properties readIndexProperties( final StorageFile file )
        throws IOException
    {
        try {
            return AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<Properties>()
             {
                public Properties run() throws IOException
                {
                    return LuceneSupport.readIndexPropertiesNoPrivs( file );
                }
             }
             );
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }

	/**
	 * Invoke a static method (possibly supplied by the user) to instantiate an index descriptor.
     * The method has no arguments.
	 */
	private static LuceneIndexDescriptor getIndexDescriptor( final String indexDescriptorMaker )
        throws PrivilegedActionException, SQLException
    {
        return AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<LuceneIndexDescriptor>()
             {
                 public LuceneIndexDescriptor run()
                     throws ClassNotFoundException, IllegalAccessException,
                     InvocationTargetException, NoSuchMethodException,
                     SQLException
                 {
                     return LuceneSupport.getIndexDescriptorNoPrivs( indexDescriptorMaker );
                 }
             }
             );
	}
	
    /** Read the index properties file */
    private void    searchAndScore( final Query luceneQuery, final TopScoreDocCollector tsdc )
        throws IOException
    {
        try {
            AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<Object>()
             {
                public Object run() throws IOException
                {
                    _searcher.search( luceneQuery, tsdc );
                    TopDocs topdocs = tsdc.topDocs();
                    _hits = topdocs.scoreDocs;

                    return null;
                }
             }
             );
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }

}
