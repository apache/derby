/*

   Class org.apache.derby.impl.optional.lucene.LuceneQueryVTI

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

package org.apache.derby.impl.optional.lucene;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.vti.RestrictedVTI;
import org.apache.derby.vti.Restriction;
import org.apache.derby.vti.Restriction.ColumnQualifier;
import org.apache.derby.vti.StringColumnVTI;
import org.apache.derby.vti.VTIContext;
import org.apache.derby.vti.VTITemplate;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

/**
 * A VTI that provides the results of Lucene queries and
 * associated Lucene assigned document ids. 
 * 
 * This is intended for use through the provided query function
 * LuceneSupport.luceneQuery.
 * 
 */
public class LuceneQueryVTI extends StringColumnVTI
{
    /////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////

    public  static  final   String  TEXT_FIELD_NAME = "luceneTextField";

    /////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////

    // constructor args
    private Connection  _connection;
    private String  _queryText;
    private double  _rankCutoff;

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
    private int _rankColumnID;
	
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
         double rankCutoff
         )
        throws SQLException
    {
        super( null );
        
        _connection = LuceneSupport.getDefaultConnection();
        _queryText = queryText;
        _rankCutoff = rankCutoff;
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
	 * $_maxKeyID + 2 == lucene rank
	 */
	public String getRawColumn( int columnid ) throws SQLException
    {
		try {
            ScoreDoc    scoreDoc = getScoreDoc();
            int     docID = scoreDoc.doc;
            
			if ( isKeyID( columnid ) ) { return _searcher.doc( docID ).get( getColumnName( columnid ) ); }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw LuceneSupport.wrap( e ); }
	}

    /** Handle float columns */
    public  float   getFloat( int columnid )    throws SQLException
    {
		try {
            if ( columnid == _rankColumnID ) { return getScoreDoc().score; }
			else if ( isKeyID( columnid ) )
            {
                Number  number = getNumberValue( columnid );

                if ( number == null ) { return 0; }
                else { return number.floatValue(); }
            }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw LuceneSupport.wrap( e ); }
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
        catch (IOException e)   { throw LuceneSupport.wrap( e ); }
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
        catch (IOException e)   { throw LuceneSupport.wrap( e ); }
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
        catch (IOException e)   { throw LuceneSupport.wrap( e ); }
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
        catch (IOException e)   { throw LuceneSupport.wrap( e ); }
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
        catch (IOException e)   { throw LuceneSupport.wrap( e ); }
    }
    
    /** Handle integer columns */
    public  int getInt( int columnid )  throws SQLException
    {
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
        catch (IOException e)   { throw LuceneSupport.wrap( e ); }
    }
    private Number getNumberValue( int columnid ) throws IOException
    {
        IndexableField  field = _searcher.doc( getScoreDoc().doc ).getField( getColumnName( columnid ) );

        if ( field == null ) { return null; }
        else
        {
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

                return null;
            }
			else { throw invalidColumnPosition( columnid ); }
		}
        catch (IOException e)   { throw LuceneSupport.wrap( e ); }
    }

    private SQLException    invalidColumnPosition( int columnid )
    {
        return LuceneSupport.newSQLException
            (
             SQLState.LANG_INVALID_COLUMN_POSITION,
             new Integer( columnid ),
             new Integer( getColumnCount() )
             );
    }

    private ScoreDoc    getScoreDoc()   throws IOException
    {
        return _hits[ _hitIndex ];
    }
	
	public boolean next()
        throws SQLException
    {
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
	
	/**
	 * Be sure to close the Lucene IndexReader
	 */
	public void finalize()
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
        catch (IOException e) { throw LuceneSupport.wrap( e ); }
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

            _rankColumnID = getColumnCount();
            _docIDColumnID = _rankColumnID - 1;
            _maxKeyID = _docIDColumnID - 1;
            _minKeyID = 1;
            
            // make sure the user has SELECT privilege on all relevant columns of the underlying table
            vetPrivileges();
        
            String indexhome = LuceneSupport.getIndexLocation( _connection, _schema, _table, _column);
				
            _indexReader = LuceneSupport.getIndexReader( new File( indexhome.toString() ) );
            _searcher = new IndexSearcher(_indexReader);
            Analyzer analyzer = new StandardAnalyzer( Version.LUCENE_45 );
            QueryParser qp = new QueryParser( Version.LUCENE_45, TEXT_FIELD_NAME, analyzer );
            Query luceneQuery = qp.parse( _queryText );
            TopScoreDocCollector tsdc = TopScoreDocCollector.create(1000, true);
            if ( _rankCutoff != 0 ) {
                tsdc = TopScoreDocCollector.create(1000, new ScoreDoc(0, (float) _rankCutoff ), true);
            }
            _searcher.search(luceneQuery, tsdc);
            TopDocs topdocs = tsdc.topDocs();
            _hits = topdocs.scoreDocs;
        }
        catch (IOException ioe) { throw LuceneSupport.wrap( ioe ); }
        catch (ParseException pe) { throw LuceneSupport.wrap( pe ); }
        catch (PrivilegedActionException pae) { throw LuceneSupport.wrap( pae ); }
    }

    /**
     * <p>
     * Make sure that the user has SELECT privilege on the text column and on all
     * the key columns of the underlying table.
     */
    private void    vetPrivileges() throws SQLException
    {
        StringBuilder   buffer = new StringBuilder();
        int _maxKeyID = getColumnCount() - 2;

        buffer.append( "select " );
        for ( int i = 0; i < _maxKeyID; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( getColumnName( i + 1 ) );
        }
        buffer.append( ", " + _column );
        buffer.append( " from " + LuceneSupport.makeTableName( _schema, _table ) );
        buffer.append( " where 1=2" );

        _connection.prepareStatement( buffer.toString() ).executeQuery().close();
    }

    /** Return true if the 1-based column ID is the ID of a key column */
    private boolean isKeyID( int columnid )
    {
        return ( (columnid > 0) && (columnid <= _maxKeyID) );
    }
    
}
