/*

   Class org.apache.derby.optional.lucene.LuceneSupport

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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

import org.apache.derby.database.Database;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.OptionalTool;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.optional.api.LuceneIndexDescriptor;
import org.apache.derby.optional.api.LuceneUtils;
import org.apache.derby.optional.utils.ToolUtilities;
import org.apache.derby.vti.VTITemplate;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

/**
 * Support for creating, updating, and querying Lucene
 * indexes in Derby, and associated utility functions.
 * 
 */
public class LuceneSupport implements OptionalTool
{
    private static  final   String  LUCENE_SCHEMA = "LuceneSupport";
    private static  final   String  LIST_INDEXES = LUCENE_SCHEMA + "." + "listIndexes";
    private static  final   String  CREATE_INDEX = LUCENE_SCHEMA + "." + "createIndex";
    private static  final   String  DROP_INDEX = LUCENE_SCHEMA + "." + "dropIndex";
    private static  final   String  UPDATE_INDEX = LUCENE_SCHEMA + "." + "updateIndex";
    private static  final   String  SEPARATOR = "__";

    // names of columns in all query table functions
    private static  final   String  SCORE = "SCORE";
    private static  final   String  DOCUMENT_ID = "DOCUMENTID";

    // for decomposing a function name into the table and column parts
    static  final   int TABLE_PART = 0;
    static  final   int COLUMN_PART = TABLE_PART + 1;
    static  final   int PART_COUNT = COLUMN_PART + 1;

    // file which holds properties specific to a Lucene index
    private static  final   String  PROPERTIES_FILE_NAME = "derby-lucene.properties";

    // properties which go in that file

    /** property identifying the static method which materializes an Analyzer for the index */
    public  static  final   String  INDEX_DESCRIPTOR_MAKER = "derby.lucene.index.descriptor.maker";

    /** class name of the Analyzer used for the index */
    public  static  final   String  ANALYZER = "derby.lucene.analyzer";

    /** version of Lucene used to create or recreate an index */
    public  static  final   String  LUCENE_VERSION = "derby.lucene.version";
	
    /** system time when the index was created/updated */
    public  static  final   String  UPDATE_TIMESTAMP = "derby.lucene.last.updated";
	
    /////////////////////////////////////////////////////////////////////
    //
    //  OptionalTool BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////

	/**
	 * 0-arg constructor as an OptionalTool
	 */
	public LuceneSupport() {
	}
	
	/**
	 * Load the procedures and functions for Lucene support:
	 * In the LuceneSupport schema, these are:
	 * listIndexes, createIndex, dropIndex,
	 * updateIndex.
	 */
	public void loadTool(String... configurationParameters) throws SQLException
    {
        forbidReadOnlyConnections();

        // not allowed during soft-upgrade
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        try {
            ConnectionUtil.getCurrentLCC().getDataDictionary().checkVersion
                ( DataDictionary.DD_VERSION_DERBY_10_11, "luceneSupport" );
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        catch (StandardException se)    { throw ToolUtilities.sqlException( se ); }
        
        Connection  conn = getDefaultConnection();
        ToolUtilities.mustBeDBO( conn );

        //
        // Lucene indexes are not allowed in encrypted databases. They leak
        // encrypted data in plaintext.
        //
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        if ( getDataFactory( conn ).databaseEncrypted() )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            throw ToolUtilities.newSQLException( SQLState.LUCENE_ENCRYPTED_DB );
        }

        if ( luceneSchemaExists( conn ) )
        {
            throw ToolUtilities.newSQLException( SQLState.LUCENE_ALREADY_LOADED );
        }

        boolean sqlAuthorizationEnabled = ToolUtilities.sqlAuthorizationEnabled( conn );
        
		StringBuilder listFunction = new StringBuilder();
		listFunction.append("create function " + LIST_INDEXES );
		listFunction.append(" () ");
		listFunction.append("returns table");
		listFunction.append("(");
		listFunction.append("schemaname varchar( 128 ),");
		listFunction.append("tablename varchar( 128 ),");
		listFunction.append("columnname varchar( 128 ),");
		listFunction.append("lastupdated timestamp,");
		listFunction.append("luceneversion varchar( 20 ),");
		listFunction.append("analyzer varchar( 32672 ),");
//IC see: https://issues.apache.org/jira/browse/DERBY-590
		listFunction.append("indexdescriptormaker varchar( 32672 )");
		listFunction.append(")");
		listFunction.append("language java ");
		listFunction.append("parameter style DERBY_JDBC_RESULT_SET ");
		listFunction.append("contains sql ");
		listFunction.append("external name '" + getClass().getName() + ".listIndexes'");
		
		executeDDL( conn, listFunction.toString() );
		
		StringBuilder createProcedure = new StringBuilder();
		createProcedure.append("create procedure " + CREATE_INDEX );
		createProcedure.append(" (schemaname varchar( 128 ),");
		createProcedure.append("tablename varchar( 128 ),");
		createProcedure.append("textcolumn varchar( 128 ),");
//IC see: https://issues.apache.org/jira/browse/DERBY-590
		createProcedure.append("indexdescriptormaker varchar( 32672 ),");
		createProcedure.append("keyColumns varchar( 32672 )...)");
		createProcedure.append("parameter style derby modifies sql data language java external name ");
		createProcedure.append("'" + getClass().getName() + ".createIndex'");
		
		executeDDL( conn, createProcedure.toString() );

		StringBuilder dropProcedure = new StringBuilder();
		dropProcedure.append("create procedure " + DROP_INDEX );
		dropProcedure.append(" (schemaname varchar( 128 ),");
		dropProcedure.append("tablename varchar( 128 ),");
		dropProcedure.append("textcolumn varchar( 128 ))");
		dropProcedure.append("parameter style java modifies sql data language java external name ");
		dropProcedure.append("'" + getClass().getName() + ".dropIndex'");
		
		executeDDL( conn, dropProcedure.toString() );

		StringBuilder updateProcedure = new StringBuilder();
		updateProcedure.append("create procedure " + UPDATE_INDEX );
		updateProcedure.append(" (schemaname varchar( 128 ),");
		updateProcedure.append("tablename varchar( 128 ),");
		updateProcedure.append("textcolumn varchar( 128 ),");
//IC see: https://issues.apache.org/jira/browse/DERBY-590
		updateProcedure.append("indexdescriptormaker varchar( 32672 ))");
		updateProcedure.append("parameter style java reads sql data language java external name ");
		updateProcedure.append("'" + getClass().getName() + ".updateIndex'");
		
		executeDDL( conn, updateProcedure.toString() );

        if ( sqlAuthorizationEnabled ) { grantPermissions(); }

//IC see: https://issues.apache.org/jira/browse/DERBY-590
        createLuceneDir( conn );
	}

    /**
     * Grant permissions to use the newly loaded LuceneSupport routines.
     */
    private void    grantPermissions()  throws SQLException
    {
        Connection  conn = getDefaultConnection();

        executeDDL( conn, "grant execute on function " + LIST_INDEXES + " to public" );
        executeDDL( conn, "grant execute on procedure " + CREATE_INDEX + " to public" );
        executeDDL( conn, "grant execute on procedure " + DROP_INDEX + " to public" );
        executeDDL( conn, "grant execute on procedure " + UPDATE_INDEX + " to public" );
    }

	/**
	 * Removes the functions and procedures loaded by loadTool and created by createIndex.
     * Drop the LuceneSupport schema. Drop the lucene subdirectory.
	 */
	public void unloadTool(String... configurationParameters)
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        forbidReadOnlyConnections();
        
        Connection  conn = getDefaultConnection();
        ToolUtilities.mustBeDBO( conn );
//IC see: https://issues.apache.org/jira/browse/DERBY-6825

        if ( !luceneSchemaExists( conn ) )
        {
            throw ToolUtilities.newSQLException( SQLState.LUCENE_ALREADY_UNLOADED );
        }

        //
        // Drop all of the functions and procedures bound to methods in this package.
        //
        String      className = getClass().getName();
        int             endPackageIdx = className.lastIndexOf( "." );
        String      packageName = className.substring( 0, endPackageIdx );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        PreparedStatement   ps = conn.prepareStatement
            (
             "select s.schemaName, a.alias, a.aliastype\n" +
             "from sys.sysschemas s, sys.sysaliases a\n" +
             "where s.schemaID = a.schemaID\n" +
             "and substr( cast( a.javaclassname as varchar( 32672 ) ), 1, ? ) = ?\n"
             );
        ps.setInt( 1, packageName.length() );
        ps.setString( 2, packageName );
        ResultSet   routines = ps.executeQuery();

        try {
            while ( routines.next() )
            {
                String  schema = routines.getString( 1 );
                String  routineName = routines.getString( 2 );
                String  routineType = ("P".equals( routines.getString( 3 ) )) ? "procedure" : "function";

                conn.prepareStatement( "drop " + routineType + " " + makeTableName( schema, routineName ) ).execute();
            }
        }
        finally { routines.close(); }

        //
        // Drop the LuceneSupport schema.
        //
        conn.prepareStatement( "drop schema " + LUCENE_SCHEMA + " restrict" ).execute();

        //
        // Now delete the Lucene subdirectory;
        //
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        StorageFactory storageFactory = getStorageFactory(conn);
        StorageFile luceneDir =
                storageFactory.newStorageFile(Database.LUCENE_DIR);
        if (exists(luceneDir)) {
            deleteFile(luceneDir);
        }
	}
	
    /////////////////////////////////////////////////////////////////////
    //
    //  LUCENE QUERY
    //
    /////////////////////////////////////////////////////////////////////

	/**
	 * Query a Lucene index created by createIndex
	 * 
	 * @param queryText a Lucene query, see the Lucene classic queryparser syntax 
	 * @param scoreCeiling Return results only below this score
	 * @return A result set in the form of LuceneQueryVTI table
	 * @throws ParseException
	 * @throws IOException
	 * @see org.apache.derby.optional.lucene.LuceneQueryVTI
	 */
	public static LuceneQueryVTI luceneQuery
        (
         String queryText,
         int    windowSize,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
         Float scoreCeiling
         )
        throws ParseException, IOException, SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
//IC see: https://issues.apache.org/jira/browse/DERBY-590
		LuceneQueryVTI lqvti = new LuceneQueryVTI( queryText, windowSize, scoreCeiling );
		return lqvti;
	}
	
    /////////////////////////////////////////////////////////////////////
    //
    //  LIST INDEXES
    //
    /////////////////////////////////////////////////////////////////////

	/**
	 * Return a list of Lucene indexes for this database. Filter by schema and table, if given.
	 */
	public static LuceneListIndexesVTI listIndexes()
        throws IOException, PrivilegedActionException, SQLException
    {
		LuceneListIndexesVTI llivti = new LuceneListIndexesVTI();
		return llivti;
	}
	
    /////////////////////////////////////////////////////////////////////
    //
    //  UPDATE INDEX
    //
    /////////////////////////////////////////////////////////////////////

	/**
	 * Update a document in a Lucene index. Drops and recreates the Lucene index
     * but does not touch the query function specific to the index.
	 * 
	 * @param schema Schema where the indexed column resides
	 * @param table table where the indexed column resides
	 * @param textcol the indexed column
	 * @param indexDescriptorMaker name of static method which instantiates the index configuration. may be null.
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void updateIndex( String schema, String table, String textcol, String indexDescriptorMaker )
        throws SQLException, IOException, PrivilegedActionException
    {
        forbidReadOnlyConnections();
//IC see: https://issues.apache.org/jira/browse/DERBY-590

        Connection              conn = getDefaultConnection();

        vetIdentifiers( schema, table, textcol );
//IC see: https://issues.apache.org/jira/browse/DERBY-6596

        // only the dbo or the schema owner can perform this function
        ToolUtilities.mustBeOwner( conn, schema );
//IC see: https://issues.apache.org/jira/browse/DERBY-6825

        if ( !tableFunctionExists( conn, schema, table, textcol ) )
        {
            throw ToolUtilities.newSQLException( SQLState.LUCENE_INDEX_DOES_NOT_EXIST );
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-590
        createOrRecreateIndex( conn, schema, table, textcol, indexDescriptorMaker, false );
	}
	
    /////////////////////////////////////////////////////////////////////
    //
    //  CREATE INDEX
    //
    /////////////////////////////////////////////////////////////////////

	/**
	 * Create a Lucene index on the specified column.
	 *  
	 * @param schema The schema of the column to index
	 * @param table The table or view containing the indexable column
	 * @param textcol The column to create the Lucene index on
	 * @param indexDescriptorMaker name of static method which instantiates the index configuration. may be null.
	 * @param keyColumns names of key columns if we're indexing a column in a view
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void createIndex
        (
         String schema,
         String table,
         String textcol,
         String indexDescriptorMaker,
         String... keyColumns
         )
        throws SQLException, IOException, PrivilegedActionException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        forbidReadOnlyConnections();
        
        Connection              conn = getDefaultConnection();
        DatabaseMetaData    dbmd = conn.getMetaData();

        vetIdentifiers( schema, table, textcol );
//IC see: https://issues.apache.org/jira/browse/DERBY-6596

        // First make sure that the text column exists and is a String type
        vetTextColumn( dbmd, schema, table, textcol );

//IC see: https://issues.apache.org/jira/browse/DERBY-590
        createOrRecreateIndex( conn, schema, table, textcol, indexDescriptorMaker, true, keyColumns );
	}

	/**
	 * Create or re-create a Lucene index on the specified column.
	 *  
	 * @param schema The schema of the column to index
	 * @param table The table of the column to index
	 * @param textcol The column to create the Lucene index on
	 * @param indexDescriptorMaker name of static method which instantiates the index configuration. may be null.
	 * @param create True if the index is to be created, false if it is to be recreated
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void createOrRecreateIndex
        (
         Connection conn,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
         String schema,
         String table,
         String textcol,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
//IC see: https://issues.apache.org/jira/browse/DERBY-590
         String indexDescriptorMaker,
         boolean create,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
         String... keyColumns
         )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
//IC see: https://issues.apache.org/jira/browse/DERBY-590
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException, IOException, PrivilegedActionException
    {
        VTITemplate.ColumnDescriptor[] primaryKeys = new VTITemplate.ColumnDescriptor[ 0 ];

        // can't override keys when the index is updated
        if ( !create )
        { primaryKeys = getKeys( conn, schema, table, textcol ); }
        // use the supplied keys if possible
        else if ( (keyColumns != null) && (keyColumns.length > 0) )
        { primaryKeys = getKeys( conn, schema, table, keyColumns ); }
        else
        { primaryKeys = getPrimaryKeys( conn, schema, table  ); }

        // can't create an index without specifying keys for joining it back to Derby data
        if ( primaryKeys.length == 0 )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            throw ToolUtilities.newSQLException( SQLState.LUCENE_NO_PRIMARY_KEY );
        }

        // don't let the user create a table function with duplicate column names
        vetColumnName( ToolUtilities.derbyIdentifier( textcol ) );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        for ( VTITemplate.ColumnDescriptor key : primaryKeys )
        {
            vetColumnName(  key.columnName );
        }
        
        int             keyCount = 0;
        StorageFile propertiesFile = getIndexPropertiesFile( conn, schema, table, textcol );
//IC see: https://issues.apache.org/jira/browse/DERBY-590

        //
        // Drop the old index directory if we're recreating the index.
        // We do this after verifying that the key exists.
        //
        if ( !create )
        {
            dropIndexDirectories( schema, table, textcol );
        }

        Version luceneVersion = LuceneUtils.currentVersion();

        // create the new directory
        DerbyLuceneDir  derbyLuceneDir = getDerbyLuceneDir( conn, schema, table, textcol );
//IC see: https://issues.apache.org/jira/browse/DERBY-590

        // get the Analyzer and the field names. use the default if the user didn't specify an override
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        if ( indexDescriptorMaker == null ) { indexDescriptorMaker = LuceneUtils.class.getName() + ".defaultIndexDescriptor"; }
        LuceneIndexDescriptor   indexDescriptor = getIndexDescriptor( indexDescriptorMaker );
        String[]  fieldNames = indexDescriptor.getFieldNames();
        Analyzer    analyzer = indexDescriptor.getAnalyzer();

        // make sure the field names don't overlap with the key names
        sortAndVetFieldNames( fieldNames, primaryKeys );
//IC see: https://issues.apache.org/jira/browse/DERBY-590

        Properties  indexProperties = new Properties();
        indexProperties.setProperty( LUCENE_VERSION, luceneVersion.toString() );
        indexProperties.setProperty( UPDATE_TIMESTAMP, Long.toString( System.currentTimeMillis() ) );
        indexProperties.setProperty( INDEX_DESCRIPTOR_MAKER, indexDescriptorMaker );
        indexProperties.setProperty( ANALYZER, analyzer.getClass().getName() );
            
        StringBuilder   tableFunction = new StringBuilder();
        tableFunction.append( "create function " + makeTableFunctionName( schema, table, textcol ) + "\n" );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        tableFunction.append( "( query varchar( 32672 ), windowSize int, scoreCeiling real )\n" );
        tableFunction.append( "returns table\n(" );

//IC see: https://issues.apache.org/jira/browse/DERBY-6597
        writeIndexProperties( propertiesFile, indexProperties );
        
        PreparedStatement   ps = null;
        ResultSet rs = null;
        IndexWriter iw = null;
        try {
            iw = getIndexWriter( luceneVersion, analyzer, derbyLuceneDir );
//IC see: https://issues.apache.org/jira/browse/DERBY-590

            // select all keys and the textcol from this column, add to lucene index
            StringBuilder query = new StringBuilder("select ");
        
            for ( VTITemplate.ColumnDescriptor keyDesc : primaryKeys )
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6730
                String  keyName = delimitID( keyDesc.columnName );
                if ( keyCount > 0 ) { query.append( ", " ); }
                query.append( keyName );

                String  keyType = mapType( keyDesc );

                if ( keyCount > 0 ) { tableFunction.append( "," ); }
                tableFunction.append( "\n\t" + keyName + " " + keyType );
                keyCount++;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            tableFunction.append(",\n\t" + DOCUMENT_ID + " int");
            tableFunction.append(",\n\t" + SCORE + " real");
            tableFunction.append( "\n)\nlanguage java parameter style derby_jdbc_result_set contains sql\n" );
            tableFunction.append( "external name '" + LuceneSupport.class.getName() + ".luceneQuery'" );
//IC see: https://issues.apache.org/jira/browse/DERBY-590

            // now create the table function for this text column
            if ( create )
            {
                conn.prepareStatement( tableFunction.toString() ).execute();
            }
        
            query.append(", ");
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            query.append( delimitID( ToolUtilities.derbyIdentifier( textcol ) ) );
            query.append(" from " + makeTableName( schema, table ) );

            ps = conn.prepareStatement( query.toString() );
            rs = ps.executeQuery();

            while ( rs.next() )
            {
                Document doc = new Document();

                for ( int i = 0; i < keyCount; i++ )
                {
                    VTITemplate.ColumnDescriptor   keyDescriptor = primaryKeys[ i ];
                    addValue( doc, keyDescriptor, rs, i + 1 );
                }

                String  textcolValue = rs.getString( keyCount + 1 );
                if ( textcolValue != null )
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                    for ( String fieldName : fieldNames )
                    {
                        doc.add( new TextField( fieldName, textcolValue, Store.NO ) );
                    }
                }
                addDocument( iw, doc );
            }
        }
        finally
        {
            try {
                 if ( iw != null ) { close( iw ); }
            }
            finally {
                try {
                    if ( rs != null ) { rs.close(); }
                }
                finally {
                    if ( ps != null ) { ps.close(); }
                }
            }
        }
	}

    /** Verify that the schema, table, and column names aren't null */
	private static void vetIdentifiers
        (
//IC see: https://issues.apache.org/jira/browse/DERBY-6596
         String schema,
         String table,
         String textcol
         )
        throws SQLException
    {
        checkNotNull( "SCHEMANAME", schema );
        checkNotNull( "TABLENAME", table );
        checkNotNull( "TEXTCOLUMN", textcol );
    }

    /**
     * Raise an exception if a field has the same name as a key or if two
     * fields have the same name.
     */
    private static  void    sortAndVetFieldNames( String[] fieldNames, VTITemplate.ColumnDescriptor[] keys )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        for ( String fieldName : fieldNames )
        {
            if ( fieldName == null )
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
                throw ToolUtilities.newSQLException( SQLState.LUCENE_DUPLICATE_FIELD_NAME, fieldName );
            }
        }
        Arrays.sort( fieldNames );
        
        HashSet<String>   keyNames = new HashSet<String>();
        for ( VTITemplate.ColumnDescriptor cd : keys ) { keyNames.add( cd.columnName ); }

        String  previousFieldName = null;
        for ( String fieldName : fieldNames )
        {
            if ( fieldName.equals( previousFieldName ) )
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
                throw ToolUtilities.newSQLException( SQLState.LUCENE_DUPLICATE_FIELD_NAME, fieldName );
            }
            previousFieldName = fieldName;

            if ( keyNames.contains( fieldName ) )
            {
                throw ToolUtilities.newSQLException( SQLState.LUCENE_FIELD_KEY_CONFLICT, fieldName );
            }
        }
    }
    
    /////////////////////////////////////////////////////////////////////
    //
    //  DROP INDEX
    //
    /////////////////////////////////////////////////////////////////////

	/**
	 * Drop a Lucene index. This removes the Lucene index directory from the filesystem.
	 * 
	 * @param schema The schema of the column that is indexed
	 * @param table The table of the column that is indexed
	 * @param textcol The column that is indexed
	 * 
	 * @throws SQLException
	 */
	public static void dropIndex( String schema, String table, String textcol )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        forbidReadOnlyConnections();
        
        vetIdentifiers( schema, table, textcol );
//IC see: https://issues.apache.org/jira/browse/DERBY-6596

        getDefaultConnection().prepareStatement
            (
             "drop function " + makeTableFunctionName( schema, table, textcol )
             ).execute();

        dropIndexDirectories( schema, table, textcol );
	}

    /**
     * <p>
     * Drop the Lucene directories which support an index.
     * </p>
     */
	private static void dropIndexDirectories( String schema, String table, String textcol )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        DerbyLuceneDir  derbyLuceneDir = getDerbyLuceneDir( getDefaultConnection(), schema, table, textcol );
//IC see: https://issues.apache.org/jira/browse/DERBY-590

        StorageFile indexDir = derbyLuceneDir.getDirectory();
		StorageFile tableDir = indexDir.getParentDir();
        StorageFile schemaDir = tableDir.getParentDir();
		
        deleteFile( indexDir );
        if ( isEmpty( tableDir ) )
        {
            deleteFile( tableDir );
            if ( isEmpty( schemaDir ) ) { deleteFile( schemaDir ); }
        }
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  TYPE HANDLING
    //
    /////////////////////////////////////////////////////////////////////

    /** Get the SQL type name for a key column */
    private static  String  mapType( VTITemplate.ColumnDescriptor keyDesc )
        throws SQLException
    {
        return mapType
            (
             keyDesc.jdbcType,
             keyDesc.precision,
             keyDesc.scale,
             keyDesc.typeName
             );
    }

    /**
     * <p>
     * Get the type of an external database's column as a Derby type name.
     * </p>
     *
     */
    private static String    mapType( int jdbcType, int precision, int scale, String typeName )
        throws SQLException
    {
        switch( jdbcType )
        {
        case    Types.BIGINT:           return "bigint";
        case    Types.BINARY:           return "char " + precisionToLength( precision ) + "  for bit data";
        case    Types.BIT:              return "boolean";
        case    Types.BLOB:             return "blob";
        case    Types.BOOLEAN:          return "boolean";
        case    Types.CHAR:             return "char" + precisionToLength( precision );
        case    Types.CLOB:             return "clob";
        case    Types.DATE:             return "date";
        case    Types.DECIMAL:          return "decimal" + precisionAndScale( precision, scale );
        case    Types.DOUBLE:           return "double";
        case    Types.FLOAT:            return "float";
        case    Types.INTEGER:          return "integer";
        case    Types.LONGVARBINARY:    return "long varchar for bit data";
        case    Types.LONGVARCHAR:      return "long varchar";
        case    Types.NUMERIC:          return "numeric" + precisionAndScale( precision, scale );
        case    Types.REAL:             return "real";
        case    Types.SMALLINT:         return "smallint";
        case    Types.TIME:             return "time";
        case    Types.TIMESTAMP:        return "timestamp";
        case    Types.TINYINT:          return "smallint";
        case    Types.VARBINARY:        return "varchar " + precisionToLength( precision ) + "  for bit data";
        case    Types.VARCHAR:          return "varchar" + precisionToLength( precision );
 
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        default:  throw ToolUtilities.newSQLException( SQLState.LUCENE_UNSUPPORTED_TYPE, typeName );
        }
    }

    /**
     * <p>
     * Turns precision into a length designator.
     * </p>
     *
     */
    private  static String  precisionToLength( int precision )
    {
        return "( " + precision + " )";
    }

    /**
     * <p>
     * Build a precision and scale designator.
     * </p>
     *
     */
    private static  String  precisionAndScale( int precision, int scale )
    {
        return "( " + precision + ", " + scale + " )";
    }

    /**
     * Add the field to the document so that it can be read by LuceneQueryVTI.
     * May raise an exception if the type is not supported.
     */
    private static  void    addValue
        (
         Document   doc,
         VTITemplate.ColumnDescriptor  keyDescriptor,
         ResultSet  rs,
         int    columnIdx   // 1-based
         )
        throws SQLException
    {
        IndexableField     field = null;
        
        switch( keyDescriptor.jdbcType )
        {
        case    Types.SMALLINT:
        case    Types.TINYINT:
        case    Types.INTEGER:
            field = getIntField( keyDescriptor, rs, columnIdx );
            break;

        case    Types.REAL:
            field = getFloatField( keyDescriptor, rs, columnIdx );
            break;

        case    Types.FLOAT:
        case    Types.DOUBLE:
            field = getDoubleField( keyDescriptor, rs, columnIdx );
            break;

        case    Types.BIGINT:
            field = getLongField( keyDescriptor, rs, columnIdx );
            break;

        case    Types.DATE:
            field = getDateField( keyDescriptor, rs, columnIdx );
            break;

        case    Types.TIME:
            field = getTimeField( keyDescriptor, rs, columnIdx );
            break;

        case    Types.TIMESTAMP:
            field = getTimestampField( keyDescriptor, rs, columnIdx );
            break;

        case    Types.CHAR:
        case    Types.CLOB:
        case    Types.DECIMAL:
        case    Types.LONGVARCHAR:
        case    Types.NUMERIC:
        case    Types.VARCHAR:
            field = getStringField( keyDescriptor, rs, columnIdx );
            break;

        case    Types.BLOB:
        case    Types.BINARY:
        case    Types.LONGVARBINARY:
        case    Types.VARBINARY:
            field = getBinaryField( keyDescriptor, rs, columnIdx );
            break;

        case    Types.BIT:
        case    Types.BOOLEAN:
            boolean booleanValue = rs.getBoolean( columnIdx );
            if ( !rs.wasNull() )
            {
                field = new StringField( keyDescriptor.columnName, booleanValue ? "true" : "false", Store.YES );
            }
            break;
            
        default:
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            throw ToolUtilities.newSQLException
                ( SQLState.LUCENE_UNSUPPORTED_TYPE, keyDescriptor.typeName );
        }

        // Lucene fields do not allow null values
        if ( rs.wasNull() ) { field = null; }
//IC see: https://issues.apache.org/jira/browse/DERBY-6602

        if ( field != null ) { doc.add( field ); }
    }
	
    /**
     * Get a string value to add to the document read by LuceneQueryVTI.
     */
    private static  IndexableField getStringField
        (
         VTITemplate.ColumnDescriptor  keyDescriptor,
         ResultSet  rs,
         int    columnIdx   // 1-based
         )
        throws SQLException
    {
        String  stringValue = rs.getString( columnIdx );
        if ( stringValue != null )
        {
            return new StringField( keyDescriptor.columnName, stringValue, Store.YES );
        }
        else { return null; }
    }
    
    /**
     * Get a float value to add to the document read by LuceneQueryVTI.
     */
    private static  IndexableField    getFloatField
        (
         VTITemplate.ColumnDescriptor  keyDescriptor,
         ResultSet  rs,
         int    columnIdx   // 1-based
         )
        throws SQLException
    {
        float   value = rs.getFloat( columnIdx );
        if ( rs.wasNull() ) { return null; }
        else
        {
            return new StoredField( keyDescriptor.columnName, value );
        }
    }
    
    /**
     * Get a double value to add to the document read by LuceneQueryVTI.
     */
    private static  IndexableField    getDoubleField
        (
         VTITemplate.ColumnDescriptor  keyDescriptor,
         ResultSet  rs,
         int    columnIdx   // 1-based
         )
        throws SQLException
    {
        double   value = rs.getDouble( columnIdx );
        if ( rs.wasNull() ) { return null; }
        else
        {
            return new StoredField( keyDescriptor.columnName, value );
        }
    }
    
    /**
     * Get an long value to add to the document read by LuceneQueryVTI.
     */
    private static  IndexableField    getLongField
        (
         VTITemplate.ColumnDescriptor  keyDescriptor,
         ResultSet  rs,
         int    columnIdx   // 1-based
         )
        throws SQLException
    {
        long    value = rs.getLong( columnIdx );
        if ( rs.wasNull() ) { return null; }
        else
        {
            return new StoredField( keyDescriptor.columnName, value );
        }
    }
    
    /**
     * Get a Date value to add to the document read by LuceneQueryVTI.
     */
    private static  IndexableField    getDateField
        (
         VTITemplate.ColumnDescriptor  keyDescriptor,
         ResultSet  rs,
         int    columnIdx   // 1-based
         )
        throws SQLException
    {
        Date    value = rs.getDate( columnIdx );
        if ( rs.wasNull() ) { return null; }
        else
        {
            return new StoredField( keyDescriptor.columnName, value.getTime() );
        }
    }
    
    /**
     * Get a Time value to add to the document read by LuceneQueryVTI.
     */
    private static  IndexableField    getTimeField
        (
         VTITemplate.ColumnDescriptor  keyDescriptor,
         ResultSet  rs,
         int    columnIdx   // 1-based
         )
        throws SQLException
    {
        Time    value = rs.getTime( columnIdx );
        if ( rs.wasNull() ) { return null; }
        else
        {
            return new StoredField( keyDescriptor.columnName, value.getTime() );
        }
    }
    
    /**
     * Get a Timestamp value to add to the document read by LuceneQueryVTI.
     */
    private static  IndexableField    getTimestampField
        (
         VTITemplate.ColumnDescriptor  keyDescriptor,
         ResultSet  rs,
         int    columnIdx   // 1-based
         )
        throws SQLException
    {
        Timestamp    value = rs.getTimestamp( columnIdx );
        if ( rs.wasNull() ) { return null; }
        else
        {
            return new StoredField( keyDescriptor.columnName, value.getTime() );
        }
    }
    
    /**
     * Get an integer value to add to the document read by LuceneQueryVTI.
     */
    private static  IndexableField    getIntField
        (
         VTITemplate.ColumnDescriptor  keyDescriptor,
         ResultSet  rs,
         int    columnIdx   // 1-based
         )
        throws SQLException
    {
        int     value = rs.getInt( columnIdx );
        if ( rs.wasNull() ) { return null; }
        else
        {
            return new StoredField( keyDescriptor.columnName, value );
        }
    }
    
    /**
     * Get a binary value to add to the document read by LuceneQueryVTI.
     */
    private static  IndexableField    getBinaryField
        (
         VTITemplate.ColumnDescriptor  keyDescriptor,
         ResultSet  rs,
         int    columnIdx   // 1-based
         )
        throws SQLException
    {
        byte[]  value = rs.getBytes( columnIdx );
        if ( value != null )
        {
            BytesRef    ref = new BytesRef( value );
            return new StoredField( keyDescriptor.columnName, ref );
        }
        else { return null; }
    }
    
    /**
     * Raise an exception if the text column doesn't exist or isn't a String datatype.
     */
	private static void vetTextColumn( DatabaseMetaData dbmd, String schema, String table, String textcol )
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        schema = ToolUtilities.derbyIdentifier( schema );
        table = ToolUtilities.derbyIdentifier( table );
        textcol = ToolUtilities.derbyIdentifier( textcol );
        
        ResultSet   rs = dbmd.getColumns( null, schema, table, textcol );

        try {
            if ( rs.next() )
            {
                switch( rs.getInt( "DATA_TYPE" ) )
                {
                case    Types.CHAR:
                case    Types.CLOB:
                case    Types.LONGVARCHAR:
                case    Types.VARCHAR:
                    return;
                }
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            throw ToolUtilities.sqlException
                ( StandardException.newException( SQLState.LUCENE_NOT_A_STRING_TYPE ) );
        }
        finally
        {
            rs.close();
        }
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  NAMESPACE
    //
    /////////////////////////////////////////////////////////////////////

    /**
     * A Lucene query table function already has system-supplied columns
     * named documentID and score. These can't be the names of the key
     * or text columns supplied by the user.
     */
    private static  void    vetColumnName( String columnName )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        String  derbyColumnName = columnName;
//IC see: https://issues.apache.org/jira/browse/DERBY-6730

        if (
            DOCUMENT_ID.equals( derbyColumnName ) ||
            SCORE.equals( derbyColumnName )
            )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            throw ToolUtilities.newSQLException( SQLState.LUCENE_BAD_COLUMN_NAME, derbyColumnName );
        }
    }

    /**
     * Return the qualified name of the table.
     */
	static String   makeTableName( String schema, String table )
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        schema = ToolUtilities.derbyIdentifier( schema );
        table = ToolUtilities.derbyIdentifier( table );

        return IdUtil.mkQualifiedName( schema, table );
    }

    /** Return the qualified name of the table function */
	private static String   makeTableFunctionName( String schema, String table, String textcol )
        throws SQLException
    {
		// Provide some basic protection against someone trying to put path modifiers (../../etc.)
		// into the arguments.
        forbidCharacter( schema, table, textcol, "." );
        forbidCharacter( schema, table, textcol, "/" );
        forbidCharacter( schema, table, textcol, "\\" );
		
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        schema = ToolUtilities.derbyIdentifier( schema );
        String  function = makeUnqualifiedTableFunctionName( table, textcol );

        return IdUtil.mkQualifiedName( schema, function );
    }

    /** Make the unqualified name of a querying table function */
    private static  String  makeUnqualifiedTableFunctionName( String table, String textcol )
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        return ToolUtilities.derbyIdentifier( table ) + SEPARATOR +
            ToolUtilities.derbyIdentifier( textcol );
    }

    /** Return true if the table function exists */
    private static  boolean tableFunctionExists( Connection conn, String schema, String table, String textcol )
        throws SQLException
    {
        schema = ToolUtilities.derbyIdentifier( schema );
        String  function = makeUnqualifiedTableFunctionName( table, textcol );

        ResultSet   rs = conn.getMetaData().getFunctions( null, schema, function );

        try {
            return rs.next();
        }
        finally { rs.close(); }
    }

    /** Decompose a function name of the form $table__$column into $table and $column */
    static  String[]    decodeFunctionName( String functionName )
    {
        int     separatorIdx = functionName.indexOf( SEPARATOR );
        String[]    retval = new String[ PART_COUNT ];

        retval[ TABLE_PART ] = functionName.substring( 0, separatorIdx );
        retval[ COLUMN_PART ] = functionName.substring( separatorIdx + SEPARATOR.length() );

        return retval;
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  MANAGE THE INDEX PROPERTIES FILE
    //
    /////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the handle on the file holding the index properties.
     * </p>
     */
	static StorageFile getIndexPropertiesFile( Connection conn, String schema, String table, String textcol )
        throws SQLException, IOException, PrivilegedActionException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        return getIndexPropertiesFile( getDerbyLuceneDir( conn, schema, table, textcol ) );
    }
    
    /**
     * <p>
     * Get the handle on the file holding the index properties.
     * </p>
     */
	static StorageFile getIndexPropertiesFile( DerbyLuceneDir dir )
        throws SQLException, IOException, PrivilegedActionException
    {
        StorageFile         propertiesFile = dir.getFile( PROPERTIES_FILE_NAME );

        return propertiesFile;
    }
    
    /** Read the index properties file */
    static  Properties readIndexPropertiesNoPrivs( StorageFile file )
        throws IOException
    {
        if ( file == null ) { return null; }
        
        Properties  properties = new Properties();
        InputStream is = file.getInputStream();

        properties.load( is );
        is.close();
                        
        return properties;
    }

    /** Write the index properties file */
    private static  void    writeIndexProperties( final StorageFile file, Properties properties )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws IOException
    {
        if (file == null || properties == null) {
            return;
        }

        OutputStream os;
        try {
            os = AccessController.doPrivileged(
                    new PrivilegedExceptionAction<OutputStream>() {
                public OutputStream run() throws IOException {
                    return file.getOutputStream();
                }
            });
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }

        properties.store( os, null );
        os.flush();
        os.close();
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  SQL/JDBC SUPPORT
    //
    /////////////////////////////////////////////////////////////////////

    /**
     * Raise an error if the connection is readonly.
     */
    private static  void    forbidReadOnlyConnections()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        if ( ConnectionUtil.getCurrentLCC().getAuthorizer().isReadOnlyConnection() )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            throw ToolUtilities.newSQLException( SQLState.AUTH_WRITE_WITH_READ_ONLY_CONNECTION );
        }
    }

	/**
	 * Get a connection to the database
	 * 
	 * @return a connection
	 * 
	 * @throws SQLException
	 */
	static Connection getDefaultConnection() throws SQLException
    {
		return DriverManager.getConnection( "jdbc:default:connection" );
	}

    /** Return true if the LuceneSupport schema exists already */
    private static  boolean luceneSchemaExists( Connection conn )
        throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement
            ( "select count(*) from sys.sysschemas where schemaName = ?" );
        ps.setString( 1, LUCENE_SCHEMA.toUpperCase() );
        ResultSet   rs = ps.executeQuery();

        try {
            rs.next();
            return ( rs.getInt( 1 ) > 0 );
        } finally
        {
            rs.close();
            ps.close();
        }
    }

	/**
	 * Execute a DDL statement
	 * 
	 * @param c a Connection
	 * @param text the text of the statement to execute
	 * @throws SQLException
	 */
	private void executeDDL(Connection c, String text) throws SQLException {
    	PreparedStatement ddl = c.prepareStatement(text);
    	ddl.execute();
    	ddl.close();
    }
	
    /** Double quote an identifier in order to preserver casing */
    static String delimitID( String id )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6730
        return IdUtil.normalToDelimited( id );
    }

    /** Raise an error if an argument is being given a null value */
    static  void    checkNotNull( String argumentName, String argumentValue )
//IC see: https://issues.apache.org/jira/browse/DERBY-6596
        throws SQLException
    {
        if ( argumentValue == null )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            throw ToolUtilities.newSQLException( SQLState.ARGUMENT_MAY_NOT_BE_NULL, argumentName );
        }
    }

    /**
     * Return the primary key columns for a table, sorted by key position.
     */
    private static  VTITemplate.ColumnDescriptor[] getPrimaryKeys
        (
         Connection conn,
         String schema,
         String table
         )
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        ResultSet   keysRS = conn.getMetaData().getPrimaryKeys
            ( null, ToolUtilities.derbyIdentifier( schema ), ToolUtilities.derbyIdentifier( table ) );
        ArrayList<VTITemplate.ColumnDescriptor>    keyArray = new ArrayList<VTITemplate.ColumnDescriptor>();
        try {
            while ( keysRS.next() )
            {
                String  columnName = keysRS.getString( "COLUMN_NAME" );
                int     keyPosition = keysRS.getInt( "KEY_SEQ" );

                ResultSet   colInfoRS = conn.prepareStatement
                    ( "select " + columnName + " from " + makeTableName( schema, table ) + " where 1=2" ).executeQuery();
                ResultSetMetaData   rsmd = colInfoRS.getMetaData();
                VTITemplate.ColumnDescriptor   keyDescriptor = new VTITemplate.ColumnDescriptor
                    (
                     columnName,
                     rsmd.getColumnType( 1 ),
                     rsmd.getPrecision( 1 ),
                     rsmd.getScale( 1 ),
                     rsmd.getColumnTypeName( 1 ),
                     keyPosition
                     );
                keyArray.add( keyDescriptor );
                colInfoRS.close();
            }
        }
        finally
        {
            keysRS.close();
        }

        VTITemplate.ColumnDescriptor[] result = new VTITemplate.ColumnDescriptor[ keyArray.size() ];
        keyArray.toArray( result );
        Arrays.sort( result );

        return result;
    }

    /**
     * Return the key columns for an existing LuceneQueryVTI table function.
     */
    private static  VTITemplate.ColumnDescriptor[] getKeys
        (
//IC see: https://issues.apache.org/jira/browse/DERBY-590
         Connection conn,
         String schema,
         String table,
         String textcol
         )
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        schema = ToolUtilities.derbyIdentifier( schema );
        String  functionName = makeUnqualifiedTableFunctionName( table, textcol );
        ArrayList<VTITemplate.ColumnDescriptor>    keyArray = new ArrayList<VTITemplate.ColumnDescriptor>();

        ResultSet   rs = conn.getMetaData().getFunctionColumns( null, schema, functionName, "%" );
        try {
            while ( rs.next() )
            {
                if ( rs.getInt( "COLUMN_TYPE" ) == DatabaseMetaData.functionColumnResult )
                {
                    VTITemplate.ColumnDescriptor   keyDescriptor = new VTITemplate.ColumnDescriptor
                        (
                         rs.getString( "COLUMN_NAME" ),
                         rs.getInt( "DATA_TYPE" ),
                         rs.getInt( "PRECISION" ),
                         rs.getInt( "SCALE" ),
                         rs.getString( "TYPE_NAME" ),
                         rs.getInt( "ORDINAL_POSITION" )
                         );
                    keyArray.add( keyDescriptor );
                }
            }
        }
        finally
        {
            rs.close();
        }
        
        VTITemplate.ColumnDescriptor[] temp = new VTITemplate.ColumnDescriptor[ keyArray.size() ];
        keyArray.toArray( temp );
        Arrays.sort( temp );

        // remove the last two columns, which are not keys. they are the DOCUMENTID and SCORE columns.
        int     count = temp.length - 2;
        VTITemplate.ColumnDescriptor[] result = new VTITemplate.ColumnDescriptor[ count ];
        for ( int i = 0; i < count; i++ ) { result[ i ] = temp[ i ]; }

        return result;
    }
    
    /**
     * Return column information for a proposed set of keys.
     */
    private static  VTITemplate.ColumnDescriptor[] getKeys
        (
         Connection conn,
         String schema,
         String table,
         String... keyColumns
         )
        throws SQLException
    {
        String      qualifiedName = makeTableName( schema, table );
        StringBuilder   buffer = new StringBuilder();

        buffer.append( "select " );
        int counter = 0;
        for ( String key : keyColumns )
        {
            checkNotNull( "KEYCOLUMNS", key );
//IC see: https://issues.apache.org/jira/browse/DERBY-6596

            if ( counter > 0 ) { buffer.append( ", " ); }
            counter++;
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            buffer.append( delimitID( ToolUtilities.derbyIdentifier( key ) ) );
        }
        buffer.append( "\nfrom " + qualifiedName );
        buffer.append( "\nwhere 1=2" );

        ArrayList<VTITemplate.ColumnDescriptor>    keyArray = new ArrayList<VTITemplate.ColumnDescriptor>();

        ResultSet   rs = conn.prepareStatement( buffer.toString() ).executeQuery();
        ResultSetMetaData   rsmd = rs.getMetaData();
        try {
            for ( int keyPosition = 1; keyPosition <= rsmd.getColumnCount(); keyPosition++ )
            {
                VTITemplate.ColumnDescriptor   keyDescriptor = new VTITemplate.ColumnDescriptor
                    (
                     rsmd.getColumnName( keyPosition ),
                     rsmd.getColumnType( keyPosition ),
                     rsmd.getPrecision( keyPosition ),
                     rsmd.getScale( keyPosition ),
                     rsmd.getColumnTypeName( keyPosition ),
                     keyPosition
                     );
                keyArray.add( keyDescriptor );
            }
        }
        finally
        {
            rs.close();
        }
        
        VTITemplate.ColumnDescriptor[] result = new VTITemplate.ColumnDescriptor[ keyArray.size() ];
        keyArray.toArray( result );

        return result;
    }
    

    /////////////////////////////////////////////////////////////////////
    //
    //  FILE MANAGEMENT
    //
    /////////////////////////////////////////////////////////////////////

    /** Return true if the directory is empty */
    private static  boolean isEmpty( final StorageFile dir )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        String[]  contents = AccessController.doPrivileged
            (
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             new PrivilegedAction<String[]>()
             {
                 public String[] run()
                {
                    return dir.list();
                }
             }
             );

        if ( contents == null ) { return true; }
        else if ( contents.length == 0 ) { return true; }
        else { return false; }
    }

    /** Return true if the file exists */
    private static  boolean exists( final StorageFile file )
    {
        return AccessController.doPrivileged
            (
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             new PrivilegedAction<Boolean>()
             {
                 public Boolean run()
                {
                    return file.exists();
                }
             }
             ).booleanValue();
    }

    /** Really delete a file */
    private static  boolean deleteFile( final StorageFile file )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        boolean result = AccessController.doPrivileged(
                new PrivilegedAction<Boolean>() {
            public Boolean run() {
                return file.isDirectory() ? file.deleteAll() : file.delete();
            }
        });

        if (!result) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            throw ToolUtilities.newSQLException
                ( SQLState.UNABLE_TO_DELETE_FILE, file.getPath() );
        }

        return result;
    }

    /** Forbid invalid character */
    private static  void    forbidCharacter( String schema, String table, String textcol, String invalidCharacter )
        throws SQLException
    {
		if (schema.indexOf( invalidCharacter ) > 0 || table.indexOf( invalidCharacter ) > 0 || textcol.indexOf( invalidCharacter ) > 0)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            throw ToolUtilities.newSQLException( SQLState.LUCENE_INVALID_CHARACTER, invalidCharacter );
		}		
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  LUCENE SUPPORT
    //
    /////////////////////////////////////////////////////////////////////

	/**
	 * Returns a Lucene IndexWriter, that writes inside the lucene directory inside the database
	 * directory.
	 * 
	 * @param luceneVersion the version of Lucene being used
	 * @param analyzer      the Analyzer being used
	 * @param derbyLuceneDir location of the lucene directory
	 * @return a Lucene IndexWriter
	 */
	private static IndexWriter getIndexWriter
        (
//IC see: https://issues.apache.org/jira/browse/DERBY-590
         final Version  luceneVersion,
         final  Analyzer    analyzer,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
         final DerbyLuceneDir   derbyLuceneDir
         )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws IOException
    {
        try {
            return AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<IndexWriter>()
             {
                 public IndexWriter run() throws IOException
                 {
                     // allow this to be overridden in the configuration during load later.
                     IndexWriterConfig iwc = new IndexWriterConfig( luceneVersion, analyzer );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                     IndexWriter iw = new IndexWriter( derbyLuceneDir, iwc );
		
                     return iw;
                 }
             }
             );
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
	}
	
	/**
	 * Add a document to a Lucene index wrier.
	 */
    private static void addDocument
        (
         final IndexWriter  indexWriter,
         final Document     document
         )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws IOException
    {
        try {
            AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<Void>()
             {
                 public Void run() throws IOException
                 {
                     indexWriter.addDocument( document );
		
                     return null;
                 }
             }
             );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }

	/**
	 * Close an IndexWriter.
	 */
    private static void close( final IndexWriter  indexWriter )
        throws IOException
    {
        try {
            AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<Void>()
             {
                 public Void run() throws IOException
                 {
                     indexWriter.close();
		
                     return null;
                 }
             }
             );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }

	/**
	 * Invoke a static method (possibly supplied by the user) to instantiate an index descriptor.
     * The method has no arguments.
	 */
	private static LuceneIndexDescriptor getIndexDescriptor( final String indexDescriptorMaker )
//IC see: https://issues.apache.org/jira/browse/DERBY-6600
        throws PrivilegedActionException, SQLException
    {
        return AccessController.doPrivileged
            (
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             new PrivilegedExceptionAction<LuceneIndexDescriptor>()
             {
                 public LuceneIndexDescriptor run()
                     throws ClassNotFoundException, IllegalAccessException,
                     InvocationTargetException, NoSuchMethodException,
                     SQLException
                 {
                     return getIndexDescriptorNoPrivs( indexDescriptorMaker );
                 }
             }
             );
	}
	
	/**
	 * Invoke a static method (possibly supplied by the user) to instantiate an index descriptor.
     * The method has no arguments.
	 */
	static LuceneIndexDescriptor getIndexDescriptorNoPrivs( String indexDescriptorMaker )
        throws ClassNotFoundException, IllegalAccessException, InvocationTargetException,
               NoSuchMethodException, SQLException
    {
        int    lastDotIdx = indexDescriptorMaker.lastIndexOf( "." );
        String  className = indexDescriptorMaker.substring( 0, lastDotIdx );
        ClassInspector  ci = getClassFactory().getClassInspector();
        Class<? extends Object>  klass = ci.getClass( className );
        String methodName = indexDescriptorMaker.substring( lastDotIdx + 1, indexDescriptorMaker.length() );
        Method method = klass.getDeclaredMethod( methodName );
                     
        return (LuceneIndexDescriptor) method.invoke( null );
	}

	/**
	 * Add a document to a Lucene index wrier.
	 */
    private static void createLuceneDir( final Connection conn )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        try {
            AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run() throws SQLException
                     {
                         StorageFactory storageFactory = getStorageFactory( conn );
                         StorageFile    luceneDir = storageFactory.newStorageFile( Database.LUCENE_DIR );

                         luceneDir.mkdir();
		
                         return null;
                     }
                 }
                 );
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        catch (PrivilegedActionException pae) {
            throw (SQLException) pae.getCause();
        }
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  DERBY STORE
    //
    /////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the handle on the Lucene directory inside the database.
     * </p>
     */
	static DerbyLuceneDir getDerbyLuceneDir( Connection conn, String schema, String table, String textcol )
        throws SQLException
    {
        StorageFactory  storageFactory = getStorageFactory( conn );
        DerbyLuceneDir  result = DerbyLuceneDir.getDirectory( storageFactory, schema, table, textcol );
//IC see: https://issues.apache.org/jira/browse/DERBY-590

        return result;
    }
    
    /** Get the StorageFactory of the connected database */
    static  StorageFactory  getStorageFactory( Connection conn )
        throws SQLException
    {
        return getDataFactory( conn ).getStorageFactory();
    }

    /** Get the DataFactory of the connected database */
    static  DataFactory  getDataFactory( Connection conn )
        throws SQLException
    {
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
            Object monitor = findService
                ( Property.DATABASE_MODULE, ((EmbedConnection) conn).getDBName() ) ;
            return (DataFactory) findServiceModule( monitor, DataFactory.MODULE );
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        catch (StandardException se) { throw ToolUtilities.wrap( se ); }
    }

	/**
//IC see: https://issues.apache.org/jira/browse/DERBY-6600
		Get the ClassFactory to use with this database.
	*/
	static  ClassFactory getClassFactory()
        throws SQLException
    {
		return ConnectionUtil.getCurrentLCC().getLanguageConnectionFactory().getClassFactory();
	}
	
    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object findServiceModule( final Object serviceModule, final String factoryInterface)
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.findServiceModule( serviceModule, factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    /**
     * Privileged service lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object findService( final String factoryInterface, final String serviceName )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.findService( factoryInterface, serviceName );
                 }
             }
             );
    }
    
}
