/*

   Derby - Class org.apache.derby.impl.load.Import

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

package org.apache.derby.impl.load;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.util.*;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.error.PublicAPI;
import org.apache.derby.iapi.security.Securable;
import org.apache.derby.iapi.security.SecurityUtil;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.StringUtil;

/**
 * This class implements import of data from a URL into a table.
 * Import functions provided here in this class shouble be called through
 * Systement Procedures. Import uses VTI , which is supprted only through 
 * Systemem procedures mechanism. 
 */

public class Import extends ImportAbstract{

    private static  int                _importCounter;

    //
    // This hashtable stores Import instances, which keep the context needed
    // to correlate Derby errors with line numbers in the file that is being
    // imported. An importing thread will access this hashtable at the very
    // beginning and the very end of its run. We cannot use Hashmap
    // because different threads may simultaneously put and delete entries.
    //
    private static  Hashtable<Integer,Import>   _importers = new Hashtable<Integer,Import>();

    private String inputFileName;
    private static short skip; //The number of header lines to be skipped
	/**
     * Constructor to Invoke Import from a select statement
	 * @param inputFileName	 The URL of the ASCII file from which import will happen
     * @exception SQLException on error
	 */
	public Import(String inputFileName, String columnDelimiter,
//IC see: https://issues.apache.org/jira/browse/DERBY-378
                  String characterDelimiter,  String codeset, 
                  int noOfColumnsExpected,  String columnTypes, 
                  boolean lobsInExtFile,
                  int importCounter,
//IC see: https://issues.apache.org/jira/browse/DERBY-4484
                  String columnTypeNames, String udtClassNamesString ) throws SQLException 
	{
		try{
			this.inputFileName = inputFileName;
            this.noOfColumnsExpected = noOfColumnsExpected;
            this.tableColumnTypesStr = columnTypes;
            this.columnTypeNamesString = columnTypeNames;
            this.udtClassNamesString = udtClassNamesString;
			controlFileReader = new ControlInfo();
			controlFileReader.setControlProperties(characterDelimiter,
												   columnDelimiter, codeset);
            this.lobsInExtFile = lobsInExtFile;

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            _importers.put( importCounter, this );
            
			doImport();

		}catch(Exception e)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-3068
			throw importError(e);
		}
	}


	private void doImport() throws Exception
	{
		if (inputFileName == null)
			throw LoadError.dataFileNull();
		doAllTheWork();

	}

	
	/**
	 * SYSCS_IMPORT_TABLE  system Procedure from ij or from a Java application
	 * invokes  this method to perform import to a table from a file.
	 *
	 * The extraArgs parameter is variadic, and is used when this method is
	 * called from SYSCS_IMPORT_TABLE_BULK, in which case extraArgs[0]
	 * specifies the number of header lines to skip.
	 *
	 * @param connection	 The Derby database connection URL for the database containing the table
	 * @param schemaName	The name of the schema where table to import exists 
	 * @param tableName     Name of the Table the data has to be imported to.
	 * @param inputFileName Name of the file from which data has to be imported.
	 * @param columnDelimiter  Delimiter that seperates columns in the file
	 * @param characterDelimiter  Delimiter that is used to quiote non-numeric types
	 * @param codeset           Codeset of the data in the file
	 * @param replace          Indicates whether the data in table has to be replaced or
	 *                         appended.(0 - append , &gt; 0 Replace the data)
     * @param lobsInExtFile true, if the lobs data is stored in an external file,
     *                      and the reference to it is stored in the main import file.
     * @exception SQLException on errors
	 */

	public static void importTable(Connection connection, String schemaName, 
//IC see: https://issues.apache.org/jira/browse/DERBY-378
                                   String tableName, String inputFileName,  
                                   String columnDelimiter, 
                                   String characterDelimiter,String codeset, 
//IC see: https://issues.apache.org/jira/browse/DERBY-4555
//IC see: https://issues.apache.org/jira/browse/DERBY-6892
                                   short replace, boolean lobsInExtFile, short... extraArgs)
		throws SQLException {


        /** Make sure that the current user has permission to perform this operation */
//IC see: https://issues.apache.org/jira/browse/DERBY-6616
        try {
            if ( lobsInExtFile )
            {
                SecurityUtil.authorize( Securable.IMPORT_TABLE_LOBS_FROM_EXTFILE );
            }
            else
            {
                SecurityUtil.authorize( Securable.IMPORT_TABLE );
            }
        }
        catch (StandardException se) { throw PublicAPI.wrapStandardException( se ); }
        if(extraArgs.length>0){
//IC see: https://issues.apache.org/jira/browse/DERBY-4555
//IC see: https://issues.apache.org/jira/browse/DERBY-6892
             skip=extraArgs[0];
	     if(skip<0) throw PublicAPI.wrapStandardException(StandardException.newException
				(SQLState.LANG_INVALID_NUMBEROF_HEADER_LINES)); 
	     
	}
        else 
            skip=0;

		performImport(connection,  schemaName,  null, //No columnList 
					  null , //No column indexes
					  tableName, inputFileName, columnDelimiter, 
					  characterDelimiter, codeset, replace, lobsInExtFile);
	}



		
	/**
	 * SYSCS_IMPORT_DATA  system Procedure from ij or from a Java application
	 * invokes  this method to perform import to a table from a file.
	 * @param connection	 The Derby database connection URL for the database containing the table
	 * @param schemaName	The name of the schema where table to import exists 
	 * @param tableName     Name of the Table the data has to be imported to.
	 * @param insertColumnList  Comma Seperated column name list to which data
	 *                          has to be imported from file.eg: 'c2,c2,'c3'.
	 * @param columnIndexes     Comma sepearted Lit Index of the columns in the file(first column
	                             starts at 1). eg: '3 ,4 , 5'
	 * @param inputFileName Name of the file from which data has to be imported.
	 * @param columnDelimiter  Delimiter that seperates columns in the file
	 * @param characterDelimiter  Delimiter that is used to quiote non-numeric types
	 * @param codeset           Codeset of the data in the file
	 * @param replace          Indicates whether the data in table has to be replaced or
	 *                         appended.(0 - append , &gt; 0 Replace the data)
     * @param lobsInExtFile true, if the lobs data is stored in an external file,
     *                      and the reference is stored in the main import file.
     * @exception SQLException on errors
	 */
	public static void importData(Connection connection, String schemaName,
//IC see: https://issues.apache.org/jira/browse/DERBY-378
                                  String tableName, String insertColumnList, 
                                  String columnIndexes, String inputFileName, 
                                  String columnDelimiter, 
                                  String characterDelimiter,
                                  String codeset, short replace, 
                                  boolean lobsInExtFile, short... extraArgs)
		throws SQLException 
	{
        /** Make sure that the current user has permission to perform this operation */
//IC see: https://issues.apache.org/jira/browse/DERBY-6616
        try {
            if ( lobsInExtFile )
            {
                SecurityUtil.authorize( Securable.IMPORT_DATA_LOBS_FROM_EXTFILE );
            }
            else
            {
                SecurityUtil.authorize( Securable.IMPORT_DATA );
            }
        }
        catch (StandardException se) { throw PublicAPI.wrapStandardException( se ); }
//IC see: https://issues.apache.org/jira/browse/DERBY-4555
//IC see: https://issues.apache.org/jira/browse/DERBY-6894
//IC see: https://issues.apache.org/jira/browse/DERBY-4555
//IC see: https://issues.apache.org/jira/browse/DERBY-6894
	if(extraArgs.length>0){
	    skip=extraArgs[0];
	    if(skip<0) throw PublicAPI.wrapStandardException(StandardException.newException
				(SQLState.LANG_INVALID_NUMBEROF_HEADER_LINES)); 
            
	}
        else 
            skip=0;
			performImport(connection,  schemaName,  insertColumnList,columnIndexes, 
						  tableName, inputFileName, columnDelimiter, 
						  characterDelimiter, codeset, replace, lobsInExtFile);
	}


	/*
	 * This function creates and executes  SQL Insert statement that performs the 
	 * the import using VTI. 
	 * eg:
	 * insert into T1 select  (cast column1 as DECIMAL), (cast column2 as
	 * INTEGER)  from new org.apache.derby.impl.load.Import('extin/Tutor1.asc') as importvti;
	 *
	 */
    private static void performImport
//IC see: https://issues.apache.org/jira/browse/DERBY-2193
        (Connection connection, 
//IC see: https://issues.apache.org/jira/browse/DERBY-378
         String schemaName, 
         String insertColumnList, 
         String columnIndexes,
         String tableName, 
         String inputFileName,  
         String  columnDelimiter, 
         String characterDelimiter, 
         String codeset, 
         short replace, 
         boolean lobsInExtFile)
        throws SQLException 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        Integer     importCounter = bumpImportCounter();
        
        try {
            if (connection == null)
                throw LoadError.connectionNull();
            
            
            
            if (tableName == null)
                throw LoadError.entityNameMissing();
            
            
            ColumnInfo columnInfo = new ColumnInfo(connection , schemaName ,
                                                   tableName, insertColumnList, 
//IC see: https://issues.apache.org/jira/browse/DERBY-4555
//IC see: https://issues.apache.org/jira/browse/DERBY-6894
                                                   columnIndexes, COLUMNNAMEPREFIX,readHeaders(inputFileName ,
												characterDelimiter,
												columnDelimiter,
												codeset));

//IC see: https://issues.apache.org/jira/browse/DERBY-4484
            String columnTypeNames = null;
            String udtClassNames = null;
            try {
                columnTypeNames = columnInfo.getColumnTypeNames();
                udtClassNames = columnInfo.getUDTClassNames();
            }
            catch (Throwable t)
            {
                throw formatImportError( (Import) _importers.get( importCounter ), inputFileName, t );
            }

            StringBuffer sb = new StringBuffer("new ");
            sb.append("org.apache.derby.impl.load.Import");
            sb.append("(") ;
            sb.append(quoteStringArgument(inputFileName));
            sb.append(",") ;
            sb.append(quoteStringArgument(columnDelimiter));
            sb.append(",") ;
            sb.append(quoteStringArgument(characterDelimiter));
            sb.append(",") ;
            sb.append(quoteStringArgument(codeset));
            sb.append(", ");
//IC see: https://issues.apache.org/jira/browse/DERBY-378
            sb.append( columnInfo.getExpectedNumberOfColumnsInFile());
            sb.append(", ");
            sb.append(quoteStringArgument(
                    columnInfo.getExpectedVtiColumnTypesAsString()));
            sb.append(", ");
            sb.append(lobsInExtFile);
            sb.append(", ");
            sb.append( importCounter.intValue() );
//IC see: https://issues.apache.org/jira/browse/DERBY-4484
            sb.append(", ");
            sb.append(quoteStringArgument( columnTypeNames ) );
            sb.append(", ");
            sb.append(quoteStringArgument( udtClassNames ) );
            sb.append(" )") ;
            
            String importvti = sb.toString();
            
            // delimit the table and schema names with quotes.
            // because they might have been  created as quoted
            // identifiers(for example when reserved words are used, names are quoted)
            
            // Import procedures are to be called with case-senisitive names. 
            // Incase of delimited table names, they need to be passed as defined
            // and when they are not delimited, they need to be passed in upper
            // case, because all undelimited names are stored in the upper case 
            // in the database. 
            
            String entityName = IdUtil.mkQualifiedName(schemaName, tableName);
            
            String insertModeValue;
            if(replace > 0)
                insertModeValue = "replace";
            else
                insertModeValue = "bulkInsert";
            
            String cNamesWithCasts = columnInfo.getColumnNamesWithCasts();
            String insertColumnNames = columnInfo.getInsertColumnNames();
            if(insertColumnNames !=null)
                insertColumnNames = "(" + insertColumnNames + ") " ;
            else
                insertColumnNames = "";
            String insertSql = "INSERT INTO " + entityName +  insertColumnNames + 
//IC see: https://issues.apache.org/jira/browse/DERBY-573
                " --DERBY-PROPERTIES insertMode=" + insertModeValue + "\n" +
                " SELECT " + cNamesWithCasts + " from " + 
                importvti + " AS importvti" ;
            
            //prepare the import statement to hit any errors before locking the table
            PreparedStatement ips = connection.prepareStatement(insertSql);
            
            //lock the table before perfoming import, because there may 
            //huge number of lockes aquired that might have affect on performance 
            //and some possible dead lock scenarios.
            Statement statement = connection.createStatement();
            String lockSql = "LOCK TABLE " + entityName + " IN EXCLUSIVE MODE";
            statement.executeUpdate(lockSql);
            
            //execute the import operaton.
            try {
                ips.executeUpdate();
            }
            catch (Throwable t)
            {
                throw formatImportError( (Import) _importers.get( importCounter ), inputFileName, t );
            }
            statement.close();
            ips.close();
        }
        finally
        {
            //
            // The importer was put into a hashtable so that we could look up
            // line numbers for error messages. The Import constructor put
            // the importer in the hashtable. Now garbage collect that entry.
            //
            _importers.remove( importCounter );
        }
    }
    
    //Read the header lines to get column names to identify columns by name
    private static String[] readHeaders(String inputFileName ,String characterDelimiter,String columnDelimiter,String codeset) 
//IC see: https://issues.apache.org/jira/browse/DERBY-4555
//IC see: https://issues.apache.org/jira/browse/DERBY-6894
	throws SQLException
	{
	try {
	    if(skip>0){
                ControlInfo controlFR = new ControlInfo();
		controlFR.setControlProperties(characterDelimiter,columnDelimiter, codeset);

		ImportReadData importReadData = new ImportReadData(inputFileName, controlFR, (short)0);
			
		String[] fullColumnName = new String[importReadData.numberOfColumns];
		String[] temp = new String[importReadData.numberOfColumns];
		for(int i=0; i< skip;i++){
                    importReadData.readNextRow(temp);
		        for(int j=0;j < importReadData.numberOfColumns;j++){
			    if(i==0)
			        fullColumnName[j]=temp[j];	
			    else
			    {
				if(temp[j]!=null)
				    fullColumnName[j]=fullColumnName[j] + " " + temp[j];
				
			    }
			}
		}
		return fullColumnName;
	    }
	    return null;
			
	}
	catch(Exception e){
	
	throw LoadError.unexpectedError(e);
	}
    }

	/** virtual method from the abstract class
	 * @exception	Exception on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-467
	ImportReadData getImportReadData() throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-4555
//IC see: https://issues.apache.org/jira/browse/DERBY-6892
		return new ImportReadData(inputFileName, controlFileReader, skip);
	}

    /*
     * Bump the import counter.
     *
     */
    private static  synchronized    int bumpImportCounter()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2193
        return ++_importCounter;
    }
    
    /*
     * Format a import error with line number
     *
     */
    private static  SQLException    formatImportError( Import importer, String inputFile, Throwable t )
    {
        int     lineNumber = -1;

        if ( importer != null ) { lineNumber = importer.getCurrentLineNumber(); }
        
        StandardException se = StandardException.newException
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            ( SQLState.UNEXPECTED_IMPORT_ERROR, lineNumber, inputFile, t.getMessage() );
        se.initCause(t);
//IC see: https://issues.apache.org/jira/browse/DERBY-2472

        return PublicAPI.wrapStandardException(se);
    }

    /**
     * Quote a string argument so that it can be used as a literal in an
     * SQL statement. If the string argument is {@code null} an SQL NULL token
     * is returned.
     *
     * @param string a string or {@code null}
     * @return the string in quotes and with proper escape sequences for
     * special characters, or "NULL" if the string is {@code null}
     */
    private static String quoteStringArgument(String string) {
        if (string == null) {
            return "NULL";
        }
        return StringUtil.quoteStringLiteral(string);
    }
}
