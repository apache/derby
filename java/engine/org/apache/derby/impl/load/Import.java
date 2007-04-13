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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.DatabaseMetaData;
import java.util.*;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.PublicAPI;

/**
 * This class implements import of data from a URL into a table.
 * Import functions provided here in this class shouble be called through
 * Systement Procedures. Import uses VTI , which is supprted only through 
 * Systemem procedures mechanism. 
 */

public class Import extends ImportAbstract{

    private static  int                _importCounter;
    private static  Hashtable   _importers = new Hashtable();

    private String inputFileName;

	/**
	 * Constructior to Invoke Import from a select statement 
	 * @param inputFileName	 The URL of the ASCII file from which import will happen
	 * @exception Exception on error 
	 */
	public Import(String inputFileName, String columnDelimiter,
                  String characterDelimiter,  String codeset, 
                  int noOfColumnsExpected,  String columnTypes, 
                  boolean lobsInExtFile,
                  int importCounter ) throws SQLException 
	{

		try{
			this.inputFileName = inputFileName;
            this.noOfColumnsExpected = noOfColumnsExpected;
            this.tableColumnTypesStr = columnTypes;
			controlFileReader = new ControlInfo();
			controlFileReader.setControlProperties(characterDelimiter,
												   columnDelimiter, codeset);
            this.lobsInExtFile = lobsInExtFile;

            _importers.put( new Integer( importCounter ), this );
            
			doImport();

		}catch(Exception e)
		{
			throw LoadError.unexpectedError(e);
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
	 * @param connection	 The Derby database connection URL for the database containing the table
	 * @param schemaName	The name of the schema where table to import exists 
	 * @param tableName     Name of the Table the data has to be imported to.
	 * @param inputFileName Name of the file from which data has to be imported.
	 * @param columnDelimiter  Delimiter that seperates columns in the file
	 * @param characterDelimiter  Delimiter that is used to quiote non-numeric types
	 * @param codeset           Codeset of the data in the file
	 * @param replace          Indicates whether the data in table has to be replaced or
	 *                         appended.(0 - append , > 0 Replace the data)
     * @param lobsInExtFile true, if the lobs data is stored in an external file,
     *                      and the reference to it is stored in the main import file.
 	 * @exception SQL Exception on errors
	 */

	public static void importTable(Connection connection, String schemaName, 
                                   String tableName, String inputFileName,  
                                   String columnDelimiter, 
                                   String characterDelimiter,String codeset, 
                                   short replace, boolean lobsInExtFile)
		throws SQLException {


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
	 *                         appended.(0 - append , > 0 Replace the data)
     * @param lobsInExtFile true, if the lobs data is stored in an external file,
     *                      and the reference is stored in the main import file.
 	 * @exception SQL Exception on errors
	 */
	public static void importData(Connection connection, String schemaName,
                                  String tableName, String insertColumnList, 
                                  String columnIndexes, String inputFileName, 
                                  String columnDelimiter, 
                                  String characterDelimiter,
                                  String codeset, short replace, 
                                  boolean lobsInExtFile)
		throws SQLException 
	{
		

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
        (Connection connection, 
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
        Integer     importCounter = new Integer( bumpImportCounter() );
        
        try {
            if (connection == null)
                throw LoadError.connectionNull();
            
            
            
            if (tableName == null)
                throw LoadError.entityNameMissing();
            
            
            ColumnInfo columnInfo = new ColumnInfo(connection , schemaName ,
                                                   tableName, insertColumnList, 
                                                   columnIndexes, COLUMNNAMEPREFIX);
            
            /* special handling of single quote delimiters
             * Single quote should be writeen with an extra quote otherwise sql will
             * throw syntac error.
             * i.e  to recognize a quote  it has to be appended with extra  quote ('')
             */
            if(characterDelimiter!=null && characterDelimiter.equals("'"))
                characterDelimiter = "''";
            if(columnDelimiter !=null && columnDelimiter.equals("'"))
                columnDelimiter = "''";
            
            
            StringBuffer sb = new StringBuffer("new ");
            sb.append("org.apache.derby.impl.load.Import");
            sb.append("(") ; 
            sb.append(	(inputFileName !=null ? "'" + inputFileName + "'" : null));
            sb.append(",") ;
            sb.append(	(columnDelimiter !=null ? "'" + columnDelimiter + "'" : null));
            sb.append(",") ;
            sb.append(	(characterDelimiter !=null ? "'" + characterDelimiter + "'" : null));
            sb.append(",") ;
            sb.append(	(codeset !=null ? "'" + codeset + "'" : null));
            sb.append(", ");
            sb.append( columnInfo.getExpectedNumberOfColumnsInFile());
            sb.append(", ");
            sb.append( "'" + columnInfo.getExpectedVtiColumnTypesAsString() + "'");
            sb.append(", ");
            sb.append(lobsInExtFile);
            sb.append(", ");
            sb.append( importCounter.intValue() );
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
            
            String entityName = (schemaName == null ? "\""+ tableName + "\"" : 
                                 "\"" + schemaName + "\"" + "." + "\"" + tableName + "\""); 
            
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

	/** virtual method from the abstract class
	 * @exception	Exception on error
	 */
	ImportReadData getImportReadData() throws Exception {
		return new ImportReadData(inputFileName, controlFileReader);
	}

    /*
     * Bump the import counter.
     *
     */
    private static  synchronized    int bumpImportCounter()
    {
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
            ( SQLState.UNEXPECTED_IMPORT_ERROR, new Integer( lineNumber ), inputFile, t.getMessage() );
        se.setNestedException( t );

        return PublicAPI.wrapStandardException(se);
    }
    
}
