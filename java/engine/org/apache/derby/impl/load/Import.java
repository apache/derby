/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.load
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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

/**
 * This class implements import of data from a URL into a table.
 * Import functions provided here in this class shouble be called through
 * Systement Procedures. Import uses VTI , which is supprted only through 
 * Systemem procedures mechanism. 
 */

public class Import extends ImportAbstract{
	/**
	   IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	private String inputFileName;

	/**
	 * Constructior to Invoke Import from a select statement 
	 * @param inputFileName	 The URL of the ASCII file from which import will happen
	 * @exception Exception on error 
	 */
	public Import(String inputFileName, String columnDelimiter,
					  String characterDelimiter,  String codeset, 
					  int noOfColumnsExpected) throws SQLException 
	{

		try{
			this.inputFileName = inputFileName;
			this.noOfColumnsExpected = noOfColumnsExpected;
			controlFileReader = new ControlInfo();
			controlFileReader.setControlProperties(characterDelimiter,
												   columnDelimiter, codeset);
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
	 * @param con	 The Cloudscape database connection URL for the database containing the table
	 * @param schemaName	The name of the schema where table to import exists 
	 * @param tableName     Name of the Table the data has to be imported to.
	 * @param inputFileName Name of the file from which data has to be imported.
	 * @param colDelimiter  Delimiter that seperates columns in the file
	 * @param characterDelimiter  Delimiter that is used to quiote non-numeric types
	 * @param codeset           Codeset of the data in the file
	 * @param replace          Indicates whether the data in table has to be replaced or
	 *                         appended.(0 - append , > 0 Replace the data)
 	 * @exception SQL Exception on errors
	 */

	public static void importTable(Connection connection, String schemaName, 
                              String tableName, String inputFileName,  
							  String columnDelimiter, String characterDelimiter,
							  String codeset, short replace)
		throws SQLException {


		performImport(connection,  schemaName,  null, //No columnList 
					  null , //No column indexes
					  tableName, inputFileName, columnDelimiter, 
					  characterDelimiter, codeset, replace);
	}



		
	/**
	 * SYSCS_IMPORT_DATA  system Procedure from ij or from a Java application
	 * invokes  this method to perform import to a table from a file.
	 * @param con	 The Cloudscape database connection URL for the database containing the table
	 * @param schemaName	The name of the schema where table to import exists 
	 * @param tableName     Name of the Table the data has to be imported to.
	 * @param insertColumnList  Comma Seperated column name list to which data
	 *                          has to be imported from file.eg: 'c2,c2,'c3'.
	 * @param columnIndexes     Comma sepearted Lit Index of the columns in the file(first column
	                             starts at 1). eg: '3 ,4 , 5'
	 * @param inputFileName Name of the file from which data has to be imported.
	 * @param colDelimiter  Delimiter that seperates columns in the file
	 * @param characterDelimiter  Delimiter that is used to quiote non-numeric types
	 * @param codeset           Codeset of the data in the file
	 * @param replace          Indicates whether the data in table has to be replaced or
	 *                         appended.(0 - append , > 0 Replace the data)
 	 * @exception SQL Exception on errors
	 */
	public static void importData(Connection connection, String schemaName,
								  String tableName, String insertColumnList, 
								  String columnIndexes,
								  String inputFileName, String columnDelimiter, 
								  String characterDelimiter,
								  String codeset, short replace)
		throws SQLException 
	{
		

			performImport(connection,  schemaName,  insertColumnList,columnIndexes, 
						  tableName, inputFileName, columnDelimiter, 
						  characterDelimiter, codeset, replace);
	}


	/*
	 * This function creates and executes  SQL Insert statement that performs the 
	 * the import using VTI. 
	 * eg:
	 * insert into T1 select  (cast column1 as DECIMAL), (cast column2 as
	 * INTEGER)  from new org.apache.derby.impl.load.Import('extin/Tutor1.asc') as importvti;
	 *
	 */
	private static void performImport(Connection connection, String schemaName, 
									  String insertColumnList, String columnIndexes,
									  String tableName, 
									  String inputFileName,  String  columnDelimiter, 
									  String characterDelimiter, String codeset, 
									  short replace)
		throws SQLException 
	{

		if (connection == null)
			throw LoadError.connectionNull();
		
		String entityName = (schemaName == null ? tableName : schemaName + "." + tableName); 
		
		if (entityName == null)
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
		sb.append(" )") ;

		String importvti = sb.toString();


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
			" PROPERTIES insertMode=" + insertModeValue + 
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
		ips.executeUpdate();
		statement.close();
		ips.close();

	}


	/** virtual method from the abstract class
	 * @exception	Exception on error
	 */
	protected ImportReadData getImportReadData() throws Exception {
		return new ImportReadData(inputFileName, controlFileReader);
	}
}



















