/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.load
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.load;

import java.sql.Connection;
import java.sql.ResultSet;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;    

/**
 * This class provides ways to export data from
 * a table or a view into a file. Export functions provided 
 * in this  class are called through Systement Procedures.  
 */
public class Export extends ExportAbstract{

	private String outputFileName;


	private void doExport() throws SQLException
	{
		try {
			if (entityName == null && selectStatement == null)
				throw LoadError.entityNameMissing();
			
			if (outputFileName == null)
				throw LoadError.dataFileNull();
			try {
				doAllTheWork();
			} catch (IOException iex) {
				//in case of ioexception, catch it and throw it as our own exception
				throw LoadError.errorWritingData();
			}
		} catch (Exception ex) {
			throw LoadError.unexpectedError(ex);
		}

	}
	
	private Export(Connection con, String schemaName , 
					   String tableName, String selectStatement ,
					   String outputFileName, String characterDelimeter,
					   String columnDelimeter, String codeset)
		throws SQLException{
		this.con = con;
		this.schemaName = schemaName;
		this.entityName = tableName;
		this.selectStatement = selectStatement;
		this.outputFileName = outputFileName;
		try{
			controlFileReader = new ControlInfo();
			controlFileReader.setControlProperties(characterDelimeter,
												   columnDelimeter, codeset);
		}catch(Exception ex)
		{
			throw LoadError.unexpectedError(ex);
		}
	}

	/**
	 * SYSCS_EXPORT_TABLE  system Procedure from ij or from a Java application
	 * invokes  this method to perform export of  a table data to a file.
	 * @param con	 The Cloudscape database connection URL for the database containing the table
	 * @param schemaName	schema name of the table data is being exported from
	 * @param tableName     Name of the Table from which  data has to be exported.
	 * @param outputFileName Name of the file to  which data has to be exported.
	 * @param colDelimiter  Delimiter that seperates columns in the output file
	 * @param characterDelimiter  Delimiter that is used to quoate non-numeric types
	 * @param codeset           Codeset that should be used to write  the data to  the file
 	 * @exception SQL Exception on errors
	 */

	public static void exportTable(Connection con, String schemaName, 
                              String tableName, String outputFileName,  
							  String columnDelimeter, String characterDelimeter,
							  String codeset)
		throws SQLException {
		
		Export fex = new Export(con, schemaName, tableName, null,
										outputFileName,	characterDelimeter,   
										columnDelimeter, codeset);

		fex.doExport();
	}

	
	/**
	 * SYSCS_EXPORT_QUERY  system Procedure from ij or from a Java application
	 * invokes  this method to perform export of the data retrieved by select statement to a file.
	 * @param con	 The Cloudscape database connection URL for the database containing the table
	 * @param selectStatement    select query that is used to export the data
	 * @param outputFileName Name of the file to  which data has to be exported.
	 * @param colDelimiter  Delimiter that seperates columns in the output file
	 * @param characterDelimiter  Delimiter that is used to quiote non-numeric types
	 * @param codeset           Codeset that should be used to write  the data to  the file
 	 * @exception SQL Exception on errors
	 */
	public static void exportQuery(Connection con, String selectStatement,
							  String outputFileName, String columnDelimeter, 
							  String characterDelimeter, String codeset)
		throws SQLException {
		
		Export fex = new Export(con, null, null, selectStatement,
										outputFileName,characterDelimeter,
										columnDelimeter, codeset);
		fex.doExport();
	}


	/**
	 * For internal use only
	 * @exception	Exception if there is an error
	 */
	//returns the control file reader corresponding to the control file passed
	protected ExportWriteDataAbstract getExportWriteData() throws Exception {
		return new ExportWriteData(outputFileName, controlFileReader);
	}
}






