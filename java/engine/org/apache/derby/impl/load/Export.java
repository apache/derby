/*

   Derby - Class org.apache.derby.impl.load.Export

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

import java.sql.Connection;
import java.io.IOException;
import java.sql.SQLException;
import java.io.File;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FileUtil;


/**
 * This class provides ways to export data from
 * a table or a view into a file. Export functions provided 
 * in this  class are called through Systement Procedures.  
 */
public class Export extends ExportAbstract{

	private String outputFileName;
	/* Name of the file to  which large object data has to be exported */
	private String lobsFileName;
          
	private void doExport() throws SQLException
	{
		try {
			if (entityName == null && selectStatement == null)
				throw LoadError.entityNameMissing();
			
			if (outputFileName == null)
				throw LoadError.dataFileNull();
                        else
                        {
                            if (dataFileExists(outputFileName))
                                throw LoadError.dataFileExists(outputFileName);
                        }

			if (lobsFileName != null && lobsFileExists(lobsFileName))
                                throw LoadError.lobsFileExists(lobsFileName);
                        
			try {
				doAllTheWork();
			} catch (IOException iex) {
				//in case of ioexception, catch it and throw it as our own exception
				throw LoadError.errorWritingData(iex);
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
     * Set the file name to which larg object data has to be exported, and 
     * also set flag to indicate that large objects are exported to a 
     * different file. 
     * @param lobsFileName  the file to to which lob data has to be exported.
     * @exception SQLException  if file name is null. 
     */
	private void setLobsExtFileName(String lobsFileName) throws SQLException
	{
		if (lobsFileName == null) {
            throw PublicAPI.wrapStandardException(
                      StandardException.newException(
                      SQLState.LOB_DATA_FILE_NULL));
        }

		this.lobsFileName = lobsFileName;
		lobsInExtFile = true;
	}

    /**
     * Checks whether the lobs file exists .
     * @param fileName  the file to to which lob data has to be exported.
     * @exception SQLException  if file name is null.	
     */
	private boolean lobsFileExists(String fileName) throws SQLException {

            if (fileName == null) {
            throw PublicAPI.wrapStandardException(
                      StandardException.newException(
		      SQLState.LOB_DATA_FILE_NULL));
            }
            fileName = FileUtil.stripProtocolFromFileName( fileName );
            File file = new File(fileName);

            return fileExists(file);

        }
    /**
     * Checks whether the data file exists .
     * @param fileName  the file to to which lob data has to be exported.
     * @exception SQLException  if file name is null.
     */ 
	private boolean dataFileExists(String fileName) throws SQLException {
            
            if (fileName == null) {
            throw PublicAPI.wrapStandardException(
                      StandardException.newException(
                      SQLState.DATA_FILE_NULL));
            }
            fileName = FileUtil.stripProtocolFromFileName( fileName );
            File file = new File(fileName);

            return fileExists(file);
        }

    /**
     * Checks if the specified file exists.
     *
     * @param file the file to check
     * @return {@code true} if the file exists, {@code false} if not.
     * @throws SecurityException if the required privileges are missing
     */
    private final boolean fileExists(final File file) {
        return ((Boolean)AccessController.doPrivileged(
                new PrivilegedAction() {
                    public Object run() {
                        return new Boolean(file.exists());
                    }
            })).booleanValue();
    }

	/**
	 * SYSCS_EXPORT_TABLE  system Procedure from ij or from a Java application
	 * invokes  this method to perform export of  a table data to a file.
	 * @param con	 The Derby database connection URL for the database containing the table
	 * @param schemaName	schema name of the table data is being exported from
	 * @param tableName     Name of the Table from which  data has to be exported.
	 * @param outputFileName Name of the file to  which data has to be exported.
	 * @param columnDelimeter  Delimiter that seperates columns in the output file
	 * @param characterDelimeter  Delimiter that is used to quoate non-numeric types
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
     * SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE system procedure from ij 
     * or from a Java application invokes  this method to perform 
     * export of a table data to a file. Large object data is exported 
     * to a different file and the reference to it is stored in the
     *  main output file. 
     * @param con	 The Derby database connection URL for the 
     *               database containing the table
     * @param schemaName  schema name of the table data is being exported from
     * @param tableName   Name of the Table from which data has to be exported.
     * @param outputFileName  Name of the file to which data has to be exported.
     * @param columnDelimeter  Delimiter that seperates columns 
     *                         in the output file.
     * @param characterDelimeter Delimiter that is used to quote 
     *                           non-numeric types.
     * @param codeset            Codeset that should be used to 
     *                           write the data to the file/
     * @param lobsFileName       Name of the file to which large object 
     *                           data has to be exported.
     * @exception SQL Exception on errors
     */

    public static void exportTable(Connection con, String schemaName, 
                                   String tableName, String outputFileName,  
                                   String columnDelimeter, String characterDelimeter,
                                   String codeset, String lobsFileName)
        throws SQLException {

        Export fex = new Export(con, schemaName, tableName, null,
                                outputFileName,	characterDelimeter,   
                                columnDelimeter, codeset);
        fex.setLobsExtFileName(lobsFileName);
        fex.doExport();
    }


	
	/**
	 * SYSCS_EXPORT_QUERY  system Procedure from ij or from a Java application
	 * invokes  this method to perform export of the data retrieved by select statement to a file.
	 * @param con	 The Derby database connection URL for the database containing the table
	 * @param selectStatement    select query that is used to export the data
	 * @param outputFileName Name of the file to  which data has to be exported.
	 * @param columnDelimeter  Delimiter that seperates columns in the output file
	 * @param characterDelimeter  Delimiter that is used to quiote non-numeric types
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
     * SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE system Procedure from ij 
     * or from a Java application invokes this method to perform 
     * export of the data retrieved by select  statement to a file.
     * Large object data is exported to a different file  and the reference 
     * to it is stored in the main output file. 
     * @param con	 The Derby database connection URL for 
     *               the database containing the table
     * @param selectStatement    select query that is used to export the data
     * @param outputFileName Name of the file to  which data has to be exported.
     * @param columnDelimeter  Delimiter that seperates columns in 
     *                         the output file
     * @param characterDelimeter  Delimiter that is used to quote 
     *                            non-numeric types
     * @param codeset Codeset that should be used to write the data to the file
     * @param lobsFileName Name of the file to which 
     *                     large object data has to be exported.
     * @exception SQL Exception on errors
     */
    public static void exportQuery(Connection con, String selectStatement,
                                   String outputFileName, String columnDelimeter, 
                                   String characterDelimeter, String codeset, 
                                   String lobsFileName)
        throws SQLException {
		
        Export fex = new Export(con, null, null, selectStatement,
                                outputFileName,characterDelimeter,
                                columnDelimeter, codeset);
        fex.setLobsExtFileName(lobsFileName);
        fex.doExport();
    }


	/**
	 * For internal use only
	 * @exception	Exception if there is an error
	 */
	//returns the control file reader corresponding to the control file passed
	protected ExportWriteDataAbstract getExportWriteData() throws Exception {
		if (lobsInExtFile) 
			return new ExportWriteData(outputFileName, 
									   lobsFileName,
									   controlFileReader);
		else 
			return new ExportWriteData(outputFileName,
									   controlFileReader);
	}
}






