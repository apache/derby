/*

   Derby - Class org.apache.derby.catalog.SystemProcedures

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.catalog;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;


import org.apache.derby.iapi.db.Factory;
import org.apache.derby.iapi.db.PropertyInfo;
import org.apache.derby.impl.load.Export;
import org.apache.derby.impl.load.Import;

import org.apache.derby.impl.sql.execute.JarDDL;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;


/**
	Some system built-in procedures, and help routines.  Now used for network server.
	These procedures are built-in to the SYSIBM schema which match the DB2 SYSIBM procedures.
	Currently information on those can be found at url: 
	ftp://ftp.software.ibm.com/ps/products/db2/info/vr8/pdf/letter/db2l2e80.pdf
*/
public class SystemProcedures  {


	private final static int SQL_BEST_ROWID = 1;
	private final static int SQL_ROWVER = 2;

	/**
	  Method used by Cloudscape Network Server to get localized message (original call
	  from jcc.

	  @param sqlcode	sqlcode, not used.
	  @param errmcLen	sqlerrmc length
	  @param sqlerrmc	sql error message tokens, variable part of error message (ie.,
						arguments) plus messageId, separated by separator.
	  @param sqlerrp	not used
	  @param errd0-5	not used
	  @param warn		not used
	  @param sqlState	5-char sql state
	  @param file		not used
	  @param localeStr	client locale in string
	  @param msg		OUTPUT parameter, localized error message
	  @param rc			OUTPUT parameter, return code -- 0 for success
	 */
	public static void SQLCAMESSAGE(int sqlcode, short errmcLen, String sqlerrmc,
										String sqlerrp, int errd0, int errd1, int errd2,
										int errd3, int errd4, int errd5, String warn,
										String sqlState, String file, String localeStr,
										String[] msg, int[] rc)
	{
		int numMessages = 1;
		byte[] b = {20, 20, 20};
		String errSeparator = new String(b);

		// Figure out if there are multiple exceptions in sqlerrmc. If so get each one
		// translated and append to make the final result.
		for (int index=0; ; numMessages++)
		{
			if (sqlerrmc.indexOf(errSeparator, index) == -1)
				break;
			index = sqlerrmc.indexOf(errSeparator, index) + errSeparator.length();
		}

		// Putting it here instead of prepareCall it directly is because inter-jar reference tool
		// cannot detect/resolve this otherwise
		if (numMessages == 1)
			MessageService.getLocalizedMessage(sqlcode, errmcLen, sqlerrmc, sqlerrp, errd0, errd1,
											errd2, errd3, errd4, errd5, warn, sqlState, file,
											localeStr, msg, rc);
		else
		{
			int startIdx=0, endIdx;
			String sqlError;
			String[] errMsg = new String[2];
			for (int i=0; i<numMessages; i++)
			{
				endIdx = sqlerrmc.indexOf(errSeparator, startIdx);
				if (i == numMessages-1)				// last error message
					sqlError = sqlerrmc.substring(startIdx);
				else sqlError = sqlerrmc.substring(startIdx, endIdx);

				if (i > 0)
				{
					/* Strip out the SQLState */
					sqlState = sqlError.substring(0, 5);
					sqlError = sqlError.substring(6);
					msg[0] += " SQLSTATE: " + sqlState + ": ";
				}

				MessageService.getLocalizedMessage(sqlcode, (short)sqlError.length(), sqlError,
											sqlerrp, errd0, errd1, errd2, errd3, errd4, errd5,
											warn, sqlState, file, localeStr, errMsg, rc);

				if (rc[0] == 0)			// success
				{
					if (i == 0)
						msg[0] = errMsg[0];
					else msg[0] += errMsg[0];	// append the new message
				}
				startIdx = endIdx + errSeparator.length();
			}
		}
	}

	/**
	 *  Get the DatabaseMetaData for the current connection for use in
	 *  mapping the jcc SYSIBM.* calls to the Cloudscape DatabaseMetaData methods 
	 *
	 *  @return The DatabaseMetaData object of the current connection
	 */
	private static DatabaseMetaData getDMD() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		return conn.getMetaData();
	}

	/**
	 *  Map SQLProcedures to EmbedDatabaseMetaData.getProcedures
	 *
	 *  @param resultset   output parameter, the resultset object 
	 *			containing the result of getProcedures
	 *  @param catalogName SYSIBM.SQLProcedures CatalogName varchar(128),
	 *  @param schemaName  SYSIBM.SQLProcedures SchemaName  varchar(128),
	 *  @param procName    SYSIBM.SQLProcedures ProcName    varchar(128),
	 *  @param options     SYSIBM.SQLProcedures Options     varchar(4000))
	 */
	public static void SQLPROCEDURES (String catalogName, String schemaName, String procName,
										String options, ResultSet[] rs) throws SQLException
	{
		rs[0] = getDMD().getProcedures(catalogName, schemaName, procName);
	}

	/**
	 *  Map SQLTables to EmbedDatabaseMetaData.getSchemas, getCatalogs, getTableTypes and getTables
	 *
	 *  @param resultset   output parameter, the resultset object 
	 *                     containing the result of the DatabaseMetaData calls
	 *  @param catalogName SYSIBM.SQLTables CatalogName varchar(128),
	 *  @param schemaName  SYSIBM.SQLTables SchemaName  varchar(128),
	 *  @param tableName   SYSIBM.SQLTables TableName   varchar(128),
	 *  @param tableType   SYSIBM.SQLTables TableType   varchar(4000))
	 *  @param options     SYSIBM.SQLTables Options     varchar(4000))
	 *			JCC overloads this method:
	 *  			If options contains the string 'GETSCHEMAS=1', call getSchemas
	 *  			If options contains the string 'GETCATALOGS=1', call getCatalogs
	 *  			If options contains the string 'GETTABLETYPES=1', call getTableTypes
	 *  			otherwise, call getTables
	 */
	public static void SQLTABLES (String catalogName, String schemaName, String tableName,
										String tableType, String options, ResultSet[] rs)
		throws SQLException
	{

		String optionValue = getOption("GETCATALOGS", options);
		if (optionValue != null && optionValue.trim().equals("1"))
		{
			rs[0] = getDMD().getCatalogs();
			return;
		}
		optionValue = getOption("GETTABLETYPES", options);
		if (optionValue != null && optionValue.trim().equals("1"))
		{
			rs[0] = getDMD().getTableTypes();
			return;
		}
		optionValue = getOption("GETSCHEMAS", options);
		if (optionValue != null && optionValue.trim().equals("1"))
		{
			rs[0] = getDMD().getSchemas();
			return;
		}
			 	

		String[] typeArray = null;
		if (tableType != null)
		{
			StringTokenizer st = new StringTokenizer(tableType,"',");
			typeArray = new String[st.countTokens()];
			int i = 0;

			while (st.hasMoreTokens()) 
			{
				typeArray[i] = st.nextToken();
				i++;
			}
		}
		rs[0] = getDMD().getTables(catalogName, schemaName, tableName, typeArray);
	}

	/**
	 *  Map SQLForeignKeys to EmbedDatabaseMetaData.getImportedKeys, getExportedKeys, and getCrossReference
	 *
	 *  @param resultset   output parameter, the resultset object 
	 *                     	containing the result of the DatabaseMetaData calls
	 *  @param pkCatalogName SYSIBM.SQLForeignKeys PKCatalogName varchar(128),
	 *  @param pkSchemaName  SYSIBM.SQLForeignKeys PKSchemaName  varchar(128),
	 *  @param pkTableName   SYSIBM.SQLForeignKeys PKTableName   varchar(128),
	 *  @param fkCatalogName SYSIBM.SQLForeignKeys FKCatalogName varchar(128),
	 *  @param fkSchemaName  SYSIBM.SQLForeignKeys FKSchemaName  varchar(128),
	 *  @param fkTableName   SYSIBM.SQLForeignKeys FKTableName   varchar(128),
	 *  @param options       SYSIBM.SQLForeignKeys Options       varchar(4000))
	 *  			 JCC overloads this method:
	 *  			 If options contains the string 'EXPORTEDKEY=1', call getImportedKeys
	 *  			 If options contains the string 'IMPORTEDKEY=1', call getExportedKeys
	 *  			 otherwise, call getCrossReference
	 */
	public static void SQLFOREIGNKEYS (String pkCatalogName, String pkSchemaName, String pkTableName,
										String fkCatalogName, String fkSchemaName, String fkTableName,
										String options, ResultSet[] rs)
		throws SQLException
	{

		String exportedKeyProp = getOption("EXPORTEDKEY", options);
		String importedKeyProp = getOption("IMPORTEDKEY", options);

		if (importedKeyProp != null && importedKeyProp.trim().equals("1"))
			rs[0] = getDMD().getImportedKeys(fkCatalogName,
										fkSchemaName,fkTableName);
		else if (exportedKeyProp != null && exportedKeyProp.trim().equals("1"))
			rs[0] = getDMD().getExportedKeys(pkCatalogName,
										pkSchemaName,pkTableName);
		else
			rs[0] = getDMD().getCrossReference (pkCatalogName,
										   pkSchemaName,
										   pkTableName,
										   fkCatalogName,
										   fkSchemaName,
										   fkTableName);
	}

	/**
	 *  Helper for SQLForeignKeys and SQLTables 
	 *
	 *  @return option	String containing the value for a given option 
	 *  @param  pattern 	String containing the option to search for
	 *  @param  options 	String containing the options to search through
	 */
	private static String getOption(String pattern, String options)
	{
		if (options == null)
			return null;
		int start = options.lastIndexOf(pattern);
		if (start < 0)  // not there
			return null;
		int valueStart = options.indexOf('=', start);
		if (valueStart < 0)  // invalid options string
			return null;
		int valueEnd = options.indexOf(';', valueStart);
		if (valueEnd < 0)  // last option
			return options.substring(valueStart + 1);
		else
			return options.substring(valueStart + 1, valueEnd);
	}


	/**
	 *  Map SQLProcedureCols to EmbedDatabaseMetaData.getProcedureColumns
	 *
	 *  @param resultset   output parameter, the resultset object containing 
	 *			the result of getProcedureColumns
	 *  @param catalogName SYSIBM.SQLProcedureCols CatalogName varchar(128),
	 *  @param schemaName  SYSIBM.SQLProcedureCols SchemaName  varchar(128),
	 *  @param procName    SYSIBM.SQLProcedureCols ProcName    varchar(128),
	 *  @param paramName   SYSIBM.SQLProcedureCols ParamName   varchar(128),
	 *  @param options     SYSIBM.SQLProcedureCols Options     varchar(4000))
	 */
	public static void SQLPROCEDURECOLS (String catalogName, String schemaName, String procName,
										String paramName, String options, ResultSet[] rs)
		throws SQLException
	{
		rs[0] = getDMD().getProcedureColumns(catalogName, schemaName, procName, paramName);
	}

	/**
	 *  Map SQLColumns to EmbedDatabaseMetaData.getColumns
	 *
	 *  @param resultset   output parameter, the resultset object 
	 *			containing the result of getProcedures
	 *  @param catalogName SYSIBM.SQLColumns CatalogName varchar(128),
	 *  @param schemaName  SYSIBM.SQLColumns SchemaName  varchar(128),
	 *  @param tableName   SYSIBM.SQLColumns TableName   varchar(128),
	 *  @param columnName  SYSIBM.SQLColumns ColumnName  varchar(128),
	 *  @param options     SYSIBM.SQLColumns Options     varchar(4000))
	 */
	public static void SQLCOLUMNS (String catalogName, String schemaName, String tableName,
										String columnName, String options, ResultSet[] rs)
		throws SQLException
	{
		rs[0] = getDMD().getColumns(catalogName, schemaName, tableName, columnName);
	}

	/**
	 *  Map SQLColPrivileges to EmbedDatabaseMetaData.getColumnPrivileges
	 *
	 *  @param resultset   output parameter, the resultset object 
	 *			containing the result of getColumnPrivileges
	 *  @param catalogName SYSIBM.SQLColPrivileges CatalogName varchar(128),
	 *  @param schemaName  SYSIBM.SQLColPrivileges SchemaName  varchar(128),
	 *  @param tableName   SYSIBM.SQLColPrivileges ProcName    varchar(128),
	 *  @param columnName  SYSIBM.SQLColPrivileges ColumnName  varchar(128),
	 *  @param options     SYSIBM.SQLColPrivileges Options     varchar(4000))
	 */
	public static void SQLCOLPRIVILEGES (String catalogName, String schemaName, String tableName,
										String columnName, String options, ResultSet[] rs)
		throws SQLException
	{
		rs[0] = getDMD().getColumnPrivileges(catalogName, schemaName, tableName, columnName);
	}

	/**
	 *  Map SQLTablePrivileges to EmbedDatabaseMetaData.getTablePrivileges
	 *
	 *  @param resultset   output parameter, the resultset object 
	 *			containing the result of getTablePrivileges
	 *  @param catalogName SYSIBM.SQLTablePrivileges CatalogName varchar(128),
	 *  @param schemaName  SYSIBM.SQLTablePrivileges SchemaName  varchar(128),
	 *  @param tableName   SYSIBM.SQLTablePrivileges ProcName    varchar(128),
	 *  @param options     SYSIBM.SQLTablePrivileges Options     varchar(4000))
	 */
	public static void SQLTABLEPRIVILEGES (String catalogName, String schemaName, String tableName,
										String options, ResultSet[] rs)
		throws SQLException
	{
		rs[0] = getDMD().getTablePrivileges(catalogName, schemaName, tableName);
	}

	/**
	 *  Map SQLPrimaryKeys to EmbedDatabaseMetaData.getPrimaryKeys
	 *
	 *  @param resultset   output parameter, the resultset object 
	 *			containing the result of getPrimaryKeys
	 *  @param catalogName SYSIBM.SQLPrimaryKeys CatalogName varchar(128),
	 *  @param schemaName  SYSIBM.SQLPrimaryKeys SchemaName  varchar(128),
	 *  @param tableName   SYSIBM.SQLPrimaryKeys TableName   varchar(128),
	 *  @param options     SYSIBM.SQLPrimaryKeys Options     varchar(4000))
	 */
	public static void SQLPRIMARYKEYS (String catalogName, String schemaName, String tableName, String options, ResultSet[] rs)
		throws SQLException
	{
		rs[0] = getDMD().getPrimaryKeys(catalogName, schemaName, tableName);
	}

	/**
	 *  Map SQLGetTypeInfo to EmbedDatabaseMetaData.getTypeInfo
	 *
	 *  @param resultset output parameter, the resultset object containing the result of getTypeInfo
	 *  @param datatType SYSIBM.SQLGetTypeInfo DataType smallint,
	 *  @param options   SYSIBM.SQLGetTypeInfo Options  varchar(4000))
	 */
	public static void SQLGETTYPEINFO (short dataType, String options, ResultSet[] rs)
		throws SQLException
	{
		rs[0] = getDMD().getTypeInfo();
	}

	/**
	 *  Map SQLStatistics to EmbedDatabaseMetaData.getIndexInfo
	 *
	 *  @param resultset   output parameter, the resultset object 
	 *			containing the result of getIndexInfo
	 *  @param catalogName SYSIBM.SQLStatistics CatalogName varchar(128),
	 *  @param schemaName  SYSIBM.SQLStatistics SchemaName  varchar(128),
	 *  @param tableName   SYSIBM.SQLStatistics TableName   varchar(128),
	 *  @param unique      SYSIBM.SQLStatistics Unique      smallint; 0=SQL_INDEX_UNIQUE(0); 1=SQL_INDEX_ALL(1),
	 *  @param approximate SYSIBM.SQLStatistics Approximate smallint; 1=true; 0=false,
	 *  @param options     SYSIBM.SQLStatistics Options     varchar(4000))
	 */
	public static void SQLSTATISTICS (String catalogName, String schemaName, String tableName,
										short unique, short approximate, String options, ResultSet[] rs)
		throws SQLException
	{
		boolean boolUnique = (unique == 0) ? true: false;
		boolean boolApproximate = (approximate == 1) ? true: false;
			
		rs[0] = getDMD().getIndexInfo(catalogName, schemaName, tableName, boolUnique, boolApproximate);
	}

	/**
	 *  Map SQLSpecialColumns to EmbedDatabaseMetaData.getBestRowIdentifier and getVersionColumns
	 *
	 *  @param resultset   output parameter, the resultset object 
	 *			containing the result of the DatabaseMetaData call
	 *  @param coltype     SYSIBM.SQLSpecialColumns ColType     smallint,
	 *			where 1 means getBestRowIdentifier and 2 getVersionColumns was called.
	 *  @param catalogName SYSIBM.SQLSpecialColumns CatalogName varchar(128),
	 *  @param schemaName  SYSIBM.SQLSpecialColumns SchemaName  varchar(128),
	 *  @param tableName   SYSIBM.SQLSpecialColumns TableName   varchar(128),
	 *  @param scope       SYSIBM.SQLSpecialColumns Scope       smallint,
	 *  @param nullable    SYSIBM.SQLSpecialColumns Nullable    smallint; 0=false, 1=true,
	 *  @param options     SYSIBM.SQLSpecialColumns Options     varchar(4000))
	 */
	public static void SQLSPECIALCOLUMNS (short colType, String catalogName, String schemaName, String tableName,
										short scope, short nullable, String options, ResultSet[] rs)
		throws SQLException
	{

		boolean boolNullable = (nullable == 1) ? true: false;
		if (colType == SQL_BEST_ROWID)
		{
			rs[0] = getDMD().getBestRowIdentifier(catalogName, schemaName, tableName, scope, boolNullable);
		}
		else // colType must be SQL_ROWVER
		{
			rs[0] = getDMD().getVersionColumns(catalogName, schemaName, tableName);
		}
	}

	/**
	 *  Map SQLUDTS to EmbedDatabaseMetaData.getUDTs
	 *
	 *  @param resultset       output parameter, the resultset object 
	 *				containing the result of getUDTs, which will be empty
	 *  @param catalogName     SYSIBM.SQLUDTS CatalogName          varchar(128),
	 *  @param schemaPattern   SYSIBM.SQLUDTS Schema_Name_Pattern  varchar(128),
	 *  @param typeNamePattern SYSIBM.SQLUDTS Type_Name_Pattern    varchar(128),
	 *  @param udtTypes        SYSIBM.SQLUDTS UDTTypes             varchar(128),
	 *  @param options         SYSIBM.SQLUDTS Options              varchar(4000))
	 */
	public static void SQLUDTS (String catalogName, String schemaPattern, String typeNamePattern,
										String udtTypes, String options, ResultSet[] rs)
		throws SQLException
	{

		int[] types = null;
		
		if( udtTypes != null && udtTypes.length() > 0)
		{
			StringTokenizer tokenizer = new StringTokenizer( udtTypes, " \t\n\t,");
			int udtTypeCount = tokenizer.countTokens();
			types = new int[ udtTypeCount];
			String udtType = "";
			try
			{
				for( int i = 0; i < udtTypeCount; i++)
				{
					udtType = tokenizer.nextToken();
					types[i] = Integer.parseInt( udtType);
				}
			}
			catch( NumberFormatException nfe)
			{
				throw new SQLException( "Invalid type, " + udtType + ", passed to getUDTs.");
			}
			catch( NoSuchElementException nsee)
			{
				throw new SQLException( "Internal failure: NoSuchElementException in getUDTs.");
			}
		}
		rs[0] = getDMD().getUDTs(catalogName, schemaPattern, typeNamePattern, types);
	}

	/*
	 *  Map SYSIBM.METADATA to appropriate EmbedDatabaseMetaData methods 
	 *  for now, using the sps in org.apache.derby.iapi.db.jdbc.datadictionary.metadata_net.properties
	 *
	 */
	public static void METADATA (ResultSet[] rs)
		throws SQLException
	{
		
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("execute statement SYSIBM.METADATA");
		rs[0] = ps.executeQuery();
		conn.close();
	}

    /**
     * Set/delete the value of a property of the database in current connection.
     * <p>
     * Will be called as SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY.
     *
     * @param key       The property key.
     * @param value     The new value, if null the property is deleted.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static void SYSCS_SET_DATABASE_PROPERTY(
    String  key,
    String  value)
        throws SQLException
    {
        PropertyInfo.setDatabaseProperty(key, value);
    }

    /**
     * Get the value of a property of the database in current connection.
     * <p>
     * Will be called as SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY.
     *
     * @param key       The property key.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static String SYSCS_GET_DATABASE_PROPERTY(
    String  key)
        throws SQLException
    {
        return(PropertyInfo.getDatabaseProperty(key));
    }

    /**
     * compress the table
     * <p>
     * Calls the "alter table compress {sequential}" sql.  This syntax
     * is not db2 compatible so it mapped by a system routine.  This
     * routine will be called when an application calls:
     *
     *     SYSCS_UTIL.SYSCS_COMPRESS_TABLE
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param schema        schema name of the table to compress.  Must be
     *                      non-null, no default is used.
     * @param tablename     table name of the table to compress.  Must be
     *                      non-null.
     * @param sequential    if non-zero then rebuild indexes sequentially,
     *                      if 0 then rebuild all indexes in parallel.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static void SYSCS_COMPRESS_TABLE(
    String  schema,
    String  tablename,
    int     sequential)
        throws SQLException
    {
        String query = 
            "alter table " + schema + "." + tablename + " compress" + 
            (sequential != 0 ? " sequential" : "");

		Connection conn = 
            DriverManager.getConnection("jdbc:default:connection");

		conn.prepareStatement(query).executeUpdate();

		conn.close();
    }

    /**
     * Freeze the database.
     * <p>
     * Call internal routine to freeze the database so that a backup
     * can be made.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static void SYSCS_FREEZE_DATABASE()
		throws SQLException
    {
        Factory.getDatabaseOfConnection().freeze();
    }

    /**
     * Unfreeze the database.
     * <p>
     * Call internal routine to unfreeze the database, which was "freezed"
     * by calling SYSCS_FREEZE_DATABASE().
     * can be made.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static void SYSCS_UNFREEZE_DATABASE()
		throws SQLException
    {
        Factory.getDatabaseOfConnection().unfreeze();
    }

    public static void SYSCS_CHECKPOINT_DATABASE()
		throws SQLException
    {
        Factory.getDatabaseOfConnection().checkpoint();
    }

    public static void SYSCS_BACKUP_DATABASE(
    String  backupDir)
		throws SQLException
    {
        Factory.getDatabaseOfConnection().backup(backupDir);
    }

    public static void SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
    String  backupDir,
    int     deleteOnlineArchivedLogFiles)
		throws SQLException
    {
        Factory.getDatabaseOfConnection().backupAndEnableLogArchiveMode(
            backupDir, 
            (deleteOnlineArchivedLogFiles != 0));
    }

    public static void SYSCS_DISABLE_LOG_ARCHIVE_MODE(
    int     deleteOnlineArchivedLogFiles)
		throws SQLException
    {
        Factory.getDatabaseOfConnection().disableLogArchiveMode(
            (deleteOnlineArchivedLogFiles != 0));
    }

    public static void SYSCS_SET_RUNTIMESTATISTICS(
    int     enable)
		throws SQLException
    {
		ConnectionUtil.getCurrentLCC().setRunTimeStatisticsMode(enable != 0 ? true : false);
    }

    public static void SYSCS_SET_STATISTICS_TIMING(
    int     enable)
		throws SQLException
    {
		ConnectionUtil.getCurrentLCC().setStatisticsTiming(enable != 0 ? true : false);
    }

    public static int SYSCS_CHECK_TABLE(
    String  schema,
    String  tablename)
		throws SQLException
    {
        boolean ret_val = 
            org.apache.derby.iapi.db.ConsistencyChecker.checkTable(
                schema, tablename);

        return(ret_val ? 1 : 0);
    }

    public static String SYSCS_GET_RUNTIMESTATISTICS()
		throws SQLException
    {

		Object rts = ConnectionUtil.getCurrentLCC().getRunTimeStatisticsObject();

		if (rts == null)
			return null;

		return rts.toString();

    }


	/*
	** SQLJ Procedures.
	*/

	/**
		Install a jar file in the database.

		SQLJ.INSTALL_JAR

		@param url URL of the jar file to be installed in the database.
		@param jar SQL name jar will be installed as.
		@param deploy Ignored.

		@exception SQLException Error installing jar file.
	*/
	public static void INSTALL_JAR(String url, String jar, int deploy)
		throws SQLException {

		try {

			String[] st = IdUtil.parseQualifiedName(jar.trim(), true);

			String schemaName = null;
			String sqlName = null;

			switch (st.length) {
			case 1:
				schemaName = null;
				sqlName = st[0];
				break;
			case 2:
				schemaName = st[0];
				sqlName = st[1];
			default:
				; // RESOLVE
			}

			checkJarSQLName(sqlName);
			JarDDL.add(schemaName, sqlName, url);
		} 
		catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	/**
		Replace a jar file in the database.

		SQLJ.REPLACE_JAR

		@param url URL of the jar file to be installed in the database.
		@param jar SQL name of jar to be replaced.

		@exception SQLException Error replacing jar file.
	*/
	public static void REPLACE_JAR(String url, String jar)
		throws SQLException {

		try {

			String[] st = IdUtil.parseQualifiedName(jar.trim(), true);

			String schemaName = null;
			String sqlName = null;

			switch (st.length) {
			case 1:
				schemaName = null;
				sqlName = st[0];
				break;
			case 2:
				schemaName = st[0];
				sqlName = st[1];
			default:
				; // RESOLVE
			}

			checkJarSQLName(sqlName);
			JarDDL.replace(schemaName, sqlName, url);
		} 
		catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}
	/**
		Remove a jar file from the database.

		@param jar SQL name of jar to be replaced.
		@param deploy Ignored.

		@exception SQLException Error removing jar file.
	*/
	public static void REMOVE_JAR(String jar, int undeploy)
		throws SQLException {

		try {

			String[] st = IdUtil.parseQualifiedName(jar.trim(), true);

			String schemaName = null;
			String sqlName = null;

			switch (st.length) {
			case 1:
				schemaName = null;
				sqlName = st[0];
				break;
			case 2:
				schemaName = st[0];
				sqlName = st[1];
			default:
				; // RESOLVE
			}

			checkJarSQLName(sqlName);

			JarDDL.drop(schemaName, sqlName);
		} 
		catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	private static void checkJarSQLName(String sqlName)
		throws StandardException {

		// weed out a few special cases that cause problems.
		if (   (sqlName.length() == 0)
			|| (sqlName.indexOf(':') != -1)
			) {

			throw StandardException.newException(SQLState.ID_PARSE_ERROR);
		}
	}

	/**
     * Export data from a table to given file.
     * <p>
     * Will be called by system procedure:
	 * SYSCS_EXPORT_TABLE(IN SCHEMANAME  VARCHAR(128), 
	 * IN TABLENAME    VARCHAR(128),  IN FILENAME VARCHAR(32672) , 
	 * IN COLUMNDELIMITER CHAR(1),  IN CHARACTERDELIMITER CHAR(1) ,  
	 * IN CODESET VARCHAR(128))
	 * @exception  StandardException  Standard exception policy.
     **/
	public static void SYSCS_EXPORT_TABLE(
	String  schemaName,
    String  tableName,
	String  fileName,
	String  columnDelimiter,
	String  characterDelimiter,
	String  codeset)
        throws SQLException
    {
		Connection conn = 
            DriverManager.getConnection("jdbc:default:connection");
		Export.exportTable(conn, schemaName , tableName , fileName ,
							  columnDelimiter , characterDelimiter, codeset);
		//export finished successfully, issue a commit 
		conn.commit();
	}

	
	/**
     * Export data from a  select statement to given file.
     * <p>
     * Will be called as 
	 * SYSCS_EXPORT_QUERY(IN SELECTSTATEMENT  VARCHAR(32672), 
	 * IN FILENAME VARCHAR(32672) , 
	 * IN COLUMNDELIMITER CHAR(1),  IN CHARACTERDELIMITER CHAR(1) ,  
	 * IN CODESET VARCHAR(128))
	 *
	 * @exception  StandardException  Standard exception policy.
     **/
	public static void SYSCS_EXPORT_QUERY(
    String  selectStatement,
	String  fileName,
	String  columnDelimiter,
	String  characterDelimiter,
	String  codeset)
        throws SQLException
    {
		Connection conn = 
            DriverManager.getConnection("jdbc:default:connection");
		Export.exportQuery(conn, selectStatement, fileName ,
							   columnDelimiter , characterDelimiter, codeset);
		
		//export finished successfully, issue a commit 
		conn.commit();
	}

	/**
     * Import  data from a given file to a table.
     * <p>
     * Will be called by system procedure as
	 * SYSCS_IMPORT_TABLE(IN SCHEMANAME  VARCHAR(128), 
	 * IN TABLENAME    VARCHAR(128),  IN FILENAME VARCHAR(32672) , 
	 * IN COLUMNDELIMITER CHAR(1),  IN CHARACTERDELIMITER CHAR(1) ,  
	 * IN CODESET VARCHAR(128), IN  REPLACE SMALLINT)
	 * @exception  StandardException  Standard exception policy.
     **/
	public static void SYSCS_IMPORT_TABLE(
	String  schemaName,
    String  tableName,
	String  fileName,
	String  columnDelimiter,
	String  characterDelimiter,
	String  codeset,
	short   replace)
        throws SQLException
    {
		Connection conn = 
            DriverManager.getConnection("jdbc:default:connection");
		try{
			Import.importTable(conn, schemaName , tableName , fileName ,
								   columnDelimiter , characterDelimiter, codeset, replace);
		}catch(SQLException se)
		{
			//issue a rollback on any errors
			conn.rollback();
			throw  se;
		}
		//import finished successfull, commit it.
		conn.commit();
	}


	/**
     * Import data from a given file into the specified table columns from the 
	 * specified columns in the file.
     * <p>
     * Will be called as 
	 * SYSCS_IMPORT_DATA (IN SCHEMANAME  VARCHAR(128), IN TABLENAME    VARCHAR(128),  
	 *                    IN INSERTCOLUMNLIST VARCHAR(32762), IN COLUMNINDEXES VARCHAR(32762),
	 *                    IN FILENAME VARCHAR(32762), IN COLUMNDELIMITER CHAR(1),  
	 *                    IN CHARACTERDELIMITER  CHAR(1) ,  IN CODESET VARCHAR(128) , 
     *                    IN  REPLACE SMALLINT)
	 *
	 * @exception  StandardException  Standard exception policy.
     **/
	public static void SYSCS_IMPORT_DATA(
    String  schemaName,
    String  tableName,
	String  insertColumnList,
	String  columnIndexes,
	String  fileName,
	String  columnDelimiter,
	String  characterDelimiter,
	String  codeset,
	short   replace)
        throws SQLException
    {
		Connection conn = 
            DriverManager.getConnection("jdbc:default:connection");
		try{
			Import.importData(conn, schemaName , tableName ,
								  insertColumnList, columnIndexes, fileName,
								  columnDelimiter, characterDelimiter, 
								  codeset, replace);
		}catch(SQLException se)
		{
			//issue a rollback on any errors
			conn.rollback();
			throw  se;
		}

		//import finished successfull, commit it.
		conn.commit();
	}

	/**
     * Perform bulk insert using the specificed vti .
     * <p>
     * Will be called as 
	 * SYSCS_BULK_INSERT (IN SCHEMANAME VARCHAR(128), IN TABLENAME    VARCHAR(128),  
	 *                    IN VTINAME VARCHAR(32762), IN VTIARG VARCHAR(32762))
	 *
	 * @exception  StandardException  Standard exception policy.
     **/
	public static void SYSCS_BULK_INSERT(
    String  schemaName,
    String  tableName,
	String  vtiName,
	String  vtiArg
	)
        throws SQLException
    {
		Connection conn = 
            DriverManager.getConnection("jdbc:default:connection");
		
		String entityName = (schemaName == null ? tableName : schemaName + "." + tableName); 
		String binsertSql = 
			"insert into " + entityName +
			" PROPERTIES insertMode=bulkInsert " +
			"select * from new " + vtiName + 
			"(" + 
			"'" + schemaName + "'" + ", " + 
			"'" + tableName + "'" +  ", " + 
			"'" + vtiArg + "'" +  ")" + 
			" as t"; 

		PreparedStatement ps = conn.prepareStatement(binsertSql);
		ps.executeUpdate();
		ps.close();
	}
	
}











