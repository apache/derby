/*

   Derby - Class org.apache.derby.tools.dblook

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

package org.apache.derby.tools;

import java.io.BufferedReader;
import java.io.StringReader;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Timestamp;

import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.derby.iapi.tools.i18n.LocalizedResource;

import org.apache.derby.impl.tools.dblook.DB_Check;
import org.apache.derby.impl.tools.dblook.DB_Index;
import org.apache.derby.impl.tools.dblook.DB_Jar;
import org.apache.derby.impl.tools.dblook.DB_Key;
import org.apache.derby.impl.tools.dblook.DB_Table;
import org.apache.derby.impl.tools.dblook.DB_Schema;
import org.apache.derby.impl.tools.dblook.DB_StoredProcedure;
import org.apache.derby.impl.tools.dblook.DB_Trigger;
import org.apache.derby.impl.tools.dblook.DB_View;
import org.apache.derby.impl.tools.dblook.Logs;

public class dblook {

	// DB2 enforces a maximum of 30 tables to be specified as part of
	// the table list.
	public static final int DB2_MAX_NUMBER_OF_TABLES = 30;

	private Connection conn;
	private static PreparedStatement getColNameFromNumberQuery;

	// Mappings from id to name for schemas and tables (for ease
	// of reference).
	protected static HashMap schemaMap;
	protected static HashMap tableIdToNameMap;

	// Command-line Parameters.
	protected static String sourceDBUrl;
	protected static String ddlFileName;
	protected static String stmtDelimiter;
	protected static boolean appendLogs;
	protected static ArrayList tableList;
	protected static String schemaParam;
	protected static String targetSchema;
	protected static boolean skipViews;
	protected static boolean verbose;
	private static String sourceDBName;

	private static String lookLogName = "dblook.log";

	private final static String DEFAULT_LOCALE= "en";
	private final static String DEFAULT_LOCALE_COUNTRY="US";
	private static LocalizedResource langUtil;

	/* ************************************************
	 * main:
	 * Initialize program state by creating a dblook object,
	 * and then start the DDL generation by calling "go".
	 * ****/

	public static void main(String[] args) {

		dblook looker = new dblook(args);
		try {
			looker.go(sourceDBUrl, sourceDBName);
		} catch (Exception e) {
		// Errors are logged and printed to console according
		// to command line arguments, so just ignore here.
		}

	}

	/* ************************************************
	 * Constructor:
	 * Parse the command line, initialize logs, echo program variables,
	 * and load the Derby driver.
	 * @param args args[0] is the database URL.  All other command-line
	 *  parameters are read as system properties.
	 * ****/

	public dblook(String[] args) {

        // Adjust the application in accordance with derby.ui.locale
		// and derby.ui.codeset
		langUtil = LocalizedResource.getInstance();

		// Initialize class variables.
		initState();

		// Parse the command line.
		if (!parseArgs(args)) {
			System.out.println(lookupMessage("CSLOOK_Usage"));
			System.exit(1);
		}

		showVariables();

		if (!loadDriver()) {
		// Failed when loading the driver.  We already printed
		// the exception, so just return.
			return;
		}

		schemaMap = new HashMap();
		tableIdToNameMap = new HashMap();

	}

	/* ************************************************
	 * initState:
	 * Initialize class variables.
	 ****/

	private void initState() {

		sourceDBUrl = null;
		ddlFileName = null;
		stmtDelimiter = null;
		appendLogs = false;
		tableList = null;
		targetSchema = null;
		schemaParam = null;
		skipViews = false;
		verbose= false;
		sourceDBName = null;
		return;

	}

	/* ************************************************
	 * parseArgs:
	 * Parse the command-line arguments.  There is only one
	 * actual argument (database url); the rest of the parameters
	 * are read in as System properties.
	 * @param args args[0] is the url for the source database.
	 * @return true if all parameters were loaded and the output
	 *  files were successfully created; false otherwise.
	 ****/

	private boolean parseArgs(String[] args) {

		if (args.length < 2)
		// must have minimum of 2 args: "-d" and "<dbUrl>".
			return false;

		int st = 0;
		for (int i = 0; i < args.length; i++) {
			st = loadParam(args, i);
			if (st == -1)
				return false;
			i = st;
		}

		if (sourceDBUrl == null) {
		// must have at least a database url.
			return false;	
		}

		// At this point, all parameters should have been read into
		// their respective class variables.  Use those
		// variables for some further processing.

		// Setup logs.
		boolean okay = Logs.initLogs(lookLogName, ddlFileName, appendLogs,
		 	verbose, (stmtDelimiter == null ? ";" : stmtDelimiter));

		// Get database name.
		sourceDBName = extractDBNameFromUrl(sourceDBUrl);

		// Set up schema restriction.
		if ((schemaParam != null) && (schemaParam.length() > 0) &&
			(schemaParam.charAt(0) != '"'))
		// not quoted, so upper case, then add quotes.
		{
			targetSchema = addQuotes(expandDoubleQuotes(
				schemaParam.toUpperCase(java.util.Locale.ENGLISH)));
		}
		else
			targetSchema = addQuotes(expandDoubleQuotes(stripQuotes(schemaParam)));
		return okay;

	}

	/* ************************************************
	 * loadParam:
	 * Read in a flag and its corresponding values from
	 * list of command line arguments, starting at
	 * the start'th argument.
	 * @return The position of the argument that was
	 *  most recently processed.
	 ****/

	private int loadParam(String [] args, int start) {

		if ((args[start].length() == 0) || args[start].charAt(0) != '-')
		// starting argument should be a flag; if it's
		// not, ignore it.
			return start;

		boolean haveVal = (args.length > start + 1);
		switch (args[start].charAt(1)) {

			case 'd':
				if (!haveVal)
					return -1;
				if (args[start].length() == 2) {
					sourceDBUrl = stripQuotes(args[++start]);
					return start;
				}
				return -1;

			case 'z':
				if (!haveVal)
					return -1;
				if (args[start].length() == 2) {
					schemaParam = args[++start];
					return start;
				}
				return -1;

			case 't':
				if (!haveVal)
					return -1;
				if (args[start].equals("-td")) {
					stmtDelimiter = args[++start];
					return start;
				}
				else if (args[start].equals("-t"))
				// list of tables.
					return extractTableNamesFromList(args, start+1);
				return -1;
			case 'o':
				if (!haveVal)
					return -1;
				if ((args[start].length() == 2) && (args[start+1].length() > 0)){
					ddlFileName = args[++start];
					return start;
				}
				return -1;

			case 'a':
				if (args[start].equals("-append")) {
					appendLogs = true;
					return start;
				}
				return -1;

			case 'n':
				if (args[start].equals("-noview")) {
					skipViews = true;
					return start;
				}
				return -1;

			case 'v':
				if (args[start].equals("-verbose")) {
					verbose = true;
					return start;
				}
				return -1;

			default:
				return -1;

		}

	}

	/* ************************************************
	 * loadDriver:
	 * Load derby driver.
	 * @param precondition sourceDBUrl has been loaded.
	 * @return false if anything goes wrong; true otherwise.
	 ****/

	private boolean loadDriver() {

		String derbyDriver = System.getProperty("driver");
		if (derbyDriver == null) {
			if (sourceDBUrl.indexOf(":net://") != -1)
				derbyDriver = "com.ibm.db2.jcc.DB2Driver";
			else
				derbyDriver = "org.apache.derby.jdbc.EmbeddedDriver";
	    }

		try {
			Class.forName(derbyDriver).newInstance();
	    }
		catch (Exception e)
		{
			Logs.debug(e);
			return false;
		}

		return true;
	}

	/* ************************************************
	 * extractDBNameFromUrl:
	 * Given a database url, parse out the actual name
	 * of the database.  This is required for creation
	 * the DB2JJARS directory (the database name is part
	 * of the path to the jar).
	 * @param dbUrl The database url from which to extract the
	 *  the database name.
	 * @return the name of the database (including its
	 *  path, if provided) that is referenced by the url.
	 ****/

	protected String extractDBNameFromUrl(String dbUrl) {

		if (dbUrl == null)
		// shouldn't happen; ignore it here, as an error
		// will be thrown we try to connect.
			return "";

		int start = dbUrl.indexOf("jdbc:derby:");
		if (start == -1)
		// not a valid url; just ignore it (an error
		// will be thrown when we try to connect).
			return "";

		start = dbUrl.indexOf("net://");
		if (start == -1)
		// standard url (jdbc:derby:<dbname>).  Database
		// name starts right after "derby:".  The "6" in
		// the following line is the length of "derby:".
			start = dbUrl.indexOf("derby:") + 6;
		else
		// Network Server url.  Database name starts right
		// after next slash (":net://hostname:port/<dbname>).
		// The "6" in the following line is the length of
		// "net://".
			start = dbUrl.indexOf("/", start+6) + 1;

		int stop = -1;
		if (dbUrl.charAt(start) == '"') {
		// database name is quoted; end of the name is the
		// closing quote.
			start++;
			stop = dbUrl.indexOf("\"", start);
		}
		else {
		// Database name ends with the start of a list of connection	
		// attributes.  This list can begin with either a colon
		// or a semi-colon.
			stop = dbUrl.indexOf(":", start);
			if (stop != -1) {
				if ((dbUrl.charAt(stop+1) == '/') ||
						(dbUrl.charAt(stop+1) == '\\'))
				// then this colon is part of the path (ex. "C:"),
				// so ignore it.
					stop = dbUrl.indexOf(":", stop+2);
			}
			int stop2 = dbUrl.length();
			if (stop == -1)
			// no colons; see if we can find a semi-colon.
				stop = dbUrl.indexOf(";", start);
			else
				stop2 = dbUrl.indexOf(";", start);
			stop = (stop <= stop2 ? stop : stop2);
		}

		if (stop == -1)
		// we have a url that ends with database name (no
		// other attributes appended).
			stop = dbUrl.length();

		return dbUrl.substring(start, stop);

	}

	/* ************************************************
	 * extractTableNamesFromList:
	 * Given an array of command line arguments containing
	 * a list of table names beginning at start'th position,
	 * read the list of table names and store them as
	 * our target table list.  Names without quotes are
	 * turned into ALL CAPS and then double quotes are
	 * added; names whcih already have double quotes are
	 * stored exactly as they are. NOTE: DB2 enforces
	 * maximum of 30 tables, and ignores the rest; so
	 * do we.
	 * @param args Array of command line arguments.
	 * @start Position of the start of the list of tables
	 *  with the args array.
	 * @return The position of the last table name in
	 *  the list of table names.
	 ****/

	private int extractTableNamesFromList(String [] args,
		int start)
	{

		int argIndex = start;
		int count = 0;
		tableList = new ArrayList();
		while (argIndex < args.length) {

			if (((args[argIndex].length() > 0) && (args[argIndex].charAt(0) == '-')) ||
				(++count > DB2_MAX_NUMBER_OF_TABLES))
			// we're done with the table list.
				break;

			if ((args[argIndex].length() > 0) && (args[argIndex].charAt(0) == '"'))
			// it's quoted.
				tableList.add(addQuotes(expandDoubleQuotes(
					stripQuotes(args[argIndex++]))));
			else
			// not quoted, so make it all caps, then add
			// quotes.
				tableList.add(addQuotes(
					expandDoubleQuotes(args[argIndex++].toUpperCase(
					java.util.Locale.ENGLISH))));

		}

		if (tableList.size() == 0)
			tableList = null;

		return argIndex - 1;

	}

	/* ************************************************
	 * showVariables:
	 * Echo primary variables to output, so user can see
	 * what s/he specified.
	 ****/

	private void showVariables() {

		if (ddlFileName != null) {
			Logs.reportString("============================\n");
			Logs.reportMessage("CSLOOK_FileCreation");
			if (verbose)
				writeVerboseOutput("CSLOOK_OutputLocation",
					ddlFileName);
		}

		Logs.reportMessage("CSLOOK_Timestamp",
			new Timestamp(System.currentTimeMillis()).toString());
		Logs.reportMessage("CSLOOK_DBName", sourceDBName);
		Logs.reportMessage("CSLOOK_DBUrl", sourceDBUrl);
		if (tableList != null)
			Logs.reportMessage("CSLOOK_TargetTables");
		if (schemaParam != null)
			Logs.reportMessage("CSLOOK_TargetSchema", stripQuotes(schemaParam));
		Logs.reportString("appendLogs: " + appendLogs + "\n");
		return;

	}

	/* ************************************************
	 * go:
	 * Connect to the source database, prepare statements,
	 * and load a list of table id-to-name mappings.  Then,
	 * generate the DDL for the various objects in the
	 * database by making calls to static methods of helper
	 * classes (one helper class for each type of database
	 * object).  If a particular object type should not be
	 * generated (because of the user-specified command-
	 * line), then we enforce that here.
	 * @precondition all user-specified parameters have
	 *  been loaded.
	 * @param srcUrl The full url of the database, as obtained
	 *  from parseArgs().
	 * @param srcName The name of the database (as opposed to
	 *  the URL), as obtained from parseArgs().  This is
	 *  needed for locating any jar files that might'
	 *  exist in the source database.
	 * @return DDL for the source database has been
	 *  generated and printed to output, subject to
	 *  user-specified restrictions.
	 * ****/

	public void go(String srcUrl, String srcName)
		throws Exception
	{

		try
		{

			// Connect to the database, prepare statements,
			// and load id-to-name mappings.
			this.conn = DriverManager.getConnection(srcUrl);
			try {
				prepForDump();
			} catch (SQLException sqlE) {
				Logs.debug(sqlE);
				Logs.debug(Logs.unRollExceptions(sqlE), (String)null);
				Logs.cleanup();
				return;
			}
			catch (Exception e) {
				Logs.debug(e);
				Logs.cleanup();
				return;
			}

			// Generate DDL.

			// Start with schemas, since we might need them to
			// exist for jars to load properly.
			DB_Schema.doSchemas(this.conn,
				(tableList != null) && (targetSchema == null));

			if (tableList == null) {
			// Don't do these if user just wants table-related objects.
				DB_Jar.doJars(srcName, this.conn);
				DB_StoredProcedure.doStoredProcedures(this.conn);
			}

			DB_Table.doTables(this.conn, tableIdToNameMap);
			DB_Index.doIndexes(this.conn);
			DB_Key.doKeys(this.conn);
			DB_Check.doChecks(this.conn);

			if (!skipViews)
				DB_View.doViews(this.conn);

			DB_Trigger.doTriggers(this.conn);

			// That's it; we're done.
			if (getColNameFromNumberQuery != null)
				getColNameFromNumberQuery.close();
			Logs.cleanup();

		}
		catch (SQLException sqlE)
		{
			Logs.debug(sqlE);
			Logs.debug(Logs.unRollExceptions(sqlE), (String)null);
			Logs.cleanup();
			throw sqlE;
		}
		catch (Exception e)
		{
			Logs.debug(e);
			Logs.cleanup();
			throw e;
		}
		finally {
		// Close our connection.
			conn.commit();
			conn.close();
		}

	}

	/* ************************************************
	 * prepForDump:
	 * Prepare any useful statements (i.e. statements that
	 * are required by more than one helper class) and load
	 * the id-to-name mappings for the source database.
	 ****/

	private void prepForDump() throws Exception {

		// We're only SELECTing throughout all of this, so no need
		// to commit (plus, disabling commit makes it easier to
		// have multiple ResultSets open on the same connection).
		this.conn.setAutoCommit(false);

		// Prepare statements.
		getColNameFromNumberQuery = conn.prepareStatement(
			"SELECT COLUMNNAME FROM SYS.SYSCOLUMNS WHERE " +
			"REFERENCEID = ? AND COLUMNNUMBER = ?");

		// Load list of user tables and table ids, for general use.
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT T.TABLEID, T.TABLENAME, " +
				"S.SCHEMANAME FROM SYS.SYSTABLES T, SYS.SYSSCHEMAS S " + 
				"WHERE T.TABLETYPE = 'T' AND T.SCHEMAID = S.SCHEMAID");

		while (rs.next()) {
			String tableName = addQuotes(expandDoubleQuotes(rs.getString(2)));
			String schemaName = addQuotes(expandDoubleQuotes(rs.getString(3)));
			tableIdToNameMap.put(rs.getString(1), 
				schemaName + "." + tableName);
		}

		// Load schema id's and names.
		rs = stmt.executeQuery("SELECT SCHEMAID, SCHEMANAME FROM " +
			"SYS.SYSSCHEMAS");
		while (rs.next()) {
			schemaMap.put(rs.getString(1),
				addQuotes(expandDoubleQuotes(rs.getString(2))));
		}

		stmt.close();

		// Load default property values.
		return;

	}

	/* ************************************************
	 * getColumnListFromDescription:
	 * Takes string description of column numbers in the
	 * form of "(2, 1, 3...)" and the id of the table
	 * having those columns, and then returns a string
	 * with the column numbers replaced by their actual
	 * names ('2' is replaced with the 2nd column in the
	 * table, '1' with the first column, etc.).
	 * @param tableId the id of the table to which the column
	 *   numbers should be applied.
	 * @param description a string holding a list of column
	 *  numbers, enclosed in parentheses and separated
	 *  by commas.
	 * @return a new string with the column numbers in
	 *  'description' replaced by their column names;
	 *  also, the parentheses have been stripped off.
	 ****/

	public static String getColumnListFromDescription(String tableId,
		String description) throws SQLException
	{

		StringBuffer sb = new StringBuffer();
		StringTokenizer tokenizer = new StringTokenizer(
			description.substring(description.indexOf("(") + 1,
				description.lastIndexOf(")")), " ,", true);

		boolean firstCol = true;
		while (tokenizer.hasMoreTokens()) {

			String tok = tokenizer.nextToken().trim();
			if (tok.equals(""))
				continue;
			else if (tok.equals(",")) {
				firstCol = false;
				continue;
			}
			try {
				String colName = getColNameFromNumber(tableId,
					(Integer.valueOf(tok)).intValue());
				if (!firstCol)
					sb.append(", ");
				sb.append(colName);
			} catch (NumberFormatException e) {
			// not a number; could be "ASC" or "DESC" tag,
			// which is okay; otherwise, something's wrong.
				tok = tok.toUpperCase();
				if (tok.equals("DESC") || tok.equals("ASC"))
				// then this is okay; just add the token to result.
					sb.append(" " + tok);
				else
				// shouldn't happen.
					Logs.debug("INTERNAL ERROR: read a non-number (" +
						tok + ") when a column number was expected:\n" +
						description, (String)null);
			}

		}

		return sb.toString();

	}

	/* ************************************************
	 * getColNameFromNumber:
	 * Takes a tableid and a column number colNum, and
	 * returns the name of the colNum'th column in the
	 * table with tableid.
	 * @param tableid id of the table.
	 * @param colNum number of the column for which we want
	 *  the name.
	 * @return The name of the colNum'th column in the
	 *  table with tableid.
	 ****/

	public static String getColNameFromNumber(String tableId,
		int colNum) throws SQLException
	{

		getColNameFromNumberQuery.setString(1, tableId);
		getColNameFromNumberQuery.setInt(2, colNum);
		ResultSet rs = getColNameFromNumberQuery.executeQuery();

		if (!rs.next()) {
		// shouldn't happen.
			Logs.debug("INTERNAL ERROR: Failed column number " +
				"lookup for table " + lookupTableId(tableId) +
				", column " + colNum, (String)null);
			rs.close();
			return "";
		}
		else {
			String colName = addQuotes(expandDoubleQuotes(rs.getString(1)));
			rs.close();
			return colName;
		}

	}

	/* ************************************************
	 * addQuotes:
	 * Add quotes to the received object name, and return
	 * the result.
	 * @param name the name to which to add quotes.
	 * @return the name with double quotes around it.
	 ****/

	public static String addQuotes(String name) {

		if (name == null)
			return null;

		return "\"" + name + "\"";

	}

	/* ************************************************
	 * stripQuotes:
	 * Takes a name and, if the name is enclosed in
	 * quotes, strips the quotes off.  This method
	 * assumes that the received String either has no quotes,
	 * or has a quote (double or single) as the very first
	 * AND very last character.
	 * @param quotedName a name with quotes as the first
	 *  and last character, or else with no quotes at all.
	 * @return quotedName, without the quotes.
	 ****/

	public static String stripQuotes(String quotedName) {

		if (quotedName == null)
			return null;

		if ((quotedName.indexOf("\"") == -1) &&
			(quotedName.indexOf("'") == -1))
		// nothing to do.
			return quotedName;

		return quotedName.substring(1, quotedName.length() - 1);

	}

	/* ************************************************
	 * isExcludedTable:
	 * Takes a table name and determines whether or not
	 * the DDL for objects related to that table should be
	 * generated.
	 * @param tableName name of the table to check.
	 * @return true if 1) the user specified a table list
	 *  and that list does NOT include the received name; or
	 *  2) if the user specified a schema restriction and
	 *  the received name does NOT have that schema; false
	 *  otherwise.
	 ****/

	public static boolean isExcludedTable(String tableName) {

		if (tableName == null)
			return true;

		int dot = tableName.indexOf(".");
		if (dot != -1) {
		// strip off the schema part of the name, and see if we're
		// okay to use it.
			if (isIgnorableSchema(tableName.substring(0, dot)))
			// then we exclude this table.
				return true;
			tableName = tableName.substring(dot + 1,
				tableName.length());
		}

		return ((tableList != null) && !tableList.contains(tableName));

	}

	/* ************************************************
	 * Takes a schema name and determines whether or
	 * not the DDL for objects with that schema should
	 * be generated.
	 * @param schemaName schema name to be checked.
	 * @return true if 1) the user specified a target
	 *  schema and that target is NOT the same as the
	 *  received schema name, or 2) the schema is a
	 *  system schema (SYS, SYSVISUAL, or SYSIBM);
	 *  false otherwise;
	 ****/

    public static final String[] ignorableSchemaNames = {
        "SYSIBM",
        "SYS",
        "SYSVISUAL",
        "SYSCAT",
        "SYSFUN",
        "SYSPROC",
        "SYSSTAT",
        "NULLID",
        "SYSCS_ADMIN",
        "SYSCS_DIAG",
        "SYSCS_UTIL",
        "SQLJ"};

	public static boolean isIgnorableSchema(String schemaName) {

		if ((targetSchema != null) && (!schemaName.equals(targetSchema)))
			return true;

		schemaName = stripQuotes(schemaName);

        boolean ret = false;

        for (int i = ignorableSchemaNames.length - 1; i >= 0;)
        {
            if ((ret = ignorableSchemaNames[i--].equalsIgnoreCase(schemaName)))
                break;
        }

        return(ret);
	}

	/* ************************************************
	 * Takes a string and determines whether or not that
	 * string makes reference to any of the table names
	 * in the user-specified table list.
	 * @param str The string in which to search for table names.
	 * @return true if 1) the user didn't specify a
	 *  target table list, or 2) the received string
	 *  contains at least one of the table names in the
	 *  user-specified target list; false otherwise.
	 ****/

	public static boolean stringContainsTargetTable(String str) {

		if (str == null)
		// if the string is null, it can't possibly contain
		// any table names.
			return false;

		if (tableList == null)
		// if we have no target tables, then default to true.
			return true;

		int strLen = str.length();
		for (int i = 0; i < tableList.size(); i++) {

			String tableName = (String)tableList.get(i);
			tableName = expandDoubleQuotes(stripQuotes(tableName));
			int nameLen = tableName.length();
			String strCopy;
			if (tableName.equals(tableName.toUpperCase(
				java.util.Locale.ENGLISH)))
			// case doesn't matter.
				strCopy = str.toUpperCase();
			else
				strCopy = str;
			int pos = strCopy.indexOf(tableName);
			while (pos != -1) {

				// If we found it, make sure it's really a match.
				// First, see if it's part of another word.
				if (!partOfWord(str, pos, nameLen, strLen)) {

					// See if the match is in quotes--if so, then
					// it should match the table name's case.
					if ((pos >= 1) && (strCopy.charAt(pos-1) == '"') &&
					  (pos + nameLen < strCopy.length()) &&
					  (strCopy.charAt(pos+nameLen) == '"'))
					{ // match is quoted; check it's case.
						if (str.substring(pos,
							pos + nameLen).equals(tableName))
						// everything checks out.
							return true;
					}
					else
					// match isn't quoted, so we're okay as is.
						return true;
				}

				pos = str.indexOf(tableName, pos + nameLen);

			}
		}

		// If we get here, we didn't find it.
		return false;

	}

	/* ************************************************
	 * partOfWord:
	 * Returns true if the part of the string given by
	 * str.substring(pos, pos + nameLen) is part of
	 * another word.
	 * @param str The string in which we're looking.
	 * @param pos The position at which the substring in
	 *  question begins.
	 * @param nameLen the length of the substring in
	 *  question.
	 * @param strLen The length of the string in which
	 *  we're looking.
	 * @return true if the substring from pos to
	 *  pos+nameLen is part of larger word (i.e.
	 *  if it has a letter/digit immediately before
	 *  or after); false otherwise.
	 ****/

	private static boolean partOfWord (String str,
		int pos, int nameLen, int strLen)
	{

		boolean somethingBefore = false;
		if (pos > 0) {
			char c = str.charAt(pos-1);
			somethingBefore = ((c == '_') ||
				Character.isLetterOrDigit(c));
		}

		boolean somethingAfter = false;
		if (pos + nameLen < strLen) {
			char c = str.charAt(pos + nameLen);
			somethingAfter = ((c == '_') ||
				Character.isLetterOrDigit(c));
		}

		return (somethingBefore || somethingAfter);

	}

	/* ************************************************
	 * expandDoubleQuotes:
	 * If the received SQL id contains a quote, we have
	 * to expand it into TWO quotes so that it can be
	 * treated correctly at parse time.
	 * @param name Id that we want to print.
	 ****/

	public static String expandDoubleQuotes(String name) {

		if ((name == null) || (name.indexOf("\"") < 0))
		// nothing to do.
			return name;

		char [] cA = name.toCharArray();

		// Worst (and extremely unlikely) case is every 
		// character is a double quote, which means the
		// escaped string would need to be 2 times as long.
		char [] result = new char[2*cA.length];

		int j = 0;
		for (int i = 0; i < cA.length; i++) {

			if (cA[i] == '"') {
				result[j++] = '"';
				result[j++] = '"';
			}
			else
				result[j++] = cA[i];

		}

		return new String(result, 0, j);

	}

	/* ************************************************
	 * lookupSchemaId:
	 * Return the schema name corresponding to the
	 * received schema id.
	 * @param schemaId The id to look up.
	 * @return the schema name.
	 ****/

	public static String lookupSchemaId(String schemaId) {

		return (String)(schemaMap.get(schemaId));

	}

	/* ************************************************
	 * lookupTableId:
	 * Return the table name corresponding to the
	 * received table id.
	 * @param tableId The id to look up.
	 * @return the table name.
	 ****/

	public static String lookupTableId(String tableId) {

		return (String)(tableIdToNameMap.get(tableId));

	}

	/* ************************************************
	 * writeVerboseOutput:
	 * Writes the received string as "verbose" output,
	 * meaning that we write it to System.err.  We
	 * choose System.err so that the string doesn't
	 * show up if the user pipes dblook output to
	 * a file (unless s/he explicitly pipes System.err
	 * output to that file, as well).
	 * @param key Key for the message to be printed as
	 *  verbose output.
	 * @param value Value to be substituted into the
	 *  message.
	 * @return message for received key has been printed
	 *  to System.err.
	 ****/

	public static void writeVerboseOutput(String key,
		String value) {

		if (value == null)
			System.err.println(lookupMessage(key));
		else
			System.err.println(lookupMessage(key,
				new String [] {value}));
		return;

	}

	/* ************************************************
	 * lookupMessage:
	 * Retrieve a localized message.
	 * @param key The key for the localized message.
	 * @return the message corresponding to the received
	 *  key.
	 ****/

	public static String lookupMessage(String key) {

		return lookupMessage(key, null);

	}

	/* ************************************************
	 * lookupMessage:
	 * Retreive a localized message.
	 * @param key The key for the localized message.
	 * @param vals Array of values to be used in the
	 *   message.
	 * @return the message corresponding to the received
	 *  key, with the received values substituted where
	 *  appropriate.
	 ****/

	public static String lookupMessage(String key, String[] vals) {
	
		String msg = "";
		if (vals == null)
			msg = langUtil.getTextMessage(key);
		else {
			switch (vals.length) {
				case 1: msg = langUtil.getTextMessage(
							key, vals[0]);
						break;
				case 2: msg = langUtil.getTextMessage(
							key, vals[0], vals[1]);
						break;
				default: /* shouldn't happen */
						break;
			}
		}

		return msg;

	}

	/* ************************************************
	 * removeNewlines:
	 * Remove any newline characters from the received
	 * string (replace them with spaces).
	 * @param str The string from which we are removing
	 *  all newline characters.
	 * @return The string, with all newline characters
	 *  replaced with spaces.
	 ****/

	public static String removeNewlines(String str) {

		if (str == null)
		// don't do anything.
			return null;

		StringBuffer result = null;
		try {

			BufferedReader strVal = new BufferedReader (new StringReader(str));
			for (String txt = strVal.readLine(); txt != null;
				txt = strVal.readLine())
			{
				if (result == null)
					result = new StringBuffer(txt);
				else {
					result.append(" ");
					result.append(txt);
				}
			}

			return result.toString();

		} catch (Exception e) {
		// if something went wrong, just return the string as is--
		// worst case is that the generated DDL is correct, it just
		// can't be run in a DB2 CLP script (because of the newline
		// characters).
			return str;
		}

	}

}

