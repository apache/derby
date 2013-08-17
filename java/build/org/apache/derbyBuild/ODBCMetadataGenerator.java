/*

   Derby - Class org.apache.derby.catalog.ODBCProcedureColsVTI

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

package org.apache.derbyBuild;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.FileWriter;

import java.util.Properties;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * This class is used at COMPILE TIME ONLY.  It is responsible for generating
 * ODBC metadata queries based on existing JDBC queries.  In a word,
 * this class reads from the org/apache/derby/impl/jdbc/metadata.properties
 * file (which is where the JDBC queries are stored), and for each query,
 * performs the changes/additions required to make it comply with ODBC
 * standards.  The generated ODBC queries are written to an output file
 * that is then used, at build time, to create a full set of both JDBC and
 * ODBC queries, all of which are then loaded into the database system
 * tables at creation time.
 *
 * For more on the ODBC specification of the metadata methods in question,
 * see:
 *
 * "http://msdn.microsoft.com/library/default.asp?url=/library/en-us/odbc/
 *  htm/odbcsqlprocedures.asp"
 *
 * For more on how the generated queries are used at execution time, see
 * EmbedDatabaseMetadata.java and SystemProcedures.java in the codeline.
 *
 */
public class ODBCMetadataGenerator {

	// Types of changes that are possible.  There are four
	// types that we handle here:
	//
	//	1. Column rename:
	//		Rename a column to have an ODBC-specified name.
	//		For ex. change "SCALE" to "DECIMAL_DIGITS"
	//	2. Where clause:
	//		Change the where clause of the query. For ex. 
	//		used to change getCrossReference "T.TABLENAME=?"
	//		to "T.TABLENAME LIKE ?" since JDBC and ODBC specs
	//		differ on whether table name must be set or not
	//	3. Type and/or value change:
	//		Cast a column to an OBDC-specified type.  At time
	//		of writing, this was just for casting INTs to
	//		SMALLINTs; OR modify an existing JDBC value
	//		to match the ODBC specification.
	//	4. Additional column(s):
	//		Add a new, ODBC-specified column to an existing
	//		result set.

	private final byte COL_RENAME_CHANGE = 0x01;
	private final byte TYPE_VALUE_CHANGE = 0x02;
	private final byte ADD_COLUMN_CHANGE = 0x04;
	private final byte WHERE_CLAUSE_CHANGE = 0x08;

	// Notice written before each generated ODBC statement.
	private final String ODBC_QUERY_NOTICE =
		"#\n# *** NOTE! ***  The following query was generated\n" +
		"# AUTOMATICALLY at build time based on the existing\n" +
		"# JDBC version of the query.  DO NOT MODIFY this\n" +
		"# generated query by hand.  Instead, modify either\n" +
		"# 1) the JDBC version of the query in the codeline \n" +
		"# file \"metadata.properties\" (which will then get\n" +
		"# propagated at build time), 2) the relevant SQL\n" +
		"# fragments in 'odbcgen_fragments.properties' in\n" +
		"# the codleine, or 3) the ODBCMetadataGenerator\n" +
		"# class in the org/apache/derbyBuild directory.\n";

	// Prefix to append to all ODBC queries.  NOTE: if you change
	// this value, you'll have to modify EmbedDatabaseMetadata.java
	// to reflect the change.
	private final String ODBC_QUERY_PREFIX = "odbc_";

	// Name to use when making JDBC queries into subqueries
	// (loaded from odbcFragments).  NOTE: if you change this value,
	// you'll have to modify "odbcgen_fragments.properties" to
	// reflect the change.
	private final String SUBQUERY_NAME = "JDBC_SUBQUERY";

	// Mock value used to accomplish insertion of new columns.
	private final String NEW_COL_PLACEHOLDER = "COLUMN_POSITION_HOLDER";

	// Used for trimming 'whitespace'.
	private final short FOLLOWING = 1;
	private final short PRECEDING = -1;

	// Used for casting BOOLEANS to INTEGERS
	private	static	final	String	BOOLEAN_COLUMNS[] =
	{
		"CASE_SENSITIVE",
		"FIXED_PREC_SCALE",
		"UNSIGNED_ATTRIBUTE",
		"AUTO_UNIQUE_VAL",
        "NON_UNIQUE",
	};
    
	// List of what types of changes are required for a given
	// metadata procedure.
	private HashMap<String, Byte> changeMap;

	// SQL fragments and keywords that are used in composing
	// ODBC metadata queries.  These are loaded from a file
	// once and then used throughout the generation process
	// to build the ODBC queries piece-by-piece.
	private Properties odbcFragments;

	// Output file; all processed statements are written to this
	// file.  At BUILD TIME, this file will clobber the copy of
	// "metadata.properties" that is in the BUILD/CLASSES
	// directory.  NOTE: this will NOT clobber the metadata
	// properties file that is in the SOURCE/CODELINE.
	private FileWriter odbcMetaFile;

	/**
	 * Constructor.
	 * Initializes SQL fragments used for generation, and
	 * then opens the output file,
	 */
	public ODBCMetadataGenerator() throws IOException {

		// SQL fragments.
		odbcFragments = new Properties();
		odbcFragments.load(this.getClass().getResourceAsStream(
			"odbcgen_fragments.properties"));

		// Prep output file.
		odbcMetaFile = new FileWriter("odbc_metadata.properties");

	}

	/* ****
	 * main:
	 * Open the metadata.properties file (the copy that is in the
	 * build directory, NOT the one in the source directory),
	 * figure out what changes are needed for the various metadata
	 * queries, and then generate the ODBC-compliant versions
	 * where needed.
	 * @param args Ignored.
	 * @return ODBC-compliant metadata statements have been
	 *  	generated and written out to "odbc_metadata.properties"
	 *		in the running directory.
	 */
	public static void main(String [] args) throws IOException {

		ODBCMetadataGenerator odbcGen = new ODBCMetadataGenerator();
		odbcGen.initChanges();
		odbcGen.generateODBCQueries(odbcGen.getClass().getResourceAsStream(
			"/org/apache/derby/impl/jdbc/metadata.properties"));

	}

	/**
	 * initChanges
	 * Create a listing of the types of changes that need to be
	 * made for each metadata query to be ODBC-compliant.
	 * If a metadata query has no entry in this map, then
	 * it is left unchanged and no ODBC-version will be created.
	 * Having this mapping allows us to skip over String
	 * parsing (which can be slow) when it's not required.
	 * For details on the changes, see the appropriate methods
	 * below.
	 * @return Map holding the list of changes to be made for
	 * 	each metadata query has been initialized.
	 */
	private void initChanges() {

		changeMap = new HashMap<String, Byte>();

		changeMap.put("getProcedures",
			new Byte(COL_RENAME_CHANGE));

		changeMap.put("getProcedureColumns",
			new Byte((byte)(COL_RENAME_CHANGE
				| TYPE_VALUE_CHANGE
				| ADD_COLUMN_CHANGE)));

		changeMap.put("getColumns",
			new Byte(TYPE_VALUE_CHANGE));

		changeMap.put("getVersionColumns",
			new Byte(TYPE_VALUE_CHANGE));

		changeMap.put("getBestRowIdentifierPrimaryKeyColumns",
			new Byte(TYPE_VALUE_CHANGE));

		changeMap.put("getBestRowIdentifierUniqueKeyColumns",
			new Byte(TYPE_VALUE_CHANGE));

		changeMap.put("getBestRowIdentifierUniqueIndexColumns",
			new Byte(TYPE_VALUE_CHANGE));

		changeMap.put("getBestRowIdentifierAllColumns",
			new Byte(TYPE_VALUE_CHANGE));

		changeMap.put("getTypeInfo",
			new Byte((byte)(COL_RENAME_CHANGE
				| TYPE_VALUE_CHANGE
				| ADD_COLUMN_CHANGE)));

		changeMap.put("getIndexInfo",
			new Byte(TYPE_VALUE_CHANGE));

		changeMap.put("getCrossReference",
			new Byte(WHERE_CLAUSE_CHANGE));

		return;

	}

	/**
	 * generateODBCQueries:
	 * Reads the existing (JDBC) metadata queries from
	 * metadata.properties and, for each one, makes a call
	 * to generate an ODBC-compliant version.
	 * @param is InputStream for reading metadata.properties.
	 */
	public void generateODBCQueries(InputStream is)
		throws IOException
	{

		// JDBC query that we read from metadata.properties.
		StringBuffer query = new StringBuffer();

		// Note: We use ISO-8859-1 because property files are
		// defined to be that encoding.
		LineNumberReader reader =
			new LineNumberReader(new InputStreamReader(is, "ISO-8859-1"));

		String line = null;
		for (line = reader.readLine(); line != null;
			line = reader.readLine())
		{

			if (line.length() == 0)
			// blank line; ignore
				continue;
			else if (line.charAt(0) == '#') {
			// comment; write it to file.
				odbcMetaFile.write(line);
				odbcMetaFile.write("\n");
				continue;
			}

			// Write the line, then add an end-of-line to maintain
			// readability.
			query.append(line);
			query.append("\n");

			// Check to see if this is the last line of the query.
			boolean done = true;
			for (int lastNonWS = line.length() - 1;
				lastNonWS >= 0; lastNonWS--)
			{
				char ch = line.charAt(lastNonWS);
				if (!Character.isWhitespace(ch)) {
				// this is the last non-whitespace character; if it's
				// a backslash, then we continue building the query
				// by reading the next line.
					if (ch == '\\') {
					// then continue building the query.
						done = false;
					}
					break;
				}
			}

			if (!done)
			// read next line and append it to current query.
				continue;

			// Take the query and see if we need to generate an ODBC-
			// compliant version.
			generateODBCQuery(query);

			// Prep for another query.
			query.delete(0, query.length());

		}

		// Make sure we didn't end up with an incomplete query somewhere.
		if (query.length() > 0) {
			throw new IOException(
				"Encountered non-terminated query while reading metadata file.");
		}

		// Close out.
		odbcMetaFile.flush();
		odbcMetaFile.close();

	}

	/**
	 * generateODBCQuery
	 * Takes a specific JDBC query, writes it to the output file,
	 * and then creates an ODBC-compliant version of that
	 * query (if needed) and writes that to the output file,
	 * as well.
	 * @param queryText SQL text from a JDBC metadata query
	 *	that was read from metadata.properties.
	 */
	private void generateODBCQuery(StringBuffer queryText)
		throws IOException
	{

		// Create a string for purposes of using "indexOf"
		// calls, which aren't allowed on a StringBuffer
		// for JDBC 2.0.
		String queryAsString = queryText.toString().trim();

		if (queryAsString.startsWith(ODBC_QUERY_PREFIX))
		// this query was automatically generated (presumably
		// by this class), so ignore it now.
			return;

		// Write the original (JDBC) query.
		odbcMetaFile.write(queryAsString, 0, queryAsString.length());
		odbcMetaFile.write("\n\n");

		// Parse out the name of this particular query.
		int pos = queryAsString.indexOf("=");
		if (pos == -1) {
			throw new IOException(
				"Failed to extract query name from a JDBC metadata query.");
		}
		String queryName = queryText.substring(0, pos);

		// Parse out the ORDER BY clause since they are not allowed
		// in subqueries; we'll re-attach it later.
		String orderBy = "";
		int orderByPos = queryAsString.lastIndexOf("ORDER BY");
		if (orderByPos != -1)
			orderBy = queryAsString.substring(orderByPos, queryAsString.length());

		// Isolate query text (remove ORDER BY clause and then query name,
		// in that order).
		if (orderByPos != -1)
			queryText.delete(orderByPos, queryText.length());
		queryText.delete(0, pos+1);

		// Three types of modifications that we may need to do.

		// -- #1: Column renaming.
		StringBuffer outerQueryText = new StringBuffer();
		boolean haveODBCChanges = renameColsForODBC(queryName, queryText);

		// -- #2: Change WHERE clause.
		if (changeWhereClause(queryName, queryText)) haveODBCChanges = true;

		// Get a list of the column definitions in the subquery, for
		// use by subsequent operations.
		ArrayList<String> colDefs = new ArrayList<String>();
		pos = getSelectColDefinitions(queryText, colDefs);

		// In some cases, we need to add "helper" columns to the
		// subquery so that we can use them in calculations for
		// the outer query.
		addHelperColsToSubquery(queryName, queryText, pos);

		// -- #3.A: Prep to add new ODBC columns.  Note: we need
		// to do this BEFORE we generate the outer SELECT statement.
		markNewColPosition(queryName, colDefs);

		// If we're going to use a subquery, generate the outer
		// SELECT statement.  This is where we enforce column
		// types (via CAST) if needed.
		generateSELECTClause(queryName, colDefs, outerQueryText);

		// -- #4: Alter column values, where needed.
		changeValuesForODBC(queryName, outerQueryText);

		// -- #3.B: Add new ODBC columns.
		addNewColumnsForODBC(queryName, outerQueryText);

		haveODBCChanges = (haveODBCChanges || (outerQueryText.length() > 0));
		if (!haveODBCChanges)
		// we didn't change anything, so nothing left to do.
			return;

		// Write out the new, ODBC version of the query.

		odbcMetaFile.write(ODBC_QUERY_NOTICE);
		odbcMetaFile.write(ODBC_QUERY_PREFIX);
		odbcMetaFile.write(queryName);
		odbcMetaFile.write("=");

		if (outerQueryText.length() == 0) {
		// all we did was change column names, so just write out the
		// original query with the new column names.
			odbcMetaFile.write(queryText.toString());

			if (orderBy.length() != 0) {
				// re-attach ORDER BY clause.
				odbcMetaFile.write(orderBy);
			}

			odbcMetaFile.write("\n\n");
			return;
		}

		// Else, we need to make the original query a subquery so that we
		// can change types/values and/or add columns.
		queryAsString = queryText.toString().trim();
		odbcMetaFile.write(outerQueryText.toString());
		odbcMetaFile.write(queryAsString);
		if (queryText.charAt(queryAsString.length()-1) == '\\')
			odbcMetaFile.write("\n\\\n) ");
		else
			odbcMetaFile.write(" \\\n\\\n) ");
		odbcMetaFile.write(SUBQUERY_NAME);
		if (orderBy.length() == 0)
			odbcMetaFile.write("\n");
		else {
		// re-attach ORDER BY clause.
			odbcMetaFile.write(" \\\n");
			odbcMetaFile.write(orderBy);
		}
		odbcMetaFile.write("\n\n");
		return;

	}

	/**
	 * renameColsForODBC
	 * Renames any columns in the received query so that they are
	 * ODBC-compliant.
	 * @param queryName Name of the query being processed.
	 * @param queryText Text of the query being processed.
	 * @return All columns requiring renaming have been renamed IN
	 *	PLACE in the received StringBuffer.  True is returned if
	 *	at least one column was renamed; false otherwise.
	 */
	private boolean renameColsForODBC(String queryName, StringBuffer queryText) {

		// If we know the received query doesn't have any columns to
		// be renamed, then there's nothing to do here.
		if (!stmtNeedsChange(queryName, COL_RENAME_CHANGE))
			return false;

		// Which columns are renamed, and what the new names are,
		// depends on which query we're processing.

		if (queryName.equals("getProcedures")) {
			renameColForODBC(queryText, "RESERVED1", "NUM_INPUT_PARAMS");
			renameColForODBC(queryText, "RESERVED2", "NUM_OUTPUT_PARAMS");
			renameColForODBC(queryText, "RESERVED3", "NUM_RESULT_SETS");
			return true;
		}
		else if (queryName.equals("getProcedureColumns")) {
			renameColForODBC(queryText, "PRECISION", "COLUMN_SIZE");
			renameColForODBC(queryText, "LENGTH", "BUFFER_LENGTH");
			renameColForODBC(queryText, "SCALE", "DECIMAL_DIGITS");
			renameColForODBC(queryText, "RADIX", "NUM_PREC_RADIX");
			return true;
		}
		else if (queryName.equals("getTypeInfo")) {
			renameColForODBC(queryText, "PRECISION", "COLUMN_SIZE");
			renameColForODBC(queryText, "AUTO_INCREMENT", "AUTO_UNIQUE_VAL");
			return true;
		}

		// No renaming was necessary.
		return false;

	}

	/**
	 * renameColForODBC
	 * Searches for the old column name in the received String
	 * buffer and replaces it with the new column name.  Note
	 * that we only replace the old column name where it is
	 * preceded by "AS", because this is the instance that
	 * determines the column name in the final result set.
	 * @param queryText The query text in which we're doing the
	 *	rename operation.
	 * @param oldVal The old column name.
	 * @param newVal The new column name.
	 */
	private void renameColForODBC(StringBuffer queryText,
		String oldVal, String newVal)
	{

		String queryString = queryText.toString();
		int pos = queryString.indexOf(oldVal);
		while (pos != -1) {

			// Next line will set pos2 to be the index of the
			// first (reading left-to-right) ignorable char
			// preceding the old column name.  That means
			// that the letters immediately preceding this
			// position should be "AS".  If not, don't
			// replace this instance.
			int pos2 = trimIgnorable(PRECEDING, queryString, pos);
			if (((pos2 - 2) > 0) && (queryString.charAt(pos2-2) == 'A')
				&& (queryString.charAt(pos2-1) == 'S'))
			{ // then this is the one we want to replace.
				break;
			}
			else {
			// look for next occurrence.
				pos = queryString.indexOf(oldVal, pos+1);
			}

		}

		if (pos == -1) {
		// couldn't find the one to replace; leave unchanged.
			return;
		}

		// Do the renaming.
		queryText.replace(pos, pos + oldVal.length(), newVal);

	}

	/**
	 * generateSELECTClause
	 * Generates an outer SELECT clause that is then wrapped around a
	 * JDBC query to change the types and/or values of the JDBC
	 * result set.  The JDBC query thus becomes a subquery.
	 *
	 * Ex. if we have a JDBC query "SELECT A, B FROM T1" and ODBC
	 * requires that "A" be a smallint, this method will generate
	 * a select clause "SELECT CAST (T2.A AS SMALLINT), T2.B FROM"
	 * that is then used to wrap the JDBC query, as follows:
	 *
	 *		SELECT CAST (T2.A AS SMALLINT), T2.B FROM
	 *			(SELECT A, B FROM T1) T2
	 *
	 * @param queryName Name of the query being processed.
	 * @param selectColDefs Array list of the SELECT columns that
	 * 	exist for the JDBC version of the query.  For the above
	 *  example, this would be an array list with two String
	 *  elements, "A" and "B".
	 * @param newQueryText StringBuffer to which the generated
	 *  outer SELECT will be appended.
	 *  On return, an outer SELECT clause has been generated and
	 *  appended to the received buffer.  The "FROM" keyword
	 *  has been appended, but the subquery itself is NOT
	 *  added here.
	 */
	private void generateSELECTClause(String queryName,
		ArrayList selectColDefs, StringBuffer newQueryText)
	{
		if (!stmtNeedsChange(queryName, TYPE_VALUE_CHANGE) &&
			!stmtNeedsChange(queryName, ADD_COLUMN_CHANGE))
		{ // then we don't need to generate a SELECT, because we
		  // don't need to use a subquery (we're only renaming).
			return;
		}

		// Begin the SELECT clause.
		newQueryText.append("SELECT \\\n\\\n");

		// For each of the SELECT columns in JDBC, either
		// just grab the column name and use it directly in
		// the generated clause, or else cast the column
		// to the required type, if appropriate.
		String colName;
		String castInfo;
		for (int i = 0; i < selectColDefs.size(); i++) {
			if (i > 0)
				newQueryText.append(", \\\n");
			colName = extractColName((String)selectColDefs.get(i));
			castInfo = getCastInfoForCol(queryName, colName);
			if (castInfo != null)
				newQueryText.append("CAST (");
            //
            // Special logic to turn booleans into integers. This is necessary
            // because you cannot cast a boolean to an integer, according to the
            // sql standard.
            //
			if ( isBoolean( colName ) ) { newQueryText.append( " ( CASE WHEN " ); }
			newQueryText.append(SUBQUERY_NAME);
			newQueryText.append(".");
			newQueryText.append(colName);
            //
            // Really special logic to force the AUTO_UNIQUE_VAL and
            // UNSIGNED_ATTRIBUTE columns to
            // be nullable. This appears to be something that the ODBC spec
            // requires.
            //
            if ( "AUTO_UNIQUE_VAL".equals( colName )  || "UNSIGNED_ATTRIBUTE".equals( colName ) )
            {
                newQueryText.append( " IS NULL THEN CAST( NULL AS INTEGER ) WHEN " );
                newQueryText.append(SUBQUERY_NAME);
                newQueryText.append(".");
                newQueryText.append(colName);
            }
			if ( isBoolean( colName ) ) { newQueryText.append( " THEN 1 ELSE 0 END ) " ); }
			if (castInfo != null) {
				newQueryText.append(" AS ");
				newQueryText.append(castInfo);
				newQueryText.append(")");
			}
			if (!colName.equals(NEW_COL_PLACEHOLDER)) {
			// don't append the "AS" clause if this is just our
			// place-holder for adding new columns.
				newQueryText.append(" AS ");
				newQueryText.append(colName);
			}
		}

		if (newQueryText.charAt(newQueryText.length() - 1) != '\\')
			newQueryText.append(" \\");

		// End the SELECT clause.
		newQueryText.append("\nFROM ( ");
		return;

	}

	/**
	 * changeValuesForODBC
	 * Searches for a JDBC column name in the received String
	 * buffer and replaces the first occurrence with an ODBC-
	 * compliant value.  This method determines what specific
	 * columns need updated values for a given query, and then
	 * makes the appropriate call for each column.
	 * @param queryName Name of the query being processed.
	 * @param newQueryText The query text in which we're doing the
	 *	change-value operation.
	 */
	private void changeValuesForODBC(String queryName,
		StringBuffer newQueryText)
	{

		if (!stmtNeedsChange(queryName, TYPE_VALUE_CHANGE))
			return;

		// Which column values are changed, and what the new
		// values are, depends on which query we're processing.

		if (queryName.equals("getColumns")) {
			changeColValueToODBC(queryName, "BUFFER_LENGTH", newQueryText);
			changeColValueToODBC(queryName, "DECIMAL_DIGITS", newQueryText);
			changeColValueToODBC(queryName, "NUM_PREC_RADIX", newQueryText);
			changeColValueToODBC(queryName, "SQL_DATA_TYPE", newQueryText);
			changeColValueToODBC(queryName, "SQL_DATETIME_SUB", newQueryText);
			changeColValueToODBC(queryName, "CHAR_OCTET_LENGTH", newQueryText);
		}
		else if (queryName.startsWith("getBestRowIdentifier")) {
			changeColValueToODBC(queryName, "BUFFER_LENGTH", newQueryText);
			changeColValueToODBC(queryName, "DECIMAL_DIGITS", newQueryText);
		}
		else if (queryName.equals("getTypeInfo")) {
			changeColValueToODBC(queryName, "NUM_PREC_RADIX", newQueryText);
			changeColValueToODBC(queryName, "SQL_DATA_TYPE", newQueryText);
			changeColValueToODBC(queryName, "SQL_DATETIME_SUB", newQueryText);
			changeColValueToODBC(queryName, "UNSIGNED_ATTRIBUTE", newQueryText);
			changeColValueToODBC(queryName, "AUTO_UNIQUE_VAL", newQueryText);
		}
		else if (queryName.equals("getProcedureColumns")) {
			changeColValueToODBC(queryName, "NUM_PREC_RADIX", newQueryText);
			changeColValueToODBC(queryName, "DECIMAL_DIGITS", newQueryText);
		}

	}

	/**
	 * changeColValueToODBC
	 * Searches for the received column name in the received String
	 * buffer and replaces it with an ODBC-compliant value.
	 * @param queryName Name of the query being processed.
	 * @param colName Name of the specific column to update.
	 * @param newQueryText The query text in which we're doing
	 *	the change-value operation.
	 */
	private void changeColValueToODBC(String queryName, String colName,
		StringBuffer newQueryText)
	{

		colName = SUBQUERY_NAME + "." + colName;
		int pos = newQueryText.toString().indexOf(colName);
		if (pos == -1)
		// column we're supposed to change isn't in the query.
			return;

		if (colName.endsWith("CHAR_OCTET_LENGTH")) {
			newQueryText.replace(pos, pos + colName.length(),
				getFragment("CHAR_OCTET_FOR_ODBC"));
		}
		else if (colName.endsWith("BUFFER_LENGTH")) {
			newQueryText.replace(pos, pos + colName.length(),
				getFragment("BUFFER_LEN_FOR_ODBC"));
		}
		else if (colName.endsWith("SQL_DATA_TYPE")) {
			newQueryText.replace(pos, pos + colName.length(),
				getFragment("SQL_DATA_TYPE_FOR_ODBC"));
		}
		else if (colName.endsWith("SQL_DATETIME_SUB")) {
			newQueryText.replace(pos, pos + colName.length(),
				getFragment("DATETIME_SUB_FOR_ODBC"));
		}
		else if (colName.endsWith("UNSIGNED_ATTRIBUTE")) {
			newQueryText.replace(pos, pos + colName.length(),
				getFragment("UNSIGNED_ATTR_FOR_ODBC"));
		}
		else if (colName.endsWith("AUTO_UNIQUE_VAL")) {
			newQueryText.replace(pos, pos + colName.length(),
				getFragment("AUTO_UNIQUE_FOR_ODBC"));
		}
		else if (colName.endsWith("DECIMAL_DIGITS")) {
			newQueryText.replace(pos, pos + colName.length(),
				getFragment("DECIMAL_DIGITS_FOR_ODBC"));
		}
		else if (colName.endsWith("NUM_PREC_RADIX")) {
			newQueryText.replace(pos, pos + colName.length(),
				getFragment("RADIX_FOR_ODBC"));
		}
		else if (colName.endsWith(NEW_COL_PLACEHOLDER)) {
		// This is a special case indication that we need to add new columns.
			if (queryName.equals("getProcedureColumns")) {
				newQueryText.replace(pos, pos + colName.length(),
					getFragment("GET_PROC_COLS_NEW_COLS"));
			}
			else if (queryName.equals("getTypeInfo")) {
				newQueryText.replace(pos, pos + colName.length(),
					getFragment("GET_TYPE_INFO_NEW_COLS"));
			}
		}

	}

	/**
	 * getSelectColDefinitions
	 * Parses the SELECT clause of a JDBC metadata SQL query
	 * and returns a list of the columns being selected.  For
	 * example, if the received statement was "SELECT A,
	 * B AS C, D * 2 FROM T1", this method will return an
	 * ArrayList with three string elements: 1) "A", 2) "B
	 * AS C", and 3) "D * 2".
	 * @param queryText The query from which we are extracting
	 *	the SELECT columns.
	 * @param colDefList ArrayList in which we want to
	 * 	store the column definitions that we find.
	 * @return Received ArrayList has one string value for
	 *	each of the columns found in the received query.
	 *	Also, an integer is returned indicating the index
	 *	in the received query of the start of the FROM
	 *	clause, for later use by the calling method.
	 */
	private int getSelectColDefinitions(StringBuffer queryText,
		ArrayList<String> colDefList)
	{

		// Create a string for purposes of using "indexOf"
		// calls, which aren't allowed on a StringBuffer
		// for JDBC 2.0.
		String query = queryText.toString().trim();
		char [] queryChars = query.toCharArray();

		// Move beyond the "SELECT" keyword, if there is one.
		int start = query.indexOf("SELECT");
		if (start != -1)
		// "+6" in the next line is length of "SELECT".
			start += 6;
		else
		// just start at the first character.
			start = 0;

		// Have to read character-by-character in order to
		// figure out where each column description ends.
		int fromClauseIndex = -1;
		int parenDepth = 0;
		for (int i = start; i < queryChars.length; i++) {

			if (queryChars[i] == '(')
				parenDepth++;
			else if (queryChars[i] == ')')
				parenDepth--;
			else if ((queryChars[i] == ',') && (parenDepth == 0)) {
			// this is a naive way of determining the end of a
			// column definition (it'll work so long as there are no
			// string constants in the query that have commas in them,
			// which was true at the time of writing.
				colDefList.add(new String(queryChars, start, (i - start)).trim());
				// Skip over non-important whitespace to find start
				// of next column definition.  Next line will set i to
				// just before the next non-whitespace character.
				i = trimIgnorable(FOLLOWING, queryChars, i);
				start = i + 1;
			}
			else if (((i+3) < queryChars.length)
				&& (parenDepth == 0)
				&& (queryChars[i] == 'F')
				&& (queryChars[i+1] == 'R')
				&& (queryChars[i+2] == 'O')
				&& (queryChars[i+3] == 'M'))
			{ // this is the end of final column definition; store it
			  // and then exit the loop, after trimming off non-important
			  // whitespace.  Next line will set i to just after the
			  // last (reading left-to-right) non-whitespace character
			  // before the FROM.
				i = trimIgnorable(PRECEDING, queryChars, i);
				fromClauseIndex = i;
				colDefList.add(new String(queryChars, start, (i - start)).trim());
				break;

			}

		}

		return fromClauseIndex;

	}

	/**
	 * addHelperColsToSubquery
	 * For some of the metadata queries, the ODBC version
	 * needs to access values that are only available in
	 * the JDBC subquery.  In such cases, we want to add
	 * those values as additional "helper" columns to
	 * the subquery result set, so that they can be
	 * referenced from the new ODBC outer query (without
	 * requiring a join).  For example, assume we have 2
	 * tables T1(int i, int j) and T2 (int a), and a
	 * subquery "SELECT T1.i, T1.j + T2.a from T1, T2)".
	 * Then we have an outer query that, instead of
	 * returning "T1.j + T2.a", needs to return the
	 * value of "2 * T2.a":
	 *
	 * SELECT VT.i, 2 * T2.a FROM
	 *	(SELECT T1.i, T1.j + T2.a FROM T1, T2) VT
	 *
	 * The above statement WON'T work, because the outer
	 * query can't see the value "T2.a".  So in such a
	 * a case, this method will add "T2.a" to the list
	 * of columns returned by the subquery, so that the
	 * outer query can then access it:
	 *
	 * SELECT VT.i, 2 * VT.a FROM
	 * 	(SELECT T1.i, T1.j + T2.a, T2.a FROM T1, T2) VT
	 *
	 * Which specific columns are added to the subquery
	 * depends on the query in question.
	 *
	 * @param queryName Name of the query in question.
	 * @param subqueryText text of the subquery in question.
	 * @param insertPos Index into the received buffer
	 *	marking the position where the helper columns
	 * 	should be inserted.
	 */
	private void addHelperColsToSubquery(String queryName,
		StringBuffer subqueryText, int insertPos)
	{

		if (queryName.equals("getColumns")) {
			subqueryText.insert(insertPos,
				getFragment("GET_COLS_HELPER_COLS"));
		}
		else if (queryName.startsWith("getBestRowIdentifier")) {
			subqueryText.insert(insertPos,
				getFragment("BEST_ROW_ID_HELPER_COLS"));
		}

	}

	/**
	 * extractColName
	 * Takes a single column definition from a SELECT clause
	 * and returns only the unqualified name of the column.
	 * Assumption here is that any column definition we see
	 * here will either 1) end with an "AS <COLUMN_NAME>"
	 * clause, or 2) consist of ONLY a column name, such
	 * as "A" or "A.B".  At the time of writing, these
	 * assumptions were true for all relevant metadata
	 * queries.
	 *
	 * Ex. If colDef is "A", this method will return "A".
	 * If colDef is "A.B", this method will return "B".
	 * If colDef is "<bunch of SQL> AS C", this method
	 * will return "C".
	 *
	 * @param colDef Column definition from which we're
	 *	trying to extract the name.
	 * @return Name of the column that is referenced in
	 *	the received column definition.
	 */
	private String extractColName(String colDef) {

		// Find out where the column name starts.
		int pos = colDef.lastIndexOf("AS ");
		if (pos == -1) {
		// we assume that the col def is _just_ a column name,
		// so start at the beginning.
			pos = 0;
		}
		else {
			// Move beyond the "AS".
			pos += 2;

			// Skip any non-important whitespace or backslashes.
			char c = colDef.charAt(pos);
			while ((c == '\\') || Character.isWhitespace(c))
				c = colDef.charAt(++pos);
		}

		// Check to see if it's a qualified name.
		int pos2 = colDef.indexOf(".", pos);
		if (pos2 == -1)
		// it's not a qualified name, so just return it.
			return colDef.substring(pos, colDef.length());

		// Else, strip off the schema and just return the col name.
		return colDef.substring(pos2+1, colDef.length());

	}

	/**
	 * getCastInfoForCol
	 * Returns the target type for a result set column that
	 * needs to be cast into an ODBC type.  This is usually
	 * for casting integers to "SMALLINT".
	 * @param queryName Name of query being processed.
	 * @param colName Name of the specific column for which
	 * 	we are trying to find the target type.
	 * @return The target type if one exists, or else null
	 *  if the received column in the received query has
	 * 	no known target type.
	 */
	private String getCastInfoForCol(String queryName,
		String colName)
	{

		if (queryName.equals("getTypeInfo")) {
			if (colName.equals("DATA_TYPE") ||
				colName.equals("CASE_SENSITIVE") ||
				colName.equals("UNSIGNED_ATTRIBUTE") ||
				colName.equals("FIXED_PREC_SCALE") ||
				colName.equals("AUTO_UNIQUE_VAL") ||
				colName.equals("SQL_DATA_TYPE") ||
				colName.equals("SQL_DATETIME_SUB"))
			{
				return "SMALLINT";
			}
		}
		else if (queryName.equals("getColumns")) {
			if (colName.equals("DECIMAL_DIGITS") ||
				colName.equals("NULLABLE") ||
				colName.equals("DATA_TYPE") ||
				colName.equals("NUM_PREC_RADIX") ||
				colName.equals("SQL_DATA_TYPE") ||
				colName.equals("SQL_DATETIME_SUB"))
			{
				return "SMALLINT";
			}
		}
		else if (queryName.equals("getProcedureColumns")) {
			if (colName.equals("DATA_TYPE")) {
				return "SMALLINT";
			}
		}
		else if (queryName.equals("getVersionColumns")) {
			if (colName.equals("DATA_TYPE")) {
				return "SMALLINT";
			}
		}
		else if (queryName.startsWith("getBestRowIdentifier")) {
			if (colName.equals("DATA_TYPE")) {
				return "SMALLINT";
			}
		}
		else if (queryName.equals("getIndexInfo")) {
			if (colName.equals("NON_UNIQUE") ||
				colName.equals("TYPE"))
			{
				return "SMALLINT";
			}
		}

		// No target type for the received column
		// in the received query (leave it unchanged).
		return null;

	}

	/**
	 * markNewColPosition
	 * In effect, "marks" the position at which additional
	 * columns are to be added for ODBC compliance.  This
	 * is accomplished by adding a dummy column name to
	 * the list of SELECT columns.  Later, in the method
	 * that actually adds the columns, we'll do a find-
	 * replace on this dummy value.
	 * @param queryName Name of the query.
	 * @param selectColDefs Array list of the SELECT
	 * 	columns that exist in the ODBC version of
	 *	the query thus far.
	 *  On return, a dummy column name has been added to
	 *	the received list of columns at the position
	 *	at which new ODBC columns should be added.
	 *  If a query doesn't require additional
	 * 	columns to be ODBC compliant, this method
	 *	leaves the received column list unchanged.
	 */
	private void markNewColPosition(String queryName,
		ArrayList<String> selectColDefs)
	{

		if (!stmtNeedsChange(queryName, ADD_COLUMN_CHANGE))
			return;

		if (queryName.equals("getProcedureColumns")) {
		// Add the new columns in front of the Derby-specific ones.
		// The "-2" in the next line is because there are 2 Derby-
		// specific columns in the JDBC version of getProcedureCols
		// (PARAMETER_ID and METHOD_ID).
			selectColDefs.add(selectColDefs.size() - 2, NEW_COL_PLACEHOLDER);
		}
		else if (queryName.equals("getTypeInfo")) {
		// just add the new column to the end.
			selectColDefs.add(NEW_COL_PLACEHOLDER);
		}

	}

	/**
	 * addNewColumnsForODBC
	 * Adds new columns to the ODBC version of a metadata
	 * query (the ODBC version is at this point being
	 * built up in newQueryText).  Before this method
	 * was called, a dummy placeholder should have been
	 * placed in the newQueryText buffer (by a call to
	 * "markNewColPosition").  This method simply replaces
	 * that dummy placeholder with the SQL text for the
	 * new columns.
	 * @param queryName Name of query being processed.
	 * @param newQueryText The buffer in which we want to
	 * 	add the new column.
	 *  On return, the dummy placeholder in the received
	 *  buffer has been replaced with any ODBC columns
	 *  that need to be added to the query in question
	 *  for ODBC compliance.
	 */
	private void addNewColumnsForODBC(String queryName,
		StringBuffer newQueryText)
	{

		if (!stmtNeedsChange(queryName, ADD_COLUMN_CHANGE))
			return;

		changeColValueToODBC(queryName, NEW_COL_PLACEHOLDER, newQueryText);

		// It's possible that the new column fragments we added
		// have placeholders in them for _other_ fragments.  We
		// need to do the substitution here.
		if (queryName.equals("getProcedureColumns")) {
			fragSubstitution("SQL_DATA_TYPE_FOR_ODBC", newQueryText);
			fragSubstitution("DATETIME_SUB_FOR_ODBC", newQueryText);
		}

		return;

	}

	/**
	 * fragSubstitution
	 * Replaces a single occurrence of the received
	 * fragment key with the text corresponding to
	 * that key.
	 * @param fragKey The fragment key for which we are
	 *	going to do the substitution.
	 * @param queryText The buffer in which we are going to do
	 * 	the substitution.
	 *  On return, fragKey has been substituted (IN PLACE)
	 *	with the fragment corresponding to it in the
	 *	received buffer.  If the fragment key could not
	 * 	be found, the buffer remains unchanged.
	 */
	private void fragSubstitution(String fragKey,
		StringBuffer queryText)
	{

		int pos = queryText.toString().indexOf(fragKey);
		if (pos != -1) {
			// NOTE: the " + 1" and " - 1" in the next line
			// are needed because the fragment key is
			// enclosed within curly braces ("{}").
			queryText.replace(pos - 1, pos + fragKey.length() + 1,
				getFragment(fragKey));
		}

	}

    /**
     * changeWhereClause
     * Substitutes patterns in the WHERE clause
     * @param queryName The name of the JDBC query; found in
     * metadata.properties
     * @param queryText The buffer in which we are going to do
     * the substitution.
     * @return the substitution is performed IN PLACE. If no changes
     * are needed on this query, the queryText buffer remains
     * unchanged.
     */
    private boolean changeWhereClause(String queryName,
                                      StringBuffer queryText) {

        if (!stmtNeedsChange(queryName, WHERE_CLAUSE_CHANGE))
            return false;

        if (queryName.equals("getCrossReference")) {
            substitutePatternWhere("T.TABLENAME=",
                                   "T.TABLENAME LIKE ",
                                   queryText);
            return true;
        }
        return false;
    }

    /**
     * Replaces a single occurrence of the received old pattern with
     * the text in the new pattern
     * @param oldPattern the text we want to remove
     * @param newPattern the text we want to replace the oldPattern
     * with
     * @param queryText The buffer in which we are going to do the
     * substitution.
     * On return, the old pattern is substituted with the new pattern (IN
     * PLACE). If the old pattern could not be found, the queryText
     * buffer remains unchanged.
     */
    private void substitutePatternWhere(String oldPattern, String newPattern,
                                        StringBuffer queryText){
        String queryTextString = queryText.toString();
        int queryLength = queryTextString.length();
        int wherePos = queryTextString.indexOf("WHERE");

        //only look for the pattern after the WHERE keyword. There may
        //actually be more than one where clause, in which case we do
        //not know if this is the right one. Since the pattern
        //substitution is only performed at compile time (on specified
        //queries), more rigorous checks are not performed her.
        //Rather, verify that
        //classes/org/apache/derby/impl/jdbc/metadata.properties is
        //updated correctly when you add new where-clause
        //substitutions.
        int posSubString = queryTextString.substring(wherePos, queryLength).
                                           indexOf(oldPattern);
        if (posSubString != -1)
            //posSubString is the position in the query *after* the
            //word WHERE. Have to add
            queryText.replace(wherePos + posSubString,
                              wherePos + posSubString + oldPattern.length(),
                              newPattern);
    }

	/**
	 * trimIgnorable
	 * Removes all 'ignorable' chars that immediately precede or
	 * follow (depending on the direction) the character at
	 * the received index.  "Ignorable" here means whitespace
	 * OR a single backslash ("\"), which is used in the
	 * metadata.properties file to indicate line continuation.
	 * @param direction +1 if we want to trim following, -1
	 *	if we want to trim preceding.
	 * @param chars The character array being processed.
	 * @param index The point before/after which to start
	 * 	trimming.
	 * @return The index into the received char array of the
	 *	"last" ignorable character w.r.t the received index
	 *	and direction.  In other words, if we're trimming
	 *	the chars FOLLOWING, the returned index will be of
	 * 	the last (reading left-to-right) ignorable char; if
	 *	we're trimming the chars PRECEDING, the returned index
	 *	will be of the first (reading left-to-right) ignorable
	 *	character.
	 */
	private int trimIgnorable(short direction, char [] chars, int index) {

		index += direction;
		while ((index >= 0) && (index < chars.length) &&
			((chars[index] == '\\') ||
			Character.isWhitespace(chars[index])))
		{
			index += direction;
		}

		// index is now on the final non-ignorable character
		// in the given direction.  Move it back one so that
		// it's on the "last" ignorable character (with
		// respect to direction).
		index -= direction;

		return index;

	}

	/**
	 * trimIgnorable
	 * Same as trimIgnorable above, except with String argument
	 * instead of char[].
	 */
	private int trimIgnorable(short direction, String str, int index) {

		index += direction;
		while ((index >= 0) && (index < str.length()) &&
			((str.charAt(index) == '\\') ||
			Character.isWhitespace(str.charAt(index))))
		{
			index += direction;
		}

		// index is now on the final non-ignorable character
		// in the given direction.  Move it back one so that
		// it's on the "first" ignorable character (with
		// respect to direction).
		index -= direction;

		return index;

	}

	/**
	 * Return true if the column is a BOOLEAN column which should
	 * be coerced to an INTEGER.
	 */
	private	boolean	isBoolean( String colName )
	{
		int		count = BOOLEAN_COLUMNS.length;

		for ( int i = 0; i < count; i++ )
		{
			if ( BOOLEAN_COLUMNS[ i ].equals( colName ) ) { return true; }
		}

		return false;
	}
    
	/**
	 * stmtNeedsChange
	 * Returns whether or not a specific metadata statement
	 * requires the received type of change.  This is determined
	 * based on the info stored in the "changeMaps" mapping.
	 * @param queryName Name of the query in question.
	 * @param changeType The type of change in question.
	 */
	private boolean stmtNeedsChange(String queryName, byte changeType) {

		Byte changeByte = changeMap.get(queryName);
		if (changeByte == null)
		// No entry means change is not needed.
			return false;

		return ((changeByte.byteValue() & changeType) == changeType);

	}

	/**
	 * getFragment
	 * Looks up an SQL fragment and returns the value as a String.
	 * @param String fragId id of the fragment to look up.
	 * @return The string fragment corresponding to the received
	 * 	fragment id.
	 */
	private String getFragment(String fragId) {
		return (String)(odbcFragments.get(fragId));
	}
	
}
