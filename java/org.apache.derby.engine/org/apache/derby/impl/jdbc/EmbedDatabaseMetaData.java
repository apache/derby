/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedDatabaseMetaData

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.jdbc;

import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derby.shared.common.info.ProductVersionHolder;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.impl.sql.execute.GenericConstantActionFactory;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.Limits;

import java.util.Properties;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.DatabaseMetaData;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.Types;

import java.io.IOException;
import java.io.InputStream;
import org.apache.derby.iapi.util.InterruptStatus;

/**
 * This class provides information about the database as a whole.
 *
 * <P>Many of the methods here return lists of information in ResultSets.
 * You can use the normal ResultSet methods such as getString and getInt
 * to retrieve the data from these ResultSets.  If a given form of
 * metadata is not available, these methods should throw a SQLException.
 *
 * <P>Some of these methods take arguments that are String patterns.  These
 * arguments all have names such as fooPattern.  Within a pattern String, "%"
 * means match any substring of 0 or more characters, and "_" means match
 * any one character. Only metadata entries matching the search pattern
 * are returned. If a search pattern argument is set to a null ref, it means
 * that argument's criteria should be dropped from the search.
 *
 * <P>A SQLException will be thrown if a driver does not support a meta
 * data method.  In the case of methods that return a ResultSet,
 * either a ResultSet (which may be empty) is returned or a
 * SQLException is thrown.
 * <p>
 * This implementation gets instructions from the Database for how to satisfy
 * most requests for information.  Each instruction is either a simple string
 * containing the desired information, or the text of a query that may be
 * executed on the database connection to gather the information.  We get the
 * instructions via an "InstructionReader," which requires the database
 * Connection for initialization.
 * <p>
 * Those few pieces of metadata that are related to the driver, rather than the
 * database, come from a separate InstructionReader.  Note that in that case it
 * probably doesn't make sense to allow an instruction to specify a query.
 *
 * @see <a href="http://java.sun.com/products/jdbc/download.html#corespec30">JDBC 3.0 Specification</a>
 */
public class EmbedDatabaseMetaData extends ConnectionChild
	implements DatabaseMetaData, java.security.PrivilegedAction<Object> {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

    private static final int ILLEGAL_UDT_TYPE = 0;
    
	/*
	** Property and values related to using
	** stored prepared statements for metatdata.
	*/

	private final String url;
	
	/*
	** Set to true if metadata is off
	*/

	private	GenericConstantActionFactory	constantActionFactory;
    
	//////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	//////////////////////////////////////////////////////////////
	/**
	    @exception SQLException on error
	 */
	public EmbedDatabaseMetaData (EmbedConnection connection, String url) 
		throws SQLException {

	    super(connection);
		this.url = url;

	}

    /** Cached query descriptions from metadata.properties. */
    private static Properties queryDescriptions;
    /** Cached query descriptions from metadata_net.properties. */
    private static Properties queryDescriptions_net;
    /**
     * Return all queries found in either metadata.properties or
     * metadata_net.properties.
     *
     * @param net if <code>true</code>, read metadata_net.properties;
     * otherwise, read metadata.properties.
     * @return a <code>Properties</code> value with the queries
     */
    private Properties getQueryDescriptions(boolean net) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1176
        Properties p = net ? queryDescriptions_net : queryDescriptions;
        if (p != null) {
            return p;
        }
        loadQueryDescriptions();
        return net ? queryDescriptions_net : queryDescriptions;
    }

    /**
     * Read the query descriptions from metadata.properties and
     * metadata_net.properties. This method must be invoked from
     * within a privileged block.
     */
    private void PBloadQueryDescriptions() {
        String[] files = {
            "metadata.properties",
            "/org/apache/derby/impl/sql/catalog/metadata_net.properties"
        };
        Properties[] props = new Properties[files.length];
        for (int i = 0; i < files.length; ++i) {
            try {
                props[i] = new Properties();
                // SECURITY PERMISSION - IP3
                InputStream is = getClass().getResourceAsStream(files[i]);
                props[i].load(is);
                is.close();
            } catch (IOException ioe) {
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT("Error reading " + files[i], ioe);
                }
            }
        }
        queryDescriptions = props[0];
        queryDescriptions_net = props[1];
    }

	//////////////////////////////////////////////////////////////
	//
	// DatabaseMetaData interface
	//
	//////////////////////////////////////////////////////////////

    //----------------------------------------------------------------------
	// First, a variety of minor information about the target database.

    /**
     * Can all the procedures returned by getProcedures be called by the
     * current user?
     *
     * @return true if so
     */
	public boolean allProceduresAreCallable() {
		return true;
	}

    /**
     * Can all the tables returned by getTable be SELECTed by the
     * current user?
     *
     * @return true if so
     */
	public boolean allTablesAreSelectable() {
		return true;
	}

    /**
     * What's the url for this database?
     *
     * @return the url or null if it can't be generated
     */
	public final String getURL()  {

		if (url == null)
			return url;
		int attributeStart = url.indexOf(';');
		if (attributeStart == -1)
			return url;
		else
			return url.substring(0,attributeStart);
	}

    /**
     * What's our user name as known to the database?
     *
     * @return our database user name
     */
	public String getUserName() {
		return (getEmbedConnection().getTR().getUserName());
	}

    /**
     * Is the database in read-only mode?
     *
     * @return true if so
     */
	public boolean isReadOnly() {
		return getLanguageConnectionContext().getDatabase().isReadOnly();
	}

    /**
     * Are NULL values sorted high?
     *
     * @return true if so
     */
	public boolean nullsAreSortedHigh() {
		return true;
	}

    /**
     * Are NULL values sorted low?
     *
     * @return true if so
     */
	public boolean nullsAreSortedLow() {
		return false;
	}

    /**
     * Are NULL values sorted at the start regardless of sort order?
     *
     * @return true if so
     */
	public boolean nullsAreSortedAtStart() {
		return false;
	}

    /**
     * Are NULL values sorted at the end regardless of sort order?
     *
     * @return true if so
     */
	public boolean nullsAreSortedAtEnd() {
		return false;
	}

    /**
     * What's the name of this database product?
     *
     * @return database product name
     */
	public String getDatabaseProductName() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
		return EmbedConnection.getMonitor().getEngineVersion().getProductName();
	}

    /**
     * What's the version of this database product?
     *
     * @return database version
     */
	public String getDatabaseProductVersion() {
		ProductVersionHolder myPVH = EmbedConnection.getMonitor().getEngineVersion();
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

//IC see: https://issues.apache.org/jira/browse/DERBY-1146
//IC see: https://issues.apache.org/jira/browse/DERBY-1547
		return myPVH.getVersionBuildString(true);
	}

    /**
     * What's the name of this JDBC driver?
     *
     * @return JDBC driver name
     */
	public String getDriverName() {
		return "Apache Derby Embedded JDBC Driver";
	}

    /**
     * What's the version of this JDBC driver?
     *
     * @return JDBC driver version
     */
	public String getDriverVersion()  {
		return getDatabaseProductVersion();
	}

    /**
     * What's this JDBC driver's major version number?
     *
     * @return JDBC driver major version
     */
	public int getDriverMajorVersion() {
		return getEmbedConnection().getLocalDriver().getMajorVersion();
	}

    /**
     * What's this JDBC driver's minor version number?
     *
     * @return JDBC driver minor version number
     */
	public int getDriverMinorVersion() {
		return getEmbedConnection().getLocalDriver().getMinorVersion();
	}

    /**
     * Does the database store tables in a local file?
     *
     * @return true if so
     */
	public boolean usesLocalFiles() {
        if (url.contains("memory"))
            return false;
        else
            return true;
	}

    /**
     * Does the database use a file for each table?
     *
     * @return true if the database uses a local file for each table
     */
	public boolean usesLocalFilePerTable() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6662
//IC see: https://issues.apache.org/jira/browse/DERBY-6662
        if (url.contains("memory"))
            return false;
        else
            return true;
	}

    /**
     * Does the database treat mixed case unquoted SQL identifiers as
     * case sensitive and as a result store them in mixed case?
     *
     * A JDBC-Compliant driver will always return false.
     *
     * @return true if so
     */
	public boolean supportsMixedCaseIdentifiers() {
		return false;
	}

    /**
     * Does the database treat mixed case unquoted SQL identifiers as
     * case insensitive and store them in upper case?
     *
     * @return true if so
     */
	public boolean storesUpperCaseIdentifiers() {
		return true;
	}

    /**
     * Does the database treat mixed case unquoted SQL identifiers as
     * case insensitive and store them in lower case?
     *
     * @return true if so
     */
	public boolean storesLowerCaseIdentifiers() {
		return false;
	}

    /**
     * Does the database treat mixed case unquoted SQL identifiers as
     * case insensitive and store them in mixed case?
     *
     * @return true if so
     */
	public boolean storesMixedCaseIdentifiers() {
		return false;
	}

    /**
     * Does the database treat mixed case quoted SQL identifiers as
     * case sensitive and as a result store them in mixed case?
     *
     * A JDBC-Compliant driver will always return true.
     *
     * @return true if so
     */
	public boolean supportsMixedCaseQuotedIdentifiers() {
		return true;
	}

    /**
     * Does the database treat mixed case quoted SQL identifiers as
     * case insensitive and store them in upper case?
     *
     * @return true if so
     */
	public boolean storesUpperCaseQuotedIdentifiers() {
		return false;
	}

    /**
     * Does the database treat mixed case quoted SQL identifiers as
     * case insensitive and store them in lower case?
     *
     * @return true if so
     */
	public boolean storesLowerCaseQuotedIdentifiers() {
		return false;
	}

    /**
     * Does the database treat mixed case quoted SQL identifiers as
     * case insensitive and store them in mixed case?
     *
     * @return true if so
     */
	public boolean storesMixedCaseQuotedIdentifiers() {
		return true;
	}

    /**
     * What's the string used to quote SQL identifiers?
     * This returns a space " " if identifier quoting isn't supported.
     *
     * A JDBC-Compliant driver always uses a double quote character.
     *
     * @return the quoting string
     */
	public String getIdentifierQuoteString() {
		return "\"";
	}

    /**
     * Get a comma separated list of all a database's SQL keywords
     * that are NOT also SQL92 keywords.
	includes reserved and non-reserved keywords.

     * @return the list
     */
	public String getSQLKeywords() {
		return "ALIAS,BIGINT,BOOLEAN,CALL,CLASS,COPY,DB2J_DEBUG,EXECUTE,EXPLAIN,FILE,FILTER,"
//IC see: https://issues.apache.org/jira/browse/DERBY-2258
//IC see: https://issues.apache.org/jira/browse/DERBY-2259
//IC see: https://issues.apache.org/jira/browse/DERBY-2260
//IC see: https://issues.apache.org/jira/browse/DERBY-2245
			+  "GETCURRENTCONNECTION,INDEX,INSTANCEOF,METHOD,NEW,OFF,PROPERTIES,RECOMPILE,"
			+  "RENAME,RUNTIMESTATISTICS,STATEMENT,STATISTICS,TIMING,WAIT";
	}

    /**
     * Get a comma separated list of JDBC escaped numeric functions.
     * Must be a complete or sub set of functions in appendix C.1 
     * of JDBC 3.0 specification (pp. 183).
     * @return the list
     */
	public String getNumericFunctions() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3063
		return "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,PI,RADIANS,RAND,SIGN,SIN,SQRT,TAN";
	}

    /**
     * Get a comma separated list of JDBC escaped string functions.
     * Must be a complete or sub set of functions in appendix C.2
     * of JDBC 3.0 specification (pp. 184).
     * @return the list
     */
	public String getStringFunctions() {
		return "CONCAT,LENGTH,LCASE,LOCATE,LTRIM,RTRIM,SUBSTRING,UCASE";
	}

    /**
     * Get a comma separated list of JDBC escaped system functions.
     * Must be a complete or sub set of functions in appendix C.4
     * of JDBC 3.0 specification (pp. 185).
     * @return the list
     */
	public String getSystemFunctions()  {
		return "USER";
	}

    /**
     * Get a comma separated list of JDBC escaped time date functions.
     * Must be a complete or sub set of functions in appendix C.3
     * of JDBC 3.0 specification.
      * @return the list
     */
	public String getTimeDateFunctions() {
//IC see: https://issues.apache.org/jira/browse/DERBY-471
		return "CURDATE,CURTIME,HOUR,MINUTE,MONTH,SECOND,TIMESTAMPADD,TIMESTAMPDIFF,YEAR";
	}

    /**
     * This is the string that can be used to escape '_' or '%' in
     * the string pattern style catalog search parameters.
        we have no default escape value, so = is the end of the next line
     * <P>The '_' character represents any single character.
     * <P>The '%' character represents any sequence of zero or
     * more characters.
     * @return the string used to escape wildcard characters
     */
	public String getSearchStringEscape()  {
		return "";
	}

    /**
     * Get all the "extra" characters that can be used in unquoted
     * identifier names (those beyond a-z, A-Z, 0-9 and _).
     *
     * @return the string containing the extra characters
     */
	public String getExtraNameCharacters()  {
		return "";
	}

    //--------------------------------------------------------------------
    // Functions describing which features are supported.

    /**
     * Is "ALTER TABLE" with add column supported?
     *
     * @return true if so
     */
	public boolean supportsAlterTableWithAddColumn() {
		return true;
	}

    /**
     * Is "ALTER TABLE" with drop column supported?
     *
     * @return true if so
     */
	public boolean supportsAlterTableWithDropColumn() {
		return true;
	}

    /**
     * Is column aliasing supported?
     *
     * <P>If so, the SQL AS clause can be used to provide names for
     * computed columns or to provide alias names for columns as
     * required.
     *
     * A JDBC-Compliant driver always returns true.
     *
     * @return true if so
     */
	public boolean supportsColumnAliasing() {
		return true;
	}

    /**
     * Are concatenations between NULL and non-NULL values NULL?
     *
     * A JDBC-Compliant driver always returns true.
     *
     * @return true if so
     */
	public boolean nullPlusNonNullIsNull()  {
		return true;
	}

    /**
     * Is the CONVERT function between SQL types supported?
     *
     * @return true if so
     */
	public boolean supportsConvert() {
		return false;
	}

    /**
     * Is CONVERT between the given SQL types supported?
     *
     * @param fromType the type to convert from
     * @param toType the type to convert to
     * @return true if so
     * @see Types
     */
	public boolean supportsConvert(int fromType, int toType) {
		/*
		 * at the moment we don't support CONVERT at all, so we take the easy
		 * way out.  Eventually we need to figure out how to handle this
		 * cleanly.
		 */
		return false;
	}

    /**
     * Are table correlation names supported?
     *
     * A JDBC-Compliant driver always returns true.
     *
     * @return true if so
     */
	public boolean supportsTableCorrelationNames()  {
		return true;
	}

    /**
     * If table correlation names are supported, are they restricted
     * to be different from the names of the tables?
     *
     * @return true if so
     */
	public boolean supportsDifferentTableCorrelationNames() {
		return true;
	}

    /**
     * Are expressions in "ORDER BY" lists supported?
     *
     * @return true if so
     */
	public boolean supportsExpressionsInOrderBy() {
		/* DERBY - 2244 : Derby does support Order By Expression (Derby-134)
		* thus changing the return value to true to relfect that the support
		* is present
		*/
		return true;
	}

    /**
     * Can an "ORDER BY" clause use columns not in the SELECT?
     *
     * @return true if so
     */
	public boolean supportsOrderByUnrelated() {
		return false;
	}

    /**
     * Is some form of "GROUP BY" clause supported?
     *
     * @return true if so
     */
	public boolean supportsGroupBy() {
		return true;
	}

    /**
     * Can a "GROUP BY" clause use columns not in the SELECT?
     *
     * @return true if so
     */
	public boolean supportsGroupByUnrelated()  {
		return true;
	}

    /**
     * Can a "GROUP BY" clause add columns not in the SELECT
     * provided it specifies all the columns in the SELECT?
     *
     * @return true if so
     */
	public boolean supportsGroupByBeyondSelect() {
		return true;
	}

    /**
     * Is the escape character in "LIKE" clauses supported?
     *
     * A JDBC-Compliant driver always returns true.
     *
     * @return true if so
     */
	public boolean supportsLikeEscapeClause() {
		return true;
	}

    /**
     * Are multiple ResultSets from a single execute supported?
     *
     * @return true if so
     */
	public boolean supportsMultipleResultSets()  {
		return true;
	}

    /**
     * Can we have multiple transactions open at once (on different
     * connections)?
     *
     * @return true if so
     */
	public boolean supportsMultipleTransactions() {
		return true;
	}

    /**
     * Can columns be defined as non-nullable?
     *
     * A JDBC-Compliant driver always returns true.
     *
     * @return true if so
     */
	public boolean supportsNonNullableColumns()  {
		return true;
	}

    /**
     * Is the ODBC Minimum SQL grammar supported?
     *
     * All JDBC-Compliant drivers must return true.
     *
     * @return true if so
     */
	public boolean supportsMinimumSQLGrammar() {
		return true;
	}

    /**
     * Is the ODBC Core SQL grammar supported?
     *
     * @return true if so
     */
	public boolean supportsCoreSQLGrammar() {
		return false;
	}

    /**
     * Is the ODBC Extended SQL grammar supported?
     *
     * @return true if so
     */
	public boolean supportsExtendedSQLGrammar() {
		return false;
	}

    /**
     * Is the ANSI92 entry level SQL grammar supported?
     *
     * All JDBC-Compliant drivers must return true.
     *
     * @return true if so
     */
	public boolean supportsANSI92EntryLevelSQL() {
		/* DERBY - 2243 : Derby does support ANSI 92 standards,
		* thus changing the return value to true to relfect that the support
		* is present
		*/
		return true;
	}

    /**
     * Is the ANSI92 intermediate SQL grammar supported?
     *
     * @return true if so
	 * 
     */
	public boolean supportsANSI92IntermediateSQL() {
		return false;
	}

    /**
     * Is the ANSI92 full SQL grammar supported?
     *
     * @return true if so
	 * 
     */
	public boolean supportsANSI92FullSQL() {
		return false;
	}

    /**
     * Is the SQL Integrity Enhancement Facility supported?
     *
     * @return true if so
	 * 
     */
	public boolean supportsIntegrityEnhancementFacility() {
		return false;
	}

    /**
     * Is some form of outer join supported?
     *
     * @return true if so
	 * 
     */
	public boolean supportsOuterJoins() {
		return true;
	}

    /**
     * Are full nested outer joins supported?
     *
     * @return true if so
	 * 
     */
	public boolean supportsFullOuterJoins()  {
		return false;
	}

    /**
     * Is there limited support for outer joins?  (This will be true
     * if supportFullOuterJoins is true.)
     *
     * @return true if so
	 * 
     */
	public boolean supportsLimitedOuterJoins() {
		return true;
	}

    /**
     * What's the database vendor's preferred term for "schema"?
     *
     * @return the vendor term
	 * 
     */
	public String getSchemaTerm() {
		return "SCHEMA";
	}

    /**
     * What's the database vendor's preferred term for "procedure"?
     *
     * @return the vendor term
	 * 
     */
	public String getProcedureTerm() {
		return "PROCEDURE";
	}

    /**
     * What's the database vendor's preferred term for "catalog"?
     *
     * @return the vendor term
	 * 
     */
	public String getCatalogTerm() {
		return "CATALOG";
	}

    /**
     * Does a catalog appear at the start of a qualified table name?
     * (Otherwise it appears at the end)
     *
     * @return true if it appears at the start
	 * 
     */
	public boolean isCatalogAtStart() {
		return false;
	}

    /**
     * What's the separator between catalog and table name?
     *
     * @return the separator string
	 * 
     */
	public String getCatalogSeparator() {
		return "";
	}

    /**
     * Can a schema name be used in a data manipulation statement?
     *
     * @return true if so
	 * 
     */
	public boolean supportsSchemasInDataManipulation() {
		return true;
	}

    /**
     * Can a schema name be used in a procedure call statement?
     *
     * @return true if so
	 * 
     */
	public boolean supportsSchemasInProcedureCalls() {
		return true;
	}

    /**
     * Can a schema name be used in a table definition statement?
     *
     * @return true if so
	 * 
     */
	public boolean supportsSchemasInTableDefinitions() {
		return true;
	}

    /**
     * Can a schema name be used in an index definition statement?
     *
     * @return true if so
     */
	public boolean supportsSchemasInIndexDefinitions() {
		return true;
	}

    /**
     * Can a schema name be used in a privilege definition statement?
     *
     * @return true if so
	 * 
     */
	public boolean supportsSchemasInPrivilegeDefinitions() {
		return true;
	}

    /**
     * Can a catalog name be used in a data manipulation statement?
     *
     * @return true if so
	 * 
     */
	public boolean supportsCatalogsInDataManipulation() {
		return false;
	}

    /**
     * Can a catalog name be used in a procedure call statement?
     *
     * @return true if so
	 * 
     */
	public boolean supportsCatalogsInProcedureCalls() {
		return false;
	}

    /**
     * Can a catalog name be used in a table definition statement?
     *
     * @return true if so
	 * 
     */
	public boolean supportsCatalogsInTableDefinitions() {
		return false;
	}

    /**
     * Can a catalog name be used in an index definition statement?
     *
     * @return true if so
     */
	public boolean supportsCatalogsInIndexDefinitions() {
		return false;
	}

    /**
     * Can a catalog name be used in a privilege definition statement?
     *
     * @return true if so
     */
	public boolean supportsCatalogsInPrivilegeDefinitions() {
		return false;
	}


    /**
     * Is positioned DELETE supported?
     *
     * @return true if so
     */
	public boolean supportsPositionedDelete() {
		return true;
	}

    /**
     * Is positioned UPDATE supported?
     *
     * @return true if so
     */
	public boolean supportsPositionedUpdate() {
		return true;
	}

    /**
     * Added in JDBC 4.2.
     *
     * Derby does not support the Types.REF_CURSOR type.
     *
     * @return true if so
     */
	public boolean supportsRefCursors()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
		return false;
	}
    
    /**
     * Is SELECT for UPDATE supported?
     *
     * @return true if so
     */
	public boolean supportsSelectForUpdate() {
		return true;
	}

    /**
     * Are stored procedure calls using the stored procedure escape
     * syntax supported?
     *
     * @return true if so
     */
	public boolean supportsStoredProcedures() {
		return true;
	}

    /**
     * Are subqueries in comparison expressions supported?
     *
     * A JDBC-Compliant driver always returns true.
     *
     * @return true if so
     */
	public boolean supportsSubqueriesInComparisons() {
		return true;
	}

    /**
     * Are subqueries in 'exists' expressions supported?
     *
     * A JDBC-Compliant driver always returns true.
     *
     * @return true if so
     */
	public boolean supportsSubqueriesInExists() {
		return true;
	}

    /**
     * Are subqueries in 'in' statements supported?
     *
     * A JDBC-Compliant driver always returns true.
     *
     * @return true if so
     */
	public boolean supportsSubqueriesInIns() {
		return true;
	}

    /**
     * Are subqueries in quantified expressions supported?
     *
     * A JDBC-Compliant driver always returns true.
     *
     * @return true if so
     */
	public boolean supportsSubqueriesInQuantifieds() {
		return true;
	}

    /**
     * Are correlated subqueries supported?
     *
     * A JDBC-Compliant driver always returns true.
     *
     * @return true if so
     */
	public boolean supportsCorrelatedSubqueries() {
		return true;
	}

    /**
     * Is SQL UNION supported?
     *
     * @return true if so
     */
	public boolean supportsUnion() {
		return true;
	}

    /**
     * Is SQL UNION ALL supported?
     *
     * @return true if so
     */
	public boolean supportsUnionAll() {
		return true;
	}

    /**
     * Can cursors remain open across commits?
     *
     * @return true if cursors always remain open; false if they might not remain open
     */
	//returns false because Derby does not support cursors that are open across commits for XA transactions.
	public boolean supportsOpenCursorsAcrossCommit() {
		return false;
	}

    /**
     * Can cursors remain open across rollbacks?
     *
     * @return true if cursors always remain open; false if they might not remain open
     */
	public boolean supportsOpenCursorsAcrossRollback() {
		return false;
	}

    /**
     * Can statements remain open across commits?
     *
     * @return true if statements always remain open; false if they might not remain open
     */
	public boolean supportsOpenStatementsAcrossCommit() {
		return true;
	}

    /**
     * Can statements remain open across rollbacks?
     *
     * @return true if statements always remain open; false if they might not remain open
     */
	public boolean supportsOpenStatementsAcrossRollback() {
		return false;
	}



    //----------------------------------------------------------------------
    // The following group of methods exposes various limitations
    // based on the target database with the current driver.
    // Unless otherwise specified, a result of zero means there is no
    // limit, or the limit is not known.

    /**
     * How many hex characters can you have in an inline binary literal?
     *
     * @return max literal length
     */
	public int getMaxBinaryLiteralLength() {
		return 0;
	}

    /**
     * What's the max length for a character literal?
     *
     * @return max literal length
     */
	public int getMaxCharLiteralLength() {
		return 0;
	}

    /**
     * What's the limit on column name length?
     *
     * @return max literal length
     */
	public int getMaxColumnNameLength() {
		return Limits.MAX_IDENTIFIER_LENGTH;
	}

    /**
     * What's the maximum number of columns in a "GROUP BY" clause?
     *
     * @return max number of columns
     */
	public int getMaxColumnsInGroupBy() {
		return 0;
	}

    /**
     * What's the maximum number of columns allowed in an index?
     *
     * @return max columns
     */
	public int getMaxColumnsInIndex() {
		return 0;
	}

    /**
     * What's the maximum number of columns in an "ORDER BY" clause?
     *
     * @return max columns
     */
	public int getMaxColumnsInOrderBy() {
		return 0;
	}

    /**
     * What's the maximum number of columns in a "SELECT" list?
     *
     * we don't have a limit...
     *
     * @return max columns
     */
	public int getMaxColumnsInSelect() {
		return 0;
	}

    /**
     * What's the maximum number of columns in a table?
     *
     * @return max columns
     */
	public int getMaxColumnsInTable()  {
		return 0;
	}

    /**
     * How many active connections can we have at a time to this database?
     *
     * @return max connections
     */
	public int getMaxConnections() {
		return 0;
	}

    /**
     * What's the maximum cursor name length?
     *
     * @return max cursor name length in bytes
     */
	public int getMaxCursorNameLength() {
		return Limits.MAX_IDENTIFIER_LENGTH;
	}

    /**
     * What's the maximum length of an index (in bytes)?
     *
     * @return max index length in bytes
     */
	public int getMaxIndexLength() {
		return 0;
	}

    /**
     * Added in JDBC 4.2.
     *
     * What's the maximum length of Derby LOB? This
     * is the maximum number of bytes in a LOB. We return the
     * default value of 0, which means "unknown". The maximum size
     * of a CLOB is a complicated because it depends on how many
     * bytes are needed to encode its string value on disk.
     *
     * @return max index length in bytes
     */
	public long getMaxLogicalLobSize() { return 0L; }

    /**
     * What's the maximum length allowed for a schema name?
     *
     * @return max name length in bytes
     */
	public int getMaxSchemaNameLength()  {
		return Limits.MAX_IDENTIFIER_LENGTH;
	}

    /**
     * What's the maximum length of a procedure name?
     *
     * @return max name length in bytes
     */
	public int getMaxProcedureNameLength() {
		return Limits.MAX_IDENTIFIER_LENGTH;
	}

    /**
     * What's the maximum length of a catalog name?
     *
     * @return max name length in bytes
     */
	public int getMaxCatalogNameLength()  {
		return 0;
	}

    /**
     * What's the maximum length of a single row?
     *
     * @return max row size in bytes
     */
	public int getMaxRowSize() {
		return 0;
	}

    /**
     * Did getMaxRowSize() include LONGVARCHAR and LONGVARBINARY
     * blobs?
     *
     * @return true if so
     */
	public boolean doesMaxRowSizeIncludeBlobs() {
		return true;
	}

    /**
     * What's the maximum length of a SQL statement?
     *
     * @return max length in bytes
     */
	public int getMaxStatementLength() {
		return 0;
	}

    /**
     * How many active statements can we have open at one time to this
     * database?
     *
     * @return the maximum
     */
	public int getMaxStatements() {
		return 0;
	}

    /**
     * What's the maximum length of a table name?
     *
     * @return max name length in bytes
     */
	public int getMaxTableNameLength() {
		return Limits.MAX_IDENTIFIER_LENGTH;
	}

    /**
     * What's the maximum number of tables in a SELECT?
     *
     * @return the maximum
     */
	public int getMaxTablesInSelect() {
		return 0;
	}

    /**
     * What's the maximum length of a user name?
     *
     * @return max name length  in bytes
     */
	public int getMaxUserNameLength() {
//IC see: https://issues.apache.org/jira/browse/DERBY-104
//IC see: https://issues.apache.org/jira/browse/DERBY-104
//IC see: https://issues.apache.org/jira/browse/DERBY-104
//IC see: https://issues.apache.org/jira/browse/DERBY-104
//IC see: https://issues.apache.org/jira/browse/DERBY-104
//IC see: https://issues.apache.org/jira/browse/DERBY-3146
		return Limits.MAX_IDENTIFIER_LENGTH;
	}

    //----------------------------------------------------------------------

    /**
     * What's the database's default transaction isolation level?  The
     * values are defined in java.sql.Connection.
     *
     * @return the default isolation level
     * @see Connection
     */
	public int getDefaultTransactionIsolation() {
		return java.sql.Connection.TRANSACTION_READ_COMMITTED;
	}

    /**
     * Are transactions supported? If not, commit is a noop and the
     * isolation level is TRANSACTION_NONE.
     *
     * @return true if transactions are supported
     */
	public boolean supportsTransactions()  {
		return true;
	}

    /**
     * Does the database support the given transaction isolation level?
	 *
	 * DatabaseMetaData.supportsTransactionIsolation() should return false for
	 * isolation levels that are not supported even if a higher level can be
	 * substituted.
     *
     * @param level the values are defined in java.sql.Connection
     * @return true if so
     * @see Connection
		*/	
	public boolean supportsTransactionIsolationLevel(int level)
							 {
		// REMIND: This is hard-coded for the moment because it doesn't nicely
		// fit within the framework we've set up for the rest of these values.
		// Part of the reason is that it has a parameter, so it's not just a
		// simple value look-up.  Some ideas for the future on how to make this
		// not hard-coded:
		//	  - code it as a query: "select true from <something> where ? in
		//      (a,b,c)" where a,b,c are the supported isolation levels.  The
		//      parameter would be set to "level".  This seems awfully awkward.
		//    - somehow what you'd really like is to enable the instructions
		//      file to contain the list, or set, of supported isolation
		//      levels.  Something like:
		//          supportsTr...ionLevel=SERIALIZABLE | REPEATABLE_READ | ...
		//      That would take some more code that doesn't seem worthwhile at
		//      the moment for this one case.

		/*
			REMIND: this could be moved into a query that is e.g.
			VALUES ( ? in (8,...) )
			so that database could control the list of supported
			isolations.  For now, it's hard coded, and just the one.
		 */

		return (level == Connection.TRANSACTION_SERIALIZABLE    ||
		        level == Connection.TRANSACTION_REPEATABLE_READ ||
			    level == Connection.TRANSACTION_READ_COMMITTED  ||
			    level == Connection.TRANSACTION_READ_UNCOMMITTED);
	}

    /**
     * Are both data definition and data manipulation statements
     * within a transaction supported?
     *
     * @return true if so
     */
	public boolean supportsDataDefinitionAndDataManipulationTransactions() {
			 return true;
	}
    /**
     * Are only data manipulation statements within a transaction
     * supported?
     *
     * @return true if so
     */
	public boolean supportsDataManipulationTransactionsOnly()
	{
			 return false;
	}
    /**
     * Does a data definition statement within a transaction force the
     * transaction to commit?
     *
     * @return true if so
	 * 
     */
	public boolean dataDefinitionCausesTransactionCommit() {
		return false;
	}
    /**
     * Is a data definition statement within a transaction ignored?
     *
     * @return true if so
	 * 
     */
	public boolean dataDefinitionIgnoredInTransactions(){
		return false;
	}


    /**
     * Get a description of stored procedures available in a
     * catalog.
     *
     * <P>Only procedure descriptions matching the schema and
     * procedure name criteria are returned.  They are ordered by
     * PROCEDURE_SCHEM, and PROCEDURE_NAME.
     *
     * <P>Each procedure description has the the following columns:
     *  <OL>
     *	<LI><B>PROCEDURE_CAT</B> String =&gt; procedure catalog (may be null)
     *	<LI><B>PROCEDURE_SCHEM</B> String =&gt; procedure schema (may be null)
     *	<LI><B>PROCEDURE_NAME</B> String =&gt; procedure name
     *  <LI> reserved for future use
     *  <LI> reserved for future use
     *  <LI> reserved for future use
     *	<LI><B>REMARKS</B> String =&gt; explanatory comment on the procedure
     *	<LI><B>PROCEDURE_TYPE</B> short =&gt; kind of procedure:
     *      <UL>
     *      <LI> procedureResultUnknown - May return a result
     *      <LI> procedureNoResult - Does not return a result
     *      <LI> procedureReturnsResult - Returns a result
     *      </UL>
     *  <LI><B>SPECIFIC_NAME</B> String =&gt; The name which uniquely 
     *  identifies this procedure within its schema (since JDBC 4.0)
     *  </OL>
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     * without a schema
     * @param procedureNamePattern a procedure name pattern
     * @return ResultSet - each row is a procedure description
     * @see #getSearchStringEscape
     * @exception SQLException thrown on failure.
     */
	public ResultSet getProcedures(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException {

        // Using the new JDBC 4.0 version of the query here. The query
        // was given a new name to allow the old query to
        // be used by ODBCMetaDataGenerator.
//IC see: https://issues.apache.org/jira/browse/DERBY-107
		return doGetProcs(catalog, schemaPattern,
//IC see: https://issues.apache.org/jira/browse/DERBY-1093
			procedureNamePattern, "getProcedures40");
	}

	/**
	 * Get a description of stored procedures available in a
	 * catalog.  Same as getProcedures() above, except that
	 * the result set will conform to ODBC specifications.
	 */
	public ResultSet getProceduresForODBC(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException {

        // For ODBC we still use the transformed version of the JDBC
        // 3.0 query, (may change in the future).
		return doGetProcs(catalog, schemaPattern,
			procedureNamePattern, "odbc_getProcedures");
	}
    
    /**
     * Implements DatabaseMetaData.getFunctions() for an embedded
     * database. Queries the database to get information about
     * functions (procedures returning values). Executes the
     * 'getFunctions' query from metadata.properties to obtain the
     * ResultSet to return.<p> Compatibility: This is a new method in
     * the API which is only available with with Derby versions &gt; 10.1 and
     * JDK versions &gt;= 1.6 <p>Upgrade: Since this is a new query it
     * does not have an SPS, and will be available as soon as any
     * database, new or old, is booted with the new version of Derby,
     * (in <b>soft and hard</b> upgrade).
     * @param catalog limit the search to functions in this catalog 
     * (not used)
     * @param schemaPattern limit the search to functions in schemas 
     * matching this pattern
     * @param functionNamePattern limit the search to functions 
     * matching this pattern
     * @return a ResultSet with metadata information
     * @throws SQLException if any of the underlying jdbc methods fail
     */
    public ResultSet getFunctions(java.lang.String catalog,
//IC see: https://issues.apache.org/jira/browse/DERBY-924
                                  java.lang.String schemaPattern,
                                  java.lang.String functionNamePattern)
        throws SQLException
    {
        return doGetProcs(catalog, schemaPattern,
                   functionNamePattern, "getFunctions");
    }

	/**
	 * Does the actual work for the getProcedures and getFunctions
	 * metadata calls.  See getProcedures() method above for parameter
	 * descriptions.
	 * @param queryName Name of the query to execute; is used
	 *	to determine whether the result set should conform to
	 *	JDBC or ODBC specifications.
	 */
	private ResultSet doGetProcs(String catalog, String schemaPattern,
		String procedureNamePattern, String queryName)
		throws SQLException {

		PreparedStatement s = getPreparedQuery(queryName);
		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schemaPattern));
		s.setString(3, swapNull(procedureNamePattern));
		return s.executeQuery();
	}

    /**
     * Get a description of a catalog's stored procedure parameters
     * and result columns.
     *
     * <P>Only descriptions matching the schema, procedure and
     * parameter name criteria are returned.  They are ordered by
     * PROCEDURE_SCHEM and PROCEDURE_NAME. Within this, the return value,
     * if any, is first. Next are the parameter descriptions in call
     * order. The column descriptions follow in column number order.
     *
     * <P>Each row in the ResultSet is a parameter description or
     * column description with the following fields:
     *  <OL>
     *	<LI><B>PROCEDURE_CAT</B> String =&gt; procedure catalog (may be null)
     *	<LI><B>PROCEDURE_SCHEM</B> String =&gt; procedure schema (may be null)
     *	<LI><B>PROCEDURE_NAME</B> String =&gt; procedure name
     *	<LI><B>COLUMN_NAME</B> String =&gt; column/parameter name
     *	<LI><B>COLUMN_TYPE</B> Short =&gt; kind of column/parameter:
     *      <UL>
     *      <LI> procedureColumnUnknown - nobody knows
     *      <LI> procedureColumnIn - IN parameter
     *      <LI> procedureColumnInOut - INOUT parameter
     *      <LI> procedureColumnOut - OUT parameter
     *      <LI> procedureColumnReturn - procedure return value
     *      <LI> procedureColumnResult - result column in ResultSet
     *      </UL>
     *  <LI><B>DATA_TYPE</B> int =&gt; SQL type from java.sql.Types
     *	<LI><B>TYPE_NAME</B> String =&gt; SQL type name
     *	<LI><B>PRECISION</B> int =&gt; precision
     *	<LI><B>LENGTH</B> int =&gt; length in bytes of data
     *	<LI><B>SCALE</B> short =&gt; scale
     *	<LI><B>RADIX</B> short =&gt; radix
     *	<LI><B>NULLABLE</B> short =&gt; can it contain NULL?
     *      <UL>
     *      <LI> procedureNoNulls - does not allow NULL values
     *      <LI> procedureNullable - allows NULL values
     *      <LI> procedureNullableUnknown - nullability unknown
     *      </UL>
     *	<LI><B>REMARKS</B> String =&gt; comment describing parameter/column
     *	<LI><B>COLUMN_DEF</B> String
     *	<LI><B>SQL_DATA_TYPE</B> int
     *	<LI><B>SQL_DATETIME_SUB</B> int
     *	<LI><B>CHAR_OCTET_LENGTH</B> int
     *	<LI><B>ORDINAL_POSITION</B> int
     *	<LI><B>IS_NULLABLE</B> String
     *	<LI><B>SPECIFIC_NAME</B> String
     *  </OL>
     *
     * <P><B>Note:</B> Some databases may not return the column
     * descriptions for a procedure. Additional columns beyond
     * SPECIFIC_NAME can be defined by the database.
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     * without a schema
     * @param procedureNamePattern a procedure name pattern
     * @param columnNamePattern a column name pattern
     * @return ResultSet - each row is a stored procedure parameter or
     *      column description
     * @see #getSearchStringEscape
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getProcedureColumns(String catalog,
			String schemaPattern,
			String procedureNamePattern,
			String columnNamePattern) throws SQLException {

		// Using the new JDBC 4.0 version of the query here. The query
		// was given a new name to allow the old query to
		// be used by ODBCMetaDataGenerator.
//IC see: https://issues.apache.org/jira/browse/DERBY-107
		return doGetProcCols(catalog, schemaPattern,
			procedureNamePattern, columnNamePattern,
//IC see: https://issues.apache.org/jira/browse/DERBY-1094
			"getProcedureColumns40");
	}

	/**
	 * Get a description of a catalog's stored procedure parameters
	 * and result columns.  Same as getProcedureColumns() above,
	 * except that the result set will conform to ODBC specifications.
	 */
	public ResultSet getProcedureColumnsForODBC(String catalog,
			String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException {

		// For ODBC we still use the transformed version of the JDBC
		// 3.0 query, (may change in the future).
		return doGetProcCols(catalog, schemaPattern,
			procedureNamePattern, columnNamePattern,
			"odbc_getProcedureColumns");
	}

    /**
     * Implements DatabaseMetaData.getFunctionColumns() for an embedded
     * database. Queries the database to get information about
     * function parameters. Executes the
     * 'getFunctionColumns' query from metadata.properties to obtain the
     * ResultSet.<p> Compatibility: This is a new method in
     * the API which is only available with with Derby versions &gt; 10.1 and
     * JDK versions &gt;= 1.6 <p>Upgrade: Since this is a new query it
     * does not have an SPS, and will be available as soon as any
     * database, new or old, is booted with the new version of Derby,
     * (in <b>soft and hard</b> upgrade).
     * @param catalog limit the search to functions in this catalog
     * (not used)
     * @param schemaPattern limit the search to functions in schemas
     * matching this pattern
     * @param functionNamePattern limit the search to functions
     * matching this pattern
     * @param parameterNamePattern limit the search parameters
     * matching this pattern
     * @return a ResultSet with metadata information
     * @throws SQLException if a database error occurs
     */
	public ResultSet getFunctionColumns(String catalog,
										   String schemaPattern,
										   String functionNamePattern,
										   String parameterNamePattern) 
		throws SQLException {

//IC see: https://issues.apache.org/jira/browse/DERBY-2145
        return doGetProcCols(catalog, 
                             schemaPattern,
                             functionNamePattern, 
                             parameterNamePattern,	
                             "getFunctionColumns");
	}

	/**
	 * Does the actual work for the getProcedureColumns metadata
	 * calls. See getProcedureColumns() method above for parameter
	 * descriptions.
	 * @param queryName Name of the query to execute; is used
	 *	to determine whether the result set should conform to
	 *	JDBC or ODBC specifications.
	 */
	private ResultSet doGetProcCols(String catalog, String schemaPattern,
			String procedureNamePattern, String columnNamePattern,
			String queryName) throws SQLException {

		PreparedStatement s = getPreparedQuery(queryName);
		// 
                // catalog is not part of the query
                //
		s.setString(1, swapNull(schemaPattern));
		s.setString(2, swapNull(procedureNamePattern));
		s.setString(3, swapNull(columnNamePattern));
		return s.executeQuery();
	}

    /**
     * Get a description of tables available in a catalog.
     *
     * <P>Only table descriptions matching the catalog, schema, table
     * name and type criteria are returned.  They are ordered by
     * TABLE_TYPE, TABLE_SCHEM and TABLE_NAME.
     *
     * <P>Each table description has the following columns:
     *  <OL>
     *	<LI><B>TABLE_CAT</B> String =&gt; table catalog (may be null)
     *	<LI><B>TABLE_SCHEM</B> String =&gt; table schema (may be null)
     *	<LI><B>TABLE_NAME</B> String =&gt; table name
     *	<LI><B>TABLE_TYPE</B> String =&gt; table type.  Typical types are "TABLE",
     *			"VIEW",	"SYSTEM TABLE", "GLOBAL TEMPORARY",
     *			"LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *	<LI><B>REMARKS</B> String =&gt; explanatory comment on the table
     *  <LI><B>TYPE_CAT</B> String =&gt; the types catalog (may be
     *          <code>null</code>)
     *  <LI><B>TYPE_SCHEM</B> String =&gt; the types schema (may be
     *          <code>null</code>)
     *  <LI><B>TYPE_NAME</B> String =&gt; type name (may be
     *          <code>null</code>)
     *  <LI><B>SELF_REFERENCING_COL_NAME</B> String =&gt; name of the
     *          designated "identifier" column of a typed table (may
     *          be <code>null</code>)
     *  <LI><B>REF_GENERATION</B> String =&gt; specifies how values in
     *          SELF_REFERENCING_COL_NAME are created. Values are
     *          "SYSTEM", "USER", "DERIVED". (may be
     *          <code>null</code>)
     *  </OL>
     *
     * <P><B>Note:</B> Some databases may not return information for
     * all tables.
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     * without a schema
     * @param tableNamePattern a table name pattern
     * @param types a list of table types to include; null returns all types
     * @return ResultSet - each row is a table description
     * @see #getSearchStringEscape
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getTables(String catalog, String schemaPattern,
		String tableNamePattern, String types[]) throws SQLException {
		PreparedStatement s = getPreparedQuery("getTables");
		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schemaPattern));
		s.setString(3, swapNull(tableNamePattern));
		//IMPORTANT
		//Whenever a new table type is added to Derby, the sql for 
		//getTables in metadata.properties will have to change and the 
		//following if else will need to be modified too. 
		//
		//The getTables sql in metadata.properties has following clause 
		//TABLETYPE IN (?, ?, ?, ?)
		//There are 4?s for IN list because Derby supports 4 tables types 
		//at the moment which are 'T','S','V' and 'A'.
		//Anytime a new table type is added, an additional ? should be
		//added to the above clause. In addition, the following code will 
		//have to change too because it will need to set value for that 
		//additional ?.
		//
		//Following explains the logic for table types handling.
		//If the user has asked for specific table types in getTables,
		//then the "if" statement below will use those types values
		//for ?s. If there are still some ?s in the IN list that are left 
		//with unassigned values, then we will set those ? to NULL.
		// So parameter 4 will be "T" for TABLE, 5 will be "V" for VIEW, 6 will be
		// "A" for SYNONYM, 7 will be "S" for system table in 
		//TABLETYPE IN (?, ?, ?, ?)
		// If the user hasn't asked for any specific table types then all
		// four values will be set.
		// When a new table type is added to Derby we will have to add a 
		// parameter to the metadata statement and handle it here.
		
		// Array for type parameters
//IC see: https://issues.apache.org/jira/browse/DERBY-3000
//IC see: https://issues.apache.org/jira/browse/DERBY-1790
		final int numberOfTableTypesInDerby = 4;
		if (types == null)  {// null means all types 
			types = new String[] {"TABLE","VIEW","SYNONYM","SYSTEM TABLE"};
		}
		String[] typeParams = new String[numberOfTableTypesInDerby];
		for (int i=0; i < numberOfTableTypesInDerby;i++)
			typeParams[i] = null;
		
		for (int i = 0; i<types.length; i++){
			if ("TABLE".equals(types[i]))
				typeParams[0] = "T";
			else if ("VIEW".equals(types[i]))
				typeParams[1] = "V";
			else if ("SYNONYM".equals(types[i]))
				typeParams[2] = "A";
			else if ("SYSTEM TABLE".equals(types[i]) ||
					"SYSTEM_TABLE".equals(types[i])) // Keep SYSTEM_TABLE since this is how we have been testing
					typeParams[3] = "S";
			// If user puts in other types we simply ignore.
			}
		
		// 	TABLETYPE IN (?,?,?,?) starts at parameter 4 so we add 4
		// Set to value passed in or null if no value was given.
		for (int i=0; i < numberOfTableTypesInDerby; i++)
			if (typeParams[i] == null)
				s.setNull(i+4,Types.CHAR);
			else
				s.setString(i+4,typeParams[i]);	
					
		return s.executeQuery();
	}

    /**
     * Get the schema names available in this database.  The results
     * are ordered by schema name.
     *
     * <P>The schema columns are:
     *  <OL>
     *  <li><strong>TABLE_SCHEM</strong> String =&gt; schema name</li>
     *  <li><strong>TABLE_CATALOG</strong> String =&gt; catalog name
     *  (may be <code>null</code>)</li>
     *  </OL>
     *
     * @return ResultSet - each row is a schema description
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getSchemas() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-876
		return getSchemas(null, null);
	}

    /**
     * Get the catalog names available in this database.  The results
     * are ordered by catalog name.
     *
     * <P>The catalog column is:
     *  <OL>
     *	<LI><B>TABLE_CAT</B> String =&gt; catalog name
     *  </OL>
     *
     * @return ResultSet - each row has a single String column that is a
     * catalog name
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getCatalogs() throws SQLException {
		return getSimpleQuery("getCatalogs");
	}

    /**
     * Get the table types available in this database.  The results
     * are ordered by table type.
     *
     * <P>The table type is:
     *  <OL>
     *	<LI><B>TABLE_TYPE</B> String =&gt; table type.  Typical types are "TABLE",
     *			"VIEW",	"SYSTEM TABLE", "GLOBAL TEMPORARY",
     *			"LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *  </OL>
     *
     * @return ResultSet - each row has a single String column that is a
     * table type
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getTableTypes() throws SQLException {
		return getSimpleQuery("getTableTypes");
	}

    /**
     * Get a description of table columns available in a catalog.
     *
     * <P>Only column descriptions matching the catalog, schema, table
     * and column name criteria are returned.  They are ordered by
     * TABLE_SCHEM, TABLE_NAME and ORDINAL_POSITION.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *	<LI><B>TABLE_CAT</B> String =&gt; table catalog (may be null)
     *	<LI><B>TABLE_SCHEM</B> String =&gt; table schema (may be null)
     *	<LI><B>TABLE_NAME</B> String =&gt; table name
     *	<LI><B>COLUMN_NAME</B> String =&gt; column name
     *	<LI><B>DATA_TYPE</B> int =&gt; SQL type from java.sql.Types
     *	<LI><B>TYPE_NAME</B> String =&gt; Data source dependent type name
     *	<LI><B>COLUMN_SIZE</B> int =&gt; column size.  For char or date
     *	    types this is the maximum number of characters, for numeric or
     *	    decimal types this is precision.
     *	<LI><B>BUFFER_LENGTH</B> is not used.
     *	<LI><B>DECIMAL_DIGITS</B> int =&gt; the number of fractional digits
     *	<LI><B>NUM_PREC_RADIX</B> int =&gt; Radix (typically either 10 or 2)
     *	<LI><B>NULLABLE</B> int =&gt; is NULL allowed?
     *      <UL>
     *      <LI> columnNoNulls - might not allow NULL values
     *      <LI> columnNullable - definitely allows NULL values
     *      <LI> columnNullableUnknown - nullability unknown
     *      </UL>
     *	<LI><B>REMARKS</B> String =&gt; comment describing column (may be null)
     * 	<LI><B>COLUMN_DEF</B> String =&gt; default value (may be null)
     *	<LI><B>SQL_DATA_TYPE</B> int =&gt; unused
     *	<LI><B>SQL_DATETIME_SUB</B> int =&gt; unused
     *	<LI><B>CHAR_OCTET_LENGTH</B> int =&gt; for char types the
     *       maximum number of bytes in the column
     *	<LI><B>ORDINAL_POSITION</B> int	=&gt; index of column in table
     *      (starting at 1)
     *	<LI><B>IS_NULLABLE</B> String =&gt; "NO" means column definitely
     *      does not allow NULL values; "YES" means the column might
     *      allow NULL values.  An empty string means nobody knows.
     *  <LI><B>SCOPE_CATALOG</B> String =&gt; catalog of table that is the
     *  scope of a reference attribute (<code>null</code> if DATA_TYPE
     *  isn't REF)
     *  <LI><B>SCOPE_SCHEMA</B> String =&gt; schema of table that is the
     *  scope of a reference attribute (<code>null</code> if the
     *  DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_TABLE</B> String =&gt; table name that this the
     *  scope of a reference attribure (<code>null</code> if the
     *  DATA_TYPE isn't REF)
     *  <LI><B>SOURCE_DATA_TYPE</B> short =&gt; source type of a distinct
     *  type or user-generated Ref type, SQL type from java.sql.Types
     *  (<code>null</code> if DATA_TYPE isn't DISTINCT or
     *  user-generated REF)
     *  <LI><B>IS_AUTOINCREMENT</B> String =&gt; Indicates whether this
     *  column is auto incremented
     *  <UL>
     *  <LI> YES --- if the column is auto incremented
     *  <LI> NO --- if the column is not auto incremented
     *  <LI> empty string --- if it cannot be determined whether the
     *       column is auto incremented parameter is unknown
     *  </UL>
     *  <LI><B>SCOPE_CATLOG</B> A redundant copy of SCOPE_CATALOG. The name
     *   of this column is deliberately mis-spelled in order to support a typo in the
     *   javadoc for DatabaseMetaData.getColumns() which was corrected by JDBC 4.1.
     *  </OL>
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     * without a schema
     * @param tableNamePattern a table name pattern
     * @param columnNamePattern a column name pattern
     * @return ResultSet - each row is a column description
     * @see #getSearchStringEscape
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getColumns(String catalog, String schemaPattern,
		String tableNamePattern, String columnNamePattern)
					throws SQLException {

//IC see: https://issues.apache.org/jira/browse/DERBY-107
		return doGetCols(catalog, schemaPattern, tableNamePattern,
			columnNamePattern, "getColumns");
	}

	/**
	 * Get a description of table columns available in a catalog.
	 * Same as getColumns() above, except that the result set
	 * will conform to ODBC specifications.
	 */
	public ResultSet getColumnsForODBC(String catalog, String schemaPattern,
		String tableNamePattern, String columnNamePattern)
		throws SQLException {

		return doGetCols(catalog, schemaPattern, tableNamePattern,
			columnNamePattern, "odbc_getColumns");
	}

	/**
	 * Does the actual work for the getColumns metadata calls.
	 * See getColumns() method above for parameter descriptions.
	 * @param queryName Name of the query to execute; is used
	 *	to determine whether the result set should conform to
	 *	JDBC or ODBC specifications.
	 */
	private ResultSet doGetCols(String catalog, String schemaPattern,
		String tableNamePattern, String columnNamePattern,
		String queryName) throws SQLException {

		PreparedStatement s = getPreparedQuery(queryName);
		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schemaPattern));
		s.setString(3, swapNull(tableNamePattern));
		s.setString(4, swapNull(columnNamePattern));
		return s.executeQuery();
	}

    /**
     * Get a description of the access rights for a table's columns.
     *
     * <P>Only privileges matching the column name criteria are
     * returned.  They are ordered by COLUMN_NAME and PRIVILEGE.
     *
     * <P>Each privilige description has the following columns:
     *  <OL>
     *	<LI><B>TABLE_CAT</B> String =&gt; table catalog (may be null)
     *	<LI><B>TABLE_SCHEM</B> String =&gt; table schema (may be null)
     *	<LI><B>TABLE_NAME</B> String =&gt; table name
     *	<LI><B>COLUMN_NAME</B> String =&gt; column name
     *	<LI><B>GRANTOR</B> =&gt; grantor of access (may be null)
     *	<LI><B>GRANTEE</B> String =&gt; grantee of access
     *	<LI><B>PRIVILEGE</B> String =&gt; name of access (SELECT,
     *      INSERT, UPDATE, REFRENCES, ...)
     *	<LI><B>IS_GRANTABLE</B> String =&gt; "YES" if grantee is permitted
     *      to grant to others; "NO" if not; null if unknown
     *  </OL>
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schema a schema name; "" retrieves those without a schema
     * @param table a table name
     * @param columnNamePattern a column name pattern
     * @return ResultSet - each row is a column privilege description
     * @see #getSearchStringEscape
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getColumnPrivileges(String catalog, String schema,
		String table, String columnNamePattern) throws SQLException {

        if (table == null) {
            throw Util.generateCsSQLException(
                           SQLState.TABLE_NAME_CANNOT_BE_NULL);
        }

		PreparedStatement s = getPreparedQuery("getColumnPrivileges");
		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schema));
		s.setString(3, table); //DERBY-1484; must match table name as stored
		s.setString(4, swapNull(columnNamePattern));
		return s.executeQuery();
	}

    /**
     * Get a description of the access rights for each table available
     * in a catalog. Note that a table privilege applies to one or
     * more columns in the table. It would be wrong to assume that
     * this priviledge applies to all columns (this may be true for
     * some systems but is not true for all.)
     *
     * <P>Only privileges matching the schema and table name
     * criteria are returned.  They are ordered by TABLE_SCHEM,
     * TABLE_NAME, and PRIVILEGE.
     *
     * <P>Each privilige description has the following columns:
     *  <OL>
     *	<LI><B>TABLE_CAT</B> String =&gt; table catalog (may be null)
     *	<LI><B>TABLE_SCHEM</B> String =&gt; table schema (may be null)
     *	<LI><B>TABLE_NAME</B> String =&gt; table name
     *	<LI><B>GRANTOR</B> =&gt; grantor of access (may be null)
     *	<LI><B>GRANTEE</B> String =&gt; grantee of access
     *	<LI><B>PRIVILEGE</B> String =&gt; name of access (SELECT,
     *      INSERT, UPDATE, REFRENCES, ...)
     *	<LI><B>IS_GRANTABLE</B> String =&gt; "YES" if grantee is permitted
     *      to grant to others; "NO" if not; null if unknown
     *  </OL>
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     * without a schema
     * @param tableNamePattern a table name pattern
     * @return ResultSet - each row is a table privilege description
     * @see #getSearchStringEscape
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
				String tableNamePattern) throws SQLException {
		PreparedStatement s = getPreparedQuery("getTablePrivileges");
		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schemaPattern));
		s.setString(3, swapNull(tableNamePattern));
		return s.executeQuery();
	}

    /**
     * Get a description of a table's optimal set of columns that
     * uniquely identifies a row. They are ordered by SCOPE.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *	<LI><B>SCOPE</B> short =&gt; actual scope of result
     *      <UL>
     *      <LI> bestRowTemporary - very temporary, while using row
     *      <LI> bestRowTransaction - valid for remainder of current transaction
     *      <LI> bestRowSession - valid for remainder of current session
     *      </UL>
     *	<LI><B>COLUMN_NAME</B> String =&gt; column name
     *	<LI><B>DATA_TYPE</B> int =&gt; SQL data type from java.sql.Types
     *	<LI><B>TYPE_NAME</B> String =&gt; Data source dependent type name
     *	<LI><B>COLUMN_SIZE</B> int =&gt; precision
     *	<LI><B>BUFFER_LENGTH</B> int =&gt; not used
     *	<LI><B>DECIMAL_DIGITS</B> short	 =&gt; scale
     *	<LI><B>PSEUDO_COLUMN</B> short =&gt; is this a pseudo column
     *      like an Oracle ROWID
     *      <UL>
     *      <LI> bestRowUnknown - may or may not be pseudo column
     *      <LI> bestRowNotPseudo - is NOT a pseudo column
     *      <LI> bestRowPseudo - is a pseudo column
     *      </UL>
     *  </OL>
     *
     * @param catalogPattern a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name; "" retrieves those without a schema
     * @param table a table name
     * @param scope the scope of interest; use same values as SCOPE
     * @param nullable include columns that are nullable?
     * @return ResultSet - each row is a column description
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getBestRowIdentifier
	(
		String catalogPattern,
		String schemaPattern,
		String table,
		int scope,
		boolean nullable
	) throws SQLException
	{
		return doGetBestRowId(catalogPattern, schemaPattern, table,
//IC see: https://issues.apache.org/jira/browse/DERBY-107
			scope, nullable, "");
	}

	/**
	 * Get a description of a table's optimal set of columns that
	 * uniquely identifies a row. They are ordered by SCOPE.
	 * Same as getBestRowIdentifier() above, except that the result
	 * set will conform to ODBC specifications.
	 */
	public ResultSet getBestRowIdentifierForODBC(String catalogPattern,
		String schemaPattern, String table, int scope,
		boolean nullable) throws SQLException {

		return doGetBestRowId(catalogPattern, schemaPattern, table,
			scope, nullable, "odbc_");
	}

	/**
	 * Does the actual work for the getBestRowIdentifier metadata
	 * calls.  See getBestRowIdentifier() method above for parameter
	 * descriptions.
	 * @param queryPrefix Prefix to be appended to the names of
	 *	the queries used in this method.  This is used
	 *	to determine whether the result set should conform to
	 *	JDBC or ODBC specifications.
	 */
	private ResultSet doGetBestRowId(String catalogPattern,
		String schemaPattern, String table, int scope,
		boolean nullable, String queryPrefix) throws SQLException {

        if (table == null) {
            throw Util.generateCsSQLException(
                           SQLState.TABLE_NAME_CANNOT_BE_NULL);
        }
        
		int nullableInIntForm = 0;
		if (nullable)
			nullableInIntForm = 1;
      
		if (catalogPattern == null)
		{
			catalogPattern = "%";
		}
		if (schemaPattern == null)
		{
			schemaPattern = "%";
		}

			PreparedStatement ps;
			boolean done;
	
			// scope value is bad, exception is thrown.
			if (scope < 0 || scope > 2) {
				throw newSQLException(SQLState.LANG_INVALID_ROWID_SCOPE,scope);
				
			}
	
			// see if there is a primary key, use it.
			ps = getPreparedQuery("getBestRowIdentifierPrimaryKey");
			ps.setString(1,catalogPattern);
			ps.setString(2,schemaPattern);
			ps.setString(3,table);
	
			ResultSet rs = ps.executeQuery();
			done = rs.next();
			String constraintId = "";
			if (done) {
			    constraintId = rs.getString(1);
			}
	
			rs.close();
			ps.close();
	
			if (done) 
			{
				// this one's it, do the real thing and return it.
				// we don't need to check catalog, schema, table name
				// or scope again.
//IC see: https://issues.apache.org/jira/browse/DERBY-107
				ps = getPreparedQuery(queryPrefix + "getBestRowIdentifierPrimaryKeyColumns");
				ps.setString(1,constraintId);
				ps.setString(2,constraintId);
				// note, primary key columns aren't nullable,
				// so we skip the nullOk parameter.
				return ps.executeQuery();
			}
	
            // Get the unique constraint with the fewest columns.
            // If there is a tie, i.e. there is more than one unique constraint
            // with the same number of columns involved, we chose the one
            // that's ordered first, cf. the query text in metadata.properties
            // (DERBY-6623)..
			ps = getPreparedQuery("getBestRowIdentifierUniqueConstraint");
			ps.setString(1,catalogPattern);
			ps.setString(2,schemaPattern);
			ps.setString(3,table);
	
			rs = ps.executeQuery();
			done = rs.next();
			if (done) {
			    constraintId = rs.getString(1);
			}
			// REMIND: we need to actually check for null columns
			// and toss out constraints with null columns if they aren't
			// desired... recode this as a WHILE returning at the
			// first match or falling off the end.
	
			rs.close();
			ps.close();
			if (done) 
			{
				// this one's it, do the real thing and return it.
//IC see: https://issues.apache.org/jira/browse/DERBY-107
				ps = getPreparedQuery(queryPrefix + "getBestRowIdentifierUniqueKeyColumns");
				ps.setString(1,constraintId);
				ps.setString(2,constraintId);
				ps.setInt(3,nullableInIntForm);
				return ps.executeQuery();
			}
	
	
			// second-to last try -- unique index with minimal # columns
			// (only non null columns if so required)
			ps = getPreparedQuery("getBestRowIdentifierUniqueIndex");
			ps.setString(1,catalogPattern);
			ps.setString(2,schemaPattern);
			ps.setString(3,table);
	
			rs = ps.executeQuery();
			done = rs.next();
			long indexNum = 0;
			if (done) {
			    indexNum = rs.getLong(1);
			}
			// REMIND: we need to actually check for null columns
			// and toss out constraints with null columns if they aren't
			// desired... recode this as a WHILE returning at the
			// first match or falling off the end.
	
			rs.close();
			ps.close();
			if (done) {
				// this one's it, do the real thing and return it.
//IC see: https://issues.apache.org/jira/browse/DERBY-107
				ps = getPreparedQuery(queryPrefix + "getBestRowIdentifierUniqueIndexColumns");
				ps.setLong(1,indexNum);
				ps.setInt(2,nullableInIntForm);
				return ps.executeQuery();
			}

			// last try -- just return all columns of the table
			// the not null ones if that restriction is upon us.
//IC see: https://issues.apache.org/jira/browse/DERBY-107
			ps = getPreparedQuery(queryPrefix + "getBestRowIdentifierAllColumns");
			ps.setString(1,catalogPattern);
			ps.setString(2,schemaPattern);
			ps.setString(3,table);
			ps.setInt(4,scope);
			ps.setInt(5,nullableInIntForm);
			return ps.executeQuery();
	}

    /**
     * Get a description of a table's columns that are automatically
     * updated when any value in a row is updated.  They are
     * unordered.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *	<LI><B>SCOPE</B> short =&gt; is not used
     *	<LI><B>COLUMN_NAME</B> String =&gt; column name
     *	<LI><B>DATA_TYPE</B> int =&gt; SQL data type from java.sql.Types
     *	<LI><B>TYPE_NAME</B> String =&gt; Data source dependent type name
     *	<LI><B>COLUMN_SIZE</B> int =&gt; precision
     *	<LI><B>BUFFER_LENGTH</B> int =&gt; length of column value in bytes
     *	<LI><B>DECIMAL_DIGITS</B> short	 =&gt; scale
     *	<LI><B>PSEUDO_COLUMN</B> short =&gt; is this a pseudo column
     *      like an Oracle ROWID
     *      <UL>
     *      <LI> versionColumnUnknown - may or may not be pseudo column
     *      <LI> versionColumnNotPseudo - is NOT a pseudo column
     *      <LI> versionColumnPseudo - is a pseudo column
     *      </UL>
     *  </OL>
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schema a schema name; "" retrieves those without a schema
     * @param table a table name
     * @return ResultSet - each row is a column description
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getVersionColumns(String catalog, String schema,
				String table) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-107
		return doGetVersionCols(catalog, schema, table, "getVersionColumns");
	}

	/**
	 * Get a description of a table's columns that are automatically
	 * updated when any value in a row is updated.  They are
	 * unordered.  Same as getVersionColumns() above, except that
	 * the result set will conform to ODBC specifications.
	 */
	public ResultSet getVersionColumnsForODBC(String catalog, String schema,
				String table) throws SQLException {
		return doGetVersionCols(catalog, schema, table, "odbc_getVersionColumns");
	}

	/**
	 * Does the actual work for the getVersionColumns metadata
	 * calls.  See getVersionColumns() method above for parameter
	 * descriptions.
	 * @param queryName Name of the query to execute; is used
	 *	to determine whether the result set should conform to
	 *	JDBC or ODBC specifications.
	 */
	private ResultSet doGetVersionCols(String catalog, String schema,
		String table, String queryName) throws SQLException {

        if (table == null) {
            throw Util.generateCsSQLException(
                           SQLState.TABLE_NAME_CANNOT_BE_NULL);
        }
        
		PreparedStatement s = getPreparedQuery(queryName);
		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schema));
		s.setString(3, table); //DERBY-1484: Must match table name as stored
		return s.executeQuery();
	}

    /**
     * check if the dictionary is at the same version as the engine. If not, 
     * then that means stored versions of the JDBC database metadata queries
     * may not be compatible with this version of the software.
     * This can happen if we are in soft upgrade mode. Since in soft upgrade 
     * mode, we can't change these stored metadata queries in a backward 
     * incompatible way, engine needs to read the metadata sql from 
     * metadata.properties or metadata_net.properties file rather than
     * rely on system tables.
     * 
     * @return true if we are not in soft upgrade mode
     * @throws SQLException
     */
	private boolean notInSoftUpgradeMode() 
//IC see: https://issues.apache.org/jira/browse/DERBY-573
		throws SQLException {
		if ( getEmbedConnection().isClosed())
			throw Util.noCurrentConnection();

		boolean notInSoftUpgradeMode;
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
        LanguageConnectionContext lcc = getLanguageConnectionContext();
		try {
			notInSoftUpgradeMode =
               lcc.getDataDictionary().checkVersion(
						DataDictionary.DD_VERSION_CURRENT,null);
            InterruptStatus.restoreIntrFlagIfSeen();
		} catch (Throwable t) {
			throw handleException(t);
		}
		return notInSoftUpgradeMode;
	}
	
	
    /**
     * Get a description of a table's primary key columns.  They
     * are ordered by COLUMN_NAME.
     *
     * <P>Each primary key column description has the following columns:
     *  <OL>
     *	<LI><B>TABLE_CAT</B> String =&gt; table catalog (may be null)
     *	<LI><B>TABLE_SCHEM</B> String =&gt; table schema (may be null)
     *	<LI><B>TABLE_NAME</B> String =&gt; table name
     *	<LI><B>COLUMN_NAME</B> String =&gt; column name
     *	<LI><B>KEY_SEQ</B> short =&gt; sequence number within primary key
     *	<LI><B>PK_NAME</B> String =&gt; primary key name (may be null)
     *  </OL>
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schema a schema name pattern; "" retrieves those
     * without a schema
     * @param table a table name
     * @return ResultSet - each row is a primary key column description
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getPrimaryKeys(String catalog, String schema,
			String table) throws SQLException {
		PreparedStatement s = getPreparedQuery("getPrimaryKeys");
//IC see: https://issues.apache.org/jira/browse/DERBY-137

        if (table == null) {
            throw Util.generateCsSQLException(
                           SQLState.TABLE_NAME_CANNOT_BE_NULL);
        }

		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schema));
		s.setString(3, table); //DERBY-1484: Must match table name as stored
		return s.executeQuery();
	}	

	/**
     * Get a description of the primary key columns that are
     * referenced by a table's foreign key columns (the primary keys
     * imported by a table).  They are ordered by PKTABLE_CAT,
     * PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
     *
     * <P>Each primary key column description has the following columns:
     *  <OL>
     *	<LI><B>PKTABLE_CAT</B> String =&gt; primary key table catalog
     *      being imported (may be null)
     *	<LI><B>PKTABLE_SCHEM</B> String =&gt; primary key table schema
     *      being imported (may be null)
     *	<LI><B>PKTABLE_NAME</B> String =&gt; primary key table name
     *      being imported
     *	<LI><B>PKCOLUMN_NAME</B> String =&gt; primary key column name
     *      being imported
     *	<LI><B>FKTABLE_CAT</B> String =&gt; foreign key table catalog (may be null)
     *	<LI><B>FKTABLE_SCHEM</B> String =&gt; foreign key table schema (may be null)
     *	<LI><B>FKTABLE_NAME</B> String =&gt; foreign key table name
     *	<LI><B>FKCOLUMN_NAME</B> String =&gt; foreign key column name
     *	<LI><B>KEY_SEQ</B> short =&gt; sequence number within foreign key
     *	<LI><B>UPDATE_RULE</B> short =&gt; What happens to
     *       foreign key when primary is updated:
     *      <UL>
     *      <LI> importedNoAction - do not allow update of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - change imported key to agree
     *               with primary key update
     *      <LI> importedKeySetNull - change imported key to NULL if
     *               its primary key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *               if its primary key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      </UL>
     *	<LI><B>DELETE_RULE</B> short =&gt; What happens to
     *      the foreign key when primary is deleted.
     *      <UL>
     *      <LI> importedKeyNoAction - do not allow delete of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to NULL if
     *               its primary key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if
     *               its primary key has been deleted
     *      </UL>
     *	<LI><B>FK_NAME</B> String =&gt; foreign key name (may be null)
     *	<LI><B>PK_NAME</B> String =&gt; primary key name (may be null)
     *	<LI><B>DEFERRABILITY</B> short =&gt; can the evaluation of foreign key
     *      constraints be deferred until commit
     *      <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *      </UL>
     *  </OL>
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schema a schema name pattern; "" retrieves those
     * without a schema
     * @param table a table name
     * @return ResultSet - each row is a primary key column description
     * @see #getExportedKeys
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getImportedKeys(String catalog, String schema,
				String table) throws SQLException {
        if (table == null) {
            throw Util.generateCsSQLException(
                           SQLState.TABLE_NAME_CANNOT_BE_NULL);
        }

		PreparedStatement s = getPreparedQuery("getImportedKeys");
		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schema));
		s.setString(3, table); //DERBY-1484: Must match table name as stored
		return s.executeQuery();
	}


    /**
     * Get a description of the foreign key columns that reference a
     * table's primary key columns (the foreign keys exported by a
     * table).  They are ordered by FKTABLE_CAT, FKTABLE_SCHEM,
     * FKTABLE_NAME, and KEY_SEQ.
     *
     * <P>Each foreign key column description has the following columns:
     *  <OL>
     *	<LI><B>PKTABLE_CAT</B> String =&gt; primary key table catalog (may be null)
     *	<LI><B>PKTABLE_SCHEM</B> String =&gt; primary key table schema (may be null)
     *	<LI><B>PKTABLE_NAME</B> String =&gt; primary key table name
     *	<LI><B>PKCOLUMN_NAME</B> String =&gt; primary key column name
     *	<LI><B>FKTABLE_CAT</B> String =&gt; foreign key table catalog (may be null)
     *      being exported (may be null)
     *	<LI><B>FKTABLE_SCHEM</B> String =&gt; foreign key table schema (may be null)
     *      being exported (may be null)
     *	<LI><B>FKTABLE_NAME</B> String =&gt; foreign key table name
     *      being exported
     *	<LI><B>FKCOLUMN_NAME</B> String =&gt; foreign key column name
     *      being exported
     *	<LI><B>KEY_SEQ</B> short =&gt; sequence number within foreign key
     *	<LI><B>UPDATE_RULE</B> short =&gt; What happens to
     *       foreign key when primary is updated:
     *      <UL>
     *      <LI> importedNoAction - do not allow update of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - change imported key to agree
     *               with primary key update
     *      <LI> importedKeySetNull - change imported key to NULL if
     *               its primary key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *               if its primary key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      </UL>
     *	<LI><B>DELETE_RULE</B> short =&gt; What happens to
     *      the foreign key when primary is deleted.
     *      <UL>
     *      <LI> importedKeyNoAction - do not allow delete of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to NULL if
     *               its primary key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if
     *               its primary key has been deleted
     *      </UL>
     *	<LI><B>FK_NAME</B> String =&gt; foreign key name (may be null)
     *	<LI><B>PK_NAME</B> String =&gt; primary key name (may be null)
     *	<LI><B>DEFERRABILITY</B> short =&gt; can the evaluation of foreign key
     *      constraints be deferred until commit
     *      <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *      </UL>
     *  </OL>
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schema a schema name pattern; "" retrieves those
     * without a schema
     * @param table a table name
     * @return ResultSet - each row is a foreign key column description
     * @see #getImportedKeys
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getExportedKeys(String catalog, String schema,
				String table) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-573

        if (table == null) {
            throw Util.generateCsSQLException(
                           SQLState.TABLE_NAME_CANNOT_BE_NULL);
        }

		PreparedStatement s = getPreparedQuery("getCrossReference");
		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schema));
		s.setString(3, table); //DERBY-1484: Must match table name as stored
		s.setString(4, swapNull(null));
		s.setString(5, swapNull(null));
		s.setString(6, swapNull(null));
		return s.executeQuery();
	}

    /**
     * Get a description of the foreign key columns in the foreign key
     * table that reference the primary key columns of the primary key
     * table (describe how one table imports another's key.) This
     * should normally return a single foreign key/primary key pair
     * (most tables only import a foreign key from a table once.)  They
     * are ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and
     * KEY_SEQ.
     *
     * <P>Each foreign key column description has the following columns:
     *  <OL>
     *	<LI><B>PKTABLE_CAT</B> String =&gt; primary key table catalog (may be null)
     *	<LI><B>PKTABLE_SCHEM</B> String =&gt; primary key table schema (may be null)
     *	<LI><B>PKTABLE_NAME</B> String =&gt; primary key table name
     *	<LI><B>PKCOLUMN_NAME</B> String =&gt; primary key column name
     *	<LI><B>FKTABLE_CAT</B> String =&gt; foreign key table catalog (may be null)
     *      being exported (may be null)
     *	<LI><B>FKTABLE_SCHEM</B> String =&gt; foreign key table schema (may be null)
     *      being exported (may be null)
     *	<LI><B>FKTABLE_NAME</B> String =&gt; foreign key table name
     *      being exported
     *	<LI><B>FKCOLUMN_NAME</B> String =&gt; foreign key column name
     *      being exported
     *	<LI><B>KEY_SEQ</B> short =&gt; sequence number within foreign key
     *	<LI><B>UPDATE_RULE</B> short =&gt; What happens to
     *       foreign key when primary is updated:
     *      <UL>
     *      <LI> importedNoAction - do not allow update of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - change imported key to agree
     *               with primary key update
     *      <LI> importedKeySetNull - change imported key to NULL if
     *               its primary key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *               if its primary key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      </UL>
     *	<LI><B>DELETE_RULE</B> short =&gt; What happens to
     *      the foreign key when primary is deleted.
     *      <UL>
     *      <LI> importedKeyNoAction - do not allow delete of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to NULL if
     *               its primary key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if
     *               its primary key has been deleted
     *      </UL>
     *	<LI><B>FK_NAME</B> String =&gt; foreign key name (may be null)
     *	<LI><B>PK_NAME</B> String =&gt; primary key name (may be null)
     *	<LI><B>DEFERRABILITY</B> short =&gt; can the evaluation of foreign key
     *      constraints be deferred until commit
     *      <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *      </UL>
     *  </OL>
     *
     * @param primaryCatalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param primarySchema a schema name pattern; "" retrieves those
     * without a schema
     * @param primaryTable the table name that exports the key
     * @param foreignCatalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param foreignSchema a schema name pattern; "" retrieves those
     * without a schema
     * @param foreignTable the table name that imports the key
     * @return ResultSet - each row is a foreign key column description
     * @see #getImportedKeys
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getCrossReference(
		String primaryCatalog, String primarySchema, String primaryTable,
		String foreignCatalog, String foreignSchema, String foreignTable
		) throws SQLException {

		if (primaryTable == null || foreignTable == null) {
			throw Util.generateCsSQLException(
							SQLState.TABLE_NAME_CANNOT_BE_NULL);
		}

		PreparedStatement s = getPreparedQuery("getCrossReference");
		s.setString(1, swapNull(primaryCatalog));
		s.setString(2, swapNull(primarySchema));
		s.setString(3, primaryTable); //JDBC spec: must match table name as stored
		s.setString(4, swapNull(foreignCatalog));
		s.setString(5, swapNull(foreignSchema));
		s.setString(6, foreignTable); //JDBC spec: must match table name as stored
		return s.executeQuery();
	}

    /**
     * In contrast to the JDBC version of getCrossReference, this
     * method allows null values for table names.
     */
    public ResultSet getCrossReferenceForODBC(
        String primaryCatalog, String primarySchema, String primaryTable,
        String foreignCatalog, String foreignSchema, String foreignTable)
        throws SQLException {

        PreparedStatement s = getPreparedQuery("odbc_getCrossReference");
        s.setString(1, swapNull(primaryCatalog));
        s.setString(2, swapNull(primarySchema));
        s.setString(3, swapNull(primaryTable));
        s.setString(4, swapNull(foreignCatalog));
        s.setString(5, swapNull(foreignSchema));
        s.setString(6, swapNull(foreignTable));
        return s.executeQuery();
    }

    /**
     * Get a description of all the standard SQL types supported by
     * this database. They are ordered by DATA_TYPE and then by how
     * closely the data type maps to the corresponding JDBC SQL type.
     *
     * <P>Each type description has the following columns:
     *  <OL>
     *	<LI><B>TYPE_NAME</B> String =&gt; Type name
     *	<LI><B>DATA_TYPE</B> int =&gt; SQL data type from java.sql.Types
     *	<LI><B>PRECISION</B> int =&gt; maximum precision
     *	<LI><B>LITERAL_PREFIX</B> String =&gt; prefix used to quote a literal
     *      (may be null)
     *	<LI><B>LITERAL_SUFFIX</B> String =&gt; suffix used to quote a literal
            (may be null)
     *	<LI><B>CREATE_PARAMS</B> String =&gt; parameters used in creating
     *      the type (may be null)
     *	<LI><B>NULLABLE</B> short =&gt; can you use NULL for this type?
     *      <UL>
     *      <LI> typeNoNulls - does not allow NULL values
     *      <LI> typeNullable - allows NULL values
     *      <LI> typeNullableUnknown - nullability unknown
     *      </UL>
     *	<LI><B>CASE_SENSITIVE</B> boolean=&gt; is it case sensitive?
     *	<LI><B>SEARCHABLE</B> short =&gt; can you use "WHERE" based on this type:
     *      <UL>
     *      <LI> typePredNone - No support
     *      <LI> typePredChar - Only supported with WHERE .. LIKE
     *      <LI> typePredBasic - Supported except for WHERE .. LIKE
     *      <LI> typeSearchable - Supported for all WHERE ..
     *      </UL>
     *	<LI><B>UNSIGNED_ATTRIBUTE</B> boolean =&gt; is it unsigned?
     *	<LI><B>FIXED_PREC_SCALE</B> boolean =&gt; can it be a money value?
     *	<LI><B>AUTO_INCREMENT</B> boolean =&gt; can it be used for an
     *      auto-increment value?
     *	<LI><B>LOCAL_TYPE_NAME</B> String =&gt; localized version of type name
     *      (may be null)
     *	<LI><B>MINIMUM_SCALE</B> short =&gt; minimum scale supported
     *	<LI><B>MAXIMUM_SCALE</B> short =&gt; maximum scale supported
     *	<LI><B>SQL_DATA_TYPE</B> int =&gt; unused
     *	<LI><B>SQL_DATETIME_SUB</B> int =&gt; unused
     *	<LI><B>NUM_PREC_RADIX</B> int =&gt; usually 2 or 10
     *  </OL>
     *
     * @return ResultSet - each row is a SQL type description
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getTypeInfo() throws SQLException {
		return getTypeInfoMinion("getTypeInfo");
	}

	/**
	 * Get a description of all the standard SQL types supported by
	 * this database. They are ordered by DATA_TYPE and then by how
	 * closely the data type maps to the corresponding JDBC SQL type.
	 * Same as getTypeInfo above, except that the result set will
	 * conform to ODBC specifications.
	 */
	public ResultSet getTypeInfoForODBC() throws SQLException {
		return getTypeInfoMinion("odbc_getTypeInfo");
	}

    /**
     * Get a description of the standard SQL types supported by this database.
     *
     * @param queryName the name of the query that fetches the information
     * @return a result set with SQL type description
     */
    private ResultSet getTypeInfoMinion(String queryName) throws SQLException {
        try {
            // DERBY-4946: BOOLEAN data type was introduced in 10.7
            boolean booleanSupported =
                    getLanguageConnectionContext().getDataDictionary().
                    checkVersion(DataDictionary.DD_VERSION_DERBY_10_7, null);

            PreparedStatement ps = getPreparedQuery(queryName);
            ps.setBoolean(1, booleanSupported);
            return ps.executeQuery();

        } catch (StandardException se) {
            throw handleException(se);
        }
    }

    /**
     * Get a description of a table's indices and statistics. They are
     * ordered by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
     *
     * <P>Each index column description has the following columns:
     *  <OL>
     *	<LI><B>TABLE_CAT</B> String =&gt; table catalog (may be null)
     *	<LI><B>TABLE_SCHEM</B> String =&gt; table schema (may be null)
     *	<LI><B>TABLE_NAME</B> String =&gt; table name
     *	<LI><B>NON_UNIQUE</B> boolean =&gt; Can index values be non-unique?
     *      false when TYPE is tableIndexStatistic
     *	<LI><B>INDEX_QUALIFIER</B> String =&gt; index catalog (may be null);
     *      null when TYPE is tableIndexStatistic
     *	<LI><B>INDEX_NAME</B> String =&gt; index name; null when TYPE is
     *      tableIndexStatistic
     *	<LI><B>TYPE</B> short =&gt; index type:
     *      <UL>
     *      <LI> tableIndexStatistic - this identifies table statistics that are
     *           returned in conjuction with a table's index descriptions
     *      <LI> tableIndexClustered - this is a clustered index
     *      <LI> tableIndexHashed - this is a hashed index
     *      <LI> tableIndexOther - this is some other style of index
     *      </UL>
     *	<LI><B>ORDINAL_POSITION</B> short =&gt; column sequence number
     *      within index; zero when TYPE is tableIndexStatistic
     *	<LI><B>COLUMN_NAME</B> String =&gt; column name; null when TYPE is
     *      tableIndexStatistic
     *	<LI><B>ASC_OR_DESC</B> String =&gt; column sort sequence, "A" =&gt; ascending,
     *      "D" =&gt; descending, may be null if sort sequence is not supported;
     *      null when TYPE is tableIndexStatistic
     *	<LI><B>CARDINALITY</B> int =&gt; When TYPE is tableIndexStatistic, then
     *      this is the number of rows in the table; otherwise, it is the
     *      number of unique values in the index.
     *	<LI><B>PAGES</B> int =&gt; When TYPE is  tableIndexStatisic then
     *      this is the number of pages used for the table, otherwise it
     *      is the number of pages used for the current index.
     *	<LI><B>FILTER_CONDITION</B> String =&gt; Filter condition, if any.
     *      (may be null)
     *  </OL>
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schema a schema name pattern; "" retrieves those without a schema
     * @param table a table name
     * @param unique when true, return only indices for unique values;
     *     when false, return indices regardless of whether unique or not
     * @param approximate when true, result is allowed to reflect approximate
     *     or out of data values; when false, results are requested to be
     *     accurate
     * @return ResultSet - each row is an index column description
	 * @exception SQLException thrown on failure.
     */
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate)
					throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-107
		return doGetIndexInfo(catalog, schema, table, unique, approximate, "getIndexInfo");
	}

	/**
	 * Get a description of a table's indices and statistics. They are
	 * ordered by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
	 * Same as getIndexInfo above, except that the result set will
	 * conform to ODBC specifications.
	 */
	public ResultSet getIndexInfoForODBC(String catalog, String schema, String table,
		boolean unique, boolean approximate) throws SQLException
	{
		return doGetIndexInfo(catalog, schema, table, unique, approximate, "odbc_getIndexInfo");
	}

	/**
	 * Does the actual work for the getIndexInfo metadata
	 * calls.  See getIndexInfo() method above for parameter
	 * descriptions.
	 * @param queryName Name of the query to execute; is used
	 *	to determine whether the result set should conform to
	 *	JDBC or ODBC specifications.
	 */
	private ResultSet doGetIndexInfo(String catalog, String schema, String table,
		boolean unique, boolean approximate, String queryName)
		throws SQLException {

        if (table == null) {
            throw Util.generateCsSQLException(
                           SQLState.TABLE_NAME_CANNOT_BE_NULL);
        }

		int approximateInInt = 0;
		if (approximate) approximateInInt = 1;
		PreparedStatement s = getPreparedQuery(queryName);
		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schema));
		s.setString(3, table); //DERBY-1484: Must match table name as stored
		s.setBoolean(4, unique);
		s.setInt(5, approximateInInt);
		return s.executeQuery();
	}

	/////////////////////////////////////////////////////////////////////////
	//
	//	JDBC 2.0	-	New public methods
	//
	/////////////////////////////////////////////////////////////////////////

    /**
     * JDBC 2.0
     *
     * Does the database support the given result set type?
     *
     * @param type defined in java.sql.ResultSet
     * @return true if so 
     * @see Connection
     */
	public boolean supportsResultSetType(int type) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
		if ((type == ResultSet.TYPE_FORWARD_ONLY) ||
		    (type == ResultSet.TYPE_SCROLL_INSENSITIVE)) {
			return true;
		}
    //we don't support TYPE_SCROLL_SENSITIVE yet.
    return false;
	}

    /**
     * JDBC 2.0
     *
     * Does the database support the concurrency type in combination
     * with the given result set type?
     *
     * @param type defined in java.sql.ResultSet
     * @param concurrency type defined in java.sql.ResultSet
     * @return true if so 
     * @see Connection
     */
	public boolean supportsResultSetConcurrency(int type, int concurrency) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
 		if (type == ResultSet.TYPE_SCROLL_SENSITIVE) {
 			// (TYPE_SCROLL_SENSITIVE, *)
//IC see: https://issues.apache.org/jira/browse/DERBY-690
  			return false;
 		} else {
 			// (FORWARD_ONLY, CONCUR_UPDATABLE)
 			// (FORWARD_ONLY, CONCUR_READ_ONLY)
 			// (TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE)
 			// (TYPE_SCROLL_INSENSITIVE, READ_ONLY)
 			return true;
 		}
	}

    /**
     * JDBC 2.0
     *
     * Determine whether a result set's updates are visible.
     *
     * @param type result set type, i.e. ResultSet.TYPE_XXX
     * @return true if updates are visible for the result set type
     */
    public boolean ownUpdatesAreVisible(int type)   {
 		if (type == ResultSet.TYPE_SCROLL_INSENSITIVE) {
 			return true;
 		} else {
 			return false;
 		}
	}
 
    /**
     * JDBC 2.0
     *
     * Determine whether a result set's deletes are visible.
     *
     * @param type result set type, i.e. ResultSet.TYPE_XXX
     * @return true if deletes are visible for the result set type
     */
    public boolean ownDeletesAreVisible(int type)   {
 		if (type == ResultSet.TYPE_SCROLL_INSENSITIVE) {
 			return true;
 		} else {
 			return false;
 		}
	}
 
     /**
      * JDBC 2.0
      *
      * Determine whether a result set's inserts are visible.
      *
      * @param type result set type, i.e. ResultSet.TYPE_XXX
      * @return true if inserts are visible for the result set type
      */
     public boolean ownInsertsAreVisible(int type)  {
 		return false;
  	}

      // Since Derby materializes a forward only ResultSet incrementally, it is 
      // possible to see changes made by others and hence following 3 metadata 
      // calls will return true for forward only ResultSets.

      /**
       * JDBC 2.0
       *
       * Determine whether updates made by others are visible.
       *
       * @param type result set type, i.e. ResultSet.TYPE_XXX
       * @return true if updates are visible for the result set type
       */
    public boolean othersUpdatesAreVisible(int type) {
		if (type == ResultSet.TYPE_FORWARD_ONLY)
			return true;
		return false;
	}

    /**
     * JDBC 2.0
     *
     * Determine whether deletes made by others are visible.
     *
     * @param type result set type, i.e. ResultSet.TYPE_XXX
     * @return true if deletes are visible for the result set type
     */
    public boolean othersDeletesAreVisible(int type)  {
		if (type == ResultSet.TYPE_FORWARD_ONLY)
			return true;
		return false;
	}

    /**
     * JDBC 2.0
     *
     * Determine whether inserts made by others are visible.
     *
     * @param type result set type, i.e. ResultSet.TYPE_XXX
     * @return true if inserts are visible for the result set type
     */
    public boolean othersInsertsAreVisible(int type)  {
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
		if (type == ResultSet.TYPE_FORWARD_ONLY)
			return true;
		return false;
	}

    /**
     * JDBC 2.0
     *
     * Determine whether or not a visible row update can be detected by 
     * calling ResultSet.rowUpdated().
     *
     * @param type result set type, i.e. ResultSet.TYPE_XXX
     * @return true if updates are detected by the resultset type
     */
    public boolean updatesAreDetected(int type) {
		if (type == ResultSet.TYPE_SCROLL_INSENSITIVE) {
//IC see: https://issues.apache.org/jira/browse/DERBY-690
			return true;
		} else {
			// For forward only resultsets, we move to before the next
			// row after a update and that is why updatesAreDetected
			// returns false.
			return false;
		}
	}

    /**
     * JDBC 2.0
     *
     * Determine whether or not a visible row delete can be detected by
     * calling ResultSet.rowDeleted().  If deletesAreDetected()
     * returns false, then deleted rows are removed from the result set.
     *
     * @param type result set type, i.e. ResultSet.TYPE_XXX
     * @return true if deletes are detected by the resultset type
     */
    public boolean deletesAreDetected(int type) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
		if (type == ResultSet.TYPE_SCROLL_INSENSITIVE) {
//IC see: https://issues.apache.org/jira/browse/DERBY-690
			return true;
		} else {
			// For forward only resultsets, we move to before the next
			// row after a delete and that is why deletesAreDetected
			// returns false
			return false;
		}
	}

    /**
     * JDBC 2.0
     *
     * Determine whether or not a visible row insert can be detected
     * by calling ResultSet.rowInserted().
     *
     * @param type result set type, i.e. ResultSet.TYPE_XXX
     * @return true if inserts are detected by the resultset type
     */
    public boolean insertsAreDetected(int type) {
		  return false;
	}

    /**
     * JDBC 2.0
     *
     * Return true if the driver supports batch updates, else return false.
     *
     */
    public boolean supportsBatchUpdates() {
		  return true;
	}

    /**
     * JDBC 2.0
     *
     * Get a description of the user-defined types defined in a particular
     * schema.  Schema specific UDTs may have type JAVA_OBJECT, STRUCT, 
     * or DISTINCT.
     *
     * <P>Only types matching the catalog, schema, type name and type  
     * criteria are returned.  They are ordered by DATA_TYPE, TYPE_SCHEM 
     * and TYPE_NAME.  The type name parameter may be a fully qualified 
     * name.  In this case, the catalog and schemaPattern parameters are
     * ignored.
     *
     * <P>Each type description has the following columns:
     *  <OL>
     *	<LI><B>TYPE_CAT</B> String =&gt; the type's catalog (may be null)
     *	<LI><B>TYPE_SCHEM</B> String =&gt; type's schema (may be null)
     *	<LI><B>TYPE_NAME</B> String =&gt; type name
     *  <LI><B>CLASS_NAME</B> String =&gt; Java class name
     *	<LI><B>DATA_TYPE</B> String =&gt; type value defined in java.sql.Types.  
     *  One of JAVA_OBJECT, STRUCT, or DISTINCT
     *	<LI><B>REMARKS</B> String =&gt; explanatory comment on the type
     *  <LI><B>BASE_TYPE</B> short =&gt; type code of the source type of
     *  a DISTINCT type or the type that implements the user-generated
     *  reference type of the SELF_REFERENCING_COLUMN of a structured
     *  type as defined in java.sql.Types (<code>null</code> if
     *  DATA_TYPE is not DISTINCT or not STRUCT with
     *  REFERENCE_GENERATION = USER_DEFINED)
     *  </OL>
     *
     * <P><B>Note:</B> If the driver does not support UDTs then an empty
     * result set is returned.
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     * without a schema
     * @param typeNamePattern a type name pattern; may be a fully qualified
     * name
     * @param types a list of user-named types to include (JAVA_OBJECT, 
     * STRUCT, or DISTINCT); null returns all types 
     * @return ResultSet - each row is a type description
     * @exception SQLException if a database-access error occurs.
     */
    public ResultSet getUDTs(String catalog, String schemaPattern, 
		      String typeNamePattern, int[] types)
      throws SQLException
    {
        //we don't have support for catalog names
        //the only type of user-named types we support are JAVA_OBJECT

        int getClassTypes = ILLEGAL_UDT_TYPE;

        // null argument, means all supported UDT types
        if ( types == null ) { getClassTypes = java.sql.Types.JAVA_OBJECT; }
        else if ( types.length > 0 )
        {
            for ( int i=0; i< types.length; i++ )
            {
                if ( types[i] == java.sql.Types.JAVA_OBJECT )
                { getClassTypes = java.sql.Types.JAVA_OBJECT; }
            }
        }

  		PreparedStatement s = getPreparedQuery("getUDTs");
  		s.setInt(1, java.sql.Types.JAVA_OBJECT); // thrown away. preserved for backward compatibility with pre-10.6 clients
  		s.setString(2, swapNull(catalog));
  		s.setString(3, swapNull(schemaPattern));
  		s.setString(4, swapNull(typeNamePattern));
  		s.setInt(5, getClassTypes);
        return s.executeQuery();
	}

    /**
     * JDBC 2.0
     *
     * Return the connection that produced this metadata object.
     *
     */
    public Connection getConnection() {
		  return getEmbedConnection().getApplicationConnection();
	}

	/**
    Following methods are for the new JDBC 3.0 methods in java.sql.DatabaseMetaData
    (see the JDBC 3.0 spec). We have the JDBC 3.0 methods in Local20
    package, so we don't have to have a new class in Local30.
    The new JDBC 3.0 methods don't make use of any new JDBC3.0 classes and
    so this will work fine in jdbc2.0 configuration.
	*/

	/////////////////////////////////////////////////////////////////////////
	//
	//	JDBC 3.0	-	New public methods
	//
	/////////////////////////////////////////////////////////////////////////

	/**
    * JDBC 3.0
    *
    * Retrieves whether this database supports statement pooling.
    *
    * @return true if statement pooling is supported; false otherwise
	*/
	public boolean supportsStatementPooling()
	{
		return false;
	}

	/**
    * JDBC 3.0
    *
    * Retrieves whether this database supports savepoints.
    *
    * @return true if savepoints are supported; false otherwise
	*/
	public boolean supportsSavepoints()
	{
		return true;
	}

	/**
    * JDBC 3.0
    *
    * Retrieves whether this database supports named parameters to callable statements.
    *
    * @return true if named parameters are supported; false otherwise
	*/
	public boolean supportsNamedParameters()
	{
		return false;
	}

	/**
    * JDBC 3.0
    *
    * Retrieves whether it is possible to have multiple ResultSet objects returned from a
    * CallableStatement object simultaneously.
    *
    * @return true if a CallableStatement object can return multiple ResultSet objects
    * simultaneously; false otherwise
	*/
	public boolean supportsMultipleOpenResults()
	{
		return true;
	}

	/**
    * JDBC 3.0
    *
    * Retrieves whether auto-generated keys can be retrieved after a statement
    * has been executed.
    *
    * @return true if auto-generated keys can be retrieved after a statement has
    * executed; false otherwise
	*/
	public boolean supportsGetGeneratedKeys()
	{
               /*
                * Currently reverting the returned value to false until there 
                * is more support for autogenerated keys in Derby.
                * (such as support for specifying the returned columns for
                * the autogenerated key)
                */
//IC see: https://issues.apache.org/jira/browse/DERBY-66
//IC see: https://issues.apache.org/jira/browse/DERBY-242
//IC see: https://issues.apache.org/jira/browse/DERBY-462
		return false;
	}

	/**
    * JDBC 3.0
    *
    * Retrieves whether this database supports the given result set holdability.
    *
    * @param holdability - one of the following constants:
    * ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
    * @return true if so; false otherwise
    * executed; false otherwise
	*/
	public boolean supportsResultSetHoldability(int holdability)
	{
		return true;
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the default holdability of this ResultSet object.
    *
    * @return the default holdability which is ResultSet.HOLD_CURSORS_OVER_COMMIT
	*/
	public int getResultSetHoldability()
  {
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the major version number of the underlying database.
    *
    * @return the underlying database's major version
	*/
	public int getDatabaseMajorVersion()
	{
		ProductVersionHolder pvh = EmbedConnection.getMonitor().getEngineVersion();
		if (pvh == null)
		{
		  return -1;
		}
		return pvh.getMajorVersion();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the minor version number of the underlying database.
    *
    * @return the underlying database's minor version
	*/
	public int getDatabaseMinorVersion()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
		ProductVersionHolder pvh = EmbedConnection.getMonitor().getEngineVersion();
		if (pvh == null)
		{
		  return -1;
		}
		return pvh.getMinorVersion();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the major JDBC version number for this driver.
    *
    * @return JDBC version major number
	*/
	public int getJDBCMajorVersion()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-1984
        return 4;
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the minor JDBC version number for this driver.
    *
    * @return JDBC version minor number
	*/
	public int getJDBCMinorVersion()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6324
        return JVMInfo.jdbcMinorVersion();
	}

	/**
    * JDBC 3.0
    *
    * Indicates whether the SQLSTATEs returned by SQLException.getSQLState
    * is X/Open (now known as Open Group) SQL CLI or SQL99.
    *
    * @return the type of SQLSTATEs, one of: sqlStateXOpen or sqlStateSQL99
	*/
	public int getSQLStateType()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
		return sqlStateSQL99;
	}

	/**
    * JDBC 3.0
    *
    * Indicates whether updates made to a LOB are made on a copy or
    * directly to the LOB.
    * <p>
    * For the embedded driver, all updates will be made to a copy.
    * Hence, this call will always return <code>true</code>
    *
    * @return true if updates are made to a copy of the LOB; false if
    * updates are made directly to the LOB
    * @exception SQLException Feature not implemented for now.
	*/
	public boolean locatorsUpdateCopy()
    throws SQLException
	{
		return true;
	}

	/**
    * JDBC 3.0
    *
    * Retrieves a description of the user-defined type (UDT) hierarchies defined
    * in a particular schema in this database. Only the immediate super type/ sub type
    * relationship is modeled.
    *
    * @param catalog - a catalog name; "" retrieves those without a catalog;
    * null means drop catalog name from the selection criteria
    * @param schemaPattern - a schema name pattern; "" retrieves those without a schema
    * @param typeNamePattern - a UDT name pattern; may be a fully-qualified name
    * @return a ResultSet object in which a row gives information about the designated UDT
    * @exception SQLException Feature not implemented for now.
	*/
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
    throws SQLException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-968
//IC see: https://issues.apache.org/jira/browse/DERBY-924
//IC see: https://issues.apache.org/jira/browse/DERBY-925
		return getSimpleQuery("getSuperTypes");
	}

	/**
    * JDBC 3.0
    *
    * Retrieves a description of the table hierarchies defined in a particular
    * schema in this database.
    *
    * @param catalog - a catalog name; "" retrieves those without a catalog;
    * null means drop catalog name from the selection criteria
    * @param schemaPattern - a schema name pattern; "" retrieves those without a schema
    * @param typeNamePattern - a UDT name pattern; may be a fully-qualified name
    * @return a ResultSet object in which each row is a type description
    * @exception SQLException if a database access error occurs
	*/
	public ResultSet getSuperTables(String catalog, String schemaPattern, String typeNamePattern)
    throws SQLException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-968
//IC see: https://issues.apache.org/jira/browse/DERBY-924
//IC see: https://issues.apache.org/jira/browse/DERBY-925
		return getSimpleQuery("getSuperTables");
	}

	/**
    * JDBC 3.0
    *
    * Retrieves a description of the given attribute of the given type for a
    * user-defined type (UDT) that is available in the given schema and catalog.
    *
    * @param catalog - a catalog name; must match the catalog name as it is
    * stored in the database; "" retrieves those without a catalog; null means that
    * the catalog name should not be used to narrow the search
    * @param schemaPattern - a schema name pattern; "" retrieves those without a schema;
    * null means that the schema name should not be used to narrow the search
    * @param typeNamePattern - a type name pattern; must match the type name as it is
    * stored in the database
    * @param attributeNamePattern - an attribute name pattern; must match the attribute
    * name as it is declared in the database
    * @return a ResultSet object in which each row is a type description
    * @exception SQLException if a database access error occurs.
	*/
	public ResultSet getAttributes(String catalog, String schemaPattern,
  String typeNamePattern, String attributeNamePattern)
    throws SQLException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-968
//IC see: https://issues.apache.org/jira/browse/DERBY-924
//IC see: https://issues.apache.org/jira/browse/DERBY-925
        return getSimpleQuery("getAttributes");
	}
	
    /////////////////////////////////////////////////////////////////////////
    //
    //  JDBC 4.0 - New public methods
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * JDBC 4.0
     *
     * <p>Returns a list of the client info properties supported by
     * the driver. The result set contains the following columns:
     *
     * <p>
     * <ol>
     *  <li>NAME String=&gt; The name of the client info property.</li>
     *  <li>MAX_LEN int=&gt; The maximum length of the value for the
     *      property.</li>
     *  <li>DEFAULT_VALUE String=&gt; The default value of the property.</li>
     *  <li>DESCRIPTION String=&gt; A description of the property.</li>
     * </ol>
     *
     * <p>The <code>ResultSet</code> is sorted by the NAME column.
     *
     * @return A <code>ResultSet</code> object; each row is a
     * supported client info property
     * @exception SQLException if an error occurs
     */
    public ResultSet getClientInfoProperties() throws SQLException {
        return getSimpleQuery("getClientInfoProperties");
    }

    /**
     * JDBC 4.0
     *
     * <p>Get the schema names available in this database. The results
     * are ordered by schema name.
     *
     * <p>The schema columns are:
     *  <ol>
     *  <li><strong>TABLE_SCHEM</strong> String =&gt; schema name</li>
     *  <li><strong>TABLE_CATALOG</strong> String =&gt; catalog name
     *  (may be <code>null</code>)</li>
     *  </ol>
     *
     * @param catalog catalog name used to narrow down the search; ""
     * means no catalog, <code>null</code> means any catalog
     * @param schemaPattern schema name used to narrow down the
     * search, <code>null</code> means schema name should not be used
     * to narrow down search
     * @return a <code>ResultSet</code> object in which each row is a
     * schema description
     * @exception SQLException if a database error occurs
     */
    public ResultSet getSchemas(String catalog, String schemaPattern)
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-876
        PreparedStatement s = getPreparedQuery("getSchemas");
        s.setString(1, swapNull(catalog));
        s.setString(2, swapNull(schemaPattern));
        return s.executeQuery();
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1984
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }

    /**
     * Returns whether or not all open {@code ResultSet}s on a {@code
     * Connection} are closed if an error occurs when auto-commit in enabled.
     *
     * @return {@code true}, since the embedded driver will close the open
     * {@code ResultSet}s
     */
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2789
        return true;
    }

    // java.sql.Wrapper interface methods

    /**
     * Returns whether or not this instance implements the specified interface.
     *
     * @param iface the interface to check for
     * @return true if this implements the interface
     */
    public final boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    /**
     * Returns {@code this} if this class implements the interface.
     *
     * @param iface the interface
     * @return an object that implements the interface
     * @throws SQLException if no object is found that implements the
     * interface
     */
    public final <T> T unwrap(Class<T> iface) throws SQLException {
        // Derby does not implement non-standard methods on JDBC objects,
        // hence return this if this class implements the interface
        // or throw an SQLException.
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw newSQLException(SQLState.UNABLE_TO_UNWRAP, iface);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  JDBC 4.1 - New public methods
    //
    /////////////////////////////////////////////////////////////////////////

    /** See DatabaseMetaData javadoc */
    public  boolean generatedKeyAlwaysReturned() { return true; }

	/**
    * See DatabaseMetaData javadoc. Empty ResultSet because Derby does
    * not support pseudo columns.
	*/
	public ResultSet getPseudoColumns
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        ( String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern )
        throws SQLException
	{
		return getSimpleQuery("getPseudoColumns");
	}

	//////////////////////////////////////////////////////////////
	//
	// MISC 
	//
	//////////////////////////////////////////////////////////////
	
    /**
     * Get metadata that the client driver will cache. The metadata is
     * fetched using SYSIBM.METADATA (found in metadata_net.properties).
     *
     * @return the result set returned by SYSIBM.METADATA
     * @exception SQLException if a database error occurs
     */
    public ResultSet getClientCachedMetaData() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1176
        return getSimpleQuery("METADATA", true);
    }

	/*
	 * utility helper routines:
	 */

    /**
     * Execute a query in metadata.properties (or SPS in the SYS
     * schema) or metadata_net.properties (or SPS in the SYSIBM
     * schema).
     *
     * @param nameKey the name of the query
     * @param net if <code>true</code>, execute a query in
     * metadata_net.properties; otherwise, execute a query in
     * metadata.properties
     * @return a <code>ResultSet</code> value
     * @exception SQLException if a database error occurs
     */
    private ResultSet getSimpleQuery(String nameKey, boolean net)
        throws SQLException
	{
		PreparedStatement ps = getPreparedQuery(nameKey, net);
		if (ps == null)
			return null;
	
		return ps.executeQuery();
	}

    /**
     * Execute a query in metadata.properties, or an SPS in the SYS
     * schema.
     *
     * @param nameKey the name of the query
     * @return a <code>ResultSet</code> value
     * @exception SQLException if a database error occurs
     */
    protected ResultSet getSimpleQuery(String nameKey) throws SQLException {
        return getSimpleQuery(nameKey, false);
    }

    /**
     * Get a stored prepared statement from the system tables.
     *
     * @param nameKey the name of the query
     * @param net if <code>true</code>, find query in SYSIBM schema;
     * otherwise, find query in SYS schema
     * @return a <code>PreparedStatement</code> value
     * @exception SQLException if a database error occurs
     */
    private PreparedStatement getPreparedQueryUsingSystemTables(String nameKey,
                                                                boolean net)
        throws SQLException 
	{
		synchronized (getConnectionSynchronization())
		{
			setupContextStack();
			PreparedStatement ps = null;

			try
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-1176
				String queryText =
					getQueryDescriptions(net).getProperty(nameKey);
				if (queryText == null)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-968
//IC see: https://issues.apache.org/jira/browse/DERBY-924
//IC see: https://issues.apache.org/jira/browse/DERBY-925
                    throw Util.notImplemented(nameKey);
				}
				
                ps = prepareSPS(nameKey, queryText, net);
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
                InterruptStatus.
                    restoreIntrFlagIfSeen(getLanguageConnectionContext());
			}

			catch (Throwable t) 
			{
				throw handleException(t);
			}

			finally 
			{
			    restoreContextStack();
			}
			return ps;
		}
	}

	/**
	 * Either get the prepared query for the metadata call from the
	 * system tables, or from the metadata.properties or
	 * metadata_net.properties file.
	 * In soft upgrade mode, the queries stored in the system tables
	 * might not be upto date with the Derby engine release because
	 * system tables can't be modified in backward incompatible way in
	 * soft upgrade mode. Because of this, if the database is in 
	 * soft upgrade mode, get the queries from metadata.properties
	 * file rather than from the system tables.
	 *
	 * Getting queries from metadata(_net).properties might cause problems
	 * if system catalogs have been changed between versions either by
	 * addition of columns or have new catalogs. To continue
	 * to support soft upgrade from older versions of database, find
	 * query that most closely matches database dictionary version.
	 *
	 * @param queryName Name of the metadata query for which we need
	 * a prepared statement
	 * @param net if <code>true</code>, use metadata_net.properties
	 * instead of metadata.properties
	 * @return PreparedStatement
	 * @exception SQLException if a database error occurs
	 */
	private PreparedStatement getPreparedQuery(String queryName,
											   boolean net)
			throws SQLException {
		PreparedStatement s;
		//We can safely goto system table since we are not in soft upgrade
		//mode and hence metadata sql in system tables are uptodate
		//with this Derby release. We also need to be writable so 
                // that we can update SPS statements (DERBY-3546)
		if (notInSoftUpgradeMode() && !isReadOnly())
//IC see: https://issues.apache.org/jira/browse/DERBY-1176
			s = getPreparedQueryUsingSystemTables(queryName, net);
		else {
			try {
				//Can't use stored prepared statements because we are in soft upgrade
				//mode or are read only, and hence need to get metadata sql from 
                                //metadata.properties file or metadata_net.properties
				String queryText = getQueryFromDescription(queryName, net);
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
                s = getEmbedConnection().prepareMetaDataStatement(queryText);
                // InterruptStatus.restoreIntrFlagIfSeen: called inside
                // prepareMetaDataStatement while we still have context pushed.
			} catch (Throwable t) {
				throw handleException(t);
			} 
		}
		return s;
	}	

    /**
     * Get a prepared query from system tables or metadata.properties.
     *
     * @param queryName name of the query
     * @return a <code>PreparedStatement</code> value
     * @exception SQLException if a database error occurs
     */
    protected PreparedStatement getPreparedQuery(String queryName)
//IC see: https://issues.apache.org/jira/browse/DERBY-1176
        throws SQLException
    {
        return getPreparedQuery(queryName, false);
    }

	/**
	 * Given a queryName, find closest match in queryDescriptions. This method
	 * should be called in soft-upgrade mode only, where current software version
	 * doesn't match dictionary version. For these cases, there may be
	 * multiple entries in queryDescriptions for given queryName. Find a
	 * version of the query that closely matches dictionary version.
	 *
	 * This method is currently coded to handle two specific queries,
	 * getColumnPrivileges and getTablePrivileges. Derby databases that are 10.1
	 * or earlier will not have new system tables added for 10.2 for privileges.
	 * 
	 * It should be possible to automate finding closest match by generating
	 * all Major_Minor versions between software version and dictionary version
	 * and try each one from Dictionary version to current version. Since only
	 * needed for two queries, overhead may not be worth it yet.
	 *
	 * @param queryName name of the query
	 * @param net if <code>true</code>, get the query from
	 * metadata_net.properties instead of metadata.properties
	 * @return the query text
	 * @exception StandardException if an error occurs
	 */
	private String getQueryFromDescription(String queryName, boolean net)
//IC see: https://issues.apache.org/jira/browse/DERBY-1124
		throws StandardException
	{
		DataDictionary dd = getLanguageConnectionContext().getDataDictionary();

		// If dictionary version is below 10.2, special case
		// getColumnPrivileges and getTablePrivileges since new system tables
		// for privileges wouldn't be present.
		if (!dd.checkVersion(DataDictionary.DD_VERSION_DERBY_10_2, null))
		{
			if (queryName.equals("getColumnPrivileges"))
				queryName = "getColumnPrivileges_10_1";

			if (queryName.equals("getTablePrivileges"))
				queryName = "getTablePrivileges_10_1";
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-1176
		return getQueryDescriptions(net).getProperty(queryName);
	}

	/*
	** Given a SPS name and a query text it returns a 
	** java.sql.PreparedStatement for the SPS. If the SPS
	** doeesn't exist is created.
	** 
	*/
	private PreparedStatement prepareSPS(String	spsName, 
//IC see: https://issues.apache.org/jira/browse/DERBY-1176
										 String	spsText,
										 boolean net)
		throws StandardException, SQLException
	{

		LanguageConnectionContext lcc = getLanguageConnectionContext();

		/* We now need to do this in sub transaction because we could possibly recompile SPS
		 * later, and the recompile is in a sub transaction, and will update the SYSSTATEMENTS
		 * entry.  Don't want to block.
		 */
		lcc.beginNestedTransaction(true);

		DataDictionary dd = getLanguageConnectionContext().getDataDictionary();
		SPSDescriptor spsd = dd.getSPSDescriptor(
										spsName, 
//IC see: https://issues.apache.org/jira/browse/DERBY-1176
										net ? dd.getSysIBMSchemaDescriptor() :
										dd.getSystemSchemaDescriptor());
		lcc.commitNestedTransaction();

		if (spsd == null)
		{
			throw Util.notImplemented(spsName);
		}

		/* manish:
		   There should be a nicer way of getting a 
		   java.sql.PreparedStatement from an SPS descriptor!
		*/
		/*
		** It is unnecessarily expensive to get the
		** the statement, and then send an EXECUTE
		** statement, but we have no (easy) way of turning
		** the statement into a java.sql.PreparedStatement.
		*/	
//IC see: https://issues.apache.org/jira/browse/DERBY-1176
		String queryText =
			"EXECUTE STATEMENT " + (net ? "SYSIBM" : "SYS") +
			".\"" + spsName + "\"";
		return getEmbedConnection().prepareMetaDataStatement(queryText);

	}

	static final protected String swapNull(String s) {
		return (s == null ? "%" : s);
	}

	/**
	  *	Gets the LanguageConnectionContext for this connection.
	  *
	  *	@return	the lcc for this connection
	  *
	  */
	private	LanguageConnectionContext	getLanguageConnectionContext()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6751
        return AccessController.doPrivileged
            (
             new PrivilegedAction<LanguageConnectionContext>()
             {
                 public LanguageConnectionContext run()
                 {
                     return getEmbedConnection().getLanguageConnection();
                 }
             }
             );
	}

	/*
	** Priv block code, moved out of the old Java2 version.
	*/

    /**
     * Loads the query descriptions from metadata.properties and
     * metadata_net.properties into <code>queryDescriptions</code> and
     * <code>queryDescriptions_net</code>.
     */
    private void loadQueryDescriptions() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1176
        java.security.AccessController.doPrivileged(this);
    }

	/**
	 * Performs a privileged action. Reads the query descriptions.
	 *
	 * @return <code>null</code>
	 */
	public final Object run() {
		// SECURITY PERMISSION - IP3
		PBloadQueryDescriptions();
		return null;
	}

}
