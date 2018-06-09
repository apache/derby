/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SystemCatalogTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests concerning the system catalogs.
 * 
 * Retaining comment from previous .sql test:
 * RESOLVE - add selects from sysdepends when simplified
 *
 */
public class SystemCatalogTest extends BaseJDBCTestCase {

    public SystemCatalogTest(String name) {
		super(name);
	}
	
	public static Test suite() {
		Test suite = TestConfiguration.defaultSuite(SystemCatalogTest.class);
		return TestConfiguration.singleUseDatabaseDecorator(suite);
	}
	
	/**
	 * Test that the user cannot execute any DDL statements on the system tables.
	 * @throws SQLException
	 */
	public void testNoUserDDLOnSystemTables() throws SQLException {
	    Statement s = createStatement();
	    
	    assertStatementError("X0Y56", s, "drop table sys.systables");
	    assertStatementError("42X62", s, "drop index sys.sysaliases_index2");
	    assertStatementError("42X62", s, "create index trash on sys.systables(tableid)");
	    assertStatementError("42X62", s, "create table sys.usertable(c1 int)");
	    assertStatementError("42X62", s, "create view sys.userview as select * from sys.systables");
	    assertStatementError("42X62", s, "alter table sys.systables drop column tablename");
	    assertStatementError("42X62", s, "alter table sys.systables add column foo int");
	    assertStatementError("42X62", s, "alter table sys.systables alter column tablename null");
	    assertStatementError("42X62", s, "alter table sys.systables drop primary key");
	    
	    s.close();
	}
	
	/**
	 * Test that the system tables cannot be changed by various DML statements.
	 * 
	 * @throws SQLException
	 */
	public void testSystemCatalogsNotUpdatable() throws SQLException{
		Connection c = getConnection();
		Statement s = c.createStatement();
		
	    c.setAutoCommit(false);
	    
	    try{
	    	s.executeUpdate("delete from sys.systables");
	    } catch (SQLException e)
	    {
	    	assertSQLState("42Y25", e);
	    }
	    
	    try{
	    	s.executeUpdate("update sys.systables set tablename = tablename || 'trash'");
	    } catch (SQLException e)
	    {
	    	assertSQLState("42Y25", e);
	    }
	    
	    try{
	    	s.executeUpdate("insert into sys.systables select * from sys.systables");
	    } catch (SQLException e)
	    {
	    	assertSQLState("42Y25", e);
	    }
	    
	    try{
	    	ResultSet rs = s.executeQuery("select tablename from sys.systables for update of tablename");
	    } catch (SQLException e)
	    {
	    	assertSQLState("42Y90", e);
	    }
	    
        c.rollback();
	    c.setAutoCommit(true);
	    
	}
	
	/**
	 * Test various default store properties for the system tables.
	 * 
	 * @throws SQLException
	 */
	public void testSystemCatalogStoreProperties() throws SQLException{
		Statement s = createStatement();
		s.execute("create function gatp(SCH VARCHAR(128), TBL VARCHAR(128)) RETURNS VARCHAR(1000) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestPropertyInfo.getAllTableProperties' LANGUAGE JAVA PARAMETER STYLE JAVA");
		s.execute("create function gaip(SCH VARCHAR(128), TBL VARCHAR(128)) RETURNS VARCHAR(1000) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestPropertyInfo.getAllIndexProperties' LANGUAGE JAVA PARAMETER STYLE JAVA");

		// get the properties for the heaps
		ResultSet rs = s.executeQuery("select tablename,gatp('SYS', tablename) from sys.systables order by tablename");
		boolean nonEmptyResultSet = false;
		String tablename = null;
		String sysdummy = "SYSDUMMY1";
		String heapResult = "{ derby.storage.initialPages=1, derby.storage.minimumRecordSize=12, derby.storage.pageReservedSpace=0, derby.storage.pageSize=4096, derby.storage.reusableRecordId=false }";
		while(rs.next()) {
			nonEmptyResultSet  = true;
			tablename = rs.getString(1);
			if (tablename.equals(sysdummy)) {
				assertTrue(rs.getString(2).startsWith("{  }"));
			} else {
				assertTrue(rs.getString(2).startsWith(heapResult));
			}
		}
		assertTrue(nonEmptyResultSet);
		rs.close();
				
		// get the properties for the indexes
		rs = s.executeQuery("select conglomeratename, gaip('SYS',	conglomeratename) from sys.sysconglomerates where isindex order by conglomeratename");
		nonEmptyResultSet = false;
		String indexResult = "{ derby.storage.initialPages=1, derby.storage.minimumRecordSize=1, derby.storage.pageReservedSpace=0, derby.storage.pageSize=4096, derby.storage.reusableRecordId=true }";
		while(rs.next()) {
			nonEmptyResultSet  = true;
			assertTrue(rs.getString(2).startsWith(indexResult));
		}
		assertTrue(nonEmptyResultSet);
	    rs.close();
	    s.close();
	}         

	/**
	 * Test that each system table has a table type of "S".
	 * 
	 * @throws SQLException
	 */
	public void testSystemCatalogTableTypes() throws SQLException {
		Statement s = createStatement();
		ResultSet rs = s.executeQuery("select TABLENAME, TABLETYPE from sys.systables order by tablename");
		
		boolean nonEmptyResultSet = false;
		while(rs.next()) {
			nonEmptyResultSet  = true;
			assertEquals("S", rs.getString(2));
		}
		assertTrue(nonEmptyResultSet);
		rs.close();
		s.close();
	}
	
	/**
	 * Check that all the tables for their expected columns.
	 *
	 * @throws SQLException
	 */
	public void testSystemCatalogColumns() throws SQLException {
		String [][] expected = {
				{"SYSALIASES", "ALIAS", "2", "VARCHAR(128) NOT NULL"},
				{"SYSALIASES", "ALIASID", "1", "CHAR(36) NOT NULL"},
				{"SYSALIASES", "ALIASINFO", "8", "org.apache.derby.catalog.AliasInfo"},
				{"SYSALIASES", "ALIASTYPE", "5", "CHAR(1) NOT NULL"},
				{"SYSALIASES", "JAVACLASSNAME", "4", "LONG VARCHAR NOT NULL"},
				{"SYSALIASES", "NAMESPACE", "6", "CHAR(1) NOT NULL"},
				{"SYSALIASES", "SCHEMAID", "3", "CHAR(36)"},
				{"SYSALIASES", "SPECIFICNAME", "9", "VARCHAR(128) NOT NULL"},
				{"SYSALIASES", "SYSTEMALIAS", "7", "BOOLEAN NOT NULL"},
				{"SYSCHECKS", "CHECKDEFINITION", "2", "LONG VARCHAR NOT NULL"},
				{"SYSCHECKS", "CONSTRAINTID", "1", "CHAR(36) NOT NULL"},
				{"SYSCHECKS", "REFERENCEDCOLUMNS", "3", "org.apache.derby.catalog.ReferencedColumns NOT NULL"},
				{"SYSCOLPERMS", "COLPERMSID", "1", "CHAR(36) NOT NULL"},
				{"SYSCOLPERMS", "COLUMNS", "6", "org.apache.derby.iapi.services.io.FormatableBitSet NOT NULL"},
				{"SYSCOLPERMS", "GRANTEE", "2", "VARCHAR(128) NOT NULL"},
				{"SYSCOLPERMS", "GRANTOR", "3", "VARCHAR(128) NOT NULL"},
				{"SYSCOLPERMS", "TABLEID", "4", "CHAR(36) NOT NULL"},
				{"SYSCOLPERMS", "TYPE", "5", "CHAR(1) NOT NULL"},
				{"SYSCOLUMNS", "AUTOINCREMENTCYCLE", "10", "BOOLEAN"},
				{"SYSCOLUMNS", "AUTOINCREMENTINC", "9", "BIGINT"},
				{"SYSCOLUMNS", "AUTOINCREMENTSTART", "8", "BIGINT"},
				{"SYSCOLUMNS", "AUTOINCREMENTVALUE", "7", "BIGINT"},
				{"SYSCOLUMNS", "COLUMNDATATYPE", "4", "org.apache.derby.catalog.TypeDescriptor NOT NULL"},
				{"SYSCOLUMNS", "COLUMNDEFAULT", "5", "java.io.Serializable"},
				{"SYSCOLUMNS", "COLUMNDEFAULTID", "6", "CHAR(36)"},
				{"SYSCOLUMNS", "COLUMNNAME", "2", "VARCHAR(128) NOT NULL"},
				{"SYSCOLUMNS", "COLUMNNUMBER", "3", "INTEGER NOT NULL"},
				{"SYSCOLUMNS", "REFERENCEID", "1", "CHAR(36) NOT NULL"},
				{"SYSCONGLOMERATES", "CONGLOMERATEID", "8", "CHAR(36) NOT NULL"},
				{"SYSCONGLOMERATES", "CONGLOMERATENAME", "4", "VARCHAR(128)"},
				{"SYSCONGLOMERATES", "CONGLOMERATENUMBER", "3", "BIGINT NOT NULL"},
				{"SYSCONGLOMERATES", "DESCRIPTOR", "6", "org.apache.derby.catalog.IndexDescriptor"},
				{"SYSCONGLOMERATES", "ISCONSTRAINT", "7", "BOOLEAN"},
				{"SYSCONGLOMERATES", "ISINDEX", "5", "BOOLEAN NOT NULL"},
				{"SYSCONGLOMERATES", "SCHEMAID", "1", "CHAR(36) NOT NULL"},
				{"SYSCONGLOMERATES", "TABLEID", "2", "CHAR(36) NOT NULL"},
				{"SYSCONSTRAINTS", "CONSTRAINTID", "1", "CHAR(36) NOT NULL"},
				{"SYSCONSTRAINTS", "CONSTRAINTNAME", "3", "VARCHAR(128) NOT NULL"},
				{"SYSCONSTRAINTS", "REFERENCECOUNT", "7", "INTEGER NOT NULL"},
				{"SYSCONSTRAINTS", "SCHEMAID", "5", "CHAR(36) NOT NULL"},
				{"SYSCONSTRAINTS", "STATE", "6", "CHAR(1) NOT NULL"},
				{"SYSCONSTRAINTS", "TABLEID", "2", "CHAR(36) NOT NULL"},
				{"SYSCONSTRAINTS", "TYPE", "4", "CHAR(1) NOT NULL"},
				{"SYSDEPENDS", "DEPENDENTFINDER", "2", "org.apache.derby.catalog.DependableFinder NOT NULL"},
				{"SYSDEPENDS", "DEPENDENTID", "1", "CHAR(36) NOT NULL"},
				{"SYSDEPENDS", "PROVIDERFINDER", "4", "org.apache.derby.catalog.DependableFinder NOT NULL"},
				{"SYSDEPENDS", "PROVIDERID", "3", "CHAR(36) NOT NULL"},
				{"SYSDUMMY1", "IBMREQD", "1", "CHAR(1)"},
				{"SYSFILES", "FILEID", "1", "CHAR(36) NOT NULL"},
				{"SYSFILES", "FILENAME", "3", "VARCHAR(128) NOT NULL"},
				{"SYSFILES", "GENERATIONID", "4", "BIGINT NOT NULL"},
				{"SYSFILES", "SCHEMAID", "2", "CHAR(36) NOT NULL"},
				{"SYSFOREIGNKEYS", "CONGLOMERATEID", "2", "CHAR(36) NOT NULL"},
				{"SYSFOREIGNKEYS", "CONSTRAINTID", "1", "CHAR(36) NOT NULL"},
				{"SYSFOREIGNKEYS", "DELETERULE", "4", "CHAR(1) NOT NULL"},
				{"SYSFOREIGNKEYS", "KEYCONSTRAINTID", "3", "CHAR(36) NOT NULL"},
				{"SYSFOREIGNKEYS", "UPDATERULE", "5", "CHAR(1) NOT NULL"},
				{"SYSKEYS", "CONGLOMERATEID", "2", "CHAR(36) NOT NULL"},
				{"SYSKEYS", "CONSTRAINTID", "1", "CHAR(36) NOT NULL"},
                {"SYSPERMS", "GRANTEE", "6", "VARCHAR(128) NOT NULL"},
                {"SYSPERMS", "GRANTOR", "5", "VARCHAR(128) NOT NULL"},
                {"SYSPERMS", "ISGRANTABLE", "7", "CHAR(1) NOT NULL"},
                {"SYSPERMS", "OBJECTID", "3", "CHAR(36) NOT NULL"},
                {"SYSPERMS", "OBJECTTYPE", "2", "VARCHAR(36) NOT NULL"},
                {"SYSPERMS", "PERMISSION", "4", "CHAR(36) NOT NULL"},
                {"SYSPERMS", "UUID", "1", "CHAR(36) NOT NULL"},
                {"SYSROLES", "GRANTEE", "3", "VARCHAR(128) NOT NULL"},
                {"SYSROLES", "GRANTOR", "4", "VARCHAR(128) NOT NULL"},
				{"SYSROLES", "ISDEF", "6", "CHAR(1) NOT NULL"},
				{"SYSROLES", "ROLEID", "2", "VARCHAR(128) NOT NULL"},
				{"SYSROLES", "UUID", "1", "CHAR(36) NOT NULL"},
				{"SYSROLES", "WITHADMINOPTION", "5", "CHAR(1) NOT NULL"},
				{"SYSROUTINEPERMS", "ALIASID", "4", "CHAR(36) NOT NULL"},
				{"SYSROUTINEPERMS", "GRANTEE", "2", "VARCHAR(128) NOT NULL"},
				{"SYSROUTINEPERMS", "GRANTOPTION", "5", "CHAR(1) NOT NULL"},
				{"SYSROUTINEPERMS", "GRANTOR", "3", "VARCHAR(128) NOT NULL"},
				{"SYSROUTINEPERMS", "ROUTINEPERMSID", "1", "CHAR(36) NOT NULL"},
				{"SYSSCHEMAS", "AUTHORIZATIONID", "3", "VARCHAR(128) NOT NULL"},
				{"SYSSCHEMAS", "SCHEMAID", "1", "CHAR(36) NOT NULL"},
				{"SYSSCHEMAS", "SCHEMANAME", "2", "VARCHAR(128) NOT NULL"},
                {"SYSSEQUENCES", "CURRENTVALUE", "5", "BIGINT"},
                {"SYSSEQUENCES", "CYCLEOPTION", "10", "CHAR(1) NOT NULL"},
                {"SYSSEQUENCES", "INCREMENT", "9", "BIGINT NOT NULL"},
                {"SYSSEQUENCES", "MAXIMUMVALUE", "8", "BIGINT NOT NULL"},
                {"SYSSEQUENCES", "MINIMUMVALUE", "7", "BIGINT NOT NULL"},
                {"SYSSEQUENCES", "SCHEMAID", "3", "CHAR(36) NOT NULL"},
                {"SYSSEQUENCES", "SEQUENCEDATATYPE", "4", "org.apache.derby.catalog.TypeDescriptor NOT NULL"},
                {"SYSSEQUENCES", "SEQUENCEID", "1", "CHAR(36) NOT NULL"},
                {"SYSSEQUENCES", "SEQUENCENAME", "2", "VARCHAR(128) NOT NULL"},
                {"SYSSEQUENCES", "STARTVALUE", "6", "BIGINT NOT NULL"},                
				{"SYSSTATEMENTS", "COMPILATIONSCHEMAID", "8", "CHAR(36)"},
                {"SYSSTATEMENTS", "LASTCOMPILED", "7", "TIMESTAMP"},
				{"SYSSTATEMENTS", "SCHEMAID", "3", "CHAR(36) NOT NULL"},
				{"SYSSTATEMENTS", "STMTID", "1", "CHAR(36) NOT NULL"},
				{"SYSSTATEMENTS", "STMTNAME", "2", "VARCHAR(128) NOT NULL"},
				{"SYSSTATEMENTS", "TEXT", "6", "LONG VARCHAR NOT NULL"},
				{"SYSSTATEMENTS", "TYPE", "4", "CHAR(1) NOT NULL"},
				{"SYSSTATEMENTS", "USINGTEXT", "9", "LONG VARCHAR"},
				{"SYSSTATEMENTS", "VALID", "5", "BOOLEAN NOT NULL"},
				{"SYSSTATISTICS", "COLCOUNT", "7", "INTEGER NOT NULL"},
				{"SYSSTATISTICS", "CREATIONTIMESTAMP", "4", "TIMESTAMP NOT NULL"},
				{"SYSSTATISTICS", "REFERENCEID", "2", "CHAR(36) NOT NULL"},
				{"SYSSTATISTICS", "STATID", "1", "CHAR(36) NOT NULL"},
				{"SYSSTATISTICS", "STATISTICS", "8", "org.apache.derby.catalog.Statistics NOT NULL"},
				{"SYSSTATISTICS", "TABLEID", "3", "CHAR(36) NOT NULL"},
				{"SYSSTATISTICS", "TYPE", "5", "CHAR(1) NOT NULL"},
				{"SYSSTATISTICS", "VALID", "6", "BOOLEAN NOT NULL"},
				{"SYSTABLEPERMS", "DELETEPRIV", "6", "CHAR(1) NOT NULL"},
				{"SYSTABLEPERMS", "GRANTEE", "2", "VARCHAR(128) NOT NULL"},
				{"SYSTABLEPERMS", "GRANTOR", "3", "VARCHAR(128) NOT NULL"},
				{"SYSTABLEPERMS", "INSERTPRIV", "7", "CHAR(1) NOT NULL"},
				{"SYSTABLEPERMS", "REFERENCESPRIV", "9", "CHAR(1) NOT NULL"},
				{"SYSTABLEPERMS", "SELECTPRIV", "5", "CHAR(1) NOT NULL"},
				{"SYSTABLEPERMS", "TABLEID", "4", "CHAR(36) NOT NULL"},
				{"SYSTABLEPERMS", "TABLEPERMSID", "1", "CHAR(36) NOT NULL"},
				{"SYSTABLEPERMS", "TRIGGERPRIV", "10", "CHAR(1) NOT NULL"},
				{"SYSTABLEPERMS", "UPDATEPRIV", "8", "CHAR(1) NOT NULL"},
				{"SYSTABLES", "LOCKGRANULARITY", "5", "CHAR(1) NOT NULL"},
				{"SYSTABLES", "SCHEMAID", "4", "CHAR(36) NOT NULL"},
				{"SYSTABLES", "TABLEID", "1", "CHAR(36) NOT NULL"},
				{"SYSTABLES", "TABLENAME", "2", "VARCHAR(128) NOT NULL"},
				{"SYSTABLES", "TABLETYPE", "3", "CHAR(1) NOT NULL"},
				{"SYSTRIGGERS", "ACTIONSTMTID", "11", "CHAR(36)"},
				{"SYSTRIGGERS", "CREATIONTIMESTAMP", "4", "TIMESTAMP NOT NULL"},
				{"SYSTRIGGERS", "EVENT", "5", "CHAR(1) NOT NULL"},
				{"SYSTRIGGERS", "FIRINGTIME", "6", "CHAR(1) NOT NULL"},
				{"SYSTRIGGERS", "NEWREFERENCINGNAME", "17", "VARCHAR(128)"},
				{"SYSTRIGGERS", "OLDREFERENCINGNAME", "16", "VARCHAR(128)"},
				{"SYSTRIGGERS", "REFERENCEDCOLUMNS", "12", "org.apache.derby.catalog.ReferencedColumns"},
				{"SYSTRIGGERS", "REFERENCINGNEW", "15", "BOOLEAN"},
				{"SYSTRIGGERS", "REFERENCINGOLD", "14", "BOOLEAN"},
				{"SYSTRIGGERS", "SCHEMAID", "3", "CHAR(36) NOT NULL"},
				{"SYSTRIGGERS", "STATE", "8", "CHAR(1) NOT NULL"},
				{"SYSTRIGGERS", "TABLEID", "9", "CHAR(36) NOT NULL"},
				{"SYSTRIGGERS", "TRIGGERDEFINITION", "13", "LONG VARCHAR"},
				{"SYSTRIGGERS", "TRIGGERID", "1", "CHAR(36) NOT NULL"},
				{"SYSTRIGGERS", "TRIGGERNAME", "2", "VARCHAR(128) NOT NULL"},
				{"SYSTRIGGERS", "TYPE", "7", "CHAR(1) NOT NULL"},
                {"SYSTRIGGERS", "WHENCLAUSETEXT", "18", "LONG VARCHAR"},
				{"SYSTRIGGERS", "WHENSTMTID", "10", "CHAR(36)"},
				{"SYSUSERS", "HASHINGSCHEME", "2", "VARCHAR(32672) NOT NULL"},
				{"SYSUSERS", "LASTMODIFIED", "4", "TIMESTAMP NOT NULL"},
				{"SYSUSERS", "PASSWORD", "3", "VARCHAR(32672) NOT NULL"},
				{"SYSUSERS", "USERNAME", "1", "VARCHAR(128) NOT NULL"},
				{"SYSVIEWS", "CHECKOPTION", "3", "CHAR(1) NOT NULL"},
				{"SYSVIEWS", "COMPILATIONSCHEMAID", "4", "CHAR(36)"},
				{"SYSVIEWS", "TABLEID", "1", "CHAR(36) NOT NULL"},
				{"SYSVIEWS", "VIEWDEFINITION", "2", "LONG VARCHAR NOT NULL"}
		};
				
		Statement s = createStatement();
		
		ResultSet rs = s.executeQuery("select TABLENAME, COLUMNNAME, COLUMNNUMBER, COLUMNDATATYPE from sys.systables t, sys.syscolumns c" +
				" where t.TABLEID=c.REFERENCEID order by TABLENAME, COLUMNNAME");
		JDBC.assertFullResultSet(rs, expected);
		rs.close();
				                      
		s.close();
	
	}
	
	public void testSystemCatalogIndexes() throws SQLException{
		String [][] expected = 
		{
				{"SYSALIASES", "SYSALIASES_HEAP", "false"},
				{"SYSALIASES", "SYSALIASES_INDEX3", "true"},
				{"SYSALIASES", "SYSALIASES_INDEX2", "true"},
				{"SYSALIASES", "SYSALIASES_INDEX1", "true"},
				{"SYSCHECKS", "SYSCHECKS_HEAP", "false"},
				{"SYSCHECKS", "SYSCHECKS_INDEX1", "true"},
				{"SYSCOLPERMS", "SYSCOLPERMS_HEAP", "false"},
				{"SYSCOLPERMS", "SYSCOLPERMS_INDEX3", "true"},
				{"SYSCOLPERMS", "SYSCOLPERMS_INDEX2", "true"},
				{"SYSCOLPERMS", "SYSCOLPERMS_INDEX1", "true"},
				{"SYSCOLUMNS", "SYSCOLUMNS_HEAP", "false"},
				{"SYSCOLUMNS", "SYSCOLUMNS_INDEX2", "true"},
				{"SYSCOLUMNS", "SYSCOLUMNS_INDEX1", "true"},
				{"SYSCONGLOMERATES", "SYSCONGLOMERATES_HEAP", "false"},
				{"SYSCONGLOMERATES", "SYSCONGLOMERATES_INDEX3", "true"},
				{"SYSCONGLOMERATES", "SYSCONGLOMERATES_INDEX2", "true"},
				{"SYSCONGLOMERATES", "SYSCONGLOMERATES_INDEX1", "true"},
				{"SYSCONSTRAINTS", "SYSCONSTRAINTS_HEAP", "false"},
				{"SYSCONSTRAINTS", "SYSCONSTRAINTS_INDEX3", "true"},
				{"SYSCONSTRAINTS", "SYSCONSTRAINTS_INDEX2", "true"},
				{"SYSCONSTRAINTS", "SYSCONSTRAINTS_INDEX1", "true"},
				{"SYSDEPENDS", "SYSDEPENDS_HEAP", "false"},
				{"SYSDEPENDS", "SYSDEPENDS_INDEX2", "true"},
				{"SYSDEPENDS", "SYSDEPENDS_INDEX1", "true"},
				{"SYSDUMMY1", "SYSDUMMY1_HEAP", "false"},
				{"SYSFILES", "SYSFILES_HEAP", "false"},
				{"SYSFILES", "SYSFILES_INDEX2", "true"},
				{"SYSFILES", "SYSFILES_INDEX1", "true"},
				{"SYSFOREIGNKEYS", "SYSFOREIGNKEYS_HEAP", "false"},
				{"SYSFOREIGNKEYS", "SYSFOREIGNKEYS_INDEX2", "true"},
				{"SYSFOREIGNKEYS", "SYSFOREIGNKEYS_INDEX1", "true"},
				{"SYSKEYS", "SYSKEYS_HEAP", "false"},
				{"SYSKEYS", "SYSKEYS_INDEX1", "true"},
                {"SYSPERMS", "SYSPERMS_HEAP", "false"},
                {"SYSPERMS", "SYSPERMS_INDEX3", "true"},
                {"SYSPERMS", "SYSPERMS_INDEX2", "true"},
                {"SYSPERMS", "SYSPERMS_INDEX1", "true"},
                {"SYSROLES", "SYSROLES_HEAP", "false"},
				{"SYSROLES", "SYSROLES_INDEX3", "true"},
				{"SYSROLES", "SYSROLES_INDEX2", "true"},
				{"SYSROLES", "SYSROLES_INDEX1", "true"},
                {"SYSROUTINEPERMS", "SYSROUTINEPERMS_HEAP", "false"},
				{"SYSROUTINEPERMS", "SYSROUTINEPERMS_INDEX3", "true"},
				{"SYSROUTINEPERMS", "SYSROUTINEPERMS_INDEX2", "true"},
				{"SYSROUTINEPERMS", "SYSROUTINEPERMS_INDEX1", "true"},
				{"SYSSCHEMAS", "SYSSCHEMAS_HEAP", "false"},
				{"SYSSCHEMAS", "SYSSCHEMAS_INDEX2", "true"},
				{"SYSSCHEMAS", "SYSSCHEMAS_INDEX1", "true"},
                {"SYSSEQUENCES", "SYSSEQUENCES_HEAP", "false"},
                {"SYSSEQUENCES", "SYSSEQUENCES_INDEX2", "true"},
                {"SYSSEQUENCES", "SYSSEQUENCES_INDEX1", "true"},
                {"SYSSTATEMENTS", "SYSSTATEMENTS_HEAP", "false"},
				{"SYSSTATEMENTS", "SYSSTATEMENTS_INDEX2", "true"},
				{"SYSSTATEMENTS", "SYSSTATEMENTS_INDEX1", "true"},
				{"SYSSTATISTICS", "SYSSTATISTICS_HEAP", "false"},
				{"SYSSTATISTICS", "SYSSTATISTICS_INDEX1", "true"},
				{"SYSTABLEPERMS", "SYSTABLEPERMS_HEAP", "false"},
				{"SYSTABLEPERMS", "SYSTABLEPERMS_INDEX3", "true"},
				{"SYSTABLEPERMS", "SYSTABLEPERMS_INDEX2", "true"},
				{"SYSTABLEPERMS", "SYSTABLEPERMS_INDEX1", "true"},
				{"SYSTABLES", "SYSTABLES_HEAP", "false"},
				{"SYSTABLES", "SYSTABLES_INDEX2", "true"},
				{"SYSTABLES", "SYSTABLES_INDEX1", "true"},
				{"SYSTRIGGERS", "SYSTRIGGERS_HEAP", "false"},
				{"SYSTRIGGERS", "SYSTRIGGERS_INDEX3", "true"},
				{"SYSTRIGGERS", "SYSTRIGGERS_INDEX2", "true"},
				{"SYSTRIGGERS", "SYSTRIGGERS_INDEX1", "true"},
				{"SYSUSERS", "SYSUSERS_HEAP", "false"},
				{"SYSUSERS", "SYSUSERS_INDEX1", "true"},
				{"SYSVIEWS", "SYSVIEWS_HEAP", "false"},
				{"SYSVIEWS", "SYSVIEWS_INDEX1", "true"},
			};
		
		Statement s = createStatement();
		
		ResultSet rs = s.executeQuery("select TABLENAME, CONGLOMERATENAME, ISINDEX from sys.systables t, sys.sysconglomerates c"
				                      +  " where t.TABLEID=c.TABLEID order by TABLENAME, ISINDEX");
		JDBC.assertFullResultSet(rs, expected);
		rs.close();
				                      
		s.close();
	}
	
	/**
	 * Check that a newly created table and its columns appear in SYSTABLES and SYSCOLUMNS
	 * @throws SQLException
	 */
	public void testNewTableInSystemCatalogs() throws SQLException {
		Statement s = createStatement();
		
		s.execute("create table t (i int, s smallint)");
		
		ResultSet rs = s.executeQuery("select TABLETYPE from sys.systables where tablename = 'T'");
		JDBC.assertSingleValueResultSet(rs, "T");
		rs.close();
		
		rs = s.executeQuery("select TABLENAME, COLUMNNAME, COLUMNNUMBER, columndatatype from sys.systables t, sys.syscolumns c" +
				" where t.TABLEID=c.REFERENCEID and t.tablename = 'T' order by TABLENAME, COLUMNNAME");
        String[][] expected = {{"T", "I", "1", "INTEGER"}, {"T", "S", "2", "SMALLINT"}};
        JDBC.assertFullResultSet(rs,expected);
        rs.close();
        
        rs = s.executeQuery("select TABLENAME, ISINDEX from sys.systables t, sys.sysconglomerates c where t.TABLEID=c.TABLEID and t.TABLENAME = 'T' order by TABLENAME, ISINDEX");
        expected = new String[][] {{"T", "false"},};
        JDBC.assertFullResultSet(rs,expected);
        rs.close();
        
        s.execute("drop table t");
        
        s.close();
	}
	
	/**
	 * Test that table and column names over thirty characters are recorded
	 * properly in the system tables.
	 * 
	 * @throws SQLException
	 */
	public void testOverThirtyCharsInTableName() throws SQLException {
		Statement s = createStatement();
		
		s.execute("create table t234567890123456789012345678901234567890 (c234567890123456789012345678901234567890 int)");
		
		ResultSet rs = s.executeQuery("select TABLENAME from sys.systables where length(TABLENAME) > 30 order by tablename");
		JDBC.assertSingleValueResultSet(rs, "T234567890123456789012345678901234567890");
		rs.close();
		
		rs = s.executeQuery("select COLUMNNAME from sys.syscolumns where {fn length(COLUMNNAME)} > 30 order by columnname");
		JDBC.assertSingleValueResultSet(rs, "C234567890123456789012345678901234567890");
		rs.close();
		
		s.execute("drop table t234567890123456789012345678901234567890");
		s.close();
	}
	
	/**
	 * Test that named constraints and unnamed constraints are recorded in the system tables properly.
	 * 
	 * @throws SQLException
	 */
	public void testPrimaryAndUniqueKeysInSystemCatalogs() throws SQLException {
		Statement s = createStatement();
		String getNamedConstraintsQuery = "select c.constraintname, c.type from sys.sysconstraints c, sys.systables t "
            + "where c.tableid = t.tableid and not t.tablename like 'UNNAMED%' order by c.constraintname";
		
		s.execute("create table primkey1 (c1 int not null constraint prim1 primary key)");
		String [][] expected = new String[][] {{"PRIM1", "P"}};
		ResultSet rs = s.executeQuery(getNamedConstraintsQuery);
		JDBC.assertFullResultSet(rs, expected, true);
		rs.close();
		
		s.execute("create table unnamed_primkey2 (c1 int not null primary key)");
		rs = s.executeQuery("select c.constraintname, c.type from sys.sysconstraints c, sys.systables t where c.tableid = t.tableid and t.tablename = 'UNNAMED_PRIMKEY2' order by c.constraintname");
		assertTrue(rs.next());
		assertEquals("P", rs.getString(2));
		assertFalse(rs.next());
		rs.close();
		rs = s.executeQuery(getNamedConstraintsQuery);
		JDBC.assertFullResultSet(rs, expected);
		rs.close();
		
		s.execute("create table primkey3 (c1 int not null, c2 int not null, constraint prim3 primary key(c2, c1))");
		expected = new String[][] {{"PRIM1", "P"}, {"PRIM3", "P"}};
		rs = s.executeQuery(getNamedConstraintsQuery);
		JDBC.assertFullResultSet(rs, expected);
		rs.close();

		s.execute("create table uniquekey1 (c1 int not null constraint uniq1 unique)");
		expected = new String[][] {{"PRIM1", "P"}, {"PRIM3", "P"}, {"UNIQ1", "U"}};
		rs = s.executeQuery(getNamedConstraintsQuery);
		JDBC.assertFullResultSet(rs, expected);
		rs.close();

		s.execute("create table unnamed_uniquekey2 (c1 int not null unique)");
		rs = s.executeQuery("select c.constraintname, c.type from sys.sysconstraints c, sys.systables t where c.tableid = t.tableid and t.tablename = 'UNNAMED_UNIQUEKEY2' order by c.constraintname");
		assertTrue(rs.next());
		assertEquals("U", rs.getString(2));
		assertFalse(rs.next());
		rs.close();
     	rs = s.executeQuery(getNamedConstraintsQuery);
		JDBC.assertFullResultSet(rs, expected);
		rs.close();
		
		s.execute("create table uniquekey3 (c1 int not null, c2 int not null, constraint uniq3 unique(c2, c1))");
		expected = new String[][] {{"PRIM1", "P"}, {"PRIM3", "P"}, {"UNIQ1", "U"}, {"UNIQ3", "U"}};
		rs = s.executeQuery(getNamedConstraintsQuery);
		JDBC.assertFullResultSet(rs, expected);
		rs.close();

	    s.execute("drop table primkey1");
	    s.execute("drop table unnamed_primkey2");
	    s.execute("drop table primkey3");
	    s.execute("drop table uniquekey1");
	    s.execute("drop table unnamed_uniquekey2");
	    s.execute("drop table uniquekey3");
	    
	    s.close();
	}
	
	/**
	 * Test that view creation is recorded in the system tables.
	 * 
	 * @throws SQLException
	 */
	public void testViewsOfSystemCatalogs() throws SQLException {
		Statement s = createStatement();
		s.execute("create table t (i int, s smallint)");
		s.execute("create table uniquekey3 (c1 int not null, c2 int not null, constraint uniq3 unique(c2, c1))");
		s.execute("create view dummyview as select * from t, uniquekey3");
		
		ResultSet rs = s.executeQuery("select tablename from sys.systables t, sys.sysviews v where t.tableid = v.tableid order by tablename");
		JDBC.assertSingleValueResultSet(rs, "DUMMYVIEW");
        rs.close();
		
		s.execute("drop view dummyview");
		s.execute("drop table t");
		s.execute("drop table uniquekey3");
		s.close();
	}

	/**
	 * This test creates a table with all supported datatypes aqnd ensures 
	 * that bound embedded and network server return the identical datatypes
	 * for those datatypes. DERBY-5407
	 * @throws SQLException
	 */
	public void testColumnDatatypesOfAllDataTypesInSystemCatalogs() throws SQLException {
		int totalNumOfColumnDatatypes = 21;
		Statement s = createStatement();
		s.execute("create table allTypesTable (" +
			"    a01 bigint," +
			"    a02 blob,\n" +
			"    a03 char( 1 ),\n" +
			"    a04 char( 1 ) for bit data ,\n" +
			"    a05 clob,\n" +
			"    a06 date,\n" +
			"    a07 decimal,\n" +
			"    a08 double,\n" +
			"    a09 float,\n" +
			"    a10 int,\n" +
			"    a11 long varchar,\n" +
			"    a12 long varchar for bit data,\n" +
			"    a13 numeric,\n" +
			"    a14 real,\n" +
			"    a15 smallint,\n" +
			"    a16 time,\n" +
			"    a17 timestamp,\n" +
			"    a18 varchar(10),\n" +
			"    a19 varchar(10) for bit data,\n" +
			"    a20 xml,\n" +
			"    a21 boolean\n" +
        	")");
		ResultSet rs = s.executeQuery("select columndatatype "+
			"from sys.systables, sys.syscolumns "+
			"where tablename='ALLTYPESTABLE' "+
			"and tableid=referenceid "+
			"order by columnname");
		for (int i=1; i<=totalNumOfColumnDatatypes; i++)
		{
			rs.next();
			switch(i)
			{
			case 1 :
				assertTrue(rs.getString(1).startsWith("BIGINT"));
				break;
			case 2 :
				assertTrue(rs.getString(1).startsWith("BLOB(2147483647)"));
				break;
			case 3 :
				assertTrue(rs.getString(1).startsWith("CHAR(1)"));
				break;
			case 4 :
				assertTrue(rs.getString(1).startsWith("CHAR (1) FOR BIT DATA"));
				break;
			case 5 :
				assertTrue(rs.getString(1).startsWith("CLOB(2147483647)"));
				break;
			case 6 :
				assertTrue(rs.getString(1).startsWith("DATE"));
				break;
			case 7 :
				assertTrue(rs.getString(1).startsWith("DECIMAL(5,0)"));
				break;
			case 8 :
				assertTrue(rs.getString(1).startsWith("DOUBLE"));
				break;
			case 9 :
				assertTrue(rs.getString(1).startsWith("DOUBLE"));
				break;
			case 10 :
				assertTrue(rs.getString(1).startsWith("INTEGER"));
				break;
			case 11 :
				assertTrue(rs.getString(1).startsWith("LONG VARCHAR"));
				break;
			case 12 :
				assertTrue(rs.getString(1).startsWith("LONG VARCHAR FOR BIT DATA"));
				break;
			case 13 :
				assertTrue(rs.getString(1).startsWith("NUMERIC(5,0)"));
				break;
			case 14 :
				assertTrue(rs.getString(1).startsWith("REAL"));
				break;
			case 15 :
				assertTrue(rs.getString(1).startsWith("SMALLINT"));
				break;
			case 16 :
				assertTrue(rs.getString(1).startsWith("TIME"));
				break;
			case 17 :
				assertTrue(rs.getString(1).startsWith("TIMESTAMP"));
				break;
			case 18 :
				assertTrue(rs.getString(1).startsWith("VARCHAR(10)"));
				break;
			case 19 :
				assertTrue(rs.getString(1).startsWith("VARCHAR (10) FOR BIT DATA"));
				break;
			case 20 :
				assertTrue(rs.getString(1).startsWith("XML"));
				break;
			case 21 :
				assertTrue(rs.getString(1).startsWith("BOOLEAN"));
				break;
			}
		}
		rs.close();
		s.execute("drop table ALLTYPESTABLE");
	}
	
	/**
	 * Check that column datatypes are reported correctly, both in
	 * embedded and client/server modes
	 * 
	 * @throws SQLException
	 */
	public void testColumnDatatypesInSystemCatalogs() throws SQLException {
		Statement s = createStatement();
		
		s.execute("create table decimal_tab (dcol decimal(5,2), ncol numeric(5,2) default 1.0)");
		ResultSet rs = s.executeQuery("select columnname, columndatatype from sys.syscolumns where columnname IN ('DCOL', 'NCOL') order by columnname");
		//DCOL
		assertTrue(rs.next());
		assertTrue(rs.getString(2).startsWith("DECIMAL(5,2)"));
		//NCOL
		assertTrue(rs.next());		
		assertTrue(rs.getString(2).startsWith("NUMERIC(5,2)"));
		assertFalse(rs.next());
		rs.close();
		
		s.execute("create index decimal_tab_idx on decimal_tab(dcol)");
		rs = s.executeQuery("select conglomeratename, descriptor from sys.sysconglomerates where conglomeratename = 'DECIMAL_TAB_IDX' order by conglomeratename");
		assertTrue(rs.next());
		assertTrue(rs.getString(2).startsWith("BTREE (1)"));
		assertFalse(rs.next());
		rs.close();
		
		s.execute("create trigger t1 after update on decimal_tab for each row values 1");
		rs = s.executeQuery("select triggername, referencedcolumns from sys.systriggers order by triggername");
		assertTrue(rs.next());
		assertNull(rs.getString(2));
		assertFalse(rs.next());
		rs.close();
		
		s.execute("drop trigger t1");
		s.execute("drop table decimal_tab");
		s.close();
	}
	
	/**
	 * Test for fix of Derby-318, confirm that it is possible to select
	 * COLUMNDEFAULT from SYSCOLUMNS after a column that is generated by
	 * default has been added.
	 * 
	 * @throws SQLException
	 */
	public void testAutoincrementColumnUpdated() throws SQLException{
		Statement s = createStatement();
		s.executeUpdate("create table defaultAutoinc(autoinccol int generated by default as identity)");
		ResultSet rs = s.executeQuery("select COLUMNDEFAULT from SYS.SYSCOLUMNS where COLUMNNAME = 'AUTOINCCOL'");
		assertTrue(rs.next());
		// Before Derby-318, this next call would have failed with an NPE
	    Object o = rs.getObject(1);
	    if (! (o instanceof java.io.Serializable)) {
	    	fail("SystemCatalogTest: invalid Object type for SYSCOLUMNS.COLUMNDEFAULT");
	    }
	    assertFalse(rs.next());
		rs.close();
		
		s.executeUpdate("drop table defaultAutoinc");
		s.close();
		
	}
	
	/**
	 * Run SYSCS_UTIL.SYSCS_CHECK_TABLE on each system table.
	 * 
	 * @throws SQLException
	 */
	public void testCheckConsistencyOfSystemCatalogs() throws SQLException {
		Statement s = createStatement();
		ResultSet rs = s.executeQuery("select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename)from sys.systables where tabletype = 'S' and tablename != 'SYSDUMMY1' order by tablename");
		
		boolean nonEmptyResultSet = false;
		while(rs.next()) {
			nonEmptyResultSet = true;
			assertEquals(rs.getInt(2), 1);
		}
		
		assertTrue(nonEmptyResultSet);
		rs.close();
		s.close();
	}
}


