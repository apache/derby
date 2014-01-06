/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.DataDictionary

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

package org.apache.derby.iapi.sql.dictionary;

import java.sql.Types;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.daemon.IndexStatisticsDaemon;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.RowLocation;

/**
 * The DataDictionary interface is used with the data dictionary to get
 * descriptors for binding and compilation. Some descriptors (such as table
 * and column descriptors) are added to and deleted from the data dictionary
 * by other modules (like the object store). Other descriptors are added and
 * deleted by the language module itself (e.g. the language module adds and
 * deletes views, because views are too high-level for modules like the object
 * store to know about).
 *
 * @version 0.1
 */

public interface DataDictionary

{
	String MODULE = "org.apache.derby.iapi.sql.dictionary.DataDictionary";

	/** Special version indicating the database must be upgraded to or created at the current engine level 
	 * 
	 * DatabaseMetaData will use this to determine if the data dictionary 
	 * is at the latest System Catalog version number. A data dictionary version
	 * will not be at latest System Catalog version when the database is getting
	 * booted in soft upgrade mode. In soft upgrade mode, engine should goto 
	 * metadata.properties to get the sql for the metadata calls rather
	 * than going to the system tables (and using stored versions of these queries). 
	 * This is required because if the metadata sql has changed between the 
	 * releases, we want to use the latest metadata sql rather than what is 
	 * stored in the system catalogs. Had to introduce this behavior for
	 * EmbeddedDatabaseMetaData in 10.2 release where optimizer overrides 
	 * syntax was changed. If 10.2 engine in soft upgrade mode for a pre-10.2 
	 * database went to system tables for stored metadata queires, the metadata 
	 * calls would fail because 10.2 release doesn't recognize the pre-10.2 
	 * optimizer overrides syntax. To get around this, the 10.2 engine in 
	 * soft upgrade mode should get the sql from metata.properties which has 
	 * been changed to 10.2 syntax for optimizer overrides. To make this 
	 * approach more generic for all soft upgrades, from 10.2 release onwards, 
	 * DatabaseMetaData calls will always look at metadata.properties so it 
	 * will get the compatible syntax for that release.
	 */
	public static final int DD_VERSION_CURRENT			= -1;
	/** Cloudscape 5.0 System Catalog version */
	public static final int DD_VERSION_CS_5_0			= 80;
	/** Cloudscape 5.1 (Arwen) System Catalog version */
	public static final int DD_VERSION_CS_5_1			= 90;
	/** Cloudscape 5.2 (Frodo) System Catalog version */
	public static final int DD_VERSION_CS_5_2			= 100;
	/** Cloudscape 8.1 (Pre-Gandalf) System Catalog version */
	public static final int DD_VERSION_CS_8_1			= 110;
	/** Cloudscape/Derby 10.0 (Gandalf) System Catalog version */
	public static final int DD_VERSION_CS_10_0			= 120;

	/** Derby 10.1 System Catalog version */
	public static final int DD_VERSION_DERBY_10_1		= 130;

	/** Derby 10.2 System Catalog version */
	public static final int DD_VERSION_DERBY_10_2		= 140;

	/** Derby 10.3 System Catalog version */
	public static final int DD_VERSION_DERBY_10_3		= 150;
	
	/** Derby 10.4 System Catalog version */
	public static final int DD_VERSION_DERBY_10_4		= 160;

	/** Derby 10.5 System Catalog version */
	public static final int DD_VERSION_DERBY_10_5		= 170;

	/** Derby 10.6 System Catalog version */
	public static final int DD_VERSION_DERBY_10_6		= 180;

	/** Derby 10.7 System Catalog version */
	public static final int DD_VERSION_DERBY_10_7		= 190;

	/** Derby 10.8 System Catalog version */
	public static final int DD_VERSION_DERBY_10_8		= 200;

	/** Derby 10.9 System Catalog version */
	public static final int DD_VERSION_DERBY_10_9		= 210;

	/** Derby 10.10 System Catalog version */
	public static final int DD_VERSION_DERBY_10_10		= 220;

	/** Derby 10.10 System Catalog version */
	public static final int DD_VERSION_DERBY_10_11		= 230;

	// general info
	public	static	final	String	DATABASE_ID = "derby.databaseID";

	// version ids
	/**
	 * DataDictionaryVersion property indicates the updgrade level of the system catalogs.
	 * Stored as a database property. Set to an instance of DD_Version with
	 * the major number one of the DataDictionary.DD_* values.
	 */
	public	static	final	String	CORE_DATA_DICTIONARY_VERSION = "DataDictionaryVersion";
	/**
	 * CreateDataDictionaryVersion property indicates the level of the system catalogs,
	 * at the time of database creation.
	 * Stored as a database property. Set to an instance of DD_Version.
	 */
	public	static	final	String	CREATE_DATA_DICTIONARY_VERSION = "CreateDataDictionaryVersion";
	/**
	 * derby.softDataDictionaryVersion property indicates the soft upgrade level of the system catalogs.
	 * Soft upgrade will sometime make minor changes to the system catalogs that can be safely consumed by
	 * earlier versions, such as correcting values.
	 * Stored as a database property. Set to an instance of DD_Version.
	 */
	public  static  final   String  SOFT_DATA_DICTIONARY_VERSION = "derby.softDataDictionaryVersion";
    public  static  final   String  PROPERTY_CONGLOMERATE_VERSION = "PropertyConglomerateVersion";
    
    /**
     * An immutable runtime type that describes the type VARCHAR(128) NOT NULL
     * with collation type UCS_BASIC and derivation IMPLICIT.
     */
    public static final DataTypeDescriptor TYPE_SYSTEM_IDENTIFIER =
        DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                Types.VARCHAR, false, 128);
      
    /**
     * An immutable catalog type that describes the type VARCHAR(128) NOT NULL
     * with collation type UCS_BASIC.
     */
    public static final TypeDescriptor CATALOG_TYPE_SYSTEM_IDENTIFIER =
        TYPE_SYSTEM_IDENTIFIER.getCatalogType();

	/*
	** CORE TABLES
	*/
	/* NOTE - SYSCONGLOMERATES must be first, since that table must exist before
	 * any other conglomerates can be created/added to the system.
	 */
	public static final int SYSCONGLOMERATES_CATALOG_NUM = 0;
	public static final int SYSTABLES_CATALOG_NUM = 1;
	public static final int SYSCOLUMNS_CATALOG_NUM = 2;
	public static final int SYSSCHEMAS_CATALOG_NUM = 3;
	
	/**
	 * Catalog numbers for non core system catalogs.
	 */
	public static final int SYSCONSTRAINTS_CATALOG_NUM = 4;
	public static final int SYSKEYS_CATALOG_NUM = 5;
	public static final int SYSDEPENDS_CATALOG_NUM = 6;
	public static final int SYSALIASES_CATALOG_NUM = 7;
	public static final int SYSVIEWS_CATALOG_NUM = 8;
	public static final int SYSCHECKS_CATALOG_NUM = 9;
	public static final int SYSFOREIGNKEYS_CATALOG_NUM = 10;
	public static final int SYSSTATEMENTS_CATALOG_NUM = 11;
	public static final int SYSFILES_CATALOG_NUM = 12;
	public static final int SYSTRIGGERS_CATALOG_NUM = 13;
	public static final int SYSSTATISTICS_CATALOG_NUM = 14;    
	public static final int SYSDUMMY1_CATALOG_NUM = 15;
	public static final int SYSTABLEPERMS_CATALOG_NUM = 16;
	public static final int SYSCOLPERMS_CATALOG_NUM = 17;
	public static final int SYSROUTINEPERMS_CATALOG_NUM = 18;
    public static final int SYSROLES_CATALOG_NUM = 19;
    public static final int SYSSEQUENCES_CATALOG_NUM = 20;
    public static final int SYSPERMS_CATALOG_NUM = 21;
    public static final int SYSUSERS_CATALOG_NUM = 22;

    /* static finals for constraints
	 * (Here because they are needed by parser, compilation and execution.)
	 */
	public static final int NOTNULL_CONSTRAINT = 1;
	public static final int PRIMARYKEY_CONSTRAINT = 2;
	public static final int UNIQUE_CONSTRAINT = 3;
	public static final int CHECK_CONSTRAINT = 4;
	public static final int DROP_CONSTRAINT = 5;
	public static final int FOREIGNKEY_CONSTRAINT = 6;
    public static final int MODIFY_CONSTRAINT = 7;

	/** Modes returned from startReading() */
	public static final int COMPILE_ONLY_MODE = 0;
	public static final int DDL_MODE = 1;

	/**
	 * Clear the DataDictionary caches, including the sequence caches if requested..
	 *
	 * @exception StandardException Standard Derby error policy
	 */
	public void clearCaches( boolean clearSequenceCaches ) throws StandardException;

	/**
	 * Clear all of the DataDictionary caches.
	 *
	 * @exception StandardException Standard Derby error policy
	 */
	public void clearCaches() throws StandardException;

	/**
	 * Clear all of the sequence number generators.
	 *
	 * @exception StandardException Standard Derby error policy
	 */
	public void clearSequenceCaches() throws StandardException;

	/**
	 * Inform this DataDictionary that we are about to start reading it.  This
	 * means using the various get methods in the DataDictionary.
	 * Generally, this is done during query compilation.
	 *
	 * @param lcc	The LanguageConnectionContext to use.
	 *
	 * @return	The mode that the reader will use, to be passed to doneReading()
	 *			Either COMPILE_ONLY_MODE or DDL_MODE.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public int startReading(LanguageConnectionContext lcc) throws StandardException;

	/**
	 * Inform this DataDictionary that we have finished reading it.  This
	 * typically happens at the end of compilation.
	 *
	 * @param mode	The mode that was returned by startReading().
	 * @param lcc	The LanguageConnectionContext to use.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void doneReading(int mode,
			LanguageConnectionContext lcc) 
		throws StandardException;

	/**
	 * Inform this DataDictionary that we are about to start writing to it.
	 * This means using the various add and drop methods in the DataDictionary.
	 * Generally, this is done during execution of DDL.
	 *
	 * @param lcc	The LanguageConnectionContext to use.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void startWriting(LanguageConnectionContext lcc) 
		throws StandardException;

	/**
	 * Inform this DataDictionary that the transaction in which writes have
	 * been done (or may have been done) has been committed or rolled back.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void transactionFinished() throws StandardException;

	/**
	 * Get the ExecutionFactory associated with this database.
	 *
	 * @return	The ExecutionFactory
	 */
	public ExecutionFactory	getExecutionFactory();

	/**
	 * Get the DataValueFactory associated with this database.
	 *
	 * @return	The ExecutionFactory
	 */
	public DataValueFactory	getDataValueFactory();

	/**
	 * Get a DataDescriptorGenerator, through which we can create
	 * objects to be stored in the DataDictionary.
	 *
	 * @return	A DataDescriptorGenerator
	 *
	 */
	public DataDescriptorGenerator	getDataDescriptorGenerator();

	/**
	 * Get authorizationID of Database Owner
	 *
	 * @return	authorizationID
	 */
	public String getAuthorizationDatabaseOwner();

	/**
	 * Get authorization model in force, SqlStandard or legacy mode
	 *
	 * @return	Whether sqlAuthorization is being used
	 */
	public boolean usesSqlAuthorization();
	
	/**
	 * Return the collation type for SYSTEM schemas. In Derby 10.3, this will 
	 * always be UCS_BASIC 
	 * 
	 * @return the collation type for SYSTEM schemas
	 */
	public int getCollationTypeOfSystemSchemas();

	/**
	 * Return the collation type for user schemas. In Derby 10.3, this is either 
	 * UCS_BASIC or TERRITORY_BASED. The exact value is decided by what has 
	 * user asked for through JDBC url optional attribute COLLATION. If that
	 * atrribute is set to UCS_BASIC, the collation type for user schemas
	 * will be UCS_BASIC. If that attribute is set to TERRITORY_BASED, the 
	 * collation type for user schemas will be TERRITORY_BASED. If the user
	 * has not provided COLLATION attribute value in the JDBC url at database
	 * create time, then collation type of user schemas will default to 
	 * UCS_BASIC. Pre-10.3 databases after upgrade to Derby 10.3 will also
	 * use UCS_BASIC for collation type of user schemas. 
	 * 
	 * @return the collation type for user schemas
	 */
	public int getCollationTypeOfUserSchemas();
	
	/**
	 * Get the descriptor for the named schema.
	   Schema descriptors include authorization ids and schema ids.
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.  Will check default schema for a match
	 * before scanning a system table.
	 * 
	 * @param schemaName	The name of the schema we're interested in. Must not be null.
	 * @param tc			TransactionController
	 *
	 * @param raiseError    whether an exception should be thrown if the schema does not exist.
	 *
	 * @return	The descriptor for the schema. Can be null (not found) if raiseError is false.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public SchemaDescriptor	getSchemaDescriptor(String schemaName,
												TransactionController tc,
												boolean raiseError)
						throws StandardException;

	/**
	 * Get the SchemaDescriptor for the given schema identifier. 
	 *
	 * @param schemaId	The id of the schema we're interested in.
	 *
	 * @param tc		The transaction controller to us when scanning
	 *					SYSSCHEMAS
	 *
	 * @return	The descriptor for the schema, null if no such schema exists.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getSchemaDescriptor(UUID schemaId, TransactionController tc)
						throws StandardException;

	/**
	 * Get the SchemaDescriptor for the given schema identifier.
	 *
	 * @param schemaId	The id of the schema we're interested in.
	 *
	 * @param isolationLevel use this explicit isolation level
	 * @param tc		The transaction controller to us when scanning
	 *					SYSSCHEMAS
	 *
	 * @return	The descriptor for the schema, null if no such schema exists.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getSchemaDescriptor(UUID schemaId,
												int isolationLevel,
												TransactionController tc)
						throws StandardException;

	/**
	 * Return true of there exists a schema whose authorizationId
	 * equals authid, i.e.  SYSSCHEMAS contains a row whose column
	 * AUTHORIZATIONID equals authid.
	 *
	 * @param authid authorizationId
	 * @param tc TransactionController
	 * @return true iff there is a matching schema
	 * @exception StandardException
	 */
	public boolean existsSchemaOwnedBy(String authid,
									   TransactionController tc)
			throws StandardException;

	/**
	 * Get the default password hasher for this database level. Returns null
     * if the system is at rev level 10.5 or earlier.
	 *
	 * @param props   The persistent properties used to configure password hashing.
	 */
    public  PasswordHasher  makePasswordHasher( Dictionary<?,?> props )
        throws StandardException;
    
	/**
	 * Get the descriptor for the system schema. Schema descriptors include 
     * authorization ids and schema ids.
     *
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getSystemSchemaDescriptor( )
						throws StandardException;

	/**
	 * Get the descriptor for the SYSIBM schema. Schema descriptors include 
     * authorization ids and schema ids.
     *
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getSysIBMSchemaDescriptor( )
						throws StandardException;

	/**
	 * Get the descriptor for the declared global temporary table schema which is always named "SESSION".
	 *
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getDeclaredGlobalTemporaryTablesSchemaDescriptor()
        throws StandardException;

    /**
     * Determine whether a string is the name of the system schema.
     *
     * @param name
     * @return	true or false
	 *
	 * @exception StandardException		Thrown on failure
     */
    public boolean isSystemSchemaName( String name)
        throws StandardException;
    

	/**
	 * Drop a role grant
	 *
	 * @param roleName	The name of the role to drop
     * @param grantee   The grantee
     * @param grantor   The grantor
	 * @param tc        Transaction Controller
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropRoleGrant(String roleName,
							  String grantee,
							  String grantor,
							  TransactionController tc)
			throws StandardException;


	/**
	 * Drop all role grants corresponding to a grant of (any)
	 * role to a named authentication identifier
	 *
     * @param grantee   The grantee
	 * @param tc        Transaction Controller
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropRoleGrantsByGrantee(String grantee,
										TransactionController tc)
			throws StandardException;


	/**
	 * Drop all role grants corresponding to a grant of the
	 * named role to any authentication identifier
	 *
     * @param roleName  The role name granted
	 * @param tc        Transaction Controller
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropRoleGrantsByName(String roleName,
									 TransactionController tc)
			throws StandardException;


	/**
	 * This method creates a new iterator over the closure of role
	 * grants starting or ending with a given role.
	 *
	 * This method will cause reading of dictionary, so should be
	 * called inside a transaction, after a {@code dd.startReading()}
	 * or {@code dd.startWriting()} call.
	 *
	 * @param tc transaction controller
	 * @param role name of starting point for closure
	 * @param inverse If {@code true}, compute closure on inverse of
	 *        relation GRANT role-a TO role-b that is, we look at
	 *        closure of all roles granted <bold>to</bold> {@code role}. If
	 *        {@code false}, we look at closure of all roles that have
	 *        been granted {@code role}.
	 * @throws StandardException
	 */
	public RoleClosureIterator createRoleClosureIterator
		(TransactionController tc,
		 String role,
		 boolean inverse
		) throws StandardException;


	/**
	 * Drop all permission descriptors corresponding to a grant to
	 * the named authentication identifier
	 *
     * @param authid    The authentication identifier
	 * @param tc        Transaction Controller
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropAllPermsByGrantee(String authid,
									  TransactionController tc)
			throws StandardException;


	/**
	 * Drop the descriptor for a schema, given the schema's name
	 *
	 * @param schemaName	The name of the schema to drop
	 * @param tc			Transaction Controller	
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropSchemaDescriptor(String schemaName,
							TransactionController tc)
						throws StandardException;

	/**
	 * Indicate whether there is anything in the 
	 * particular schema.  Checks for tables in the
	 * the schema, on the assumption that there cannot
	 * be any other objects in a schema w/o a table.
	 *
	 * @param sd schema descriptor
	 *
	 * @return true/false
	 *
	 * @exception StandardException on error
	 */
	public boolean isSchemaEmpty(SchemaDescriptor sd)
		throws StandardException;

	/**
	 * Get the descriptor for the named table within the given schema.
	 * If the schema parameter is NULL, it looks for the table in the
	 * current (default) schema. Table descriptors include object ids,
	 * object types (table, view, etc.)
	 *
	 * @param tableName	The name of the table to get the descriptor for
	 * @param schema	The descriptor for the schema the table lives in.
	 *			If null, use the current (default) schema.
	 * @param tc Transaction context.
	 * @return	The descriptor for the table, null if table does not
	 *		existe.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TableDescriptor		getTableDescriptor(String tableName,
					SchemaDescriptor schema, TransactionController tc)
						throws StandardException;

	/**
	 * Get the descriptor for the table with the given UUID.
	 *
	 * NOTE: I'm assuming that the object store will define an UUID for
	 * persistent objects. I'm also assuming that UUIDs are unique across
	 * schemas, and that the object store will be able to do efficient
	 * lookups across schemas (i.e. that no schema descriptor parameter
	 * is needed).
	 *
	 * @param tableID	The UUID of the table to get the descriptor for
	 *
	 * @return	The descriptor for the table, null if the table does
	 *		not exist.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TableDescriptor		getTableDescriptor(UUID tableID)
						throws StandardException;

	/**
	 * Drop the table descriptor.
	 *
	 * @param td	The table descriptor to drop
	 * @param schema		A descriptor for the schema the table
	 *						is a part of.  If this parameter is
	 *						NULL, then the table is part of the
	 *						current (default) schema
	 * @param tc			TransactionController for the transaction
	 * @exception StandardException		Thrown on error
	 */
	public void	dropTableDescriptor(TableDescriptor td, SchemaDescriptor schema,
									TransactionController tc)
		throws StandardException;

	/**
	 * Update the lockGranularity for the specified table.
	 *
	 * @param td				The TableDescriptor for the table
	 * @param schema			The SchemaDescriptor for the table
	 * @param lockGranularity	The new lockGranularity
	 * @param tc				The TransactionController to use.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void updateLockGranularity(TableDescriptor td, SchemaDescriptor schema,
									  char lockGranularity, TransactionController tc)
		throws StandardException;

	/**
	 * Drop all table descriptors for a schema.
	 *
	 * @param schema	A descriptor for the schema to drop the tables
	 *			from.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	/*
	public void dropAllTableDescriptors(SchemaDescriptor schema)
						throws StandardException;
	*/

	/**
	 * Get a ColumnDescriptor given its Default ID.
	 *
	 * @param uuid	The UUID of the default
	 *
	 * @return The ColumnDescriptor for the column.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ColumnDescriptor getColumnDescriptorByDefaultId(UUID uuid)
				throws StandardException;

	/**
	 * Given a column name and a table ID, drops the column descriptor
	 * from the table.
	 *
	 * @param tableID	The UUID of the table to drop the column from
	 * @param columnName	The name of the column to drop
	 * @param tc		TransactionController for the transaction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropColumnDescriptor(UUID tableID,
				String columnName, TransactionController tc)
						throws StandardException;

	/**
	 * Drops all column descriptors from the given table.  Useful for
	 * DROP TABLE.
	 *
	 * @param tableID	The UUID of the table from which to drop
	 *			all the column descriptors
	 * @param tc		TransactionController for the transaction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropAllColumnDescriptors(UUID tableID, TransactionController tc)
						throws StandardException;

	/**
	 * Drops all table and column permission descriptors for the given table.
	 *
	 * @param tableID	The UUID of the table for which to drop
	 *			all the table and column permission descriptors
	 * @param tc		TransactionController for the transaction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropAllTableAndColPermDescriptors(UUID tableID, TransactionController tc)
						throws StandardException;


	/**
	 * Need to update SYSCOLPERMS for a given table because a new column has 
	 * been added to that table. SYSCOLPERMS has a column called "COLUMNS"
	 * which is a bit map for all the columns in a given user table. Since
	 * ALTER TABLE .. ADD COLUMN .. has added one more column, we need to
	 * expand "COLUMNS" for that new column
	 *
	 * Currently, this code gets called during execution phase of
	 * ALTER TABLE .. ADD COLUMN .. 
	 *
	 * @param tableID	The UUID of the table to which a column has been added
	 * @param tc		TransactionController for the transaction
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	updateSYSCOLPERMSforAddColumnToUserTable(UUID tableID, TransactionController tc)
	throws StandardException;
	
	/**
	 * Update SYSCOLPERMS to reflect the dropping of a column from a table.
	 *
	 * This method rewrites SYSCOLPERMS rows to update the COLUMNS bitmap
	 * to reflect the removal of a column from a table.
	 *
	 * Currently, this code gets called during execution phase of
	 * ALTER TABLE .. DROP COLUMN .. 
	 *
	 * @param tableID	The UUID of the table whose column has been dropped
	 * @param tc		TransactionController for the transaction
	 * @param columnDescriptor   Info about the dropped column
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	updateSYSCOLPERMSforDropColumn(UUID tableID,
			TransactionController tc, ColumnDescriptor columnDescriptor)
	throws StandardException;
	
	
	/**
	 * Drops all routine permission descriptors for the given routine.
	 *
	 * @param routineID	The UUID of the routine for which to drop
	 *			all the permission descriptors
	 * @param tc		TransactionController for the transaction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropAllRoutinePermDescriptors(UUID routineID, TransactionController tc)
						throws StandardException;

	/**
	 * Gets the viewDescriptor for the view with the given UUID.
	 *
	 * @param uuid	The UUID for the view
	 *
	 * @return  A descriptor for the view
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ViewDescriptor	getViewDescriptor(UUID uuid)
		throws StandardException;

	/**
	 * Gets the viewDescriptor for the view given its TableDescriptor.
	 *
	 * @param td	The TableDescriptor for the view.
	 *
	 * @return	A descriptor for the view
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ViewDescriptor	getViewDescriptor(TableDescriptor td)
 						throws StandardException;

	/**
	 * Drops the view descriptor from the data dictionary.
	 *
	 * @param viewDescriptor	A descriptor for the view to be dropped
	 * @param tc				TransactionController to use
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropViewDescriptor(ViewDescriptor viewDescriptor,
								   TransactionController tc)
						throws StandardException;

	/**
	 * Get a ConstraintDescriptor given its UUID.
	 *
	 * @param uuid	The UUID
	 *
	 * @return The ConstraintDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptor getConstraintDescriptor(UUID uuid)
				throws StandardException;

	/**
	 * Get a ConstraintDescriptor given its name and schema ID.
	 *
	 * @param constraintName	Constraint name.
	 * @param schemaID			The schema UUID
	 *
	 * @return The ConstraintDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptor getConstraintDescriptor
	(
		String	constraintName,
		UUID	schemaID
    )
		throws StandardException;

	/**
	 * Load up the constraint descriptor list for this table
     * descriptor (or all) and return it.  If the descriptor list
     * is already loaded up, it is returned without further
	 * ado.
	 *
     * @param td The table descriptor.
     * @return   The ConstraintDescriptorList for the table. If null, return
     *           a list of all the constraint descriptors in all schemas.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptorList getConstraintDescriptors(TableDescriptor td)
		throws StandardException;

	/**
	 * Convert a constraint descriptor list into a list
	 * of active constraints, that is, constraints which
	 * must be enforced. For the Core product, these
	 * are just the constraints on the original list.
	 * However, during REFRESH we may have deferred some
	 * constraints until statement end. This method returns
	 * the corresponding list of constraints which AREN'T
	 * deferred.
	 *
	 * @param cdl	The constraint descriptor list to wrap with
	 *				an Active constraint descriptor list.
	 *
	 * @return The corresponding Active ConstraintDescriptorList
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptorList getActiveConstraintDescriptors(ConstraintDescriptorList cdl)
		throws StandardException;

	/**
	 * Reports whether an individual constraint must be
	 * enforced. For the Core product, this routine always
	 * returns true.
	 *
	 * However, during REFRESH we may have deferred some
	 * constraints until statement end. This method returns
	 * false if the constraint deferred
	 *
	 * @param constraint	the constraint to check
	 *
	 *
	 * @return The corresponding Active ConstraintDescriptorList
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public boolean activeConstraint( ConstraintDescriptor constraint )
		throws StandardException;

	/** 
	 * Get the constraint descriptor given a table and the UUID String
	 * of the backing index.
	 *
	 * @param td			The table descriptor.
	 * @param uuid			The UUID  for the backing index.
	 *
	 * @return The ConstraintDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptor getConstraintDescriptor(TableDescriptor td, 
														UUID uuid)
				throws StandardException;


	/**
	 * Get the constraint descriptor given a table and the UUID String
	 * of the constraint
	 *
	 * @param td			The table descriptor.
	 * @param uuid			The UUID for the constraint
	 *
	 * @return The ConstraintDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptor getConstraintDescriptorById
	(
		TableDescriptor	td,
		UUID			uuid
    )
		throws StandardException;

	/** 
	 * Get the constraint descriptor given a TableDescriptor and the constraint name.
	 *
	 * @param td				The table descriptor.
	 * @param sd				The schema descriptor for the constraint
	 * @param constraintName	The constraint name.
	 * @param forUpdate			Whether or not access is for update
	 *
	 * @return The ConstraintDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptor getConstraintDescriptorByName(TableDescriptor td, 
															  SchemaDescriptor sd,
															  String constraintName,
															  boolean forUpdate)
				throws StandardException;

	/**
	 * Return a table descriptor corresponding to the TABLEID
	 * field in SYSCONSTRAINTS where CONSTRAINTID matches
	 * the constraintId passed in.
	 *
	 * @param constraintId	The id of the constraint
	 *
	 * @return	the corresponding table descriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	public TableDescriptor getConstraintTableDescriptor(UUID constraintId)
			throws StandardException;

	/**
	 * Return a list of foreign keys constraints referencing
	 * this constraint.  Returns both enabled and disabled
	 * constraints.  
	 *
	 * @param constraintId	The id of the referenced constraint
	 *
	 * @return	list of constraints
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ConstraintDescriptorList getForeignKeys(UUID constraintId)
			throws StandardException;

	/**
	 * Adds the given ConstraintDescriptor to the data dictionary,
	 * associated with the given table and constraint type.
	 *
	 * @param descriptor	The descriptor to add
	 * @param tc			The transaction controller
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	addConstraintDescriptor(
			ConstraintDescriptor descriptor,
			TransactionController tc)
						throws StandardException;

	/**
	 * Drops the given ConstraintDescriptor from the data dictionary.
	 *
	 * NOTE: Caller is responsible for dropping any backing index
	 *
	 * @param descriptor	The descriptor to drop
	 * @param tc	The TransactionController.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropConstraintDescriptor(
			ConstraintDescriptor descriptor,
			TransactionController tc)
						throws StandardException;

	/**
	 * Drops all ConstraintDescriptors from the data dictionary
	 * that are associated with the given table.
	 *
	 * NOTE: Caller is responsible for dropping any backing index
	 *
	 * @param table	The table from which to drop all
	 *			constraint descriptors
	 * @param tc	The TransactionController.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropAllConstraintDescriptors(TableDescriptor table,
											 TransactionController tc)
						throws StandardException;

	/**
	 * Update the constraint descriptor in question.  Updates
	 * every row in the base conglomerate.  
	 *
	 * @param cd					The Constraintescriptor
	 * @param formerUUID			The UUID for this column in SYSCONSTRAINTS,
	 *								may differ from what is in cd if this
	 *								is the column that is being set.
	 * @param colsToSet 			Array of ints of columns to be modified,
	 *								1 based.  May be null (all cols).
	 * @param tc					The TransactionController to use
	 *
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateConstraintDescriptor(ConstraintDescriptor cd,
										UUID		formerUUID,
										int[]		colsToSet,
										TransactionController tc)
		throws StandardException;

	/**
	 * Get a SubKeyConstraintDescriptor from syskeys or sysforeignkeys for
	 * the specified constraint id.  For primary foreign and and unique
	 * key constraints.
	 *
	 * @param constraintId	The UUID for the constraint.
	 * @param type	The type of the constraint 
	 *		(e.g. DataDictionary.FOREIGNKEY_CONSTRAINT)
	 *
	 * @return SubKeyConstraintDescriptor	The Sub descriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SubKeyConstraintDescriptor getSubKeyConstraint(UUID constraintId, int type)
		throws StandardException;

	/**
	 * Get a SPSDescriptor given its UUID.
	 *
	 * @param uuid	The UUID
	 *
	 *
	 * @return The SPSDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SPSDescriptor getSPSDescriptor(UUID uuid)
				throws StandardException;

	/** 
	 * Get the stored prepared statement descriptor given 
	 * a sps name.
	 *
	 * @param name	The sps name.
	 * @param sd	The schema descriptor.
	 *
	 * @return The SPSDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SPSDescriptor getSPSDescriptor(String name, SchemaDescriptor sd)
				throws StandardException;

	/**
	 * Get every statement in this database.
	 * Return the SPSDescriptors in an list.
	 *
	 * @return the list of descriptors
	 *
	 * @exception StandardException		Thrown on failure
	 */
    public List<SPSDescriptor> getAllSPSDescriptors() throws StandardException;

	/**
	 * Get all the parameter descriptors for an SPS.
	 * Look up the params in SYSCOLUMNS and turn them
	 * into parameter descriptors.  
	 *
	 * @param spsd	sps descriptor
	 * @param defaults the parameter defaults.  If not null,
	 *					all the parameter defaults will be stuffed
	 *					in here.
	 *
	 * @return array of data type descriptors
	 *
	 * @exception StandardException		Thrown on error
	 */
    public DataTypeDescriptor[] getSPSParams(SPSDescriptor spsd, List<DataValueDescriptor> defaults)
            throws StandardException;

	/**
	 * Adds the given SPSDescriptor to the data dictionary,
	 * associated with the given table and constraint type.
	 *
	 * @param descriptor	The descriptor to add
	 * @param tc			The transaction controller
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	addSPSDescriptor
	(
		SPSDescriptor 			descriptor,
		TransactionController	tc
	) throws StandardException;

	/**
	 * Updates SYS.SYSSTATEMENTS with the info from the
	 * SPSD. 
	 *
	 * @param spsd	The descriptor to add
	 * @param tc			The transaction controller
	 * @param recompile		whether to recompile or invalidate
	 * @param updateSYSCOLUMNS indicate whether syscolumns needs to be updated
	 *							or not.
	 * @param firstCompilation  first time SPS is getting compiled.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	updateSPS(
			SPSDescriptor		spsd,
			TransactionController	tc,
			boolean                 recompile,
			boolean					updateSYSCOLUMNS,
			boolean                 firstCompilation)
						throws StandardException;

	/**
	 * Drops the given SPSDescriptor.
	 *
	 * @param descriptor	The descriptor to drop
	 * @param tc	The TransactionController.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropSPSDescriptor(SPSDescriptor descriptor,
			TransactionController tc)
						throws StandardException;

	/**
	 * Drops the given SPSDescriptor. 
	 *
	 * @param uuid	the statement uuid
	 * @param tc	The TransactionController.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropSPSDescriptor
	(
		UUID 					uuid,
		TransactionController	tc
	) throws StandardException;

	/**
	 * Invalidate all the stored plans in SYS.SYSSTATEMENTS for
	 *  the given language connection context.
	 * @exception StandardException		Thrown on error
	 */
	public void invalidateAllSPSPlans(LanguageConnectionContext lcc) throws StandardException;

	/**
	 * Invalidate all the stored plans in SYS.SYSSTATEMENTS. 
	 * @exception StandardException		Thrown on error
	 */
	public void invalidateAllSPSPlans() throws StandardException;
						
	/**
	 * Get a TriggerDescriptor given its UUID.
	 *
	 * @param uuid	The UUID
	 *
	 *
	 * @return The TriggerDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TriggerDescriptor getTriggerDescriptor(UUID uuid)
				throws StandardException;

	/** 
	 * Get the stored prepared statement descriptor given 
	 * a sps name.
	 *
	 * @param name	The sps name.
	 * @param sd	The schema descriptor.
	 *
	 * @return The TriggerDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TriggerDescriptor getTriggerDescriptor(String name, SchemaDescriptor sd)
				throws StandardException;

	/**
	 * This method does the job of transforming the trigger action plan text
	 * as shown below. 
	 * 	DELETE FROM t WHERE c = old.c
	 * turns into
	 *  DELETE FROM t WHERE c = org.apache.derby.iapi.db.Factory::
	 *  	getTriggerExecutionContext().getOldRow().
	 *      getInt(columnNumberFor'C'inRuntimeResultset);
	 * In addition to that, for CREATE TRIGGER time, it does the job of 
	 * collecting the column positions of columns referenced in trigger 
	 * action plan through REFERENCEs clause. This information will get
	 * saved in SYSTRIGGERS table by the caller in CREATE TRIGGER case.
	 *  
	 * It gets called either 
	 * 1)at the trigger creation time for row level triggers or
	 * 2)if the trigger got invalidated by some other sql earlier and the 
	 * current sql needs that trigger to fire. For such a trigger firing 
	 * case, this method will get called only if it is row level trigger 
	 * with REFERENCES clause. This work was done as part of DERBY-4874. 
	 * Before DERBY-4874, once the stored prepared statement for trigger 
	 * action plan was generated, it was never updated ever again. But, 
	 * one case where the trigger action plan needs to be regenerated is say
	 * when the column length is changed for a column which is REFERENCEd as
	 * old or new column value. eg of such a case would be say the Alter
	 * table has changed the length of a varchar column from varchar(30) to
	 * varchar(64) but the stored prepared statement associated with the 
	 * trigger action plan continued to use varchar(30). To fix varchar(30) 
	 * in stored prepared statement for trigger action sql to varchar(64), 
	 * we need to regenerate the trigger action sql. This new trigger 
	 * action sql will then get updated into SYSSTATEMENTS table.
	 * 
	 * If we are here for case 1) above, then we will collect all column 
	 * references in trigger action through new/old transition variables. 
	 * Information about them will be saved in SYSTRIGGERS table DERBY-1482
	 * (if we are dealing with pre-10.7 db, then we will not put any 
	 * information about trigger action columns in the system table to ensure 
	 * backward compatibility). This information along with the trigger 
	 * columns will decide what columns from the trigger table will be
	 * fetched into memory during trigger execution.
	 * 
	 * If we are here for case 2) above, then all the information about 
	 * column references in trigger action has already been collected during
	 * CREATE TRIGGER time and hence we can use that available information 
	 * about column positions to do the transformation of OLD/NEW transient 
	 * references.
	 * 
	 * More information on case 1) above. 
	 * DERBY-1482 One of the work done by this method for row level triggers
	 * is to find the columns which are referenced in the trigger action 
	 * through the REFERENCES clause ie thro old/new transition variables.
	 * This information will be saved in SYSTRIGGERS so it can be retrieved
	 * during the trigger execution time. The purpose of this is to recognize
	 * what columns from the trigger table should be read in during trigger
	 * execution. Before these code changes, during trigger execution, Derby
	 * was opting to read all the columns from the trigger table even if they
	 * were not all referenced during the trigger execution. This caused Derby
	 * to run into OOM at times when it could really be avoided.
	 *
	 * We go through the trigger action text and collect the column positions
	 * of all the REFERENCEd columns through new/old transition variables. We
	 * keep that information in SYSTRIGGERS. At runtime, when the trigger is
	 * fired, we will look at this information along with trigger columns from
	 * the trigger definition and only fetch those columns into memory rather
	 * than all the columns from the trigger table.
	 * This is especially useful when the table has LOB columns and those
	 * columns are not referenced in the trigger action and are not recognized
	 * as trigger columns. For such cases, we can avoid reading large values of
	 * LOB columns into memory and thus avoiding possibly running into OOM
	 * errors.
	 * 
	 * If there are no trigger columns defined on the trigger, we will read all
	 * the columns from the trigger table when the trigger fires because no
	 * specific columns were identified as trigger column by the user. The
	 * other case where we will opt to read all the columns are when trigger
	 * columns and REFERENCING clause is identified for the trigger but there
	 * is no trigger action column information in SYSTRIGGERS. This can happen
	 * for triggers created prior to 10.7 release and later that database got
	 * hard/soft-upgraded to 10.7 or higher release.
	 *
	 * @param actionStmt This is needed to get access to the various nodes
	 * 	generated by the Parser for the trigger action sql. These nodes will be
	 * 	used to find REFERENCEs column nodes.
	 * 
	 * @param oldReferencingName The name specified by the user for REFERENCEs
	 * 	to old row columns
	 * 
	 * @param newReferencingName The name specified by the user for REFERENCEs
	 * 	to new row columns
	 * 
	 * @param triggerDefinition The original trigger action text provided by
	 * 	the user during CREATE TRIGGER time.
	 * 
	 * @param referencedCols Trigger is defined on these columns (will be null
	 *   in case of INSERT AND DELETE Triggers. Can also be null for DELETE
	 *   Triggers if UPDATE trigger is not defined on specific column(s))
	 *   
	 * @param referencedColsInTriggerAction	what columns does the trigger 
	 * 	action reference through old/new transition variables (may be null)
	 * 
	 * @param actionOffset offset of start of action clause
	 * 
	 * @param triggerTableDescriptor Table descriptor for trigger table
	 * 
	 * @param triggerEventMask TriggerDescriptor.TRIGGER_EVENT_XXX
	 * 
	 * @param createTriggerTime True if here for CREATE TRIGGER,
	 * 	false if here because an invalidated row level trigger with 
	 *  REFERENCEd columns has been fired and hence trigger action
	 *  sql associated with SPSDescriptor may be invalid too.
	 * 
	 * @return Transformed trigger action sql
	 * @throws StandardException
	 */
	public String getTriggerActionString(
			Visitable actionStmt,
			String oldReferencingName,
			String newReferencingName,
			String triggerDefinition,
			int[] referencedCols,
			int[] referencedColsInTriggerAction,
			int actionOffset,
			TableDescriptor triggerTableDescriptor,
			int triggerEventMask,
			boolean createTriggerTime)
	throws StandardException;
	

	/**
	 * Load up the trigger descriptor list for this table
	 * descriptor and return it.  If the descriptor list
     * is already loaded up, it is returned without further
     * ado. The descriptors are returned in the order in
     * which the triggers were created, with the oldest first.
	 *
	 * @param td			The table descriptor.
	 *
	 * @return The ConstraintDescriptorList for the table
	 *
	 * @exception StandardException		Thrown on failure
	 */
    public TriggerDescriptorList getTriggerDescriptors(TableDescriptor td)
		throws StandardException;

	/**
	 * Update the trigger descriptor in question.  Updates
	 * every row in the base conglomerate.  
	 *
	 * @param triggerd				The Triggerescriptor
	 * @param formerUUID			The UUID for this column in SYSTRIGGERS,
	 *								may differ from what is in triggerd if this
	 *								is the column that is being set.
	 * @param colsToSet 			Array of ints of columns to be modified,
	 *								1 based.  May be null (all cols).
	 * @param tc					The TransactionController to use
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateTriggerDescriptor
	(
		TriggerDescriptor 	triggerd,
		UUID					formerUUID,
		int[]					colsToSet,
		TransactionController	tc
	) throws StandardException;

	/**
	 * Drops the given TriggerDescriptor that is associated
	 * with the given table and constraint type from the data dictionary.
	 *
	 * @param descriptor	The descriptor to drop
	 * @param tc	The TransactionController.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropTriggerDescriptor
	(
		TriggerDescriptor 		descriptor,
		TransactionController 	tc
	) throws StandardException;

	/**
	 * Get all of the ConglomerateDescriptors in the database and
	 * hash them by conglomerate number.
	 * This is useful as a performance optimization for the locking VTIs.
	 * NOTE:  This method will scan SYS.SYSCONGLOMERATES at READ COMMITTED.
	 * It should really scan at READ UNCOMMITTED, but there is no such
	 * thing yet.
	 *
	 * @param tc		TransactionController for the transaction
	 *
	 * @return	A Hashtable with all of the ConglomerateDescriptors
	 *		in the database hashed by conglomerate number.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @SuppressWarnings("UseOfObsoleteCollectionType")
    public Hashtable<Long, ConglomerateDescriptor>
        hashAllConglomerateDescriptorsByNumber(TransactionController tc)
            throws StandardException;

	/**
	 * Get all of the TableDescriptors in the database and hash them by TableId
	 * This is useful as a performance optimization for the locking VTIs.
	 * NOTE:  This method will scan SYS.SYSTABLES at READ COMMITTED.
	 * It should really scan at READ UNCOMMITTED, but there is no such
	 * thing yet.
	 *
	 * @param tc		TransactionController for the transaction
	 *
	 * @return	A Hashtable with all of the Table descriptors in the database
	 *			hashed by TableId
	 *
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @SuppressWarnings("UseOfObsoleteCollectionType")
    public Hashtable<UUID, TableDescriptor> hashAllTableDescriptorsByTableId(TransactionController tc)
		throws StandardException;

	/**
	 * Get a ConglomerateDescriptor given its UUID.  If it is an index
	 * conglomerate shared by at least another duplicate index, this returns
	 * one of the ConglomerateDescriptors for those indexes. 
	 *
	 * @param uuid	The UUID
	 *
	 *
	 * @return A ConglomerateDescriptor for the conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor getConglomerateDescriptor(UUID uuid)
				throws StandardException;

	/**
	 * Get an array of ConglomerateDescriptors given the UUID.  If it is a
	 * heap conglomerate or an index conglomerate not shared by a duplicate
	 * index, the size of the return array is 1. If the uuid argument is null, then
     * this method retrieves descriptors for all of the conglomerates in the database.
	 *
	 * @param uuid	The UUID
	 *
	 *
	 * @return An array of ConglomerateDescriptors for the conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor[] getConglomerateDescriptors(UUID uuid)
				throws StandardException;

	/**
	 * Get a ConglomerateDescriptor given its conglomerate number.  If it is an
	 * index conglomerate shared by at least another duplicate index, this
	 * returns one of the ConglomerateDescriptors for those indexes. 
	 *
	 * @param conglomerateNumber	The conglomerate number.
	 *
	 *
	 * @return A ConglomerateDescriptor for the conglomerate.  Returns NULL if
	 *				no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor	getConglomerateDescriptor(
						long conglomerateNumber)
						throws StandardException;

	/**
	 * Get an array of conglomerate descriptors for the given conglomerate
	 * number.  If it is a heap conglomerate or an index conglomerate not
	 * shared by a duplicate index, the size of the return array is 1.
	 *
	 * @param conglomerateNumber	The number for the conglomerate
	 *				we're interested in
	 *
	 * @return	An array of ConglomerateDescriptors that share the requested
	 *		conglomerate. Returns size 0 array if no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor[]	getConglomerateDescriptors(
						long conglomerateNumber)
						throws StandardException;

	/**
	 * Gets a conglomerate descriptor for the named index in the given schema,
	 * getting an exclusive row lock on the matching row in 
	 * sys.sysconglomerates (for DDL concurrency) if requested.
	 *
	 * @param indexName	The name of the index we're looking for
	 * @param sd		The schema descriptor
	 * @param forUpdate	Whether or not to get an exclusive row 
	 *					lock on the row in sys.sysconglomerates.
	 *
	 * @return	A ConglomerateDescriptor describing the requested
	 *		conglomerate. Returns NULL if no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor	getConglomerateDescriptor(
						String indexName,
						SchemaDescriptor sd,
						boolean forUpdate)
						throws StandardException;

	/**
	 * Drops a conglomerate descriptor
	 *
	 * @param conglomerate	The ConglomerateDescriptor for the conglomerate
	 * @param tc		TransactionController for the transaction
	 *
	 * @exception StandardException		Thrown on failure
	 */

	public void dropConglomerateDescriptor(
						ConglomerateDescriptor conglomerate,
						TransactionController tc)
						throws StandardException;
 
	/**
	 * Drops all conglomerates associated with a table.
	 *
	 * @param td		The TableDescriptor of the table 
	 * @param tc		TransactionController for the transaction
	 *
	 * @exception StandardException		Thrown on failure
	 */

	public void dropAllConglomerateDescriptors(
						TableDescriptor td,
						TransactionController tc)
						throws StandardException;

	/**
	 * Update the conglomerateNumber for an array of ConglomerateDescriptors.
	 * In case of more than one ConglomerateDescriptor, they are for duplicate
	 * indexes sharing one conglomerate.
	 * This is useful, in 1.3, when doing a bulkInsert into an 
	 * empty table where we insert into a new conglomerate.
	 * (This will go away in 1.4.)
	 *
	 * @param cds					The array of ConglomerateDescriptors
	 * @param conglomerateNumber	The new conglomerate number
	 * @param tc					The TransactionController to use
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateConglomerateDescriptor(ConglomerateDescriptor[] cds,
											 long conglomerateNumber,
											 TransactionController tc)
		throws StandardException;

	/**
	 * Update the conglomerateNumber for a ConglomerateDescriptor.
	 * This is useful, in 1.3, when doing a bulkInsert into an 
	 * empty table where we insert into a new conglomerate.
	 * (This will go away in 1.4.)
	 *
	 * @param cd					The ConglomerateDescriptor
	 * @param conglomerateNumber	The new conglomerate number
	 * @param tc					The TransactionController to use
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateConglomerateDescriptor(ConglomerateDescriptor cd,
											 long conglomerateNumber,
											 TransactionController tc)
		throws StandardException;

	/**
	 * Gets a list of the dependency descriptors for the given dependent's id.
	 *
	 * @param dependentID		The ID of the dependent we're interested in
	 *
	 * @return	List			Returns a list of DependencyDescriptors. 
	 *							Returns an empty list if no stored dependencies for the
	 *							dependent's ID.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    List<DependencyDescriptor> getDependentsDescriptorList(String dependentID)
		throws StandardException;

	/**
	 * Gets a list of the dependency descriptors for the given provider's id.
	 *
	 * @param providerID		The ID of the provider we're interested in
	 *
	 * @return	List			Returns a list of DependencyDescriptors. 
	 *							Returns an empty List if no stored dependencies for the
	 *							provider's ID.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    List<DependencyDescriptor> getProvidersDescriptorList(String providerID)
		throws StandardException;

	/**
	 * Build and return an List with DependencyDescriptors for
	 * all of the stored dependencies.  
	 * This is useful for consistency checking.
	 *
	 * @return List		List of all DependencyDescriptors.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    public List<TupleDescriptor> getAllDependencyDescriptorsList()
				throws StandardException;

	/** 
	 * Drop a dependency from the data dictionary.
	 * 
	 * @param dd	The DependencyDescriptor.
	 * @param tc	TransactionController for the transaction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void dropStoredDependency(DependencyDescriptor dd,
									TransactionController tc )
				throws StandardException;

	/** 
	 * Remove all of the stored dependencies for a given dependent's ID 
	 * from the data dictionary.
	 * 
	 * @param dependentsUUID	Dependent's uuid
	 * @param tc				TransactionController for the transaction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void dropDependentsStoredDependencies(UUID dependentsUUID,
									   TransactionController tc) 
				throws StandardException;	

	/**
	 * Get the UUID Factory.  (No need to make the UUIDFactory a module.)
	 *
	 * @return UUIDFactory	The UUID Factory for this DataDictionary.
	 */
	UUIDFactory getUUIDFactory();

    /**
     * Get the alias descriptor for an ANSI UDT.
     *
     * @param tc The transaction to use: if null, use the compilation transaction
     * @param dtd The UDT's type descriptor
     *
     * @return The UDT's alias descriptor if it is an ANSI UDT; null otherwise.
     */
    public AliasDescriptor getAliasDescriptorForUDT( TransactionController tc, DataTypeDescriptor dtd ) throws StandardException;
    
	/**
	 * Get an AliasDescriptor given its UUID.
	 *
	 * @param uuid	The UUID
	 *
	 *
	 * @return The AliasDescriptor for method alias.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public AliasDescriptor getAliasDescriptor(UUID uuid)
				throws StandardException;

	/**
	 * Get a AliasDescriptor by alias name and name space.
	 * NOTE: caller responsible for handling no match.
	 *
	   @param schemaID		schema identifier
	 * @param aliasName		The alias name.
	 * @param nameSpace		The alias name space.
	 *
	 * @return AliasDescriptor	AliasDescriptor for the alias name and name space
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public AliasDescriptor getAliasDescriptor(String schemaID, String aliasName, char nameSpace)
			throws StandardException;

	/**
		Get the list of routines matching the schema and routine name.
	 */
    public List<AliasDescriptor> getRoutineList(
        String schemaID,
        String routineName,
        char nameSpace) throws StandardException;
	
	/** 
	 * Drop an AliasDescriptor from the DataDictionary
	 *
	 * @param ad	The AliasDescriptor to drop
	 * @param tc	The TransactionController
	 *
	 * @exception StandardException		Thrown on failure
	 */

	public void dropAliasDescriptor(AliasDescriptor ad, 
									TransactionController tc)
			throws StandardException;

	/**
	 * Update a user. Changes all columns in the corresponding SYSUSERS row
     * except for the user name.
	 *
	 * @param newDescriptor New values for columns in the SYSUSERS row.
	 * @param tc					The TransactionController to use
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateUser( UserDescriptor newDescriptor,TransactionController tc )
		throws StandardException;

	/**
	 * Return the credentials descriptor for the named user.
	 *
	 * @param userName      Name of the user whose credentials we want.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public UserDescriptor getUser( String userName )
		throws StandardException;

	/** 
	 * Drop a User from the DataDictionary
	 *
	 * @param userName The user to drop.
	 * @param tc	The TransactionController
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void dropUser( String userName, TransactionController tc )
        throws StandardException;
    
	public	int	getEngineType();

	/**
	 * Get a FileInfoDescriptor given its id.
	 *
	 * @param id The descriptor's id.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public FileInfoDescriptor getFileInfoDescriptor(UUID id)
				throws StandardException;

	/**
	 * Get a FileInfoDescriptor given its SQL name and
	 * schema name.  
	 *
	 * @param sd        the schema that holds the FileInfoDescriptor.
	 * @param name		SQL name of file.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public FileInfoDescriptor getFileInfoDescriptor(SchemaDescriptor sd, String name)
				throws StandardException;

	/**
	 * Drop a FileDescriptor from the datadictionary.
	 *
	 * @exception StandardException Oops
	 */
	public void dropFileInfoDescriptor(FileInfoDescriptor fid)
				throws StandardException;


	/**
	 * returns an array of RowLocations corresponding to
	 * the autoincrement columns in the table. The RowLocation points to the 
	 * row in SYSCOLUMNS for this particular ai column.
	 * The array has as many elements as there are columns in the table. If a column
	 * is not an ai column, the entry is NULL.
	 *
	 * @param 	tc		TransactionControler to use to compute the row location.
	 * @param   td		TableDescriptor
	 * 
	 * @return  array of row locations, null if table has no autoinc columns.
	 *
     * @exception StandardException if an error happens
	 */
	public RowLocation[] computeAutoincRowLocations(TransactionController tc,
											   TableDescriptor td)
		throws StandardException;

        /* Returns a row location template for a table */
	public RowLocation getRowLocationTemplate( LanguageConnectionContext lcc, TableDescriptor td)
		throws StandardException;

	/**
	 * getSetAutoincrementValue fetches the autoincrement value from 
	 * SYSCOLUMNS given a row location. If doUpdate is true it updates
	 * the autoincrement column with the new value.
	 * the value returned by this routine is the new value and *NOT* the
	 * value in the system catalogs.
	 * 
	 * @param rl		RowLocation of the entry in SYSCOLUMNS.
	 * @param tc		TransactionController to use.
	 * @param doUpdate  Write the new value to disk if TRUE.
	 * @param newValue	A NumberDataValue to use to return incremented value. If
	 * null, then the caller simply wants the current value fromd disk.
	 * @param wait		If true, then the caller wants to wait for locks. When
	 * using a nested user xaction we want to timeout right away if the parent
	 * holds the lock.
	 */
	public NumberDataValue 	getSetAutoincrementValue(RowLocation rl,
											 TransactionController tc,
											 boolean doUpdate,
											 NumberDataValue newValue,
											 boolean wait)
		throws StandardException;

	/**
	 * sets a new value in SYSCOLUMNS for a particular
	 * autoincrement column.
	 * 
	 * @param tc		 Transaction Controller to use.
	 * @param tableUUID		 Table Descriptor
	 * @param columnName Name of the column.
	 * @param aiValue	 Value to write to SYSCOLUMNS.
	 * @param incrementNeeded Whether we should increment the value passed in by
	 * the user (aiValue) before writing the value to SYSCOLUMNS.
	 */
	public void setAutoincrementValue(TransactionController tc,
									  UUID tableUUID,
									  String columnName,
									  long aiValue,
									  boolean incrementNeeded)
		throws StandardException;
	
	/**
	 * Get the next number from an ANSI/ISO sequence generator
     * which was created with the CREATE SEQUENCE statement. May
     * raise an exception if the sequence was defined as NO CYCLE and
     * the range of the sequence is exhausted. May allocate a range of
     * sequence numbers and update the CURRENTVALUE column of the
     * corresponding row in SYSSEQUENCES. This work is done in the
     * execution transaction of the current session.
	 * 
	 * @param sequenceUUIDstring String value of the UUID which identifies the sequence
	 * @param returnValue This is a data value to be stuffed with the next sequence number.
     *
     * @throws StandardException if the sequence does not cycle and its range is exhausted
	 */
    public void getCurrentValueAndAdvance
        ( String sequenceUUIDstring, NumberDataValue returnValue )
        throws StandardException;

    /**
     * <p>
     * Peek at the next value which will be returned by a sequence generator.
     * </p>
     */
    public Long peekAtSequence( String schemaName, String sequenceName )
        throws StandardException;

	/**
	 * Gets all statistics Descriptors for a given table.
	 */
	public List<StatisticsDescriptor> getStatisticsDescriptors(TableDescriptor td)
		throws StandardException;

	/**
	 * Drops all statistics descriptors for a given table/index column
	 * combination. If the index is not specified, then all statistics for the
	 * table are dropped.
	 *
	 * @param 	tableUUID 	  UUID of the table 
	 * @param   referenceUUID UUID of the index. This can be null.
	 * @param   tc 			  Transcation Controller to use.
	 */
	public void dropStatisticsDescriptors(UUID tableUUID, UUID referenceUUID,
										  TransactionController tc)
		throws StandardException;
	
	/**
	 * Returns the dependency manager for this DataDictionary. Associated with
	 * each DataDictionary object there is a DependencyManager object which
	 * keeps track of both persistent and stored dependencies. 
	 * 
	 * @see org.apache.derby.iapi.sql.depend.DependencyManager
	 */
	public DependencyManager getDependencyManager();


	/**
	 * Returns the cache mode of the data dictionary.
	 */
	public int getCacheMode();

	/**
	  *	Returns a unique system generated name of the form SQLyymmddhhmmssxxn
	  *	  yy - year, mm - month, dd - day of month, hh - hour, mm - minute, ss - second,
	  *	  xx - the first 2 digits of millisec because we don't have enough space to keep the exact millisec value,
	  *	  n - number between 0-9
	  *
	  *	@return	system generated unique name
	  */
	public String getSystemSQLName();

	/**
	 * Adds a descriptor to a system catalog identified by the catalogNumber. 
	 *
	 * @param tuple			   descriptor to insert.
	 * @param parent           parent descriptor; e.g for a column parent is the
	 * tabledescriptor to which the descriptor is beign inserted. for most other
	 * objects it is the schema descriptor.
	 * @param catalogNumber	   a value which identifies the catalog into which
	 * the descriptor should be inserted. It is the users responsibility to
	 * ensure that the catalogNumber is consistent with the tuple being
	 * inserted. 
	 * @see DataDictionary#SYSCONGLOMERATES_CATALOG_NUM
	 * @param allowsDuplicates whether an exception should be thrown if the
	 * insert results in a duplicate; if this parameter is FALSE then one
	 * of the following exception will be thrown; LANG_OBJECT_ALREADY_EXISTS (if
	 * parent is null) or LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT (if parent is not
	 * null). The error message is created by getting the name and type of the
	 * tuple and parent.
	 * @see org.apache.derby.impl.sql.catalog.DataDictionaryImpl#duplicateDescriptorException
	 * @param 	tc	the transaction controller to use to do all of this.
	 *
	 * @see #addDescriptorArray
	 */
	public void addDescriptor(TupleDescriptor tuple, TupleDescriptor parent,
							  int catalogNumber, boolean allowsDuplicates,
							  TransactionController tc) 
		throws StandardException;

	/** array version of addDescriptor.
	 * @see #addDescriptor
	 */
	public void addDescriptorArray(TupleDescriptor[] tuple, TupleDescriptor parent,
								   int catalogNumber, boolean allowsDuplicates,
								   TransactionController tc) 
		throws StandardException;


	/**
		Check to see if a database has been upgraded to the required
		level in order to use a langauge feature that is.
		<P>
		This is used to ensure new functionality that would lead on disk
		information not understood by a previous release is not executed
		while in soft upgrade mode. Ideally this is called at compile time
		and the parser has a utility method to enable easy use at parse time.
		<P>
		To use this method, a feature implemented in a certain release (DataDictionary version)
		would call it with the constant matching the release. E.g. for a new feature added
		in 10.1, a call such as 
		<PRE>
		// check and throw an exception if the database is not at 10.1
		dd.checkVersion(DataDictionary.DD_VERSION_DERBY_10_1, "NEW FEATURE NAME");
		
		</PRE>
		This call would occur during the compile time, usually indirectly through
		the parser utility method, but direct calls can be made during QueryNode initialization,
		or even at bind time.
		<BR>
		It is not expected that this method would be called at execution time.

		@param majorVersion Data Dictionary major version (DataDictionary.DD_ constant)
		@param feature Non-null to throw an error, null to return the state of the version match.

		@return True if the database has been upgraded to the required level, false otherwise.
	*/
	public boolean checkVersion(int majorVersion, String feature) throws StandardException;

    /**
     * Check if the database is read only and requires some form of upgrade
     * that makes the stored prepared statements invalid.
     *
     * @return {@code true} if the stored prepared statements are invalid
     * because of an upgrade and the database is read only, {@code false}
     * otherwise
     */
    public boolean isReadOnlyUpgrade();
	
    /**
     * Add or remove a permission to the permission database.
     *
     * @param add if true then add the permission, if false remove it.
     * @param perm
     * @param grantee
     * @param tc
     *
     * @return True means revoke has removed a privilege from system
     * table and hence the caller of this method should send invalidation 
     * actions to PermssionDescriptor's dependents.
     */
    public boolean addRemovePermissionsDescriptor( boolean add,
                                                 PermissionsDescriptor perm,
                                                 String grantee,
                                                 TransactionController tc)
        throws StandardException;

    /**
     * Get one user's privileges on a table using tableUUID and authorizationid
     *
     * @param tableUUID
     * @param authorizationId The user name
     *
     * @return a TablePermsDescriptor or null if the user has no permissions on the table.
     *
     * @exception StandardException
     */
    public TablePermsDescriptor getTablePermissions( UUID tableUUID, String authorizationId)
        throws StandardException;

    /**
     * Get one user's privileges on a table using tablePermsUUID
     *
     * @param tablePermsUUID
     *
     * @return a TablePermsDescriptor
     *
     * @exception StandardException
     */
    public TablePermsDescriptor getTablePermissions( UUID tablePermsUUID)
    throws StandardException;

    /**
     * Get one user's column privileges for a table.
     *
     * @param tableUUID
     * @param privType Authorizer.SELECT_PRIV, Authorizer.UPDATE_PRIV, or Authorizer.REFERENCES_PRIV
     * @param forGrant
     * @param authorizationId The user name
     *
     * @return a ColPermsDescriptor or null if the user has no separate column
     *         permissions of the specified type on the table. Note that the user may have been granted
     *         permission on all the columns of the table (no column list), in which case this routine
     *         will return null. You must also call getTablePermissions to see if the user has permission
     *         on a set of columns.
     *
     * @exception StandardException
     */
    public ColPermsDescriptor getColumnPermissions( UUID tableUUID,
                                                    int privType,
                                                    boolean forGrant,
                                                    String authorizationId)
        throws StandardException;


    /**
     * Get one user's column privileges for a table. This routine gets called 
     * during revoke privilege processing
     *
     * @param tableUUID
     * @param privTypeStr (as String) Authorizer.SELECT_PRIV, Authorizer.UPDATE_PRIV, or Authorizer.REFERENCES_PRIV
     * @param forGrant
     * @param authorizationId The user name
     *
     * @return a ColPermsDescriptor or null if the user has no separate column
     *         permissions of the specified type on the table. Note that the user may have been granted
     *         permission on all the columns of the table (no column list), in which case this routine
     *         will return null. You must also call getTablePermissions to see if the user has permission
     *         on a set of columns.
     *
     * @exception StandardException
     */
    public ColPermsDescriptor getColumnPermissions( UUID tableUUID,
            String privTypeStr,
            boolean forGrant,
            String authorizationId)
    throws StandardException;
    /**
     * Get one user's column privileges on a table using colPermsUUID
     *
     * @param colPermsUUID
     *
     * @return a ColPermsDescriptor
     *
     * @exception StandardException
     */
    public ColPermsDescriptor getColumnPermissions( UUID colPermsUUID)
    throws StandardException;

    /**
     * Get one user's permissions for a routine (function or procedure).
     *
     * @param routineUUID
     * @param authorizationId The user's name
     *
     * @return The descriptor of the users permissions for the routine.
     *
     * @exception StandardException
     */
    public RoutinePermsDescriptor getRoutinePermissions( UUID routineUUID, String authorizationId)
        throws StandardException;
    
    /**
     * Get one user's privileges for a routine using routinePermsUUID
     *
     * @param routinePermsUUID
     *
     * @return a RoutinePermsDescriptor
     *
     * @exception StandardException
     */
    public RoutinePermsDescriptor getRoutinePermissions( UUID routinePermsUUID)
    throws StandardException;

	/**
	 * Return the Java class to use for the VTI to which the received
	 * table descriptor maps.
	 *
	 * There are two kinds of VTI mappings that we do: the first is for
	 * "table names", the second is for "table function names".  Table
	 * names can only be mapped to VTIs that do not accept any arguments;
	 * any VTI that has at least one constructor which accepts one or more
	 * arguments must be mapped from a table *function* name.
	 *
	 * An example of a VTI "table name" is the following:
	 *
	 *   select * from SYSCS_DIAG.LOCK_TABLE
	 *
	 * In this case "SYSCS_DIAG.LOCK_TABLE" is the table name that we want
	 * to map.  Since the corresonding VTI does not accept any arguments,
	 * this VTI table name can be used anywhere a normal base table name
	 * can be used.
	 *
	 * An example of a VTI "table function name" is the following:
	 *
	 *   select * from TABLE(SYSCS_DIAG.SPACE_TABLE(?)) x
	 *
	 * In this case "SYSCS_DIAG.SPACE_TABLE" is the table function name that
	 * we want to map.  Since the corresponding VTI can take either one or
	 * two arguments we have to use the TABLE constructor syntax to pass the
	 * argument(s) in as if we were making a function call.  Hence the term
	 * "table function".
	 *
	 * @param td Table descriptor used for the VTI look-up.
	 * @param asTableFunction If false then treat td's descriptor name as a
	 *  VTI "table name"; if true, treat the descriptor name as a VTI "table
	 *  function name".
	 * @return Java class name to which "td" maps, or null if no mapping
	 *  is found.
	 */
	public String getVTIClass(TableDescriptor td, boolean asTableFunction)
		throws StandardException;

	/**
	 * Return the Java class to use for a builtin VTI to which the received
	 * table descriptor maps.
	 *
	 *
	 * @param td Table descriptor used for the VTI look-up.
	 * @param asTableFunction If false then treat td's descriptor name as a
	 *  VTI "table name"; if true, treat the descriptor name as a VTI "table
	 *  function name".
	 * @return Java class name of builtin VTI to which "td" maps, or null if no mapping
	 *  is found.
	 */
	public String getBuiltinVTIClass(TableDescriptor td, boolean asTableFunction)
		throws StandardException;


	/**
	 * Get a role grant descriptor for a role definition.
	 *
	 * @param roleName The name of the role whose definition we seek
	 *
	 * @throws StandardException error
	 */
	public RoleGrantDescriptor getRoleDefinitionDescriptor(String roleName)
			throws StandardException;

	/**
	 * Get the role grant descriptor corresponding to the uuid provided
	 *
	 * @param uuid
	 *
	 * @return The descriptor for the role grant descriptor
	 *
	 * @exception StandardException  Thrown on error
	 */
	public RoleGrantDescriptor getRoleGrantDescriptor(UUID uuid)
			throws StandardException;

	/**
	 * Get a descriptor for a role grant
	 *
	 * @param roleName The name of the role whose definition we seek
	 * @param grantee  The grantee
	 * @param grantor  The grantor
	 *
	 * @throws StandardException error
	 */
	public RoleGrantDescriptor getRoleGrantDescriptor(String roleName,
													  String grantee,
													  String grantor)
		throws StandardException;


	/** 
	 * Remove all of the stored dependencies for a given dependent's ID 
	 * from the data dictionary.
	 * 
	 * @param dependentsUUID	Dependent's uuid
	 * @param tc				TransactionController for the transaction
	 * @param wait  If true, then the caller wants to wait for locks. False will
	 *	            be when we using a nested user xaction - we want to timeout 
	 *              right away if the parent holds the lock. 
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void dropDependentsStoredDependencies(UUID dependentsUUID,
									   TransactionController tc,
									   boolean wait) 
				throws StandardException;	

	/**
	 * Check all dictionary tables and return true if there is any GRANT
	 * descriptor containing <code>authId</code> as its grantee.
	 *
	 * @param authId grantee for which a grant exists or not
	 * @param tc TransactionController for the transaction
	 * @return boolean true if such a grant exists
	 */
	public boolean existsGrantToAuthid(String authId,
									   TransactionController tc)
				throws StandardException;

	
	/**
	 * Drop and recreate metadata stored prepared statements.
	 * 
     * @param tc the xact
	 * @throws StandardException
	 */
	public void updateMetadataSPSes(TransactionController tc) throws StandardException;

    /**
     * Drop a sequence descriptor.
     * @param sequenceDescriptor
     * @param tc
     */
    public void dropSequenceDescriptor(SequenceDescriptor sequenceDescriptor, TransactionController tc) throws StandardException;

    /**
     * get a descriptor for a Sequence by uuid
     * @param uuid uuid of the sequence
     * @return the SequenceDescriptor
     * @throws StandardException error
     */
    public SequenceDescriptor getSequenceDescriptor(UUID uuid) throws StandardException;

    /**
     * get a descriptor for a Sequence by sequence name
     * @param sequenceName Name of the sequence
     * @param sd The scemadescriptor teh sequence belongs to
     * @return The SequenceDescriptor
     * @throws StandardException error
     */
    public SequenceDescriptor getSequenceDescriptor(SchemaDescriptor sd, String sequenceName)
            throws StandardException;

    /**
     * Get permissions granted to one user for an object using the object's Id
     * and the user's authorization Id.
     *
     * @param objectUUID ID of the object being protected
     * @param objectType Type of the object (e.g., PermDescriptor.SEQUENCE_TYPE)
     * @param privilege The kind of privilege needed (e.g., PermDescriptor.USAGE_PRIV)
     * @param granteeAuthId The user who needs the permission
     *
     * @return The descriptor of the permissions for the object
     *
     * @exception StandardException
     */
    public PermDescriptor getGenericPermissions(UUID objectUUID, String objectType, String privilege, String granteeAuthId)
        throws StandardException;

    /**
     * Get one user's privileges for an object using the permUUID
     *
     * @param permUUID
     *
     * @return a PermDescriptor
     *
     * @exception StandardException
     */
    public PermDescriptor getGenericPermissions(UUID permUUID)
    throws StandardException;

    /**
     * Drops all permission descriptors for the given object
     *
     * @param objectID The UUID of the object from which to drop
     *                  all permissions
     * @param tc        TransactionController for the transaction
     * @throws StandardException Thrown on error
     */
    public void dropAllPermDescriptors(UUID objectID, TransactionController tc)
            throws StandardException;

    /**
     * Tells if an index statistics refresher should be created for this
     * database.
     * <p>
     * The only reason not to create an index statistics refresher is if one
     * already exists.
     *
     * @return {@code true} if an index statistics refresher should be created,
     *      {@code false} if one already exists.
     */
    public boolean doCreateIndexStatsRefresher();

    /**
     * Creates an index statistics refresher for this data dictionary.
     * <p>
     * The index statistics refresher is used to create and refresh index
     * cardinality statistics, either automatically or on user demand (i.e.
     * by invoking SYSCS_UTIL.SYSCS_UPDATE_STATISTICS).
     *
     * @param db the database object associated with this data dictionary
     * @param databaseName the name of the database
     */
    public void createIndexStatsRefresher(Database db, String databaseName);

    /**
     * Returns the index statistics refresher.
     *
     * @param asDaemon whether the usage is automatic ({@code true}) or
     *      explicit ({@code false})
     * @return The index statistics refresher instance, or {@code null} if
     *      disabled. If {@code asDaemon} is {@code false}, an instance will
     *      always be returned.
     */
    public IndexStatisticsDaemon getIndexStatsRefresher(boolean asDaemon);

    /**
     * Disables automatic refresh/creation of index statistics at runtime.
     * <p>
     * If the daemon is disabled, it can only be enabled again by rebooting
     * the database. Note that this method concerns diabling the daemon at
     * runtime, and only the automatic updates of statistics. If wanted, the
     * user would disable the daemon at boot-time by setting a property
     * (system-wide or database property).
     * <p>
     * <em>Usage note:</em> This method was added to allow the index refresher
     * itself to notify the data dictionary that it should be disabled. This
     * only happens if the refresher/daemon experiences severe errors, or a
     * large amount of errors. It would then disable itself to avoid eating up
     * system resources and potentially cause side-effects due to the errors.
     */
    public void disableIndexStatsRefresher();

    /**
     * Get a {@code DependableFinder} instance.
     *
     * @param formatId the format id
     * @return an instance capable of finding {@code Dependable}s with the
     * specified format id
     */
    public DependableFinder getDependableFinder(int formatId);

    /**
     * Get a {@code DependableFinder} instance for referenced columns in
     * a table.
     *
     * @param formatId the format id
     * @param columnBitMap byte array encoding the bitmap of referenced columns
     * @return an instance capable of finding {@code Dependable}s with the
     * specified format id
     */
    public DependableFinder getColumnDependableFinder(
            int formatId, byte[] columnBitMap);
}
