/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.DataDictionary

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;

import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import java.util.List;

import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

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
 * @author Jeff Lichtman
 */

public interface DataDictionary
{
	String MODULE = "org.apache.derby.iapi.sql.dictionary.DataDictionary";

	/** Special version indicating the database must be upgraded to or created at the current engine level */
	public static final int DD_VERSION_CURRENT			= -1;
	/** Cloudscape 5.0 System Catalog version */
	public static final int DD_VERSION_CS_5_0			= 80;
	/** Cloudscape 5.1 (Arwen) System Catalog version */
	public static final int DD_VERSION_CS_5_1			= 90;
	/** Cloudscape 5.2 (Frodo) System Catalog version */
	public static final int DD_VERSION_CS_5_2			= 100;
	/** Cloudscape 8.1 (Pre-Gandalf) System Catalog version */
	public static final int DD_VERSION_CS_8_1			= 110;
	/** Cloudscape 10.0 (Gandalf) System Catalog version */
	public static final int DD_VERSION_CS_10_0			= 120;

	// general info
	public	static	final	String	DATABASE_ID = "derby.databaseID";

	// version ids
	public	static	final	String	CORE_DATA_DICTIONARY_VERSION = "DataDictionaryVersion";
	public	static	final	String	CREATE_DATA_DICTIONARY_VERSION = "CreateDataDictionaryVersion";
	public  static  final   String  SOFT_DATA_DICTIONARY_VERSION = "derby.softDataDictionaryVersion";
    public  static  final   String  PROPERTY_CONGLOMERATE_VERSION = "PropertyConglomerateVersion";

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

	/* static finals for constraints 
	 * (Here because they are needed by parser, compilation and execution.)
	 */
	public static final int NOTNULL_CONSTRAINT = 1;
	public static final int PRIMARYKEY_CONSTRAINT = 2;
	public static final int UNIQUE_CONSTRAINT = 3;
	public static final int CHECK_CONSTRAINT = 4;
	public static final int DROP_CONSTRAINT = 5;
	public static final int FOREIGNKEY_CONSTRAINT = 6;

	/** Modes returned from startReading() */
	public static final int COMPILE_ONLY_MODE = 0;
	public static final int DDL_MODE = 1;

	/**
	 * Push a data dictionary context onto the
	 * current context manager.
	 *
	 * @param nested true iff this is a nested data dictionary context.
	 */
	DataDictionaryContext pushDataDictionaryContext(ContextManager cm, boolean nested);

	/**
	 * Clear all of the DataDictionary caches.
	 *
	 * @exception StandardException Standard Cloudscape error policy
	 */
	public void clearCaches() throws StandardException;

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
 	  *	Get the tabinfo of a system catalog. Paw through the tabinfo arrays looking for the tabinfo
 	  *	corresponding to this table name.
 	  *
 	  * RESOLVE: This does not bother to fault in the TabInfo. It assumes it already
 	  *          has been faulted in. This seems odd.
 	  *
 	  *	@param	tableName	name of table to get the TabInfo for.
 	  *
 	  *	@return	tabinfo corresponding to tablename
 	  *
 	  * @exception StandardException		Thrown on error
 	  */
	public	TabInfo	getTabInfo( String tableName ) throws StandardException;

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
	 * Get the descriptor for the named schema. If the schemaId
	 * parameter is NULL, it gets the descriptor for the current (default)
	 * schema. Schema descriptors include authorization ids and schema ids.
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.
	 *
	 * @param schemaId	The id of the schema we're interested in.
	 *			If the name is NULL, get the descriptor for the
	 *			current schema.
	 *
	 * @param tc		The transaction controller to us when scanning
	 *					SYSSCHEMAS
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getSchemaDescriptor(UUID schemaId, TransactionController tc)
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
	 * Get the descriptor for the SYSCS_DIAG schema. Schema descriptors 
     * include authorization ids and schema ids.
     *
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getSystemDiagSchemaDescriptor( )
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
	 * @param schema descriptor
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
	 *
	 * @return	The descriptor for the table, null if table does not
	 *		existe.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TableDescriptor		getTableDescriptor(String tableName,
					SchemaDescriptor schema)
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
	 * @param descriptor	The table descriptor to drop
	 * @param schema		A descriptor for the schema the table
	 *						is a part of.  If this parameter is
	 *						NULL, then the table is part of the
	 *						current (default) schema
	 * @param tc			TransactionController for the transaction
	 *
	 * @return	Nothing
	 *
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
	 * @return Nothing.
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
	 * descriptor and return it.  If the descriptor list
	 * is already loaded up, it is retuned without further
	 * ado.
	 *
	 * @param table			The table descriptor.
	 *
	 * @return The ConstraintDescriptorList for the table
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
	 * @param table			The table descriptor.
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
	 * @param table			The table descriptor.
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
	 * @param table				The table descriptor.
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
	 * the constraintId passsed in.
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
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	addConstraintDescriptor(
			ConstraintDescriptor descriptor,
			TransactionController tc)
						throws StandardException;

	/**
	 * Drops the given ConstraintDescriptor that is associated
	 * with the given table and constraint type from the data dictionary.
	 *
	 * NOTE: Caller is responsible for dropping any backing index
	 *
	 * @param table	The table from which to drop the
	 *			constraint descriptor
	 * @param descriptor	The descriptor to drop
	 * @param tc	The TransactionController.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropConstraintDescriptor(TableDescriptor table,
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
	 * @return Nothing.
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
	public List getAllSPSDescriptors()
		throws StandardException;

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
	public DataTypeDescriptor[] getSPSParams(SPSDescriptor spsd, Vector defaults)
		throws StandardException;

	/**
	 * Adds the given SPSDescriptor to the data dictionary,
	 * associated with the given table and constraint type.
	 *
	 * @param descriptor	The descriptor to add
	 * @param tc			The transaction controller
	 * @param wait			To wait for lock or not
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	addSPSDescriptor
	(
		SPSDescriptor 			descriptor,
		TransactionController	tc,
		boolean					wait
	) throws StandardException;

	/**
	 * Updates SYS.SYSSTATEMENTS with the info from the
	 * SPSD. 
	 *
	 * @param descriptor	The descriptor to add
	 * @param tc			The transaction controller
	 * @param recompile		whether to recompile or invalidate
	 * @param updateSYSCOLUMNS indicate whether syscolumns needs to be updated
	 *							or not.
	 * @param wait		If true, then the caller wants to wait for locks. False will be
	 * @param firstCompilation  first time SPS is getting compiled.
	 * when we using a nested user xaction - we want to timeout right away if
	 * the parent holds the lock.  (bug 4821)
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	updateSPS(
			SPSDescriptor		spsd,
			TransactionController	tc,
			boolean                 recompile,
			boolean					updateSYSCOLUMNS,
			boolean					wait,
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
	 * Load up the trigger descriptor list for this table
	 * descriptor and return it.  If the descriptor list
	 * is already loaded up, it is retuned without further
	 * ado.
	 *
	 * @param table			The table descriptor.
	 *
	 * @return The ConstraintDescriptorList for the table
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public GenericDescriptorList getTriggerDescriptors(TableDescriptor td)
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
	 * @return Nothing.
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
	public Hashtable hashAllConglomerateDescriptorsByNumber(TransactionController tc)
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
	public Hashtable hashAllTableDescriptorsByTableId(TransactionController tc)
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
	 * index, the size of the return array is 1.
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
	 * @param table		The TableDescriptor of the table 
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
	 * @return Nothing.
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
	 * @return Nothing.
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
	public List getDependentsDescriptorList(String dependentID)
		throws StandardException;

	/**
	 * Gets a list of the dependency descriptors for the given provider's id.
	 *
	 * @param dependentID		The ID of the provider we're interested in
	 *
	 * @return	List			Returns a list of DependencyDescriptors. 
	 *							Returns an empty List if no stored dependencies for the
	 *							provider's ID.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public List getProvidersDescriptorList(String providerID)
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
	public List getAllDependencyDescriptorsList()
				throws StandardException;

	/** 
	 * Drop a dependency from the data dictionary.
	 * 
	 * @param dd	The DependencyDescriptor.
	 * @param tc	TransactionController for the transaction
	 *
	 * @return Nothing.
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
	 * @return Nothing.
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
	   @param schemaId		schema identifier
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
	public java.util.List getRoutineList(String schemaID, String routineName, char nameSpace)
			throws StandardException;	
	
	/** 
	 * Drop an AliasDescriptor from the DataDictionary
	 *
	 * @param ad	The AliasDescriptor to drop
	 * @param tc	The TransactionController
	 *
	 * @return	Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */

	public void dropAliasDescriptor(AliasDescriptor ad, 
									TransactionController tc)
			throws StandardException;

	/**
	 * Get core catalog info.
	 *
	 * @param coreNum	The index into coreInfo[].
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public TabInfo getCoreCatalog(int coreNum)
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
	 * @param SQLName	the FileInfoDescriptor SQLname.
	 * @param sd        the schema that holds the FileInfoDescriptor.
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
	 * @exception standard exception on error.
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
	 * @param td		 Table Descriptor
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
	 * Gets all statistics Descriptors for a given table.
	 */
	public List getStatisticsDescriptors(TableDescriptor td)
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

		@param majorVersion Data Dictionary major version
		@param feature Non-null to throw an error, null to return the state of the version match.

		@return True if the database has been upgraded to the required level, false otherwise.
	*/
	public boolean checkVersion(int majorVersion, String feature) throws StandardException;
}	
