/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericConstantActionFactory

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.depend.DependableList;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.ResultDescription;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.GenericDescriptorList;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.ListOfRowLists;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.AliasInfo;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.util.Properties;

import java.sql.Timestamp;

/**
 * Factory for creating ConstantActions.
 *
 * <P>Implemetation note: For most operations, the ResultSetFactory
 *    determines if the operation is allowed in a readonly/target database.
 *    Because we perform JAR add/drop/replace with a utility rather than
 *    using normal language processing we never get a result set for these
 *    operations. For this reason, the ConstantActionFactory rather than
 *    the ResultSetFactory checks if the these operations are allowed.
 *
 * @author Rick
 */
public class GenericConstantActionFactory
{
	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////
    public	GenericConstantActionFactory()
	{
	}

	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTANT ACTION MANUFACTORIES
	//
	///////////////////////////////////////////////////////////////////////




	/**
	 * Get ConstantAction for ALTER STATEMENT statement.
	 *
	 *  @param sd			descriptor of the schema in which
	 *						our beloved stmt resides
	 *  @param spsName		Name of sps.  if null, all statements
	 *						are recompiled
	 *	@param usingText	the text of the USING clause	
	 *	@param invalidOnly	only recompile if invalid.  Only used
	 *						for the case where all statements are
	 *						recompiled.
	 */
	public	ConstantAction	getAlterSPSConstantAction
	(
		SchemaDescriptor	sd,
		String				spsName,
		String				usingText,
		boolean				invalidOnly
    )
	{
		return	new AlterSPSConstantAction(sd, spsName, usingText, invalidOnly);
	}
	
	/**
	 * Get ConstantAction for SET CONSTRAINTS statement.
	 *
	 *  @param cdl			the constraints to set, if null,
	 *						we'll go ahead and set them all
	 *  @param enable		if true, turn them on, if false
	 *						disable them
	 *  @param unconditionallyEnforce	Replication sets this to true at
	 *									the end of REFRESH. This forces us
	 *									to run the included foreign key constraints even
	 *									if they're already marked ENABLED.
	 *	@param ddlList		Replication list of actions to propagate,
	 *						null unless a replication source
	 */
	public	ConstantAction getSetConstraintsConstantAction
	(
		ConstraintDescriptorList	cdl,
		boolean						enable,
		boolean						unconditionallyEnforce,
		Object[]					ddlList
    )
	{
		// ignore rep arg
		return new SetConstraintsConstantAction(cdl, enable, unconditionallyEnforce);
	}


	/**
	 *	Make the AlterAction for an ALTER TABLE statement.
	 *
	 *  @param sd			descriptor for the schema that table lives in.
	 *  @param tableName	Name of table.
	 *	@param tableId		UUID of table.
	 *	@param tableConglomerateId	heap conglomerate id of table
	 *  @param tableType	Type of table (e.g., BASE).
	 *  @param columnInfo	Information on all the columns in the table.
	 *  @param constraintActions	ConstraintConstantAction[] for constraints
	 *	@param stageTokens			Compiled array of versioned metadata tokens.
	 *	@param deleteRowLists	lists of tuples to be deleted from Publication catalogs.
	 *	@param insertRowLists	lists of tuples to be inserted into Publication catalogs.
	 *  @param publicationIDs	IDs of publications which must be altered
	 *	@param dependableLists	List, per publication, of objects that this publication depends on.
	 * @param lockGranularity	The lock granularity.
	 *	@param compressTable	Whether or not this is a compress table
	 *	@param behavior			drop behavior of dropping column
	 *	@param sequential	If compress table/drop column, whether or not sequential
	 *	@param compressTable	Whether or not this is a truncate table
	 */
	public	ConstantAction	getAlterTableConstantAction
	(
		SchemaDescriptor			sd,
		String						tableName,
		UUID						tableId,
		long						tableConglomerateId,
		int							tableType,
		ColumnInfo[]				columnInfo,
		ConstraintConstantAction[] 	constraintActions,

		Object[]					stageControl,
		Object[]					ddlList,
		ListOfRowLists				deleteRowLists,
		ListOfRowLists				insertRowLists,

		UUID[]						publicationIDs,
		DependableList[]			dependableLists,
		char						lockGranularity,
		boolean						compressTable,
		int							behavior,
		boolean						sequential,
		boolean                     truncateTable
    )
	{
		// the replication arguments should be null
		return new	AlterTableConstantAction( sd, tableName, tableId, tableConglomerateId, 
											  tableType, columnInfo, constraintActions, 
											  lockGranularity, compressTable,
											  behavior, sequential, truncateTable);
	}

	/**
	 *	Make a ConstantAction for a constraint.
	 *
	 *  @param constraintName	Constraint name.
	 *  @param constraintType	Constraint type.
	 *  @param tableName		Table name.
	 *	@param tableId			UUID of table.
	 *  @param schemaName		Schema that table lives in.
	 *  @param columnNames		String[] for column names
	 *  @param indexAction		IndexConstantAction for constraint (if necessary)
	 *  @param constraintText	Text for check constraint
	 *  RESOLVE - the next parameter should go away once we use UUIDs
	 *			  (Generated constraint names will be based off of uuids)
	 *	@param constraintId		UUID of constraint. null => we should generate one.
	 *	@param enabled			Should the constraint be created as enabled 
	 *							(enabled == true), or disabled (enabled == false).
	 *	@param ConstraintInfo	The referenced constraint, if a foreign key constraint
	 *  @param providerInfo Information on all the Providers
	 */
	public	CreateConstraintConstantAction	getCreateConstraintConstantAction
	(
		String				constraintName,
		int					constraintType,
		String				tableName,
		UUID				tableId,
		String				schemaName,
		String[]			columnNames,
		IndexConstantAction indexAction,
		String				constraintText,
		boolean				enabled,
		ConstraintInfo		otherConstraint,
		ProviderInfo[]		providerInfo
	)
	{
		return new CreateConstraintConstantAction
			( constraintName, constraintType, tableName, 
			  tableId, schemaName, columnNames, indexAction, constraintText, 
			  enabled, otherConstraint, providerInfo );
	}


	/**
	 *	Make the ConstantAction for a CREATE INDEX statement.
	 *
	 *  @param unique		True means it will be a unique index
	 *  @param indexType	The type of index (BTREE, for example)
	 *  @param schemaName			the schema that table (and index) lives in.
	 *  @param indexName	Name of the index
	 *  @param tableName	Name of table the index will be on
	 *	@param tableId		UUID of table.
	 *  @param conglomId	Conglomerate ID of the index, if known in advance
	 *  @param columnNames	Names of the columns in the index, in order
	 *  @param isAscending	Array of booleans telling asc/desc on each column
	 *  @param isConstraint	TRUE if index is backing up a constraint, else FALSE
	 *  @param conglomerateUUID	ID of conglomerate
	 *  @param properties	The optional properties list associated with the index.
	 */
	public	CreateIndexConstantAction	getCreateIndexConstantAction
	(
		boolean			unique,
		String			indexType,
		String			schemaName,
		String			indexName,
		String			tableName,
		UUID			tableId,
		long			conglomId,
		String[]		columnNames,
		boolean[]		isAscending,
		boolean			isConstraint,
		UUID			conglomerateUUID,
		Properties		properties
    )
	{
		return	new CreateIndexConstantAction
			( unique, indexType, schemaName, indexName, tableName, tableId,
			  conglomId, columnNames, isAscending, isConstraint,
			  conglomerateUUID, properties );
	}


	/**
	 *	Make the ConstantAction for a CREATE ALIAS statement.
	 *
	 *  @param aliasName		Name of alias.
	 *  @param schemaName		Alias's schema. 
	 *  @param javaClassName	Name of java class.
	 *  @param methodName		Name of method.
	 *  @param targetClassName	Name of java class at Target database.
	 *  @param targetMethodName	Name of method at Target database.
	 *  @param aliasType		The alias type
	 */
	public	ConstantAction	getCreateAliasConstantAction
	(
		String	aliasName,
		String	schemaName,
		String	javaClassName,
		AliasInfo	aliasInfo,
		char	aliasType)
	{
		return new CreateAliasConstantAction
			(aliasName, schemaName, javaClassName, aliasInfo, aliasType );
	}


	/**
	 * Make the ConstantAction for a CREATE STORED PREPARED STATEMENT statement.
	 * Adds an extra parameter that allows the user to designate whether
	 * this sps can be created in the SYS schema.
	 *
	 *  @param schemaName			name for the schema that table lives in.
	 *  @param spsName		Name of statement
	 *	@param spsText		Text of query expression for sps definition
	 *	@param usingText	the text of the USING clause
	 *	@param okInSys		ok to create in sys schema
	 *	@param nocompile	don't try to compile the sps when it is created
	 *	@param compSchemaId	the compilation schema id
	 */
	public	ConstantAction	getCreateSPSConstantAction
	(
		String				schemaName,
		String				spsName,
		String				spsText,
		String				usingText,
		boolean				okInSys,
		boolean				nocompile,
		UUID				compSchemaId
	)
	{
		return	new CreateSPSConstantAction(schemaName, spsName, spsText, 
						usingText, compSchemaId, okInSys, nocompile);
	}

	/**
	 * Make the ConstantAction for a CREATE SCHEMA statement.
	 *
	 *  @param schemaName	Name of table.
	 *  @param aid			Authorizaton id
	 *  @param schemaId		ID of table. If null, we allocate one.
	 *	@param setToDefault	if true, set the default schema to
	 *			the new schema once it is created.
	 */
	public	ConstantAction	getCreateSchemaConstantAction
	(
		String			schemaName,
		String			aid)
	{
		return new CreateSchemaConstantAction(schemaName, aid);
	}

	/**
	 *	Make the ConstantAction for a CREATE TABLE statement.
	 *
	 *  @param schemaName	name for the schema that table lives in.
	 *  @param tableName	Name of table.
	 *  @param tableType	Type of table (e.g., BASE, global temporary table).
	 *  @param columnInfo	Information on all the columns in the table.
	 *		 (REMIND tableDescriptor ignored)
	 *  @param constraintActions	CreateConstraintConstantAction[] for constraints
	 *  @param properties	Optional table properties
	 * @param lockGranularity	The lock granularity.
	 * @param onCommitDeleteRows	If true, on commit delete rows else on commit preserve rows of temporary table.
	 * @param onRollbackDeleteRows	If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
	 */
	public	ConstantAction	getCreateTableConstantAction
	(
		String			schemaName,
		String			tableName,
		int				tableType,
		ColumnInfo[]	columnInfo,
		CreateConstraintConstantAction[] constraintActions,
		Properties		properties,
		char			lockGranularity,
		boolean			onCommitDeleteRows,
		boolean			onRollbackDeleteRows)
	{
		return new CreateTableConstantAction( schemaName, tableName, tableType, columnInfo,
											  constraintActions, properties,
											  lockGranularity, onCommitDeleteRows, onRollbackDeleteRows);
	}

	/**
	 *	Make the ConstantAction for a savepoint statement (ROLLBACK savepoint, RELASE savepoint and SAVEPOINT).
	 *
	 *  @param savepointName	name for the savepoint.
	 *  @param statementType	Type of savepoint statement ie rollback, release or set savepoint
	 */
	public	ConstantAction	getSavepointConstantAction
	(
		String			savepointName,
		int				statementType)
	{
		return new SavepointConstantAction( savepointName, statementType);
	}


	/**
	 *	Make the ConstantAction for a CREATE VIEW statement.
	 *
	 *  @param sd			descriptor for the schema that table lives in.
	 *  @param tableName	Name of table.
	 *  @param tableType	Type of table (e.g., BASE).
	 *	@param viewText		Text of query expression for view definition
	 *  @param checkOption	Check option type
	 *  @param columnInfo	Information on all the columns in the table.
	 *  @param providerInfo Information on all the Providers
	 *	@param compSchemaId	ID of schema in which the view is to be bound
	 *						when accessed in the future.
	 *		 (REMIND tableDescriptor ignored)
	 */
	public	ConstantAction	getCreateViewConstantAction
	(
		String	schemaName,
		String			tableName,
		int				tableType,
		String			viewText,
		int				checkOption,
		ColumnInfo[]	columnInfo,
		ProviderInfo[]  providerInfo,
		UUID			compSchemaId)
	{
		return new CreateViewConstantAction( schemaName, tableName, tableType, 
											 viewText, checkOption, columnInfo,
											 providerInfo, compSchemaId );
	}



	/**
	 *	Make the ConstantAction for a Replicated DELETE statement.
	 *
	 *  @param conglomId			Conglomerate ID.
	 *  @param tableType			type of this table
	 *	@param heapSCOCI			StaticCompiledOpenConglomInfo for heap.
	 *  @param irgs					Index descriptors
	 *  @param indexCIDS			Conglomerate IDs of indices
	 *	@param indexSCOCIs	StaticCompiledOpenConglomInfos for indexes.
	 *  @param emptyHeapRow			Template for heap row.
	 *	@param deferred				True means deferred delete
	 *  @param tableIsPublished		true if table is published
	 *  @param tableID				table id
	 *	@param lockMode				The lock mode to use
	 *								  (row or table, see TransactionController)
	 *  @param deleteUndoable   	undoable for the delete
	 *  @param endRowsUndoable  	undoable for the end rows token
	 *  @param endStatementUndoable undoable for the end statement token
	 *  @param keySignature     	signature for the key(null for source)
	 *  @param keyPositions     	positions of primary key columns in base row
	 *  @param keyConglomId  		conglomerate id for the key
	 *								(-1 for the souce)
	 *  @param schemaName    		schemaName(null for source)
	 *  @param tableName        	tableName(null for source)
	 *  @param resultDescription	A description of the columns in the row
	 *			to be deleted.  Only set in replication or during cascade Delete.
	 *	@param fkInfo				Array of structures containing foreign key 
	 *								info, if any (may be null)
	 *	@param triggerInfo			Array of structures containing trigger
	 *								info, if any (may be null)

	 *  @param numColumns			Number of columns to read
	 *  @param dependencyId			UUID for dependency system
	 *  @param baseRowReadList      Map of columns read in.  1 based.
	 *	@param baseRowReadMap		BaseRowReadMap[heapColId]->ReadRowColumnId.
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *  @param singleRowSource		Whether or not source is a single row source
	 *
	 *  @exception StandardException		Thrown on failure
	 */
	public	ConstantAction	getDeleteConstantAction
	(
								long				conglomId,
								int					tableType,
								StaticCompiledOpenConglomInfo heapSCOCI,
								IndexRowGenerator[]	irgs,
								long[]				indexCIDS,
								StaticCompiledOpenConglomInfo[] indexSCOCIs,
								ExecRow				emptyHeapRow,
								boolean				deferred,
								boolean				tableIsPublished,
								UUID				tableID,
								int					lockMode,
								Object         		deleteToken,
								Object		     	keySignature,
								int[]				keyPositions,
								long                keyConglomId,
								String				schemaName,
								String				tableName,
								ResultDescription	resultDescription,
								FKInfo[]			fkInfo,
								TriggerInfo			triggerInfo,
								FormatableBitSet				baseRowReadList,
								int[]				baseRowReadMap,
								int[]               streamStorableHeapColIds,
								int					numColumns,
								UUID				dependencyId,
								boolean				singleRowSource,
								ConstantAction[]	dependentConstantActions
	)
			throws StandardException
	{
		// ignore replication args, which should be null
		return new DeleteConstantAction(
										conglomId,
										heapSCOCI,
										irgs,
										indexCIDS,
										indexSCOCIs,
										emptyHeapRow,
										deferred,
										tableID,
										lockMode,
										fkInfo,
										triggerInfo,
										baseRowReadList,
										baseRowReadMap,
										streamStorableHeapColIds,
										numColumns,
										singleRowSource,
										resultDescription,
										dependentConstantActions
										);
	}


	/**
	 *	Make ConstantAction to drop a constraint.
	 *
	 *  @param constraintName	Constraint name.
	 *	@param constraintSchemaName		Constraint Schema Name
	 *  @param tableName		Table name.
	 *	@param tableId			UUID of table.
	 *  @param tableSchemaName				the schema that table lives in.
	 *  @param indexAction		IndexConstantAction for constraint (if necessary)
	 *	@param behavior			The drop behavior (e.g. StatementType.RESTRICT)
     *  @param verifyType       Verify that the constraint is of this type.
	 */
	public	ConstraintConstantAction	getDropConstraintConstantAction
	(
		String					constraintName,
		String					constraintSchemaName,
		String					tableName,
		UUID					tableId,
		String					tableSchemaName,
		IndexConstantAction indexAction,
		int						behavior,
        int                     verifyType
    )
	{
		return	new DropConstraintConstantAction( constraintName, constraintSchemaName, tableName, 
												  tableId, tableSchemaName, indexAction, behavior, verifyType);
	}


	/**
	 *	Make the ConstantAction for a DROP INDEX statement.
	 *
	 *
	 *	@param	fullIndexName		Fully qualified index name
	 *	@param	indexName			Index name.
	 *	@param	tableName			The table name
	 *	@param	schemaName					Schema that index lives in.
	 *  @param  tableId				UUID for table
	 *  @param  tableConglomerateId	heap conglomerate ID for table
	 *
	 */
	public	DropIndexConstantAction	getDropIndexConstantAction
	(
		String				fullIndexName,
		String				indexName,
		String				tableName,
		String				schemaName,
		UUID				tableId,
		long				tableConglomerateId
    )
	{
		return	new DropIndexConstantAction( fullIndexName, indexName, tableName, schemaName,
											 tableId, tableConglomerateId );
	}


	/**
	 *	Make the ConstantAction for a DROP ALIAS statement.
	 *
	 *
	 *	@param	aliasName			Alias name.
	 *	@param	aliasType			Alias type.
	 *
	 */
	public	ConstantAction	getDropAliasConstantAction(SchemaDescriptor	sd, String aliasName, char aliasType)
	{
		return	new DropAliasConstantAction(sd, aliasName, aliasType );
	}


	/**
	 *	Make the ConstantAction for a DROP STATEMENT statement.
	 *
	 *	@param	sd					Schema that stored prepared statement lives in.
	 *	@param	spsName				Name of the SPS
	 *
	 */
	public	ConstantAction	getDropSPSConstantAction
	(
		SchemaDescriptor	sd,
		String				spsName
    )
	{
		return	new DropSPSConstantAction( sd, spsName );
	}


	/**
	 *	Make the ConstantAction for a DROP TABLE statement.
	 *
	 *	@param	schemaName			Table name.
	 *
	 */
	public	ConstantAction	getDropSchemaConstantAction(String	schemaName)
	{
		return	new DropSchemaConstantAction( schemaName );
	}


	/**
	 *	Make the ConstantAction for a DROP TABLE statement.
	 *
	 *
	 *	@param	fullTableName		Fully qualified table name
	 *	@param	tableName			Table name.
	 *	@param	sd					Schema that table lives in.
	 *  @param  conglomerateNubmer	Conglomerate number for heap
	 *  @param  tableId				UUID for table
	 *  @param  behavior			drop behavior, CASCADE, RESTRICT or DEFAULT
	 *
	 */
	public	ConstantAction	getDropTableConstantAction
	(
		String				fullTableName,
		String				tableName,
		SchemaDescriptor	sd,
		long				conglomerateNumber,
		UUID				tableId,
		int					behavior
	)
	{
		return	new DropTableConstantAction( fullTableName, tableName, sd, conglomerateNumber, tableId, behavior );
	}


	/**
	 *	Make the ConstantAction for a DROP VIEW statement.
	 *
	 *
	 *	@param	fullTableName		Fully qualified table name
	 *	@param	tableName			Table name.
	 *	@param	sd					Schema that view lives in.
	 *
	 */
	public	ConstantAction	getDropViewConstantAction
	(
		String				fullTableName,
		String				tableName,
		SchemaDescriptor	sd
    )
	{
		return new DropViewConstantAction( fullTableName, tableName, sd );
	}

	/**
	 *	Make the ConstantAction for a RENAME TABLE/COLUMN/INDEX statement.
	 *
	 *	@param	fullTableName Fully qualified table name
	 *	@param	tableName   Table name.
	 *	@param	oldObjectName   Old object name
	 *	@param	newObjectName   New object name.
	 *	@param	sd    Schema that table lives in.
	 *	@param	tableId   UUID for table
	 *  @param	usedAlterTable	True if used Alter Table command, false if used Rename
	 *  @param	renamingWhat	Value indicates if Rename Column/Index.
	 *
	 */
	public	ConstantAction	getRenameConstantAction
	(
		String				fullTableName,
		String				tableName,
		String				oldObjectName,
		String				newObjectName,
		SchemaDescriptor	sd,
		UUID				tableId,
		boolean				usedAlterTable,
		int				renamingWhat
	)
	{
		return	new RenameConstantAction( fullTableName, tableName, oldObjectName, newObjectName,
		sd, tableId, usedAlterTable, renamingWhat );
	}

	/**
	 *	Make the ConstantAction for a Replicated INSERT statement.
	 *
	 *  @param conglomId		Conglomerate ID.
	 *  @param tableType		type of this table
	 *  @param heapSCOCI		StaticCompiledOpenConglomInfo for target heap.
	 *  @param irgs				Index descriptors
	 *  @param indexCIDS		Conglomerate IDs of indices
	 *	@param indexSCOCIs		StaticCompiledOpenConglomInfos for indexes.
	 *  @param indexNames		Names of indices on this table for error 
	 *							reporting.
	 *	@param deferred			True means deferred insert
	 *  @param tableIsPublished	true if table is published, false otherwise
	 *  @param tableID			table id
	 *  @param targetProperties	Properties on the target table
	 *	@param fkInfo			Array of structures containing foreign key info, 
	 *							if any (may be null)
	 *	@param triggerInfo		Array of structures containing trigger info, 
	 *  @param baseRowReadList      Map of columns read in.  1 based.
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *							if any (may be null)
	 *  @param indexedCols		boolean[] of which (0-based) columns are indexed.
	 *  @param dependencyId		UUID for dependency system
	 *	@param stageControl		Stage Control Tokens
	 *	@param ddlList			List of DDL to log. This is for BULK INSERT into a published table at the Source.
	 *  @param singleRowSource	Whether or not source is a single row source
	 *  @param autoincRowLocation array of row locations into syscolumns for
	                              autoincrement columns
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public	ConstantAction getInsertConstantAction(
								TableDescriptor		tableDescriptor,
								long				conglomId,
								StaticCompiledOpenConglomInfo heapSCOCI,
								IndexRowGenerator[]	irgs,
								long[]				indexCIDS,
								StaticCompiledOpenConglomInfo[] indexSCOCIs,
								String[]			indexNames,
								boolean				deferred,
								boolean				tableIsPublished,
								UUID				tableID,
								int					lockMode,
								Object         		insertToken,
								Object				rowSignature,
								Properties			targetProperties,
								FKInfo[]			fkInfo,
								TriggerInfo			triggerInfo,
								int[]               streamStorableHeapColIds,
								boolean[]			indexedCols,
								UUID				dependencyId,
								Object[]			stageControl,
								Object[]			ddlList,
								boolean				singleRowSource,
								RowLocation[]		autoincRowLocation
							)
			throws StandardException
	{
		return new InsertConstantAction(tableDescriptor,
										conglomId,
										heapSCOCI,
										irgs,
										indexCIDS,
										indexSCOCIs,
										indexNames,
										deferred,
										targetProperties,
										tableID,
										lockMode,
										fkInfo,
										triggerInfo,
										streamStorableHeapColIds,
										indexedCols,
										singleRowSource,
										autoincRowLocation
										);
	}

	/**
	 *	Make the ConstantAction for an updatable VTI statement.
	 *
	 * @param deferred					Deferred mode?
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction getUpdatableVTIConstantAction( int statementType, boolean deferred)
			throws StandardException
	{
		return new UpdatableVTIConstantAction( statementType, deferred, null);
	}

	/**
	 *	Make the ConstantAction for an updatable VTI statement.
	 *
	 * @param deferred					Deferred mode?
     * @param changedColumnIds Array of ids of changed columns
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction getUpdatableVTIConstantAction( int statementType,
                                                         boolean deferred,
                                                         int[] changedColumnIds)
			throws StandardException
	{
		return new UpdatableVTIConstantAction( statementType, deferred, changedColumnIds);
	}

	/**
	 * Make the ConstantAction for a LOCK TABLE statement.
	 *
	 *  @param fullTableName		Full name of the table.
	 *  @param conglomerateNumber	Conglomerate number for the heap
	 *  @param exclusiveMode		Whether or not to get an exclusive lock.
	 */
	public	ConstantAction	getLockTableConstantAction(
					String fullTableName,
					long conglomerateNumber, boolean exclusiveMode)
	{
		return new LockTableConstantAction( 
						fullTableName, conglomerateNumber, exclusiveMode );
	}


	/**
	 * Make the ConstantAction for a SET SCHEMA statement.
	 *
	 *  @param schemaName	Name of schema.
	 *  @param type			Literal, USER or ?
	 */
	public	ConstantAction	getSetSchemaConstantAction(String schemaName, int type)
	{
		return new SetSchemaConstantAction( schemaName , type );
	}

	/**
	 * Make the ConstantAction for a SET TRANSACTION ISOLATION statement.
	 *
	 * @param isolationLevel	The new isolation level.
	 */
	public ConstantAction getSetTransactionIsolationConstantAction(int isolationLevel)
	{
		return new SetTransactionIsolationConstantAction(isolationLevel);
	}


	/**
	 *	Make the ConstantAction for a Replicated DELETE statement.
	 *
	 *  @param conglomId			Conglomerate ID.
	 *  @param tableType			type of this table
	 *	@param heapSCOCI			StaticCompiledOpenConglomInfo for heap.
	 *  @param irgs					Index descriptors
	 *  @param indexCIDS			Conglomerate IDs of indices
	 *	@param indexSCOCIs	StaticCompiledOpenConglomInfos for indexes.
	 *  @param emptyHeapRow			Template for heap row.
	 *	@param deferred				True means deferred update
	 *	@param targetUUID			UUID of target table
	 *	@param lockMode				The lock mode to use
	 *								  (row or table, see TransactionController)
	 *  @param tableIsPublished		true if table is published, false otherwise
	 *	@param changedColumnIds		Array of ids of changes columns
	 *  @param keyPositions     	positions of primary key columns in base row
	 *  @param updateUndoable		an update token
	 *  @param endRowsUndoable		an end rows token
	 *  @param endStatementUndoable an end statement token
	 *	@param fkInfo				Array of structures containing foreign key info, 
	 *								if any (may be null)
	 *	@param triggerInfo			Array of structures containing trigger info, 
	 *  @param baseRowReadList      Map of columns read in.  1 based.
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *								if any (may be null)
	 *  @param baseRowReadMap		map of columns to be selected from the base row
	 *								(partial row). 1 based.
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *  @param numColumns			The number of columns being read.
	 *	@param positionedUpdate		is this a positioned update
	 *  @param singleRowSource		Whether or not source is a single row source
	 *
	 *  @exception StandardException Thrown on failure
	 */
	public	UpdateConstantAction	getUpdateConstantAction(
								long				conglomId,
								int					tableType,
								StaticCompiledOpenConglomInfo heapSCOCI,
								IndexRowGenerator[]	irgs,
								long[]				indexCIDS,
								StaticCompiledOpenConglomInfo[] indexSCOCIs,
								String[]			indexNames,	
								ExecRow				emptyHeapRow,
								boolean				deferred,
								UUID				targetUUID,
								int					lockMode,
								boolean				tableIsPublished,
								int[]				changedColumnIds,
								int[]				keyPositions,
								Object         		updateToken,
								FKInfo[]			fkInfo,
								TriggerInfo			triggerInfo,
								FormatableBitSet				baseRowReadList,
								int[]				baseRowReadMap,
								int[]				streamStorableHeapColIds,
								int					numColumns,
								boolean				positionedUpdate,
								boolean				singleRowSource
							)
			throws StandardException
	{
		return new UpdateConstantAction(
										conglomId,
										heapSCOCI,
										irgs,
										indexCIDS,
										indexSCOCIs,
										indexNames,
										emptyHeapRow,
										deferred,
										targetUUID,
										lockMode,
										changedColumnIds,
										fkInfo,
										triggerInfo,
										baseRowReadList,
										baseRowReadMap,
										streamStorableHeapColIds,
										numColumns,
										positionedUpdate,
										singleRowSource
										);
	}

	/**
	 * Make the ConstantAction to Add a jar file to a database.
	 *
	 *	@param	id					The id for the jar file -
	 *                              (null means create one)
	 *	@param	schemaName			The SchemaName for the jar file.
	 *	@param	sqlName			    The sqlName for the jar file.
	 *  @param  fileName            The name of the file that holds the jar.
	 *  @exception StandardException Ooops
	 */
	public	ConstantAction getAddJarConstantAction(UUID id,
														 String schemaName,
														 String sqlName,
														 String externalPath)
		 throws StandardException
	{
		getAuthorizer().authorize(Authorizer.JAR_WRITE_OP);
		return new AddJarConstantAction(id,schemaName,sqlName,externalPath);
	}
	/**
	 * Make the ConstantAction to replace a jar file in a database.
	 *
	 *	@param	id					The id for the jar file -
	 *                              (Ignored if null)
	 *	@param	schemaName			The SchemaName for the jar file.
	 *	@param	sqlName			    The sqlName for the jar file.
	 *  @param  fileName            The name of the file that holds the new jar.
	 *  @exception StandardException Ooops
	 */
	public	ConstantAction getReplaceJarConstantAction(UUID id,
														 String schemaName,
														 String sqlName,
														 String externalPath)
		 throws StandardException
	{
		getAuthorizer().authorize(Authorizer.JAR_WRITE_OP);
		return new ReplaceJarConstantAction(id,schemaName,sqlName,externalPath);
	}
	/**
	 * Make the ConstantAction to drop a jar file from a database.
	 *
	 *	@param	id					The id for the jar file -
	 *                              (Ignored if null)
	 *	@param	schemaName			The SchemaName for the jar file.
	 *	@param	sqlName			    The sqlName for the jar file.
	 *  @exception StandardException Ooops
	 */
	public	ConstantAction getDropJarConstantAction(UUID id,
														  String schemaName,
														  String sqlName)
		 throws StandardException
	{
		getAuthorizer().authorize(Authorizer.JAR_WRITE_OP);
		return new DropJarConstantAction(id,schemaName,sqlName);
	}

	static protected Authorizer getAuthorizer()
	{
		LanguageConnectionContext lcc = (LanguageConnectionContext)
			ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
		return lcc.getAuthorizer();
	}

	/**
	 *	Make the ConstantAction for a CREATE TRIGGER statement.
	 *
	 * @param triggerSd		descriptor for the schema that trigger lives in.
	 * @param triggerName	Name of trigger
	 * @param eventMask		TriggerDescriptor.TRIGGER_EVENT_XXXX
	 * @param isBefore		is this a before (as opposed to after) trigger 
	 * @param isRow			is this a row trigger or statement trigger
	 * @param isEnabled		is this trigger enabled or disabled
	 * @param triggerTable	the table upon which this trigger is defined
	 * @param whenSPSId		the sps id for the when clause (may be null)
	 * @param whenText		the text of the when clause (may be null)
	 * @param actionSPSId	the spsid for the trigger action (may be null)
	 * @param actionText	the text of the trigger action (may be null)
	 * @param spsCompSchemaId	the compilation schema for the action and when
	 *							spses.   If null, will be set to the current default
	 *							schema
	 * @param creationTimestamp	when was this trigger created?  if null, will be
	 *						set to the time that executeConstantAction() is invoked
	 * @param referencedCols	what columns does this trigger reference (may be null)
	 * @param originalActionText The original user text of the trigger action
	 * @param referencingOld whether or not OLD appears in REFERENCING clause
	 * @param referencingNew whether or not NEW appears in REFERENCING clause
	 * @param oldReferencingName old referencing table name, if any, that appears in REFERCING clause
	 * @param newReferencingName new referencing table name, if any, that appears in REFERCING clause
	 */
	public ConstantAction getCreateTriggerConstantAction
	(
		String				triggerSchemaName,
		String				triggerName,
		int					eventMask,
		boolean				isBefore,
		boolean 			isRow,
		boolean 			isEnabled,
		TableDescriptor		triggerTable,
		UUID				whenSPSId,
		String				whenText,
		UUID				actionSPSId,
		String				actionText,
		UUID				spsCompSchemaId,
		Timestamp			creationTimestamp,
		int[]				referencedCols,
		String				originalActionText,
		boolean				referencingOld,
		boolean				referencingNew,
		String				oldReferencingName,
		String				newReferencingName
	)
	{
		return new CreateTriggerConstantAction(triggerSchemaName, triggerName, 
				eventMask, isBefore, isRow, isEnabled, triggerTable, whenSPSId,
				whenText, actionSPSId, actionText, spsCompSchemaId, creationTimestamp,
				referencedCols, originalActionText,
				referencingOld, referencingNew, oldReferencingName, newReferencingName);
	}

	/**
	 * Make the ConstantAction for a DROP TRIGGER statement.
	 *
	 * @param	sd					Schema that stored prepared statement lives in.
	 * @param	triggerName			Name of the Trigger
	 * @param	tableId				The table this trigger is defined upon
	 */
	public ConstantAction getDropTriggerConstantAction
	(
		SchemaDescriptor	sd,
		String				triggerName,
		UUID				tableId
	)
	{
		return new DropTriggerConstantAction(sd, triggerName, tableId);
	}
	
	/**
	 * Make the constant action for a UPDATE STATISTICS statement.
	 *
	 * @param forTable		whether for an index or table.
	 * @param objectName	name of the object (either table or index) for which
	 * this statistic is being created.
	 * @param tableUUID		UUID of the table for which statistics are being
	 * created.
	 * @param objectUUID    array of UUID's, one for each index conglomerate for
	 * which statistics are being created. 
	 * @param conglomerateNumber array of conglomerate numbers, one for each
	 * index conglomerate for which statistics are being created.
	 * @param indexRow		array of index rows, one for each index. This row is
	 * used by the constant action to read data from the indices.
	 */
	public ConstantAction getUpdateStatisticsConstantAction
	(
	 boolean forTable,
	 String objectName,
	 UUID tableUUID,
	 UUID[] objectUUID,
	 long[] conglomerateNumber,
	 ExecIndexRow[] indexRow
	)
	{
		return new UpdateStatisticsConstantAction(forTable, 
												  objectName,
												  tableUUID,
												  objectUUID,
												  conglomerateNumber,
												  indexRow);
	}

	/**
	 * Make the constant action for Drop Statistics statement.
	 *
	 * @param sd			Schema Descriptor of the schema in which the object 
	 * resides. 
	 * @param fullTableName full name of the object for which statistics are
	 * being dropped.
	 * @param objectName	 object name for which statistics are being dropped.
	 * @param forTable 		 is it an index or table whose statistics aer being
	 * consigned to the garbage heap?
	 */
	public ConstantAction getDropStatisticsConstantAction
		(SchemaDescriptor sd, String fullTableName, String objectName, boolean forTable)
	{
		return new DropStatisticsConstantAction(sd, fullTableName, objectName, forTable);
	}
}
