/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericConstantActionFactory

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.ResultDescription;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.AliasInfo;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.impl.sql.compile.TableName;

import java.util.List;
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
	 * Get ConstantAction for SET CONSTRAINTS statement.
	 *
     *  @param constraints  The constraints to set, if null,
     *                      set them ALL.
     *  @param initiallyDeferred   ncodes IMMEDIATE (false), DEFERRED (true)
	 */
    public  ConstantAction getSetConstraintsConstantAction(
            List<TableName> constraints,
            boolean         initiallyDeferred) {
        return new SetConstraintsConstantAction(constraints, initiallyDeferred);
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
	 * @param lockGranularity	The lock granularity.
	 *	@param compressTable	Whether or not this is a compress table
	 *	@param behavior			drop behavior of dropping column
	 *	@param sequential	If compress table/drop column, whether or not sequential
	 *  @param truncateTable	    Whether or not this is a truncate table
	 *  @param purge				PURGE during INPLACE COMPRESS?
	 *  @param defragment			DEFRAGMENT during INPLACE COMPRESS?
	 *  @param truncateEndOfTable	TRUNCATE END during INPLACE COMPRESS?
	 *  @param updateStatistics		TRUE means we are here to update statistics
	 *  @param updateStatisticsAll	TRUE means we are here to update statistics
	 *  	of all the indexes. False means we are here to update statistics of
	 *  	only one index.
	 *  @param dropStatistics		TRUE means we are here to drop statistics
	 *  @param dropStatisticsAll	TRUE means we are here to drop statistics
	 *  	of all the indexes. False means we are here to drop statistics of
	 *  	only one index.
	 *  @param indexNameForStatistics	Will name the index whose statistics
	 *  	will be updated/dropped. This param is looked at only if 
	 *  	updateStatisticsAll/dropStatisticsAll is set to false and
	 *  	updateStatistics/dropStatistics is set to true.
	 *  .
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

		char						lockGranularity,
		boolean						compressTable,
		int							behavior,
		boolean						sequential,
		boolean                     truncateTable,
		boolean						purge,
		boolean						defragment,
		boolean						truncateEndOfTable,
		boolean						updateStatistics,
		boolean						updateStatisticsAll,
		boolean						dropStatistics,
		boolean						dropStatisticsAll,
		String						indexNameForStatistics
    )
	{
		return new	AlterTableConstantAction( sd, tableName, tableId, tableConglomerateId, 
											  tableType, columnInfo, constraintActions, 
											  lockGranularity, compressTable,
											  behavior, sequential, truncateTable,
											  purge, defragment, truncateEndOfTable,
											  updateStatistics, 
											  updateStatisticsAll,
											  dropStatistics, 
											  dropStatisticsAll,
											  indexNameForStatistics);
	}

	/**
	 *	Make a ConstantAction for a constraint.
	 *
	 *  @param constraintName	Constraint name.
	 *  @param constraintType	Constraint type.
     *  @param constraintCharacteristics
     *                          Constraint characteristics, see {@link
     *  org.apache.derby.impl.sql.compile.ConstraintDefinitionNode#characteristics}
     *  @param forCreateTable   True if for a CREATE TABLE
	 *  @param tableName		Table name.
	 *	@param tableId			UUID of table.
	 *  @param schemaName		Schema that table lives in.
	 *  @param columnNames		String[] for column names
	 *  @param indexAction		IndexConstantAction for constraint (if necessary)
	 *  @param constraintText	Text for check constraint
	 *	@param otherConstraint	The referenced constraint, if a foreign key constraint
	 *  @param providerInfo Information on all the Providers
	 */
	public	CreateConstraintConstantAction	getCreateConstraintConstantAction
	(
		String				constraintName,
		int					constraintType,
        boolean[]           constraintCharacteristics,
        boolean             forCreateTable,
		String				tableName,
		UUID				tableId,
		String				schemaName,
		String[]			columnNames,
		IndexConstantAction indexAction,
		String				constraintText,
		ConstraintInfo		otherConstraint,
		ProviderInfo[]		providerInfo
	)
	{
        return new CreateConstraintConstantAction(
                constraintName,
                constraintType,
                constraintCharacteristics,
                forCreateTable,
                tableName,
                tableId,
                schemaName,
                columnNames,
                indexAction,
                constraintText,
                otherConstraint,
                providerInfo );
	}


	/**
     * 	Make the ConstantAction for a CREATE INDEX statement.
     * 
     * @param forCreateTable Executed as part of a CREATE TABLE
     * @param unique		True means it will be a unique index
     * @param uniqueWithDuplicateNulls  True means index check and disallow
     *                                  any duplicate key if key has no 
     *                                  column with a null value.  If any 
     *                                  column in the key has a null value,
     *                                  no checking is done and insert will
     *                                  always succeed.
     * @param hasDeferrableChecking True if the index is used to back a
     *                              deferrable constraint
     * @param initiallyDeferred  True means the deferrable constraint has
     *                           deferred mode initially.
     * @param indexType	The type of index (BTREE, for example)
     * @param schemaName			the schema that table (and index) lives in.
     * @param indexName	Name of the index
     * @param tableName	Name of table the index will be on
     * @param tableId		UUID of table.
     * @param columnNames	Names of the columns in the index, in order
     * @param isAscending	Array of booleans telling asc/desc on each column
     * @param isConstraint	TRUE if index is backing up a constraint, else FALSE
     * @param conglomerateUUID	ID of conglomerate
     * @param properties	The optional properties list associated with the index.
     */
    public IndexConstantAction getCreateIndexConstantAction
	(
        boolean forCreateTable,
		boolean			unique,
		boolean			uniqueWithDuplicateNulls,
        boolean         hasDeferrableChecking,
        boolean         initiallyDeferred,
		String			indexType,
		String			schemaName,
		String			indexName,
		String			tableName,
		UUID			tableId,
		String[]		columnNames,
		boolean[]		isAscending,
		boolean			isConstraint,
		UUID			conglomerateUUID,
		Properties		properties
    )
	{
        return new CreateIndexConstantAction(
            forCreateTable,
            unique,
            uniqueWithDuplicateNulls,
            hasDeferrableChecking,
            initiallyDeferred,
            indexType,
            schemaName,
            indexName,
            tableName,
            tableId,
            columnNames,
            isAscending,
            isConstraint,
            conglomerateUUID,
            properties);
	}


	/**
	 *	Make the ConstantAction for a CREATE ALIAS statement.
	 *
	 *  @param aliasName		Name of alias.
	 *  @param schemaName		Alias's schema. 
	 *  @param javaClassName	Name of java class.
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
	 * Make the ConstantAction for a CREATE SCHEMA statement.
	 *
	 *  @param schemaName	Name of table.
	 *  @param aid			Authorizaton id
	 */
	public	ConstantAction	getCreateSchemaConstantAction
	(
		String			schemaName,
		String			aid)
	{
		return new CreateSchemaConstantAction(schemaName, aid);
	}


    /**
	 * Make the ConstantAction for a CREATE ROLE statement.
	 *
	 * @param roleName	Name of role.
	 */
	public	ConstantAction	getCreateRoleConstantAction(String roleName)
	{
		return new CreateRoleConstantAction(roleName);
	}


	/**
	 * Make the ConstantAction for a SET ROLE statement.
	 *
	 * @param roleName  Name of role
	 * @param type      Literal (== 0)
	 *                  or ?    (== StatementType.SET_ROLE_DYNAMIC)
	 */
	public ConstantAction getSetRoleConstantAction(String roleName,
												   int type)
	{
		return new SetRoleConstantAction(roleName, type);
	}

    /**
	 * Make the ConstantAction for a CREATE SEQUENCE statement.
	 *
	 * @param sequenceName	Name of sequence.
     * @param dataType
     * @param initialValue
     * @param stepValue
     * @param maxValue
     * @param minValue
     * @param cycle
	 */
	public	ConstantAction	getCreateSequenceConstantAction
    (
            TableName   sequenceName,
            DataTypeDescriptor dataType,
            long        initialValue,
            long        stepValue,
            long        maxValue,
            long        minValue,
            boolean     cycle
    )
	{
        return new CreateSequenceConstantAction(sequenceName.getSchemaName(),
                sequenceName.getTableName(),
                dataType,
                initialValue,
                stepValue,
                maxValue,
                minValue,
                cycle);
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
	 *  @param schemaName	Name of the schema that table lives in.
	 *  @param tableName	Name of table.
	 *  @param tableType	Type of table (in this case TableDescriptor.VIEW_TYPE).
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
	 *	@param deferred				True means deferred delete
	 *  @param tableIsPublished		true if table is published
	 *  @param tableID				table id
	 *	@param lockMode				The lock mode to use
	 *								  (row or table, see TransactionController)
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
	 *  @param underMerge   True if this is an action of a MERGE statement.
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
								ConstantAction[]	dependentConstantActions,
								boolean				underMerge
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
										dependentConstantActions,
										underMerge
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
     *  Make ConstantAction to drop a constraint.
     *
     *  @param constraintName   Constraint name.
     *  @param constraintSchemaName     Constraint Schema Name
     *  @param characteristics  The presumably altered characteristics
     *  @param tableName        Table name.
     *  @param tableId          UUID of table.
     *  @param tableSchemaName  The schema that table lives in.
     *  @param indexAction      IndexConstantAction for constraint (if necessary)
     */
    public  ConstraintConstantAction    getAlterConstraintConstantAction
    (
        String                  constraintName,
        String                  constraintSchemaName,
        boolean[]               characteristics,
        String                  tableName,
        UUID                    tableId,
        String                  tableSchemaName,
        IndexConstantAction     indexAction
    )
    {
        return  new AlterConstraintConstantAction(
                constraintName,
                constraintSchemaName,
                characteristics,
                tableName,
                tableId,
                tableSchemaName,
                indexAction);
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
    public IndexConstantAction getDropIndexConstantAction
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
	 *	Make the ConstantAction for a DROP ROLE statement.
	 *
	 *	@param	roleName			role name to be dropped
	 *
	 */
	public ConstantAction getDropRoleConstantAction(String roleName)
	{
		return new DropRoleConstantAction(roleName);
	}

    /**
	 *	Make the ConstantAction for a DROP SEQUENCE statement.
	 *
     *  @param sd the schema the sequence object belongs to
	 *	@param	seqName	name of sequence to be dropped
	 *
	 */
	public ConstantAction getDropSequenceConstantAction(SchemaDescriptor sd, String seqName)
	{
		return new DropSequenceConstantAction(sd, seqName);
	}


    /**
	 *	Make the ConstantAction for a DROP SCHEMA statement.
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
	 *  @param  conglomerateNumber	Conglomerate number for heap
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
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *							if any (may be null)
	 *  @param indexedCols		boolean[] of which (0-based) columns are indexed.
	 *  @param dependencyId		UUID for dependency system
	 *	@param stageControl		Stage Control Tokens
	 *	@param ddlList			List of DDL to log. This is for BULK INSERT into a published table at the Source.
	 *  @param singleRowSource	Whether or not source is a single row source
	 *  @param autoincRowLocation array of row locations into syscolumns for
	                              autoincrement columns
	 *  @param underMerge   True if this is an INSERT action of a MERGE statement.
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
								RowLocation[]		autoincRowLocation,
								boolean		underMerge
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
										autoincRowLocation,
										underMerge
										);
	}

	/**
	 *	Make the ConstantAction for an updatable VTI statement.
	 *
     * @param statementType             Statement type, cf.
     * {@link org.apache.derby.vti.DeferModification#INSERT_STATEMENT} etc.
     * @param deferred                  Deferred processing mode?
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
     * @param statementType    Statement type, cf.
     * {@link org.apache.derby.vti.DeferModification#INSERT_STATEMENT} etc.
     * @param deferred         Deferred processing mode?
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
	 *	@param deferred				True means deferred update
	 *	@param targetUUID			UUID of target table
	 *	@param lockMode				The lock mode to use
	 *								  (row or table, see TransactionController)
	 *  @param tableIsPublished		true if table is published, false otherwise
	 *	@param changedColumnIds		Array of ids of changes columns
	 *  @param keyPositions     	positions of primary key columns in base row
	 *	@param fkInfo				Array of structures containing foreign key info, 
	 *								if any (may be null)
	 *	@param triggerInfo			Array of structures containing trigger info, 
	 *  @param baseRowReadList      Map of columns read in.  1 based.
	 *  @param baseRowReadMap		map of columns to be selected from the base row
	 *								(partial row). 1 based.
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *  @param numColumns			The number of columns being read.
	 *	@param positionedUpdate		is this a positioned update
	 *  @param singleRowSource		Whether or not source is a single row source
	 *  @param underMerge   True if this is an action of a MERGE statement.
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
								boolean				singleRowSource,
								boolean				underMerge
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
										singleRowSource,
										underMerge
										);
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
	 * @param triggerSchemaName		Name of the schema that trigger lives in.
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
	 * @param referencedColsInTriggerAction	what columns does the trigger 
	 *						action reference through old/new transition variables
	 *						(may be null)
     * @param originalWhenText The original user text of the WHEN clause (may be null)
	 * @param originalActionText The original user text of the trigger action
	 * @param referencingOld whether or not OLD appears in REFERENCING clause
	 * @param referencingNew whether or not NEW appears in REFERENCING clause
	 * @param oldReferencingName old referencing table name, if any, that appears in REFERCING clause
	 * @param newReferencingName new referencing table name, if any, that appears in REFERCING clause
     * @param providerInfo array of providers that the trigger depends on
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
		int[]				referencedColsInTriggerAction,
        String              originalWhenText,
		String				originalActionText,
		boolean				referencingOld,
		boolean				referencingNew,
		String				oldReferencingName,
        String              newReferencingName,
        ProviderInfo[]      providerInfo
	)
	{
		return new CreateTriggerConstantAction(triggerSchemaName, triggerName, 
				eventMask, isBefore, isRow, isEnabled, triggerTable, whenSPSId,
				whenText, actionSPSId, actionText, spsCompSchemaId, creationTimestamp,
                referencedCols, referencedColsInTriggerAction,
                originalWhenText, originalActionText,
                referencingOld, referencingNew,
                oldReferencingName, newReferencingName, providerInfo);
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

	/**
	 * Make the constant action for a Grant statement
	 *
	 * @param privileges The list of privileges to be granted
	 * @param grantees The list of grantees
	 */
	public ConstantAction getGrantConstantAction( PrivilegeInfo privileges,
								List grantees)
	{
		return new GrantRevokeConstantAction( true, privileges, grantees);
	}


    /**
	 * Make the ConstantAction for a GRANT role statement.
	 *
	 * @param roleNames list of roles to be granted
	 * @param grantees  list of authentication ids (user or roles) to
	 *                  which roles(s) are to be granted
	 */
	public ConstantAction getGrantRoleConstantAction(List roleNames,
													 List grantees)
	{
		return new GrantRoleConstantAction(roleNames, grantees);
	}


	/**
	 * Make the constant action for a Revoke statement
	 * 
	 * @param privileges The list of privileges to be revokeed
	 * @param grantees The list of grantees
	 */
	public ConstantAction getRevokeConstantAction( PrivilegeInfo privileges,
								List grantees)
	{
		return new GrantRevokeConstantAction( false, privileges, grantees);
	}


    /**
	 * Make the ConstantAction for a REVOKE role statement.
	 *
	 * @param roleNames list of roles to be revoked
	 * @param grantees  list of authentication ids (user or roles) for whom
	 *                  roles are to be revoked
	 */
	public ConstantAction getRevokeRoleConstantAction(List roleNames,
													  List grantees)
	{
		return new RevokeRoleConstantAction(roleNames, grantees);
	}

	/**
	 * Make the ConstantAction for a WHEN [ NOT ] MATCHED clause.
	 */
	public	ConstantAction	getMatchingClauseConstantAction
	(
         int    clauseType,
         String matchRefinementName,
         ResultDescription  thenColumnSignature,
         String rowMakingMethodName,
         int[]  thenColumns,
         String resultSetFieldName,
         String actionMethodName,
         ConstantAction thenAction
     )
	{
		return new MatchingClauseConstantAction
            (
             clauseType,
             matchRefinementName,
             thenColumnSignature,
             rowMakingMethodName,
             thenColumns,
             resultSetFieldName,
             actionMethodName,
             thenAction
             );
	}

	/**
	 * Make the ConstantAction for a MERGE statement.
	 */
	public	MergeConstantAction	getMergeConstantAction
        (
         ConstantAction[] matchingClauses
         )
	{
		return new MergeConstantAction( matchingClauses );
	}

}
