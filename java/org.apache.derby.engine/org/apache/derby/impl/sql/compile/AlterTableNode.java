/*

   Derby - Class org.apache.derby.impl.sql.compile.AlterTableNode

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Limits;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.ConstraintConstantAction;
import org.apache.derby.impl.sql.execute.CreateConstraintConstantAction;

/**
 * A AlterTableNode represents a DDL statement that alters a table.
 * It contains the name of the object to be created.
 *
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class AlterTableNode extends DDLStatementNode
{
	// The alter table action
	public	TableElementList	tableElementList = null;
     char               lockGranularity;

	/**
	 * updateStatistics will indicate that we are here for updating the
	 * statistics. It could be statistics of just one index or all the
	 * indexes on a given table. 
	 */
	private	boolean				updateStatistics = false;
	/**
	 * The flag updateStatisticsAll will tell if we are going to update the 
	 * statistics of all indexes or just one index on a table. 
	 */
	private	boolean				updateStatisticsAll = false;
	/**
	 * dropStatistics will indicate that we are here for dropping the
	 * statistics. It could be statistics of just one index or all the
	 * indexes on a given table. 
	 */
	private	    boolean					    dropStatistics;
	/**
	 * The flag dropStatisticsAll will tell if we are going to drop the 
	 * statistics of all indexes or just one index on a table. 
	 */
	private	    boolean					    dropStatisticsAll;
	/**
	 * If statistic is getting updated/dropped for just one index, then 
	 * indexNameForStatistics will tell the name of the specific index 
	 * whose statistics need to be updated/dropped.
	 */
	private	String				indexNameForStatistics;
	
	public	boolean				compressTable = false;
	public	boolean				sequential = false;
	//The following three (purge, defragment and truncateEndOfTable) apply for 
	//inplace compress
	public	boolean				purge = false;
	public	boolean				defragment = false;
	public	boolean				truncateEndOfTable = false;
	
	public	int					behavior;	// currently for drop column

	public	TableDescriptor		baseTable;

	protected	int						numConstraints;

	private		int				changeType = UNKNOWN_TYPE;

	private boolean             truncateTable = false;

	// constant action arguments

	protected	SchemaDescriptor			schemaDescriptor = null;
	protected	ColumnInfo[] 				colInfos = null;
	protected	ConstraintConstantAction[]	conActions = null;


	/**
     * Constructor for TRUNCATE TABLE
	 *
     * @param tableName The name of the table being truncated
     * @param cm Context manager
     * @exception StandardException
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    AlterTableNode(TableName tableName,
                   ContextManager cm) throws StandardException {
        super(tableName, cm);
		truncateTable = true;
		schemaDescriptor = getSchemaDescriptor();
	}
	
	/**
     * Constructor  for COMPRESS using temporary tables
     * rather than in place compress
	 *
     * @param tableName The name of the table being altered
     * @param sequential Whether or not the COMPRESS is SEQUENTIAL
     * @param cm Context manager
	 *
	 * @exception StandardException		Thrown on error
	 */
    AlterTableNode(TableName tableName,
                   boolean sequential,
                   ContextManager cm) throws StandardException {
        super(tableName, cm);
        this.sequential = sequential;
		compressTable = true;
		schemaDescriptor = getSchemaDescriptor();
	}

	/**
     * Constructor for INPLACE COMPRESS
	 *
     * @param tableName The name of the table being altered
     * @param purge PURGE during INPLACE COMPRESS?
     * @param defragment DEFRAGMENT during INPLACE COMPRESS?
     * @param truncateEndOfTable TRUNCATE END during INPLACE COMPRESS?
     * @param cm Context manager
	 *
	 * @exception StandardException		Thrown on error
	 */
    AlterTableNode(TableName tableName,
                   boolean purge,
                   boolean defragment,
                   boolean truncateEndOfTable,
                   ContextManager cm) throws StandardException {
        super(tableName, cm);
        this.purge = purge;
        this.defragment = defragment;
        this.truncateEndOfTable = truncateEndOfTable;
		compressTable = true;
		schemaDescriptor = getSchemaDescriptor(true, false);
	}

    /**
     * Constructor for UPDATE_STATISTICS or DROP_STATISTICS
     *
     * @param tableName The name of the table being altered
     * @param changeType update or drop statistics
     * @param statsAll {@code true} means update or drop
     *        the statistics of all the indexes on the table.
     *        {@code false} means update or drop the statistics of
     *        only the index name provided by next parameter.
     * @param indexName Name of the index for which statistics is to be updated
     *                  or dropped
     * @param cm Context manager
     * @throws StandardException
     */
    AlterTableNode(
            TableName tableName,
            int changeType,
            boolean statsAll,
            String indexName,
            ContextManager cm) throws StandardException {
        super(tableName, cm);
        this.changeType = changeType;
        this.indexNameForStatistics = indexName;

        switch (changeType) {
            case UPDATE_STATISTICS:
                this.updateStatisticsAll = statsAll;
                updateStatistics = true;
                break;
            case DROP_STATISTICS:
                this.dropStatisticsAll = statsAll;
                dropStatistics = true;
                break;
            default:
                if (SanityManager.DEBUG) {
                    SanityManager.NOTREACHED();
                }
        }

        schemaDescriptor = getSchemaDescriptor();
    }

    /**
     * Constructor for ADD_TYPE, DROP_TYPE, MODIFY_TYPE and LOCK_TYPE
     * @param tableName  The name of the table being altered
     * @param changeType add, drop, modify or lock
     * @param impactedElements list of table elements impacted
     * @param lockGranularity lock granularity encoded in a single character
     * @param behavior cascade or restrict (for DROP_TYPE)
     * @param cm Context Manager
     * @throws StandardException
     */
    AlterTableNode(
            TableName tableName,
            int changeType,
            TableElementList impactedElements,
            char lockGranularity,
            int behavior,
            ContextManager cm) throws StandardException {
        super(tableName, cm);
        this.changeType = changeType;

        switch (changeType) {
            case ADD_TYPE:
		    case DROP_TYPE:
		    case MODIFY_TYPE:
		    case LOCKING_TYPE:
                this.tableElementList = impactedElements;
                this.lockGranularity = lockGranularity;
                this.behavior = behavior;
				break;
		    default:
                if (SanityManager.DEBUG) {
                    SanityManager.NOTREACHED();
                }
        }
        schemaDescriptor = getSchemaDescriptor();
    }

    /**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() +
//IC see: https://issues.apache.org/jira/browse/DERBY-4087
				"objectName: " + getObjectName() + "\n" +
				"lockGranularity: " + lockGranularity + "\n" +
				"compressTable: " + compressTable + "\n" +
				"sequential: " + sequential + "\n" +
				"truncateTable: " + truncateTable + "\n" +
				"purge: " + purge + "\n" +
				"defragment: " + defragment + "\n" +
				"truncateEndOfTable: " + truncateEndOfTable + "\n" +
				"updateStatistics: " + updateStatistics + "\n" +
				"updateStatisticsAll: " + updateStatisticsAll + "\n" +
				"dropStatistics: " + dropStatistics + "\n" +
				"dropStatisticsAll: " + dropStatisticsAll + "\n" +
				"indexNameForStatistics: " +
				indexNameForStatistics + "\n";
		}
		else
		{
			return "";
		}
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 * @param depth		The depth to indent the sub-nodes
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void printSubNodes(int depth) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4087
		if (SanityManager.DEBUG) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4397
//IC see: https://issues.apache.org/jira/browse/DERBY-4
			if (tableElementList != null) {
				printLabel(depth, "tableElementList: ");
				tableElementList.treePrint(depth + 1);
			}

		}
	}

public String statementToString()
	{
		if(truncateTable)
			return "TRUNCATE TABLE";
		else
			return "ALTER TABLE";
	}

	public	int	getChangeType() { return changeType; }

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this AlterTableNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 * For example, verifying that the user is not trying to add a 
	 * non-nullable column.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void bindStatement() throws StandardException
	{
		DataDictionary	dd = getDataDictionary();
		int					numCheckConstraints = 0;
		int numReferenceConstraints = 0;
        int numGenerationClauses = 0;
		int numBackingIndexes = 0;

		/*
		** Get the table descriptor.  Checks the schema
		** and the table.
		*/
		if(compressTable && (purge || defragment || truncateEndOfTable)) {
			//We are dealing with inplace compress here and inplace compress is 
			//allowed on system schemas. In order to support inplace compress
			//on user as well as system tables, we need to use special 
			//getTableDescriptor(boolean) call to get TableDescriptor. This
			//getTableDescriptor(boolean) allows getting TableDescriptor for
			//system tables without throwing an exception.
			baseTable = getTableDescriptor(false);
		} else
			baseTable = getTableDescriptor();

		//throw an exception if user is attempting to alter a temporary table
		if (baseTable.getTableType() == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
		{
				throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_DECLARED_GLOBAL_TEMP_TABLE);
		}

		/* Statement is dependent on the TableDescriptor */
		getCompilerContext().createDependency(baseTable);

		//If we are dealing with add column character type, then set that 
		//column's collation type to be the collation type of the schema.
		//The collation derivation of such a column would be "implicit".
//IC see: https://issues.apache.org/jira/browse/DERBY-2530
		if (changeType == ADD_TYPE) {//the action is of type add.
			if (tableElementList != null) {//check if is is add column
				for (int i=0; i<tableElementList.size();i++) {
					if (tableElementList.elementAt(i) instanceof ColumnDefinitionNode) {
						ColumnDefinitionNode cdn = (ColumnDefinitionNode) tableElementList.elementAt(i);
						//check if we are dealing with add character column
                        //
                        // For generated columns which omit an explicit
                        // datatype, we have to defer this work until we bind
                        // the generation clause
                        //

                        if ( cdn.hasGenerationClause() && ( cdn.getType() == null ) ) { continue; }
//IC see: https://issues.apache.org/jira/browse/DERBY-3923

                        if ( cdn.getType() == null )
                        {
                            throw StandardException.newException
                                ( SQLState.LANG_NEEDS_DATATYPE, cdn.getColumnName() );
                        }
                        
						if (cdn.getType().getTypeId().isStringTypeId()) {
							//we found what we are looking for. Set the 
							//collation type of this column to be the same as
							//schema descriptor's collation. Set the collation
							//derivation as implicit
							cdn.setCollationType(schemaDescriptor.getCollationType());
			        	}						
					}
				}
				
			}
		}
		if (tableElementList != null)
		{
			tableElementList.validate(this, dd, baseTable);

			/* Only 1012 columns allowed per table */
//IC see: https://issues.apache.org/jira/browse/DERBY-104
			if ((tableElementList.countNumberOfColumns() + baseTable.getNumberOfColumns()) > Limits.DB2_MAX_COLUMNS_IN_TABLE)
			{
				throw StandardException.newException(SQLState.LANG_TOO_MANY_COLUMNS_IN_TABLE_OR_VIEW,
					String.valueOf(tableElementList.countNumberOfColumns() + baseTable.getNumberOfColumns()),
					getRelativeName(),
					String.valueOf(Limits.DB2_MAX_COLUMNS_IN_TABLE));
			}
			/* Number of backing indexes in the alter table statment */
			numBackingIndexes = tableElementList.countConstraints(DataDictionary.PRIMARYKEY_CONSTRAINT) +
									tableElementList.countConstraints(DataDictionary.FOREIGNKEY_CONSTRAINT) +
									tableElementList.countConstraints(DataDictionary.UNIQUE_CONSTRAINT);
			/* Check the validity of all check constraints */
			numCheckConstraints = tableElementList.countConstraints(
									DataDictionary.CHECK_CONSTRAINT);
            
            numReferenceConstraints = tableElementList.countConstraints(
									DataDictionary.FOREIGNKEY_CONSTRAINT);
            
            numGenerationClauses = tableElementList.countGenerationClauses();
		}

		//If the sum of backing indexes for constraints in alter table statement and total number of indexes on the table
		//so far is more than 32767, then we need to throw an exception 
//IC see: https://issues.apache.org/jira/browse/DERBY-104
		if ((numBackingIndexes + baseTable.getTotalNumberOfIndexes()) > Limits.DB2_MAX_INDEXES_ON_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEXES_ON_TABLE, 
				String.valueOf(numBackingIndexes + baseTable.getTotalNumberOfIndexes()),
				getRelativeName(),
				String.valueOf(Limits.DB2_MAX_INDEXES_ON_TABLE));
		}

		if ( (numCheckConstraints > 0) || (numGenerationClauses > 0) || (numReferenceConstraints > 0))
		{
			/* In order to check the validity of the check constraints and
			 * generation clauses
			 * we must goober up a FromList containing a single table, 
			 * the table being alter, with an RCL containing the existing and
			 * new columns and their types.  This will allow us to
			 * bind the constraint definition trees against that
			 * FromList.  When doing this, we verify that there are
			 * no nodes which can return non-deterministic results.
			 */
			FromList fromList = makeFromList( dd, tableElementList, false );
            FormatableBitSet    generatedColumns = baseTable.makeColumnMap( baseTable.getGeneratedColumns() );

			/* Now that we've finally goobered stuff up, bind and validate
			 * the check constraints and generation clauses.
			 */
			if  (numGenerationClauses > 0)
//IC see: https://issues.apache.org/jira/browse/DERBY-4145
            { tableElementList.bindAndValidateGenerationClauses( schemaDescriptor, fromList, generatedColumns, baseTable ); }
			if  (numCheckConstraints > 0) { tableElementList.bindAndValidateCheckConstraints(fromList); }
            if ( numReferenceConstraints > 0) { tableElementList.validateForeignKeysOnGenerationClauses( fromList, generatedColumns ); }
		}

        // must be done after resolving the datatypes of the generation clauses
        if (tableElementList != null) { tableElementList.validatePrimaryKeyNullability(); }

		//Check if we are in alter table to update/drop the statistics. If yes,
		// then check if we are here to update/drop the statistics of a specific
		// index. If yes, then verify that the indexname provided is a valid one.
		if ((updateStatistics && !updateStatisticsAll) || (dropStatistics && !dropStatisticsAll))
		{
			ConglomerateDescriptor	cd = null;
			if (schemaDescriptor.getUUID() != null) 
				cd = dd.getConglomerateDescriptor(indexNameForStatistics, schemaDescriptor, false);

			if (cd == null)
			{
				throw StandardException.newException(
						SQLState.LANG_INDEX_NOT_FOUND, 
						schemaDescriptor.getSchemaName() + "." + indexNameForStatistics);
			}			
		}

		/* Unlike most other DDL, we will make this ALTER TABLE statement
		 * dependent on the table being altered.  In general, we try to
		 * avoid this for DDL, but we are already requiring the table to
		 * exist at bind time (not required for create index) and we don't
		 * want the column ids to change out from under us before
		 * execution.
		 */
		getCompilerContext().createDependency(baseTable);
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public boolean referencesSessionSchema()
		throws StandardException
	{
		//If alter table is on a SESSION schema table, then return true. 
		return isSessionSchema(baseTable.getSchemaName());
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
    public ConstantAction makeConstantAction() throws StandardException
	{
		prepConstantAction();

		return	getGenericConstantActionFactory().getAlterTableConstantAction(schemaDescriptor,
											 getRelativeName(),
											 baseTable.getUUID(),
											 baseTable.getHeapConglomerateId(),
											 TableDescriptor.BASE_TABLE_TYPE,
											 colInfos,
											 conActions,
											 lockGranularity,
											 compressTable,
											 behavior,
        								     sequential,
 										     truncateTable,
 										     purge,
 										     defragment,
 										     truncateEndOfTable,
 										     updateStatistics,
 										     updateStatisticsAll,
 										     dropStatistics,
 										     dropStatisticsAll,
 										     indexNameForStatistics);
	}

	/**
	  *	Generate arguments to constant action. Called by makeConstantAction() in this class and in
	  *	our subclass RepAlterTableNode.
	  *
	  *
	  * @exception StandardException		Thrown on failure
	  */
	private void	prepConstantAction() throws StandardException
	{
		if (tableElementList != null)
		{
			genColumnInfo();
		}

		/* If we've seen a constraint, then build a constraint list */

		if (numConstraints > 0)
		{
			conActions = new ConstraintConstantAction[numConstraints];

//IC see: https://issues.apache.org/jira/browse/DERBY-3008
			tableElementList.genConstraintActions(false, conActions, getRelativeName(), schemaDescriptor,
												  getDataDictionary());

//IC see: https://issues.apache.org/jira/browse/DERBY-4244
			for (int conIndex = 0; conIndex < conActions.length; conIndex++)
			{
				ConstraintConstantAction cca = conActions[conIndex];

				if (cca instanceof CreateConstraintConstantAction)
				{
					int constraintType = cca.getConstraintType();
					if (constraintType == DataDictionary.PRIMARYKEY_CONSTRAINT)
					{
						DataDictionary dd = getDataDictionary();
						// Check to see if a constraint of the same type 
						// already exists
						ConstraintDescriptorList cdl = 
                                dd.getConstraintDescriptors(baseTable);

						if (cdl.getPrimaryKey() != null)
						{
							throw StandardException.newException(
                                    SQLState.LANG_ADD_PRIMARY_KEY_FAILED1, 
                                    baseTable.getQualifiedName());
						}
					}
				}
			}
		}
	}
	  
	/**
	  *	Generate the ColumnInfo argument for the constant action. Return the number of constraints.
	  */
	public	void	genColumnInfo()
        throws StandardException
	{
		// for each column, stuff system.column
		colInfos = new ColumnInfo[tableElementList.countNumberOfColumns()]; 

	    numConstraints = tableElementList.genColumnInfos(colInfos);
	}


	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
    @Override
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

		if (tableElementList != null)
		{
			tableElementList.accept(v);
		}
	}

	/*
	 * class interface
	 */
}




