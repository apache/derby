/*

   Derby - Class org.apache.derby.impl.sql.compile.AlterTableNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.DB2Limit;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.ConstraintConstantAction;

/**
 * A AlterTableNode represents a DDL statement that alters a table.
 * It contains the name of the object to be created.
 *
 * @author Jerry Brenner
 */

public class AlterTableNode extends DDLStatementNode
{
	// The alter table action
	public	TableElementList	tableElementList = null;
	public  char				lockGranularity;
	public	boolean				compressTable = false;
	public	boolean				sequential = false;
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
	 * Initializer for a TRUNCATE TABLE
	 *
	 * @param objectName		The name of the table being truncated
	 * @exception StandardException		Thrown on error
	 */

	public void init(Object objectName)
		throws StandardException
	{

		//truncate table is not suppotted in this release
		//semantics are not yet clearly defined by SQL Council yet
		//truncate will be allowed only in DEBUG builds for testing purposes.
		if (SanityManager.DEBUG)
		{
			initAndCheck(objectName);
			/* For now, this init() only called for truncate table */
			truncateTable = true;
			schemaDescriptor = getSchemaDescriptor();
		}else
		{
			throw StandardException.newException(SQLState.NOT_IMPLEMENTED,
												 "truncate table");
		}
	}

	/**
	 * Initializer for a AlterTableNode for COMPRESS
	 *
	 * @param objectName		The name of the table being altered
	 * @param sequential		Whether or not the COMPRESS is SEQUENTIAL
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(Object objectName,
					 Object sequential)
		throws StandardException
	{
		initAndCheck(objectName);

		this.sequential = ((Boolean) sequential).booleanValue();
		/* For now, this init() only called for compress table */
		compressTable = true;

		schemaDescriptor = getSchemaDescriptor();
	}

	/**
	 * Initializer for a AlterTableNode
	 *
	 * @param objectName		The name of the table being altered
	 * @param tableElementList	The alter table action
	 * @param lockGranularity	The new lock granularity, if any
	 * @param changeType		ADD_TYPE or DROP_TYPE
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
							Object objectName,
							Object tableElementList,
							Object lockGranularity,
							Object changeType,
							Object behavior,
							Object sequential )
		throws StandardException
	{
		initAndCheck(objectName);
		this.tableElementList = (TableElementList) tableElementList;
		this.lockGranularity = ((Character) lockGranularity).charValue();

		int[]	ct = (int[]) changeType, bh = (int[]) behavior;
		this.changeType = ct[0];
		this.behavior = bh[0];
		boolean[]	seq = (boolean[]) sequential;
		this.sequential = seq[0];
		switch ( this.changeType )
		{
		    case ADD_TYPE:
		    case DROP_TYPE:
		    case MODIFY_TYPE:
		    case LOCKING_TYPE:

				break;

		    default:

				throw StandardException.newException(SQLState.NOT_IMPLEMENTED);
		}

		schemaDescriptor = getSchemaDescriptor();
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() +
				"objectName: " + "\n" + getObjectName() + "\n" +
				"tableElementList: " + "\n" + tableElementList + "\n" +
				"lockGranularity: " + "\n" + lockGranularity + "\n" +
				"compressTable: " + "\n" + compressTable + "\n" +
				"sequential: " + "\n" + sequential + "\n" +
				"truncateTable: " + "\n" + truncateTable + "\n";
		}
		else
		{
			return "";
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
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode bind() throws StandardException
	{
		DataDictionary	dd = getDataDictionary();
		int					numCheckConstraints = 0;
		int numBackingIndexes = 0;

		/*
		** Get the table descriptor.  Checks the schema
		** and the table.
		*/
		baseTable = getTableDescriptor();
		//throw an exception if user is attempting to alter a temporary table
		if (baseTable.getTableType() == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
		{
				throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_DECLARED_GLOBAL_TEMP_TABLE);
		}

		/* Statement is dependent on the TableDescriptor */
		getCompilerContext().createDependency(baseTable);

		if (tableElementList != null)
		{
			tableElementList.validate(this, dd, baseTable);

			/* Only 1012 columns allowed per table */
			if ((tableElementList.countNumberOfColumns() + baseTable.getNumberOfColumns()) > DB2Limit.DB2_MAX_COLUMNS_IN_TABLE)
			{
				throw StandardException.newException(SQLState.LANG_TOO_MANY_COLUMNS_IN_TABLE_OR_VIEW,
					String.valueOf(tableElementList.countNumberOfColumns() + baseTable.getNumberOfColumns()),
					getRelativeName(),
					String.valueOf(DB2Limit.DB2_MAX_COLUMNS_IN_TABLE));
			}
			/* Number of backing indexes in the alter table statment */
			numBackingIndexes = tableElementList.countConstraints(DataDictionary.PRIMARYKEY_CONSTRAINT) +
									tableElementList.countConstraints(DataDictionary.FOREIGNKEY_CONSTRAINT) +
									tableElementList.countConstraints(DataDictionary.UNIQUE_CONSTRAINT);
			/* Check the validity of all check constraints */
			numCheckConstraints = tableElementList.countConstraints(
									DataDictionary.CHECK_CONSTRAINT);
		}

		//If the sum of backing indexes for constraints in alter table statement and total number of indexes on the table
		//so far is more than 32767, then we need to throw an exception 
		if ((numBackingIndexes + baseTable.getTotalNumberOfIndexes()) > DB2Limit.DB2_MAX_INDEXES_ON_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEXES_ON_TABLE, 
				String.valueOf(numBackingIndexes + baseTable.getTotalNumberOfIndexes()),
				getRelativeName(),
				String.valueOf(DB2Limit.DB2_MAX_INDEXES_ON_TABLE));
		}

		if (numCheckConstraints > 0)
		{
			/* In order to check the validity of the check constraints
			 * we must goober up a FromList containing a single table, 
			 * the table being alter, with an RCL containing the existing and
			 * new columns and their types.  This will allow us to
			 * bind the constraint definition trees against that
			 * FromList.  When doing this, we verify that there are
			 * no nodes which can return non-deterministic results.
			 */
			FromList fromList = (FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager());
			FromBaseTable table = (FromBaseTable)
									getNodeFactory().getNode(
										C_NodeTypes.FROM_BASE_TABLE,
										getObjectName(),
										null,
										null,
										null,
										getContextManager());
			fromList.addFromTable(table);
			fromList.bindTables(dd,
							(FromList) getNodeFactory().getNode(
								C_NodeTypes.FROM_LIST,
								getNodeFactory().doJoinOrderOptimization(),
								getContextManager()));
			tableElementList.appendNewColumnsToRCL(table);

			/* Now that we've finally goobered stuff up, bind and validate
			 * the check constraints.
			 */
			tableElementList.bindAndValidateCheckConstraints(fromList);

		}

		/* Unlike most other DDL, we will make this ALTER TABLE statement
		 * dependent on the table being altered.  In general, we try to
		 * avoid this for DDL, but we are already requiring the table to
		 * exist at bind time (not required for create index) and we don't
		 * want the column ids to change out from under us before
		 * execution.
		 */
		getCompilerContext().createDependency(baseTable);

		return this;
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
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
	public ConstantAction	makeConstantAction() throws StandardException
	{
		prepConstantAction();

		return	getGenericConstantActionFactory().getAlterTableConstantAction(schemaDescriptor,
											 getRelativeName(),
											 baseTable.getUUID(),
											 baseTable.getHeapConglomerateId(),
											 TableDescriptor.BASE_TABLE_TYPE,
											 colInfos,
											 conActions,
											 null, null, null, null, null, null,
											 lockGranularity,
											 compressTable,
											 behavior,
        								     sequential,
 										     truncateTable);
	}

	/**
	  *	Generate arguments to constant action. Called by makeConstantAction() in this class and in
	  *	our subclass RepAlterTableNode.
	  *
	  *
	  * @exception StandardException		Thrown on failure
	  */
	public void	prepConstantAction() throws StandardException
	{
		if (tableElementList != null)
		{
			genColumnInfo();
		}

		/* If we've seen a constraint, then build a constraint list */

		if (numConstraints > 0)
		{
			conActions = new ConstraintConstantAction[numConstraints];

			tableElementList.genConstraintActions(conActions, getRelativeName(), schemaDescriptor,
												  getDataDictionary());
		}
	}
	  
	/**
	  *	Generate the ColumnInfo argument for the constant action. Return the number of constraints.
	  *
	  *	@return	number of constraints
	  */
	public	void	genColumnInfo()
	{
		// for each column, stuff system.column
		colInfos = new ColumnInfo[tableElementList.countNumberOfColumns()]; 

	    numConstraints = tableElementList.genColumnInfos(colInfos);
	}


	/*
	 * class interface
	 */
}




