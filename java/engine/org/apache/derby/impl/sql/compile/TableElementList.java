/*

   Derby - Class org.apache.derby.impl.sql.compile.TableElementList

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.depend.ProviderList;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.ConstraintInfo;
import org.apache.derby.impl.sql.execute.ConstraintConstantAction;
import org.apache.derby.impl.sql.execute.IndexConstantAction;

import	org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;

import org.apache.derby.catalog.UUID;

import java.util.Hashtable;
import java.util.Vector;

/**
 * A TableElementList represents the list of columns and other table elements
 * such as constraints in a CREATE TABLE or ALTER TABLE statement.
 *
 * @author Jeff Lichtman
 */

public class TableElementList extends QueryTreeNodeVector
{
	int				numColumns;
	TableDescriptor td;

	/**
	 * Add a TableElementNode to this TableElementList
	 *
	 * @param tableElement	The TableElementNode to add to this list
	 *
	 * @return	Nothing
	 */

	public void addTableElement(TableElementNode tableElement)
	{
		addElement(tableElement);
		if ((tableElement instanceof ColumnDefinitionNode) ||
			tableElement.getElementType() == TableElementNode.AT_DROP_COLUMN)
		{
			numColumns++;
		}
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
			StringBuffer	buffer = new StringBuffer("");

			for (int index = 0; index < size(); index++)
			{
				buffer.append(elementAt(index).toString()).append("\n");
			}

			return buffer.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Validate this TableElementList.  This includes checking for
	 * duplicate columns names, and checking that user types really exist.
	 *
	 * @param ddlStmt	DDLStatementNode which contains this list
	 * @param dd		DataDictionary to use
	 * @param td		TableDescriptor for table, if existing table.
	 *
	 * @return	None
	 *
	 * @exception StandardException		Thrown on error
	 */
	void validate(DDLStatementNode ddlStmt,
					     DataDictionary dd,
						 TableDescriptor td)
					throws StandardException
	{
 		this.td = td;
		int numAutoCols = 0;

		int			size = size();
		Hashtable	columnHT = new Hashtable(size + 2, (float) .999);
		Hashtable	constraintHT = new Hashtable(size + 2, (float) .999);
		//all the primary key/unique key constraints for this table
		Vector constraintsVector = new Vector();

		//special case for alter table (td is not null in case of alter table)
		if (td != null)
		{
			//In case of alter table, get the already existing primary key and unique
			//key constraints for this table. And then we will compare them with  new
			//primary key/unique key constraint column lists.
			ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
			ConstraintDescriptor cd;

			if (cdl != null) //table does have some pre-existing constraints defined on it
			{
				for (int i=0; i<cdl.size();i++)
				{
					cd = cdl.elementAt(i);
					//if the constraint type is not primary key or unique key, ignore it.
					if (cd.getConstraintType() == DataDictionary.PRIMARYKEY_CONSTRAINT ||
					cd.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT)
						constraintsVector.addElement(cd);
				}
			}
		}

		int tableType = TableDescriptor.BASE_TABLE_TYPE;
		if (ddlStmt instanceof CreateTableNode)
			tableType = ((CreateTableNode)ddlStmt).tableType;

		for (int index = 0; index < size; index++)
		{
			TableElementNode tableElement = (TableElementNode) elementAt(index);

			if (tableElement instanceof ColumnDefinitionNode)
			{
				ColumnDefinitionNode cdn = (ColumnDefinitionNode) elementAt(index);
				if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE &&
					(cdn.getDataTypeServices().getTypeId().isLongConcatableTypeId() ||
					cdn.getDataTypeServices().getTypeId().isUserDefinedTypeId()))
				{
					throw StandardException.newException(SQLState.LANG_LONG_DATA_TYPE_NOT_ALLOWED, cdn.getColumnName());
				}
				checkForDuplicateColumns(ddlStmt, columnHT, cdn.getColumnName());
				cdn.checkUserType(td);
				cdn.bindAndValidateDefault(dd, td);
				
				cdn.validateAutoincrement(dd, td, tableType);

				if (tableElement instanceof ModifyColumnNode)
				{
					ModifyColumnNode mcdn = (ModifyColumnNode)cdn;
					mcdn.checkExistingConstraints(td);
				} else if (cdn.isAutoincrementColumn())
					numAutoCols ++;
			}
			else if (tableElement.getElementType() == TableElementNode.AT_DROP_COLUMN)
			{
				String colName = tableElement.getName();
				if (td.getColumnDescriptor(colName) == null)
				{
					throw StandardException.newException(
												SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,
												colName,
												td.getQualifiedName());
				}
				break;
			}

			/* The rest of this method deals with validating constraints */
			if (! (tableElement.hasConstraint()))
			{
				continue;
			}

			ConstraintDefinitionNode cdn = (ConstraintDefinitionNode) tableElement;

			cdn.bind(ddlStmt, dd);

			//if constraint is primary key or unique key, add it to the vector
			if (cdn.getConstraintType() == DataDictionary.PRIMARYKEY_CONSTRAINT ||
			cdn.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT)
			{
				/* In case of create table, the vector can have only ConstraintDefinitionNode
				* elements. In case of alter table, it can have both ConstraintDefinitionNode
				* (for new constraints) and ConstraintDescriptor(for pre-existing constraints).
				*/

				Object destConstraint;
				String destName = null;
				String[] destColumnNames = null;

				for (int i=0; i<constraintsVector.size();i++)
				{

					destConstraint = constraintsVector.elementAt(i);
					if (destConstraint instanceof ConstraintDefinitionNode)
					{
						ConstraintDefinitionNode destCDN = (ConstraintDefinitionNode)destConstraint;
						destName = destCDN.getConstraintMoniker();
						destColumnNames = destCDN.getColumnList().getColumnNames();
					}
					else if (destConstraint instanceof ConstraintDescriptor)
					{
						//will come here only for pre-existing constraints in case of alter table
						ConstraintDescriptor destCD = (ConstraintDescriptor)destConstraint;
						destName = destCD.getConstraintName();
						destColumnNames = destCD.getColumnDescriptors().getColumnNames();
					}
					//check if there are multiple constraints with same set of columns
					if (columnsMatch(cdn.getColumnList().getColumnNames(), destColumnNames))
						throw StandardException.newException(SQLState.LANG_MULTIPLE_CONSTRAINTS_WITH_SAME_COLUMNS,
						cdn.getConstraintMoniker(), destName);
				}
				constraintsVector.addElement(cdn);
			}

			/* Make sure that there are no duplicate constraint names in the list */
			if (cdn instanceof ConstraintDefinitionNode)
				checkForDuplicateConstraintNames(ddlStmt, constraintHT, cdn.getConstraintMoniker());

			/* Make sure that the constraint we are trying to drop exists */
			if (cdn.getConstraintType() == DataDictionary.DROP_CONSTRAINT)
			{
				/*
				** If no schema descriptor, then must be an invalid
				** schema name.
				*/

				String dropConstraintName = cdn.getConstraintMoniker();

				if (dropConstraintName != null) {

					String dropSchemaName = cdn.getDropSchemaName();

					SchemaDescriptor sd = dropSchemaName == null ? td.getSchemaDescriptor() : 
											getSchemaDescriptor(dropSchemaName);

					ConstraintDescriptor cd =
								dd.getConstraintDescriptorByName(
										td, sd, dropConstraintName,
										false);
					if (cd == null)
					{
						throw StandardException.newException(SQLState.LANG_DROP_NON_EXISTENT_CONSTRAINT,
								(sd.getSchemaName() + "."+ dropConstraintName),
								td.getQualifiedName());
					}
					/* Statement is dependendent on the ConstraintDescriptor */
					getCompilerContext().createDependency(cd);
				}
			}

			/* For primary/unique/unique keys, verify that the constraint's column
			 * list contains valid columns and does not contain any duplicates
			 * (Also, all columns in a primary key will be set to non-null,
				but only in Cloudscape mode. SQL and DB2 require explict NOT NULL.
			 */
			if (cdn.hasPrimaryKeyConstraint() ||
				cdn.hasForeignKeyConstraint() ||
				cdn.hasUniqueKeyConstraint())
			{
				verifyUniqueColumnList(ddlStmt, cdn);
				/* Raise error if primary or unique key columns can be nullable. */
				if (cdn.hasPrimaryKeyConstraint() || cdn.hasUniqueKeyConstraint())
				{
					setColumnListToNotNull(cdn, td);
				}
			}
		}

		/* Can have only one autoincrement column in DB2 mode */
		if (numAutoCols > 1)
			throw StandardException.newException(SQLState.LANG_MULTIPLE_AUTOINCREMENT_COLUMNS);

	}
	
	/**
	 * Count the number of constraints of the specified type.
	 *
	 * @param constraintType	The constraint type to search for.
	 *
	 * @return int	The number of constraints of the specified type.
	 */
	public int countConstraints(int constraintType)
	{
		int	numConstraints = 0;
		int size = size();

		for (int index = 0; index < size; index++)
		{
			ConstraintDefinitionNode cdn;
			TableElementNode element = (TableElementNode) elementAt(index);

			if (! (element instanceof ConstraintDefinitionNode))
			{
				continue;
			}

			cdn = (ConstraintDefinitionNode) element;

			if (constraintType == cdn.getConstraintType())
			{
				numConstraints++;
			}
		}

		return numConstraints;
	}

	/**
	 * Count the number of columns.
	 *
	 * @return int	The number of columns.
	 */
	public int countNumberOfColumns()
	{
		return numColumns;
	}

	/**
	 * Fill in the ColumnInfo[] for this table element list.
	 * 
	 * @param colInfos	The ColumnInfo[] to be filled in.
	 *
	 * @return int		The number of constraints in the create table.
	 */
	public int genColumnInfos(ColumnInfo[] colInfos)
	{
		int	numConstraints = 0;
		int size = size();

		for (int index = 0; index < size; index++)
		{
			if (((TableElementNode) elementAt(index)).getElementType() == TableElementNode.AT_DROP_COLUMN)
			{
				colInfos[index] = new ColumnInfo(
								((TableElementNode) elementAt(index)).getName(),
								null, null, null, null, null,
								ColumnInfo.DROP, 0, 0);
				break;
			}

			if (! (elementAt(index) instanceof ColumnDefinitionNode))
			{
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT( elementAt(index) instanceof ConstraintDefinitionNode,
						"elementAt(index) expected to be instanceof " +
						"ConstraintDefinitionNode");
				}

				/* Remember how many constraints that we've seen */
				numConstraints++;
				continue;
			}

			ColumnDefinitionNode coldef = (ColumnDefinitionNode) elementAt(index);

			colInfos[index - numConstraints] = 
				new ColumnInfo(coldef.getColumnName(),
							   coldef.getDataTypeServices(),
							   coldef.getDefaultValue(),
							   coldef.getDefaultInfo(),
							   (UUID) null,
							   coldef.getOldDefaultUUID(),
							   coldef.getAction(),
							   (coldef.isAutoincrementColumn() ? 
								coldef.getAutoincrementStart() : 0),
							   (coldef.isAutoincrementColumn() ? 
								coldef.getAutoincrementIncrement() : 0));

			/* Remember how many constraints that we've seen */
			if (coldef.hasConstraint())
			{
				numConstraints++;
			}
		}

		return numConstraints;
	}
	/**
	 * Append goobered up ResultColumns to the table's RCL.
	 * This is useful for binding check constraints for CREATE and ALTER TABLE.
	 *
	 * @param table		The table in question.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void appendNewColumnsToRCL(FromBaseTable table)
		throws StandardException
	{
		int				 size = size();
		ResultColumnList rcl = table.getResultColumns();
		TableName		 exposedName = table.getTableName();

		for (int index = 0; index < size; index++)
		{
			if (elementAt(index) instanceof ColumnDefinitionNode)
			{
				ColumnDefinitionNode cdn = (ColumnDefinitionNode) elementAt(index);
				ResultColumn	resultColumn;
				ValueNode		valueNode;

				/* Build a ResultColumn/BaseColumnNode pair for the column */
				valueNode = (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.BASE_COLUMN_NODE,
											cdn.getColumnName(),
									  		exposedName,
											cdn.getDataTypeServices(),
											getContextManager());

				resultColumn = (ResultColumn) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN,
												cdn.getDataTypeServices(), 
												valueNode,
												getContextManager());
				resultColumn.setName(cdn.getColumnName());
				rcl.addElement(resultColumn);
			}
		}
	}

	/**
	 * Bind and validate all of the check constraints in this list against
	 * the specified FromList.  
	 *
	 * @param FromList		The FromList in question.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void bindAndValidateCheckConstraints(FromList fromList)
		throws StandardException
	{
		CompilerContext cc;
		FromBaseTable				table = (FromBaseTable) fromList.elementAt(0);
		int						  size = size();

		cc = getCompilerContext();

		Vector aggregateVector = new Vector();

		for (int index = 0; index < size; index++)
		{
			ConstraintDefinitionNode cdn;
			TableElementNode element = (TableElementNode) elementAt(index);
			ValueNode	checkTree;

			if (! (element instanceof ConstraintDefinitionNode))
			{
				continue;
			}

			cdn = (ConstraintDefinitionNode) element;

			if (cdn.getConstraintType() != DataDictionary.CHECK_CONSTRAINT)
			{
				continue;
			}

			checkTree = cdn.getCheckCondition();

			// bind the check condition
			// verify that it evaluates to a boolean
			final int previousReliability = cc.getReliability();
			try
			{
				/* Each check constraint can have its own set of dependencies.
				 * These dependencies need to be shared with the prepared
				 * statement as well.  We create a new auxiliary provider list
				 * for the check constraint, "push" it on the compiler context
				 * by swapping it with the current auxiliary provider list
				 * and the "pop" it when we're done by restoring the old 
				 * auxiliary provider list.
				 */
				ProviderList apl = new ProviderList();

				ProviderList prevAPL = cc.getCurrentAuxiliaryProviderList();
				cc.setCurrentAuxiliaryProviderList(apl);

				// Tell the compiler context to only allow deterministic nodes
				cc.setReliability( CompilerContext.CHECK_CONSTRAINT );
				checkTree = checkTree.bindExpression(fromList, (SubqueryList) null,
										 aggregateVector);

				// no aggregates, please
				if (aggregateVector.size() != 0)
				{
					throw StandardException.newException(SQLState.LANG_INVALID_CHECK_CONSTRAINT, cdn.getConstraintText());
				}
				
				checkTree = checkTree.checkIsBoolean();
				cdn.setCheckCondition(checkTree);

				/* Save the APL off in the constraint node */
				if (apl.size() > 0)
				{
					cdn.setAuxiliaryProviderList(apl);
				}

				// Restore the previous AuxiliaryProviderList
				cc.setCurrentAuxiliaryProviderList(prevAPL);
			}
			finally
			{
				cc.setReliability(previousReliability);
			}
	
			/* We have a valid check constraint, now build an array of
			 * 1-based columnIds that the constraint references.
			 */
			ResultColumnList rcl = table.getResultColumns();
			int		numReferenced = rcl.countReferencedColumns();
			int[]	checkColumnReferences = new int[numReferenced];

			rcl.recordColumnReferences(checkColumnReferences, 1);
			cdn.setCheckColumnReferences(checkColumnReferences);

			/* Now we build a list with only the referenced columns and
			 * copy it to the cdn.  Thus we can build the array of
			 * column names for the referenced columns during generate().
			 */
			ResultColumnList refRCL =
						(ResultColumnList) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager());
			rcl.copyReferencedColumnsToNewList(refRCL);

			/* A column check constraint can only refer to that column. If this is a
			 * column constraint, we should have an RCL with that column
			 */
			if (cdn.getColumnList() != null)
			{
				String colName = ((ResultColumn)(cdn.getColumnList().elementAt(0))).getName();
				if (numReferenced > 1 ||
					!colName.equals(((ResultColumn)(refRCL.elementAt(0))).getName()))
					throw StandardException.newException(SQLState.LANG_DB2_INVALID_CHECK_CONSTRAINT, colName);
				
			}
			cdn.setColumnList(refRCL);

			/* Clear the column references in the RCL so each check constraint
			 * starts with a clean list.
			 */
			rcl.clearColumnReferences();
		}
	}

	/**
	 * Fill in the ConstraintConstantAction[] for this create/alter table.
	 * 
	 * @param conActions	The ConstraintConstantAction[] to be filled in.
	 * @param tableName		The name of the Table being created.
	 * @param sd			The schema for that table.
	 * @param dd		The DataDictionary
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	void genConstraintActions(
				ConstraintConstantAction[] conActions,
				String tableName,
				SchemaDescriptor tableSd,
				DataDictionary dd)
		throws StandardException
	{
		int size = size();
		int conActionIndex = 0;
		for (int index = 0; index < size; index++)
		{
			String[]	columnNames = null;
			String		generatedConstraintName;
			TableElementNode ten = (TableElementNode) elementAt(index);
			IndexConstantAction indexAction = null;

			if (! ten.hasConstraint())
			{
				continue;
			}

			if (ten instanceof ColumnDefinitionNode)
			{
				continue;
			}

			ConstraintDefinitionNode constraintDN = (ConstraintDefinitionNode) ten;

			if (constraintDN.getColumnList() != null)
			{
				columnNames = new String[constraintDN.getColumnList().size()];
				constraintDN.getColumnList().exportNames(columnNames);
			}

			int constraintType = constraintDN.getConstraintType();
			String constraintText = constraintDN.getConstraintText();

			/*
			** If the constraint is not named (e.g.
			** create table x (x int primary key)), then
			** the constraintSd is the same as the table.
			*/
			String constraintName = constraintDN.getConstraintMoniker();

			/* At execution time, we will generate a unique name for the backing
			 * index (for CREATE CONSTRAINT) and we will look up the conglomerate
			 * name (for DROP CONSTRAINT).
			 */
			if (constraintDN.requiresBackingIndex())
			{
				indexAction = genIndexAction(constraintDN.requiresUniqueIndex(),
											 null, constraintDN, 
											 columnNames, true, tableSd, tableName,
											 constraintType, dd);
			}

			if (constraintType == DataDictionary.DROP_CONSTRAINT)
			{
				conActions[conActionIndex] = 
					getGenericConstantActionFactory().
						getDropConstraintConstantAction(
												 constraintName, 
												 constraintDN.getDropSchemaName(), /// FiX
												 tableName,
												 td.getUUID(),
												 tableSd.getSchemaName(),
												 indexAction,
												 constraintDN.getDropBehavior(),
                                                 constraintDN.getVerifyType());
			}
			else
			{
				ProviderList apl = constraintDN.getAuxiliaryProviderList();
				ConstraintInfo refInfo = null;
				ProviderInfo[]	providerInfos = null;

				if (constraintDN instanceof FKConstraintDefinitionNode)
				{
					refInfo = ((FKConstraintDefinitionNode)constraintDN).getReferencedConstraintInfo();
				}				

				/* Create the ProviderInfos, if the constraint is dependent on any Providers */
				if (apl != null && apl.size() > 0)
				{
					/* Get all the dependencies for the current statement and transfer
					 * them to this view.
					 */
					DependencyManager dm = dd.getDependencyManager();
					providerInfos = dm.getPersistentProviderInfos(apl);
				}
				else
				{
					providerInfos = new ProviderInfo[0];
				}

				conActions[conActionIndex++] = 
					getGenericConstantActionFactory().
						getCreateConstraintConstantAction(
												 constraintName, 
											     constraintType,
												 tableName,
												 ((td != null) ? td.getUUID() : (UUID) null),
												 tableSd.getSchemaName(),
												 columnNames,
												 indexAction,
												 constraintText,
												 true, 		// enabled
												 refInfo,
												 providerInfos);
			}
		}
	}

      //check if one array is same as another 
	private boolean columnsMatch(String[] columnNames1, String[] columnNames2)
		throws StandardException
	{
		int srcCount, srcSize, destCount,destSize;
		boolean match = true;

		if (columnNames1.length != columnNames2.length)
			return false;

		srcSize = columnNames1.length;
		destSize = columnNames2.length;

		for (srcCount = 0; srcCount < srcSize; srcCount++)
		{
			match = false;
			for (destCount = 0; destCount < destSize; destCount++) {
				if (columnNames1[srcCount].equals(columnNames2[destCount])) {
					match = true;
					break;
				}
			}
			if (match == false)
				return false;
		}

		return true;
	}

	private IndexConstantAction genIndexAction(
										boolean	isUnique,
										String indexName,
										ConstraintDefinitionNode cdn,
										String[] columnNames,
										boolean isConstraint,
										SchemaDescriptor sd,
										String tableName,
										int constraintType,
										DataDictionary dd)
		throws StandardException
	{
		if ( indexName == null ) { indexName = cdn.getBackingIndexName(dd); }

		if (constraintType == DataDictionary.DROP_CONSTRAINT)
		{
			return	getGenericConstantActionFactory().getDropIndexConstantAction(
									  null,
									  indexName,
									  tableName,
									  sd.getSchemaName(),
									  td.getUUID(),
									  td.getHeapConglomerateId());
		}
		else
		{
			boolean[]	isAscending = new boolean[columnNames.length];
			for (int i = 0; i < isAscending.length; i++)
				isAscending[i] = true;
			return	getGenericConstantActionFactory().getCreateIndexConstantAction(
									isUnique,
									"BTREE", // indexType
									sd.getSchemaName(),
									indexName,
									tableName,
									((td != null) ? td.getUUID() : (UUID) null),
									0, // conglomId
									columnNames,
									isAscending,
									isConstraint,
									cdn.getBackingIndexUUID(),
									cdn.getProperties());
		}
	}

	/**
	 * Check to make sure that there are no duplicate column names
	 * in the list.  (The comparison here is case sensitive.
	 * The work of converting column names that are not quoted
	 * identifiers to upper case is handled by the parser.)
	 * RESOLVE: This check will also be performed by alter table.
	 *
	 * @param ddlStmt	DDLStatementNode which contains this list
	 * @param ht		Hashtable for enforcing uniqueness.
	 * @param colName	Column name to check for.
	 *
	 * @return	None
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void checkForDuplicateColumns(DDLStatementNode ddlStmt,
									Hashtable ht,
									String colName)
			throws StandardException
	{
		Object object = ht.put(colName, colName);
		if (object != null)
		{
			/* RESOLVE - different error messages for create and alter table */
			if (ddlStmt instanceof CreateTableNode)
			{
				throw StandardException.newException(SQLState.LANG_DUPLICATE_COLUMN_NAME_CREATE, colName);
			}
		}
	}

	/**
	 * Check to make sure that there are no duplicate constraint names
	 * in the list.  (The comparison here is case sensitive.
	 * The work of converting column names that are not quoted
	 * identifiers to upper case is handled by the parser.)
	 * RESOLVE: This check will also be performed by alter table.
	 *
	 * @param ddlStmt	DDLStatementNode which contains this list
	 * @param outer		The element to check against.  Only check
	 *						TableElements that come after this one, since this
	 *						one has been checked against the TableElements
	 *						before it.
	 *
	 * @return	None
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void checkForDuplicateConstraintNames(DDLStatementNode ddlStmt,
									Hashtable ht,
									String constraintName)
			throws StandardException
	{
		if (constraintName == null)
			return;

		Object object = ht.put(constraintName, constraintName);
		if (object != null) {

			/* RESOLVE - different error messages for create and alter table */
			if (ddlStmt instanceof CreateTableNode)
			{
				/* RESOLVE - new error message */
				throw StandardException.newException(SQLState.LANG_DUPLICATE_CONSTRAINT_NAME_CREATE, 
						constraintName);
			}
		}
	}

	/**
	 * Verify that a primary/unique table constraint has a valid column list.
	 * (All columns in table and no duplicates.)
	 *
	 * @param ddlStmt	The outer DDLStatementNode
	 * @param cdn		The ConstraintDefinitionNode
	 *
	 * @return Nothing.
	 *
	 * @exception	StandardException	Thrown if the column list is invalid
	 */
	private void verifyUniqueColumnList(DDLStatementNode ddlStmt,
								ConstraintDefinitionNode cdn)
		throws StandardException
	{
		String invalidColName;

		/* Verify that every column in the list appears in the table's list of columns */
		if (ddlStmt instanceof CreateTableNode)
		{
			invalidColName = cdn.getColumnList().verifyCreateConstraintColumnList(this);
			if (invalidColName != null)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_CREATE_CONSTRAINT_COLUMN_LIST, 
								ddlStmt.getRelativeName(),
								invalidColName);
			}
		}
		else
		{
			/* RESOLVE - alter table will need to get table descriptor */
		}

		/* Check the uniqueness of the column names within the list */
		invalidColName = cdn.getColumnList().verifyUniqueNames(false);
		if (invalidColName != null)
		{
			throw StandardException.newException(SQLState.LANG_DUPLICATE_CONSTRAINT_COLUMN_NAME, invalidColName);
		}
	}

	/**
	 * Set all columns in that appear in a primary/unique key constraint in a create
	 * table statement to NOT NULL in Cloudscape mode and raises an error in DB2 mode.
	 *
	 * @param cdn		The ConstraintDefinitionNode
	 * @param td		TableDescriptor for the table
	 *
	 * @return Nothing.
	 */
	private void setColumnListToNotNull(ConstraintDefinitionNode cdn, TableDescriptor td)
		throws StandardException
	{
		ResultColumnList rcl = cdn.getColumnList();
		int rclSize = rcl.size();
		for (int index = 0; index < rclSize; index++)
		{
			String colName = ((ResultColumn) rcl.elementAt(index)).getName();

			/* For ALTER TABLE ADD CONSTRAINT, make sure columns are not nullable for
			 * primary and unique constraints.
			 */
			if (td != null && cdn instanceof ConstraintDefinitionNode)
			{
				ColumnDescriptor cd = td.getColumnDescriptor(colName);
				if (cd != null && cd.getType().isNullable())
					throw StandardException.newException(SQLState.LANG_DB2_ADD_UNIQUE_OR_PRIMARY_KEY_ON_NULL_COLS, colName);
			}

			setColumnToNotNull(colName);
		}
	}

	/**
	 * Set a column that appears in a primary/unique key constraint in
	 * a create table statement to NOT NULL (but only in Cloudscape mode).
	 *
	 * @param colName	The column name
	 *
	 * @return Nothing.
	 */
	private void setColumnToNotNull(String colName) throws StandardException
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
			TableElementNode tableElement = (TableElementNode) elementAt(index);

			if (tableElement instanceof ColumnDefinitionNode)
			{
				ColumnDefinitionNode cdn = (ColumnDefinitionNode) tableElement;
				if (colName.equals(cdn.getColumnName()))
				{
					DataTypeDescriptor dtd = cdn.getDataTypeServices();

					if (dtd.isNullable())
						throw StandardException.newException(SQLState.LANG_DB2_ADD_UNIQUE_OR_PRIMARY_KEY_ON_NULL_COLS, colName);
				}
			}
		}
	}

	/**
	 * Determine whether or not the parameter matches a column name in this list.
	 *
	 * @param colName	The column name to search for.
	 *
	 * @return boolean  Whether or not a match is found.
	 */
	public boolean containsColumnName(String colName)
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			TableElementNode tableElement = (TableElementNode) elementAt(index);

			if (tableElement instanceof ColumnDefinitionNode)
			{
				if (colName.equals(((ColumnDefinitionNode) tableElement).getName()))
				{
					return true;
				}
			}
		}

		return false;
	}
}

