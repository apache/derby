/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateTableNode

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

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.DB2Limit;

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.CreateConstraintConstantAction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import java.util.Properties;

/**
 * A CreateTableNode is the root of a QueryTree that represents a CREATE TABLE or DECLARE GLOBAL TEMPORARY TABLE
 * statement.
 *
 * @author Jeff Lichtman
 */

public class CreateTableNode extends CreateStatementNode
{
	private char				lockGranularity;
	private boolean				onCommitDeleteRows; //If true, on commit delete rows else on commit preserve rows of temporary table.
	private boolean				onRollbackDeleteRows; //If true, on rollback delete rows from temp table if it was logically modified in that UOW. true is the only supported value
	private Properties			properties;
	private TableElementList	tableElementList;
	protected int	tableType; //persistent table or global temporary table

	/**
	 * Initializer for a CreateTableNode for a base table
	 *
	 * @param objectName		The name of the new object being created (ie base table)
	 * @param tableElementList	The elements of the table: columns,
	 *				constraints, etc.
	 * @param properties		The optional list of properties associated with
	 *							the table.
	 * @param lockGranularity	The lock granularity.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
			Object newObjectName,
			Object tableElementList,
			Object properties,
			Object lockGranularity)
		throws StandardException
	{
		tableType = TableDescriptor.BASE_TABLE_TYPE;
		this.lockGranularity = ((Character) lockGranularity).charValue();
		implicitCreateSchema = true;

		if (SanityManager.DEBUG)
		{
			if (this.lockGranularity != TableDescriptor.TABLE_LOCK_GRANULARITY &&
				this.lockGranularity != TableDescriptor.ROW_LOCK_GRANULARITY)
			{
				SanityManager.THROWASSERT(
				"Unexpected value for lockGranularity = " + this.lockGranularity);
			}
		}

		initAndCheck(newObjectName);
		this.tableElementList = (TableElementList) tableElementList;
		this.properties = (Properties) properties;
	}

	/**
	 * Initializer for a CreateTableNode for a global temporary table
	 *
	 * @param objectName		The name of the new object being declared (ie temporary table)
	 * @param tableElementList	The elements of the table: columns,
	 *				constraints, etc.
	 * @param properties		The optional list of properties associated with
	 *							the table.
	 * @param onCommitDeleteRows	If true, on commit delete rows else on commit preserve rows of temporary table.
	 * @param onRollbackDeleteRows	If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
			Object newObjectName,
			Object tableElementList,
			Object properties,
			Object onCommitDeleteRows,
			Object onRollbackDeleteRows)
		throws StandardException
	{
		tableType = TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE;
		newObjectName = tempTableSchemaNameCheck(newObjectName);
		this.onCommitDeleteRows = ((Boolean) onCommitDeleteRows).booleanValue();
		this.onRollbackDeleteRows = ((Boolean) onRollbackDeleteRows).booleanValue();
		initAndCheck(newObjectName);
		this.tableElementList = (TableElementList) tableElementList;
		this.properties = (Properties) properties;

		if (SanityManager.DEBUG)
		{
			if (this.onRollbackDeleteRows == false)
			{
				SanityManager.THROWASSERT(
				"Unexpected value for onRollbackDeleteRows = " + this.onRollbackDeleteRows);
			}
		}
	}

	/**
	 * If no schema name specified for global temporary table, SESSION is the implicit schema.
	 * Otherwise, make sure the specified schema name for global temporary table is SESSION.
	 * @param objectName		The name of the new object being declared (ie temporary table)
	*/
	private Object tempTableSchemaNameCheck(Object objectName)
		throws StandardException {
		TableName	tempTableName = (TableName) objectName;
		if (tempTableName != null)
		{
			if (tempTableName.getSchemaName() == null)
				tempTableName.setSchemaName(SchemaDescriptor.STD_DECLARED_GLOBAL_TEMPORARY_TABLES_SCHEMA_NAME); //If no schema specified, SESSION is the implicit schema.
			else if (!(isSessionSchema(tempTableName.getSchemaName())))
				throw StandardException.newException(SQLState.LANG_DECLARED_GLOBAL_TEMP_TABLE_ONLY_IN_SESSION_SCHEMA);
		}
		return(tempTableName);
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
			String tempString = "tableElementList: " + "\n" + tableElementList + "\n";
			if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			{
				tempString = tempString + "onCommitDeleteRows: " + "\n" + onCommitDeleteRows + "\n";
				tempString = tempString + "onRollbackDeleteRows: " + "\n" + onRollbackDeleteRows + "\n";
			} else
				tempString = tempString + "properties: " + "\n" + properties + "\n" + "lockGranularity: " + "\n" + lockGranularity + "\n";
			return super.toString() +  tempString;
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			return "DECLARE GLOBAL TEMPORARY TABLE";
		else
			return "CREATE TABLE";
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this CreateTableNode.  This means doing any static error checking that can be
	 * done before actually creating the base table or declaring the global temporary table.
	 * For eg, verifying that the TableElementList does not contain any duplicate column names.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bind() throws StandardException
	{
		DataDictionary	dataDictionary = getDataDictionary();
		int numPrimaryKeys = 0;
		int numCheckConstraints = 0;
		int numReferenceConstraints = 0;
		int numUniqueConstraints = 0;

		tableElementList.validate(this, dataDictionary, (TableDescriptor) null);

		/* Only 1012 columns allowed per table */
		if (tableElementList.countNumberOfColumns() > DB2Limit.DB2_MAX_COLUMNS_IN_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_COLUMNS_IN_TABLE_OR_VIEW,
				String.valueOf(tableElementList.countNumberOfColumns()),
				getRelativeName(),
				String.valueOf(DB2Limit.DB2_MAX_COLUMNS_IN_TABLE));
		}

		numPrimaryKeys = tableElementList.countConstraints(
								DataDictionary.PRIMARYKEY_CONSTRAINT);

		/* Only 1 primary key allowed per table */
		if (numPrimaryKeys > 1)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_PRIMARY_KEY_CONSTRAINTS, getRelativeName());
		}

		/* Check the validity of all check constraints */
		numCheckConstraints = tableElementList.countConstraints(
									DataDictionary.CHECK_CONSTRAINT);

		numReferenceConstraints = tableElementList.countConstraints(
									DataDictionary.FOREIGNKEY_CONSTRAINT);

		numUniqueConstraints = tableElementList.countConstraints(
									DataDictionary.UNIQUE_CONSTRAINT);

		//temp tables can't have primary key or check or foreign key or unique constraints defined on them
		if ((tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE) &&
			(numPrimaryKeys > 0 || numCheckConstraints > 0 || numReferenceConstraints > 0 || numUniqueConstraints > 0))
				throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_DECLARED_GLOBAL_TEMP_TABLE);

		//each of these constraints have a backing index in the back. We need to make sure that a table never has more
		//more than 32767 indexes on it and that is why this check.
		if ((numPrimaryKeys + numReferenceConstraints + numUniqueConstraints) > DB2Limit.DB2_MAX_INDEXES_ON_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEXES_ON_TABLE, 
				String.valueOf(numPrimaryKeys + numReferenceConstraints + numUniqueConstraints),
				getRelativeName(),
				String.valueOf(DB2Limit.DB2_MAX_INDEXES_ON_TABLE));
		}

		if (numCheckConstraints > 0)
		{
			/* In order to check the validity of the check constraints
			 * we must goober up a FromList containing a single table,
			 * the table being created, with an RCL containing the
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
			table.setTableNumber(0);
			fromList.addFromTable(table);
			table.setResultColumns((ResultColumnList) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager()));
			tableElementList.appendNewColumnsToRCL(table);

			/* Now that we've finally goobered stuff up, bind and validate
			 * the check constraints.
			 */
			tableElementList.bindAndValidateCheckConstraints(fromList);
		}

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
		//If table being created/declared is in SESSION schema, then return true.
		return isSessionSchema(getSchemaDescriptor());
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		TableElementList		coldefs = tableElementList;

		// for each column, stuff system.column
		ColumnInfo[] colInfos = new ColumnInfo[coldefs.countNumberOfColumns()];

	    int numConstraints = coldefs.genColumnInfos(colInfos);

		/* If we've seen a constraint, then build a constraint list */
		CreateConstraintConstantAction[] conActions = null;

		SchemaDescriptor sd = getSchemaDescriptor();

		if (numConstraints > 0)
		{
			conActions =
                new CreateConstraintConstantAction[numConstraints];

			coldefs.genConstraintActions(
                conActions, getRelativeName(), sd, getDataDictionary());
		}

        // if the any of columns are "long" and user has not specified a
        // page size, set the pagesize to 32k.
        // Also in case where the approximate sum of the column sizes is
        // greater than the bump threshold , bump the pagesize to 32k

        boolean table_has_long_column = false;
        int approxLength = 0;

        for (int i = 0; i < colInfos.length; i++)
        {
			DataTypeDescriptor dts = colInfos[i].dataType;
            if (dts.getTypeId().isLongConcatableTypeId())
            {
                table_has_long_column = true;
                break;
            }

            approxLength += dts.getTypeId().getApproximateLengthInBytes(dts);
        }

        if (table_has_long_column || (approxLength > Property.TBL_PAGE_SIZE_BUMP_THRESHOLD))
        {
			if (((properties == null) ||
                 (properties.get(Property.PAGE_SIZE_PARAMETER) == null)) &&
                (PropertyUtil.getServiceProperty(
                     getLanguageConnectionContext().getTransactionCompile(),
                     Property.PAGE_SIZE_PARAMETER) == null))
            {
                // do not override the user's choice of page size, whether it
                // is set for the whole database or just set on this statement.

                if (properties == null)
                    properties = new Properties();

                properties.put(
                    Property.PAGE_SIZE_PARAMETER,
                    Property.PAGE_SIZE_DEFAULT_LONG);
            }
        }

		return(
            getGenericConstantActionFactory().getCreateTableConstantAction(
                sd.getSchemaName(),
                getRelativeName(),
                tableType,
                colInfos,
                conActions,
                properties,
                lockGranularity,
                onCommitDeleteRows,
                onRollbackDeleteRows));
	}
}
