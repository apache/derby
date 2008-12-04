/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateTableNode

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

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Limits;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.depend.ProviderList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.conn.Authorizer;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.CreateConstraintConstantAction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.StringDataValue;
import java.util.Properties;

/**
 * A CreateTableNode is the root of a QueryTree that represents a CREATE TABLE or DECLARE GLOBAL TEMPORARY TABLE
 * statement.
 *
 */

public class CreateTableNode extends DDLStatementNode
{
	private char				lockGranularity;
	private boolean				onCommitDeleteRows; //If true, on commit delete rows else on commit preserve rows of temporary table.
	private boolean				onRollbackDeleteRows; //If true, on rollback delete rows from temp table if it was logically modified in that UOW. true is the only supported value
	private Properties			properties;
	private TableElementList	tableElementList;
	protected int	tableType; //persistent table or global temporary table
	private ResultColumnList	resultColumns;
	private ResultSetNode		queryExpression;

	/**
	 * Initializer for a CreateTableNode for a base table
	 *
	 * @param newObjectName		The name of the new object being created (ie base table)
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
	 * @param newObjectName		The name of the new object being declared (ie temporary table)
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
	 * Initializer for a CreateTableNode for a base table create from a query
	 * 
	 * @param newObjectName		The name of the new object being created
	 * 	                        (ie base table).
	 * @param resultColumns		The optional column list.
	 * @param queryExpression	The query expression for the table.
	 */
	public void init(
			Object newObjectName,
			Object resultColumns,
			Object queryExpression)
		throws StandardException
	{
		tableType = TableDescriptor.BASE_TABLE_TYPE;
		lockGranularity = TableDescriptor.DEFAULT_LOCK_GRANULARITY;
		implicitCreateSchema = true;
		initAndCheck(newObjectName);
		this.resultColumns = (ResultColumnList) resultColumns;
		this.queryExpression = (ResultSetNode) queryExpression;
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
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindStatement() throws StandardException
	{
		DataDictionary	dataDictionary = getDataDictionary();
		int numPrimaryKeys = 0;
		int numCheckConstraints = 0;
		int numReferenceConstraints = 0;
		int numUniqueConstraints = 0;
        int numGenerationClauses = 0;

        SchemaDescriptor sd = getSchemaDescriptor
            ( tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE, true);

		if (queryExpression != null)
		{
			FromList fromList = (FromList) getNodeFactory().getNode(
					C_NodeTypes.FROM_LIST,
					getNodeFactory().doJoinOrderOptimization(),
					getContextManager());
			
			CompilerContext cc = getCompilerContext();
			ProviderList prevAPL = cc.getCurrentAuxiliaryProviderList();
			ProviderList apl = new ProviderList();
			
			try
			{
				cc.setCurrentAuxiliaryProviderList(apl);
				cc.pushCurrentPrivType(Authorizer.SELECT_PRIV);
				
				/* Bind the tables in the queryExpression */
				queryExpression =
					queryExpression.bindNonVTITables(dataDictionary, fromList);
				queryExpression = queryExpression.bindVTITables(fromList);
				
				/* Bind the expressions under the resultSet */
				queryExpression.bindExpressions(fromList);
				
				/* Bind the query expression */
				queryExpression.bindResultColumns(fromList);
				
				/* Reject any untyped nulls in the RCL */
				/* e.g. CREATE TABLE t1 (x) AS VALUES NULL WITH NO DATA */
				queryExpression.bindUntypedNullsToResultColumns(null);
			}
			finally
			{
				cc.popCurrentPrivType();
				cc.setCurrentAuxiliaryProviderList(prevAPL);
			}
			
			/* If there is an RCL for the table definition then copy the
			 * names to the queryExpression's RCL after verifying that
			 * they both have the same size.
			 */
			ResultColumnList qeRCL = queryExpression.getResultColumns();
			
			if (resultColumns != null)
			{
				if (resultColumns.size() != qeRCL.visibleSize())
				{
					throw StandardException.newException(
							SQLState.LANG_TABLE_DEFINITION_R_C_L_MISMATCH,
							getFullName());
				}
				qeRCL.copyResultColumnNames(resultColumns);
			}
			
			int schemaCollationType = sd.getCollationType();
	    
			/* Create table element list from columns in query expression */
			tableElementList = new TableElementList();
			
			for (int index = 0; index < qeRCL.size(); index++)
			{
				ResultColumn rc = (ResultColumn) qeRCL.elementAt(index);
				if (rc.isGenerated()) 
				{
					continue;
				}
				/* Raise error if column name is system generated. */
				if (rc.isNameGenerated())
				{
					throw StandardException.newException(
							SQLState.LANG_TABLE_REQUIRES_COLUMN_NAMES);
				}

				DataTypeDescriptor dtd = rc.getExpression().getTypeServices();
				if ((dtd != null) && !dtd.isUserCreatableType())
				{
					throw StandardException.newException(
							SQLState.LANG_INVALID_COLUMN_TYPE_CREATE_TABLE,
							dtd.getFullSQLTypeName(),
							rc.getName());
				}
				//DERBY-2879  CREATE TABLE AS <subquery> does not maintain the 
				//collation for character types. 
				//eg for a territory based collation database
				//create table t as select tablename from sys.systables with no data;
				//Derby at this point does not support for a table's character 
				//columns to have a collation different from it's schema's
				//collation. Which means that in a territory based database, 
				//the query above will cause table t's character columns to
				//have collation of UCS_BASIC but the containing schema of t
				//has collation of territory based. This is not supported and
				//hence we will throw an exception below for the query above in
				//a territory based database. 
				if (dtd.getTypeId().isStringTypeId() && 
						dtd.getCollationType() != schemaCollationType)
				{
					String schemaCollationName =
			        	(schemaCollationType == 
			        		StringDataValue.COLLATION_TYPE_UCS_BASIC ? 
			                Property.UCS_BASIC_COLLATION : 
			                Property.TERRITORY_BASED_COLLATION);
					throw StandardException.newException(
							SQLState.LANG_CAN_NOT_CREATE_TABLE,
							dtd.getCollationName(),
							schemaCollationName);
				}

				ColumnDefinitionNode column = new ColumnDefinitionNode();
				column.init(rc.getName(), null, rc.getType(), null);
				tableElementList.addTableElement(column);
			}
		} else {
			//Set the collation type and collation derivation of all the 
			//character type columns. Their collation type will be same as the 
			//collation of the schema they belong to. Their collation 
			//derivation will be "implicit". 
			//Earlier we did this in makeConstantAction but that is little too 
			//late (DERBY-2955)
			//eg 
			//CREATE TABLE STAFF9 (EMPNAME CHAR(20),
			//  CONSTRAINT STAFF9_EMPNAME CHECK (EMPNAME NOT LIKE 'T%'))
			//For the query above, when run in a territory based db, we need 
			//to have the correct collation set in bind phase of create table 
			//so that when LIKE is handled in LikeEscapeOperatorNode, we have 
			//the correct collation set for EMPNAME otherwise it will throw an 
			//exception for 'T%' having collation of territory based and 
			//EMPNAME having the default collation of UCS_BASIC
			tableElementList.setCollationTypesOnCharacterStringColumns(
				getSchemaDescriptor(
					tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE,
					true));
		}

		tableElementList.validate(this, dataDictionary, (TableDescriptor) null);

		/* Only 1012 columns allowed per table */
		if (tableElementList.countNumberOfColumns() > Limits.DB2_MAX_COLUMNS_IN_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_COLUMNS_IN_TABLE_OR_VIEW,
				String.valueOf(tableElementList.countNumberOfColumns()),
				getRelativeName(),
				String.valueOf(Limits.DB2_MAX_COLUMNS_IN_TABLE));
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

        numGenerationClauses = tableElementList.countGenerationClauses();

		//temp tables can't have primary key or check or foreign key or unique constraints defined on them
		if ((tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE) &&
			(numPrimaryKeys > 0 || numCheckConstraints > 0 || numReferenceConstraints > 0 || numUniqueConstraints > 0))
				throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_DECLARED_GLOBAL_TEMP_TABLE);

		//each of these constraints have a backing index in the back. We need to make sure that a table never has more
		//more than 32767 indexes on it and that is why this check.
		if ((numPrimaryKeys + numReferenceConstraints + numUniqueConstraints) > Limits.DB2_MAX_INDEXES_ON_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEXES_ON_TABLE, 
				String.valueOf(numPrimaryKeys + numReferenceConstraints + numUniqueConstraints),
				getRelativeName(),
				String.valueOf(Limits.DB2_MAX_INDEXES_ON_TABLE));
		}

		if ( (numCheckConstraints > 0) || (numGenerationClauses > 0) || (numReferenceConstraints > 0) )
		{
			/* In order to check the validity of the check constraints and
			 * generation clauses
			 * we must goober up a FromList containing a single table,
			 * the table being created, with an RCL containing the
			 * new columns and their types.  This will allow us to
			 * bind the constraint definition trees against that
			 * FromList.  When doing this, we verify that there are
			 * no nodes which can return non-deterministic results.
			 */
			FromList fromList = makeFromList( null, tableElementList, true );
            FormatableBitSet    generatedColumns = new FormatableBitSet();

			/* Now that we've finally goobered stuff up, bind and validate
			 * the check constraints and generation clauses.
			 */
			if  (numGenerationClauses > 0) { tableElementList.bindAndValidateGenerationClauses( sd, fromList, generatedColumns ); }
			if  (numCheckConstraints > 0) { tableElementList.bindAndValidateCheckConstraints(fromList); }
            if ( numReferenceConstraints > 0) { tableElementList.validateForeignKeysOnGenerationClauses( fromList, generatedColumns ); }
		}

        if ( numPrimaryKeys > 0 ) { tableElementList.validatePrimaryKeyNullability(); }
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
		return isSessionSchema(
			getSchemaDescriptor(
				tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE,
				true));
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

		SchemaDescriptor sd = getSchemaDescriptor(
			tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE,
			true);

		
		if (numConstraints > 0)
		{
			conActions =
                new CreateConstraintConstantAction[numConstraints];

			coldefs.genConstraintActions(true,
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
