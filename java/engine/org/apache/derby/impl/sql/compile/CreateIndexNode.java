/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateIndexNode

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

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.types.DataTypeDescriptor;

/**
 * A CreateIndexNode is the root of a QueryTree that represents a CREATE INDEX
 * statement.
 *
 */

class CreateIndexNode extends DDLStatementNode
{
    private boolean             unique;
    private Properties          properties;
    private String              indexType;
    private TableName           indexName;
    private TableName           tableName;
    private List<String>        columnNameList;
    private String[]            columnNames;
    private boolean[]           isAscending;
    private int[]               boundColumnIDs;
    private TableDescriptor     td;

	/**
     * Constructor for a CreateIndexNode
	 *
	 * @param unique	True means it's a unique index
	 * @param indexType	The type of index
	 * @param indexName	The name of the index
	 * @param tableName	The name of the table the index will be on
	 * @param columnNameList	A list of column names, in the order they
	 *							appear in the index.
	 * @param properties	The optional properties list associated with the index.
     * @param cm Context manager
	 *
	 * @exception StandardException		Thrown on error
	 */
    CreateIndexNode(boolean unique,
                    String indexType,
                    TableName indexName,
                    TableName tableName,
                    List<String> columnNameList,
                    Properties properties,
                    ContextManager cm) throws StandardException
	{
        super(indexName, cm);
        this.unique = unique;
        this.indexType = indexType;
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnNameList = columnNameList;
        this.properties = properties;
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
				"unique: " + unique + "\n" +
				"indexType: " + indexType + "\n" +
				"indexName: " + indexName + "\n" +
				"tableName: " + tableName + "\n" +
				"properties: " + properties + "\n";
		}
		else
		{
			return "";
		}
	}

    String statementToString()
	{
		return "CREATE INDEX";
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this CreateIndexNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 * For example, verifying that the column name list does not
	 * contain any duplicate column names.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void bindStatement() throws StandardException
	{
		int						columnCount;

        getSchemaDescriptor(); // want checking side-effects only

		td = getTableDescriptor(tableName);

		//throw an exception if user is attempting to create an index on a temporary table
		if (td.getTableType() == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
		{
				throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_DECLARED_GLOBAL_TEMP_TABLE);
		}

		//If total number of indexes on the table so far is more than 32767, then we need to throw an exception
		if (td.getTotalNumberOfIndexes() > Limits.DB2_MAX_INDEXES_ON_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEXES_ON_TABLE,
				String.valueOf(td.getTotalNumberOfIndexes()),
				tableName,
				String.valueOf(Limits.DB2_MAX_INDEXES_ON_TABLE));
		}

		/* Validate the column name list */
		verifyAndGetUniqueNames();

		columnCount = columnNames.length;
		boundColumnIDs = new int[ columnCount ];

		// Verify that the columns exist
		for (int i = 0; i < columnCount; i++)
		{
			ColumnDescriptor			columnDescriptor;

			columnDescriptor = td.getColumnDescriptor(columnNames[i]);
			if (columnDescriptor == null)
			{
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,
															columnNames[i],
															tableName);
			}
			boundColumnIDs[ i ] = columnDescriptor.getPosition();

			// Don't allow a column to be created on a non-orderable type
			if ( ! columnDescriptor.getType().getTypeId().
												orderable(getClassFactory()))
			{
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION,
					columnDescriptor.getType().getTypeId().getSQLTypeName());
			}
		}

		/* Check for number of key columns to be less than 16 to match DB2 */
		if (columnCount > 16)
			throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEX_KEY_COLS);

		/* See if the index already exists in this schema.
		 * NOTE: We still need to check at execution time
		 * since the index name is only unique to the schema,
		 * not the table.
		 */
//  		if (dd.getConglomerateDescriptor(indexName.getTableName(), sd, false) != null)
//  		{
//  			throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
//  												 "Index",
//  												 indexName.getTableName(),
//  												 "schema",
//  												 sd.getSchemaName());
//  		}

		/* Statement is dependent on the TableDescriptor */
		getCompilerContext().createDependency(td);

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
		//If create index is on a SESSION schema table, then return true.
		return isSessionSchema(td.getSchemaName());
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
    public ConstantAction makeConstantAction() throws StandardException
	{
		SchemaDescriptor		sd = getSchemaDescriptor();

		int columnCount = columnNames.length;
		int approxLength = 0;

		// bump the page size for the index,
		// if the approximate sizes of the columns in the key are
		// greater than the bump threshold.
		// Ideally, we would want to have atleast 2 or 3 keys fit in one page
		// With fix for beetle 5728, indexes on long types is not allowed
		// so we do not have to consider key columns of long types
		for (int i = 0; i < columnCount; i++)
		{
			ColumnDescriptor columnDescriptor = td.getColumnDescriptor(columnNames[i]);
			DataTypeDescriptor dts = columnDescriptor.getType();
			approxLength += dts.getTypeId().getApproximateLengthInBytes(dts);
		}


        if (approxLength > Property.IDX_PAGE_SIZE_BUMP_THRESHOLD)
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


		return getGenericConstantActionFactory().getCreateIndexConstantAction(
                    false, // not for CREATE TABLE
                    unique,
                    false, // it's not a UniqueWithDuplicateNulls Index
                    false, // it's not a constraint, so its checking
                           // is not deferrable
                    false, // initiallyDeferred: N/A
                    -1,    // constraintType: N/A
                    indexType,
                    sd.getSchemaName(),
                    indexName.getTableName(),
                    tableName.getTableName(),
                    td.getUUID(),
                    columnNames,
                    isAscending,
                    false,
                    null,
                    properties);
	}

	/**
	 * Check the uniqueness of the column names within the derived column list.
	 *
	 * @exception StandardException	Thrown if column list contains a
	 *											duplicate name.
	 */
	private void verifyAndGetUniqueNames()
				throws StandardException
	{
		int size = columnNameList.size();
        HashSet<String> seenNames = new HashSet<String>(size + 2, 0.999f);
		columnNames = new String[size];
		isAscending = new boolean[size];

		for (int index = 0; index < size; index++)
		{
			/* Verify that this column's name is unique within the list
			 * Having a space at the end meaning descending on the column
			 */
            columnNames[index] = columnNameList.get(index);
			if (columnNames[index].endsWith(" "))
			{
				columnNames[index] = columnNames[index].substring(0, columnNames[index].length() - 1);
				isAscending[index] = false;
			}
			else
				isAscending[index] = true;

            boolean alreadySeen = !seenNames.add(columnNames[index]);

			if (alreadySeen)
			{
				throw StandardException.newException(SQLState.LANG_DUPLICATE_COLUMN_NAME_CREATE_INDEX, columnNames[index]);
			}
		}
	}

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (indexName != null) {
            indexName = (TableName) indexName.accept(v);
        }

        if (tableName != null) {
            tableName = (TableName) tableName.accept(v);
        }
    }
}
