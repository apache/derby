/*

   Derby - Class org.apache.derby.impl.sql.compile.CursorNode

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.ResultColumnDescriptor;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.impl.sql.CursorInfo;
import org.apache.derby.impl.sql.CursorTableReference;

import java.util.ArrayList;
import java.util.Vector;

/**
 * A CursorNode represents a result set that can be returned to a client.
 * A cursor can be a named cursor created by the DECLARE CURSOR statement,
 * or it can be an unnamed cursor associated with a SELECT statement (more
 * precisely, a table expression that returns rows to the client).  In the
 * latter case, the cursor does not have a name.
 *
 * @author Jeff Lichtman
 */

public class CursorNode extends ReadCursorNode
{
	public final static int UNSPECIFIED = 0;
	public final static int READ_ONLY = 1;
	public final static int UPDATE = 2;

	private String		name;
	private OrderByList	orderByList;
	private String		statementType;
	private int		updateMode;
	private boolean		needTarget;

	/**
	** There can only be a list of updatable columns when FOR UPDATE
	** is specified as part of the cursor specification.
	*/
	private Vector	updatableColumns;
	private FromTable updateTable;
	private ResultColumnList	targetColumns;
	private ResultColumnDescriptor[]	targetColumnDescriptors;

	//If cursor references session schema tables, save the list of those table names in savedObjects in compiler context
	//Following is the position of the session table names list in savedObjects in compiler context
	//At generate time, we save this position in activation for easy access to session table names list from compiler context
	protected int indexOfSessionTableNamesInSavedObjects = -1;

	/**
	 * Initializer for a CursorNode
	 *
	 * @param statementType	Type of statement (SELECT, UPDATE, INSERT)
	 * @param resultSet	A ResultSetNode specifying the result set for
	 *			the cursor
	 * @param name		The name of the cursor, null if no name
	 * @param orderByList	The order by list for the cursor, null if no
	 *			order by list
	 * @param updateMode	The user-specified update mode for the cursor,
	 *			for example, CursorNode.READ_ONLY
	 * @param updatableColumns The list of updatable columns specified by
	 *			the user in the FOR UPDATE clause, null if no
	 *			updatable columns specified.  May only be
	 *			provided if the updateMode parameter is
	 *			CursorNode.UPDATE.
	 */

	public	void init(
		Object statementType,
		Object resultSet,
		Object name,
		Object orderByList,
		Object updateMode,
		Object updatableColumns)
	{
		init(resultSet);
		this.name = (String) name;
		this.statementType = (String) statementType;
		if (orderByList != null)
		{
			// The above "if" is redundant but jdk118 will load
			// OrderByList.class even if orderByList is null; i.e
			// this.orderByList = (OrderByList)null; 
			// will load orderByList.class-- this is a cheap way to not load
			// this class under jdk118. 
			// :) 
			this.orderByList = (OrderByList) orderByList;
		}
		this.updateMode = ((Integer) updateMode).intValue();
		this.updatableColumns = (Vector) updatableColumns;

		/*
		** This is a sanity check and not an error since the parser
		** controls setting updatableColumns and updateMode.
		*/
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(this.updatableColumns == null ||
			this.updatableColumns.size() == 0 || this.updateMode == UPDATE,
			"Can only have explicit updatable columns if update mode is UPDATE");
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
			return "name: " + name + "\n" +
				"updateMode: " + updateModeString(updateMode) + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return statementType;
	}

	/**
	 * Support routine for translating an updateMode identifier to a String
	 *
	 * @param updateMode	An updateMode identifier
	 *
	 * @return	A String representing the update mode.
	 */

	private static String updateModeString(int updateMode)
	{
		if (SanityManager.DEBUG)
		{
			switch (updateMode)
			{
			  case UNSPECIFIED:
				return "UNSPECIFIED (" + UNSPECIFIED + ")";

			  case READ_ONLY:
				return "READ_ONLY (" + READ_ONLY + ")";

			  case UPDATE:
				return "UPDATE (" + UPDATE + ")";

			  default:
				return "UNKNOWN VALUE (" + updateMode + ")";
			}
		}
		else
		{
			return "";
		}
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 *
	 * @return	Nothing
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			printLabel(depth, "orderByList: ");
			if (orderByList != null)
				orderByList.treePrint(depth + 1);
		}
	}

	/**
	 * Bind this CursorNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc. It also includes determining whether an UNSPECIFIED cursor
	 * is updatable or not, and verifying that an UPDATE cursor actually is.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bind() throws StandardException
	{
		DataDictionary				dataDictionary;

		dataDictionary = getDataDictionary();

		// This is how we handle queries like: SELECT A FROM T ORDER BY B.
		// We pull up the order by columns (if they don't appear in the SELECT
		// LIST) and let the bind() do the job.  Note that the pullup is done
		// before the bind() and we may avoid pulling up ORDERBY columns that
		// would otherwise be avoided, e.g., "SELECT * FROM T ORDER BY B".
		// Pulled-up ORDERBY columns that are duplicates (like the above "SELECT
		// *" query will be removed in bindOrderByColumns().
		// Finally, given that extra columns may be added to the SELECT list, we
		// inject a ProjectRestrictNode so that only the user-specified columns
		// will be returned (see genProjectRestrict() in SelectNode.java).
		if (orderByList != null)
		{
			orderByList.pullUpOrderByColumns(resultSet);
		}

		super.bind(dataDictionary);

		// bind the order by
		if (orderByList != null)
		{
			orderByList.bindOrderByColumns(resultSet);
		}

		// bind the updatability

		// if it says it is updatable, verify it.
		if (updateMode == UPDATE)
		{
			int checkedUpdateMode;

			checkedUpdateMode = determineUpdateMode(dataDictionary);
			if (SanityManager.DEBUG)
			SanityManager.DEBUG("DumpUpdateCheck","update mode is UPDATE ("+updateMode+") checked mode is "+checkedUpdateMode);
			if (updateMode != checkedUpdateMode)
					throw StandardException.newException(SQLState.LANG_STMT_NOT_UPDATABLE);
		}

		// if it doesn't know if it is updatable, determine it
		if (updateMode == UNSPECIFIED)
		{
			/*
			** NOTE: THIS IS NOT COMPATIBLE WITH THE ISO/ANSI STANDARD!!!
			**
			** According to ANSI, cursors are updatable by default, unless
			** they can't be (e.g. they contain joins).  But this would mean
			** that we couldn't use an index on any single-table select,
			** unless it was declared FOR READ ONLY.  This would be pretty
			** terrible, so we are breaking the ANSI rules and making all
			** cursors (i.e. select statements) read-only by default.
			** Users will have to say FOR UPDATE if they want a cursor to
			** be updatable.  Later, we may have an ANSI compatibility
			** mode so we can pass the NIST tests.
			*/
			updateMode = READ_ONLY;

			/* updateMode = determineUpdateMode(); */

			//if (SanityManager.DEBUG)
			//SanityManager.DEBUG("DumpUpdateCheck","update mode is UNSPECIFIED ("+UNSPECIFIED+") checked mode is "+updateMode);

			if (updateMode == READ_ONLY)
				updatableColumns = null; // don't need them any more
		}
	
		// bind the update columns
		if (updateMode == UPDATE)
		{
			bindUpdateColumns(updateTable);

			// If the target table is a FromBaseTable, mark the updatable
			// columns.  (I can't think of a way that an updatable table
			// could be anything but a FromBaseTable at this point, but
			// it's better to be careful.
			if (updateTable instanceof FromTable)
			{
				((FromTable) updateTable).markUpdatableByCursor(updatableColumns);
			}
		}

		resultSet.renameGeneratedResultNames();

		//need to look for SESSION tables only if global temporary tables declared for the connection
		if (getLanguageConnectionContext().checkIfAnyDeclaredGlobalTempTablesForThisConnection())
		{
			//If this cursor has references to session schema tables, save the names of those tables into compiler context
			//so they can be passed to execution phase.
			ArrayList sessionSchemaTableNames = getSessionSchemaTableNamesForCursor();
			if (sessionSchemaTableNames != null)
				indexOfSessionTableNamesInSavedObjects = getCompilerContext().addSavedObject(sessionSchemaTableNames);
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
		//If this node references a SESSION schema table, then return true. 
		return resultSet.referencesSessionSchema();
	}

	//Check if this cursor references any session schema tables. If so, pass those names to execution phase through savedObjects
	//This list will be used to check if there are any holdable cursors referencing temporary tables at commit time.
	//If yes, then the data in those temporary tables should be preserved even if they are declared with ON COMMIT DELETE ROWS option
	protected ArrayList getSessionSchemaTableNamesForCursor()
		throws StandardException
	{
		FromList fromList = resultSet.getFromList();
		int fromListSize = fromList.size();
		FromTable fromTable;
		ArrayList sessionSchemaTableNames = null;

		for( int i = 0; i < fromListSize; i++)
		{
			fromTable = (FromTable) fromList.elementAt(i);
			if (fromTable instanceof FromBaseTable && isSessionSchema(fromTable.getTableDescriptor().getSchemaDescriptor()))
			{
				if (sessionSchemaTableNames == null)
					sessionSchemaTableNames = new ArrayList();
				sessionSchemaTableNames.add(fromTable.getTableName().getTableName());
			}
		}

		return sessionSchemaTableNames;
	}

	/**
	 * Take a cursor and determine if it is UPDATE
	 * or READ_ONLY based on the shape of the cursor specification.
	 * <p>
	 * The following conditions make a cursor read only:
	 * <UL>
	 * <LI>if it says FOR READ ONLY
	 * <LI>if it says ORDER BY
	 * <LI>if its query specification is not read only. At present this
	 *     is explicitly tested here, with these conditions.  At some future
	 *     point in time, this checking ought to be moved into the
	 *     ResultSet nodes themselves.  The conditions for a query spec.
     *     not to be read only include:
	 *     <UL>
	 *     <LI>if it has a set operation such as UNION or INTERSECT, i.e.
	 *         does not have a single outermost SELECT
	 *     <LI>if it does not have exactly 1 table in its FROM list;
	 *         0 tables would occur if we ever support a SELECT without a
	 *         FROM e.g., for generating a row without an underlying table
	 *         (like what we do for an INSERT of a VALUES list); >1 tables
	 *         occurs when joins are in the tree.
	 *     <LI>if the table in its FROM list is not a base table (REMIND
	 *         when views/from subqueries are added, this should be relaxed to
     *         be that the table is not updatable)
	 *     <LI>if it has a GROUP BY or HAVING (NOTE I am assuming that if
	 *         and aggregate is detected in a SELECT w/o a GROUP BY, one
	 *         has been added to show that the whole table is a group)
	 *     <LI> NOTE that cursors are updatable even if none of the columns
	 *         in the select are updatable -- what they care about is the
	 *         updatability of the columns of the target table.
	 *     </UL>
	 * </UL>
	 *
	 * @return the known update mode for the cursor.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private int determineUpdateMode(DataDictionary dataDictionary)
		throws StandardException
	{
		SelectNode selectNode;
		FromList tables;
		FromTable targetTable;

		if (updateMode == READ_ONLY)
		{
			return READ_ONLY;
		}

		if (orderByList != null)
		{
			if (SanityManager.DEBUG)
			SanityManager.DEBUG("DumpUpdateCheck","cursor has order by");
			return READ_ONLY;
		}

		// get the ResultSet to tell us what it thinks it is
		// and the target table
		if (! resultSet.isUpdatableCursor(dataDictionary))
		{
			return READ_ONLY;
		}

		// The FOR UPDATE clause has two uses:
		//
		// for positioned cursor updates
		//
		// to change locking behaviour of the select
		// to reduce deadlocks on subsequent updates
		// in the same transaction.
		//
		// We now support this latter case, without requiring
		// that the source of the rows be able to implement
		// a positioned update.

		updateTable = resultSet.getCursorTargetTable();

		/* Tell the table that it is the cursor target */
		if (updateTable.markAsCursorTargetTable()) {
			/* Cursor is updatable - remember to generate the position code */
			needTarget = true;

			/* We must generate the target column list at bind time
			 * because the optimizer may transform the FromBaseTable from
			 * a table scan into an index scan.
			 */
			genTargetResultColList();
		}




		return UPDATE;
	}

	/**
	 * Optimize a DML statement (which is the only type of statement that
	 * should need optimizing, I think). This method over-rides the one
	 * in QueryTreeNode.
	 *
	 * This method takes a bound tree, and returns an optimized tree.
	 * It annotates the bound tree rather than creating an entirely
	 * new tree.
	 *
	 * Throws an exception if the tree is not bound, or if the binding
	 * is out of date.
	 *
	 * @return	An optimized QueryTree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode optimize() throws StandardException
	{
		// Push the order by list down to the ResultSet
		if (orderByList != null)
		{
			// If we have more than 1 ORDERBY columns, we may be able to
			// remove duplicate columns, e.g., "ORDER BY 1, 1, 2".
			if (orderByList.size() > 1)
			{
				orderByList.removeDupColumns();
			}

			resultSet.pushOrderByList(orderByList);
			orderByList = null;
		}
		return super.optimize();
	}

	/**
	 * Returns the type of activation this class
	 * generates.
	 * 
	 * @return either (NEED_CURSOR_ACTIVATION
	 *
	 * @exception StandardException		Thrown on error
	 */
	 
	int activationKind()
	{
		return NEED_CURSOR_ACTIVATION;
	}

	/**
	 * Do code generation for this CursorNode
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The method the generated code is to go into
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb) throws StandardException
	{
		if (indexOfSessionTableNamesInSavedObjects != -1 ) //if this cursor references session schema tables, do following
		{
			MethodBuilder constructor = acb.getConstructor();
			constructor.pushThis();
			constructor.push(indexOfSessionTableNamesInSavedObjects);
			constructor.putField(org.apache.derby.iapi.reference.ClassName.BaseActivation, "indexOfSessionTableNamesInSavedObjects", "int");
			constructor.endStatement();
    }

		// generate the parameters
		generateParameterValueSet(acb);

		// tell the outermost result set that it is the outer
		// result set of the statement.
		resultSet.markStatementResultSet();

	    // this will generate an expression that will be a ResultSet
        super.generate(acb, mb);

		/*
		** Generate the position code if this cursor is updatable.  This
		** involves generating methods to get the cursor result set, and
		** the target result set (which is for the base row).  Also,
		** generate code to store the cursor result set in a generated
		** field.
		*/
		if (needTarget)
		{
			// PUSHCOMPILE - could be put into a single method
			acb.rememberCursor(mb);
			acb.addCursorPositionCode();
		}

		/*
		** ensure all parameters have been generated
		*/
		generateParameterHolders(acb);
	}

	// class interface

	public String getUpdateBaseTableName() 
	{
		return (updateTable == null) ? null : updateTable.getBaseTableName();
	}

	public String getUpdateExposedTableName() 
		throws StandardException
	{
		return (updateTable == null) ? null : updateTable.getExposedName();
	}

	public String getUpdateSchemaName() 
		throws StandardException
	{
		//we need to use the base table for the schema name
		return (updateTable == null) ? null : ((FromBaseTable)updateTable).getTableNameField().getSchemaName();
	}

	public int getUpdateMode()
	{
		return updateMode;
	}

	/**
	 * Return String[] of names from the FOR UPDATE OF List
	 *
	 * @return	String[] of names from the FOR UPDATE OF list.
	 */
	private String[] getUpdatableColumns()
	{
		return (updatableColumns == null) ?
				(String[])null :
				getUpdateColumnNames();
	}

	/**
		Positioned update needs to know what the target result set
		looks like. This is generated from the UpdateColumnList
		available for the cursor, to describe the rows coming from
		the target result set under the cursor. This result set contains
		a superset of the updatable columns; the caller must verify that
		only those listed in the FOR UPDATE clause are used.

		@return a result column list containing a description of
		the target table (this may contain non-updatable columns).
	 * @exception StandardException		Thrown on error
	 */
	public ResultColumnDescriptor[] genTargetResultColList()
		throws StandardException
	{
		ResultColumnList newList;

		/*
		   updateTable holds the FromTable that is the target.
		   copy its ResultColumnList, making BaseColumn references
		   for use in the CurrentOfNode, which behaves as if it had
		   base columns for the statement it is in.

			updateTable is null if the cursor is not updatable.
		 */
		if (updateTable == null) return null;

		if (targetColumnDescriptors != null) return targetColumnDescriptors;

		newList = (ResultColumnList) getNodeFactory().getNode(
										C_NodeTypes.RESULT_COLUMN_LIST,
										getContextManager());
		ResultColumnList rcl = updateTable.getResultColumns();
		int rclSize = rcl.size();
		for (int index = 0; index < rclSize; index++)
		{
			ResultColumn origCol, newCol;
			ValueNode newNode;

			origCol = (ResultColumn) rcl.elementAt(index);

			// Build a ResultColumn/BaseColumnNode pair for the column
			newNode = (ValueNode) getNodeFactory().getNode(
							C_NodeTypes.BASE_COLUMN_NODE,
							origCol.getName(),
							makeTableName(origCol.getSchemaName(),
										  origCol.getTableName()),								
							origCol.getTypeServices(),
							getContextManager());
			newCol = (ResultColumn) getNodeFactory().getNode(
									C_NodeTypes.RESULT_COLUMN,
									origCol.columnDescriptor,
									newNode,
									getContextManager());

			/* Build the ResultColumnList to return */
			newList.addResultColumn(newCol);
		}

		// we save the result so we only do this once
		targetColumns = newList;
		targetColumnDescriptors = newList.makeResultDescriptors();
		return targetColumnDescriptors;
	}

	/**
	 * Returns whether or not this Statement requires a set/clear savepoint
	 * around its execution.  The following statement "types" do not require them:
	 *		Cursor	- unnecessary and won't work in a read only environment
	 *		Xact	- savepoint will get blown away underneath us during commit/rollback
	 *
	 * @return boolean	Whether or not this Statement requires a set/clear savepoint
	 */
	public boolean needsSavepoint()
	{
		return false;
	}

	/**
	 * Get information about this cursor.  For sps,
	 * this is info saved off of the original query
	 * tree (the one for the underlying query).
	 *
	 * @return	the cursor info
	 * @exception StandardException thrown if generation fails
	 */
	public Object getCursorInfo()
		throws StandardException
	{
		if (!needTarget)
			return null;

		return new CursorInfo(updateMode,
								new CursorTableReference(
										getUpdateExposedTableName(),
										getUpdateBaseTableName(),
										getUpdateSchemaName()),
								genTargetResultColList(),
								getUpdatableColumns());
	}

	/**
		Bind the update columns by their names to the target table
		of the cursor specification.
		Doesn't check for duplicates in the list, although it could...
		REVISIT: If the list is empty, should it expand it out? at present,
		it leaves it empty.
	
		@param dataDictionary	The DataDictionary to use for binding
		@param targetTable	The underlying target table 
	
		@exception StandardException		Thrown on error
	 */
	private void bindUpdateColumns(FromTable targetTable)
					throws StandardException 
	{
		int size = updatableColumns.size();
		TableDescriptor tableDescriptor;
		String columnName;

		for (int index = 0; index < size; index++)
		{
		    columnName = (String) updatableColumns.elementAt(index);
		    tableDescriptor = targetTable.getTableDescriptor();
		    if ( tableDescriptor.getColumnDescriptor(columnName) == null)
		    {
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND, columnName);
		    }

		}
	}

	/**
	 * Get an array of strings for each updatable column
	 * in this list.
	 *
	 * @return an array of strings
	 */
	private String[] getUpdateColumnNames()
	{
		int size = updatableColumns.size();
		if (size == 0)
		{
			return (String[])null;
		}

		String[] names = new String[size];

		updatableColumns.copyInto(names);

		return names;
	}
}
