/*

   Derby - Class org.apache.derby.impl.sql.compile.ResultSetNode

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.OptimizerFactory;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.catalog.types.DefaultInfoImpl;

import java.util.Properties;
import java.util.Vector;

/**
 * A ResultSetNode represents a result set, that is, a set of rows.  It is
 * analogous to a ResultSet in the LanguageModuleExternalInterface.  In fact,
 * code generation for a a ResultSetNode will create a "new" call to a
 * constructor for a ResultSet.
 *
 * @author Jeff Lichtman
 */

public abstract class ResultSetNode extends QueryTreeNode
{
	int					resultSetNumber;
	/* Bit map of referenced tables under this ResultSetNode */
	JBitSet				referencedTableMap;
	ResultColumnList	resultColumns;
	boolean				statementResultSet;
	boolean				cursorTargetTable;
	boolean				insertSource;

	CostEstimate 		costEstimate;
	CostEstimate		scratchCostEstimate;
	Optimizer			optimizer;

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
			return 	"resultSetNumber: " + resultSetNumber + "\n" +
				"referencedTableMap: " +
				(referencedTableMap != null 
						? referencedTableMap.toString() 
						: "null") + "\n" +
				"statementResultSet: " + statementResultSet + "\n" +
				super.toString();
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

			if (resultColumns != null)
			{
				printLabel(depth, "resultColumns: ");
				resultColumns.treePrint(depth);
			}
		}
	}

	/**
	 * Get the resultSetNumber in this ResultSetNode. Expected to be set during
	 * generate().
	 *
	 * @return int 	The resultSetNumber.
	 */

	public int getResultSetNumber()
	{
		return resultSetNumber;
	}

	/**
	 * Get the CostEstimate for this ResultSetNode.
	 *
	 * @return	The CostEstimate for this ResultSetNode.
	 */
	public CostEstimate getCostEstimate()
	{
		if (SanityManager.DEBUG)
		{
			if (costEstimate == null)
			{
				SanityManager.THROWASSERT(
					"costEstimate is not expected to be null for " +
					getClass().getName());
			}
		}
		return costEstimate;
	}

	/**
	 * Get the final CostEstimate for this ResultSetNode.
	 *
	 * @return	The final CostEstimate for this ResultSetNode.
	 */
	public CostEstimate getFinalCostEstimate()
	{
		if (SanityManager.DEBUG)
		{
			if (costEstimate == null)
			{
				SanityManager.THROWASSERT(
					"costEstimate is not expected to be null for " +
					getClass().getName());
			}
		}
		return costEstimate;
	}

	/**
	 * Assign the next resultSetNumber to the resultSetNumber in this ResultSetNode. 
	 * Expected to be done during generate().
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void assignResultSetNumber() throws StandardException
	{
		resultSetNumber = getCompilerContext().getNextResultSetNumber();
		resultColumns.setResultSetNumber(resultSetNumber);
	}

	/**
	 * Bind the non VTI tables in this ResultSetNode.  This includes getting their
	 * descriptors from the data dictionary and numbering them.
	 *
	 * @param dataDictionary	The DataDictionary to use for binding
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode bindNonVTITables(DataDictionary dataDictionary, 
							FromList fromListParam) 
							throws StandardException {
		return this;
	}


	/**
	 * Bind the VTI tables in this ResultSetNode.  This includes getting their
	 * descriptors from the data dictionary and numbering them.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode bindVTITables(FromList fromListParam) 
		throws StandardException {
		return this;
	}

	/**
	 * Bind the expressions in this ResultSetNode.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is for
	 * each expression.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindExpressions(FromList fromListParam)
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, 
					"bindExpressions() is not expected to be called for " + 
					this.getClass().toString());
	}

	/**
	 * Bind the expressions in this ResultSetNode if it has tables.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is for
	 * each expression.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindExpressionsWithTables(FromList fromListParam)
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, 
					"bindExpressionsWithTables() is not expected to be called for " + 
					this.getClass().toString());
	}

	/**
	 * Bind the expressions in the target list.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is
	 * for each expression.  This is useful for EXISTS subqueries, where we
	 * need to validate the target list before blowing it away and replacing
	 * it with a SELECT true.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindTargetExpressions(FromList fromListParam)
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, 
					"bindTargetExpressions() is not expected to be called for " + 
					this.getClass().toString());
	}

	/**
	 * Set the type of each parameter in the result column list for this table constructor.
	 *
	 * @param typeColumns	The ResultColumnList containing the desired result
	 *						types.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void setTableConstructorTypes(ResultColumnList typeColumns)
			throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(resultColumns.size() <= typeColumns.size(),
				"More columns in ResultColumnList than in base table");

		/* Look for ? parameters in the result column list */
		int rclSize = resultColumns.size();
		for (int index = 0; index < rclSize; index++)
		{
			ResultColumn	rc = (ResultColumn) resultColumns.elementAt(index);

			ValueNode re = rc.getExpression();

			if (re.isParameterNode())
			{
				ResultColumn	typeCol =
					(ResultColumn) typeColumns.elementAt(index);

				/*
				** We found a ? - set its type to the type of the
				** corresponding column of the target table.
				*/
				((ParameterNode) re).setDescriptor(
										typeCol.getTypeServices());
			}
			else if (re instanceof CharConstantNode)
			{
				// Character constants are of type CHAR (fixed length string).
				// This causes a problem (beetle 5160) when multiple row values are provided
				// as constants for insertion into a variable length string column.
				//
				// This issue is the query expression
				// VALUES 'abc', 'defghi'
				// has type of CHAR(6), ie. the length of largest row value for that column.
				// This is from the UNION defined behaviour.
				// This causes strings with less than the maximum length to be blank padded
				// to that length (CHAR semantics). Thus if this VALUES clause is used to
				// insert into a variable length string column, then these blank padded values
				// are inserted, which is not what is required ...
				// 
				// BECAUSE, when the VALUES is used as a table constructor SQL standard says the
				// types of the table constructor's columns are set by the table's column types.
				// Thus, in this case, each of those string constants should be of type VARCHAR
				// (or the matching string type for the table).
				//
				//
				// This is only an issue for fixed length character (CHAR, BIT) string or
				// binary consraints being inserted into variable length types.
				// This is because any other type's fundemental literal value is not affected
				// by its data type. E.g. Numeric types such as INT, REAL, BIGINT, DECIMAL etc.
				// do not have their value modifed by the union since even if the type is promoted
				// to a higher type, its fundemental value remains unchanged. 
				// values (1.2, 34.4567, 234.47) will be promoted to
				// values (1.2000, 34.4567, 234.4700)
				// but their numeric value remains the same.
				//
				//
				//
				// The fix is to change the base type of the table constructor's value to
				// match the column type. Its length can be left as-is, because there is
				// still a normailzation step when the value is inserted into the table.
				// That will set the correct length and perform truncation checks etc.

				ResultColumn	typeCol =
					(ResultColumn) typeColumns.elementAt(index);

				TypeId colTypeId = typeCol.getTypeId();

				if (colTypeId.isStringTypeId()) {

					if (colTypeId.getJDBCTypeId() != java.sql.Types.CHAR) {

						int maxWidth = re.getTypeServices().getMaximumWidth();

						re.setType(new DataTypeDescriptor(colTypeId, true, maxWidth));
					}
				}
				else if (colTypeId.isBitTypeId()) {
					if (colTypeId.getJDBCTypeId() == java.sql.Types.VARBINARY) {
					// then we're trying to cast a char literal into a
					// variable bit column.  We can't change the base
					// type of the table constructor's value from char
					// to bit, so instead, we just change the base type
					// of that value from char to varchar--that way,
					// no padding will be added when we convert to
					// bits later on (Beetle 5306).
						TypeId tId = TypeId.getBuiltInTypeId(java.sql.Types.VARCHAR);
						re.setType(new DataTypeDescriptor(tId, true));
						typeColumns.setElementAt(typeCol, index);
					}
					else if (colTypeId.getJDBCTypeId() == java.sql.Types.LONGVARBINARY) {
						TypeId tId = TypeId.getBuiltInTypeId(java.sql.Types.LONGVARCHAR);
						re.setType(new DataTypeDescriptor(tId, true));
						typeColumns.setElementAt(typeCol, index);
					}
				}

			}
			else if (re instanceof BitConstantNode)
			{
				ResultColumn	typeCol =
					(ResultColumn) typeColumns.elementAt(index);

				TypeId colTypeId = typeCol.getTypeId();

				if (colTypeId.isBitTypeId()) {

					// NOTE: Don't bother doing this if the column type is BLOB,
					// as we don't allow bit literals to be inserted into BLOB
					// columns (they have to be explicitly casted first); beetle 5266.
					if ((colTypeId.getJDBCTypeId() != java.sql.Types.BINARY) &&
						(colTypeId.getJDBCTypeId() != java.sql.Types.BLOB)) {

						int maxWidth = re.getTypeServices().getMaximumWidth();

						re.setType(new DataTypeDescriptor(colTypeId, true, maxWidth));
					}
				}
				else if (colTypeId.isStringTypeId()) {
					if (colTypeId.getJDBCTypeId() == java.sql.Types.VARCHAR) {
					// then we're trying to cast a bit literal into a
					// variable char column.  We can't change the base
					// type of the table constructor's value from bit
					// to char, so instead, we just change the base
					// type of that value from bit to varbit--that way,
					// no padding will be added when we convert to
					// char later on.
						TypeId tId = TypeId.getBuiltInTypeId(java.sql.Types.VARBINARY);
						re.setType(new DataTypeDescriptor(tId, true));
						typeColumns.setElementAt(typeCol, index);
					}
					else if (colTypeId.getJDBCTypeId() == java.sql.Types.LONGVARCHAR) {
						TypeId tId = TypeId.getBuiltInTypeId(java.sql.Types.LONGVARBINARY);
						re.setType(new DataTypeDescriptor(tId, true));
						typeColumns.setElementAt(typeCol, index);
					}
				}
			}
		}
	}

	/**
	 * Remember that this node is the source result set for an INSERT.
	 */
	public void setInsertSource()
	{
		insertSource = true;
	}

	/**
	 * Verify that a SELECT * is valid for this type of subquery.
	 *
	 * @param outerFromList	The FromList from the outer query block(s)
	 * @param subqueryType	The subquery type
	 *
	 * @return	None
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void verifySelectStarSubquery(FromList outerFromList, int subqueryType)
					throws StandardException
{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, 
					"verifySelectStarSubquery() is not expected to be called for " + 
					this.getClass().toString());
	}

	/**
	 * Expand "*" into a ResultColumnList with all of the columns
	 * in the table's result list.
	 *
	 * @param allTableName		The qualifier on the "*"
	 *
	 * @return ResultColumnList The expanded list
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultColumnList getAllResultColumns(String allTableName)
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
							 "getAllResultColumns() not expected to be called for " + this.getClass().getName() + this);
		return null;
	}

	/**
	 * Try to find a ResultColumn in the table represented by this FromTable
	 * that matches the name in the given ColumnReference.
	 *
	 * @param columnReference	The columnReference whose name we're looking
	 *				for in the given table.
	 *
	 * @return	A ResultColumn whose expression is the ColumnNode
	 *			that matches the ColumnReference.
	 *		Returns null if there is no match.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultColumn getMatchingColumn(
						ColumnReference columnReference)
						throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
							 "getMatchingColumn() not expected to be called for " + this);
		return null;
	}

	/**
	 * Set the result column for the subquery to a boolean true,
	 * Useful for transformations such as
	 * changing:
	 *		where exists (select ... from ...) 
	 * to:
	 *		where (select true from ...)
	 *
	 * NOTE: No transformation is performed if the ResultColumn.expression is
	 * already the correct boolean constant.
	 * 
	 * @param onlyConvertAlls	Boolean, whether or not to just convert *'s
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setResultToBooleanTrueNode(boolean onlyConvertAlls)
				throws StandardException
	{
		BooleanConstantNode	booleanNode;
		ResultColumn		resultColumn;

		/* We need to be able to handle both ResultColumn and AllResultColumn
		 * since they are peers.
		 */
		if (resultColumns.elementAt(0) instanceof AllResultColumn)
		{
			resultColumn = (ResultColumn) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN,
												"",
												null,
												getContextManager());
		}
		else if (onlyConvertAlls)
		{
			return;
		}
		else
		{
			resultColumn = (ResultColumn) resultColumns.elementAt(0);
	
			/* Nothing to do if query is already select TRUE ... */
			if (resultColumn.getExpression().isBooleanTrue())
			{
				return;
			}
		}
		
		booleanNode = (BooleanConstantNode) getNodeFactory().getNode(
										C_NodeTypes.BOOLEAN_CONSTANT_NODE,
										Boolean.TRUE,
										getContextManager());

		resultColumn.setExpression(booleanNode);
		resultColumn.setType(booleanNode.getTypeServices());
		/* VirtualColumnIds are 1-based, RCLs are 0-based */
		resultColumn.setVirtualColumnId(1);
		resultColumns.setElementAt(resultColumn, 0);
	}

	/**
	 * Get the FromList.  Create and return an empty FromList.  (Subclasses
	 * which actuall have FromLists will override this.)  This is useful because
	 * there is a FromList parameter to bindExpressions() which is used as
	 * the common FromList to bind against, allowing us to support
	 * correlation columns under unions in subqueries.
	 *
	 * @return FromList
	 * @exception StandardException		Thrown on error
	 */
	public FromList getFromList()
		throws StandardException
	{
		return (FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager());
	}

	/**
	 * Bind the result columns of this ResultSetNode when there is no
	 * base table to bind them to.  This is useful for SELECT statements,
	 * where the result columns get their types from the expressions that
	 * live under them.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindResultColumns(FromList fromListParam)
				throws StandardException
	{
		resultColumns.bindResultColumnsToExpressions();
	}

	/**
	 * Bind the result columns for this ResultSetNode to a base table.
	 * This is useful for INSERT and UPDATE statements, where the
	 * result columns get their types from the table being updated or
	 * inserted into.
	 * If a result column list is specified, then the verification that the 
	 * result column list does not contain any duplicates will be done when
	 * binding them by name.
	 *
	 * @param targetTableDescriptor	The TableDescriptor for the table being
	 *				updated or inserted into
	 * @param targetColumnList	For INSERT statements, the user
	 *					does not have to supply column
	 *					names (for example, "insert into t
	 *					values (1,2,3)".  When this
	 *					parameter is null, it means that
	 *					the user did not supply column
	 *					names, and so the binding should
	 *					be done based on order.  When it
	 *					is not null, it means do the binding
	 *					by name, not position.
	 * @param statement			Calling DMLStatementNode (Insert or Update)
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindResultColumns(TableDescriptor targetTableDescriptor,
					FromVTI targetVTI,
					ResultColumnList targetColumnList,
					DMLStatementNode statement,
					FromList fromListParam)
				throws StandardException
	{
		/* For insert select, we need to expand any *'s in the
		 * select before binding the result columns
		 */
		if (this instanceof SelectNode)
		{
			resultColumns.expandAllsAndNameColumns(((SelectNode)this).fromList);
		}

		/* If specified, copy the result column names down to the 
		 * source's target list.
		 */
		if (targetColumnList != null)
		{
			resultColumns.copyResultColumnNames(targetColumnList);
		}

		if (targetColumnList != null)
		{
			if (targetTableDescriptor != null)
			{
				resultColumns.bindResultColumnsByName(
						targetTableDescriptor, statement);
			}
			else
			{
				resultColumns.bindResultColumnsByName(
						targetVTI.getResultColumns(), targetVTI, statement);
			}
		}
		else
			resultColumns.bindResultColumnsByPosition(targetTableDescriptor);
	}

	/**
	 * Bind untyped nulls to the types in the given ResultColumnList.
	 * This is used for binding the nulls in row constructors and
	 * table constructors.  In all other cases (as of the time of
	 * this writing), we do nothing.
	 *
	 * @param rcl	The ResultColumnList with the types to bind nulls to
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindUntypedNullsToResultColumns(ResultColumnList rcl)
				throws StandardException
	{
		return;
	}

	/**
	 * Preprocess a ResultSetNode - this currently means:
	 *	o  Generating a referenced table map for each ResultSetNode.
	 *  o  Putting the WHERE and HAVING clauses in conjunctive normal form (CNF).
	 *  o  Converting the WHERE and HAVING clauses into PredicateLists and
	 *	   classifying them.
	 *  o  Ensuring that a ProjectRestrictNode is generated on top of every 
	 *     FromBaseTable and generated in place of every FromSubquery.  
	 *  o  Pushing single table predicates down to the new ProjectRestrictNodes.
	 *
	 * @param numTables			The number of tables in the DML Statement
	 * @param gbl				The group by list, if any
	 * @param fromList			The from list, if any
	 *
	 * @return ResultSetNode at top of preprocessed tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
					"preprocess() not expected to be called for " + getClass().toString());
		return null;
	}

    /**
     * Find the unreferenced result columns and project them out.
     */
    void projectResultColumns() throws StandardException
    {
        // It is only necessary for joins
    }

	/**
	 * Ensure that the top of the RSN tree has a PredicateList.
	 *
	 * @param numTables			The number of tables in the query.
	 * @return ResultSetNode	A RSN tree with a node which has a PredicateList on top.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode ensurePredicateList(int numTables) 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
					"ensurePredicateList() not expected to be called for " + getClass().toString());
		return null;
	}

	/**
	 * Add a new predicate to the list.  This is useful when doing subquery
	 * transformations, when we build a new predicate with the left side of
	 * the subquery operator and the subquery's result column.
	 *
	 * @param predicate		The predicate to add
	 *
	 * @return ResultSetNode	The new top of the tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode addNewPredicate(Predicate predicate)
			throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
					"addNewPredicate() not expected to be called for " + getClass().toString());
		return null;
	}

	/**
	 * Evaluate whether or not the subquery in a FromSubquery is flattenable.  
	 * Currently, a FSqry is flattenable if all of the following are true:
	 *		o  Subquery is a SelectNode. (ie, not a RowResultSetNode or a UnionNode)
	 *		o  It contains no top level subqueries.  (RESOLVE - we can relax this)
	 *		o  It does not contain a group by or having clause
	 *		o  It does not contain aggregates.
	 *
	 * @param fromList	The outer from list
	 *
	 * @return boolean	Whether or not the FromSubquery is flattenable.
	 */
	public boolean flattenableInFromSubquery(FromList fromList)
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
					"flattenableInFromSubquery() not expected to be called for " + getClass().toString());
		return false;
	}

	/**
	 * Get a parent ProjectRestrictNode above us.
	 * This is useful when we need to preserve the
	 * user specified column order when reordering the
	 * columns in the distinct when we combine
	 * an order by with a distinct.
	 *
	 * @return A parent ProjectRestrictNode to do column reordering
	 *
	 * @exception StandardException		Thrown on error
	 */
	ResultSetNode genProjectRestrictForReordering()
				throws StandardException
	{
		ResultColumnList	prRCList;

		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		prRCList = resultColumns;
		resultColumns = resultColumns.copyListAndObjects();

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 * NOTE: We don't want to mark the underlying RCs as referenced, otherwise
		 * we won't be able to project out any of them.
		 */
		prRCList.genVirtualColumnNodes(this, resultColumns, false);

		/* Finally, we create the new ProjectRestrictNode */
		return (ResultSetNode) getNodeFactory().getNode(
								C_NodeTypes.PROJECT_RESTRICT_NODE,
								this,
								prRCList,
								null,	/* Restriction */
								null,   /* Restriction as PredicateList */
								null,	/* Project subquery list */
								null,	/* Restrict subquery list */
								null,
								getContextManager()	 );
	}

	/**
	 * Optimize a ResultSetNode. This means choosing the best access
	 * path for each table under the ResultSetNode, among other things.
	 * 
	 * The only RSNs that need to implement their own optimize() are a 
	 * SelectNode and those RSNs that can appear above a SelectNode in the 
	 * query tree.  Currently, a ProjectRestrictNode is the only RSN that 
	 * can appear above a SelectNode.
	 *
	 * @param dataDictionary	The DataDictionary to use for optimization
	 * @param predicateList		The PredicateList to apply.
	 * @param outerRows			The number of outer joining rows
	 *
	 * @return	ResultSetNode	The top of the optimized query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode optimize(DataDictionary dataDictionary,
								  PredicateList predicates,
								  double outerRows) 
				throws StandardException	
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, 
					"optimize() is not expected to be called for " + 
					this.getClass().toString());
		return null;
	}

	/**
	 * Modify the access paths according to the decisions the optimizer
	 * made.  This can include adding project/restrict nodes,
	 * index-to-base-row nodes, etc.
	 *
	 * @return	The modified query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode modifyAccessPaths() throws StandardException
	{
		/* Default behavior is to do nothing */
		return this;
	}

	ResultColumnDescriptor[] makeResultDescriptors(ExecutionContext ec)
	{
	    return resultColumns.makeResultDescriptors(ec);
	}

	/*
	** Check whether the column lengths and types of the result columns
	** match the expressions under those columns.  This is useful for
	** INSERT and UPDATE statements.  For SELECT statements this method
	** should always return true.  There is no need to call this for a
	** DELETE statement.
	**
	** @return	true means all the columns match their expressions,
	**		false means at least one column does not match its
	**		expression
	*/

	boolean columnTypesAndLengthsMatch()
		throws StandardException
	{
		return resultColumns.columnTypesAndLengthsMatch();
	}

	/**
	 * Set the resultColumns in this ResultSetNode
	 *
	 * @param resultColumns		The new ResultColumnList for this ResultSetNode
	 *
	 * @return None.
	 */
	public void setResultColumns(ResultColumnList newRCL)
	{
		resultColumns = newRCL;
	}

	/**
	 * Get the resultColumns for this ResultSetNode
	 *
	 * @return ResultColumnList for this ResultSetNode
	 */
	public ResultColumnList getResultColumns()
	{
		return resultColumns;
	}

	/**
	 * Set the referencedTableMap in this ResultSetNode
	 *
	 * @param newRTM	The new referencedTableMap for this ResultSetNode
	 *
	 * @return None.
	 */
	public void setReferencedTableMap(JBitSet newRTM)
	{
		referencedTableMap = newRTM;
	}

	/**
	 * Get the referencedTableMap for this ResultSetNode
	 *
	 * @return JBitSet	Referenced table map for this ResultSetNode
	 */
	public JBitSet getReferencedTableMap()
	{
		return referencedTableMap;
	}

	/**
	 * Fill the referencedTableMap with this ResultSetNode.
	 *
	 * @param passedMap	The table map to fill in.
	 *
	 * @return Nothing.
	 */
	public void fillInReferencedTableMap(JBitSet passedMap)
	{
	}

	/**
	 * Check for (and reject) ? parameters directly under the ResultColumns.
	 * This is done for SELECT statements.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown if a ? parameter found
	 *									directly under a ResultColumn
	 */

	public void rejectParameters() throws StandardException
	{
		/* Okay if no resultColumns yet - means no parameters there */
		if (resultColumns != null)
		{
			resultColumns.rejectParameters();
		}
	}

	/**
	 * Rename generated result column names as '1', '2' etc... These will be the result
	 * column names seen by JDBC clients.
	 */
	public void renameGeneratedResultNames() throws StandardException
	{
		for (int i=0; i<resultColumns.size(); i++)
		{
			ResultColumn rc = (ResultColumn) resultColumns.elementAt(i);
			if (rc.isNameGenerated())
				rc.setName(Integer.toString(i+1));
		}
	}

	/**
		This method is overridden to allow a resultset node to know
		if it is the one controlling the statement -- i.e., it is
		the outermost result set node for the statement.
	 */
	public void markStatementResultSet()
	{
		statementResultSet = true;
	}

	/**
		This utility method is used by result set nodes that can be
		statement nodes to determine what their final argument is;
		if they are the statement result set, and there is a current
		date/time request, then a method will have been generated.
		Otherwise, a simple null is passed in to the result set method.
	 */
	void closeMethodArgument(ExpressionClassBuilder acb,
									MethodBuilder mb)
	{
		/*
			For supporting current datetime, we may have a method
			that needs to be called when the statement's result set 
			is closed.
		 */
		if (statementResultSet)
		{
			acb.pushResultSetClosedMethodFieldAccess(mb);
		}
		else
		{
			mb.pushNull(ClassName.GeneratedMethod);
		}
	}

	/**
	 * This ResultSet is the source for an Insert.  The target RCL
	 * is in a different order and/or a superset of this RCL.  In most cases
	 * we will reorder and/or add defaults to the current RCL so that is
	 * matches the target RCL.  Those RSNs whose generate() method does
	 * not handle projects will insert a PRN, with a new RCL which matches
	 * the target RCL, above the current RSN.
	 * NOTE - The new or enhanced RCL will be fully bound.
	 *
	 * @param numTargetColumns	# of columns in target RCL
	 * @param colMap[]			int array representation of correspondence between
	 *							RCLs - colmap[i] = -1 -> missing in current RCL
	 *								   colmap[i] = j -> targetRCL(i) <-> thisRCL(j+1)
	 * @param dataDictionary	DataDictionary to use
	 * @param targetTD			TableDescriptor for target if the target is not a VTI, null if a VTI
     * @param targetVTI         Target description if it is a VTI, null if not a VTI
	 *
	 * @return ResultSetNode	The new top of the tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode enhanceRCLForInsert(int numTargetColumns, int[] colMap, 
											 DataDictionary dataDictionary,
											 TableDescriptor targetTD,
                                             FromVTI targetVTI)
			throws StandardException
	{
		// our newResultCols are put into the bound form straight away.
		ResultColumnList newResultCols =
							(ResultColumnList) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager());
		int numResultSetColumns = resultColumns.size();

		/* Create a massaged version of the source RCL.
		 * (Much simpler to build new list and then assign to source,
		 * rather than massage the source list in place.)
		 */
		for (int index = 0; index < numTargetColumns; index++)
		{
			ResultColumn	newResultColumn = null;
			ColumnReference newColumnReference;

			if (colMap[index] != -1)
			{
				// getResultColumn uses 1-based positioning, so offset the colMap entry appropriately
				newResultColumn = resultColumns.getResultColumn(colMap[index]+1);
			}
			else
			{
				newResultColumn = genNewRCForInsert(targetTD, targetVTI, index + 1, dataDictionary);
			}

			newResultCols.addResultColumn(newResultColumn);
		}

		/* Set the source RCL to the massaged version */
		resultColumns = newResultCols;

		return this;
	}

	/**
	 * Generate the RC/expression for an unspecified column in an insert.
	 * Use the default if one exists.
	 *
	 * @param targetTD			Target TableDescriptor if the target is not a VTI, null if a VTI.
     * @param targetVTI         Target description if it is a VTI, null if not a VTI
	 * @param columnNumber		The column number
	 * @param dataDictionary	The DataDictionary
	 * @return	The RC/expression for the unspecified column.
	 *
	 * @exception StandardException		Thrown on error
	 */
	ResultColumn genNewRCForInsert(TableDescriptor targetTD,
                                   FromVTI targetVTI,
                                   int columnNumber,
								   DataDictionary dataDictionary)
		throws StandardException
	{
		ResultColumn newResultColumn = null;

		// the i-th column's value was not specified, so create an
		// expression containing its default value (null for now)
		// REVISIT: will we store trailing nulls?

        if( targetVTI != null)
        {
            newResultColumn = targetVTI.getResultColumns().getResultColumn( columnNumber);
            newResultColumn = newResultColumn.cloneMe();
            newResultColumn.setExpressionToNullNode();
        }
        else
        {
            // column position is 1-based, index is 0-based.
            ColumnDescriptor colDesc = targetTD.getColumnDescriptor(columnNumber);
            DataTypeDescriptor colType = colDesc.getType();

            // Check for defaults
            DefaultInfoImpl defaultInfo = (DefaultInfoImpl) colDesc.getDefaultInfo();

            if (defaultInfo != null)
            {
                //RESOLVEPARAMETER - skip the tree if we have the value
                /*
                  if (defaultInfo.getDefaultValue() != null)
                  {
                  }
                  else
                */
                {
                    // Generate the tree for the default
                    String defaultText = defaultInfo.getDefaultText();
                    ValueNode defaultTree = parseDefault(defaultText);
                    defaultTree = defaultTree.bindExpression(
                        getFromList(),
                        (SubqueryList) null,
                        (Vector) null);
                    newResultColumn = (ResultColumn) getNodeFactory().getNode(
                        C_NodeTypes.RESULT_COLUMN,
                        defaultTree.getTypeServices(),
                        defaultTree,
                        getContextManager());

                    DefaultDescriptor defaultDescriptor = colDesc.getDefaultDescriptor(dataDictionary);
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.ASSERT(defaultDescriptor != null,
                                             "defaultDescriptor expected to be non-null");
                    }
                    getCompilerContext().createDependency(defaultDescriptor); 
                }
            }
            else if (colDesc.isAutoincrement())
            {
                newResultColumn = 
                  (ResultColumn)getNodeFactory().getNode(
                      C_NodeTypes.RESULT_COLUMN,
                      colDesc, null,
                      getContextManager());
                newResultColumn.setAutoincrementGenerated();
            }
            else
            {
                newResultColumn = (ResultColumn) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    colType,
                    getNullNode(
                        colType.getTypeId(),
                        getContextManager()
                        ),
                    getContextManager()
                    );
            }
        }

		// Mark the new RC as generated for an unmatched column in an insert
		newResultColumn.markGeneratedForUnmatchedColumnInInsert();

		return newResultColumn;
	}

	/**
	  *	Parse a default and turn it into a query tree.
	  *
	  *	@param	defaultText			Text of Default.
	  *
	  * @return	The parsed default as a query tree.
	  *
	  * @exception StandardException		Thrown on failure
	  */
	public	ValueNode	parseDefault
	(
		String				defaultText
    )
		throws StandardException
	{
		Parser						p;
		ValueNode					defaultTree;
		LanguageConnectionContext	lcc = getLanguageConnectionContext();
		CompilerContext 			compilerContext = getCompilerContext();

		/* Get a Statement to pass to the parser */

		/* We're all set up to parse. We have to build a compilable SQL statement
		 * before we can parse -  So, we goober up a VALUES defaultText.
		 */
		String values = "VALUES " + defaultText;
		
		/*
		** Get a new compiler context, so the parsing of the select statement
		** doesn't mess up anything in the current context (it could clobber
		** the ParameterValueSet, for example).
		*/
		CompilerContext newCC = lcc.pushCompilerContext();

		p = newCC.getParser();
				
		/* Finally, we can call the parser */
		// Since this is always nested inside another SQL statement, so topLevel flag
		// should be false
		QueryTreeNode qt = p.parseStatement(values);
		if (SanityManager.DEBUG)
		{
			if (! (qt instanceof CursorNode))
			{
				SanityManager.THROWASSERT(
					"qt expected to be instanceof CursorNode, not " +
					qt.getClass().getName());
			}
			CursorNode cn = (CursorNode) qt;
			if (! (cn.getResultSetNode() instanceof RowResultSetNode))
			{
				SanityManager.THROWASSERT(
					"cn.getResultSetNode() expected to be instanceof RowResultSetNode, not " +
					cn.getResultSetNode().getClass().getName());
			}
		}

		defaultTree = ((ResultColumn) 
							((CursorNode) qt).getResultSetNode().getResultColumns().elementAt(0)).
									getExpression();

		lcc.popCompilerContext(newCC);

		return	defaultTree;
	}

	/**
	 * Make a ResultDescription for use in a ResultSet.
	 * This is useful when generating/executing a NormalizeResultSet, since
	 * it can appear anywhere in the tree.
	 *
	 * @return	A ResultDescription for this ResultSetNode.
	 */

	public ResultDescription makeResultDescription()
	{
		ExecutionContext ec = (ExecutionContext) getContextManager().getContext(
			ExecutionContext.CONTEXT_ID);
	    ResultColumnDescriptor[] colDescs = makeResultDescriptors(ec);

	    return ec.getExecutionFactory().getResultDescription(colDescs, null );
	}

	/**
		Determine if this result set is updatable or not, for a cursor
		(i.e., is it a cursor-updatable select).  This returns false
		and we expect selectnode to refine it for further checking.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean isUpdatableCursor(DataDictionary dd) throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.DEBUG("DumpUpdateCheck","cursor is not a select result set");
		return false;
	}

	/**
		return the target table of an updatable cursor result set.
		since this is not updatable, just return null.
	 */
	FromTable getCursorTargetTable()
	{
		return null;
	}

	/**
		Mark this ResultSetNode as the target table of an updatable
		cursor.  Most types of ResultSetNode can't be target tables.
		@return true if the target table supports positioned updates.
	 */
	public boolean markAsCursorTargetTable()
	{
		return false;
	}

	/**
		Mark this ResultSetNode as *not* the target table of an updatable
		cursor.
	 */
	void notCursorTargetTable()
	{
		cursorTargetTable = false;
	}

	/** 
	 * Put a ProjectRestrictNode on top of this ResultSetNode.
	 * ColumnReferences must continue to point to the same ResultColumn, so
	 * that ResultColumn must percolate up to the new PRN.  However,
	 * that ResultColumn will point to a new expression, a VirtualColumnNode, 
	 * which points to the FromTable and the ResultColumn that is the source for
	 * the ColumnReference.  
	 * (The new PRN will have the original of the ResultColumnList and
	 * the ResultColumns from that list.  The FromTable will get shallow copies
	 * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
	 * will remain at the FromTable, with the PRN getting a new 
	 * VirtualColumnNode for each ResultColumn.expression.)
	 *
	 * This is useful for UNIONs, where we want to generate a DistinctNode above
	 * the UnionNode to eliminate the duplicates, because DistinctNodes expect
	 * their immediate child to be a PRN.
	 *
	 * @return The generated ProjectRestrictNode atop the original ResultSetNode.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode genProjectRestrict()
				throws StandardException
	{
		ResultColumnList	prRCList;

		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		prRCList = resultColumns;
		resultColumns = resultColumns.copyListAndObjects();

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
		prRCList.genVirtualColumnNodes(this, resultColumns);

		/* Finally, we create the new ProjectRestrictNode */
		return (ResultSetNode) getNodeFactory().getNode(
								C_NodeTypes.PROJECT_RESTRICT_NODE,
								this,
								prRCList,
								null,	/* Restriction */
								null,   /* Restriction as PredicateList */
								null,	/* Project subquery list */
								null,	/* Restrict subquery list */
								null,
								getContextManager()				 );
	}

	/** 
	 * Put a ProjectRestrictNode on top of each FromTable in the FromList.
	 * ColumnReferences must continue to point to the same ResultColumn, so
	 * that ResultColumn must percolate up to the new PRN.  However,
	 * that ResultColumn will point to a new expression, a VirtualColumnNode, 
	 * which points to the FromTable and the ResultColumn that is the source for
	 * the ColumnReference.  
	 * (The new PRN will have the original of the ResultColumnList and
	 * the ResultColumns from that list.  The FromTable will get shallow copies
	 * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
	 * will remain at the FromTable, with the PRN getting a new 
	 * VirtualColumnNode for each ResultColumn.expression.)
	 * We then project out the non-referenced columns.  If there are no referenced
	 * columns, then the PRN's ResultColumnList will consist of a single ResultColumn
	 * whose expression is 1.
	 *
	 * @param numTables			Number of tables in the DML Statement
	 *
	 * @return The generated ProjectRestrictNode atop the original FromTable.
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected ResultSetNode genProjectRestrict(int numTables)
				throws StandardException
	{
		return genProjectRestrict();
	}

	/** 
	 * Put a NormalizeResultSetNode on top of the specified ResultSetNode.
	 * ColumnReferences must continue to point to the same ResultColumn, so
	 * that ResultColumn must percolate up to the new PRN.  However,
	 * that ResultColumn will point to a new expression, a VirtualColumnNode, 
	 * which points to the FromTable and the ResultColumn that is the source for
	 * the ColumnReference.  
	 * (The new NRSN will have the original of the ResultColumnList and
	 * the ResultColumns from that list.  The FromTable will get shallow copies
	 * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
	 * will remain at the FromTable, with the PRN getting a new 
	 * VirtualColumnNode for each ResultColumn.expression.)
	 *
	 * This is useful for UNIONs, where we want to generate a DistinctNode above
	 * the UnionNode to eliminate the duplicates, because the type going into the
	 * sort has to agree with what the sort expects.
	 * (insert into t1 (smallintcol) values 1 union all values 2;
	 *
	 * @param normalizeChild	Child result set for new NRSN.
	 * @param forUpdate			If the normalize result set is being used as a
	 * child for an update statement, then this is true. 
	 *
	 * @return The generated NormalizeResultSetNode atop the original UnionNode.
	 *
	 * @exception StandardException		Thrown on error
	 * @see NormalizeResultSetNode#init
	 */

	public NormalizeResultSetNode 
		genNormalizeResultSetNode(ResultSetNode	normalizeChild, 
								  boolean forUpdate)
				throws StandardException
	{
		NormalizeResultSetNode	nrsn;
		ResultColumnList		prRCList;

		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		prRCList = resultColumns;
		resultColumns = resultColumns.copyListAndObjects();

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the NormalizeResultSetNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
		prRCList.genVirtualColumnNodes(this, resultColumns);

		/* Finally, we create the new NormalizeResultSetNode */
		nrsn = (NormalizeResultSetNode) getNodeFactory().getNode(
								C_NodeTypes.NORMALIZE_RESULT_SET_NODE,
								normalizeChild,
								prRCList,
								null, new Boolean(forUpdate),
								getContextManager());
		// Propagate the referenced table map if it's already been created
		if (normalizeChild.getReferencedTableMap() != null)
		{
			nrsn.setReferencedTableMap((JBitSet) normalizeChild.getReferencedTableMap().clone());
		}
		return nrsn;
	}

	/**
	 * Generate the code for a NormalizeResultSet.
	   The call must push two items before calling this method
	   <OL>
	   <LI> pushGetResultSetFactoryExpression
	   <LI> the expression to normalize
	   </OL>
	 *
	 * @param acb				The ActivationClassBuilder
	 * @param mb				The method to put the generated code in
	 * @param resultSetNumber	The result set number for the NRS
	 * @param resultDescription	The ERD for the ResultSet
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateNormalizationResultSet(
						ActivationClassBuilder acb,
						MethodBuilder mb, 
						int resultSetNumber,
						ResultDescription resultDescription)
			throws StandardException
	{
		int erdNumber = acb.addItem(resultDescription);

		// instance and first arg are pushed by caller

		acb.pushThisAsActivation(mb);
		mb.push(resultSetNumber);
		mb.push(erdNumber);
		mb.push(getCostEstimate().rowCount());
		mb.push(getCostEstimate().getEstimatedCost());
		mb.push(false);
		closeMethodArgument(acb, mb);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getNormalizeResultSet",
					ClassName.NoPutResultSet, 8);
	}

	/**
	 * The optimizer's decision on the access path for a result set
	 * may require the generation of extra result sets.  For example,
	 * if it chooses an index for a FromBaseTable, we need an IndexToBaseRowNode
	 * above the FromBaseTable (and the FromBaseTable has to change its
	 * column list to match the index.
	 *
	 * This method in the parent class does not generate any extra result sets.
	 * It may be overridden in child classes.
	 *
	 * @return	A ResultSetNode tree modified to do any extra processing for
	 *			the chosen access path
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode changeAccessPath() throws StandardException
	{
		return this;
	}

	/**
	 * Search to see if a query references the specifed table name.
	 *
	 * @param name		Table name (String) to search for.
	 * @param baseTable	Whether or not name is for a base table
	 *
	 * @return	true if found, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		return false;
	}

	/**
	 * Return whether or not this ResultSetNode contains a subquery with a
	 * reference to the specified target.
	 * 
	 * @param name	The table name.
	 *
	 * @return boolean	Whether or not a reference to the table was found.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean subqueryReferencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		return false;
	}

	/**
	 * Return whether or not the underlying ResultSet tree will return
	 * a single row, at most.
	 * This is important for join nodes where we can save the extra next
	 * on the right side if we know that it will return at most 1 row.
	 *
	 * @return Whether or not the underlying ResultSet tree will return a single row.
	 * @exception StandardException		Thrown on error
	 */
	public boolean isOneRowResultSet()	throws StandardException
	{
		// Default is false
		return false;
	}

	/**
	 * Return whether or not the underlying ResultSet tree is for a NOT EXISTS
	 * join.
	 *
	 * @return Whether or not the underlying ResultSet tree if for NOT EXISTS.
	 */
	public boolean isNotExists()
	{
		// Default is false
		return false;
	}

	/**
	 * Get an optimizer to use for this ResultSetNode.  Only get it once -
	 * subsequent calls return the same optimizer.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected Optimizer getOptimizer(
							OptimizableList optList,
							OptimizablePredicateList predList,
							DataDictionary dataDictionary,
							RequiredRowOrdering requiredRowOrdering)
			throws StandardException
	{
		if (optimizer == null)
		{
			/* Get an optimizer. */
			OptimizerFactory optimizerFactory = getLanguageConnectionContext().getOptimizerFactory();

			optimizer = optimizerFactory.getOptimizer(
											optList,
											predList,
											dataDictionary,
											requiredRowOrdering,
											getCompilerContext().getNumTables(),
								getLanguageConnectionContext());
		}

		return optimizer;
	}

	/**
	 * Get a cost estimate to use for this ResultSetNode.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected CostEstimate getNewCostEstimate()
			throws StandardException
	{
		OptimizerFactory optimizerFactory = getLanguageConnectionContext().getOptimizerFactory();
		return optimizerFactory.getCostEstimate();
	}

	/**
	 * Accept a visitor, and call v.visit()
	 * on child nodes as necessary.  
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	public Visitable accept(Visitor v) 
		throws StandardException
	{
		Visitable returnNode = v.visit(this);

		if (v.skipChildren(this))
		{
			return returnNode;
		}

		if (resultColumns != null && !v.stopTraversal())
		{
			resultColumns = (ResultColumnList)resultColumns.accept(v);
		}
		return returnNode;
	}

	/**
	 * Consider materialization for this ResultSet tree if it is valid and cost effective
	 * (It is not valid if incorrect results would be returned.)
	 *
	 * @return Top of the new/same ResultSet tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode considerMaterialization(JBitSet outerTables)
		throws StandardException
	{
		return this;
	}

	/**
	 * Return whether or not to materialize this ResultSet tree.
	 *
	 * @return Whether or not to materialize this ResultSet tree.
	 *			would return valid results.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean performMaterialization(JBitSet outerTables)
		throws StandardException
	{
		return false;
	}

	/** 
	 * Determine whether or not the specified name is an exposed name in
	 * the current query block.
	 *
	 * @param name	The specified name to search for as an exposed name.
	 * @param schemaName	Schema name, if non-null.
	 * @param exactMatch	Whether or not we need an exact match on specified schema and table
	 *						names or match on table id.
	 *
	 * @return The FromTable, if any, with the exposed name.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected FromTable getFromTableByName(String name, String schemaName, boolean exactMatch)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("getFromTableByName() not expected to be called for " +
									  getClass().getName());
		}
		return null;
	}

	/**
	 * Decrement (query block) level (0-based) for 
	 * all of the tables in this ResultSet tree.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 *
	 * @return Nothing;
	 */
	abstract void decrementLevel(int decrement);

	/**
	 * Push the order by list down from the cursor node
	 * into its child result set so that the optimizer
	 * has all of the information that it needs to 
	 * consider sort avoidance.
	 *
	 * @param orderByList	The order by list
	 *
	 * @return Nothing.
	 */
	void pushOrderByList(OrderByList orderByList)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("pushOrderByList() not expected to be called for " +
									  getClass().getName());
		}
	}

	/**
	 * General logic shared by Core compilation and by the Replication Filter
	 * compiler. A couple ResultSets (the ones used by PREPARE SELECT FILTER)
	 * implement this method.
	 *
	 * @param ecb	The ExpressionClassBuilder for the class being built
	 * @param eb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateResultSet(ExpressionClassBuilder acb,
										   MethodBuilder mb)
									throws StandardException
	{
		System.out.println("I am a " + getClass());
		if (SanityManager.DEBUG)
			SanityManager.NOTREACHED();
		return;
	}


	/**
	 * Get the lock mode for the target of an update statement
	 * (a delete or update).  The update mode will always be row for
	 * CurrentOfNodes.  It will be table if there is no where clause.
	 *
	 * @see TransactionController
	 *
	 * @return	The lock mode
	 */
	public int updateTargetLockMode()
	{
		return TransactionController.MODE_TABLE;
	}

	/**
	 * Mark this node and its children as not being a flattenable join.
	 *
	 * @return Nothing.
	 */
	void notFlattenableJoin()
	{
	}

	/**
	 * Return whether or not the underlying ResultSet tree
	 * is ordered on the specified columns.
	 * RESOLVE - This method currently only considers the outermost table 
	 * of the query block.
	 *
	 * @param	crs					The specified ColumnReference[]
	 * @param	permuteOrdering		Whether or not the order of the CRs in the array can be permuted
	 * @param	fbtVector			Vector that is to be filled with the FromBaseTable	
	 *
	 * @return	Whether the underlying ResultSet tree
	 * is ordered on the specified column.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean isOrderedOn(ColumnReference[] crs, boolean permuteOrdering, Vector fbtVector)
				throws StandardException
	{
		return false;
	}

	/**
	 * Return whether or not this ResultSet tree is guaranteed to return
	 * at most 1 row based on heuristics.  (A RowResultSetNode and a
	 * SELECT with a non-grouped aggregate will return at most 1 row.)
	 *
	 * @return Whether or not this ResultSet tree is guaranteed to return
	 * at most 1 row based on heuristics.
	 */
	boolean returnsAtMostOneRow()
	{
		return false;
	}

	/**
	 * Replace any DEFAULTs with the associated tree for the default.
	 *
	 * @param ttd	The TableDescriptor for the target table.
	 * @param tcl	The RCL for the target table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void replaceDefaults(TableDescriptor ttd, ResultColumnList tcl) 
		throws StandardException
	{
		// Only subclasses with something to do override this.
	}

	/**
	 * Is it possible to do a distinct scan on this ResultSet tree.
	 * (See SelectNode for the criteria.)
	 *
	 * @return Whether or not it is possible to do a distinct scan on this ResultSet tree.
	 */
	boolean isPossibleDistinctScan()
	{
		return false;
	}

	/**
	 * Mark the underlying scan as a distinct scan.
	 *
	 * @return Nothing.
	 */
	void markForDistinctScan()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"markForDistinctScan() not expected to be called for " +
				getClass().getName());
		}
	}

	/**
	 * Notify the underlying result set tree that the result is
	 * ordering dependent.  (For example, no bulk fetch on an index
	 * if under an IndexRowToBaseRow.)
	 *
	 * @return Nothing.
	 */
	void markOrderingDependent()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"markOrderingDependent() not expected to be called for " +
				getClass().getName());
		}
	}


	/**
	 * Count the number of distinct aggregates in the list.
	 * By 'distinct' we mean aggregates of the form:
	 *	<UL><I>SELECT MAX(DISTINCT x) FROM T<\I><\UL>
	 *
	 * @return number of aggregates
	 */
	protected static final int numDistinctAggregates(Vector aggregateVector)
	{
		int		count = 0;
		int		size = aggregateVector.size();

		for (int index = 0; index < size; index++)
		{
			count += (((AggregateNode) aggregateVector.elementAt(index)).isDistinct() == true) ?
						1 : 0;
		}
		
		return count;
	}

	// It may be we have a SELECT view underneath a LOJ.
	// Return null for now.. we don't do any optimization.
	public JBitSet LOJgetReferencedTables(int numTables)
				throws StandardException
	{
		if (this instanceof FromTable)
		{
			if (((FromTable)this).tableNumber != -1)
			{
				JBitSet map = new JBitSet(numTables);
				map.set(((FromTable)this).tableNumber);
				return map;
			}
		}

		return null;
	}
	
}
