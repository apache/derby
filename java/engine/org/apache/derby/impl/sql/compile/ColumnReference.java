/*

   Derby - Class org.apache.derby.impl.sql.compile.ColumnReference

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

import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.NodeFactory;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.util.JBitSet;

import java.util.Vector;

/**
 * A ColumnReference represents a column in the query tree.  The parser generates a
 * ColumnReference for each column reference.  A column refercence could be a column in
 * a base table, a column in a view (which could expand into a complex
 * expression), or a column in a subquery in the FROM clause.
 *
 * @author Jerry Brenner
 */

public class ColumnReference extends ValueNode
{
	public String	columnName;

	/*
	** This is the user-specified table name.  It will be null if the
	** user specifies a column without a table name.  Leave it null even
	** when the column is bound as it is only used in binding.
	*/
	public TableName	tableName;
	/* The table this column reference is bound to */
	public int			tableNumber;	
	/* The column number in the underlying base table */
	public int			columnNumber;	
	/* This is where the value for this column reference will be coming from */
	public ResultColumn	source;

	/* For unRemapping */
	ResultColumn	origSource;
	public String	origName;
	int				origTableNumber = -1;
	int				origColumnNumber = -1;

	/* Reuse generated code where possible */
	//Expression genResult;

	private boolean		replacesAggregate;

	private int			nestingLevel = -1;
	private int			sourceLevel = -1;

	/*
	** These fields are used to track the being and end
	** offset of the token from which the column name came.
	*/
	private int		tokBeginOffset = -1;
	private int		tokEndOffset = -1;

	/**
	 * Initializer.
	 * This one is called by the parser where we could
	 * be dealing with delimited identifiers.
	 *
	 * @param columnName	The name of the column being referenced
	 * @param tableName		The qualification for the column
	 * @param tokBeginOffset begin position of token for the column name 
	 *					identifier from parser.
	 * @param tokEndOffset	end position of token for the column name 
	 *					identifier from parser.
	 */

	public void init(Object columnName, 
					 Object tableName,
			 		 Object	tokBeginOffset,
					 Object	tokEndOffset
					 )
	{
		this.columnName = (String) columnName;
		this.tableName = (TableName) tableName;
		this.tokBeginOffset = ((Integer) tokBeginOffset).intValue();
		this.tokEndOffset = ((Integer) tokEndOffset).intValue();
		tableNumber = -1;
	}

	/**
	 * Initializer.
	 *
	 * @param columnName	The name of the column being referenced
	 * @param tableName		The qualification for the column
	 */

	public void init(Object columnName, Object tableName)
	{
		this.columnName = (String) columnName;
		this.tableName = (TableName) tableName;
		tableNumber = -1;
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
			return "columnName: " + columnName + "\n" +
				"tableNumber: " + tableNumber + "\n" +
				"columnNumber: " + columnNumber + "\n" +
				"replacesAggregate: " + replacesAggregate + "\n" +
				( ( tableName != null) ?
						tableName.toString() :
						"tableName: null\n") +
				"nestingLevel: " + nestingLevel + "\n" +
				"sourceLevel: " + sourceLevel + "\n" +
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

			if (source != null)
			{
				printLabel(depth, "source: ");
				source.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Get the begin offset of the parser token for the column name
	 * Will only be set when the CR was generated by the
	 * parser.
	 *
	 * @return the begin offset of the token.  -1 means unknown
	 */
	public int getTokenBeginOffset()
	{
		/* We should never get called if not initialized, as
		 * begin/end offset has no meaning unless this CR was
		 * created in the parser.
		 */
		if (SanityManager.DEBUG)
		{
			if (tokBeginOffset == -1)
			{
				SanityManager.THROWASSERT(
					"tokBeginOffset not expected to be -1");
			}
		}
		return tokBeginOffset;
	}

	/**
	 * Get the end offset of the parser token for the column name.
	 * Will only be set when the CR was generated by the
	 * parser.
	 *
	 * @return the end offset of the token.  -1 means unknown
	 */
	public int getTokenEndOffset()
	{
		/* We should never get called if not initialized, as
		 * begin/end offset has no meaning unless this CR was
		 * created in the parser.
		 */
		if (SanityManager.DEBUG)
		{
			if (tokEndOffset == -1)
			{
				SanityManager.THROWASSERT(
					"tokEndOffset not expected to be -1");
			}
		}
		return tokEndOffset;
	}

	/**
	 * Return whether or not this CR is correlated.
	 *
	 * @return Whether or not this CR is correlated.
	 */
	boolean getCorrelated()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(nestingLevel != -1,
				"nestingLevel on "+columnName+" is not expected to be -1");
			SanityManager.ASSERT(sourceLevel != -1,
				"sourceLevel on "+columnName+" is not expected to be -1");
		}
		return sourceLevel != nestingLevel;
	}

	/**
	 * Set the nesting level for this CR.  (The nesting level
	 * at which the CR appears.)
	 *
	 * @param nestingLevel	The Nesting level at which the CR appears.
	 *
	 * @return Nothing.
	 */
	void setNestingLevel(int nestingLevel)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(nestingLevel != -1,
				"nestingLevel is not expected to be -1");
		}
		this.nestingLevel = nestingLevel;
	}

	/**
	 * Get the nesting level for this CR.
	 *
	 * @return	The nesting level for this CR.
	 */
	int getNestingLevel()
	{
		return nestingLevel;
	}

	/**
	 * Set the source level for this CR.  (The nesting level
	 * of the source of the CR.)
	 *
	 * @param sourceLevel	The Nesting level of the source of the CR.
	 *
	 * @return Nothing.
	 */
	void setSourceLevel(int sourceLevel)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(sourceLevel != -1,
				"sourceLevel is not expected to be -1");
		}
		this.sourceLevel = sourceLevel;
	}

	/**
	 * Get the source level for this CR.
	 *
	 * @return	The source level for this CR.
	 */
	int getSourceLevel()
	{
		return sourceLevel;
	}

	/**
	 * Mark this node as being generated to replace an aggregate.
	 * (Useful for replacing aggregates in the HAVING clause with 
	 * column references to the matching aggregate in the 
	 * user's SELECT.
	 *
	 * @return Nothing.
	 */
	public void markGeneratedToReplaceAggregate()
	{
		replacesAggregate = true;
	}

	/**
	 * Determine whether or not this node was generated to
	 * replace an aggregate in the user's SELECT.
	 *
	 * @return boolean	Whether or not this node was generated to replace
	 *					an aggregate in the user's SELECT.
	 */
	public boolean getGeneratedToReplaceAggregate()
	{
		return replacesAggregate;
	}

	/**
	 * Return a clone of this node.
	 *
	 * @return ValueNode	A clone of this node.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public ValueNode getClone()
		throws StandardException
	{
		ColumnReference newCR = (ColumnReference) getNodeFactory().getNode(
									C_NodeTypes.COLUMN_REFERENCE,
									columnName,
									tableName,
									getContextManager());

		newCR.copyFields(this);
		return newCR;
	}

	/**
	 * Copy all of the "appropriate fields" for a shallow copy.
	 *
	 * @param oldCR		The ColumnReference to copy from.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public void copyFields(ColumnReference oldCR)
		throws StandardException
	{
		super.copyFields(oldCR);

		tableName = oldCR.getTableNameNode();
		tableNumber = oldCR.getTableNumber();
		columnNumber = oldCR.getColumnNumber();
		source = oldCR.getSource();
		nestingLevel = oldCR.getNestingLevel();
		sourceLevel = oldCR.getSourceLevel();
		replacesAggregate = oldCR.getGeneratedToReplaceAggregate();
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * NOTE: We must explicitly check for a null FromList here, column reference
	 * without a FROM list, as the grammar allows the following:
	 *			insert into t1 values(c1)
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
			Vector aggregateVector) 
				throws StandardException
	{

		ResultColumn matchingRC;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromList != null, "fromList is expected to be non-null");
		}

		if (fromList.size() == 0)
		{
			throw StandardException.newException(SQLState.LANG_ILLEGAL_COLUMN_REFERENCE, columnName);
		}

		matchingRC = fromList.bindColumnReference(this);

		/* Error if no match found in fromList */
		if (matchingRC == null)
		{
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND, getFullColumnName());
		}

		/* Set the columnNumber from the base table.
 		 * Useful for optimizer and generation.
		 */
		columnNumber = matchingRC.getColumnPosition();

		return this;
	}

	/**
	 * Get the full column name of this column for purposes of error
	 * messages or debugging.  The full column name includes the table
	 * name and schema name, if any.
	 *
	 * @return	The full column name in the form schema.table.column
	 */

	public String getFullColumnName()
	{
		String	fullColumnName = "";

		if (tableName != null)
			fullColumnName += tableName.getFullTableName() + ".";
		fullColumnName += columnName;

		return fullColumnName;
	}

	/**
	 * Get the name of this column
	 *
	 * @return	The name of this column
	 */

	public String getColumnName()
	{
		return columnName;
	}

	/**
	 * Set the name of this column
	 *
	 * @param columName	The name of this column
	 *
	 * @return None.
	 */

	public void setColumnName(String columnName)
	{
		this.columnName = columnName;
	}

	/**
	 * Get the table number for this ColumnReference.
	 *
	 * @return	int The table number for this ColumnReference
	 */

	public int getTableNumber()
	{
		return tableNumber;
	}

	/**
	 * Set this ColumnReference to refer to the given table number.
	 *
	 * @param table	The table number this ColumnReference will refer to
	 *
	 * @return	Nothing
	 */

	public void setTableNumber(int tableNumber)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(tableNumber != -1,
				"tableNumber not expected to be -1");
		}
		this.tableNumber = tableNumber;
	}

	/**
	 * Get the user-supplied table name of this column.  This will be null
	 * if the user did not supply a name (for example, select a from t).
	 *
	 * @return	The user-supplied name of this column.  Null if no user-
	 * 		supplied name.
	 */

	public String getTableName()
	{
		return ( ( tableName != null) ? tableName.getTableName() : null );
	}

	/**
	 * Get the name of the table this column comes from.
	 *
	 * @return	The name of the table that this column comes from.  
	 *			Null if not a ColumnReference.
	 */

	public String getSourceTableName()
	{
		return ( ( tableName != null) ? tableName.getTableName() : 
					((source != null) ? source.getTableName() : null));
	}

	/**
	  Return the table name as the node it is.
	  @return the column's table name.
	 */
	public TableName getTableNameNode()
	{
		return tableName;
	}

	public void setTableNameNode(TableName tableName)
	{
		this.tableName = tableName;
	}

	/**
	 * Get the column number for this ColumnReference.
	 *
	 * @return	int The column number for this ColumnReference
	 */

	public int getColumnNumber()
	{
		return columnNumber;
	}

	/**
	 * Get the source this columnReference
	 *
	 * @return	The source of this columnReference
	 */

	public ResultColumn getSource()
	{
		return source;
	}

	/**
	 * Set the source this columnReference
	 *
	 * @param source	The source of this columnReference
	 *
	 * @return None.
	 */

	public void setSource(ResultColumn source)
	{
		this.source = source;
	}

	/**
	 * Do the 1st step in putting an expression into conjunctive normal
	 * form.  This step ensures that the top level of the expression is
	 * a chain of AndNodes.
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode putAndsOnTop() 
					throws StandardException
	{
		BinaryComparisonOperatorNode		equalsNode;
		BooleanConstantNode	trueNode;
		NodeFactory		nodeFactory = getNodeFactory();
		ValueNode		andNode;

        trueNode = (BooleanConstantNode) nodeFactory.getNode(
										C_NodeTypes.BOOLEAN_CONSTANT_NODE,
										Boolean.TRUE,
										getContextManager());
		equalsNode = (BinaryComparisonOperatorNode) 
						nodeFactory.getNode(
										C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
										this,
										trueNode,
										getContextManager());
		/* Set type info for the operator node */
		equalsNode.bindComparisonOperator();
		andNode = (ValueNode) nodeFactory.getNode(
									C_NodeTypes.AND_NODE,
									equalsNode,
									trueNode,
									getContextManager());
		((AndNode) andNode).postBindFixup();
		return andNode;
	}

	/**
	 * Categorize this predicate.  Initially, this means
	 * building a bit map of the referenced tables for each predicate.
	 * If the source of this ColumnReference (at the next underlying level) 
	 * is not a ColumnReference or a VirtualColumnNode then this predicate
	 * will not be pushed down.
	 *
	 * For example, in:
	 *		select * from (select 1 from s) a (x) where x = 1
	 * we will not push down x = 1.
	 * NOTE: It would be easy to handle the case of a constant, but if the
	 * inner SELECT returns an arbitrary expression, then we would have to copy
	 * that tree into the pushed predicate, and that tree could contain
	 * subqueries and method calls.
	 *
	 * Also, don't allow a predicate to be pushed down if it contains a
	 * ColumnReference that replaces an aggregate.  This can happen if
	 * the aggregate is in the HAVING clause.  In this case, we would be
	 * pushing the predicate into the SelectNode that evaluates the aggregate,
	 * which doesn't make sense, since the having clause is supposed to be
	 * applied to the result of the SelectNode.
	 *
	 * RESOLVE - revisit this issue once we have views.
	 *
	 * @param referencedTabs	JBitSet with bit map of referenced FromTables
	 * @param simplePredsOnly	Whether or not to consider method
	 *							calls, field references and conditional nodes
	 *							when building bit map
	 *
	 * @return boolean		Whether or not source.expression is a ColumnReference
	 *						or a VirtualColumnNode or a ConstantNode.
	 */
	public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(tableNumber >= 0,
							 "tableNumber is expected to be non-negative");
		referencedTabs.set(tableNumber);

		return ( ! replacesAggregate ) &&
			   ( (source.getExpression() instanceof ColumnReference) ||
			     (source.getExpression() instanceof VirtualColumnNode) ||
				 (source.getExpression() instanceof ConstantNode));
	}

	/**
	 * Remap all of the ColumnReferences in this expression tree
	 * to point to the ResultColumn that is 1 level under their
	 * current source ResultColumn.
	 * This is useful for pushing down single table predicates.
	 *
	 * RESOLVE: Once we start pushing join clauses, we will need to walk the
	 * ResultColumn/VirtualColumnNode chain for them to remap the references.
	 *
	 * @return None.
	 */
	public void remapColumnReferences()
	{
		ValueNode expression = source.getExpression();

		if (SanityManager.DEBUG)
		{
			// SanityManager.ASSERT(origSource == null,
			// 		"Trying to remap ColumnReference twice without unremapping it.");
		}

		if ( ! ( (expression instanceof VirtualColumnNode) ||
				 (expression instanceof ColumnReference) )
			)
		{
			return;
		}

		/* Find the matching ResultColumn */
		origSource = source;
		source = getSourceResultColumn();
		origName = columnName;
		columnName = source.getName();
		origColumnNumber = columnNumber;
		columnNumber = source.getColumnPosition();

		origTableNumber = tableNumber;
		if (source.getExpression() instanceof ColumnReference)
		{
			ColumnReference cr = (ColumnReference) source.getExpression();
			tableNumber = cr.getTableNumber();
			if (SanityManager.DEBUG)
			{
				// if dummy cr generated to replace aggregate, it may not have table number
				// because underneath can be more than 1 table.
				if (tableNumber == -1 && ! cr.getGeneratedToReplaceAggregate())
				{
					SanityManager.THROWASSERT(
						"tableNumber not expected to be -1, origName = " + origName);
				}
			}
		}
	}

	public void unRemapColumnReferences()
	{
		if (origSource == null)
			return;

		if (SanityManager.DEBUG)
		{
			// SanityManager.ASSERT(origSource != null,
			// 	"Trying to unremap a ColumnReference that was not remapped.");
		}

		source = origSource;
		origSource = null;
		columnName = origName;
		origName = null;
		tableNumber = origTableNumber;
		columnNumber = origColumnNumber;
	}

	/*
	 * Get the ResultColumn that the source points to.  This is useful for
	 * getting what the source will be after this ColumnReference is remapped.
	 */
	public ResultColumn getSourceResultColumn()
	{
		ValueNode expression = source.getExpression();

		/* Find the matching ResultColumn */
		if (expression instanceof VirtualColumnNode) 
		{
			return ((VirtualColumnNode) expression).getSourceResultColumn();
		}
		else
		{
			/* RESOLVE - If expression is a ColumnReference, then we are hitting
			 * the top of a query block (derived table or view.)
			 * In order to be able to push the expression down into the next
			 * query block, it looks like we should reset the contents of the
			 * current ColumnReference to be the same as expression.  (This probably
			 * only means names and tableNumber.)  We would then "rebind" the top
			 * level predicate somewhere up the call stack and see if we could push
			 * the predicate through.
			 */
			return ((ColumnReference) expression).getSourceResultColumn();
		}
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return ValueNode			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public ValueNode remapColumnReferencesToExpressions()
		throws StandardException
	{
		ResultColumn	rc;
		ResultColumn	sourceRC = source;

		/* Nothing to do if we are not pointing to a redundant RC */
		if (! source.isRedundant())
		{
			return this;
		}

		/* Find the last redundant RC in the chain.  We
		 * want to clone its expression.
		 */
		for (rc = source; rc != null && rc.isRedundant(); )
		{
			ResultColumn	nextRC = null;
			ValueNode		expression = rc.getExpression();

			/* Find the matching ResultColumn */
			if (expression instanceof VirtualColumnNode) 
			{
				nextRC = ((VirtualColumnNode) expression).getSourceResultColumn();
			}
			else if (expression instanceof ColumnReference)
			{
				nextRC = ((ColumnReference) expression).getSourceResultColumn();
			}
			else
			{
				nextRC = null;
			}

			if (nextRC != null && nextRC.isRedundant())
			{
				sourceRC = nextRC;
			}
			rc = nextRC;
		}

		if (SanityManager.DEBUG)
		{
			if (sourceRC == null)
			{
				SanityManager.THROWASSERT(
					"sourceRC is expected to be non-null for " +
					columnName);
			}

			if ( ! sourceRC.isRedundant())
			{
				SanityManager.THROWASSERT(
					"sourceRC is expected to be redundant for " +
					columnName);
			}
		}

		/* If last expression is a VCN, then we can't clone it.
		 * Instead, we just reset our source to point to the
		 * source of the VCN, those chopping out the layers.
		 * Otherwise, we return a clone of the underlying expression.
		 */
		if (sourceRC.getExpression() instanceof VirtualColumnNode)
		{
			VirtualColumnNode vcn =
				(VirtualColumnNode) (sourceRC.getExpression());
			ResultSetNode rsn = vcn.getSourceResultSet();
			if (rsn instanceof FromTable)
			{
				tableNumber = ((FromTable) rsn).getTableNumber();
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(tableNumber != -1,
						"tableNumber not expected to be -1");
				}
			}
			else
			{
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("rsn expected to be a FromTable, but is a " + rsn.getClass().getName());
				}
			}
			source = ((VirtualColumnNode) sourceRC.getExpression()).
										getSourceResultColumn();
			return this;
		}
		else
		{
			return sourceRC.getExpression().getClone();
		}
	}

	/** 
	 * Update the table map to reflect the source
	 * of this CR.
	 *
	 * @param refs	The table map.
	 *
	 * @return Nothing.
	 */
	void getTablesReferenced(JBitSet refs)
	{
		if (refs.size() < tableNumber)
			refs.grow(tableNumber);

		if (tableNumber != -1)	// it may not be set if replacesAggregate is true
			refs.set(tableNumber);
	}

	/**
	 * Return whether or not this expression tree is cloneable.
	 *
	 * @return boolean	Whether or not this expression tree is cloneable.
	 */
	public boolean isCloneable()
	{
		return true;
	}

	/** @see ValueNode#constantExpression */
	public boolean constantExpression(PredicateList whereClause)
	{
		return whereClause.constantColumn(this);
	}

	/**
	 * ColumnReference's are to the current row in the system.
	 * This lets us generate
	 * a faster get that simply returns the column from the
	 * current row, rather than getting the value out and
	 * returning that, only to have the caller (in the situations
	 * needed) stuffing it back into a new column holder object.
	 * We will assume the general generate() path is for getting
	 * the value out, and use generateColumn() when we want to
	 * keep the column wrapped.
	 *
	 * @exception StandardException		Thrown on error
	 */
	 public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	 {
		int sourceResultSetNumber = source.getResultSetNumber();

		//PUSHCOMPILE
		/* Reuse generated code, where possible */

		/*
		** If the source is redundant, return the generation of its source.
		** Most redundant nodes will be flattened out by this point, but
		** in at least one case (elimination of redundant ProjectRestricts
		** during generation) we don't do this.
		*/
		if (source.isRedundant())
		{
			source.generateExpression(acb, mb);
			return;
		}

		if (SanityManager.DEBUG)
		{
			if (sourceResultSetNumber < 0)
			{
				SanityManager.THROWASSERT("sourceResultSetNumber expected to be >= 0");
			}
		}

		/* The ColumnReference is from an immediately underlying ResultSet.
		 * The Row for that ResultSet is Activation.row[sourceResultSetNumber], 
		 * where sourceResultSetNumber is the resultSetNumber for that ResultSet.
		 *
		 * The generated java is the expression:
		 *	(<interface>) this.row[sourceResultSetNumber].getColumn(#columnId);
		 *
		 * where <interface> is the appropriate Datatype protocol interface
		 * for the type of the column.
		 */
	    acb.pushColumnReference(mb, sourceResultSetNumber, 
	    									source.getVirtualColumnId());

		mb.cast(getTypeCompiler().interfaceName());

		/* Remember generated code for possible resuse */
	 }

	/**
	 * Get the user-supplied schema name of this column.  This will be null
	 * if the user did not supply a name (for example, select t.a from t).
	 *
	 * @return	The user-supplied schema name of this column.  Null if no user-
	 * 		supplied name.
	 */

	public String getSchemaName()
	{
		return ( ( tableName != null) ? tableName.getSchemaName() : null );
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *							  (constant expressions)
	 *
	 * @return	The variant type for the underlying expression.
	 */
	protected int getOrderableVariantType()
	{
		// ColumnReferences are invariant for the life of the scan
		return Qualifier.SCAN_INVARIANT;
	}

	/**
	 * Return whether or not the source of this ColumnReference is itself a ColumnReference.
	 *
	 * @return Whether or not the source of this ColumnReference is itself a ColumnReference.
	 */
	boolean pointsToColumnReference()
	{ 
		return (source.getExpression() instanceof ColumnReference);
	}

	/**
	 * Get the DataTypeServices from this Node.
	 *
	 * @return	The DataTypeServices from this Node.  This
	 *		may be null if the node isn't bound yet.
	 */
	public DataTypeDescriptor getTypeServices()
	{
        DataTypeDescriptor dtd = super.getTypeServices();
        if( dtd == null && source != null)
        {
            dtd = source.getTypeServices();
            if( dtd != null)
                setType( dtd);
        }
        return dtd;
    } // end of getTypeServices
}
