/*

   Derby - Class org.apache.derby.impl.sql.compile.ColumnReference

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

import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.util.JBitSet;

/**
 * A ColumnReference represents a column in the query tree.  The parser
 * generates a ColumnReference for each column reference.  A column reference
 * could be a column in a base table, a column in a view (which could expand
 * into a complex expression), or a column in a subquery in the FROM clause.
 *
 */

public class ColumnReference extends ValueNode
{
    // For associating columns with the SOURCE and TARGET tables of MERGE statements.
    public  static  final   int MERGE_UNKNOWN = 0;
    public  static  final   int MERGE_SOURCE = MERGE_UNKNOWN + 1;
    public  static  final   int MERGE_TARGET = MERGE_SOURCE + 1;
    
	private String	_columnName;

	/*
	** This is the user-specified table name.  It will be null if the
	** user specifies a column without a table name.  Leave it null even
	** when the column is bound as it is only used in binding.
	*/
	private TableName	_qualifiedTableName;

    /**
     * The FromTable this column reference is bound to.
     */
    private int tableNumber;

    /**
     * The column number in the underlying FromTable. But note {@code source}.
     * @see #source
     */
    private int columnNumber;

    /**
     * This is where the value for this column reference will be coming from.
     * Note that for join nodes, {@code tableNumber}/{@code columnNumber} will
     * point to the column in the left or right join participant {@code
     * FromTable}, whereas {@code source} will be bound to the RC in the result
     * column list of the join node. See also the comment at the end of
     * JoinNode#getMatchingColumn.
     * @see JoinNode#getMatchingColumn
     * @see #columnNumber
     * @see #tableNumber
     */
    private ResultColumn source;

	/* For unRemapping */
	private ResultColumn	_origSource;
	private String	origName;
	private int				_origTableNumber = -1;
	private int				_origColumnNumber = -1;

    /* For remembering original (tn,cn) of this CR during join flattening. */
    private int tableNumberBeforeFlattening = -1;
    private int columnNumberBeforeFlattening = -1;

	/* Reuse generated code where possible */
	//Expression genResult;

	private boolean		replacesAggregate;
	private boolean		replacesWindowFunctionCall;

	private int			nestingLevel = -1;
	private int			sourceLevel = -1;

	/* Whether or not this column reference been scoped for the
	   sake of predicate pushdown.
	 */
	private boolean		scoped;

	/* List of saved remap data if this ColumnReference is scoped
	   and has been remapped multiple times.
	 */
	private java.util.ArrayList<RemapInfo> remaps;

    /** Columns mentioned by MERGE statements need to be associated
     * the SOURCE or TARGET table */
    private int _mergeTableID = MERGE_UNKNOWN;

    /**
     * Constructor.
	 * This one is called by the parser where we could
	 * be dealing with delimited identifiers.
	 *
	 * @param columnName	The name of the column being referenced
	 * @param tableName		The qualification for the column
	 * @param tokBeginOffset begin position of token for the column name 
	 *					identifier from parser.
	 * @param tokEndOffset	end position of token for the column name 
	 *					identifier from parser.
     * @param cm           The context manager
	 */
    ColumnReference(String         columnName,
                    TableName      tableName,
                    int            tokBeginOffset,
                    int            tokEndOffset,
                    ContextManager cm)  {
        super(cm);
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
        _columnName = columnName;
        _qualifiedTableName = tableName;
        this.setBeginOffset(tokBeginOffset);
        this.setEndOffset(tokEndOffset);
		tableNumber = -1;
		remaps = null;
	}

	/**
     * Constructor.
	 *
	 * @param columnName	The name of the column being referenced
	 * @param tableName		The qualification for the column
     * @param cm           The context manager
	 */
    ColumnReference(String         columnName,
                    TableName      tableName,
                    ContextManager cm) {
        super(cm);
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
        _columnName = columnName;
        _qualifiedTableName = tableName;
		tableNumber = -1;
		remaps = null;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
			return "columnName: " + _columnName + "\n" +
				"tableNumber: " + tableNumber + "\n" +
				"columnNumber: " + columnNumber + "\n" +
				"replacesAggregate: " + replacesAggregate + "\n" +
				"replacesWindowFunctionCall: " +
				    replacesWindowFunctionCall + "\n" +
				"tableName: " + ( ( _qualifiedTableName != null) ?
								  _qualifiedTableName.toString() :
								  "null") + "\n" +
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
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void printSubNodes(int depth)
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
	 * Return whether or not this CR is correlated.
	 *
	 * @return Whether or not this CR is correlated.
	 */
	boolean getCorrelated()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(nestingLevel != -1,
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
				"nestingLevel on "+_columnName+" is not expected to be -1");
			SanityManager.ASSERT(sourceLevel != -1,
				"sourceLevel on "+_columnName+" is not expected to be -1");
		}
		return sourceLevel != nestingLevel;
	}

	/**
	 * Set the nesting level for this CR.  (The nesting level
	 * at which the CR appears.)
	 *
	 * @param nestingLevel	The Nesting level at which the CR appears.
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
	private int getNestingLevel()
	{
		return nestingLevel;
	}

	/**
	 * Set the source level for this CR.  (The nesting level
	 * of the source of the CR.)
	 *
	 * @param sourceLevel	The Nesting level of the source of the CR.
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
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void markGeneratedToReplaceAggregate()
	{
		replacesAggregate = true;
	}


	/**
	 * Mark this node as being generated to replace a window function call.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void markGeneratedToReplaceWindowFunctionCall()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-3634
//IC see: https://issues.apache.org/jira/browse/DERBY-4069
		replacesWindowFunctionCall = true;
	}

	/**
	 * Determine whether or not this node was generated to
	 * replace an aggregate in the user's SELECT.
	 *
	 * @return boolean	Whether or not this node was generated to replace
	 *					an aggregate in the user's SELECT.
	 */
    boolean getGeneratedToReplaceAggregate()
	{
		return replacesAggregate;
	}


	/**
	 * Determine whether or not this node was generated to
	 * replace a window function call in the user's SELECT.
	 *
	 * @return boolean	Whether or not this node was generated to replace
	 *					a window function call in the user's SELECT.
	 */
    boolean getGeneratedToReplaceWindowFunctionCall()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-3634
//IC see: https://issues.apache.org/jira/browse/DERBY-4069
		return replacesWindowFunctionCall;
	}

	/**
	 * Return a clone of this node.
	 *
	 * @return ValueNode	A clone of this node.
	 *
	 * @exception StandardException			Thrown on error
	 */
    @Override
    ValueNode getClone()
		throws StandardException
	{
        ColumnReference newCR =
                new ColumnReference(_columnName, _qualifiedTableName, getContextManager());
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

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
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void copyFields(ColumnReference oldCR)
		throws StandardException
	{
		super.copyFields(oldCR);

		setQualifiedTableName( oldCR.getQualifiedTableName() );
		tableNumber = oldCR.getTableNumber();
		columnNumber = oldCR.getColumnNumber();
		source = oldCR.getSource();
		nestingLevel = oldCR.getNestingLevel();
		sourceLevel = oldCR.getSourceLevel();
		replacesAggregate = oldCR.getGeneratedToReplaceAggregate();
//IC see: https://issues.apache.org/jira/browse/DERBY-3634
//IC see: https://issues.apache.org/jira/browse/DERBY-4069
		replacesWindowFunctionCall =
			oldCR.getGeneratedToReplaceWindowFunctionCall();
		scoped = oldCR.isScoped();
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
        copyTagsFrom( oldCR );
        if ( oldCR._mergeTableID != MERGE_UNKNOWN )
        {
            setMergeTableID( oldCR.getMergeTableID() );
        }
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
     * @param aggregates        The aggregate list being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ColumnReference bindExpression(FromList fromList,
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                             SubqueryList subqueryList,
                             List<AggregateNode> aggregates)
            throws StandardException
	{
		ResultColumn matchingRC;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromList != null, "fromList is expected to be non-null");
		}

		if (fromList.size() == 0)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
			throw StandardException.newException(SQLState.LANG_ILLEGAL_COLUMN_REFERENCE, _columnName);
		}

        matchingRC = fromList.bindColumnReference(this);

		/* Error if no match found in fromList */
		if (matchingRC == null)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-18
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND, getSQLColumnName());
		}

		return this;
	}

	/**
	 * Get the column name for purposes of error
	 * messages or debugging. This returns the column
	 * name as used in the SQL statement. Thus if it was qualified
	 * with a table, alias name that will be included.
	 *
	 * @return	The  column name in the form [[schema.]table.]column
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getSQLColumnName()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		if (_qualifiedTableName == null)
        {
			return _columnName;
        }
		
		return _qualifiedTableName.toString() + "." + _columnName;
	}

	/**
	 * Get the name of this column
	 *
	 * @return	The name of this column
	 */
    @Override
	public String getColumnName()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		return _columnName;
	}

	/**
	 * Get the table number for this ColumnReference.
	 *
	 * @return	int The table number for this ColumnReference
	 */

    int getTableNumber()
	{
		return tableNumber;
	}

	/**
	 * Set this ColumnReference to refer to the given table number.
	 *
	 * @param tableNumber	The table number this ColumnReference will refer to
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void setTableNumber(int tableNumber)
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
	 * The method will return B for this example, select b.a from t as b
	 * The method will return T for this example, select t.a from t
	 *
	 * @return	The user-supplied name of this column.  Null if no user-
	 * 		supplied name.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getTableName()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		return ( ( _qualifiedTableName != null) ? _qualifiedTableName.getTableName() : null );
	}

	/**
	 * Get the name of the underlying(base) table this column comes from, if any.
	 * Following example queries will all return T
	 * select a from t
	 * select b.a from t as b
	 * select t.a from t
	 *
	 * @return	The name of the base table that this column comes from.
	 *			Null if not a ColumnReference.
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getSourceTableName()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-189
		return ((source != null) ? source.getTableName() : null);
	}

	/**
	 * Get the name of the schema for the Column's base table, if any.
	 * Following example queries will all return APP (assuming user is in schema APP)
	 * select t.a from t
	 * select b.a from t as b
	 * select app.t.a from t
	 *
	 * @return	The name of the schema for Column's base table. If the column
	 *		is not in a schema (i.e. is a derived column), it returns NULL.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getSourceSchemaName() throws StandardException
	{
		return ((source != null) ? source.getSchemaName() : null);
	}

	/**
	 * Is the column wirtable by the cursor or not. (ie, is it in the list of FOR UPDATE columns list)
	 *
	 * @return TRUE, if the column is a base column of a table and is 
	 * writable by cursor.
	 */
    @Override
	public boolean updatableByCursor()
	{
		return ((source != null) ? source.updatableByCursor() : false);
	}

	/**
	  Return the table name as the node it is.
	  @return the column's table name.
	 */
	public TableName getQualifiedTableName()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		return _qualifiedTableName;
	}

    void setQualifiedTableName(TableName tableName)
	{
		_qualifiedTableName = tableName;
	}

	/**
	 * Get the column number for this ColumnReference.
	 *
	 * @return	int The column number for this ColumnReference
	 */

    int getColumnNumber()
	{
		return columnNumber;
	}

	/**
	 * Set the column number for this ColumnReference.  This is
	 * used when scoping predicates for pushdown.
	 *
	 * @param colNum The new column number.
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void setColumnNumber(int colNum)
	{
		this.columnNumber = colNum;
	}

	/**
	 * Get the source this columnReference
	 *
	 * @return	The source of this columnReference
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultColumn getSource()
	{
		return source;
	}

	/**
	 * Set the source this columnReference
	 *
	 * @param source	The source of this columnReference
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void setSource(ResultColumn source)
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
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode putAndsOnTop()
					throws StandardException
	{
		BinaryComparisonOperatorNode		equalsNode;
		BooleanConstantNode	trueNode;

        trueNode = new BooleanConstantNode(true, getContextManager());
        equalsNode = new BinaryRelationalOperatorNode(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                BinaryRelationalOperatorNode.K_EQUALS,
                this,
                trueNode,
                false,
                getContextManager());
		/* Set type info for the operator node */
		equalsNode.bindComparisonOperator();

        AndNode andNode =
                new AndNode(equalsNode, trueNode, getContextManager());
       andNode.postBindFixup();
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
	 * This also goes for column references that replaces a window function.
	 *
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
    @Override
    boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(tableNumber >= 0,
							 "tableNumber is expected to be non-negative");
		referencedTabs.set(tableNumber);

		return ( ! replacesAggregate ) &&
//IC see: https://issues.apache.org/jira/browse/DERBY-3634
//IC see: https://issues.apache.org/jira/browse/DERBY-4069
			   ( ! replacesWindowFunctionCall ) &&
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
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void remapColumnReferences()
	{
		ValueNode expression = source.getExpression();

		if (SanityManager.DEBUG)
		{
			// SanityManager.ASSERT(_origSource == null,
			// 		"Trying to remap ColumnReference twice without unremapping it.");
		}

		if ( ! ( (expression instanceof VirtualColumnNode) ||
				 (expression instanceof ColumnReference) )
			)
		{
			return;
		}

		/* Scoped column references are a special case: they can be
		 * remapped several times (once for every ProjectRestrictNode
		 * through which the scoped ColumnReference is pushed before
		 * reaching its target result set) and will be un-remapped
		 * several times, as well (as the scoped predicate is "pulled"
		 * back up the query tree to it's original location).  So we
		 * have to keep track of the "orig" info for every remap
		 * operation, not just for the most recent one.
		 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		if (scoped && (_origSource != null))
		{
			if (remaps == null)
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
				remaps = new java.util.ArrayList<RemapInfo>();
			remaps.add(new RemapInfo(
				columnNumber, tableNumber, _columnName, source));
		}
		else
		{
			_origSource = source;
			origName = _columnName;
			_origColumnNumber = columnNumber;
			_origTableNumber = tableNumber;
		}

		/* Find the matching ResultColumn */
		source = getSourceResultColumn();
		_columnName = source.getName();
        // Use the virtual column id if the ResultColumn's expression
        // is a virtual column (DERBY-5933).
        columnNumber = source.getExpression() instanceof VirtualColumnNode ?
                source.getVirtualColumnId() : source.getColumnPosition();

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

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void unRemapColumnReferences()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		if (_origSource == null)
        {
			return;
        }

		if (SanityManager.DEBUG)
		{
			// SanityManager.ASSERT(_origSource != null,
			// 	"Trying to unremap a ColumnReference that was not remapped.");
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        if ((remaps == null) || (remaps.isEmpty()))
		{
			source = _origSource;
			_origSource = null;
			_columnName = origName;
			origName = null;
			tableNumber = _origTableNumber;
			columnNumber = _origColumnNumber;
		}
		else
		{
			// This CR is multiply-remapped, so undo the most
			// recent (and only the most recent) remap operation.
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			RemapInfo rI = remaps.remove(remaps.size() - 1);
			source = rI.getSource();
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
			_columnName = rI.getColumnName();
			tableNumber = rI.getTableNumber();
			columnNumber = rI.getColumnNumber();
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
            if (remaps.isEmpty())
				remaps = null;
		}
	}

	/**
	 * Returns true if this ColumnReference has been remapped; false
	 * otherwise.
	 *
	 * @return Whether or not this ColumnReference has been remapped.
	 */
	protected boolean hasBeenRemapped()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		return (_origSource != null);
	}

	/*
	 * Get the ResultColumn that the source points to.  This is useful for
	 * getting what the source will be after this ColumnReference is remapped.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultColumn getSourceResultColumn()
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
        
        return source.getExpression().getSourceResultColumn();
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return ValueNode			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode remapColumnReferencesToExpressions()
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
			/* Find the matching ResultColumn */
            ResultColumn nextRC = rc.getExpression().getSourceResultColumn();

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
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
					_columnName);
			}

			if ( ! sourceRC.isRedundant())
			{
				SanityManager.THROWASSERT(
					"sourceRC is expected to be redundant for " +
					_columnName);
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
				FromTable ft = (FromTable)rsn;

				/* It's not enough to just set the table number.  Depending
				 * on the original query specified and on whether or not
				 * subquery flattening has occurred, it's possible that
				 * the expression to which we're remapping has a different
				 * RCL ordering than the one to which we were mapped before
				 * we got here.  In that case we also need to update the
				 * columnNumber to point to the correct column in "ft".
				 * See DERBY-2526 for details.
                 * See DERBY-3023 and DERBY-4679 for further improvement
                 * details.
				 */

                ResultColumnList rcl = ft.getResultColumns();
//IC see: https://issues.apache.org/jira/browse/DERBY-4679
//IC see: https://issues.apache.org/jira/browse/DERBY-2526
//IC see: https://issues.apache.org/jira/browse/DERBY-3023

                // Need to save original (tn,cn) in case we have several
                // flattenings so we can relocate the correct column many
                // times. After the first flattening, the (tn,cn) pair points
                // to the top RCL which is going away..
                if (tableNumberBeforeFlattening == -1) {
                    tableNumberBeforeFlattening = tableNumber;
                    columnNumberBeforeFlattening = columnNumber;
                }

                // Covers references to a table not being flattened out, e.g.
                // inside a join tree, which can have many columns in the rcl
                // with the same name, so looking up via column name can give
                // the wrong column. DERBY-4679.
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                ResultColumn ftRC = rcl.getResultColumn(
                    tableNumberBeforeFlattening,
                    columnNumberBeforeFlattening,
                    _columnName);
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

                if (ftRC == null) {
                    // The above lookup won't work for references to a base
                    // column, so fall back on column name, which is unique
                    // then.
                    ftRC = rcl.getResultColumn(_columnName);
                }

                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(
                        ftRC != null,
                        "Failed to find column '" + _columnName +
                        "' in the " + "RCL for '" + ft.getTableName() +
                        "'.");
                }

                tableNumber = ft.getTableNumber();

				if (SanityManager.DEBUG) {
					SanityManager.ASSERT(tableNumber != -1,
						"tableNumber not expected to be -1");
				}

				/* Use the virtual column id if the ResultColumn's expression
				 * is a virtual column (DERBY-3023).
				 */
				columnNumber =
					(ftRC.getExpression() instanceof VirtualColumnNode)
						? ftRC.getVirtualColumnId()
						: ftRC.getColumnPosition();
			}
			else
			{
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("rsn expected to be a FromTable, but is a " + rsn.getClass().getName());
				}
			}
			source = sourceRC.getExpression().getSourceResultColumn();
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
    @Override
    boolean isCloneable()
	{
		return true;
	}

	/** @see ValueNode#constantExpression */
    @Override
    boolean constantExpression(PredicateList whereClause)
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
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
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
				SanityManager.THROWASSERT("sourceResultSetNumber expected to be >= 0 for " + getTableName() + "." + getColumnName());
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
	 * Another example for null return value (for example, select b.a from t as b).
	 * But for following query select app.t.a from t, this will return APP
	 * Code generation of aggregate functions relies on this method
	 *
	 * @return	The user-supplied schema name of this column.  Null if no user-
	 * 		supplied name.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getSchemaName()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		return ( ( _qualifiedTableName != null) ? _qualifiedTableName.getSchemaName() : null );
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
    @Override
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
	 * The type of a ColumnReference is the type of its
     * source unless the source is null then it is
     * the type that has been set on this node.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    DataTypeDescriptor getTypeServices()
	{        
        if (source == null)
            return super.getTypeServices();
       
        return source.getTypeServices();
    }

	/**
	 * Find the source result set for this ColumnReference and
	 * return it.  Also, when the source result set is found,
	 * return the position (within the source result set's RCL)
	 * of the column referenced by this ColumnReference.  The
	 * position is returned vai the colNum parameter.
	 *
	 * @param colNum Place to store the position of the column
	 *  to which this ColumnReference points (position is w.r.t
	 *  the source result set).
	 * @return The source result set for this ColumnReference;
	 *  null if there is no source result set.
	 */
	protected ResultSetNode getSourceResultSet(int [] colNum)
		throws StandardException
	{
		if (source == null)
		{
			/* this can happen if column reference is pointing to a column
			 * that is not from a base table.  For example, if we have a
			 * VALUES clause like
			 *
			 *    (values (1, 2), (3, 4)) V1 (i, j)
			 *
			 * and then a column reference to VI.i, the column reference
			 * won't have a source.
			 */
			return null;
		}

		ResultColumn rc = getSource();

		// Walk the ResultColumn->ColumnReference chain until we
		// find a ResultColumn whose expression is a VirtualColumnNode.

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        ValueNode rcExpr = rc.getExpression();
		colNum[0] = getColumnNumber();

		/* We have to make sure we enter this loop if rc is redundant,
		 * so that we can navigate down to the actual source result
		 * set (DERBY-1777). If rc *is* redundant, then rcExpr is not
		 * guaranteed to be a ColumnReference, so we have to check
		 * for that case inside the loop.
		 */
		while ((rcExpr != null) &&
			(rc.isRedundant() || (rcExpr instanceof ColumnReference)))
		{
			if (rcExpr instanceof ColumnReference)
			{
				colNum[0] = ((ColumnReference)rcExpr).getColumnNumber();
				rc = ((ColumnReference)rcExpr).getSource();
			}

			/* If "rc" is redundant then that means it points to another
			 * ResultColumn that in turn points to the source expression.
			 * This can happen in cases where "rc" points to a subquery
			 * that has been flattened into the query above it (flattening
			 * of subqueries occurs during preprocessing).  In that case
			 * we want to skip over the redundant rc and find the
			 * ResultColumn that actually holds the source expression.
			 */
			while (rc.isRedundant())
			{
				rcExpr = rc.getExpression();
				if (rcExpr instanceof VirtualColumnNode)
					rc = rcExpr.getSourceResultColumn();
				else if (rcExpr instanceof ColumnReference)
				{
					colNum[0] = ((ColumnReference)rcExpr).getColumnNumber();
					rc = ((ColumnReference)rcExpr).getSource();
				}
				else
				{
					/* If rc isn't pointing to a VirtualColumnNode nor
					 * to a ColumnReference, then it's not pointing to
					 * a result set.  It could, for example, be pointing
					 * to a constant node or to the result of an aggregate
					 * or function.  Break out of both loops and return
					 * null since there is no source result set.
					 */
					break;
				}
			}
			rcExpr = rc.getExpression();
		}

		// If we found a VirtualColumnNode, return the VirtualColumnNode's
		// sourceResultSet.  The column within that sourceResultSet that
		// is referenced by this ColumnReference is also returned, via
		// the colNum parameter, and was set above.
		if ((rcExpr != null) && (rcExpr instanceof VirtualColumnNode))
			return ((VirtualColumnNode)rcExpr).getSourceResultSet();

		// If we get here then the ColumnReference doesn't reference
		// a result set, so return null.
		colNum[0] = -1;
		return null;
	}
	
    boolean isEquivalent(ValueNode o) throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        if (! isSameNodeKind(o)) {
			return false;
		}

		ColumnReference other = (ColumnReference)o;
		return (tableNumber == other.tableNumber 
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
				&& _columnName.equals(other.getColumnName()));
	}

	/**
	 * Mark this column reference as "scoped", which means that it
	 * was created (as a clone of another ColumnReference) to serve
	 * as the left or right operand of a scoped predicate.
	 */
	protected void markAsScoped()
	{
		scoped = true;
	}

	/**
	 * Return whether or not this ColumnReference is scoped.
	 */
	protected boolean isScoped()
	{
		return scoped;
	}

    /** Associate this column with a SOURCE or TARGET table of a MERGE statement */
    void    setMergeTableID( int mergeTableID )
    {
        // Changing the association of a column means we are confused. Shouldn't happen.
        if ( (_mergeTableID != MERGE_UNKNOWN) && (_mergeTableID != mergeTableID)  )
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT
                    (
                     (_mergeTableID == mergeTableID),
                     "MERGE statement can't re-associate column " + getSQLColumnName() +
                     " from " + prettyPrintMergeTableID( _mergeTableID ) +
                     " to " + prettyPrintMergeTableID( mergeTableID )
                     );
            }
        }

        _mergeTableID = mergeTableID;
    }
    private String  prettyPrintMergeTableID( int mergeTableID )
    {
        switch ( mergeTableID )
        {
        case MERGE_SOURCE: return "SOURCE";
        case MERGE_TARGET: return "TARGET";
        default: return "UNKNOWN";
        }
    }

    /** Get the MERGE table (SOURCE or TARGET) associated with this column */
    int getMergeTableID()
    {
        return _mergeTableID;
    }

	/**
	 * Helper class to keep track of remap data when a ColumnReference
	 * is remapped multiple times.  This allows the CR to be UN-
	 * remapped multiple times, as well.
	 */
    private static class RemapInfo
	{
		int colNum;
		int tableNum;
		String colName;
		ResultColumn source;

		RemapInfo(int cNum, int tNum, String cName, ResultColumn rc)
		{
			colNum = cNum;
			tableNum = tNum;
			colName = cName;
			source = rc;
		}

		int getColumnNumber() { return colNum; }
		int getTableNumber() { return tableNum; }
		String getColumnName() { return colName; }
		ResultColumn getSource() { return source; }

		void setColNumber(int cNum) { colNum = cNum; }
		void setTableNumber(int tNum) { tableNum = tNum; }
		void setColName(String cName) { colName = cName; }
		void setSource(ResultColumn rc) { source = rc; }
	}

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (_qualifiedTableName != null) {
            setQualifiedTableName( (TableName) _qualifiedTableName.accept(v) );
        }
    }
}
