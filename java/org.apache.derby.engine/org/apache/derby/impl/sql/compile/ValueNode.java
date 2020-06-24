/*

   Derby - Class org.apache.derby.impl.sql.compile.ValueNode

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
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.JBitSet;

/**
 * A ValueNode is an abstract class for all nodes that can represent data
 * values, that is, constants, columns, and expressions.
 *
 */

public abstract class ValueNode extends QueryTreeNode
{
    /**
     * The data type for this node.
     */
	private DataTypeDescriptor	dataTypeServices;

	// Whether or not additional predicates have been created from this one.
	boolean	transformed;

	/*
	** Constructor for untyped ValueNodes, for example, untyped NULLs
	** and parameter nodes.
	**
	** Binding will replace all untyped ValueNodes with typed ValueNodes
	** when it figures out what their types should be.
	*/
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode(ContextManager cm) {
        super(cm);
	}
    
    /**
     * Set this node's type from type components.
     */
    final void setType(TypeId typeId,
            boolean isNullable,
            int maximumWidth)
       throws StandardException
       
       {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        setType(new DataTypeDescriptor(typeId, isNullable, maximumWidth));
       }

    /**
     * Set this node's type from type components.
     */
    final void setType(TypeId typeId,
            int precision, int scale,
            boolean isNullable,
            int maximumWidth)
       throws StandardException
    {
        setType(
                new DataTypeDescriptor(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                            typeId,
                            precision,
                            scale,
                            isNullable,
                            maximumWidth
                        )
                    );   
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
			return "dataTypeServices: " +
				( ( dataTypeServices != null) ?
						dataTypeServices.toString() : "null" ) + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Get the DataTypeServices from this ValueNode.
	 *
	 * @return	The DataTypeServices from this ValueNode.  This
	 *		may be null if the node isn't bound yet.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    DataTypeDescriptor getTypeServices()
	{
		return dataTypeServices;
	}
    
    /**
     * Set the nullability of this value.
     * @throws StandardException 
     */
    public void setNullability(boolean nullability) throws StandardException
    {
        setType(getTypeServices().getNullabilityType(nullability));
    }
    
    /**
     * Set the collation type and derivation of this node based upon
     * the collation information in the passed in type. Note that the
     * base type of this node is not changed (e.g. INTEGER), only its
     * collation settings. This may result in a different object being
     * returned from getTypeServices().
     * 
     * @param collationInfoType Type to take collation type and derivation from.
     * @throws StandardException Error setting type.
     */
    public void setCollationInfo(DataTypeDescriptor collationInfoType)
    throws StandardException
    {
        setCollationInfo(collationInfoType.getCollationType(),
                collationInfoType.getCollationDerivation());
    }

    /**
     * Set the collation type and derivation of this node based upon
     * the collation information passed in.
     * This may result in a different object being
     * returned from getTypeServices().
     * 
     * @param collationType Collation type
     * @param collationDerivation Collation derivation
     * @throws StandardException Error setting type
     */
    public void setCollationInfo(int collationType, int collationDerivation)
        throws StandardException
    {
        setType(getTypeServices().getCollatedType(
                collationType, collationDerivation));
    }

	/**
	 * Get the TypeId from this ValueNode.
	 *
	 * @return	The TypeId from this ValueNode.  This
	 *		may be null if the node isn't bound yet.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    TypeId getTypeId() throws StandardException
	{
        DataTypeDescriptor dtd = getTypeServices();
        if (dtd != null)
            return dtd.getTypeId();
		return null;
	}


	/**
		Return the DataValueFactory
	*/
	protected final DataValueFactory getDataValueFactory() {
		return getLanguageConnectionContext().getDataValueFactory();
	}

	/**
	 * Get the TypeCompiler from this ValueNode, based on its TypeId
     * using getTypeId().
	 *
	 * @return	This ValueNode's TypeCompiler
	 *
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    final TypeCompiler getTypeCompiler() throws StandardException
	{
		return getTypeCompiler(getTypeId());
	}

	/**
	 * Set the DataTypeServices for this ValueNode.  This method is
	 * overridden in ParameterNode.
	 *
	 * @param dataTypeServices	The DataTypeServices to set in this
	 *				ValueNode
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void setType(DataTypeDescriptor dataTypeServices) throws StandardException
	{
        // bind the type in case it is a user defined type. this will create a dependency on the udt.
//IC see: https://issues.apache.org/jira/browse/DERBY-4484
        if ( dataTypeServices != null )
        {
            dataTypeServices = bindUserType( dataTypeServices );
        }

		this.dataTypeServices = dataTypeServices;
        
        // create a dependency on this type if it is an ANSI UDT
        if ( dataTypeServices != null ) { createTypeDependency( dataTypeServices ); }
	}
	
	/**
	 * Set the collation based upon the current schema with derivation
	 * type implicit.
	 * 
	 * @throws StandardException
	 */
	protected final void setCollationUsingCompilationSchema()
	throws StandardException {
		setCollationUsingCompilationSchema(
				StringDataValue.COLLATION_DERIVATION_IMPLICIT);
	}	
	
	/**
	 * There are many subclasses of ValueNode where we want the 
	 * DataTypeDescriptor of the node to have the same collation type as the 
	 * compilation schema's collation type. For that purpose, this method in 
	 * the baseclass here can be utilized by the subclasses. In addition, the
	 * subclasses can pass the collationDerivation that they expect the
	 * DataTypeDescriptor to have.
	 * 
	 * @param collationDerivation This can be 
	 * StringDataValue#COLLATION_DERIVATION_IMPLICIT
	 * StringDataValue#COLLATION_DERIVATION_NONE
	 * StringDataValue#COLLATION_DERIVATION_EXPLICIT
	 * 
	 * @throws StandardException
	 */
	protected final void setCollationUsingCompilationSchema(int collationDerivation)
//IC see: https://issues.apache.org/jira/browse/DERBY-2599
	throws StandardException {
        setCollationInfo(
                getSchemaDescriptor(null, false).getCollationType(),
                collationDerivation);
	}


	/**
	 * Get the source for this ValueNode.
	 *
	 * @return	The source of this ValueNode, null if this node
     * is not sourced by a column.
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultColumn getSourceResultColumn()
	{
		return null;
	}

	/**
	 * Mark this predicate has having been transformed (other predicates
	 * were generated from it).  This will help us with ensure that the
	 * predicate does not get calculated into the selectivity multiple
	 * times.
	 */
	void setTransformed()
	{
		transformed = true;
	}

	/** 
	 * Return whether or not this predicate has been transformed.
	 *
	 * @return Whether or not this predicate has been transformed.
	 */
	boolean getTransformed()
	{
		return transformed;
	}

	
    ValueNode bindExpression(
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
		throws StandardException
	{
        return bindExpression(fromList, subqueryList, aggregates, false);
	}
	

	/**
	 * Bind this expression.  This is a place-holder method - it should never
	 * be called.
	 *
	 * @param fromList			The FROM list to use for binding
	 * @param subqueryList		The SubqueryList we are building as we hit
	 *							SubqueryNodes.
     * @param aggregates        The aggregate list being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException	Thrown on error
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode bindExpression(FromList fromList,
                             SubqueryList subqueryList,
                             List<AggregateNode> aggregates,
                             boolean forQueryRewrite) throws StandardException
	{
		/* There are a bizillion classes which extend ValueNode.  Here is info
		 * on some of the classes that bindExpression() should not be called on
		 * and why:
		 *	o  BaseColumnNodes should only appear under the ResultColumnList
		 *     in the FromBaseTable.  They are created/bound when binding the
		 *     FromBaseTable.
		 */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(false, 
						"bindExpression() not expected to be called on a " + 
						this.getClass().toString());
		}

		return this;
	}

	/**
	 * Generate a SQL-&gt;Java-&gt;SQL conversion tree above the current node
	 * and bind the new nodes individually.
	 * This is useful when doing comparisons, built-in functions, etc. on
	 * java types which have a direct mapping to system built-in types.
	 *
	 * @return ValueNode	The new tree.
	 *
	 * @exception StandardException	Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode genSQLJavaSQLTree()
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(getTypeId() != null,
				"genSQLJavaSQLTree() only expected to be called on a bound node");
			SanityManager.ASSERT(getTypeId().userType(),
				"genSQLJavaSQLTree() only expected to be called on user types");
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        final ContextManager cm = getContextManager();
        JavaValueNode stjvn = new SQLToJavaValueNode(this, cm);

        ValueNode jtsvn = new JavaToSQLValueNode(stjvn, cm);

        DataTypeDescriptor  resultType;
        if ( (getTypeServices() != null) && getTypeId().userType() ) { resultType = getTypeServices(); }
        else { resultType = DataTypeDescriptor.getSQLDataTypeDescriptor(stjvn.getJavaTypeName()); }

		jtsvn.setType( resultType );

		return jtsvn;
	}

	/**
	 * Preprocess an expression tree.  We do a number of transformations
	 * here (including subqueries, IN lists, LIKE and BETWEEN) plus
	 * subquery flattening.
	 * NOTE: This is done before the outer ResultSetNode is preprocessed.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
		return this;
	}

    /**
     * If this node is known to always evaluate to the same value, return a
     * node that represents that known value as a constant. Typically used to
     * transform operators with constant operands into constants.
     *
     * @return a constant representing the value to which this node is
     * guaranteed to evaluate, or {@code this} if the value is not known
     * @throws StandardException if an error occurs during evaluation
     * @see ConstantExpressionVisitor
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-4416
    ValueNode evaluateConstantExpressions() throws StandardException {
        // We normally don't know what the node evaluates to up front, so
        // don't do anything in the default implementation.
        return this;
    }

	/**
	 * Eliminate NotNodes in the current query block.  We traverse the tree, 
	 * inverting ANDs and ORs and eliminating NOTs as we go.  We stop at 
	 * ComparisonOperators and boolean expressions.  We invert 
	 * ComparisonOperators and replace boolean expressions with 
	 * boolean expression = false.
	 * NOTE: Since we do not recurse under ComparisonOperators, there
	 * still could be NotNodes left in the tree.
	 *
	 * @param	underNotNode		Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	ValueNode eliminateNots(boolean underNotNode) 
					throws StandardException
	{
		if (! underNotNode)
		{
			return this;
		}

		/* bind() has ensured that this node's type is SQLBoolean */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(
				getTypeId().isBooleanTypeId(),
					"Node's type (" +
					getTypeId().getSQLTypeName() +
					") is expected to be boolean");

		/* Return ValueNode = false */
		return genEqualsFalseTree();
	}

	/**
	 * Transform this into this = false.  Useful for NOT elimination.
	 *
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode genEqualsFalseTree()
			throws StandardException
	{
		BinaryRelationalOperatorNode equalsNode;
		BooleanConstantNode		 falseNode;
		boolean 				nullableResult;

        falseNode = new BooleanConstantNode(false, getContextManager());
        equalsNode = new BinaryRelationalOperatorNode(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                BinaryRelationalOperatorNode.K_EQUALS,
                this,
                falseNode,
                false,
                getContextManager());
		nullableResult = getTypeServices().isNullable();
		equalsNode.setType(new DataTypeDescriptor(
									TypeId.BOOLEAN_ID,
									nullableResult)
						  );
		return equalsNode;
	}

	/**
     * Transform this into this IS NULL or IS NOT NULL.
	 *
     * @param notNull if true, transform this into IS NOT NULL;
     *                otherwise, transform this into IS NULL
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
    ValueNode genIsNullTree(boolean notNull)
			throws StandardException
	{
		IsNullNode isNullNode;

        isNullNode = new IsNullNode(this, notNull, getContextManager());
		isNullNode.setType(new DataTypeDescriptor(
									TypeId.BOOLEAN_ID,
									false)
						  );
		return isNullNode;
	}

	/**
	 * Verify that eliminateNots() did its job correctly.  Verify that
	 * there are no NotNodes above the top level comparison operators
	 * and boolean expressions.
	 *
	 * @return		Boolean which reflects validity of the tree.
	 */
	boolean verifyEliminateNots()
	{
		if (SanityManager.ASSERT)
		{
			return (! (this instanceof NotNode));
		}
		else
		{
			return true;
		}
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
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode putAndsOnTop()
					throws StandardException
	{
        BooleanConstantNode trueNode =
                new BooleanConstantNode(true, getContextManager());
        AndNode andNode = new AndNode(this, trueNode, getContextManager());
		andNode.postBindFixup();
		return andNode;
	}

	/**
	 * Verify that putAndsOnTop() did its job correctly.  Verify that the top level 
	 * of the expression is a chain of AndNodes.
	 *
	 * @return		Boolean which reflects validity of the tree.
	 */
    boolean verifyPutAndsOnTop()
	{
		return true;
	}

	/**
	 * Finish putting an expression into conjunctive normal
	 * form.  An expression tree in conjunctive normal form meets
	 * the following criteria:
	 *		o  If the expression tree is not null,
	 *		   the top level will be a chain of AndNodes terminating
	 *		   in a true BooleanConstantNode.
	 *		o  The left child of an AndNode will never be an AndNode.
	 *		o  Any right-linked chain that includes an AndNode will
	 *		   be entirely composed of AndNodes terminated by a true BooleanConstantNode.
	 *		o  The left child of an OrNode will never be an OrNode.
	 *		o  Any right-linked chain that includes an OrNode will
	 *		   be entirely composed of OrNodes terminated by a false BooleanConstantNode.
	 *		o  ValueNodes other than AndNodes and OrNodes are considered
	 *		   leaf nodes for purposes of expression normalization.
	 *		   In other words, we won't do any normalization under
	 *		   those nodes.
	 *
	 * In addition, we track whether or not we are under a top level AndNode.  
	 * SubqueryNodes need to know this for subquery flattening.
	 *
	 * @param	underTopAndNode		Whether or not we are under a top level AndNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode changeToCNF(boolean underTopAndNode)
					throws StandardException
	{
		return this;
	}

	/**
	 * Verify that changeToCNF() did its job correctly.  Verify that:
	 *		o  AndNode  - rightOperand is not instanceof OrNode
	 *				      leftOperand is not instanceof AndNode
	 *		o  OrNode	- rightOperand is not instanceof AndNode
	 *					  leftOperand is not instanceof OrNode
	 *
	 * @return		Boolean which reflects validity of the tree.
	 */
    boolean verifyChangeToCNF()
	{
		return true;
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
	 * RESOLVE - revisit this issue once we have views.
	 *
	 * @param referencedTabs	JBitSet with bit map of referenced FromTables
	 * @param simplePredsOnly	Whether or not to consider method
	 *							calls, field references and conditional nodes
	 *							when building bit map
	 *
	 * @return boolean		Whether or not source.expression is a ColumnReference
	 *						or a VirtualColumnNode.
	 *
	 * @exception StandardException			Thrown on error
	 */
    boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		return true;
	}

	/**
	 * This returns the user-supplied schema name of the column.
	 * At this class level, it simply returns null. But, the subclasses
	 * of ValueNode will overwrite this method to return the
	 * user-supplied schema name.
	 * 
	 * When the value node is in a result column of a select list,
	 * the user can request metadata information. The result column
	 * won't have a column descriptor, so we return some default
	 * information through the expression. This lets expressions that
	 * are simply columns return all of the info, and others use
	 * this supertype's default values.
	 *
	 * @return the default schema name for an expression -- null
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getSchemaName() throws StandardException
	{
		return null;
	}

	/**
	 * This returns the user-supplied table name of the column.
	 * At this class level, it simply returns null. But, the subclasses
	 * of ValueNode will overwrite this method to return the
	 * user-supplied table name.
	 *
	 * When the value node is in a result column of a select list,
	 * the user can request metadata information. The result column
	 * won't have a column descriptor, so we return some default
	 * information through the expression. This lets expressions that
	 * are simply columns return all of the info, and others use
	 * this supertype's default values.
	 *
	 * @return the default table name for an expression -- null
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getTableName()
	{
		return null;
	}

	/**
	 * @return the default updatability for an expression - false
	 */
	public boolean updatableByCursor()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-189
		return false;
	}

	/**
	 * This is null so that the caller will substitute in the resultset generated
	 * name as needed.
	 *
	 * @return the default column name for an expression -- null.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getColumnName()
	{
		return null;
	}

	/**
	 * Get a bit map of table references in this expression
	 *
	 * @return	A bit map of table numbers referred to in this expression
	 *
	 * @exception StandardException			Thrown on error
	 */
	JBitSet getTablesReferenced()
		throws StandardException
	{
		ReferencedTablesVisitor rtv = new ReferencedTablesVisitor(new JBitSet(0));
		accept(rtv);
		return rtv.getTableMap();
	}

	/**
	 * Return whether or not this expression tree is cloneable.
	 *
	 * @return boolean	Whether or not this expression tree is cloneable.
	 */
    boolean isCloneable()
	{
		return false;
	}

	/**
	 * Return a clone of this node.
	 *
	 * @return ValueNode	A clone of this node.
	 *
	 * @exception StandardException			Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode getClone() throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(false,
				"getClone() not expected to be called for " +
				getClass().getName());
		}
		return null;
	}

	/**
	 * Copy all of the "appropriate fields" for a shallow copy.
	 *
	 * @param oldVN		The ValueNode to copy from.
	 *
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void copyFields(ValueNode oldVN) throws StandardException
	{
		dataTypeServices = oldVN.getTypeServices();
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return ValueNode			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode remapColumnReferencesToExpressions() throws StandardException
	{
		return this;
	}

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
    boolean isConstantExpression()
	{
		return false;
	}

	/**
	 * Return whether or not this expression tree represents a constant value.
	 * In this case, "constant" means that it will always evaluate to the
	 * same thing, even if it includes columns.  A column is constant if it
	 * is compared to a constant expression.
	 *
	 * @return	True means this expression tree represents a constant value.
	 */
    boolean constantExpression(PredicateList whereClause)
	{
		return false;
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
	 * @exception StandardException		Thrown on error
	 */
	protected int getOrderableVariantType() throws StandardException
	{
		// The default is VARIANT
		return Qualifier.VARIANT;
	}


	/**
	  * Bind time logic. Raises an error if this ValueNode does not resolve to
	  *	a boolean value. This method is called by WHERE clauses.
	  *
	  *	@return	bound coercion of this node to a builtin type as necessary
	  *
	  * @exception StandardException		Thrown on error
	  */
	public	ValueNode	checkIsBoolean()
		throws StandardException
	{
		ValueNode	whereClause = this;

		/*
		** Is the datatype of the WHERE clause BOOLEAN?
		**
		** NOTE: This test is not necessary in SQL92 entry level, because
		** it is syntactically impossible to have a non-Boolean WHERE clause
		** in that level of the standard.  But we intend to extend the
		** language to allow Boolean user functions in the WHERE clause,
		** so we need to test for the error condition.
		*/
		TypeId whereTypeId = whereClause.getTypeId();

		/* If the where clause is not a built-in type, then generate a bound 
		 * conversion tree to a built-in type.
		 */
//IC see: https://issues.apache.org/jira/browse/DERBY-776
		if (whereTypeId.userType())
		{
			whereClause = whereClause.genSQLJavaSQLTree();
			whereTypeId = whereClause.getTypeId();
		}

		if (! whereTypeId.equals(TypeId.BOOLEAN_ID))
		{
			throw StandardException.newException(SQLState.LANG_NON_BOOLEAN_WHERE_CLAUSE, 
				whereTypeId.getSQLTypeName()
				);
		}

		return	whereClause;
	}

	/**
	 * Return an Object representing the bind time value of this
	 * expression tree.  If the expression tree does not evaluate to
	 * a constant at bind time then we return null.
	 * This is useful for bind time resolution of VTIs.
	 * RESOLVE: What do we do for primitives?
	 *
	 * @return	An Object representing the bind time value of this expression tree.
	 *			(null if not a bind time constant.)
	 *
	 * @exception StandardException		Thrown on error
	 */
	Object getConstantValueAsObject()
		throws StandardException
	{
		return null;
	}

	/////////////////////////////////////////////////////////////////////////
	//
	//	The ValueNode defers its generate() work to a method that works on
	//	ExpressionClassBuilders rather than ActivationClassBuilders. This
	//	is so that expression generation can be shared by the Core compiler
	//	AND the Replication Filter compiler.
	//
	/////////////////////////////////////////////////////////////////////////


	/**
	 * Do the code generation for this node.  Call the more general
	 * routine that generates expressions.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    final void generate(ActivationClassBuilder acb, MethodBuilder mb)
								throws StandardException
	{
		generateExpression( acb, mb );
	}

	/**
	 * The default selectivity for value nodes is 50%.  This is overridden
	 * in specific cases, such as the RelationalOperators.
	 */
	public double selectivity(Optimizable optTable)
//IC see: https://issues.apache.org/jira/browse/DERBY-582
	throws StandardException
	{
		// Return 1 if additional predicates have been generated from this one.
		if (transformed)
		{
			return 1.0;
		}
		else
		{
			return 0.5d;
		}
	}

	/**
	 * Update the array of columns in = conditions with expressions without
	 * column references from the same table.  This is useful when doing
	 * subquery flattening on the basis of an equality condition.
	 * eqOuterCols or tableColMap may be null if the calling routine
	 * doesn't need the information provided
	 *
	 * @param tableNumber	The tableNumber of the table from which
	 *						the columns of interest come from.
	 * @param eqOuterCols	Array of booleans for noting which columns
	 *						are in = predicates without columns from the
	 *						subquery block. May be null.
	 * @param tableNumbers	Array of table numbers in this query block.
	 * @param tableColMap	Array of bits for noting which columns
	 *						are in = predicates for each table in the
	 *						query block. May be null.
	 * @param resultColTable True if tableNumber is the table containing result
	 * 						columns
	 *
	 * @exception StandardException			Thrown on error
	 * 
	 */
	void checkTopPredicatesForEqualsConditions(
				int tableNumber, boolean[] eqOuterCols, int[] tableNumbers, 
				JBitSet[] tableColMap, boolean resultColTable)
		throws StandardException
	{
		for (ValueNode whereWalker = this; whereWalker instanceof AndNode; 
			 whereWalker = ((AndNode) whereWalker).getRightOperand())
		{
			// See if this is a candidate =
			AndNode and = (AndNode) whereWalker;

			if (!and.getLeftOperand().isRelationalOperator() ||
				!(((RelationalOperator)(and.getLeftOperand())).getOperator() == RelationalOperator.EQUALS_RELOP))
			{
				continue;
			}

			BinaryRelationalOperatorNode beon =
					(BinaryRelationalOperatorNode) and.getLeftOperand();
			ValueNode left = beon.getLeftOperand();
			ValueNode right = beon.getRightOperand();
			int resultTable = 0;
			if (resultColTable)
			{
				for ( ; resultTable < tableNumbers.length; resultTable++)
				{
					if (tableNumbers[resultTable] == tableNumber)
						break;
				}
			}
			else
				resultTable = -1;

			/* Is this = of the right form? */
			if ((left instanceof ColumnReference) &&
				((ColumnReference) left).getTableNumber() == tableNumber)
			{
				updateMaps(tableColMap, eqOuterCols, tableNumbers, tableNumber,
					resultTable, right, left);
			}
			else if ((right instanceof ColumnReference) &&
					 ((ColumnReference) right).getTableNumber() == tableNumber)
			{
				updateMaps(tableColMap, eqOuterCols, tableNumbers, tableNumber,
					resultTable, left, right);
			}
		}
	}

	/**
	 * Does this represent a true constant.
	 *
	 * @return Whether or not this node represents a true constant.
	 */
	boolean isBooleanTrue()
	{
		return false;
	}

	/**
	 * Does this represent a false constant.
	 *
	 * @return Whether or not this node represents a false constant.
	 */
	boolean isBooleanFalse()
	{
		return false;
	}

	/**
	 * Generate code for this calculation.  This is a place-holder method -
	 * it should not be called.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
						throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, "Code generation for this type of ValueNode is unimplemented");
	}

	/**
	 * Set the correct bits in tableColMap and set the boolean value in eqOuterCols 
	 * given two arguments to an = predicate
	 * tableColMap[t] - bit is set if the column is in an = predicate with a column 
	 *					in table t, or a bit is set if the column is in an 
	 *					= predicate with a constant,parameter or correlation variable 
	 *				    (for all table t, if this tableColMap is not for the
  	 *					table with the result columns)
	 * eqOuterCols[c] - is true if the column is in an = predicate with a constant,
	 *					parameter or correlation variable
	 * 
	 *
	 * @param tableColMap	Array of bitmaps for noting which columns are in = 
	 *						predicates with columns from each table
	 * @param eqOuterCols	Array of booleans for noting which columns
	 *						are in = predicates without columns from the
	 *						subquery block.
	 * @param tableNumber	table number for which we are setting up the Maps
	 * @param resultTable	-1 if this table is not the result table; otherwise
	 *						the index into tableNumbers for the result table
	 * @param arg1			one side of the = predicate
	 * @param arg2			other side of the = predicate
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void updateMaps(JBitSet[] tableColMap, boolean[] eqOuterCols,
		int[] tableNumbers,  int tableNumber, int resultTable,
		ValueNode arg1, ValueNode arg2)
			throws StandardException
	{
		/* arg2 is a column from our table.  This
		 * is a good = for both All tables and Outer arrays
		 * if the right side is a constant or a parameter
		 * or a column from an outer table.
		 * It is a good = for only the All array if
		 * the right side is a column from this query block.
		 */
//IC see: https://issues.apache.org/jira/browse/DERBY-582
		if ((arg1 instanceof ConstantNode) || (arg1.requiresTypeFromContext()))
		{
			setValueCols(tableColMap, eqOuterCols,
				((ColumnReference) arg2).getColumnNumber(), resultTable);
		}
		else if((arg1 instanceof ColumnReference &&
					((ColumnReference) arg1).getTableNumber() != tableNumber))
		{
			/* See if other columns is a correlation column */
			int otherTN = ((ColumnReference) arg1).getTableNumber();
			int index = 0;
			int colNumber =	((ColumnReference) arg2).getColumnNumber();

			for ( ; index < tableNumbers.length; index++)
			{
				if (otherTN == tableNumbers[index])
				{
					break;
				}
			}
			/* Correlation column, so we can treat it as a constant */
			if (index == tableNumbers.length)
			{
				setValueCols(tableColMap, eqOuterCols, colNumber, resultTable);
			}
			else if (tableColMap != null)
			{
				tableColMap[index].set(colNumber);
			}

		}
		else
		{
			/* See if other side contains a column reference from the same table */
			JBitSet referencedTables = arg1.getTablesReferenced();
			/* See if other columns are all correlation columns */
			int index = 0;
			int colNumber =	((ColumnReference) arg2).getColumnNumber();
			for ( ; index < tableNumbers.length; index++)
			{
				if (referencedTables.get(tableNumbers[index]))
				{
					break;
				}
			}
			/* Correlation column, so we can treat it as a constant */
			if (index == tableNumbers.length)
			{
				setValueCols(tableColMap, eqOuterCols, colNumber, resultTable);
			}
			else if (tableColMap != null && !referencedTables.get(tableNumber))
			{
				tableColMap[index].set(colNumber);
			}
		}
	}
	/**
	 * Set eqOuterCols and the column in all the tables for constants,
	 * parmeters and correlation columns
	 * The column in the tableColMap is set only for the current table
	 * if the table is the result column table.  For other tables in the
	 * query we set the column for all the tables since the constant will
	 * reduced the number of columns required in a unique multicolumn index for
	 * distinctness.
	 * For example, given an unique index on t1(a,b), setting b=1 means that
	 * t1(a) is unique since there can be no duplicates for a where b=1 without
	 * destroying the uniqueness of t1(a,b).  However, for the result columns
	 * setting b=1, does not mean that a select list of t1.a is distinct if
	 * t1.a is the only column used in joining with another table
	 * e.g. select t1.a from t1, t2 where t1.a = t2.a and t1.b = 1;
	 * 
	 * 	t1			t2			result
	 *	a	b		a			a
	 *  1	1		1			1
	 *  1 	2		2			1
	 *	2	1
	 * 
	 *
	 * @param tableColMap	Array of bitmaps for noting which columns are in = 
	 *						predicates with columns from each table
	 * @param eqOuterCols	Array of booleans for noting which columns
	 *						are in = predicates without columns from the
	 *						subquery block.
	 * @param colReference	The column to set
	 * @param resultTable	If -1 set all the bit for all the tables for that
	 *						column; otherwise set the bit for the specified table
	 *
	 *
	 */
	private void setValueCols(JBitSet[] tableColMap, boolean[] eqOuterCols, 
		int colReference, int resultTable)
	{
		if (eqOuterCols != null)
			eqOuterCols[colReference] = true;

		if (tableColMap != null)
		{
			if (resultTable == -1)
			{
				for (int i = 0; i < tableColMap.length; i++)
					tableColMap[i].set(colReference);
			}
			else
				tableColMap[resultTable].set(colReference);
		}
	}

	/**
	 * Returns true if this ValueNode is a relational operator. Relational
	 * Operators are &lt;, &lt;=, =, &gt;, &gt;=, &lt;&gt; as well as IS NULL and IS NOT
	 * NULL. This is the preferred way of figuring out if a ValueNode is
	 * relational or not. 
	 * @see RelationalOperator
	 * @see BinaryRelationalOperatorNode
	 * @see IsNullNode
	*/
    boolean isRelationalOperator()
	{
		return false;
	}
	
	/**
	 * Returns true if this value node is a <em>equals</em> operator. 
	 *
	 * @see ValueNode#isRelationalOperator
	 */
    boolean isBinaryEqualsOperatorNode()
	{
		return false;
	}

	/**
	 * Returns true if this value node is an operator created
	 * for optimized performance of an IN list.
	 *
	 * Or more specifically, returns true if this value node is
	 * an equals operator of the form "col = ?" that we generated
	 * during preprocessing to allow index multi-probing.
	 */
    boolean isInListProbeNode()
	{
		return false;
	}

	/** Return true if the predicate represents an optimizable equality node.
	 * an expression is considered to be an optimizable equality node if all the
	 * following conditions are met:
	 * <ol>
	 * <li> the operator is an <em>=</em> or <em>IS NULL</em> operator </li>
	 * <li> one of the operands is a column specified by optTable/columnNumber</li>
	 * <li> Both operands are not the same column; i.e tab.col = tab.col </li>
	 * <li> There are no implicit varchar comparisons of the operands; i.e
	 * either both operands are string like (varchar, char, longvarchar) or
	 * neither operand is string like </li>
	 * </ol>
	 * 
	 * @param optTable	the table being optimized. Column reference must be from
	 * this table.
	 * @param columnNumber the column number. One of the operands of this
	 * predicate must be the column number specified by optTable/columnNumber
	 * @param isNullOkay if set to true we also consider IS NULL predicates;
	 * otherwise consider only = predicates.
	 */
    boolean optimizableEqualityNode(Optimizable optTable,
										   int columnNumber, 
										   boolean isNullOkay)
		throws StandardException
	{
		return false;
	}

	/**
	 * Returns TRUE if the type of this node will be determined from the
	 * context in which it is getting used. If true is returned then
     * after bindExpression() is called on the node, its type
     * must be set (from the relevant context) using setType().
	 *
	 * @return Whether this node's type will be determined from the context
	 */
    boolean requiresTypeFromContext()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-582
		return false;
	}

	/**
	 * Returns TRUE if this is a parameter node. We do lots of special things
	 * with Parameter Nodes.
	 *
	 */
    boolean isParameterNode()
	{
		return false;
	}
	
	/**
	 * Tests if this node is equivalent to the specified ValueNode. Two 
	 * ValueNodes are considered equivalent if they will evaluate to the same
	 * value during query execution. 
	 * <p> 
	 * This method provides basic expression matching facility for the derived 
	 * class of ValueNode and it is used by the language layer to compare the 
	 * node structural form of the two expressions for equivalence at bind 
	 * phase.  
	 *  <p>
	 * Note that it is not comparing the actual row values at runtime to produce 
	 * a result; hence, when comparing SQL NULLs, they are considered to be 
	 * equivalent and not unknown.  
	 *  <p>
	 * One usage case of this method in this context is to compare the select 
	 * column expression against the group by expression to check if they are 
	 * equivalent.  e.g.:
	 *  <p>
	 * SELECT c1+c2 FROM t1 GROUP BY c1+c2   
	 *  <p>
	 * In general, node equivalence is determined by the derived class of 
	 * ValueNode.  But they generally abide to the rules below:
	 *  <ul>
	 * <li>The two ValueNodes must be of the same node type to be considered 
	 *   equivalent.  e.g.:  CastNode vs. CastNode - equivalent (if their args 
	 *   also match), ColumnReference vs CastNode - not equivalent.
	 *   
	 * <li>If node P contains other ValueNode(s) and so on, those node(s) must 
	 *   also be of the same node type to be considered equivalent.
	 *   
	 * <li>If node P takes a parameter list, then the number of arguments and its 
	 *   arguments for the two nodes must also match to be considered 
	 *   equivalent.  e.g.:  CAST(c1 as INTEGER) vs CAST(c1 as SMALLINT), they 
	 *   are not equivalent.
	 *   
	 * <li>When comparing SQL NULLs in this context, they are considered to be 
	 *   equivalent.
	 * 
	 * <li>If this does not apply or it is determined that the two nodes are not 
	 *   equivalent then the derived class of this method should return false; 
	 *   otherwise, return true.
	 * </ul>   
	 *   
	 * @param other the node to compare this ValueNode against.
	 * @return <code>true</code> if the two nodes are equivalent, 
	 * <code>false</code> otherwise.
	 * 
	 * @throws StandardException 
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    abstract boolean isEquivalent(ValueNode other)
		throws StandardException;

    /**
     * Some node classes represent several logical node types (to reduce
     * footprint), which we call <em>kinds</em>.
     * This means that implementations of {@link #isEquivalent}
     * cannot always just use {@code instanceof} to check if the other node
     * represents the same kind. Hence this method needs to be
     * overridden by all node classes that represent several kinds.
     * This default implementation does not look at kinds.
     * It is only called from implementations of {@code isEquivalent}.
     *
     * @param other The other value node whose kind we want to compare with.
     * @return {@code true} if {@code this} and {@code o} represent the same
     *         logical node type, i.e. kind.
     */
    boolean isSameNodeKind(ValueNode other) {
        // Default implementation does not look at kinds.
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        return other != null && other.getClass().equals(this.getClass());
    }
}
