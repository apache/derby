/*

   Derby - Class org.apache.derby.impl.sql.compile.BinaryOperatorNode

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

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.JBitSet;

/**
 * A BinaryOperatorNode represents a built-in binary operator as defined by
 * the ANSI/ISO SQL standard.  This covers operators like +, -, *, /, =, <, etc.
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 */

class BinaryOperatorNode extends OperatorNode
{
	String	operator;
	String	methodName;
	ValueNode	receiver; // used in generation

	/*
	** These identifiers are used in the grammar.
	*/
    final static int PLUS   = 1;
    final static int MINUS  = 2;
    final static int TIMES  = 3;
    final static int DIVIDE = 4;
    final static int CONCATENATE    = 5;
    final static int EQ = 6;
    final static int NE = 7;
    final static int GT = 8;
    final static int GE = 9;
    final static int LT = 10;
    final static int LE = 11;
    final static int AND    = 12;
    final static int OR = 13;
    final static int LIKE   = 14;

	ValueNode	leftOperand;
	ValueNode	rightOperand;

	String		leftInterfaceType;
	String		rightInterfaceType;
	String		resultInterfaceType;

	// At the time of adding XML support, it was decided that
	// we should avoid creating new OperatorNodes where possible.
	// So for the XML-related binary operators we just add the
	// necessary code to _this_ class, similar to what is done in
	// TernarnyOperatorNode. Subsequent binary operators (whether
	// XML-related or not) should follow this example when
	// possible.

    // Allowed kinds
    final static int K_XMLEXISTS = 0;
    final static int K_XMLQUERY = 1;
    final static int K_BASE = 2; // when BinaryOperatorNode is used
                                 // as a base class
    /**
     * This class is used to hold logically different objects for
     * space efficiency. {@code kind} represents the logical object
     * type. See also {@link ValueNode#isSameNodeKind}.
     */
    final int kind;

	// NOTE: in the following 4 arrays, order
	// IS important.

	static final String[] BinaryOperators = {
		"xmlexists",
		"xmlquery"
	};

	static final String[] BinaryMethodNames = {
		"XMLExists",
		"XMLQuery"
	};

	static final String[] BinaryResultTypes = {
		ClassName.BooleanDataValue,		// XMLExists
		ClassName.XMLDataValue			// XMLQuery
	};

	static final String[][] BinaryArgTypes = {
		{ClassName.StringDataValue, ClassName.XMLDataValue},	// XMLExists
		{ClassName.StringDataValue, ClassName.XMLDataValue}		// XMLQuery
	};

    /** The query expression if the operator is XMLEXISTS or XMLQUERY. */
    private String xmlQuery;

    BinaryOperatorNode(ContextManager cm) {
        super(cm);
        kind = K_BASE;
    }

    BinaryOperatorNode(
            ValueNode leftOperand,
            ValueNode rightOperand,
            String operator,
            String methodName,
            String leftInterfaceType,
            String rightInterfaceType,
            ContextManager cm) {
        super(cm);
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
        this.operator = operator;
        this.methodName = methodName;
        this.leftInterfaceType = leftInterfaceType;
        this.rightInterfaceType = rightInterfaceType;
        this.kind = K_BASE;
    }

    BinaryOperatorNode(
            ValueNode leftOperand,
            ValueNode rightOperand,
            String leftInterfaceType,
            String rightInterfaceType,
            ContextManager cm)
	{
        super(cm);
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
        this.leftInterfaceType = leftInterfaceType;
        this.rightInterfaceType = rightInterfaceType;
        this.kind = K_BASE;
	}

	/**
     * Constructor for a concrete BinaryOperatorNode
	 *
	 * @param leftOperand	The left operand of the node
	 * @param rightOperand	The right operand of the node
     * @param kind          The kind of operator.
	 */
    BinaryOperatorNode(
            ValueNode leftOperand,
            ValueNode rightOperand,
            int kind,
            ContextManager cm)
	{
        super(cm);
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
        this.kind = kind;
        this.operator = BinaryOperators[this.kind];
        this.methodName = BinaryMethodNames[this.kind];
        this.leftInterfaceType = BinaryArgTypes[this.kind][0];
        this.rightInterfaceType = BinaryArgTypes[this.kind][1];
        this.resultInterfaceType = BinaryResultTypes[this.kind];
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
			return "operator: " + operator + "\n" +
				"methodName: " + methodName + "\n" + 
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Set the operator.
	 *
	 * @param operator	The operator.
	 */
	void setOperator(String operator)
	{
		this.operator = operator;
	}

	/**
	 * Set the methodName.
	 *
	 * @param methodName	The methodName.
	 */
	void setMethodName(String methodName)
	{
		this.methodName = methodName;
	}

	/**
	 * Set the interface type for the left and right arguments.
	 * Used when we don't know the interface type until
	 * later in binding.
	 */
    void setLeftRightInterfaceType(String iType)
	{
		leftInterfaceType = iType;
		rightInterfaceType = iType;
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
    void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (leftOperand != null)
			{
				printLabel(depth, "leftOperand: ");
				leftOperand.treePrint(depth + 1);
			}

			if (rightOperand != null)
			{
				printLabel(depth, "rightOperand: ");
				rightOperand.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
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
    ValueNode bindExpression(
        FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
			throws StandardException
	{
		leftOperand = leftOperand.bindExpression(fromList, subqueryList, 
            aggregates);
		rightOperand = rightOperand.bindExpression(fromList, subqueryList, 
            aggregates);

        if ((kind == K_XMLEXISTS) ||
            (kind == K_XMLQUERY)) {
			return bindXMLQuery();
        }

		/* Is there a ? parameter on the left? */
		if (leftOperand.requiresTypeFromContext())
		{
			/*
			** It's an error if both operands are ? parameters.
			*/
			if (rightOperand.requiresTypeFromContext())
			{
				throw StandardException.newException(SQLState.LANG_BINARY_OPERANDS_BOTH_PARMS, 
																	operator);
			}

			/* Set the left operand to the type of right parameter. */
			leftOperand.setType(rightOperand.getTypeServices());
		}

		/* Is there a ? parameter on the right? */
		if (rightOperand.requiresTypeFromContext())
		{
			/* Set the right operand to the type of the left parameter. */
			rightOperand.setType(leftOperand.getTypeServices());
		}

		return genSQLJavaSQLTree();
	}

    /**
     * Bind an XMLEXISTS or XMLQUERY operator.  Makes sure
     * the operand type and target type are both correct
     * and sets the result type.
     *
     * @exception StandardException Thrown on error
     */
    public ValueNode bindXMLQuery()
        throws StandardException
    {
        // Check operand types.
        TypeId rightOperandType = rightOperand.getTypeId();

        // Left operand is query expression and must be a string
        // literal.  SQL/XML spec doesn't allow params nor expressions
        // 6.17: <XQuery expression> ::= <character string literal> 
        if (!(leftOperand instanceof CharConstantNode))
        {
            throw StandardException.newException(
                SQLState.LANG_INVALID_XML_QUERY_EXPRESSION);
        }
        else {
            xmlQuery = ((CharConstantNode)leftOperand).getString();
        }

        // Right operand must be an XML data value.  NOTE: This
        // is a Derby-specific restriction, not an SQL/XML one.
        // We have this restriction because the query engine
        // that we use (currently Xalan) cannot handle non-XML
        // context items.
        if ((rightOperandType != null) &&
            !rightOperandType.isXMLTypeId())
        {
            throw StandardException.newException(
                SQLState.LANG_INVALID_CONTEXT_ITEM_TYPE,
                rightOperandType.getSQLTypeName());
        }

        // Is there a ? parameter on the right?
        if (rightOperand.requiresTypeFromContext())
        {
            // For now, since JDBC has no type defined for XML, we
            // don't allow binding to an XML parameter.
            throw StandardException.newException(
                SQLState.LANG_ATTEMPT_TO_BIND_XML);
        }

        // Set the result type of this operator.
        if (kind == K_XMLEXISTS) {
            // For XMLEXISTS, the result type is always SQLBoolean.
            // The "true" in the next line says that the result
            // can be nullable--which it can be if evaluation of
            // the expression returns a null (this is per SQL/XML
            // spec, 8.4)
            setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, true));
        }
        else {
            // The result of an XMLQUERY operator is always another
            // XML data value, per SQL/XML spec 6.17: "...yielding a value
            // X1 of an XML type."
            setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.SQLXML));
        }

        return genSQLJavaSQLTree();
    }

	/** generate a SQL->Java->SQL conversion tree above the left and right
	 * operand of this Binary Operator Node if needed. Subclasses can override
	 * the default behavior.
	 */
    @Override
    ValueNode genSQLJavaSQLTree() throws StandardException
	{
		TypeId leftTypeId = leftOperand.getTypeId();
		
		if (leftTypeId.userType())
			leftOperand = leftOperand.genSQLJavaSQLTree();

		TypeId rightTypeId = rightOperand.getTypeId();
		if (rightTypeId.userType())
			rightOperand = rightOperand.genSQLJavaSQLTree();

		return this;
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
    @Override
    ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
		leftOperand = leftOperand.preprocess(numTables,
											 outerFromList, outerSubqueryList,
											 outerPredicateList);
		rightOperand = rightOperand.preprocess(numTables,
											   outerFromList, outerSubqueryList,
											   outerPredicateList);
		return this;
	}

	/**
	 * Do code generation for this binary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
		throws StandardException
	{
		/* If this BinaryOperatorNode was created as a part of an IN-list
		 * "probe predicate" then we do not want to generate the relational
		 * operator itself; instead we want to generate the underlying
		 * IN-list for which this operator node was created.
		 *
		 * We'll get here in situations where the optimizer chooses a plan
		 * for which the probe predicate is *not* a useful start/stop key
		 * and thus is not being used for execution-time index probing.
		 * In this case we are effectively "reverting" the probe predicate
		 * back to the InListOperatorNode from which it was created.  Or put
		 * another way, we are "giving up" on index multi-probing and simply
		 * generating the original IN-list as a regular restriction.
		 */
		if (this instanceof BinaryRelationalOperatorNode)
		{
			InListOperatorNode ilon =
				((BinaryRelationalOperatorNode)this).getInListOp();

			if (ilon != null)
			{
				ilon.generateExpression(acb, mb);
				return;
			}
		}

		String		resultTypeName;
		String		receiverType;

/*
** if i have a operator.getOrderableType() == constant, then just cache 
** it in a field.  if i have QUERY_INVARIANT, then it would be good to
** cache it in something that is initialized each execution,
** but how?
*/

        // The number of arguments to pass to the method that implements the
        // operator, depends on the type of the operator.
        int numArgs;

		// If we're dealing with XMLEXISTS or XMLQUERY, there is some
		// additional work to be done.
		boolean xmlGen =
           (kind == K_XMLQUERY) || (kind == K_XMLEXISTS);

		/*
		** The receiver is the operand with the higher type precedence.
		** Like always makes the left the receiver.
		**
		*/
		if (leftOperand.getTypeId().typePrecedence() >
			rightOperand.getTypeId().typePrecedence())
		{
			receiver = leftOperand;
			/*
			** let the receiver type be determined by an
			** overridable method so that if methods are
			** not implemented on the lowest interface of
			** a class, they can note that in the implementation
			** of the node that uses the method.
			*/
            receiverType = (kind == K_BASE)
				? getReceiverInterfaceName()
				: leftInterfaceType;

			/*
			** Generate (with <left expression> only being evaluated once)
			**
			**	<left expression>.method(<left expression>, <right expression>...)
			*/

			leftOperand.generateExpression(acb, mb);
			mb.cast(receiverType); // cast the method instance
			// stack: left
			
			mb.dup();
			mb.cast(leftInterfaceType);
			// stack: left, left
			
			rightOperand.generateExpression(acb, mb);
			mb.cast(rightInterfaceType); // second arg with cast
			// stack: left, left, right

            // We've pushed two arguments
            numArgs = 2;
		}
		else
		{
			receiver = rightOperand;
			/*
			** let the receiver type be determined by an
			** overridable method so that if methods are
			** not implemented on the lowest interface of
			** a class, they can note that in the implementation
			** of the node that uses the method.
			*/
            receiverType = (kind == K_BASE)
				? getReceiverInterfaceName()
				: rightInterfaceType;

			/*
			** Generate (with <right expression> only being evaluated once)
			**
			**	<right expression>.method(<left expression>, <right expression>)
			**
			** UNLESS we're generating an XML operator such as XMLEXISTS.
			** In that case we want to generate
			** 
			**  <right expression>.method(sqlXmlUtil)
			*/

			rightOperand.generateExpression(acb, mb);			
			mb.cast(receiverType); // cast the method instance
			// stack: right
			
            if (xmlGen) {
                // Push one argument (the SqlXmlUtil instance)
                numArgs = 1;
                pushSqlXmlUtil(acb, mb, xmlQuery, operator);
                // stack: right,sqlXmlUtil
            } else {
                // Push two arguments (left, right)
                numArgs = 2;

				mb.dup();
				mb.cast(rightInterfaceType);
				// stack: right,right
			
                leftOperand.generateExpression(acb, mb);
                mb.cast(leftInterfaceType); // second arg with cast
                // stack: right,right,left

                mb.swap();
                // stack: right,left,right
            }
		}

		/* Figure out the result type name */
        resultTypeName = (kind == K_BASE)
			? getTypeCompiler().interfaceName()
			: resultInterfaceType;

        // Boolean return types don't need a result field. For other types,
        // allocate an object for re-use to hold the result of the operator.
        LocalField resultField = getTypeId().isBooleanTypeId() ?
            null : acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);

        // Push the result field onto the stack, if there is a result field.
		if (resultField != null) {
			/*
			** Call the method for this operator.
			*/
			mb.getField(resultField); // third arg

            // Adjust number of arguments for the result field
            numArgs++;

			/* pass statically calculated scale to decimal divide method to make
			 * result set scale consistent, beetle 3901
			 */
			int jdbcType;
			if ((getTypeServices() != null) &&
				((jdbcType = getTypeServices().getJDBCTypeId()) == java.sql.Types.DECIMAL ||
				 jdbcType == java.sql.Types.NUMERIC) &&
				operator.equals("/"))
			{
				mb.push(getTypeServices().getScale());		// 4th arg
                numArgs++;
			}
        }

        mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType,
                      methodName, resultTypeName, numArgs);

        // Store the result of the method call, if there is a result field.
        if (resultField != null) {
			//the need for following if was realized while fixing bug 5704 where decimal*decimal was resulting an overflow value but we were not detecting it
			if (getTypeId().variableLength())//since result type is numeric variable length, generate setWidth code.
			{
				if (getTypeId().isNumericTypeId())
				{
					// to leave the DataValueDescriptor value on the stack, since setWidth is void
					mb.dup();

					mb.push(getTypeServices().getPrecision());
					mb.push(getTypeServices().getScale());
					mb.push(true);
					mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.VariableSizeDataValue, "setWidth", "void", 3);
				}
			}


			/*
			** Store the result of the method call in the field, so we can re-use
			** the object.
			*/

			mb.putField(resultField);
		}
	}

	/**
	 * Set the leftOperand to the specified ValueNode
	 *
	 * @param newLeftOperand	The new leftOperand
	 */
    void setLeftOperand(ValueNode newLeftOperand)
	{
		leftOperand = newLeftOperand;
	}

	/**
	 * Get the leftOperand
	 *
	 * @return The current leftOperand.
	 */
    ValueNode getLeftOperand()
	{
		return leftOperand;
	}

	/**
	 * Set the rightOperand to the specified ValueNode
	 *
	 * @param newRightOperand	The new rightOperand
	 */
    void setRightOperand(ValueNode newRightOperand)
	{
		rightOperand = newRightOperand;
	}

	/**
	 * Get the rightOperand
	 *
	 * @return The current rightOperand.
	 */
    ValueNode getRightOperand()
	{
		return rightOperand;
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
	 * @exception StandardException			Thrown on error
	 */
    @Override
    boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		boolean pushable;
		pushable = leftOperand.categorize(referencedTabs, simplePredsOnly);
		pushable = (rightOperand.categorize(referencedTabs, simplePredsOnly) && pushable);
		return pushable;
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
    ValueNode remapColumnReferencesToExpressions()
		throws StandardException
	{
		leftOperand = leftOperand.remapColumnReferencesToExpressions();
		rightOperand = rightOperand.remapColumnReferencesToExpressions();
		return this;
	}

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
    @Override
    boolean isConstantExpression()
	{
		return (leftOperand.isConstantExpression() &&
				rightOperand.isConstantExpression());
	}

	/** @see ValueNode#constantExpression */
    @Override
    boolean constantExpression(PredicateList whereClause)
	{
		return (leftOperand.constantExpression(whereClause) &&
				rightOperand.constantExpression(whereClause));
	}

	/**
	 * Determine the type the binary method is called on.
	 * By default, based on the receiver.
	 *
	 * Override in nodes that use methods on super-interfaces of
	 * the receiver's interface, such as comparisons.
	 *
	 * @exception StandardException		Thrown on error
	 */
    String getReceiverInterfaceName() throws StandardException {
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(receiver!=null,"can't get receiver interface name until receiver is set");
		}

		return receiver.getTypeCompiler().interfaceName();
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *		CONSTANT			- immutable
	 *
	 * @return	The variant type for the underlying expression.
	 * @exception StandardException	thrown on error
	 */
    @Override
	protected int getOrderableVariantType() throws StandardException
	{
		int leftType = leftOperand.getOrderableVariantType();
		int rightType = rightOperand.getOrderableVariantType();

		return Math.min(leftType, rightType);
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
    @Override
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

		if (leftOperand != null)
		{
			leftOperand = (ValueNode)leftOperand.accept(v);
		}

		if (rightOperand != null)
		{
			rightOperand = (ValueNode)rightOperand.accept(v);
		}
	}

    @Override
    boolean isSameNodeKind(ValueNode o) {
        return super.isSameNodeKind(o) &&
                ((BinaryOperatorNode)o).kind == this.kind;
    }

    /**
     * @inheritDoc
     */
    boolean isEquivalent(ValueNode o) throws StandardException
    {
        if (isSameNodeKind(o)) {
            BinaryOperatorNode other = (BinaryOperatorNode)o;
            return methodName.equals(other.methodName)
                    && leftOperand.isEquivalent(other.leftOperand)
                    && rightOperand.isEquivalent(other.rightOperand);
        } else {
            return false;
        }
    }
}
