/*

   Derby - Class org.apache.derby.impl.sql.compile.UnaryOperatorNode

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

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.sql.compile.Visitor;

import org.apache.derby.iapi.reference.JDBC40Translation;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import java.lang.reflect.Modifier;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.sql.Types;
import java.util.Vector;

/**
 * A UnaryOperatorNode represents a built-in unary operator as defined by
 * the ANSI/ISO SQL standard.  This covers operators like +, -, NOT, and IS NULL.
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 */

public class UnaryOperatorNode extends OperatorNode
{
	String	operator;
	String	methodName;
    
    /**
     * Operator type, only valid for XMLPARSE and XMLSERIALIZE.
     */
	private int operatorType;

	String		resultInterfaceType;
	String		receiverInterfaceType;

	/**
	 * WARNING: operand may be NULL for COUNT(*).  
	 */
	ValueNode	operand;

	// At the time of adding XML support, it was decided that
	// we should avoid creating new OperatorNodes where possible.
	// So for the XML-related unary operators we just add the
	// necessary code to _this_ class, similar to what is done in
	// TernarnyOperatorNode. Subsequent unary operators (whether
	// XML-related or not) should follow this example when
	// possible.
    //
    // This has lead to this class having somewhat of
    // a confused personality. In one mode it is really
    // a parent (abstract) class for various unary operator
    // node implementations, in its other mode it is a concrete
    // class for XMLPARSE and XMLSERIALIZE.

	public final static int XMLPARSE_OP = 0;
	public final static int XMLSERIALIZE_OP = 1;

	// NOTE: in the following 4 arrays, order
	// IS important.

	static final String[] UnaryOperators = {
		"xmlparse",
		"xmlserialize"
	};

	static final String[] UnaryMethodNames = {
		"XMLParse",
		"XMLSerialize"
	};

	static final String[] UnaryResultTypes = {
		ClassName.XMLDataValue, 		// XMLParse
		ClassName.StringDataValue		// XMLSerialize
	};

	static final String[] UnaryArgTypes = {
		ClassName.StringDataValue,		// XMLParse
		ClassName.XMLDataValue			// XMLSerialize
	};

	// Array to hold Objects that contain primitive
	// args required by the operator method call.
	private Object [] additionalArgs;

	/**
	 * Initializer for a UnaryOperatorNode.
	 *
	 * <ul>
	 * @param operand	The operand of the node
	 * @param operatorOrOpType	Either 1) the name of the operator,
	 *  OR 2) an Integer holding the operatorType for this operator.
	 * @param methodNameOrAddedArgs	Either 1) name of the method
	 *  to call for this operator, or 2) an array of Objects
	 *  from which primitive method parameters can be
	 *  retrieved.
	 */

	public void init(
					Object	operand,
					Object		operatorOrOpType,
					Object		methodNameOrAddedArgs)
	{
		this.operand = (ValueNode) operand;
		if (operatorOrOpType instanceof String) {
		// then 2nd and 3rd params are operator and methodName,
		// respectively.
			this.operator = (String) operatorOrOpType;
			this.methodName = (String) methodNameOrAddedArgs;
			this.operatorType = -1;
		}
		else {
		// 2nd and 3rd params are operatorType and additional args,
		// respectively.
			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(
					((operatorOrOpType instanceof Integer) &&
						((methodNameOrAddedArgs == null) ||
						(methodNameOrAddedArgs instanceof Object[]))),
					"Init params in UnaryOperator node have the " +
					"wrong type.");
			}
			this.operatorType = ((Integer) operatorOrOpType).intValue();
			this.operator = UnaryOperators[this.operatorType];
			this.methodName = UnaryMethodNames[this.operatorType];
			this.resultInterfaceType = UnaryResultTypes[this.operatorType];
			this.receiverInterfaceType = UnaryArgTypes[this.operatorType];
			this.additionalArgs = (Object[])methodNameOrAddedArgs;
		}
	}

	/**
	 * Initializer for a UnaryOperatorNode
	 *
	 * @param operand	The operand of the node
	 */
	public void init(Object	operand)
	{
		this.operand = (ValueNode) operand;
		this.operatorType = -1;
	}

	/**
	 * Set the operator.
	 *
	 * @param operator	The operator.
	 */
	void setOperator(String operator)
	{
		this.operator = operator;
		this.operatorType = -1;
	}

	/**
	 * Get the operator of this unary operator.
	 *
	 * @return	The operator of this unary operator.
	 */
	String getOperatorString()
	{
		return operator;
	}

	/**
	 * Set the methodName.
	 *
	 * @param methodName	The methodName.
	 */
	void setMethodName(String methodName)
	{
		this.methodName = methodName;
		this.operatorType = -1;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return		This object as a String
	 */

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
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (operand != null)
			{
				printLabel(depth, "operand: ");
				operand.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Get the operand of this unary operator.
	 *
	 * @return	The operand of this unary operator.
	 */
	public ValueNode getOperand()
	{
		return operand;
	}

	/**
	 * Get the parameter operand of this unary operator.
	 * For the example below, for abs unary operator node, we want to get ?
	 * select * from t1 where -? = max_cni(abs(-?), sqrt(+?))
	 * 
	 * This gets called when ParameterNode is needed to get parameter
	 * specific information like getDefaultValue(), getParameterNumber() etc 
	 * 
	 * @return	The parameter operand of this unary operator else null.
	 */
	public ParameterNode getParameterOperand() throws StandardException
	{
		if (requiresTypeFromContext() == false)
			return null;
		else {
			UnaryOperatorNode tempUON = this;
			while (!(tempUON.getOperand() instanceof ParameterNode)) 
				tempUON = (UnaryOperatorNode)tempUON.getOperand();
			return (ParameterNode)(tempUON.getOperand());
		}
	}


	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
     * This method is the implementation for XMLPARSE and XMLSERIALIZE.
     * Sub-classes need to implement their own bindExpression() method
     * for their own specific rules.
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

	public ValueNode bindExpression(
					FromList fromList, SubqueryList subqueryList,
					Vector	aggregateVector)
				throws StandardException
	{
		bindOperand(fromList, subqueryList, aggregateVector);
        if (operatorType == XMLPARSE_OP)
            bindXMLParse();
        else if (operatorType == XMLSERIALIZE_OP)
            bindXMLSerialize();
        return this;
	}

	/**
	 * Bind the operand for this unary operator.
     * Binding the operator may change the operand node.
     * Sub-classes bindExpression() methods need to call this
     * method to bind the operand.
	 */
	protected void bindOperand(
					FromList fromList, SubqueryList subqueryList,
					Vector	aggregateVector)
				throws StandardException
	{
		operand = operand.bindExpression(fromList, subqueryList,
								aggregateVector);

		if (operand.requiresTypeFromContext()) {
			bindParameter();
            // If not bound yet then just return.
            // The node type will be set by either
            // this class' bindExpression() or a by
            // a node that contains this expression.
            if (operand.getTypeServices() == null)
                return;
        }

		/* If the operand is not a built-in type, then generate a bound conversion
		 * tree to a built-in type.
		 */
		if (! (operand instanceof UntypedNullConstantNode) &&
			operand.getTypeId().userType() &&
			! (this instanceof IsNullNode))
		{
			operand = operand.genSQLJavaSQLTree();
		}
	}

    /**
     * Bind an XMLPARSE operator.  Makes sure the operand type
     * is correct, and sets the result type.
     *
     * @exception StandardException Thrown on error
     */
    private void bindXMLParse() throws StandardException
    {
        // Check the type of the operand - this function is allowed only on
        // string value (char) types.
        TypeId operandType = operand.getTypeId();
        if (operandType != null) {
            switch (operandType.getJDBCTypeId())
            {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CLOB:
                    break;
                default:
                {
                    throw StandardException.newException(
                        SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
                        methodName,
                        operandType.getSQLTypeName());
                }
            }
        }

        // The result type of XMLParse() is always an XML type.
        setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                JDBC40Translation.SQLXML));
    }

    /**
     * Bind an XMLSERIALIZE operator.  Makes sure the operand type
     * and target type are both correct, and sets the result type.
     *
     * @exception StandardException Thrown on error
     */
    private void bindXMLSerialize() throws StandardException
    {
        TypeId operandType;

        // Check the type of the operand - this function is allowed only on
        // the XML type.
        operandType = operand.getTypeId();
        if ((operandType != null) && !operandType.isXMLTypeId())
        {
            throw StandardException.newException(
                SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
                methodName,
                operandType.getSQLTypeName());
        }

        // Check the target type.  We only allow string types to be used as
        // the target type.  The targetType is stored as the first Object
        // in our list of additional parameters, so we have to retrieve
        // it from there.
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                ((additionalArgs != null) && (additionalArgs.length > 0)),
                "Failed to locate target type for XMLSERIALIZE operator");
        }

        DataTypeDescriptor targetType =
            (DataTypeDescriptor)additionalArgs[0];

        TypeId targetTypeId = targetType.getTypeId();
        switch (targetTypeId.getJDBCTypeId())
        {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                break;
            default:
            {
                throw StandardException.newException(
                    SQLState.LANG_INVALID_XMLSERIALIZE_TYPE,
                    targetTypeId.getSQLTypeName());
            }
        }

        // The result type of XMLSerialize() is always a string; which
        // kind of string is determined by the targetType field.
        setType(targetType);
		//Set the collation type to be same as the current schema's 
		//collation type. 
        setCollationUsingCompilationSchema();
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
	public ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
		if (operand != null)
		{
			operand = operand.preprocess(numTables,
										 outerFromList, outerSubqueryList,
										 outerPredicateList);
		}
		return this;
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
	public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		return (operand == null) ? 
				false : 
				operand.categorize(referencedTabs, simplePredsOnly);
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
		if (operand != null)
		{
			operand = operand.remapColumnReferencesToExpressions();
		}
		return this;
	}

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
	public boolean isConstantExpression()
	{
		return (operand == null) ? true: operand.isConstantExpression();
	}

	/** @see ValueNode#constantExpression */
	public boolean constantExpression(PredicateList whereClause)
	{
		return (operand == null) ?
					true :
					operand.constantExpression(whereClause);
	}

	/**
	 * By default unary operators don't accept ? parameters as operands.
	 * This can be over-ridden for particular unary operators.
	 *
	 *	We throw an exception if the parameter doesn't have a datatype
	 *	assigned to it yet.
	 *
	 * @exception StandardException		Thrown if ?  parameter doesn't
	 *									have a type bound to it yet.
	 *									? parameter where it isn't allowed.
	 */

	void bindParameter() throws StandardException
	{
		if (operatorType == XMLPARSE_OP)
		{
			/* SQL/XML[2006] allows both binary and character strings for
			 * the XMLParse parameter (section 10.16:Function).  The spec
			 * also goes on to say, in section 6.15:Conformance Rules:4,
			 * that:
			 *
			 * "Without Feature X066, XMLParse: BLOB input and DOCUMENT
			 * option, in conforming SQL language, the declared type of
			 * the <string value expression> immediately contained in
			 * <XML parse> shall not be a binary string type."
			 *
			 * Thus since Derby doesn't currently support BLOB input,
			 * we have to ensure that the "declared type" of the parameter
			 * is not a binary string type; i.e. it must be a character
			 * string type.  Since there's no way to determine what the
			 * declared type is from the XMLPARSE syntax, the user must
			 * explicitly declare the type of the parameter, and it must
			 * be a character string. They way s/he does that is by
			 * specifying an explicit CAST on the parameter, such as:
			 *
			 *  insert into myXmlTable (xcol) values
			 *    XMLPARSE(DOCUMENT cast (? as CLOB) PRESERVE WHITESPACE);
			 *
			 * If that was done then we wouldn't be here; we only get
			 * here if the parameter was specified without a cast.  That
			 * means we don't know what the "declared type" is and so
			 * we throw an error.
			 */
			throw StandardException.newException(
				SQLState.LANG_XMLPARSE_UNKNOWN_PARAM_TYPE);
		}
		else if (operatorType == XMLSERIALIZE_OP) {
        // For now, since JDBC has no type defined for XML, we
        // don't allow binding to an XML parameter.
	        throw StandardException.newException(
 	           SQLState.LANG_ATTEMPT_TO_BIND_XML);
		}
		else if (operand.getTypeServices() == null)
		{
			throw StandardException.newException(SQLState.LANG_UNARY_OPERAND_PARM, operator);
		}
	}

	/**
	 * Do code generation for this unary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		String resultTypeName = 
			(operatorType == -1)
				? getTypeCompiler().interfaceName()
				: resultInterfaceType;
			
		// System.out.println("resultTypeName " + resultTypeName + " method " + methodName);
		// System.out.println("isBooleanTypeId() " + getTypeId().isBooleanTypeId());

		boolean needField = !getTypeId().isBooleanTypeId();

		String receiverType = getReceiverInterfaceName();
		operand.generateExpression(acb, mb);
		mb.cast(receiverType);

		if (needField) {

			/* Allocate an object for re-use to hold the result of the operator */
			LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);
			mb.getField(field);

            int numArgs = 1;

            // XML operators take extra arguments.
            numArgs += addXmlOpMethodParams(acb, mb, field);

            mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                          methodName, resultTypeName, numArgs);

			/*
			** Store the result of the method call in the field, so we can re-use
			** the object.
			*/
			mb.putField(field);
		} else {
			mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
				methodName, resultTypeName, 0);
		}
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
	public String getReceiverInterfaceName() throws StandardException {
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(operand!=null,
								"cannot get interface without operand");
		}

		if (operatorType != -1)
			return receiverInterfaceType;
		
		return operand.getTypeCompiler().interfaceName();
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
	 *		CONSTANT			- immutable
	 *
	 * @return	The variant type for the underlying expression.
	 * @exception StandardException	thrown on error
	 */
	protected int getOrderableVariantType() throws StandardException
	{
		/*
		** If we have nothing in the operator, then
		** it must be constant.
		*/
		return (operand != null) ?
				operand.getOrderableVariantType() :
				Qualifier.CONSTANT;
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

		if (operand != null)
		{
			operand = (ValueNode)operand.accept(v);
		}
	}

    /**
     * Add some additional arguments to our method call for
     * XML related operations like XMLPARSE and XMLSERIALIZE.
     *
     * @param acb the builder for the class in which the method lives
     * @param mb The MethodBuilder that will make the call.
     * @param resultField the field that contains the previous result
     * @return Number of parameters added.
     */
    protected int addXmlOpMethodParams(ExpressionClassBuilder acb,
		MethodBuilder mb, LocalField resultField) throws StandardException
    {
        if ((operatorType != XMLPARSE_OP) && (operatorType != XMLSERIALIZE_OP))
        // nothing to do.
            return 0;

        if (operatorType == XMLSERIALIZE_OP) {
        // We push the target type's JDBC type id as well as
        // the maximum width, since both are required when
        // we actually perform the operation, and both are
        // primitive types.  Note: we don't have to save
        // any objects for XMLSERIALIZE because it doesn't
        // require any XML-specific objects: it just returns
        // the serialized version of the XML value, which we
        // already found when the XML value was created (ex.
        // as part of the XMLPARSE work).
        // We also need to pass the collation type of the current
        // compilation schema. If the JDBC type id is of type
        // StringDataValue, then we should use the collation to
        // decide whether we need to generate collation sensitive
        // StringDataValue.
            DataTypeDescriptor targetType =
                (DataTypeDescriptor)additionalArgs[0];
            mb.push(targetType.getJDBCTypeId());
            mb.push(targetType.getMaximumWidth());
            mb.push(getSchemaDescriptor(null, false).getCollationType());
            return 3;
        }

        /* Else we're here for XMLPARSE. */

        // XMLPARSE is different from other unary operators in that the method
        // must be called on the result object (the XML value) and not on the
        // operand (the string value). We must therefore make sure the result
        // object is not null.
        MethodBuilder constructor = acb.getConstructor();
        acb.generateNull(constructor, getTypeCompiler(),
                         getTypeServices().getCollationType());
        constructor.setField(resultField);

        // Swap operand and result object so that the method will be called
        // on the result object.
        mb.swap();

        // Push whether or not we want to preserve whitespace.
        mb.push(((Boolean)additionalArgs[0]).booleanValue());

        // Push the SqlXmlUtil instance as the next argument.
        pushSqlXmlUtil(acb, mb, null, null);

        return 2;
    }
    
    /**
     * @throws StandardException 
     * {@inheritDoc}
     */
    protected boolean isEquivalent(ValueNode o) throws StandardException
    {
    	if (isSameNodeType(o)) 
    	{
		// the first condition in the || covers the case when 
	    	// both operands are null.
    		UnaryOperatorNode other = (UnaryOperatorNode)o;
    		return (operator.equals(other.operator) && 
			((operand == other.operand)|| 
			 ((operand != null) && operand.isEquivalent(other.operand))));
    	}
    	return false;
    }
}
