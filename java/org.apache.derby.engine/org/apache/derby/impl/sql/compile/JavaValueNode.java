/*

   Derby - Class org.apache.derby.impl.sql.compile.JavaValueNode

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

import java.lang.reflect.Modifier;
import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.JSQLType;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.JBitSet;

/**
 * This abstract node class represents a data value in the Java domain.
 */

abstract class JavaValueNode extends QueryTreeNode
{
	private boolean	mustCastToPrimitive;

	protected boolean forCallStatement;
	private boolean valueReturnedToSQLDomain;
	private boolean returnValueDiscarded;

	protected	JSQLType	jsqlType;

	/* Name of field holding receiver value, if any */
	private LocalField receiverField;

        // * Collation type of schema where method is defined. 
	private int collationType;

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    JavaValueNode(ContextManager cm) {
        super(cm);
    }

    /**
     * Get the resolved data type of this node. May be overridden by descendants.
     */
    DataTypeDescriptor getDataType() throws StandardException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4469
        return DataTypeDescriptor.getSQLDataTypeDescriptor( getJavaTypeName()) ;
    }

    final boolean isPrimitiveType() throws StandardException
	{
		JSQLType	myType = getJSQLType();
		
		if ( myType == null ) { return false; }
		else { return ( myType.getCategory() == JSQLType.JAVA_PRIMITIVE ); }
	}

    String getJavaTypeName() throws StandardException
	{
		JSQLType	myType = getJSQLType();

		if ( myType == null ) { return ""; }

		switch( myType.getCategory() )
		{
		    case JSQLType.JAVA_CLASS: return myType.getJavaClassName();

		    case JSQLType.JAVA_PRIMITIVE: return JSQLType.getPrimitiveName( myType.getPrimitiveKind() );

		    default:

				if (SanityManager.DEBUG)
				{ SanityManager.THROWASSERT( "Inappropriate JSQLType: " + myType ); }
		}

		return "";
	}

    final void setJavaTypeName(String javaTypeName)
	{
		jsqlType = new JSQLType( javaTypeName );
	}

    String getPrimitiveTypeName()
		throws StandardException
	{
		JSQLType	myType = getJSQLType();

		if ( myType == null ) { return ""; }

		switch( myType.getCategory() )
		{
		    case JSQLType.JAVA_PRIMITIVE: return JSQLType.getPrimitiveName( myType.getPrimitiveKind() );
//IC see: https://issues.apache.org/jira/browse/DERBY-4293
//IC see: https://issues.apache.org/jira/browse/DERBY-4293

		    default:

				if (SanityManager.DEBUG)
				{ SanityManager.THROWASSERT( "Inappropriate JSQLType: " + myType ); }
		}

		return "";
	}

	/**
	  *	Toggles whether the code generator should add a cast to extract a primitive
	  *	value from an object.
	  *
	  *	@param	booleanValue	true if we want the code generator to add a cast
	  *							false otherwise
	  */
    final void castToPrimitive(boolean booleanValue)
	{
		mustCastToPrimitive = booleanValue;
	}

	/**
	  *	Reports whether the code generator should add a cast to extract a primitive
	  *	value from an object.
	  *
	  *	@return	true if we want the code generator to add a cast
	  *				false otherwise
	  */
    final boolean mustCastToPrimitive() { return mustCastToPrimitive; }

	/**
	  *	Get the JSQLType that corresponds to this node. Could be a SQLTYPE,
	  *	a Java primitive, or a Java class.
	  *
	  *	@return	the corresponding JSQLType
	  *
	  */
    JSQLType getJSQLType() throws StandardException {
        return jsqlType;
    }


	/**
	  *	Map a JSQLType to a compilation type id.
	  *
	  *	@param	jsqlType	the universal type to map
	  *
	  *	@return	the corresponding compilation type id
	  *
	  */
    static TypeId mapToTypeID(JSQLType jsqlType)
//IC see: https://issues.apache.org/jira/browse/DERBY-4484
        throws StandardException
	{
		DataTypeDescriptor	dts = jsqlType.getSQLType();

		if ( dts == null ) { return null; }

		return dts.getTypeId();
	}

	/**
	 * Mark this node as being for a CALL Statement.
	 * (void methods are only okay for CALL Statements)
	 */
    final void markForCallStatement()
	{
		forCallStatement = true;
	}

	/**
	 * @see ValueNode#remapColumnReferencesToExpressions
	 *
	 * @exception StandardException		Thrown on error
	 */
    abstract JavaValueNode remapColumnReferencesToExpressions()
		throws StandardException;

	/**
	 * @see ValueNode#categorize
	 *
	 * @exception StandardException		Thrown on error
	 */
    abstract boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException;

	/**
	 * @see ValueNode#bindExpression
	 *
	 * @return the new node, usually this
	 *
	 * @exception StandardException		Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
    abstract JavaValueNode bindExpression(FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
							throws StandardException;
	/**
	 * @see ValueNode#preprocess
	 *
	 * @exception StandardException		Thrown on error
	 */
    abstract void preprocess(int numTables,
									FromList outerFromList,
									SubqueryList outerSubqueryList,
									PredicateList outerPredicateList)
							throws StandardException;

	/** @see ValueNode#getConstantValueAsObject 
	 *
	 * @exception StandardException		Thrown on error
	 */
	Object getConstantValueAsObject()
		throws StandardException
	{
		return null;
	}

	/**
	 * Do the code generation for this node.  Call the more general
	 * routine that generates expressions.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb the method  the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */

    @Override
    final   void generate(ActivationClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{
		generateExpression( acb, mb );
	}

	/**
	 * Generate the expression that evaluates to the receiver. This is
	 * for the case where a java expression is being returned to the SQL
	 * domain, and we need to check whether the receiver is null (if so,
	 * the SQL value should be set to null, and this Java expression
	 * not evaluated). Instance method calls and field references have
	 * receivers, while class method calls and calls to constructors do
	 * not. If this Java expression does not have a receiver, this method
	 * returns null.
	 *
	 * The implementation of this method should only generate the receiver
	 * once and cache it in a field. This is because there will be two
	 * references to the receiver, and we want to evaluate it only once.
	 *
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb the method  the expression will go into
	 *
	 * @return		True if has compiled receiver.
	 *
	 * @exception StandardException		Thrown on error
	 */
    boolean generateReceiver(ExpressionClassBuilder acb,
													MethodBuilder mb)
									throws StandardException
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
	 */
    int getOrderableVariantType() throws StandardException
	{
		// The default is VARIANT
		return Qualifier.VARIANT;
		//return Qualifier.SCAN_INVARIANT;
	}

	/**
	 * General logic shared by Core compilation and by the Replication Filter
	 * compiler. Every child of ValueNode must implement one of these methods.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb the method the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */
    abstract void generateExpression(
											ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException;

	/**
	 * Generate the expression that evaluates to the receiver. This is
	 * for the case where a java expression is being returned to the SQL
	 * domain, and we need to check whether the receiver is null (if so,
	 * the SQL value should be set to null, and this Java expression
	 * not evaluated). Instance method calls and field references have
	 * receivers, while class method calls and calls to constructors do
	 * not. If this Java expression does not have a receiver, this method
	 * returns null.
	 *
	 * This also covers the case where a java expression is being returned
	 * to the Java domain. In this case, we need to check whether the
	 * receiver is null only if the value returned by the Java expression
	 * is an object (not a primitive type). We don't want to generate the
	 * expression here if we are returning a primitive type to the Java
	 * domain, because there's no point in checking whether the receiver
	 * is null in this case (we can't make the expression return a null
	 * value).
	 *
	 * Only generate the receiver once and cache it in a field. This is
	 * because there will be two references to the receiver, and we want
	 * to evaluate it only once.
	 *
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb the method the expression will go into
	 * @param receiver	The query tree form of the receiver expression
	 *
	 * @return		The compiled receiver, if any.
	 *
	 * @exception StandardException		Thrown on error
	 */
    final boolean generateReceiver(ExpressionClassBuilder acb,
											MethodBuilder mb,
											JavaValueNode receiver)
									throws StandardException
	{
		/*
		** Don't generate the expression now if it returns a primitive
		** type to the Java domain.
		*/
		if ( (! valueReturnedToSQLDomain()) &&
//IC see: https://issues.apache.org/jira/browse/DERBY-5055
				ClassInspector.primitiveType(getJavaTypeName()))
		{
			return false;
		}

		/*
		** Generate the following:
		**
		** <receiver class> <field name>;
		** <field name> = <receiver>;
		**
		** for non-static calls.
		*/

		String receiverClassName = receiver.getJavaTypeName();
		receiverField =
				acb.newFieldDeclaration(Modifier.PRIVATE, receiverClassName);

		receiver.generateExpression(acb, mb);
		mb.putField(receiverField);
		return true;
	}

	/**
	 * Get an expression that has the value of the receiver. If a field
	 * holding the receiver value was already generated, use that.  If not,
	 * generate the receiver value.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb the method  the expression will go into
	 * @param receiver	The query tree form of the receiver expression
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    final void getReceiverExpression(ExpressionClassBuilder acb,
									MethodBuilder mb,
									JavaValueNode receiver)
										throws StandardException
	{
		if (receiverField != null)
		{
			mb.getField(receiverField);
		}
		else
		{
			receiver.generateExpression(acb, mb);
		}
	}

	/** Inform this node that it returns its value to the SQL domain */
    void returnValueToSQLDomain()
	{
		valueReturnedToSQLDomain = true;
	}

	/** Tell whether this node returns its value to the SQL domain */
    boolean valueReturnedToSQLDomain()
	{
		return valueReturnedToSQLDomain;
	}

	/** Tell this node that nothing is done with the returned value */
    void markReturnValueDiscarded()
	{
		returnValueDiscarded = true;
	}

	/** Tell whether the return value from this node is discarded */
    boolean returnValueDiscarded()
	{
		return returnValueDiscarded;
	}

	/**
		Check the reliability type of this java value.

	    @exception StandardException		Thrown on error

		@see org.apache.derby.iapi.sql.compile.CompilerContext
	*/
    void checkReliability(ValueNode sqlNode) throws StandardException {
        sqlNode.checkReliability( 
                CompilerContext.FUNCTION_CALL_ILLEGAL,
                SQLState.LANG_JAVA_METHOD_CALL_OR_FIELD_REF
                );
	}

    /**
     * @return collationType as set by setCollationType
     */
    int getCollationType() {
        return collationType;
    }
    
    /**
     * Set the collation type.
     * This will be used to determine the collation type for 
     * the SQLToJavaValueNode.
     * 
     * @param type one of <code>StringDataValue.COLLATION_TYPE_UCS_BASIC </code> or
     *                    <code>StringDataValue.COLLATION_TYPE_TERRITORY_BASED </code>  
     */
    void setCollationType(int type) {
        collationType = type;
    }
    
    
}
