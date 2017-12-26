/*

   Derby - Class org.apache.derby.impl.sql.compile.ParameterNode

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

import java.sql.Types;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.JSQLType;
import org.apache.derby.iapi.types.TypeId;

/**
 * This node type represents a ? parameter.
 *
 */

public class ParameterNode extends ValueNode
{

	/*
	** The parameter number for this parameter.  The numbers start at 0.
	*/
	private int	parameterNumber;

	/**
	** Pointer to the array in the CompilerContext that holds array
    * of types for all the user-visible paramerers..  When each parameter is
	** bound, it fills in its type descriptor in this array.  Note that
	** the array is allocated in the parser, but the individual elements
	** are not filled in until their corresponding parameters are bound.
    *
    * This array is not read in this class, but is read from the
    * CompilerContext on completion of compiling the statement.
    * 
    *  In some case a parameter node may exist but is not a visble
    *  user parameter, in this case typeServices will be null
    *  so that setting its type will not modify the user's set.
	*/

	private DataTypeDescriptor[]	userParameterTypes;

	/*
	** The default value for this parameter.  Currently, the only
	** reason for a parameter having a default value is for a
	** stored prepared statement, where they are supplied for
	** optimization.
	*/
	private DataValueDescriptor		defaultValue;

	/**
	  *	This ParameterNode may turn up as an argument to a replicated Work Unit.
	  *	If so, the remote system will have figured out the type of this node.
	  *	That's what this variable is for.
	  */
	private	JSQLType			jsqlType;

	private int orderableVariantType = Qualifier.QUERY_INVARIANT;

	/**
	 * By default, we assume we are just a normal, harmless
	 * little ole parameter.  But sometimes we may be a return
	 * parameter (e.g. ? = CALL myMethod()).  
	 */
	private ValueNode returnOutputParameter;

	/**
	 * If this parameter node was created as part of a "probe predicate"
	 * for an InListOperatorNode then it does not actually correspond to
	 * a specific value--we just created it as a start-key place-holder
	 * for IN-list values at execution time.  In order to serve that
	 * purpose we need to generate some value that can be used as the
	 * place-holder.  Since this parameter node is "fake" and does not
	 * correspond to an actual parameter, we can't really generate it;
	 * so the following field holds some legitimate ValueNode--either a
	 * constant node or a "real" parameter node--that we can generate to
	 * serve as the place-holder.
	 */
	private ValueNode valToGenerate;

	/**
     * Constructor for a ParameterNode.
	 *
	 * @param parameterNumber			The number of this parameter,
	 *									(unique per query starting at 0)
	 * @param defaultValue				The default value for this parameter
     * @param cm                        The context manager
	 */
    ParameterNode(int parameterNumber,
                  DataValueDescriptor defaultValue,
                  ContextManager cm) {
        super(cm);
        this.parameterNumber = parameterNumber;
        this.defaultValue = defaultValue;
	}

	/**
	 * Get the parameter number
	 *
	 * @return	The parameter number
	 */

	int getParameterNumber()
	{
		return parameterNumber;
	}

	/**
	 * Set the descriptor array
	 *
	 * @param	descriptors	The array of DataTypeServices to fill in when the parameters
	 *			are bound.
	 */

	void setDescriptors(DataTypeDescriptor[] descriptors)
	{
		userParameterTypes = descriptors;
	}

	/**
	 * Set the DataTypeServices for this parameter
	 *
	 * @param descriptor	The DataTypeServices to set for this parameter
	 */
    @Override
    void setType(DataTypeDescriptor descriptor) throws StandardException
	{
		/* Make sure the type is nullable. */

		/*
		 ** Generate a new descriptor with all the same properties as
		 ** the given one, except that it is nullable.
		 */
		descriptor = descriptor.getNullabilityType(true);
		

        if (userParameterTypes != null)
		    userParameterTypes[parameterNumber] = descriptor;

		//make sure we are calling super's setType. We will get into
		//an infinite loop if this setType ends up calling the local
		//setType method
		super.setType(descriptor);

		if ( getJSQLType() == null ) { setJSQLType(  new JSQLType( descriptor ) ); }
	}

	/**
	 * Mark this as a return output parameter (e.g.
	 * ? = CALL myMethod())
	 */
    void setReturnOutputParam(ValueNode valueNode)
	{
		returnOutputParameter = valueNode;
	}

	/**
	 * Is this as a return output parameter (e.g.
	 * ? = CALL myMethod())
	 *
	 * @return true if it is a return param
	 */
    boolean isReturnOutputParam()
	{
		return returnOutputParameter != null;
	}

	/**
	 * Bind this expression.  A parameter can't figure out what its type
	 * is without knowing where it appears, so this method does nothing.
	 * It is up to the node that points to this parameter node to figure
	 * out the type of the parameter and set it, using the setType()
	 * method above.
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
    ValueNode bindExpression(FromList fromList,
                             SubqueryList subqueryList,
                             List<AggregateNode> aggregates)
            throws StandardException
	{
		checkReliability( "?", CompilerContext.UNNAMED_PARAMETER_ILLEGAL );

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
		return true;
	}

	/** @see ValueNode#constantExpression */
    @Override
    boolean constantExpression(PredicateList whereClause)
	{
		return true;
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
		// Parameters are invariant for the life of the query
		return orderableVariantType;
	}

	/**
	 * In a special circumstance, we want to consider
	 * parameters as constants.  For that situation, we
	 * allow a caller to temporarily set us to CONSTANT
	 * and then restore us.
	 */
	void setOrderableVariantType(int type)
	{
		orderableVariantType = type;
	}

	////////////////////////////////////////////////////////////////////////
	//
	//	OVERRIDE METHODS IN VALUE NODE THAT ARE USED WHILE BINDING REPLICATED
	//	CALL WORK STATEMENTS.
	//
	//	In this scenario, a JSQLType was replicated along with this parameter.
	//	The JSQLType represents the bind() decision of the remote system, which
	//	we want to reproduce locally.
	//
	////////////////////////////////////////////////////////////////////////

	/**
	  *	Set the JSQLType of this parameter. This supports the unnamed parameters
	  *	that we use for replicated work units.
	  *
	  *	@param	type	the JSQLType associated with this parameter
	  */
	public	void	setJSQLType
	(
		JSQLType	type
	)
	{ jsqlType = type; }

	/**
	  *	Get the JSQLType associated with this parameter. Again, part of method
	  *	resolution for replicated work units.
	  *
	  *	@return	the JSQLType that the remote system assigned
	  */
	public	JSQLType	getJSQLType()
	{
		return jsqlType;
	}


	////////////////////////////////////////////////////////////////////
	//
	//	CODE GENERATOR
	//
	////////////////////////////////////////////////////////////////////

	/**
	 * For a ParameterNode, we generate for the return value:
	 *
	 *		(<java type name>)
	 *			( (BaseActivation) this.getParameter(parameterNumber) )
	 *
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{
		/* If we were given a specific ValueNode to generate then
		 * just use that.  See, in particular, the preprocess method
		 * of InListOperatorNode.
		 */
		if (valToGenerate != null)
		{
			valToGenerate.generateExpression(acb, mb);
			return;
		}

		DataTypeDescriptor dtd = getTypeServices();
		if ((dtd != null) && dtd.getTypeId().isXMLTypeId()) {
		// We're a parameter that corresponds to an XML column/target,
		// which we don't allow.  We throw the error here instead of
		// in "bindExpression" because at the time of bindExpression,
		// we don't know yet what the type is going to be (only when
		// the node that points to this parameter calls
		// "setType" do we figure out the type).
			throw StandardException.newException(
				SQLState.LANG_ATTEMPT_TO_BIND_XML);
		}

        /* Generate the return value */

        mb.pushThis();
        mb.push(parameterNumber); // arg

        mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "getParameter",
                      ClassName.DataValueDescriptor, 1);

		// For some types perform host variable checking
		// to match DB2/JCC where if a host variable is too
		// big it is not accepted, regardless of any trailing padding.

		switch (dtd.getJDBCTypeId()) {
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		case Types.BLOB:
			mb.dup();
			mb.push(dtd.getMaximumWidth());
			mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "checkHostVariable",
                      "void", 1);
			break;

		default:
			break;
		}

        /* Cast the result to its specific interface */
        mb.cast(getTypeCompiler().interfaceName());
	} // End of generateExpression

    @Override
    TypeId getTypeId() throws StandardException
	{
		return (returnOutputParameter != null) ?
			returnOutputParameter.getTypeId() : super.getTypeId();
	}

	////////////////////////////////////////////////////////////////////
	//
	//	STATIC ROUTINES
	//
	////////////////////////////////////////////////////////////////////

	/**
	 * Generate the code to create the ParameterValueSet, if necessary,
	 * when constructing the activation.  Also generate the code to call
	 * a method that will throw an exception if we try to execute without
	 * all the parameters being set.
	 * 
	 * This generated code goes into the Activation's constructor early on.
	 * 
	 * @param acb					The ExpressionClassBuilder for the class we're building
	 * @param numberOfParameters	number of parameters for this statement
	 * @param parameterList			The parameter list for the statement.
	 *
	 * @exception StandardException on error
	 */
    static void generateParameterValueSet(ExpressionClassBuilder acb,
								   int		numberOfParameters,
                                   List<ParameterNode>     parameterList)
		throws StandardException
	{
		if (numberOfParameters > 0)
		{
			MethodBuilder	constructor = acb.getConstructor();

			/*
			** Check the first parameter to see if it is a return
			** parameter.
			*/
            boolean
                hasReturnParam = (parameterList.get(0)).isReturnOutputParam();

			/*
			** Generate the following:
			**
			** pvs =
			**		getLanguageConnectionContext()
			**			.getLanguageFactory()
			**					.getParameterValueSet(numberOfParameters);
			**
			** pvs is a ParameterValueSet that lives in the superclass of
			** the activation being generated.
			*/

			constructor.pushThis(); // for the put field down below

			/* Generate the call to getContext */
			//?X constructor.pushThis();
			//?Xconstructor.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.Activation, "getLanguageConnectionContext",
			//?X					ClassName.LanguageConnectionContext, 0);
			/*
			** Call getLanguageFactory()
			*/
			//?Xconstructor.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getLanguageFactory",
			//?X					ClassName.LanguageFactory, 0);

			/*
			** Call getParameterValueSet(<number of parameters>, <hasReturnParam>)
			*/

			constructor.push(numberOfParameters); // first arg
			constructor.push(hasReturnParam); // second arg

			constructor.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation,
									"setParameterValueSet", "void", 2);

			//?Xconstructor.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getParameterValueSet",
			//?X					ClassName.ParameterValueSet, 2);

			/* Assign the return from getParameterValueSet() to the field */
			//?Xconstructor.putField(ClassName.BaseActivation, "pvs", ClassName.ParameterValueSet);
			//?Xconstructor.endStatement();

			/*
			** Add a call to the execute() method to check
			** for missing parameters
			*/
			MethodBuilder	executeMethod = acb.getExecuteMethod();

			executeMethod.pushThis();
			executeMethod.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "throwIfMissingParms", "void", 0);
		}
	}

	/**
	 * Get the default value for the parameter.  Parameters
	 * may get default values for optimization purposes.
	 *
	 * @return the value, may be null
	 */
	DataValueDescriptor getDefaultValue()
	{
		return defaultValue;
	}

	/**
	 * @see ValueNode#requiresTypeFromContext
	 */
    @Override
    boolean requiresTypeFromContext()
	{
		return true;
	}
	
	/**
	 * @see ValueNode#isParameterNode
	 */
    @Override
    boolean isParameterNode()
	{
		return true;
	}

    /**
     * @inheritDoc
     */
    boolean isEquivalent(ValueNode o)
    {
    	return false;
    }

    /**
     * Save the received ValueNode locally so that we can generate it
     * (in place of "this") at generation time.  See the preprocess()
     * method of InListOperatorNode for more on how this is used.
     *
     * @param vn The ValueNode to generate in place of this ParameterNode.
     */
    protected void setValueToGenerate(ValueNode vn)
    {
        valToGenerate = vn;
    }
}
