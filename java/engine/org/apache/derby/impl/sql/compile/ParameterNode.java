/*

   Derby - Class org.apache.derby.impl.sql.compile.ParameterNode

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

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.types.JSQLType;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.impl.sql.execute.BaseActivation;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.sql.Types;

import java.util.Enumeration;
import java.util.Vector;

/**
 * This node type represents a ? parameter.
 *
 * @author Jeff Lichtman
 */

public class ParameterNode extends ValueNode
{

	/*
	** The parameter number for this parameter.  The numbers start at 0.
	*/
	int	parameterNumber;

	/*
	** We want to know if this node was generated or not.
	** It will be skipped if it was in a predicate that
	** was optimized out of the query.  Skipped parameters
	** need to do some minimal generation so that we can
	** make users pass in parameter values for us to ignore.
	*/
	private boolean generated;

	/*
	** Pointer to the array in the DMLStatementNode that holds the
	** DataTypeServices for the parameters.  When each parameter is
	** bound, it fills in its type descriptor in this array.  Note that
	** the array is allocated in the parser, but the individual elements
	** are not filled in until their corresponding parameters are bound.
	*/

	DataTypeDescriptor[]	typeServices;

	/*
	** The default value for this parameter.  Currently, the only
	** reason for a parameter having a default value is for a
	** stored prepared statement, where they are supplied for
	** optimization.
	*/
	DataValueDescriptor		defaultValue;

	/**
	  *	This ParameterNode may turn up as an argument to a replicated Work Unit.
	  *	If so, the remote system will have figured out the type of this node.
	  *	That's what this variable is for.
	  */
	protected	JSQLType			jsqlType;

	private int orderableVariantType = Qualifier.QUERY_INVARIANT;

	/**
	 * By default, we assume we are just a normal, harmless
	 * little ole parameter.  But sometimes we may be a return
	 * parameter (e.g. ? = CALL myMethod()).  
	 */
	private ValueNode returnOutputParameter;

	/**
	 * Constructor for use by the NodeFactory
	 */
	public ParameterNode()
	{
	}

	/**
	 * Initializer for a ParameterNode.
	 *
	 * @param parameterNumber			The number of this parameter,
	 *									(unique per query starting at 0)
	 * @param defaultValue				The default value for this parameter
	 *
	 */

	public void init(Object parameterNumber, Object defaultValue)
	{
		this.defaultValue = (DataValueDescriptor) defaultValue;
		this.parameterNumber = ((Integer) parameterNumber).intValue();
	}

	/**
	 * Get the parameter number
	 *
	 * @return	The parameter number
	 */

	public	int getParameterNumber()
	{
		return parameterNumber;
	}

	/**
	 * Set the descriptor array
	 *
	 * @param	The array of DataTypeServices to fill in when the parameters
	 *			are bound.
	 *
	 * @return	Nothing
	 */

	public void setDescriptors(DataTypeDescriptor[] descriptors)
	{

        // The following is commented out for #3546, for create publication 
        // or target ddl creations there could be multiple statements trying
        // to bind their own parameters. So the following assumptions does not
        // hold true. 

	//	if (SanityManager.DEBUG)
	//	SanityManager.ASSERT(typeServices == null,
	//		"Attempt to re-set typeServices");

		typeServices = descriptors;
	}

	/**
	 * Set the DataTypeServices for this parameter
	 *
	 * @param descriptor	The DataTypeServices to set for this parameter
	 *
	 * @return	Nothing
	 */

	public void setDescriptor(DataTypeDescriptor descriptor)
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(typeServices != null,
			"typeServices not initialized");

		// if type already determined, there's nothing to do.
		// this can occur if a named parameter ("?paramName") is
		// set equal to a unnamed parameter ("?") in a COPY PUBLICATION
		// statement. in this case, the named parameter may be referenced
		// multiple times. each time it must resolve to the same "?"
		// parameter.

		if ( typeServices[parameterNumber] != null ) { return; }

		/* Make sure the type is nullable. */
		if ( ! descriptor.isNullable())
		{
			/*
			** Generate a new descriptor with all the same properties as
			** the given one, except that it is nullable.
			*/
			descriptor = new DataTypeDescriptor(descriptor, true);
		}

		typeServices[parameterNumber] = descriptor;

		setType(descriptor);

		if ( getJSQLType() == null ) { setJSQLType(  new JSQLType( descriptor ) ); }
	}

	/**
	 * Mark this as a return output parameter (e.g.
	 * ? = CALL myMethod())
	 */
	public void setReturnOutputParam(ValueNode valueNode)
	{
		returnOutputParameter = valueNode;
	}

	/**
	 * Is this as a return output parameter (e.g.
	 * ? = CALL myMethod())
	 *
	 * @return true if it is a return param
	 */
	public boolean isReturnOutputParam()
	{
		return returnOutputParameter != null;
	}

	/**
	 * Bind this expression.  A parameter can't figure out what its type
	 * is without knowing where it appears, so this method does nothing.
	 * It is up to the node that points to this parameter node to figure
	 * out the type of the parameter and set it, using the setDescriptor()
	 * method above.
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
		checkReliability( "?", CompilerContext.UNNAMED_PARAMETER_ILLEGAL );

		return this;
	}

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
	public boolean isConstantExpression()
	{
		return true;
	}

	/** @see ValueNode#constantExpression */
	public boolean constantExpression(PredicateList whereClause)
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
	 * For a ParameterNode, we generate a call to the type factory
	 * to get a DataValueDescriptor, and pass the result to the
	 * setStorableDataValue method of the ParameterValueSet for the
	 * generated class.  We push the DataValueDescriptor field as the
	 * generated expression.
	 *
	 * Generated code:
	 *
	 *   In the constructor for the generated class:
	 *
	 *	((ParameterValueSet) pvs).
	 *		setStorableDataValue(
	 *				<generated null>,
	 *				parameterNumber, jdbcType, className);
	 *
	 *   For the return value:
	 *
	 *		(<java type name>)
	 *			( (ParameterValueSet) pvs.
	 *					getParameter(parameterNumber) )
	 *
	 * pvs is a ParameterValueSet that lives in the superclass of the class
	 * being generated.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		// PUSHCOMPILE
		/* Reuse code if possible */
		//if (genRetval != null)
		//	return genRetval;

        /*
        ** First, generate the holder in the constructor.
        */
        generateHolder(acb);

        /* now do the return value */

        /* First, get the field that holds the ParameterValueSet */
        acb.pushPVSReference(mb);

        mb.push(parameterNumber); // arg

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getParameter",
                      ClassName.DataValueDescriptor, 1);

		// For some types perform host variable checking
		// to match DB2/JCC where if a host variable is too
		// big it is not accepted, regardless of any trailing padding.
		DataTypeDescriptor dtd = getTypeServices();

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

	/*
	** parameters might never be used, but still need
	** to have space allocated for them and be assigned
	** to, for the query to operate.
	**
	** This generates the minimum code needed to make
	** the parameter exist.
	*/
	void generateHolder(ExpressionClassBuilder acb) throws StandardException {

		MethodBuilder	constructor = acb.getConstructor();

		if (generated) return;
		generated = true;

		/*
		** First, build the statement in the constructor.
		*/
		acb.pushPVSReference(constructor);

		acb.generateNull(constructor, getTypeCompiler()); constructor.upCast(ClassName.DataValueDescriptor);

		constructor.push(parameterNumber); // second arg
		TypeId myId = getTypeId();
		constructor.push(myId.getJDBCTypeId()); // third arg
		constructor.push(myId.getCorrespondingJavaTypeName()); // fouth arg

		constructor.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "setStorableDataValue", "void", 4);

		/* The constructor portion is done */
	}

	public TypeId getTypeId()
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
	 * @return	Nothing
	 *
	 * @exception StandardException on error
	 */
	static public	void generateParameterValueSet(ExpressionClassBuilder	acb,
								   int		numberOfParameters,
								   Vector	parameterList)
		throws StandardException
	{
		if (numberOfParameters > 0)
		{
			MethodBuilder	constructor = acb.getConstructor();

			/*
			** Check the first parameter to see if it is a return
			** parameter.
			*/
			boolean hasReturnParam = ((ParameterNode)parameterList.elementAt(0)).isReturnOutputParam();

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

	/*
	** When all other generation is done for the statement,
	** we need to ensure all of the parameters have been touched.
	*
	*	@param	acb				ExpressionClassBuilder
	*	@param	parameterList	list of parameter
	*
	* @exception StandardException		Thrown on error
	*/
	static	public	void generateParameterHolders
	( ExpressionClassBuilder acb, Vector parameterList ) 
		throws StandardException
	{
		if (parameterList == null) return;

		for (Enumeration paramEnum = parameterList.elements(); 
			 paramEnum.hasMoreElements(); )
		{
			((ParameterNode)paramEnum.nextElement()).generateHolder(acb);
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
	 * @see ValueNode#isParameterNode
	 */
	public boolean isParameterNode()
	{
		return true;
	}
}
