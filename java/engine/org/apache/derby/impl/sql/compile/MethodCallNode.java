/*

   Derby - Class org.apache.derby.impl.sql.compile.MethodCallNode

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

import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.JSQLType;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.JDBC30Translation;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.util.JBitSet;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.catalog.types.RoutineAliasInfo;

import java.lang.reflect.Modifier;
import java.lang.reflect.Member;

import java.util.Vector;

/**
 * A MethodCallNode represents a Java method call.  Method calls can be done
 * through DML (as expressions) or through the CALL statement.
 *
 * @author Jeff Lichtman
 */

public abstract class MethodCallNode extends JavaValueNode
{
	/*
	** Name of the method.
	*/
	protected String	methodName;

    /** The name of the class containing the method. May not be known until bindExpression() has been called.
     * @see #bindExpression
     * @see #getJavaClassName()
     */
    protected String javaClassName;
	
	/**
		For a procedure or function call
	*/
	RoutineAliasInfo routineInfo;


	/**
		True if this is an internal call, just used to set up a generated method call.
	*/
	boolean internalCall;

	/**
		For resolution of procedure INOUT/OUT parameters to the primitive
		form, such as int[]. May be null.
	*/
	private String[] procedurePrimitiveArrayType;

	// bound signature of arguments, stated in universal types (JSQLType)
	protected JSQLType[]				signature;

	/*
	** Parameters to the method, if any.  No elements if no parameters.
	*/
	protected JavaValueNode[]	methodParms;

	/* The method call */
	protected Member method;

	protected String actualMethodReturnType;

	/**
	  *	Gets the signature of JSQLTypes needed to propagate a work unit from
	  *	target to source.
	  *
	  *	@return	the JSQLType signature
	  */
	public	JSQLType[]	getSignature()
	{
		return	signature;
	}

	/**
		The parameter types for the resolved method.
	*/
	String[] methodParameterTypes;

	/**
	 * Initializer for a MethodCallNode
	 *
	 * @param	methodName	The name of the method to call
	 */
	public void init(Object methodName)
	{
		this.methodName = (String) methodName;
	}

	public String getMethodName()
	{
		return  methodName;
	}

    /**
     * @return the name of the class that contains the method, null if not known. It may not be known
     *         until this node has been bound.
     */
    public String getJavaClassName()
    {
        return javaClassName;
    }
    
	/**
	 * Set the clause that this node appears in.
	 *
	 * @param clause	The clause that this node appears in.
	 *
	 * @return Nothing.
	 */
	public void setClause(int clause)
	{
		super.setClause(clause);
		if (methodParms != null)
		{
			for (int parm = 0; parm < methodParms.length; parm++)
			{
				if (methodParms[parm] != null)
				{
					methodParms[parm].setClause(clause);
				}
			}
		}
	}

	/**
	 * Add the parameter list.
	 * (This flavor is useful when transforming a non-static method call node
	 * to a static method call node.)
	 *
	 * @param methodParms		JavaValueNode[]
	 *
	 * @return	Nothing
	 */

	public void addParms(JavaValueNode[] methodParms)
	{
		this.methodParms = methodParms;
	}

	/**
	 * Add the parameter list
	 *
	 * @param parameterList		A Vector of the parameters
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void addParms(Vector parameterList) throws StandardException
	{
		methodParms = new JavaValueNode[parameterList.size()];

		int	plSize = parameterList.size();
		for (int index = 0; index < plSize; index++)
		{
			QueryTreeNode	qt;

			qt = (QueryTreeNode) parameterList.elementAt(index);



			/*
			** If the parameter is a SQL ValueNode, there are two
			** possibilities.  Either it is a JavaValueNode with
			** a JavaToSQLValueNode on top of it, or it is a plain
			** SQL ValueNode.  In the former case, just get rid of
			** the JavaToSQLValueNode.  In the latter case, put a
			** SQLToJavaValueNode on top of it.  In general, we
			** want to avoid converting the same value back and forth
			** between the SQL and Java domains.
			*/
			if ( ! (qt instanceof JavaValueNode))
			{
				if (qt instanceof JavaToSQLValueNode)
				{
					qt = ((JavaToSQLValueNode) qt).getJavaValueNode();
				}
				else
				{
					qt = (SQLToJavaValueNode) getNodeFactory().
							getNode(
								C_NodeTypes.SQL_TO_JAVA_VALUE_NODE,
								qt,
								getContextManager());
				}
			}

			methodParms[index] = (JavaValueNode) qt;
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
			int	parm;

			super.printSubNodes(depth);
			if (methodParms != null)
			{
				for (parm = 0; parm < methodParms.length; parm++)
				{
					if (methodParms[parm] != null)
					{
						printLabel(depth, "methodParms[" + parm + "] :");
						methodParms[parm].treePrint(depth + 1);
					}
				}
			}
		}
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
			return "methodName: " +
					(methodName != null ? methodName : "null") + "\n" +
					super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	this
	 *
	 * @exception StandardException		Thrown on error
	 */

	final void bindParameters(
		FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector) 
			throws StandardException
	{
		/* Bind the parameters */
		if (methodParms != null)
		{
			int		count = methodParms.length;

			// with a procedure call the signature
			// is preformed in StaticMethodCall from
			// the procedures signature.
			if (signature == null) 
				signature = new JSQLType[ count ];

			for (int parm = 0; parm < count; parm++)
			{
				if (methodParms[parm] != null)
				{
					methodParms[parm] =
						methodParms[parm].bindExpression(
							fromList, subqueryList, aggregateVector);

					if (routineInfo == null)
						signature[ parm ] = methodParms[ parm ].getJSQLType();
                    
                    // prohibit LOB columns/types
                    if (signature[parm] != null) {
                        String type = signature[parm].getSQLType().getTypeId().getSQLTypeName();
                        if (type.equals("BLOB") || type.equals("CLOB") || type.equals("NCLOB")) {
                            throw StandardException.newException(SQLState.LOB_AS_METHOD_ARGUMENT_OR_RECEIVER);
                        }
                    }
				}
			}
		}
	}

	/**
	 * Return whether or not all of the parameters to this node are
	 * QUERY_INVARIANT or CONSTANT.  This is useful for VTIs - a VTI is a candidate
	 * for materialization if all of its parameters are QUERY_INVARIANT or CONSTANT
	 *
	 * @return Whether or not all of the parameters to this node are QUERY_INVARIANT or CONSTANT
	 * @exception StandardException	thrown on error
	 */
	 protected boolean areParametersQueryInvariant() throws StandardException
	 {
		return (getVariantTypeOfParams() == Qualifier.QUERY_INVARIANT);
	 }

	/**
	 * Build parameters for error message and throw the exception when there
	 * is no matching signature found.
	 *
	 * @param receiverTypeName	Type name for receiver
	 * @param parmTypeNames		Type names for parameters as object types
	 * @param primParmTypeNames	Type names for parameters as primitive types
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void throwNoMethodFound(String receiverTypeName,
									  String[] parmTypeNames,
									  String[] primParmTypeNames)
		throws StandardException
	{
		/* Put the parameter type names into a single string */
		StringBuffer	parmTypes = new StringBuffer();
		for (int i = 0; i < parmTypeNames.length; i++)
		{
			if (i != 0)
				parmTypes.append(", ");
			/* RESOLVE - shouldn't be using hard coded strings for output */
			parmTypes.append( (parmTypeNames[i].length() != 0 ?
								parmTypeNames[i] :
								"UNTYPED"));
			if ((primParmTypeNames != null) &&
				! primParmTypeNames[i].equals(parmTypeNames[i]))  // has primitive
				parmTypes.append("(" + primParmTypeNames[i] + ")");
		}

		throw StandardException.newException(SQLState.LANG_NO_METHOD_FOUND, 
												receiverTypeName,
												methodName,
											 	parmTypes);
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
	 * @return	Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void preprocess(int numTables,
							FromList outerFromList,
							SubqueryList outerSubqueryList,
							PredicateList outerPredicateList) 
					throws StandardException
	{
		int	parm;

		/* Preprocess the parameters */
		if (methodParms != null)
		{
			for (parm = 0; parm < methodParms.length; parm++)
			{
				if (methodParms[parm] != null)
				{
					methodParms[parm].preprocess(numTables,
												 outerFromList,
												 outerSubqueryList,
												 outerPredicateList);
				}
			}
		}
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
	public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		/* We stop here when only considering simple predicates
		 *  as we don't consider method calls when looking
		 * for null invariant predicates.
		 */
		if (simplePredsOnly)
		{
			return false;
		}

		boolean pushable = true;
		int		param;

		if (methodParms != null)
		{
			for (param = 0; param < methodParms.length; param++)
			{
				if (methodParms[param] != null)
				{
					pushable = methodParms[param].categorize(referencedTabs, simplePredsOnly) &&
							   pushable;
				}
			}
		}

		/* We need to push down method call.  Then the predicate can be used for start/stop
		 * key for index scan.  The fact that method call's cost is not predictable and can
		 * be expensive doesn't mean we shouldn't push it down. Beetle 4826.
		 */
		return pushable;
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return JavaValueNode			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public JavaValueNode remapColumnReferencesToExpressions()
		throws StandardException
	{
		int	param;

		if (methodParms != null)
		{
			for (param = 0; param < methodParms.length; param++)
			{
				if (methodParms[param] != null)
				{
					methodParms[param] =
						methodParms[param].remapColumnReferencesToExpressions();
				}
			}
		}
		return this;
	}

	/**
	 * Generate the parameters to the given method call
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb the method  the expression will go into
	 *
	 * @return	Count of arguments to the method.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public	int generateParameters(ExpressionClassBuilder acb,
											MethodBuilder mb)
			throws StandardException
	{
		int				param;

		String[] expectedTypes = methodParameterTypes;

		ClassInspector classInspector = getClassFactory().getClassInspector();

		/* Generate the code for each user parameter, generating the appropriate
		 * cast when the passed type needs to get widened to the expected type.
		 */
		for (param = 0; param < methodParms.length; param++)
		{
			generateOneParameter( acb, mb, param );

			// type from the SQL-J expression
			String argumentType = getParameterTypeName( methodParms[param] );

			// type of the method
			String parameterType = expectedTypes[param];

			if (!parameterType.equals(argumentType))
			{
				// since we reached here through method resolution
				// casts are only required for primitive types.
				// In any other case the expression type must be assignable
				// to the parameter type.
				if (classInspector.primitiveType(parameterType)) {

					mb.cast(parameterType);

				} else {

					// for a prodcedure
					if (routineInfo != null) {
						continue; // probably should be only for INOUT/OUT parameters.
					}

					if (SanityManager.DEBUG) {
						SanityManager.ASSERT(classInspector.assignableTo(argumentType, parameterType),
							"Argument type " + argumentType + " is not assignable to parameter " + parameterType);
					}

					/*
					** Set the parameter type in case the argument type is narrower
					** than the parameter type.
					*/
					mb.upCast(parameterType);

				}
			}

		}

		return methodParms.length;
	}

	static	public	String	getParameterTypeName( JavaValueNode param )
		throws StandardException
	{
		String	argumentType;

		// RESOLVE - shouldn't this logic be inside JavaValueNode ??
		// I.e. once the value is primitive then its java type name is its
		// primitive type name.
		if (param.isPrimitiveType()) { argumentType = param.getPrimitiveTypeName(); }
		else { argumentType = param.getJavaTypeName(); }

		return	argumentType;
	}

	/**
	 * Generate one parameter to the given method call. This method is overriden by
	 * RepStaticMethodCallNode.
	 *
	 * @param acb				The ExpressionClassBuilder for the class we're generating
	 * @param mb the method the expression will go into
	 * @param parameterNumber	Identifies which parameter to generate. 0 based.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public	void generateOneParameter(ExpressionClassBuilder acb,
											MethodBuilder mb,
											int parameterNumber )
			throws StandardException
	{
		methodParms[parameterNumber].generateExpression(acb, mb);
	}

	/**
	 * Set the appropriate type information for a null passed as a parameter.
	 * This method is called after method resolution, when a signature was
	 * successfully matched.
	 *
	 * @param parmTypeNames	String[] with the java type names for the parameters
	 *        as declared by the method
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	setNullParameterInfo(String[] parmTypeNames)
			throws StandardException
	{
		for (int i = 0; i < methodParms.length; i++)
		{
			/* null parameters are represented by a java type name of "" */
			if (methodParms[i].getJavaTypeName().equals(""))
			{		
				/* Set the type information in the null constant node */
				DataTypeDescriptor dts = DataTypeDescriptor.getSQLDataTypeDescriptor(parmTypeNames[i]);
				((SQLToJavaValueNode)methodParms[i]).value.setDescriptor(
																	dts);

				/* Set the correct java type name */
				methodParms[i].setJavaTypeName(parmTypeNames[i]);
				signature[i] = methodParms[i].getJSQLType();
			}
		}
	}

	protected void resolveMethodCall(String javaClassName,
									 boolean staticMethod) 
				throws StandardException
	{
		// only allow direct method calls through routines and internal SQL.
		if (routineInfo == null && !internalCall)
		{
			// See if we are being executed in an internal context
			if ((getCompilerContext().getReliability() & CompilerContext.INTERNAL_SQL_ILLEGAL) != 0) {
				throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,  javaClassName + (staticMethod ? "::" : ".") + methodName);
			}
		}

		int			count = signature.length;

		ClassInspector classInspector = getClassFactory().getClassInspector();

		String[]		parmTypeNames = getObjectSignature();
		String[]		primParmTypeNames = null;
		boolean[]		isParam = getIsParam();

		boolean hasDynamicResultSets = (routineInfo != null) && (count != 0) && (count != methodParms.length);

		/*
		** Find the matching method that is public.
		*/
		try
		{
			/* First try with built-in types and mappings */
			method = classInspector.findPublicMethod(javaClassName,
												methodName,
												parmTypeNames,
												null,
												isParam,
												staticMethod,
												hasDynamicResultSets);


			// DB2 LUW does not support Java object types for SMALLINT, INTEGER, BIGINT, REAL, DOUBLE
			// and these are the only types that can map to a primitive or an object type according
			// to SQL part 13. So we never have a second chance match.
			if (routineInfo == null) {

				/* If no match, then retry with combinations of object and 
				 * primitive types.
				 */
				if (method == null)
				{
					primParmTypeNames = getPrimitiveSignature(false);

					method = classInspector.findPublicMethod(javaClassName,
												methodName,
												parmTypeNames,
												primParmTypeNames,
												isParam,
												staticMethod,
												hasDynamicResultSets);
				}
			}
		}
		catch (ClassNotFoundException e)
		{
			/*
			** If one of the classes couldn't be found, just act like the
			** method couldn't be found.  The error lists all the class names,
			** which should give the user enough info to diagnose the problem.
			*/
			method = null;
		}

		/* Throw exception if no matching signature found */
		if (method == null)
		{
			throwNoMethodFound(javaClassName, parmTypeNames, primParmTypeNames);
		}

		String	typeName = classInspector.getType(method);
		actualMethodReturnType = typeName;

		if (routineInfo == null) {

			/* void methods are only okay for CALL Statements */
			if (typeName.equals("void"))
			{
				if (!forCallStatement)
					throw StandardException.newException(SQLState.LANG_VOID_METHOD_CALL);
			}
		}
		else
		{
			String promoteName = null;
			TypeDescriptor returnType = routineInfo.getReturnType();
			String requiredType;
			if (returnType == null)
			{
				// must have a void method for a procedure call.
				requiredType = "void";
			}
			else
			{


				// DB2 LUW does not support Java object types for SMALLINT, INTEGER, BIGINT, REAL, DOUBLE
				// and these are the only types that can map to a primitive or an object type according
				// to SQL part 13. So always map to the primitive type. We can not use the getPrimitiveSignature()
				// as it (incorrectly but historically always has) maps a DECIMAL to a double. 

				
				TypeId returnTypeId = TypeId.getBuiltInTypeId(returnType.getJDBCTypeId());
				switch (returnType.getJDBCTypeId()) {
				case java.sql.Types.SMALLINT:
				case java.sql.Types.INTEGER:
				case java.sql.Types.BIGINT:
				case java.sql.Types.REAL:
				case java.sql.Types.DOUBLE:
					TypeCompiler tc = getTypeCompiler(returnTypeId);
					requiredType = tc.getCorrespondingPrimitiveTypeName();
					if (!routineInfo.calledOnNullInput() && routineInfo.getParameterCount() != 0)
					{
						promoteName = returnTypeId.getCorrespondingJavaTypeName();
					}

					break;
				default:
					requiredType = returnTypeId.getCorrespondingJavaTypeName();
					break;
				}

			}

			if (!requiredType.equals(typeName))
			{
				throwNoMethodFound(requiredType + " " + javaClassName, parmTypeNames, primParmTypeNames);
			}

			// for a returns null on null input with a primitive
			// type we need to promote to an object so we can return null.
			if (promoteName != null)
				typeName = promoteName;
		}
	 	setJavaTypeName( typeName );

		methodParameterTypes = classInspector.getParameterTypes(method);

		for (int i = 0; i < methodParameterTypes.length; i++)
		{
			String methodParameter = methodParameterTypes[i];

			if (routineInfo != null) {
				if (i < routineInfo.getParameterCount()) {
					int parameterMode = routineInfo.getParameterModes()[i];

					switch (parameterMode) {
					case JDBC30Translation.PARAMETER_MODE_IN:
						break;
					case JDBC30Translation.PARAMETER_MODE_IN_OUT:
						// we need to see if the type of the array is
						// primitive, not the array itself.
						methodParameter = methodParameter.substring(0, methodParameter.length() - 2);
						break;

					case JDBC30Translation.PARAMETER_MODE_OUT:
						// value is not obtained *from* parameter.
						continue;
					}
				}
			}

			if (classInspector.primitiveType(methodParameter))
				methodParms[i].castToPrimitive(true);
		}

		/* Set type info for any null parameters */
		if ( someParametersAreNull() )
		{
			setNullParameterInfo(methodParameterTypes);
		}


    
		/* bug 4450 - if the callable statement is ? = call form, generate the metadata
		infor for the return parameter. We don't really need that info in order to
		execute the callable statement. But with jdbc3.0, this information should be
		made available for return parameter through ParameterMetaData class.
		Parser sets a flag in compilercontext if ? = call. If the flag is set,
		we generate the metadata info for the return parameter and reset the flag
		in the compilercontext for future call statements*/
		DataTypeDescriptor dts = DataTypeDescriptor.getSQLDataTypeDescriptor(typeName);
		if (getCompilerContext().getReturnParameterFlag()) {
			getCompilerContext().getParameterTypes()[0] = dts;
		}
  }

	/**
	  *	Return true if some parameters are null, false otherwise.
	  */
	protected	boolean	someParametersAreNull()
	{
		int		count = signature.length;
		
		for ( int ictr = 0; ictr < count; ictr++ )
		{
			if ( signature[ictr] == null )
			{
				return true;
			}
		}

		return false;
	}

	/**
	  *	Build an array of names of the argument types. These types are biased toward
	  *	Java objects. That is, if an argument is of SQLType, then we map it to the
	  *	corresponding Java synonym class (e.g., SQLINT is mapped to 'java.lang.Integer').
	  *
	  *
	  *	@return	array of type names
	  *
	  * @exception StandardException		Thrown on error
	  */
	protected	String[]	getObjectSignature( )
		throws StandardException
	{
		int		count = signature.length;
		String	parmTypeNames[] = new String[ count ];

		for ( int i = 0; i < count; i++ ) { parmTypeNames[i] = getObjectTypeName( signature[ i ] ); }

		return parmTypeNames;
	}

	/**
	 * Build an array of booleans denoting whether or not a given method
	 * parameter is a ?.
	 *
	 * @return array of booleans denoting wheter or not a given method
	 * parameter is a ?.
	 */
	protected boolean[] getIsParam()
	{
		if (methodParms == null)
		{
			return new boolean[0];
		}
		
		boolean[] isParam = new boolean[methodParms.length];

		for (int index = 0; index < methodParms.length; index++)
		{
			if (methodParms[index] instanceof SQLToJavaValueNode)
			{
				SQLToJavaValueNode stjvn = (SQLToJavaValueNode) methodParms[index];
				if (stjvn.value.isParameterNode())
				{
					isParam[index] = true;
				}
			}
		}

		return isParam;
	}

	private	String	getObjectTypeName( JSQLType jsqlType )
		throws StandardException
	{
		if ( jsqlType != null )
		{
			switch( jsqlType.getCategory() )
			{
			    case JSQLType.SQLTYPE: 

					TypeId	ctid = mapToTypeID( jsqlType );

					if ( ctid == null ) { return null; }
					else {
						// DB2 LUW does not support Java object types for SMALLINT, INTEGER, BIGINT, REAL, DOUBLE
						// and these are the only types that can map to a primitive or an object type according
						// to SQL part 13. So always map to the primitive type. We can not use the getPrimitiveSignature()
						// as it (incorrectly but historically always has) maps a DECIMAL to a double. 

						switch (ctid.getJDBCTypeId()) {
						case java.sql.Types.SMALLINT:
						case java.sql.Types.INTEGER:
						case java.sql.Types.BIGINT:
						case java.sql.Types.REAL:
						case java.sql.Types.DOUBLE:
							if (routineInfo != null) {
								TypeCompiler tc = getTypeCompiler(ctid);
								return tc.getCorrespondingPrimitiveTypeName();
							}
							// fall through
						default:
							return ctid.getCorrespondingJavaTypeName();
						}
					}

		        case JSQLType.JAVA_CLASS: return jsqlType.getJavaClassName();

		        case JSQLType.JAVA_PRIMITIVE: return JSQLType.primitiveNames[ jsqlType.getPrimitiveKind() ];

		        default:

					if (SanityManager.DEBUG)
					{ SanityManager.THROWASSERT( "Unknown JSQLType: " + jsqlType ); }

			}
		}

		return "";
	}

	String[]	getPrimitiveSignature( boolean castToPrimitiveAsNecessary )
		throws StandardException
	{
		int					count = signature.length;
		String[] 			primParmTypeNames = new String[ count ];
		JSQLType			jsqlType;

		for (int i = 0; i < count; i++)
		{
			jsqlType = signature[ i ];

			if ( jsqlType == null ) { primParmTypeNames[i] = ""; }
			else
			{
				switch( jsqlType.getCategory() )
			    {
			        case JSQLType.SQLTYPE:

						if ((procedurePrimitiveArrayType != null)
							&& (i < procedurePrimitiveArrayType.length)
							&& (procedurePrimitiveArrayType[i] != null)) {

							primParmTypeNames[i] = procedurePrimitiveArrayType[i];

						} else {


							TypeId	ctid = mapToTypeID( jsqlType );

							if (ctid.isNumericTypeId() || ctid.isBooleanTypeId())
							{
								TypeCompiler tc = getTypeCompiler(ctid);
								primParmTypeNames[i] = tc.getCorrespondingPrimitiveTypeName();
								if ( castToPrimitiveAsNecessary) { methodParms[i].castToPrimitive(true); }
							}
							else { primParmTypeNames[i] = ctid.getCorrespondingJavaTypeName(); }
						}

						break;

		            case JSQLType.JAVA_CLASS:

						primParmTypeNames[i] = jsqlType.getJavaClassName();
						break;

		            case JSQLType.JAVA_PRIMITIVE:

						primParmTypeNames[i] = JSQLType.primitiveNames[ jsqlType.getPrimitiveKind() ];
						if ( castToPrimitiveAsNecessary) { methodParms[i].castToPrimitive(true); }
						break;

		            default:

						if (SanityManager.DEBUG)
							{ SanityManager.THROWASSERT( "Unknown JSQLType: " + jsqlType ); }

				}	// end switch

			}		// end if

		}			// end for

		return primParmTypeNames;
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *							  (constant expressions)
	 *
	 * @return	The variant type for the underlying expression.
	 */
	protected int getOrderableVariantType() throws StandardException
	{
		// beetle 4880. We return the most variant type of the parameters. If no
		// params then query-invariant. This makes more sense, and we can evaluate
		// only once per query (good for performance) because method call could be
		// expensive.  And if we push down method qualifier to store, language
		// can pre-evaluate the method call.  This avoids letting store evaluate
		// the method while holding page latch, causing deadlock.

		return getVariantTypeOfParams();
	}

	private int getVariantTypeOfParams() throws StandardException
	{
		int variance = Qualifier.QUERY_INVARIANT;

		if (methodParms != null)
		{
			for (int parm = 0; parm < methodParms.length; parm++)
			{
				if (methodParms[parm] != null)
				{
					int paramVariantType =
						methodParms[parm].getOrderableVariantType();
					if (paramVariantType < variance)	//return the most variant type
						variance = paramVariantType;
				}
				else
				{
					variance = Qualifier.VARIANT;
				}
			}
		}

		return variance;
	}


	/////////////////////////////////////////////////////////////////////
	//
	//	ACCESSORS
	//
	/////////////////////////////////////////////////////////////////////
	/**
	 * Get the method parameters.
	 * 
	 * @return	The method parameters
	 */
	public JavaValueNode[]	getMethodParms()
	{
		return methodParms;
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
		Visitable		returnNode = v.visit(this);

		if (v.skipChildren(this))
		{
			return returnNode;
		}

		for (int parm = 0; 
			!v.stopTraversal() && parm < methodParms.length; 
			parm++)
		{
			if (methodParms[parm] != null)
			{
				methodParms[parm] = (JavaValueNode)methodParms[parm].accept(v);
			}
		}

		return returnNode;
	}
}
