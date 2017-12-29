/*

   Derby - Class org.apache.derby.impl.sql.compile.StaticMethodCallNode

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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ParameterMetaData;
import java.util.List;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.JSQLType;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.JBitSet;

/**
 * A StaticMethodCallNode represents a static method call from a Class
 * (as opposed to from an Object).

   For a procedure the call requires that the arguments be ? parameters.
   The parameter is *logically* passed into the method call a number of different ways.

   <P>
   For a application call like CALL MYPROC(?) the logically Java method call is
   (in psuedo Java/SQL code) (examples with CHAR(10) parameter)
   <BR>
   Fixed length IN parameters - com.acme.MyProcedureMethod(?)
   <BR>
   Variable length IN parameters - com.acme.MyProcedureMethod(CAST (? AS CHAR(10))
   <BR>
   Fixed length INOUT parameter -
		String[] holder = new String[] {?}; com.acme.MyProcedureMethod(holder); ? = holder[0]
   <BR>
   Variable length INOUT parameter -
		String[] holder = new String[] {CAST (? AS CHAR(10)}; com.acme.MyProcedureMethod(holder); ? = CAST (holder[0] AS CHAR(10))

   <BR>
   Fixed length OUT parameter -
		String[] holder = new String[1]; com.acme.MyProcedureMethod(holder); ? = holder[0]

   <BR>
   Variable length INOUT parameter -
		String[] holder = new String[1]; com.acme.MyProcedureMethod(holder); ? = CAST (holder[0] AS CHAR(10))


    <P>
	For static method calls there is no pre-definition of an IN or INOUT parameter, so a call to CallableStatement.registerOutParameter()
	makes the parameter an INOUT parameter, provided:
		- the parameter is passed directly to the method call (no casts or expressions).
		- the method's parameter type is a Java array type.

    Since this is a dynamic decision we compile in code to take both paths,
    based upon a boolean is INOUT which is derived from the
    ParameterValueSet. Code is logically (only single parameter String[] shown
    here). Note, no casts can exist here.

	boolean isINOUT = getParameterValueSet().getParameterMode(0) == PARAMETER_IN_OUT;
	if (isINOUT) {
		String[] holder = new String[] {?}; com.acme.MyProcedureMethod(holder); ? = holder[0]
	   
	} else {
		com.acme.MyProcedureMethod(?)
	}

 *
 */
class StaticMethodCallNode extends MethodCallNode
{
	private TableName procedureName;

	private LocalField[] outParamArrays;
	private int[]		 applicationParameterNumbers; 

	private boolean		isSystemCode;

    /**
     * This flag is true while bindExpression() is executing. It is used to
     * avoid infinite recursion when bindExpression() is reentered.
     */
    private boolean isInsideBind;

    /**
     * Generated boolean field to hold the indicator
     * for if any of the parameters to a
     * RETURNS NULL ON NULL INPUT function are NULL.
     * Only set if this node is calling such a function.
     * Set at generation time.
     */
	private LocalField	returnsNullOnNullState;

    /**
     * Authorization id of user owning schema in which routine is defined.
     */
    private String routineDefiner = null;

	AliasDescriptor	ad;

    private AggregateNode   resolvedAggregate;

    private boolean appearsInGroupBy = false;


	/**
     * Constructor for a NonStaticMethodCallNode
	 *
	 * @param methodName		The name of the method to call
     * @param javaClassName     The name of the java class that the static
     *                          method belongs to.
     * @param cm                The context manager
	 */
    StaticMethodCallNode(
            String methodName,
            String javaClassName,
            ContextManager cm) {
        super(methodName, cm);
        this.javaClassName = javaClassName;
    }

    /**
     * Constructor for a StaticMethodCallNode
     *
     * @param methodName        The name of the method to call
     * @param javaClassName     The name of the java class that the static
     *                          method belongs to.
     * @param cm                The context manager
     */
    StaticMethodCallNode(
            TableName methodName,
            String javaClassName,
            ContextManager cm) {
        super(methodName.getTableName(), cm);
        procedureName = methodName;
        this.javaClassName = javaClassName;
    }

    /**
     * Get the aggregate, if any, which this method call resolves to.
     */
    public  AggregateNode   getResolvedAggregate() { return resolvedAggregate; }

    /** Flag that this function invocation appears in a GROUP BY clause */
    public  void    setAppearsInGroupBy() { appearsInGroupBy = true; }
    
    @Override
    TableName getFullName()
	{
		return  procedureName;
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList		The FROM list for the query this
     *                      expression is in, for binding columns.
     * @param subqueryList  The subquery list being built as we find SubqueryNodes
     * @param aggregates    The aggregate list being built as we find AggregateNodes
	 *
	 * @return	this or an AggregateNode
	 *
	 * @exception StandardException		Thrown on error
	 */
    JavaValueNode bindExpression(
        FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
			throws StandardException
	{
		// for a function we can get called recursively
        if (isInsideBind) {
            return this;
        }

        isInsideBind = true;
        try {
            return bindExpressionMinion(fromList, subqueryList, aggregates);
        } finally {
            isInsideBind = false;
        }
    }

    private JavaValueNode bindExpressionMinion(
            FromList fromList,
            SubqueryList subqueryList,
            List<AggregateNode> aggregates)
        throws StandardException
    {
        bindParameters(fromList, subqueryList, aggregates);

		
		/* If javaClassName is null then we assume that the current methodName
		 * is an alias and we must go to sysmethods to
		 * get the real method and java class names for this alias.
		 */
		if (javaClassName == null)
		{
			CompilerContext cc = getCompilerContext();

			// look for a routine

			String schemaName = procedureName.getSchemaName();
								
			boolean noSchema = schemaName == null;

			SchemaDescriptor sd = getSchemaDescriptor(schemaName, schemaName != null);

            // The field methodName is used by resolveRoutine and
            // is set to the name of the routine (procedureName.getTableName()).
            resolveRoutine( fromList, subqueryList, aggregates, sd, noSchema );

            if ( (ad != null) && (ad.getAliasType() == AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR) )
            {
                resolvedAggregate = new AggregateNode(
                     ((SQLToJavaValueNode) methodParms[ 0 ]).getSQLValueNode(),
                     new UserAggregateDefinition( ad ), 
                     procedureName,
                     false,
                     ad.getJavaClassName(),
                     getContextManager()
                     );

                // Propagate tags used to flag nodes which need privilege checks. See DERBY-6429.
                resolvedAggregate.copyTagsFrom( this );

                // The parser may have noticed that this aggregate is invoked in a
                // GROUP BY clause. That is not allowed.
                if ( appearsInGroupBy )
                {
                    throw StandardException.newException(SQLState.LANG_AGGREGATE_IN_GROUPBY_LIST);
                }
                
                return this;
            }

            SchemaDescriptor savedSd = sd;

            if (ad == null && noSchema && !forCallStatement)
            {
                // Resolve to a built-in SYSFUN function but only
                // if this is a function call and the call
                // was not qualified. E.g. COS(angle). The
                // SYSFUN functions are not in SYSALIASES but
                // an in-memory table, set up in DataDictioanryImpl.
                sd = getSchemaDescriptor("SYSFUN", true);

                resolveRoutine(fromList, subqueryList, aggregates, sd, noSchema);
            }

            if (ad == null) {
                // DERBY-2927. Check if a procedure is being used as a
                // function, or vice versa.
                sd = savedSd;

                if (!forCallStatement) {
                    // Procedure as function. We have JDBC escape syntax which
                    // may entice users to try that:
                    //      "{? = CALL <proc>}"
                    //
                    // but we don't currently support it (it's not std SQL
                    // either). By resolving it as a procedure we can give a
                    // better error message.
                    //
                    // Note that with the above escape syntax one *can* CALL a
                    // function, though:
                    //      "{? = CALL <func>}"
                    //
                    // but such cases have already been resolved above.

                    forCallStatement = true; // temporarily: resolve
                                             // as procedure
                    resolveRoutine(fromList, subqueryList, aggregates, sd, noSchema);
                    forCallStatement = false; // restore it

                    if (ad != null) {
                        throw StandardException.newException
                            (SQLState.LANG_PROC_USED_AS_FUNCTION,
                             procedureName);
                    }
                } else {
                    // Maybe a function is being CALLed ?
                    forCallStatement = false; // temporarily: resolve
                                              // as function
                    resolveRoutine(fromList, subqueryList, aggregates, sd, noSchema);
                    forCallStatement = true; // restore it

                    if (ad != null) {
                        throw StandardException.newException
                            (SQLState.LANG_FUNCTION_USED_AS_PROC,
                             procedureName);
                    }
                }
            }

			/* Throw exception if no routine found */
			if (ad == null)
			{
				throw StandardException.newException(
                        SQLState.LANG_NO_SUCH_METHOD_ALIAS, procedureName);
			}

            if (noSchema) {
                // If no schema was specified, register where we found the
                // routine.
                procedureName.setSchemaName(sd.getSchemaName());
            }

            if ( !routineInfo.isDeterministic() )
            {
                checkReliability( getMethodName(), CompilerContext.NON_DETERMINISTIC_ILLEGAL );
            }
            if ( permitsSQL( routineInfo ) )
            {
                checkReliability( getMethodName(), CompilerContext.SQL_IN_ROUTINES_ILLEGAL );
            }
			


			/* Query is dependent on the AliasDescriptor */
			cc.createDependency(ad);


			methodName = ad.getAliasInfo().getMethodName();
			javaClassName = ad.getJavaClassName();
            
            // DERBY-2330 Do not allow a routine to resolve to
            // a Java method that is part of the Derby runtime code base.
            // This is a security measure to stop user-defined routines
            // bypassing security by making calls directly to Derby's
            // internal methods. E.g. open a table's conglomerate
            // directly and read the file, bypassing any authorization.
            // This is a simpler mechanism than analyzing all of
            // Derby's public static methods and ensuring they have
            // no Security holes.
            //
            // A special exception is made for the optional tools methods.
            if (
                javaClassName.startsWith( "org.apache.derby." ) &&
                !javaClassName.startsWith( "org.apache.derby.impl.tools.optional." ) &&
                !javaClassName.startsWith( "org.apache.derby.optional.lucene." ) &&
                !javaClassName.startsWith( "org.apache.derby.optional.json." ) &&
                !javaClassName.startsWith( "org.apache.derby.optional.api." ) &&
                !javaClassName.startsWith( "org.apache.derby.optional.dump." ) &&
                !javaClassName.startsWith( "org.apache.derby.vti." )
                )
            {
                if (!sd.isSystemSchema())
                    throw StandardException.newException(
                        SQLState.LANG_TYPE_DOESNT_EXIST2, (Throwable) null,
                        javaClassName);
            }
		}

		verifyClassExist(javaClassName);

		/* Resolve the method call */
		resolveMethodCall( javaClassName, true );


		if (isPrivilegeCollectionRequired())
			getCompilerContext().addRequiredRoutinePriv(ad);

		// If this is a function call with a variable length
		// return type, then we need to push a CAST node.
		if (routineInfo != null)
		{
			if (methodParms != null) 
				optimizeDomainValueConversion();
			
			TypeDescriptor returnType = routineInfo.getReturnType();

            // create type dependency if return type is an ANSI UDT
            if ( returnType != null ) { createTypeDependency( DataTypeDescriptor.getType( returnType ) ); }

			if ( returnType != null && !returnType.isRowMultiSet() && !returnType.isUserDefinedType() )
			{
				TypeId returnTypeId = TypeId.getBuiltInTypeId(returnType.getJDBCTypeId());

				if (returnTypeId.variableLength()) {
					// Cast the return using a cast node, but have to go
					// into the SQL domain, and back to the Java domain.

					DataTypeDescriptor returnValueDtd = new DataTypeDescriptor(
								returnTypeId,
								returnType.getPrecision(),
								returnType.getScale(),
								returnType.isNullable(),
								returnType.getMaximumWidth()
							);
							

                    ValueNode returnValueToSQL =
                            new JavaToSQLValueNode(this, getContextManager());

                    ValueNode returnValueCastNode = new CastNode(
									returnValueToSQL, 
									returnValueDtd,
									getContextManager());
                    
                    // DERBY-2972  Match the collation of the RoutineAliasInfo
                    returnValueCastNode.setCollationInfo(
                            returnType.getCollationType(),
                            StringDataValue.COLLATION_DERIVATION_IMPLICIT);

                    JavaValueNode returnValueToJava = new SQLToJavaValueNode(
                            returnValueCastNode, getContextManager());
					returnValueToJava.setCollationType(returnType.getCollationType());
                    return returnValueToJava.bindExpression(fromList, subqueryList, aggregates);
				}

			}
		}

		return this;
	}

	/**
	 * Returns true if the routine permits SQL.
	 */
    private boolean permitsSQL( RoutineAliasInfo rai )
    {
        short       sqlAllowed = rai.getSQLAllowed();

        switch( sqlAllowed )
        {
        case RoutineAliasInfo.MODIFIES_SQL_DATA:
        case RoutineAliasInfo.READS_SQL_DATA:
        case RoutineAliasInfo.CONTAINS_SQL:
            return true;

        default:    return false;
        }
    }
    
	/**
	 * If this SQL function has parameters which are SQLToJavaValueNode over
	 * JavaToSQLValueNode and the java value node underneath is a SQL function
	 * defined with CALLED ON NULL INPUT, then we can get rid of the wrapper
	 * nodes over the java value node for such parameters. This is because
	 * SQL functions defined with CALLED ON NULL INPUT need access to only
	 * java domain values.
	 * This can't be done for parameters which are wrappers over SQL function
	 * defined with RETURN NULL ON NULL INPUT because such functions need
	 * access to both sql domain value and java domain value. - Derby479
     * This optimization is not available if the outer function is
	 * RETURN NULL ON NULL INPUT. That is because the SQLToJavaNode is
	 * responsible for compiling the byte code which skips the method call if
     * the parameter is null--if we remove the SQLToJavaNode, then we don't
     * compile that check and we get bug DERBY-1030.
	 */
	private void optimizeDomainValueConversion() throws StandardException {

        //
        // This optimization is not possible if we are compiling a call to
        // a NULL ON NULL INPUT method. See DERBY-1030 and the header
        // comment above.
        //
        if ( !routineInfo.calledOnNullInput() ) { return; }
        
		int		count = methodParms.length;
		for (int parm = 0; parm < count; parm++)
		{
            //
            // We also skip the optimization if the argument must be cast to a primitive. In this case we need
            // a runtime check to make sure that the argument is not null. See DERBY-4459.
            //
            if ( (methodParms != null) && methodParms[ parm ].mustCastToPrimitive() ) { continue; }
            
			if (methodParms[parm] instanceof SQLToJavaValueNode &&
				((SQLToJavaValueNode)methodParms[parm]).getSQLValueNode() instanceof
				JavaToSQLValueNode)
			{
				//If we are here, then it means that the parameter is
				//SQLToJavaValueNode on top of JavaToSQLValueNode
				JavaValueNode paramIsJavaValueNode =
					((JavaToSQLValueNode)((SQLToJavaValueNode)methodParms[parm]).getSQLValueNode()).getJavaValueNode();
				if (paramIsJavaValueNode instanceof StaticMethodCallNode)
				{
					//If we are here, then it means that the parameter has
					//a MethodCallNode underneath it.
					StaticMethodCallNode paramIsMethodCallNode = (StaticMethodCallNode)paramIsJavaValueNode;
					//If the MethodCallNode parameter is defined as
					//CALLED ON NULL INPUT, then we can remove the wrappers
					//for the param and just set the parameter to the
					//java value node.
					if (paramIsMethodCallNode.routineInfo != null &&
							paramIsMethodCallNode.routineInfo.calledOnNullInput())
						methodParms[parm] =
							((JavaToSQLValueNode)((SQLToJavaValueNode)methodParms[parm]).getSQLValueNode()).getJavaValueNode();
				}
			}
		}
	}

	/**
	 * Resolve a routine. Obtain a list of routines from the data dictionary
	 * of the correct type (functions or procedures) and name.
	 * Pick the best routine from the list. Currently only a single routine
	 * with a given type and name is allowed, thus if changes are made to
	 * support overloaded routines, careful code inspection and testing will
	 * be required.
	 */
    private void resolveRoutine(FromList fromList, SubqueryList subqueryList,
                                List<AggregateNode> aggregates, SchemaDescriptor sd,
                                boolean noSchema)
            throws StandardException {
		if (sd.getUUID() != null) {

        List<AliasDescriptor> list = getDataDictionary().getRoutineList(
            sd.getUUID().toString(),
            methodName,
            forCallStatement ?
                AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR :
                AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR);

		for (int i = list.size() - 1; i >= 0; i--) {

            AliasDescriptor proc = list.get(i);

			RoutineAliasInfo rai = (RoutineAliasInfo) proc.getAliasInfo();
			int parameterCount = rai.getParameterCount();
            boolean hasVarargs = rai.hasVarargs();

            if ( hasVarargs )
            {
                // a varargs method can be called with no values supplied
                // for the trailing varargs argument
                if ( methodParms.length < (parameterCount - 1) ) { continue; }
            }
			else if (parameterCount != methodParms.length)
            { continue; }

			// pre-form the method signature. If it is a dynamic result set procedure
			// then we need to add in the ResultSet array

			TypeDescriptor[] parameterTypes = rai.getParameterTypes();

			int sigParameterCount = parameterCount;
			if (rai.getMaxDynamicResultSets() > 0)
            { sigParameterCount++; }

			signature = new JSQLType[sigParameterCount];
			for (int p = 0; p < parameterCount; p++) {

				// find the declared type.

				TypeDescriptor td = parameterTypes[p];

				TypeId typeId = TypeId.getTypeId(td);

				TypeId parameterTypeId = typeId;


				// if it's an OUT or INOUT parameter we need an array.
				int parameterMode = rai.getParameterModes()[ getRoutineArgIdx( rai, p ) ];

                if (parameterMode != (ParameterMetaData.parameterModeIn)) {

					String arrayType;
					switch (typeId.getJDBCTypeId()) {
						case java.sql.Types.BOOLEAN:
						case java.sql.Types.SMALLINT:
						case java.sql.Types.INTEGER:
						case java.sql.Types.BIGINT:
						case java.sql.Types.REAL:
						case java.sql.Types.DOUBLE:
							arrayType = getTypeCompiler(typeId).getCorrespondingPrimitiveTypeName().concat("[]");
							break;
						default:
							arrayType = typeId.getCorrespondingJavaTypeName().concat("[]");
							break;
					}

					typeId = TypeId.getUserDefinedTypeId(arrayType);
				}

				// this is the type descriptor of the require method parameter
				DataTypeDescriptor methoddtd = new DataTypeDescriptor(
						typeId,
						td.getPrecision(),
						td.getScale(),
						td.isNullable(),
						td.getMaximumWidth()
					);

				signature[p] = new JSQLType(methoddtd);

				// this is the SQL type of the procedure parameter.
				DataTypeDescriptor paramdtd = new DataTypeDescriptor(
					parameterTypeId,
					td.getPrecision(),
					td.getScale(),
					td.isNullable(),
					td.getMaximumWidth()
				);

                //
                // Now coerce the actual method parameter to the declared type
                // of this routine arg.
                //

                // if this is the last argument of a varargs routine...
                if ( hasVarargs && (p == parameterCount-1) )
                {
                    //
                    // The invocation of a varargs routine may have more actual parameters
                    // than the number of declared routine arguments. All of the trailing
                    // parameters must be coercible to the type of the last declared argument.
                    // Furthermore, it may turn out that there isn't a parameter corresponding to the last
                    // declared argument of the varargs routine.
                    //
                    for ( int idx = p; idx < methodParms.length; idx++ )
                    {
                        coerceMethodParameter
                            (
                             fromList, subqueryList, aggregates,
                             rai,
                             methodParms.length,
                             paramdtd, parameterTypeId, parameterMode,
                             idx
                             );
                    }
                }
                else    // NOT the last argument of a varargs routine
                {
                    coerceMethodParameter
                        (
                         fromList, subqueryList, aggregates,
                         rai,
                         methodParms.length,
                         paramdtd, parameterTypeId, parameterMode,
                         p
                         );
                }
			}

			if (sigParameterCount != parameterCount) {

				DataTypeDescriptor dtd = new DataTypeDescriptor(
						TypeId.getUserDefinedTypeId("java.sql.ResultSet[]"),
						0,
						0,
						false,
						-1
					);

				signature[parameterCount] = new JSQLType(dtd);

			}

			this.routineInfo = rai;
			ad = proc;

			// If a procedure is in the system schema and defined as executing
			// SQL, note that we are in system code.
			if (
                sd.isSystemSchema() &&
                (routineInfo.getReturnType() == null) &&
                routineInfo.getSQLAllowed() != RoutineAliasInfo.NO_SQL
                )
            { isSystemCode = true; }

            routineDefiner = sd.getAuthorizationId();

			break;
		}
        }

        if ( (ad == null) && (methodParms.length == 1) )
        {
            ad = AggregateNode.resolveAggregate
                ( getDataDictionary(), sd, methodName, noSchema );
        }
	}

    /**
     * <p>
     * Coerce an actual method parameter to the declared type of the corresponding
     * routine argument.
     * </p>
     */
    private void    coerceMethodParameter
        (
         FromList fromList,
         SubqueryList subqueryList,
         List<AggregateNode> aggregates,
         RoutineAliasInfo rai,
         int    parameterCount, // number of declared routine args
         DataTypeDescriptor paramdtd,   // declared type of routine arg
         TypeId parameterTypeId,    // declared type id of routine arg
         int    parameterMode,
         int    p   // index of actual method parameter in array of parameters
         )
        throws StandardException
    {
        // check parameter is a ? node for INOUT and OUT parameters.

        ValueNode sqlParamNode = null;

        if (methodParms[p] instanceof SQLToJavaValueNode)
        {
            SQLToJavaValueNode sql2j = (SQLToJavaValueNode) methodParms[p];
            sqlParamNode = sql2j.getSQLValueNode();
        }

        boolean isParameterMarker = true;
        if ((sqlParamNode == null) || !sqlParamNode.requiresTypeFromContext())
        {
            if (parameterMode != (ParameterMetaData.parameterModeIn))
            {
                throw StandardException.newException
                    (
                     SQLState.LANG_DB2_PARAMETER_NEEDS_MARKER,
                     RoutineAliasInfo.parameterMode(parameterMode),
                     rai.getParameterNames()[p]
                     );
            }
            isParameterMarker = false;
        }
        else
        {
            if (applicationParameterNumbers == null)
            { applicationParameterNumbers = new int[parameterCount]; }
            if (sqlParamNode instanceof UnaryOperatorNode)
            {
                ParameterNode pn = ((UnaryOperatorNode)sqlParamNode).getParameterOperand();
                applicationParameterNumbers[p] = pn.getParameterNumber();
            } else
            { applicationParameterNumbers[p] = ((ParameterNode) sqlParamNode).getParameterNumber(); }
        }

        boolean needCast = false;
        if (!isParameterMarker)
        {
            // can only be an IN parameter.
            // check that the value can be assigned to the
            // type of the procedure parameter.
            if (sqlParamNode instanceof UntypedNullConstantNode)
            {
                sqlParamNode.setType(paramdtd);
            }
            else
            {
                DataTypeDescriptor dts;
                TypeId argumentTypeId;
                
                if (sqlParamNode != null)
                {
                    // a node from the SQL world
                    argumentTypeId = sqlParamNode.getTypeId();
                    dts = sqlParamNode.getTypeServices();
                }
                else
                {
                    // a node from the Java world
                    dts = DataTypeDescriptor.getSQLDataTypeDescriptor(methodParms[p].getJavaTypeName());
                    if (dts == null)
                    {
                        throw StandardException.newException
                            (
                             SQLState.LANG_NO_CORRESPONDING_S_Q_L_TYPE, 
                             methodParms[p].getJavaTypeName()
                             );
                    }

                    argumentTypeId = dts.getTypeId();
                }

                if (! getTypeCompiler(parameterTypeId).storable(argumentTypeId, getClassFactory()))
                {
                    throw StandardException.newException
                        (
                         SQLState.LANG_NOT_STORABLE, 
                         parameterTypeId.getSQLTypeName(),
                         argumentTypeId.getSQLTypeName()
                         );
                }

                // if it's not an exact length match then some cast will be needed.
                if (!paramdtd.isExactTypeAndLengthMatch(dts))   { needCast = true; }
            }
        }
        else
        {
            // any variable length type will need a cast from the
            // Java world (the ? parameter) to the SQL type. This
            // ensures values like CHAR(10) are passed into the procedure
            // correctly as 10 characters long.
            if (parameterTypeId.variableLength())
            {
                if (parameterMode != (ParameterMetaData.parameterModeOut))
                { needCast = true; }
            }
        }

        if (needCast)
        {
            // push a cast node to ensure the
            // correct type is passed to the method
            // this gets tacky because before we knew
            // it was a procedure call we ensured all the
            // parameter are JavaNodeTypes. Now we need to
            // push them back to the SQL domain, cast them
            // and then push them back to the Java domain.
            
            if (sqlParamNode == null)
            {
                sqlParamNode =
                    new JavaToSQLValueNode(methodParms[p], getContextManager());
            }

            ValueNode castNode = makeCast
                (
                 sqlParamNode,
                 paramdtd,
                 getContextManager()
                 );

            methodParms[p] =
                    new SQLToJavaValueNode(castNode, getContextManager());

            methodParms[p] = methodParms[p].bindExpression(
                    fromList, subqueryList, aggregates);
        }

        // only force the type for a ? so that the correct type shows up
        // in parameter meta data
        if (isParameterMarker)  { sqlParamNode.setType(paramdtd); }
    }

    /**
     * Wrap a parameter in a CAST node.
     */
    public static ValueNode makeCast (ValueNode parameterNode,
                                      DataTypeDescriptor targetType,
                                      ContextManager cm)
        throws StandardException
    {
        ValueNode castNode = new CastNode(parameterNode, targetType, cm);

        // Argument type has the same semantics as assignment:
        // Section 9.2 (Store assignment). There, General Rule 
        // 2.b.v.2 says that the database should raise an exception
        // if truncation occurs when stuffing a string value into a
        // VARCHAR, so make sure CAST doesn't issue warning only.
        ((CastNode)castNode).setAssignmentSemantics();

        return castNode;
    }

	/**
	 * Add code to set up the SQL session context for a stored
	 * procedure or function which needs a nested SQL session
	 * context (only needed for those which can contain SQL).
	 *
     * The generated code calls pushNestedSessionContext.
     * @see LanguageConnectionContext#pushNestedSessionContext
	 *
	 * @param acb activation class builder
	 * @param mb  method builder
	 */
    private void generatePushNestedSessionContext(
        ActivationClassBuilder acb,
        MethodBuilder mb,
        boolean hadDefinersRights,
        String definer) throws StandardException {

		// Generates the following Java code:
		// ((Activation)this).getLanguageConnectionContext().
        //       pushNestedSessionContext((Activation)this);

		acb.pushThisAsActivation(mb);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
					  "getLanguageConnectionContext",
					  ClassName.LanguageConnectionContext, 0);
		acb.pushThisAsActivation(mb);
        mb.push(hadDefinersRights);
        mb.push(definer);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                      "pushNestedSessionContext",
                      "void", 3);
	}


	/**
		Push extra code to generate the casts within the
		arrays for the parameters passed as arrays.
	*/
    @Override
    void generateOneParameter(ExpressionClassBuilder acb,
											MethodBuilder mb,
											int parameterNumber )
			throws StandardException
	{
		int parameterMode;

		SQLToJavaValueNode sql2j = null;
		if (methodParms[parameterNumber] instanceof SQLToJavaValueNode)
			sql2j = (SQLToJavaValueNode) methodParms[parameterNumber];
		
		if (routineInfo != null) {
			parameterMode = routineInfo.getParameterModes()[ getRoutineArgIdx( parameterNumber ) ];
		} else {
			// for a static method call the parameter always starts out as a in parameter, but
			// may be registered as an IN OUT parameter. For a static method argument to be
			// a dynmaically registered out parameter it must be a simple ? parameter

            parameterMode = (ParameterMetaData.parameterModeIn);

			if (sql2j != null) {
				if (sql2j.getSQLValueNode().requiresTypeFromContext()) {
	  				ParameterNode pn;
		  			if (sql2j.getSQLValueNode() instanceof UnaryOperatorNode) 
		  				pn = ((UnaryOperatorNode)sql2j.getSQLValueNode()).getParameterOperand();
		  			else
		  				pn = (ParameterNode) (sql2j.getSQLValueNode());

					// applicationParameterNumbers is only set up for a procedure.
					int applicationParameterNumber = pn.getParameterNumber();

					String parameterType = methodParameterTypes[ getRoutineArgIdx( parameterNumber ) ];

					if (parameterType.endsWith("[]")) {

						// constructor  - setting up correct parameter type info
						MethodBuilder constructor = acb.getConstructor();
						acb.pushThisAsActivation(constructor);
						constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
											"getParameterValueSet", ClassName.ParameterValueSet, 0);

						constructor.push(applicationParameterNumber);
                        constructor.push(ParameterMetaData.parameterModeUnknown);
						constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
											"setParameterMode", "void", 2);
						constructor.endStatement();
					}
				}
			} 
		}

		switch (parameterMode) {
        case (ParameterMetaData.parameterModeIn):
        case (ParameterMetaData.parameterModeInOut):
        case (ParameterMetaData.parameterModeUnknown):
			if (sql2j != null)
				sql2j.returnsNullOnNullState = returnsNullOnNullState;
			super.generateOneParameter(acb, mb, parameterNumber);
			break;

        case (ParameterMetaData.parameterModeOut):
			// For an OUT parameter we require nothing to be pushed into the
			// method call from the parameter node.
			break;
		}

		switch (parameterMode) {
        case (ParameterMetaData.parameterModeIn):
        case (ParameterMetaData.parameterModeUnknown):
			break;

        case (ParameterMetaData.parameterModeInOut):
        case (ParameterMetaData.parameterModeOut):
		{
			// Create the array used to pass into the method. We create a
			// new array for each call as there is a small chance the
			// application could retain a reference to it and corrupt
			// future calls with the same CallableStatement object.

			String methodParameterType = methodParameterTypes[ getRoutineArgIdx( parameterNumber ) ];
			String arrayType = methodParameterType.substring(0, methodParameterType.length() - 2);

            // if a varargs arg, then strip off the extra array dimension added by varargs
            if ( isVararg( parameterNumber ) )
            {
                methodParameterType = stripOneArrayLevel( methodParameterType );
                arrayType = stripOneArrayLevel( arrayType );
            }
            
			LocalField lf = acb.newFieldDeclaration(Modifier.PRIVATE, methodParameterType);

			if (outParamArrays == null)
            { outParamArrays = new LocalField[methodParms.length]; }

			outParamArrays[parameterNumber] = lf;

			mb.pushNewArray(arrayType, 1);
			mb.putField(lf);

			// set the IN part of the parameter into the INOUT parameter.
            if (parameterMode != (ParameterMetaData.parameterModeOut)) {
				mb.swap();
				mb.setArrayElement(0);
				mb.getField(lf);
			}
			break;
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
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
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

		pushable = pushable && super.categorize(referencedTabs, simplePredsOnly);

		return pushable;
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
			return "javaClassName: " +
				(javaClassName != null ? javaClassName : "null") + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Do code generation for this method call
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{
		if (routineInfo != null) {

			if (!routineInfo.calledOnNullInput() && routineInfo.getParameterCount() != 0)
				returnsNullOnNullState = acb.newFieldDeclaration(Modifier.PRIVATE, "boolean");

		}

		// reset the parameters are null indicator.
		if (returnsNullOnNullState != null) {
			mb.push(false);
			mb.setField(returnsNullOnNullState);

			// for the call to the generated method below.
			mb.pushThis();
		}

		int nargs = generateParameters(acb, mb);

		LocalField functionEntrySQLAllowed = null;

		if (routineInfo != null) {

			short sqlAllowed = routineInfo.getSQLAllowed();

			// Before we set up our authorization level, add a check to see if this
			// method can be called. If the routine is NO SQL or CONTAINS SQL 
			// then there is no need for a check. As follows:
			//
			// Current Level = NO_SQL - CALL will be rejected when getting CALL result set
			// Current Level = anything else - calls to procedures defined as NO_SQL and CONTAINS SQL both allowed.


			if (sqlAllowed != RoutineAliasInfo.NO_SQL)
			{
				
				int sqlOperation;
				
				if (sqlAllowed == RoutineAliasInfo.READS_SQL_DATA)
					sqlOperation = Authorizer.SQL_SELECT_OP;
				else if (sqlAllowed == RoutineAliasInfo.MODIFIES_SQL_DATA)
					sqlOperation = Authorizer.SQL_WRITE_OP;
				else
					sqlOperation = Authorizer.SQL_ARBITARY_OP;
				
				generateAuthorizeCheck((ActivationClassBuilder) acb, mb, sqlOperation);
			}

			int statmentContextReferences = isSystemCode ? 2 : 1;
			
			boolean isFunction = routineInfo.getReturnType() != null;

			if (isFunction)
				statmentContextReferences++;


			if (statmentContextReferences != 0) {
				acb.pushThisAsActivation(mb);
				mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getLanguageConnectionContext", ClassName.LanguageConnectionContext, 0);
				mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getStatementContext", "org.apache.derby.iapi.sql.conn.StatementContext", 0);

				for (int scc = 1; scc < statmentContextReferences; scc++)
					mb.dup();
			}

			/**
				Set the statement context to reflect we are running
				System procedures, so that we can execute non-standard SQL.
			*/
			if (isSystemCode) {
				mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"setSystemCode", "void", 0);
			}

			// If no SQL, there is no need to setup a nested session
			// context.
			if (sqlAllowed != RoutineAliasInfo.NO_SQL) {
                generatePushNestedSessionContext(
                    (ActivationClassBuilder) acb,
                    mb,
                    routineInfo.hasDefinersRights(),
                    routineDefiner);
			}

			// for a function we need to fetch the current SQL control
			// so that we can reset it once the function is complete.
			// 
			if (isFunction)
			{
				functionEntrySQLAllowed = acb.newFieldDeclaration(Modifier.PRIVATE, "short");
				mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getSQLAllowed", "short", 0);
				mb.setField(functionEntrySQLAllowed);

			}
			
			
			// Set up the statement context to reflect the
			// restricted SQL execution allowed by this routine.

			mb.push(sqlAllowed);
			mb.push(false);
			mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
								"setSQLAllowed", "void", 2);

		}

		// add in the ResultSet arrays. note that varargs and dynamic ResultSets
        // both make claims on the trailing arguments of the method invocation.
        // a routine may make use of both varargs and dynamic ResultSets.
		if ( routineInfo != null && !hasVarargs() )
        {
			int compiledResultSets = methodParameterTypes.length - methodParms.length;

			if (compiledResultSets != 0) {

				// Add a method that indicates the maxium number of dynamic result sets.
				int maxDynamicResults = routineInfo.getMaxDynamicResultSets();
				if (maxDynamicResults > 0) {
					MethodBuilder gdr = acb.getClassBuilder().newMethodBuilder(Modifier.PUBLIC, "int", "getMaxDynamicResults");
					gdr.push(maxDynamicResults);
					gdr.methodReturn();
					gdr.complete();
				}

				// add a method to return all the dynamic result sets (unordered)
				MethodBuilder gdr = acb.getClassBuilder().newMethodBuilder(Modifier.PUBLIC, "java.sql.ResultSet[][]", "getDynamicResults");

				MethodBuilder cons = acb.getConstructor();
				// if (procDef.getParameterStyle() == RoutineAliasInfo.PS_JAVA)
				{
					// PARAMETER STYLE JAVA

					LocalField procedureResultSetsHolder = acb.newFieldDeclaration(Modifier.PRIVATE, "java.sql.ResultSet[][]");

					// getDynamicResults body
					gdr.getField(procedureResultSetsHolder);

					// create the holder of all the ResultSet arrays, new java.sql.ResultSet[][compiledResultSets]
					cons.pushNewArray("java.sql.ResultSet[]", compiledResultSets);
					cons.setField(procedureResultSetsHolder);


					// arguments for the dynamic result sets
					for (int i = 0; i < compiledResultSets; i++) {

						mb.pushNewArray("java.sql.ResultSet", 1);
						mb.dup();

						mb.getField(procedureResultSetsHolder);
						mb.swap();

						mb.setArrayElement(i);
					}
				} 

				// complete the method that returns the ResultSet[][] to the 
				gdr.methodReturn();
				gdr.complete();

				nargs += compiledResultSets;
			}

		}

		String javaReturnType = getJavaTypeName();

		MethodBuilder mbnc = null;
		MethodBuilder mbcm = mb;


		// If any of the parameters are null then
		// do not call the method, just return null.
		if (returnsNullOnNullState != null)
		{
			mbnc = acb.newGeneratedFun(javaReturnType, Modifier.PRIVATE, methodParameterTypes);

			// add the throws clause for the public static method we are going to call.
			Class[] throwsSet = ((java.lang.reflect.Method) method).getExceptionTypes();
			for (int te = 0; te < throwsSet.length; te++)
			{
				mbnc.addThrownException(throwsSet[te].getName());
			}

			mbnc.getField(returnsNullOnNullState);
			mbnc.conditionalIf();

			// set up for a null!!
			// for objects is easy.
			mbnc.pushNull(javaReturnType);

			mbnc.startElseCode();	

			if (!actualMethodReturnType.equals(javaReturnType))
				mbnc.pushNewStart(javaReturnType);

			// fetch all the arguments
			for (int pa = 0; pa < nargs; pa++)
			{
				mbnc.getParameter(pa);
			}

			mbcm = mbnc;
		}

		mbcm.callMethod(VMOpcode.INVOKESTATIC, method.getDeclaringClass().getName(), methodName,
					actualMethodReturnType, nargs);


		if (returnsNullOnNullState != null)
		{
			// DERBY-3360. In the case of function returning
			// a SMALLINT if we specify RETURN NULL ON NULL INPUT
			// the javaReturnType will be java.lang.Integer. In
			// order to initialize the integer properly, we need
			// to upcast the short.  This is a special case for
			// SMALLINT functions only as other types are 
			// compatible with their function return types.
			if (!actualMethodReturnType.equals(javaReturnType)) {
				if (actualMethodReturnType.equals("short") &&
						javaReturnType.equals("java.lang.Integer"))
					mbnc.upCast("int");
			
				mbnc.pushNewComplete(1);
			}
			mbnc.completeConditional();

			mbnc.methodReturn();
			mbnc.complete();

			// now call the wrapper method
			mb.callMethod(VMOpcode.INVOKEVIRTUAL, acb.getClassBuilder().getFullName(), mbnc.getName(),
					javaReturnType, nargs);
			mbnc = null;
		}


		if (routineInfo != null) {

			// reset the SQL allowed setting that we set upon
			// entry to the method.
			if (functionEntrySQLAllowed != null) {
				acb.pushThisAsActivation(mb);
				mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getLanguageConnectionContext", ClassName.LanguageConnectionContext, 0);
				mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getStatementContext", "org.apache.derby.iapi.sql.conn.StatementContext", 0);
				mb.getField(functionEntrySQLAllowed);
				mb.push(true); // override as we are ending the control set by this function all.
				mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"setSQLAllowed", "void", 2);

			}

			if (outParamArrays != null) {

				MethodBuilder constructor = acb.getConstructor();

				// constructor  - setting up correct parameter type info
				acb.pushThisAsActivation(constructor);
				constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getParameterValueSet", ClassName.ParameterValueSet, 0);

				// execute  - passing out parameters back.
				acb.pushThisAsActivation(mb);
				mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getParameterValueSet", ClassName.ParameterValueSet, 0);

				int[] parameterModes = routineInfo.getParameterModes();
				for (int i = 0; i < outParamArrays.length; i++) {

					int parameterMode = parameterModes[ getRoutineArgIdx( i ) ];
                    
                    if (parameterMode != (ParameterMetaData.parameterModeIn)) {

						// must be a parameter if it is INOUT or OUT.
						ValueNode sqlParamNode = ((SQLToJavaValueNode) methodParms[i]).getSQLValueNode();


						int applicationParameterNumber = applicationParameterNumbers[i];

						// Set the correct parameter nodes in the ParameterValueSet at constructor time.
						constructor.dup();
						constructor.push(applicationParameterNumber);
						constructor.push(parameterMode);
						constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
										"setParameterMode", "void", 2);

						// Pass the value of the outparameters back to the calling code
						LocalField lf = outParamArrays[i];

						mb.dup(); 
						mb.push(applicationParameterNumber);
						mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getParameter", ClassName.DataValueDescriptor, 1);

						// see if we need to set the desired length/scale/precision of the type
						DataTypeDescriptor paramdtd = sqlParamNode.getTypeServices();

						boolean isNumericType = paramdtd.getTypeId().isNumericTypeId();
						boolean isAnsiUDT = paramdtd.getTypeId().getBaseTypeId().isAnsiUDT();

						// is the underlying type for the OUT/INOUT parameter primitive.
                        // if this is a varargs arg then we have to strip off another array level
                        Class<?> cellType = ((Method) method).
                                getParameterTypes()[ getRoutineArgIdx( i ) ].
                                getComponentType();

                        if ( isVararg( i ) ) {
                            cellType = cellType.getComponentType();
                        }
						boolean isPrimitive = cellType.isPrimitive();

						if (isNumericType) {
							// need to up-cast as the setValue(Number) method only exists on NumberDataValue

							if (!isPrimitive)
								mb.cast(ClassName.NumberDataValue);
						}
						else if (paramdtd.getTypeId().isBooleanTypeId())
						{
							// need to cast as the setValue(Boolean) method only exists on BooleanDataValue
							if (!isPrimitive)
								mb.cast(ClassName.BooleanDataValue);
						}

						if (paramdtd.getTypeId().variableLength()) {
							// need another DVD reference for the set width below.
							mb.dup();
						}


						mb.getField(lf); // pvs, dvd, array
						mb.getArrayElement(0); // pvs, dvd, value

						// The value needs to be set thorugh the setValue(Number) method.
						if (isNumericType && !isPrimitive)
						{
							mb.upCast("java.lang.Number");
						}

						// The value needs to be set thorugh the setValue(Object) method.
						if (isAnsiUDT)
						{
							mb.upCast("java.lang.Object");
						}

						mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "setValue", "void", 1);

						if (paramdtd.getTypeId().variableLength()) {
							mb.push(isNumericType ? paramdtd.getPrecision() : paramdtd.getMaximumWidth());
							mb.push(paramdtd.getScale());
							mb.push(isNumericType);
							mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.VariableSizeDataValue, "setWidth", "void", 3);
							// mb.endStatement();
						}
					}
				}
				constructor.endStatement();
				mb.endStatement();
			}

		}
	}

	/**
	 * Set default privilege of EXECUTE for this node. 
	 */
	int getPrivType()
	{
		return Authorizer.EXECUTE_PRIV;
	}

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (procedureName != null) {
            procedureName = (TableName) procedureName.accept(v);
        }
    }
}
