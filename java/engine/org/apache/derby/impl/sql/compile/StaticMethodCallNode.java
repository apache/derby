/*

   Derby - Class org.apache.derby.impl.sql.compile.StaticMethodCallNode

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

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.types.JSQLType;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.iapi.services.compiler.LocalField;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.iapi.sql.conn.Authorizer;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.catalog.UUID;

import java.util.Vector;
import java.lang.reflect.Modifier;

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

    Since this is a dynmaic decision we compile in code to take both paths, based upon a boolean isINOUT which is dervied from the
	ParameterValueSet. Code is logically (only single parameter String[] shown here). Note, no casts can exist here.

	boolean isINOUT = getParameterValueSet().getParameterMode(0) == PARAMETER_IN_OUT;
	if (isINOUT) {
		String[] holder = new String[] {?}; com.acme.MyProcedureMethod(holder); ? = holder[0]
	   
	} else {
		com.acme.MyProcedureMethod(?)
	}

 *
 * @author Jerry Brenner
 */
public class StaticMethodCallNode extends MethodCallNode
{
	private TableName procedureName;

	private LocalField[] outParamArrays;
	private int[]		 applicationParameterNumbers; 

	private boolean		isSystemCode;
	private boolean		alreadyBound;

	private LocalField	returnsNullOnNullState;


	AliasDescriptor	ad;


	/**
	 * Intializer for a NonStaticMethodCallNode
	 *
	 * @param methodName		The name of the method to call
	 * @param javaClassName		The name of the java class that the static method belongs to.
	 */
	public void init(Object methodName, Object javaClassName)
	{
		if (methodName instanceof String)
			init(methodName);
		else {
			procedureName = (TableName) methodName;
			init(procedureName.getTableName());
		}

		this.javaClassName = (String) javaClassName;
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
	 * @return	this or an AggregateNode
	 *
	 * @exception StandardException		Thrown on error
	 */

	public JavaValueNode bindExpression(
		FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector) 
			throws StandardException
	{
		// for a function we can get called recursively
		if (alreadyBound)
			return this;


		bindParameters(fromList, subqueryList, aggregateVector);

		
		/* If javaClassName is null then we assume that the current methodName
		 * is an alias and we must go to sysmethods to
		 * get the real method and java class names for this alias.
		 */
		if (javaClassName == null)
		{
			CompilerContext cc = getCompilerContext();

			// look for a routine
			if (ad == null) {

				String schemaName = procedureName != null ?
									procedureName.getSchemaName() : null;

				SchemaDescriptor sd = getSchemaDescriptor(schemaName, schemaName != null);


				if (sd.getUUID() != null) {

				java.util.List list = getDataDictionary().getRoutineList(
					sd.getUUID().toString(), methodName,
					forCallStatement ? AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR : AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR
					);

				for (int i = list.size() - 1; i >= 0; i--) {

					AliasDescriptor proc = (AliasDescriptor) list.get(i);

					RoutineAliasInfo routineInfo = (RoutineAliasInfo) proc.getAliasInfo();
					int parameterCount = routineInfo.getParameterCount();
					if (parameterCount != methodParms.length)
						continue;

					// pre-form the method signature. If it is a dynamic result set procedure
					// then we need to add in the ResultSet array

					TypeDescriptor[] parameterTypes = routineInfo.getParameterTypes();

					int sigParameterCount = parameterCount;
					if (routineInfo.getMaxDynamicResultSets() > 0)
						sigParameterCount++;

					signature = new JSQLType[sigParameterCount];
					for (int p = 0; p < parameterCount; p++) {

						// find the declared type.

						TypeDescriptor td = parameterTypes[p];

						TypeId typeId = TypeId.getBuiltInTypeId(td.getJDBCTypeId());

						TypeId parameterTypeId = typeId;


						// if it's an OUT or INOUT parameter we need an array.
						int parameterMode = routineInfo.getParameterModes()[p];

						if (parameterMode != JDBC30Translation.PARAMETER_MODE_IN) {

							String arrayType;
							switch (typeId.getJDBCTypeId()) {
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

							typeId = TypeId.getUserDefinedTypeId(arrayType, false);
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

						// check parameter is a ? node for INOUT and OUT parameters.

						ValueNode sqlParamNode = null;

						if (methodParms[p] instanceof SQLToJavaValueNode) {
							SQLToJavaValueNode sql2j = (SQLToJavaValueNode) methodParms[p];
							sqlParamNode = sql2j.getSQLValueNode();
						}
						else
						{
						}

						boolean isParameterMarker = true;
						if ((sqlParamNode == null) || !sqlParamNode.isParameterNode())
						{
							if (parameterMode != JDBC30Translation.PARAMETER_MODE_IN) {
							 
								throw StandardException.newException(SQLState.LANG_DB2_PARAMETER_NEEDS_MARKER,
									RoutineAliasInfo.parameterMode(parameterMode),
									routineInfo.getParameterNames()[p]);
							}
							isParameterMarker = false;
						}
						else
						{
							if (applicationParameterNumbers == null)
								applicationParameterNumbers = new int[parameterCount];
							applicationParameterNumbers[p] = ((ParameterNode) sqlParamNode).getParameterNumber();
						}

						// this is the SQL type of the procedure parameter.
						DataTypeDescriptor paramdtd = new DataTypeDescriptor(
							parameterTypeId,
							td.getPrecision(),
							td.getScale(),
							td.isNullable(),
							td.getMaximumWidth()
						);

						boolean needCast = false;
						if (!isParameterMarker)
						{

							// can only be an IN parameter.
							// check that the value can be assigned to the
							// type of the procedure parameter.
							if (sqlParamNode instanceof UntypedNullConstantNode)
							{
								sqlParamNode.setDescriptor(paramdtd);
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
										throw StandardException.newException(SQLState.LANG_NO_CORRESPONDING_S_Q_L_TYPE, 
											methodParms[p].getJavaTypeName());
									}

									argumentTypeId = dts.getTypeId();
								}

								if (! getTypeCompiler(parameterTypeId).storable(argumentTypeId, getClassFactory()))
										throw StandardException.newException(SQLState.LANG_NOT_STORABLE, 
											parameterTypeId.getSQLTypeName(),
											argumentTypeId.getSQLTypeName() );

								// if it's not an exact length match then some cast will be needed.
								if (!paramdtd.isExactTypeAndLengthMatch(dts))
									needCast = true;
							}
						}
						else
						{
							// any variable length type will need a cast from the
							// Java world (the ? parameter) to the SQL type. This
							// ensures values like CHAR(10) are passed into the procedure
							// correctly as 10 characters long.
							if (parameterTypeId.variableLength()) {

								if (parameterMode != JDBC30Translation.PARAMETER_MODE_OUT)
									needCast = true;
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

							if (sqlParamNode == null) {

								sqlParamNode = (ValueNode) getNodeFactory().getNode(
									C_NodeTypes.JAVA_TO_SQL_VALUE_NODE,
									methodParms[p], 
									getContextManager());
							}

							ValueNode castNode = (ValueNode) getNodeFactory().getNode(
								C_NodeTypes.CAST_NODE,
								sqlParamNode, 
								paramdtd,
								getContextManager());


							methodParms[p] = (JavaValueNode) getNodeFactory().getNode(
									C_NodeTypes.SQL_TO_JAVA_VALUE_NODE,
									castNode, 
									getContextManager());

							methodParms[p] = methodParms[p].bindExpression(fromList, subqueryList, aggregateVector);
						}

						// only force the type for a ? so that the correct type shows up
						// in parameter meta data
						if (isParameterMarker)
							sqlParamNode.setDescriptor(paramdtd);
					}

					if (sigParameterCount != parameterCount) {

						TypeId typeId = TypeId.getUserDefinedTypeId("java.sql.ResultSet[]", false);

						DataTypeDescriptor dtd = new DataTypeDescriptor(
								typeId,
								0,
								0,
								false,
								-1
							);

						signature[parameterCount] = new JSQLType(dtd);

					}

					this.routineInfo = routineInfo;
					ad = proc;

					// If a procedure is in the system schema and defined as executing
					// SQL do we set we are in system code.
					if (sd.isSystemSchema() && (routineInfo.getReturnType() == null) && routineInfo.getSQLAllowed() != RoutineAliasInfo.NO_SQL)
						isSystemCode = true;

					break;
				}
			}
	
			}

			/* Throw exception if no alias found */
			if (ad == null)
			{
				Object errName;
				if (procedureName == null)
					errName = methodName;
				else
					errName = procedureName;

				throw StandardException.newException(SQLState.LANG_NO_SUCH_METHOD_ALIAS, errName);
			}
	


			/* Query is dependent on the AliasDescriptor */
			cc.createDependency(ad);


			methodName = ad.getAliasInfo().getMethodName();
			javaClassName = ad.getJavaClassName();
		}


		javaClassName = verifyClassExist(javaClassName, true);

		/* Resolve the method call */
		resolveMethodCall(javaClassName, true);


		alreadyBound = true;

		// If this is a function call with a variable length
		// return type, then we need to push a CAST node.
		if (routineInfo != null)
		{
			TypeDescriptor returnType = routineInfo.getReturnType();
			if (returnType != null)
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


					ValueNode returnValueToSQL = (ValueNode) getNodeFactory().getNode(
								C_NodeTypes.JAVA_TO_SQL_VALUE_NODE,
								this, 
								getContextManager());

					ValueNode returnValueCastNode = (ValueNode) getNodeFactory().getNode(
									C_NodeTypes.CAST_NODE,
									returnValueToSQL, 
									returnValueDtd,
									getContextManager());


					JavaValueNode returnValueToJava = (JavaValueNode) getNodeFactory().getNode(
										C_NodeTypes.SQL_TO_JAVA_VALUE_NODE,
										returnValueCastNode, 
										getContextManager());

					return returnValueToJava.bindExpression(fromList, subqueryList, aggregateVector);
				}

			}
		}

		return this;
	}

	/**
		Push extra code to generate the casts within the
		arrays for the parameters passed as arrays.
	*/
	public	void generateOneParameter(ExpressionClassBuilder acb,
											MethodBuilder mb,
											int parameterNumber )
			throws StandardException
	{
		int parameterMode;


		SQLToJavaValueNode sql2j = null;
		if (methodParms[parameterNumber] instanceof SQLToJavaValueNode)
			sql2j = (SQLToJavaValueNode) methodParms[parameterNumber];
		
		if (routineInfo != null) {
			parameterMode = routineInfo.getParameterModes()[parameterNumber];
		} else {
			// for a static method call the parameter always starts out as a in parameter, but
			// may be registered as an IN OUT parameter. For a static method argument to be
			// a dynmaically registered out parameter it must be a simple ? parameter

			parameterMode = JDBC30Translation.PARAMETER_MODE_IN;

			if (sql2j != null) {
				if (sql2j.getSQLValueNode().isParameterNode()) {

					// applicationParameterNumbers is only set up for a procedure.
					int applicationParameterNumber = ((ParameterNode) (sql2j.getSQLValueNode())).getParameterNumber();

					String parameterType = methodParameterTypes[parameterNumber];

					if (parameterType.endsWith("[]")) {

						// constructor  - setting up correct paramter type info
						MethodBuilder constructor = acb.getConstructor();
						acb.pushThisAsActivation(constructor);
						constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
											"getParameterValueSet", ClassName.ParameterValueSet, 0);

						constructor.push(applicationParameterNumber);
						constructor.push(JDBC30Translation.PARAMETER_MODE_UNKNOWN);
						constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
											"setParameterMode", "void", 2);
						constructor.endStatement();
					}
				}
			} 
		}

		switch (parameterMode) {
		case JDBC30Translation.PARAMETER_MODE_IN:
		case JDBC30Translation.PARAMETER_MODE_IN_OUT:
		case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
			if (sql2j != null)
				sql2j.returnsNullOnNullState = returnsNullOnNullState;
			super.generateOneParameter(acb, mb, parameterNumber);
			break;

		case JDBC30Translation.PARAMETER_MODE_OUT:
			// For an OUT parameter we require nothing to be pushed into the
			// method call from the parameter node.
			break;
		}

		switch (parameterMode) {
		case JDBC30Translation.PARAMETER_MODE_IN:
		case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
			break;

		case JDBC30Translation.PARAMETER_MODE_IN_OUT:
		case JDBC30Translation.PARAMETER_MODE_OUT:
		{
			// Create the array used to pass into the method. We create a
			// new array for each call as there is a small chance the
			// application could retain a reference to it and corrupt
			// future calls with the same CallableStatement object.

			String methodParameterType = methodParameterTypes[parameterNumber];
			String arrayType = methodParameterType.substring(0, methodParameterType.length() - 2);
			LocalField lf = acb.newFieldDeclaration(Modifier.PRIVATE, methodParameterType);

			if (outParamArrays == null)
				outParamArrays = new LocalField[methodParms.length];

			outParamArrays[parameterNumber] = lf;

			mb.pushNewArray(arrayType, 1);
			mb.putField(lf);

			// set the IN part of the parameter into the INOUT parameter.
			if (parameterMode != JDBC30Translation.PARAMETER_MODE_OUT) {
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

		pushable = pushable && super.categorize(referencedTabs, simplePredsOnly);

		return pushable;
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

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		if (routineInfo != null) {

			if (!routineInfo.calledOnNullInput() && routineInfo.getParameterCount() != 0)
				returnsNullOnNullState = acb.newFieldDeclaration(Modifier.PRIVATE, "boolean");

		}

		// reset the parameters are null indicator.
		if (returnsNullOnNullState != null) {
			mb.push(false);
			mb.putField(returnsNullOnNullState);
			mb.endStatement();

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

			// for a function we need to fetch the current SQL control
			// so that we can reset it once the function is complete.
			// 
			if (isFunction)
			{
				functionEntrySQLAllowed = acb.newFieldDeclaration(Modifier.PRIVATE, "short");
				mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getSQLAllowed", "short", 0);
				mb.putField(functionEntrySQLAllowed);
				mb.endStatement();

			}
			
			
			// Set up the statement context to reflect the
			// restricted SQL execution allowed by this routine.

			mb.push(sqlAllowed);
			mb.push(false);
			mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
								"setSQLAllowed", "void", 2);

		}

		// add in the ResultSet arrays.
		if (routineInfo != null) {

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
					cons.putField(procedureResultSetsHolder);
					cons.endStatement();


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
			if (!actualMethodReturnType.equals(javaReturnType))
				mbnc.pushNewComplete(1);

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

				// constructor  - setting up correct paramter type info
				acb.pushThisAsActivation(constructor);
				constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getParameterValueSet", ClassName.ParameterValueSet, 0);

				// execute  - passing out parameters back.
				acb.pushThisAsActivation(mb);
				mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
									"getParameterValueSet", ClassName.ParameterValueSet, 0);

				int[] parameterModes = routineInfo.getParameterModes();
				for (int i = 0; i < outParamArrays.length; i++) {

					int parameterMode = parameterModes[i];
					if (parameterMode != JDBC30Translation.PARAMETER_MODE_IN) {

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

						if (isNumericType) {
							if (!paramdtd.getTypeId().isDecimalTypeId()) {

								if (!((java.lang.reflect.Method) method).getParameterTypes()[i].getComponentType().isPrimitive())
									mb.cast(ClassName.NumberDataValue);
							}
						}
						else if (paramdtd.getTypeId().isBooleanTypeId())
						{
							if (!((java.lang.reflect.Method) method).getParameterTypes()[i].getComponentType().isPrimitive())
								mb.cast(ClassName.BooleanDataValue);
						}



						if (paramdtd.getTypeId().variableLength()) {
							// need another DVD reference for the set width below.
							mb.dup();
						}


						mb.getField(lf); // pvs, dvd, array
						mb.getArrayElement(0); // pvs, dvd, value
						mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "setValue", "void", 1);

						if (paramdtd.getTypeId().variableLength()) {
							mb.push(isNumericType ? paramdtd.getPrecision() : paramdtd.getMaximumWidth());
							mb.push(paramdtd.getScale());
							mb.push(isNumericType);
							mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.VariableSizeDataValue, "setWidth", ClassName.DataValueDescriptor, 3);
							mb.endStatement();
						}
					}
				}
				constructor.endStatement();
				mb.endStatement();
			}

		}
	}
}
