/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateAliasNode

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.DB2Limit;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.execute.BaseActivation;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.RoutineAliasInfo;

import java.lang.reflect.Member;
import java.util.Vector;

/**
 * A CreateAliasNode represents a CREATE ALIAS statement.
 *
 * @author Jerry Brenner
 */

public class CreateAliasNode extends CreateStatementNode
{
	String				javaClassName;
	String				methodName;
	char				aliasType; 
	boolean				delimitedIdentifier;

	private AliasInfo aliasInfo;


	/**
	 * Initializer for a CreateAliasNode
	 *
	 * @param aliasName				The name of the alias
	 * @param javaClassName			The full class name
	 * @param methodName		    The method name
	 * @param aliasType				The alias type
	 * @param delimitedIdentifier	Whether or not to treat the class name
	 *								as a delimited identifier if trying to
	 *								resolve it as a class alias
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
						Object aliasName,
						Object javaClassName,
						Object methodName,
						Object aliasSpecificInfo,
						Object aliasType,
						Object delimitedIdentifier)
		throws StandardException
	{		
		TableName qn = (TableName) aliasName;

		initAndCheck(qn);
			
		this.javaClassName = (String) javaClassName;
		this.methodName = (String) methodName;
		this.aliasType = ((Character) aliasType).charValue();
		this.delimitedIdentifier =
								((Boolean) delimitedIdentifier).booleanValue();


		switch (this.aliasType)
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
			{
				//routineElements contains the description of the procedure.
				// 
				// 0 - Object[] 3 element array for parameters
				// 1 - TableName - specific name
				// 2 - Integer - dynamic result set count
				// 3 - String language (always java) - ignore
				// 4 - String external name (also passed directly to create alias node - ignore
				// 5 - Integer parameter style 
				// 6 - Short - SQL control
				// 7 - Boolean - CALLED ON NULL INPUT (always TRUE for procedures)
				// 8 - TypeDescriptor - return type (always NULL for procedures)

				Object[] routineElements = (Object[]) aliasSpecificInfo;
				Object[] parameters = (Object[]) routineElements[0];
				int paramCount = ((Vector) parameters[0]).size();

				String[] names = null;
				TypeDescriptor[] types = null;
				int[] modes = null;

				if (paramCount > DB2Limit.DB2_MAX_PARAMS_IN_STORED_PROCEDURE)
					throw StandardException.newException(SQLState.LANG_TOO_MANY_PARAMETERS_FOR_STORED_PROC,
							String.valueOf(DB2Limit.DB2_MAX_PARAMS_IN_STORED_PROCEDURE), aliasName, String.valueOf(paramCount));

				if (paramCount != 0) {

					names = new String[paramCount];
					((Vector) parameters[0]).copyInto(names);

					types = new TypeDescriptor[paramCount];
					((Vector) parameters[1]).copyInto(types);

					modes = new int[paramCount];
					for (int i = 0; i < paramCount; i++) {
						modes[i] = ((Integer) (((Vector) parameters[2]).elementAt(i))).intValue();

						if (TypeId.getBuiltInTypeId(types[i].getJDBCTypeId()).isLongConcatableTypeId())
							throw StandardException.newException(SQLState.LANG_LONG_DATA_TYPE_NOT_ALLOWED, names[i]);

					}

					if (paramCount > 1) {
						String[] dupNameCheck = new String[paramCount];
						System.arraycopy(names, 0, dupNameCheck, 0, paramCount);
						java.util.Arrays.sort(dupNameCheck);
						for (int dnc = 1; dnc < dupNameCheck.length; dnc++) {
							if (dupNameCheck[dnc].equals(dupNameCheck[dnc - 1]))
								throw StandardException.newException(SQLState.LANG_DB2_DUPLICATE_NAMES, dupNameCheck[dnc], getFullName());
						}
					}
				}

				Integer drso = (Integer) routineElements[2];
				int drs = drso == null ? 0 : drso.intValue();

				short sqlAllowed;
				Short sqlAllowedObject = (Short) routineElements[6];
				if (sqlAllowedObject != null)
					sqlAllowed = sqlAllowedObject.shortValue();
				else
					sqlAllowed = (this.aliasType == AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR ?
					RoutineAliasInfo.MODIFIES_SQL_DATA : RoutineAliasInfo.READS_SQL_DATA);

				Boolean calledOnNullInputO = (Boolean) routineElements[7];
				boolean calledOnNullInput;
				if (calledOnNullInputO == null)
					calledOnNullInput = true;
				else
					calledOnNullInput = calledOnNullInputO.booleanValue();

				aliasInfo = new RoutineAliasInfo(this.methodName, paramCount, names, types, modes, drs,
						((Short) routineElements[5]).shortValue(),	// parameter style
						sqlAllowed, calledOnNullInput, (TypeDescriptor) routineElements[8]);

				implicitCreateSchema = true;
				}
				break;


			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Unexpected value for aliasType (" + aliasType + ")");
				}
		}
	}

	public	String	getAliasName() { return getRelativeName(); }
    public	String	getJavaClassName() { return javaClassName; }
    public	String	getMethodName() { return methodName; }
    public	char	getAliasType() { return aliasType; }


	public String statementToString()
	{
		switch (this.aliasType)
		{
		case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
			return "CREATE PROCEDURE";
		default:
			return "CREATE FUNCTION";
		}
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this CreateAliasNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 * For example, verifying that the column name list does not
	 * contain any duplicate column names.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bind() throws StandardException
	{
		// Procedures do not check class or method validity until runtime execution of the procedure.

		return this;
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		String schemaName;
		switch (aliasType) {
		case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
		case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
			schemaName = getSchemaDescriptor().getSchemaName();
			break;
		default:
			schemaName = null;
		}

		return	getGenericConstantActionFactory().getCreateAliasConstantAction(
											  getRelativeName(),
											  schemaName,
											  javaClassName,
											  aliasInfo,
											  aliasType);
	}
}
