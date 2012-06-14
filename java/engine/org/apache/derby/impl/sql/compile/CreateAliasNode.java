/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateAliasNode

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

import java.util.Vector;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.catalog.types.SynonymAliasInfo;
import org.apache.derby.catalog.types.UDTAliasInfo;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * A CreateAliasNode represents a CREATE ALIAS statement.
 *
 */

public class CreateAliasNode extends DDLStatementNode
{
    // indexes into routineElements
    public static final int PARAMETER_ARRAY = 0;
    public static final int TABLE_NAME = PARAMETER_ARRAY + 1;
    public static final int DYNAMIC_RESULT_SET_COUNT = TABLE_NAME + 1;
    public static final int LANGUAGE = DYNAMIC_RESULT_SET_COUNT + 1;
    public static final int EXTERNAL_NAME = LANGUAGE + 1;
    public static final int PARAMETER_STYLE = EXTERNAL_NAME + 1;
    public static final int SQL_CONTROL = PARAMETER_STYLE + 1;
    public static final int DETERMINISTIC = SQL_CONTROL + 1;
    public static final int NULL_ON_NULL_INPUT = DETERMINISTIC + 1;
    public static final int RETURN_TYPE = NULL_ON_NULL_INPUT + 1;
    public static final int ROUTINE_SECURITY_DEFINER = RETURN_TYPE + 1;

    // Keep ROUTINE_ELEMENT_COUNT last (determines set cardinality).
    // Note: Remember to also update the map ROUTINE_CLAUSE_NAMES in
    // sqlgrammar.jj when elements are added.
    public static final int ROUTINE_ELEMENT_COUNT =
        ROUTINE_SECURITY_DEFINER + 1;

	private String				javaClassName;
	private String				methodName;
	private char				aliasType; 

	private AliasInfo aliasInfo;


	/**
	 * Initializer for a CreateAliasNode
	 *
	 * @param aliasName				The name of the alias
	 * @param targetObject			Target name
	 * @param methodName		    The method name
	 * @param aliasType				The alias type
     *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
						Object aliasName,
						Object targetObject,
						Object methodName,
						Object aliasSpecificInfo,
                        Object aliasType)
		throws StandardException
	{		
		TableName qn = (TableName) aliasName;
		this.aliasType = ((Character) aliasType).charValue();

		initAndCheck(qn);

		switch (this.aliasType)
		{
			case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
				this.javaClassName = (String) targetObject;
				aliasInfo = new UDTAliasInfo();

				implicitCreateSchema = true;
                break;
                
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
			{
				this.javaClassName = (String) targetObject;
				this.methodName = (String) methodName;

				//routineElements contains the description of the procedure.
				// 
				// 0 - Object[] 3 element array for parameters
				// 1 - TableName - specific name
				// 2 - Integer - dynamic result set count
				// 3 - String language (always java) - ignore
				// 4 - String external name (also passed directly to create alias node - ignore
				// 5 - Integer parameter style 
				// 6 - Short - SQL control
				// 7 - Boolean - whether the routine is DETERMINISTIC
				// 8 - Boolean - CALLED ON NULL INPUT (always TRUE for procedures)
				// 9 - TypeDescriptor - return type (always NULL for procedures)

				Object[] routineElements = (Object[]) aliasSpecificInfo;
				Object[] parameters = (Object[]) routineElements[PARAMETER_ARRAY];
				int paramCount = ((Vector) parameters[0]).size();
				
				// Support for Java signatures in Derby was added in 10.1
				// Check to see the catalogs have been upgraded to 10.1 before
				// accepting such a method name for a routine. Otherwise
				// a routine that works in 10.1 soft upgrade mode would
				// exist when running 10.0 but not resolve to anything.
				if (this.methodName.indexOf('(') != -1)
				{
					getDataDictionary().checkVersion(
							DataDictionary.DD_VERSION_DERBY_10_1,
                            "EXTERNAL NAME 'class.method(<signature>)'");
					
				}

				String[] names = null;
				TypeDescriptor[] types = null;
				int[] modes = null;
				
				if (paramCount > Limits.DB2_MAX_PARAMS_IN_STORED_PROCEDURE)
					throw StandardException.newException(SQLState.LANG_TOO_MANY_PARAMETERS_FOR_STORED_PROC,
							String.valueOf(Limits.DB2_MAX_PARAMS_IN_STORED_PROCEDURE), aliasName, String.valueOf(paramCount));

				if (paramCount != 0) {

					names = new String[paramCount];
					((Vector) parameters[0]).copyInto(names);

					types = new TypeDescriptor[paramCount];
					((Vector) parameters[1]).copyInto(types);

					modes = new int[paramCount];
					for (int i = 0; i < paramCount; i++) {
                        int currentMode =  ((Integer) (((Vector) parameters[2]).get(i))).intValue();
                        modes[i] = currentMode;
  
                        //
                        // We still don't support XML values as parameters.
                        // Presumably, the XML datatype would map to a JDBC java.sql.SQLXML type.
                        // We have no support for that type today.
                        //
                        if ( !types[ i ].isUserDefinedType() )
                        {
                            if (TypeId.getBuiltInTypeId(types[i].getJDBCTypeId()).isXMLTypeId())
                            { throw StandardException.newException(SQLState.LANG_LONG_DATA_TYPE_NOT_ALLOWED, names[i]); }
                        }
                    }

					if (paramCount > 1) {
						String[] dupNameCheck = new String[paramCount];
						System.arraycopy(names, 0, dupNameCheck, 0, paramCount);
						java.util.Arrays.sort(dupNameCheck);
						for (int dnc = 1; dnc < dupNameCheck.length; dnc++) {
							if (! dupNameCheck[dnc].equals("") && dupNameCheck[dnc].equals(dupNameCheck[dnc - 1]))
								throw StandardException.newException(SQLState.LANG_DB2_DUPLICATE_NAMES, dupNameCheck[dnc], getFullName());
						}
					}
				}

				Integer drso = (Integer) routineElements[DYNAMIC_RESULT_SET_COUNT];
				int drs = drso == null ? 0 : drso.intValue();

				short sqlAllowed;
				Short sqlAllowedObject = (Short) routineElements[SQL_CONTROL];
				if (sqlAllowedObject != null)
					sqlAllowed = sqlAllowedObject.shortValue();
				else
					sqlAllowed = (this.aliasType == AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR ?
					RoutineAliasInfo.MODIFIES_SQL_DATA : RoutineAliasInfo.READS_SQL_DATA);

				Boolean isDeterministicO = (Boolean) routineElements[DETERMINISTIC];
                boolean isDeterministic = (isDeterministicO == null) ? false : isDeterministicO.booleanValue();

                Boolean definersRightsO =
                    (Boolean) routineElements[ROUTINE_SECURITY_DEFINER];
                boolean definersRights  =
                    (definersRightsO == null) ? false :
                    definersRightsO.booleanValue();

				Boolean calledOnNullInputO = (Boolean) routineElements[NULL_ON_NULL_INPUT];
				boolean calledOnNullInput;
				if (calledOnNullInputO == null)
					calledOnNullInput = true;
				else
					calledOnNullInput = calledOnNullInputO.booleanValue();

                // bind the return type if it is a user defined type. this fills
                // in the class name.
                TypeDescriptor returnType = (TypeDescriptor) routineElements[RETURN_TYPE];
                if ( returnType != null )
                {
                    DataTypeDescriptor dtd = DataTypeDescriptor.getType( returnType );
                    
                    dtd = bindUserType( dtd );
                    returnType = dtd.getCatalogType();
                }

                aliasInfo = new RoutineAliasInfo(
                    this.methodName,
                    paramCount,
                    names,
                    types,
                    modes,
                    drs,
                    // parameter style:
                    ((Short) routineElements[PARAMETER_STYLE]).shortValue(),
                    sqlAllowed,
                    isDeterministic,
                    definersRights,
                    calledOnNullInput,
                    returnType );

				implicitCreateSchema = true;
				}
				break;

			case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
				String targetSchema;
				implicitCreateSchema = true;
				TableName t = (TableName) targetObject;
				if (t.getSchemaName() != null)
					targetSchema = t.getSchemaName();
				else targetSchema = getSchemaDescriptor().getSchemaName();
				aliasInfo = new SynonymAliasInfo(targetSchema, t.getTableName());
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Unexpected value for aliasType (" + aliasType + ")");
				}
		}
	}

	public String statementToString()
	{
		switch (this.aliasType)
		{
		case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
			return "CREATE TYPE";
		case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
			return "CREATE PROCEDURE";
		case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
			return "CREATE SYNONYM";
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
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindStatement() throws StandardException
	{
		//Are we dealing with user defined function or procedure?
		if (aliasType == AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR ||
				aliasType == AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR) {
            
            // Set the collation for all string types in parameters
            // and return types including row multi-sets to be that of
            // the schema the routine is being defined in.
            ((RoutineAliasInfo)aliasInfo).setCollationTypeForAllStringTypes(
                    getSchemaDescriptor().getCollationType());

            bindParameterTypes( (RoutineAliasInfo)aliasInfo );
		}
        
        // validity checking for UDTs
        if ( aliasType == AliasInfo.ALIAS_TYPE_UDT_AS_CHAR )
        {
            //
            // Make sure that the java class name is not the name of a builtin
            // type. This skirts problems caused by logic across the system
            // which assumes a tight association between the builtin SQL types
            // and the Java classes which implement them.
            //
            // For security reasons we do not allow the user to bind a UDT
            // to a Derby class.
            //
            TypeId[] allSystemTypeIds = TypeId.getAllBuiltinTypeIds();
            int systemTypeCount = allSystemTypeIds.length;

            boolean foundConflict = javaClassName.startsWith( "org.apache.derby." );

            if ( !foundConflict )
            {
                for ( int i = 0; i < systemTypeCount; i++ )
                {
                    TypeId systemType = allSystemTypeIds[ i ];
                    String systemTypeName = systemType.getCorrespondingJavaTypeName();
                    
                    if ( systemTypeName.equals( javaClassName ) )
                    {
                        foundConflict = true;
                        break;
                    }
                }
            }
            
            if ( foundConflict )
            {
                throw StandardException.newException
                    ( SQLState.LANG_UDT_BUILTIN_CONFLICT, javaClassName );
            }
            
            return;
        }

		// Procedures and functions do not check class or method validity until
		// runtime execution. Synonyms do need some validity checks.
		if (aliasType != AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR)
			return;

		// Don't allow creating synonyms in SESSION schema. Causes confusion if
		// a temporary table is created later with same name.
		if (isSessionSchema(getSchemaDescriptor().getSchemaName()))
			throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);

		String targetSchema = ((SynonymAliasInfo)aliasInfo).getSynonymSchema();
		String targetTable = ((SynonymAliasInfo)aliasInfo).getSynonymTable();
		if (this.getObjectName().equals(targetSchema, targetTable))
			throw StandardException.newException(SQLState.LANG_SYNONYM_CIRCULAR,
						this.getFullName(),
						targetSchema+"."+targetTable);

		SchemaDescriptor targetSD = getSchemaDescriptor(targetSchema, false);
		if ((targetSD != null) && isSessionSchema(targetSD))
			throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);

	}

    /** Bind the class names for UDTs */
    private void bindParameterTypes( RoutineAliasInfo aliasInfo ) throws StandardException
    {
        TypeDescriptor[] parameterTypes = aliasInfo.getParameterTypes();

        if ( parameterTypes == null ) { return; }

        int count = parameterTypes.length;
        for ( int i = 0; i < count; i++ )
        {
            TypeDescriptor td = parameterTypes[ i ];

            // if this is a user defined type, resolve the Java class name
            if ( td.isUserDefinedType() )
            {
                DataTypeDescriptor dtd = DataTypeDescriptor.getType( td );

                dtd = bindUserType( dtd );
                parameterTypes[ i ] = dtd.getCatalogType();
            }
        }
    }

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		String schemaName = getSchemaDescriptor().getSchemaName();

		return	getGenericConstantActionFactory().getCreateAliasConstantAction(
											  getRelativeName(),
											  schemaName,
											  javaClassName,
											  aliasInfo,
											  aliasType);
	}
}
