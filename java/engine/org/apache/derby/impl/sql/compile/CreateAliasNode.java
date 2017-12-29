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

import java.sql.Types;
import java.util.List;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.AggregateAliasInfo;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.catalog.types.SynonymAliasInfo;
import org.apache.derby.catalog.types.UDTAliasInfo;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * A CreateAliasNode represents a CREATE ALIAS statement.
 *
 */

class CreateAliasNode extends DDLStatementNode
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
    public static final int VARARGS = ROUTINE_SECURITY_DEFINER + 1;

    // Keep ROUTINE_ELEMENT_COUNT last (determines set cardinality).
    // Note: Remember to also update the map ROUTINE_CLAUSE_NAMES in
    // sqlgrammar.jj when elements are added.
    public static final int ROUTINE_ELEMENT_COUNT = VARARGS + 1;

    //
    // These are the names of 1-arg builtin functions which are represented in the
    // grammar as non-reserved keywords. These names may not be used as
    // the unqualified names of user-defined aggregates.
    //
    // If additional 1-arg builtin functions are added to the grammar, they should
    // be put in this table.
    //
    private static  final   String[]  NON_RESERVED_FUNCTION_NAMES =
    {
        "ABS",
        "ABSVAL",
        "DATE",
        "DAY",
        "LCASE",
        "LENGTH",
        "MONTH",
        "SQRT",
        "TIME",
        "TIMESTAMP",
        "UCASE",
    };

    //
    // These are aggregate names defined by the SQL Standard which do not
    // behave as reserved keywords in Derby.
    //
    private static  final   String[]    NON_RESERVED_AGGREGATES =
    {
        "COLLECT",
        "COUNT",
        "EVERY",
        "FUSION",
        "INTERSECTION",
        "STDDEV_POP",
        "STDDEV_SAMP",
        "VAR_POP",
        "VAR_SAMP",
    };

    // aggregate arguments
    public  static  final   int AGG_FOR_TYPE = 0;
    public  static  final   int AGG_RETURN_TYPE = AGG_FOR_TYPE + 1;
    public  static  final   int AGG_ELEMENT_COUNT = AGG_RETURN_TYPE + 1;

	private String				javaClassName;
	private String				methodName;
	private char				aliasType; 

	private AliasInfo aliasInfo;


	/**
     * Constructor
	 *
	 * @param aliasName				The name of the alias
     * @param targetObject          Target name string or, if
     *        aliasType == ALIAS_TYPE_SYNONYM_AS_CHAR, a TableName
	 * @param methodName		    The method name
     * @param aliasSpecificInfo     An array of objects, see code for
     *                              interpretation
     * @param cm                    The context manager
     * @exception StandardException Thrown on error
	 */
    CreateAliasNode(    TableName aliasName,
						Object targetObject,
                        String methodName,
						Object aliasSpecificInfo,
                        char aliasType,
                        ContextManager cm)
		throws StandardException
	{		
        super(aliasName, cm);
        this.aliasType = aliasType;

		switch (this.aliasType)
		{
			case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
				this.javaClassName = (String) targetObject;

				Object[] aggElements = (Object[]) aliasSpecificInfo;
                TypeDescriptor  aggForType = bindUserCatalogType( (TypeDescriptor) aggElements[ AGG_FOR_TYPE ] );
                TypeDescriptor  aggReturnType = bindUserCatalogType( (TypeDescriptor) aggElements[ AGG_RETURN_TYPE ] );

                // XML not allowed because SQLXML support has not been implemented
                if (
                    (aggForType.getJDBCTypeId() == Types.SQLXML) ||
                    (aggReturnType.getJDBCTypeId() == Types.SQLXML)
                   )
                {
                    throw StandardException.newException( SQLState.LANG_XML_NOT_ALLOWED_DJRS );
                }

				aliasInfo = new AggregateAliasInfo( aggForType, aggReturnType );
				implicitCreateSchema = true;
                break;
                
			case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
				this.javaClassName = (String) targetObject;
				aliasInfo = new UDTAliasInfo();

				implicitCreateSchema = true;
                break;
                
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
			{
				this.javaClassName = (String) targetObject;
                this.methodName = methodName;

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
				int paramCount = ((List) parameters[0]).size();
				
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
				
				if (paramCount != 0) {

                    names = new String[paramCount];
                    types = new TypeDescriptor[paramCount];
					modes = new int[paramCount];

					for (int i = 0; i < paramCount; i++) {
                        names[i] = (String) ((List) parameters[0]).get(i);
                        types[i] = (TypeDescriptor) ((List) parameters[1]).get(i);
                        int currentMode =  ((Integer) (((List) parameters[2]).get(i))).intValue();
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

				Boolean hasVarargsO = (Boolean) routineElements[ VARARGS ];
                boolean hasVarargs = (hasVarargsO == null) ? false : hasVarargsO.booleanValue();

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
                    hasVarargs,
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

    String statementToString()
	{
		switch (this.aliasType)
		{
		case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
			return "CREATE DERBY AGGREGATE";
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
    @Override
	public void bindStatement() throws StandardException
	{
		//Are we dealing with user defined function or procedure?
		if (aliasType == AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR ||
				aliasType == AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR) {

            RoutineAliasInfo    rai = (RoutineAliasInfo)aliasInfo;
            
            // Set the collation for all string types in parameters
            // and return types including row multi-sets to be that of
            // the schema the routine is being defined in.
            rai.setCollationTypeForAllStringTypes(
                    getSchemaDescriptor().getCollationType());

            bindParameterTypes( (RoutineAliasInfo)aliasInfo );

            if ( rai.hasVarargs() )
            {
                switch ( rai.getParameterStyle() )
                {
                case RoutineAliasInfo.PS_DERBY_JDBC_RESULT_SET:
                case RoutineAliasInfo.PS_DERBY:
                    break;

                default:
                    throw StandardException.newException( SQLState.LANG_VARARGS_PARAMETER_STYLE );
                }

                if ( rai.getMaxDynamicResultSets() > 0 )
                {
                    throw StandardException.newException( SQLState.LANG_VARARGS_RETURN_RESULT_SETS );
                }
            }

            if (
                (rai.getParameterStyle() == RoutineAliasInfo.PS_DERBY) &&
                !rai.hasVarargs()
                )
            {
                throw StandardException.newException( SQLState.LANG_DERBY_PARAMETER_STYLE );
            }
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

        // validity checking for aggregates
        if ( aliasType == AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR ) { bindAggregate(); }
        
		// Aggregates, procedures and functions do not check class or method validity until
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

    /** Extra logic for binding user-defined aggregate definitions */
    private void    bindAggregate() throws StandardException
    {
        String                      unqualifiedName = getRelativeName();

        //
        // A user-defined aggregate cannot have the name of a builtin function which takes 1 argument.
        //
        SchemaDescriptor    sysfun = getSchemaDescriptor( "SYSFUN", true );
        List<AliasDescriptor> systemFunctions =
            getDataDictionary().getRoutineList(
                sysfun.getUUID().toString(),
                unqualifiedName,
                AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR);

        for ( int i = 0; i < systemFunctions.size(); i++ )
        {
            AliasDescriptor function = systemFunctions.get(i);

			RoutineAliasInfo routineInfo = (RoutineAliasInfo) function.getAliasInfo();
			int parameterCount = routineInfo.getParameterCount();
			if ( parameterCount == 1 )  { throw illegalAggregate(); }
        }
        
        //
        // Additional builtin 1-arg functions which are represented in the grammar
        // as non-reserved keywords.
        //
        for ( int i = 0; i < NON_RESERVED_FUNCTION_NAMES.length; i++ )
        {
            if ( NON_RESERVED_FUNCTION_NAMES[ i ].equals( unqualifiedName ) )  { throw illegalAggregate(); }
        }

        //
        // Additional SQL Standard aggregate names which are not represented in
        // the Derby grammar as reserved keywords.
        //
        for ( int i = 0; i < NON_RESERVED_AGGREGATES.length; i++ )
        {
            if ( NON_RESERVED_AGGREGATES[ i ].equals( unqualifiedName ) )  { throw illegalAggregate(); }
        }

        // now bind the input and return types
        AggregateAliasInfo  aai = (AggregateAliasInfo) aliasInfo;

        aai.setCollationTypeForAllStringTypes( getSchemaDescriptor().getCollationType() );
    }

    /** Construct an exception flagging an illegal aggregate name */
    private StandardException   illegalAggregate()
    {
        return StandardException.newException( SQLState.LANG_ILLEGAL_UDA_NAME, getRelativeName() );
    }

    /** Bind the class names for UDTs */
    private void bindParameterTypes( RoutineAliasInfo aliasInfo ) throws StandardException
    {
        TypeDescriptor[] parameterTypes = aliasInfo.getParameterTypes();

        if ( parameterTypes == null ) { return; }

        int count = parameterTypes.length;
        for ( int i = 0; i < count; i++ )
        {
            parameterTypes[ i ] = bindUserCatalogType( parameterTypes[ i ] );
        }

        aliasInfo.setParameterTypes( parameterTypes );
    }

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
    public ConstantAction makeConstantAction() throws StandardException
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
