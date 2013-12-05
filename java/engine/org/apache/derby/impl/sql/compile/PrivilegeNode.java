/*

   Derby - Class org.apache.derby.impl.sql.compile.PrivilegeNode

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

import java.util.HashMap;
import java.util.List;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.PrivilegedSQLObject;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.impl.sql.execute.GenericPrivilegeInfo;
import org.apache.derby.impl.sql.execute.PrivilegeInfo;

/**
 * This node represents a set of privileges that are granted or revoked on one object.
 */
class PrivilegeNode extends QueryTreeNode
{
    // Privilege object type
    public static final int TABLE_PRIVILEGES = 0;
    public static final int ROUTINE_PRIVILEGES = 1;
    public static final int SEQUENCE_PRIVILEGES = 2;
    public static final int UDT_PRIVILEGES = 3;
    public static final int AGGREGATE_PRIVILEGES = 4;

    //
    // State initialized when the node is instantiated
    //
    private int objectType;
    private TableName objectName;
    private TablePrivilegesNode specificPrivileges; // Null for routine and usage privs
    private RoutineDesignator routineDesignator; // null for table and usage privs

    private String privilege;  // E.g., PermDescriptor.USAGE_PRIV
    private boolean restrict;

    //
    // State which is filled in by the bind() logic.
    //
    private Provider dependencyProvider;
    
    /**
     * Initialize a PrivilegeNode for use against SYS.SYSTABLEPERMS and SYS.SYSROUTINEPERMS.
     *
     * @param objectType
     * @param objectOfPrivilege  (a TableName or RoutineDesignator)
     * @param specificPrivileges null for routines and usage
     * @param cm                 the context manager
     */
    PrivilegeNode(int                 objectType,
                  Object              objectOfPrivilege,
                  TablePrivilegesNode specificPrivileges,
                  ContextManager      cm) throws StandardException {
        super(cm);
        this.objectType = objectType;

        if ( SanityManager.DEBUG)
        {
            SanityManager.ASSERT( objectOfPrivilege != null,
                                  "null privilge object");
        }

        switch( this.objectType)
        {
        case TABLE_PRIVILEGES:
            if( SanityManager.DEBUG)
            {
                SanityManager.ASSERT( specificPrivileges != null,
                                      "null specific privileges used with table privilege");
            }
            objectName = (TableName) objectOfPrivilege;
            this.specificPrivileges = specificPrivileges;
            break;
            
        case ROUTINE_PRIVILEGES:
            if( SanityManager.DEBUG)
            {
                SanityManager.ASSERT( specificPrivileges == null,
                                      "non-null specific privileges used with execute privilege");
            }
            routineDesignator = (RoutineDesignator) objectOfPrivilege;
            objectName = routineDesignator.name;
            break;
            
        default:
            throw unimplementedFeature();
        }
    }

    /**
     * Constructor a PrivilegeNode for use against SYS.SYSPERMS.
     *
     * @param objectType E.g., SEQUENCE
     * @param objectName A possibles schema-qualified name
     * @param privilege  A PermDescriptor privilege, e.g.
     *                   {@code PermDescriptor.USAGE_PRIV}
     * @param restrict   True if this is a REVOKE...RESTRICT action
     * @param cm         The context manager
     */
    PrivilegeNode(int            objectType,
                  TableName      objectName,
                  String         privilege,
                  boolean        restrict,
                  ContextManager cm)
    {
        super(cm);
        this.objectType = objectType;
        this.objectName = objectName;
        this.privilege = privilege;
        this.restrict = restrict;
    }
    
    /**
     * Bind this GrantNode. Resolve all table, column, and routine references. Register
     * a dependency on the object of the privilege if it has not already been done
     *
     * @param dependencies The list of privilege objects that this statement has already seen.
     *               If the object of this privilege is not in the list then this statement is registered
     *               as dependent on the object.
     * @param grantees The list of grantees
     * @param isGrant grant if true; revoke if false
     * @return the bound node
     *
     * @exception StandardException	Standard error policy.
     */
    public QueryTreeNode bind(
            HashMap<Provider,Provider> dependencies,
            List<String> grantees,
            boolean isGrant ) throws StandardException
	{
        // The below code handles the case where objectName.getSchemaName()
        // returns null, in which case we'll fetch the schema descriptor for
        // the current compilation schema (see getSchemaDescriptor).
        SchemaDescriptor sd = getSchemaDescriptor( objectName.getSchemaName(), true);
        objectName.setSchemaName( sd.getSchemaName() );
        
        // Can not grant/revoke permissions from self
        if (grantees.contains(sd.getAuthorizationId()))
        {
            throw StandardException.newException
                (SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, objectName.getFullTableName());
        }

        switch( objectType)
        {
        case TABLE_PRIVILEGES:

            // can't grant/revoke privileges on system tables
            if (sd.isSystemSchema())
            {
                throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, objectName.getFullTableName());
            }
            
            TableDescriptor td = getTableDescriptor( objectName.getTableName(), sd);
            if( td == null)
            {
                throw StandardException.newException( SQLState.LANG_TABLE_NOT_FOUND, objectName);
            }

            // Don't allow authorization on SESSION schema tables. Causes confusion if
            // a temporary table is created later with same name.
            if (isSessionSchema(sd.getSchemaName()))
            {
                throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);
            }

            if (td.getTableType() != TableDescriptor.BASE_TABLE_TYPE &&
            		td.getTableType() != TableDescriptor.VIEW_TYPE)
            {
                throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, objectName.getFullTableName());
            }

            specificPrivileges.bind( td, isGrant);
            dependencyProvider = td;
            break;

        case ROUTINE_PRIVILEGES:
            if (!sd.isSchemaWithGrantableRoutines())
            {
                throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, objectName.getFullTableName());
            }
				
            AliasDescriptor proc = null;
            List<AliasDescriptor> list = getDataDictionary().getRoutineList(
                sd.getUUID().toString(),
                objectName.getTableName(),
                routineDesignator.isFunction ?
                    AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR :
                    AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR);

            if( routineDesignator.paramTypeList == null)
            {
                // No signature was specified. Make sure that there is exactly one routine with that name.
                if( list.size() > 1)
                {
                    throw StandardException.newException( ( routineDesignator.isFunction ? SQLState.LANG_AMBIGUOUS_FUNCTION_NAME
                                                            : SQLState.LANG_AMBIGUOUS_PROCEDURE_NAME),
                                                          objectName.getFullTableName());
                }
                if( list.size() != 1) {
                    if (routineDesignator.isFunction) {
                        throw StandardException.newException(SQLState.LANG_NO_SUCH_FUNCTION, 
                                objectName.getFullTableName());
                    } else {
                        throw StandardException.newException(SQLState.LANG_NO_SUCH_PROCEDURE, 
                                objectName.getFullTableName());
                    }
                }
                proc = list.get(0);
            }
            else
            {
                // The full signature was specified
                boolean found = false;
                for (int i = list.size() - 1; (!found) && i >= 0; i--)
                {
                    proc = list.get(i);

                    RoutineAliasInfo
                        routineInfo = (RoutineAliasInfo) proc.getAliasInfo();
                    int parameterCount = routineInfo.getParameterCount();
                    if (parameterCount != routineDesignator.paramTypeList.size())
                        continue;
                    TypeDescriptor[] parameterTypes = routineInfo.getParameterTypes();
                    found = true;
                    for( int parmIdx = 0; parmIdx < parameterCount; parmIdx++)
                    {
                        if( ! parameterTypes[parmIdx].equals( routineDesignator.paramTypeList.get( parmIdx)))
                        {
                            found = false;
                            break;
                        }
                    }
                }
                if( ! found)
                {
                    // reconstruct the signature for the error message
                    StringBuilder sb =
                            new StringBuilder(objectName.getFullTableName());
                    sb.append( "(");
                    for( int i = 0; i < routineDesignator.paramTypeList.size(); i++)
                    {
                        if( i > 0)
                            sb.append(",");
                        sb.append( routineDesignator.paramTypeList.get(i).toString());
                    }
                    throw StandardException.newException(SQLState.LANG_NO_SUCH_METHOD_ALIAS, sb.toString());
                }
            }
            routineDesignator.setAliasDescriptor( proc);
            dependencyProvider = proc;
            break;

        case AGGREGATE_PRIVILEGES:
            
            dependencyProvider = getDataDictionary().getAliasDescriptor
                ( sd.getUUID().toString(), objectName.getTableName(), AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR  );
            if ( dependencyProvider == null )
            {
                throw StandardException.newException
                    (SQLState.LANG_OBJECT_NOT_FOUND, "DERBY AGGREGATE", objectName.getFullTableName());
            }
            break;
            
        case SEQUENCE_PRIVILEGES:
            
            dependencyProvider = getDataDictionary().getSequenceDescriptor( sd, objectName.getTableName() );
            if ( dependencyProvider == null )
            {
                throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "SEQUENCE", objectName.getFullTableName());
            }
            break;
            
        case UDT_PRIVILEGES:
            
            dependencyProvider = getDataDictionary().getAliasDescriptor
                ( sd.getUUID().toString(), objectName.getTableName(), AliasInfo.ALIAS_NAME_SPACE_UDT_AS_CHAR  );
            if ( dependencyProvider == null )
            {
                throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "TYPE", objectName.getFullTableName());
            }
            break;
            
        default:
            throw unimplementedFeature();
        }

        if( dependencyProvider != null)
        {
            if( dependencies.get( dependencyProvider) == null)
            {
                getCompilerContext().createDependency( dependencyProvider);
                dependencies.put( dependencyProvider, dependencyProvider);
            }
        }
        return this;
    } // end of bind


    /**
     * @return PrivilegeInfo for this node
     */
    PrivilegeInfo makePrivilegeInfo() throws StandardException
    {
        switch( objectType)
        {
        case TABLE_PRIVILEGES:
            return specificPrivileges.makePrivilegeInfo();

        case ROUTINE_PRIVILEGES:
            return routineDesignator.makePrivilegeInfo();

        case AGGREGATE_PRIVILEGES:
        case SEQUENCE_PRIVILEGES:
        case UDT_PRIVILEGES:
            return new GenericPrivilegeInfo( (PrivilegedSQLObject) dependencyProvider, privilege, restrict );

        default:
            throw unimplementedFeature();
        }
    }

    /** Report an unimplemented feature */
    private StandardException unimplementedFeature()
    {
        return StandardException.newException( SQLState.BTREE_UNIMPLEMENTED_FEATURE );
    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (objectName != null) {
            objectName = (TableName) objectName.accept(v);
        }
    }
}
