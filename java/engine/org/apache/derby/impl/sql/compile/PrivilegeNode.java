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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.impl.sql.execute.PrivilegeInfo;
import org.apache.derby.catalog.TypeDescriptor;

import java.util.HashMap;
import java.util.List;

/**
 * This node represents a set of privileges that are granted or revoked on one object.
 */
public class PrivilegeNode extends QueryTreeNode
{
    // Privilege object type
    public static final int TABLE_PRIVILEGES = 0;
    public static final int ROUTINE_PRIVILEGES = 1;

    private int objectType;
    private Object objectOfPrivilege;
    private TablePrivilegesNode specificPrivileges; // Null for routines

    /**
     * initialize a PrivilegesNode
     *
     * @param objectType (an Integer)
     * @param objectOfPrivilege (a TableName or RoutineDesignator)
     * @param specificPrivileges null for routines
     */
    public void init( Object objectType, Object objectOfPrivilege, Object specificPrivileges)
    {
        this.objectType = ((Integer) objectType).intValue();
        this.objectOfPrivilege = objectOfPrivilege;
        this.specificPrivileges = (TablePrivilegesNode) specificPrivileges;
        if( SanityManager.DEBUG)
        {
            SanityManager.ASSERT( objectOfPrivilege != null,
                                  "null privilge object");
            switch( this.objectType)
            {
            case TABLE_PRIVILEGES:
                SanityManager.ASSERT( objectOfPrivilege instanceof TableName,
                                      "incorrect name type, " + objectOfPrivilege.getClass().getName()
                                      + ", used with table privilege");
                SanityManager.ASSERT( specificPrivileges != null,
                                      "null specific privileges used with table privilege");
                break;

            case ROUTINE_PRIVILEGES:
                SanityManager.ASSERT( objectOfPrivilege instanceof RoutineDesignator,
                                      "incorrect name type, " + objectOfPrivilege.getClass().getName()
                                      + ", used with table privilege");
                SanityManager.ASSERT( specificPrivileges == null,
                                      "non-null specific privileges used with execute privilege");
                break;

            default:
                SanityManager.THROWASSERT( "Invalid privilege objectType: " + this.objectType);
            }
        }
    } // end of init

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
	public QueryTreeNode bind( HashMap dependencies, List grantees, boolean isGrant ) throws StandardException
	{
        Provider dependencyProvider = null;
        SchemaDescriptor sd = null;
		
        switch( objectType)
        {
        case TABLE_PRIVILEGES:
            TableName tableName = (TableName) objectOfPrivilege;
            sd = getSchemaDescriptor( tableName.getSchemaName(), true);
            if (sd.isSystemSchema())
                throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, tableName.getFullTableName());
				
            TableDescriptor td = getTableDescriptor( tableName.getTableName(), sd);
            if( td == null)
                throw StandardException.newException( SQLState.LANG_TABLE_NOT_FOUND, tableName);

            // Don't allow authorization on SESSION schema tables. Causes confusion if
            // a temporary table is created later with same name.
            if (isSessionSchema(sd.getSchemaName()))
                throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);

            if (td.getTableType() != TableDescriptor.BASE_TABLE_TYPE &&
            		td.getTableType() != TableDescriptor.VIEW_TYPE)
                throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, tableName.getFullTableName());

			// Can not grant/revoke permissions from self
			if (grantees.contains(sd.getAuthorizationId()))
				throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED,
						 td.getQualifiedName());

            specificPrivileges.bind( td, isGrant);
            dependencyProvider = td;
            break;

        case ROUTINE_PRIVILEGES:
            RoutineDesignator rd = (RoutineDesignator) objectOfPrivilege;
            sd = getSchemaDescriptor( rd.name.getSchemaName(), true);

            if (!sd.isSchemaWithGrantableRoutines())
                throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, rd.name.getFullTableName());
				
            AliasDescriptor proc = null;
            RoutineAliasInfo routineInfo = null;
            java.util.List list = getDataDictionary().getRoutineList(
                sd.getUUID().toString(), rd.name.getTableName(),
                rd.isFunction ? AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR : AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR
                );

			// Can not grant/revoke permissions from self
			if (grantees.contains(sd.getAuthorizationId()))
				throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED,
						 rd.name.getFullTableName());

            if( rd.paramTypeList == null)
            {
                // No signature was specified. Make sure that there is exactly one routine with that name.
                if( list.size() > 1)
                    throw StandardException.newException( ( rd.isFunction ? SQLState.LANG_AMBIGUOUS_FUNCTION_NAME
                                                            : SQLState.LANG_AMBIGUOUS_PROCEDURE_NAME),
                                                          rd.name.getFullTableName());
                if( list.size() != 1)
                    throw StandardException.newException(SQLState.LANG_NO_SUCH_METHOD_ALIAS, rd.name.getFullTableName());
                proc = (AliasDescriptor) list.get(0);
            }
            else
            {
                // The full signature was specified
                boolean found = false;
                for (int i = list.size() - 1; (!found) && i >= 0; i--)
                {
                    proc = (AliasDescriptor) list.get(i);

                    routineInfo = (RoutineAliasInfo) proc.getAliasInfo();
                    int parameterCount = routineInfo.getParameterCount();
                    if (parameterCount != rd.paramTypeList.size())
                        continue;
                    TypeDescriptor[] parameterTypes = routineInfo.getParameterTypes();
                    found = true;
                    for( int parmIdx = 0; parmIdx < parameterCount; parmIdx++)
                    {
                        if( ! parameterTypes[parmIdx].equals( rd.paramTypeList.get( parmIdx)))
                        {
                            found = false;
                            break;
                        }
                    }
                }
                if( ! found)
                {
                    // reconstruct the signature for the error message
                    StringBuffer sb = new StringBuffer( rd.name.getFullTableName());
                    sb.append( "(");
                    for( int i = 0; i < rd.paramTypeList.size(); i++)
                    {
                        if( i > 0)
                            sb.append(",");
                        sb.append( rd.paramTypeList.get(i).toString());
                    }
                    throw StandardException.newException(SQLState.LANG_NO_SUCH_METHOD_ALIAS, sb.toString());
                }
            }
            rd.setAliasDescriptor( proc);
            dependencyProvider = proc;
            break;
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
    PrivilegeInfo makePrivilegeInfo()
    {
        switch( objectType)
        {
        case TABLE_PRIVILEGES:
            return specificPrivileges.makePrivilegeInfo();

        case ROUTINE_PRIVILEGES:
            return ((RoutineDesignator) objectOfPrivilege).makePrivilegeInfo();
        }
        return null;
    }
}
