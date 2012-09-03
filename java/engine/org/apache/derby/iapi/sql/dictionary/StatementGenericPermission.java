/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.StatementGenericPermission

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.dictionary.PermDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.services.context.ContextManager;

/**
 * This class describes a generic permission (such as USAGE)
 * required by a statement.
 */

public final class StatementGenericPermission extends StatementPermission
{
	private UUID _objectID;
    private String _objectType; // e.g., PermDescriptor.SEQUENCE_TYPE
    private String _privilege; // e.g., PermDescriptor.USAGE_PRIV

	public StatementGenericPermission( UUID objectID, String objectType, String privilege )
	{
		_objectID = objectID;
        _objectType = objectType;
        _privilege = privilege;
	}

    // accessors
	public UUID getObjectID() { return _objectID; }
    public String getPrivilege() { return _privilege; }

	/**
	 * @see StatementPermission#getObjectType
	 */
    public String getObjectType() { return _objectType; }

	/**
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   boolean forGrant,
					   Activation activation) throws StandardException
	{
        genericCheck( lcc, forGrant, activation, _privilege );
	}


	/**
	 * @see StatementPermission#isCorrectPermission
	 */
    public boolean isCorrectPermission( PermissionsDescriptor raw )
    {
        if ( (raw == null) || !( raw instanceof PermDescriptor) ) { return false; }

        PermDescriptor pd = (PermDescriptor) raw;
        
        return
            pd.getPermObjectId().equals( _objectID ) &&
            pd.getObjectType().equals( _objectType ) &&
            pd.getPermission().equals( _privilege )
            ;
    }

	/**
	 * @see StatementPermission#getPrivilegedObject
	 */
    public PrivilegedSQLObject getPrivilegedObject( DataDictionary dd ) throws StandardException
    {
        if ( PermDescriptor.UDT_TYPE.equals( _objectType ) ) { return dd.getAliasDescriptor( _objectID ); }
        else if ( PermDescriptor.AGGREGATE_TYPE.equals( _objectType ) ) { return dd.getAliasDescriptor( _objectID ); }
        else if ( PermDescriptor.SEQUENCE_TYPE.equals( _objectType ) ) { return dd.getSequenceDescriptor( _objectID ); }
        else
        {
            throw StandardException.newException( SQLState.BTREE_UNIMPLEMENTED_FEATURE );
        }
    }

	/**
	 * @see StatementPermission#getPermissionDescriptor
	 */
	public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		return dd.getGenericPermissions( _objectID, _objectType, _privilege, authid );
	}


	public String toString()
	{
		return "StatementGenericPermission( " + _objectID + ", " + _objectType + ", " + _privilege + " )";
	}
}
