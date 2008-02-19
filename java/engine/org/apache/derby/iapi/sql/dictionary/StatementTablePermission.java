/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.StatementTablePermission

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

/**
 * This class describes a table permission required by a statement.
 */

public class StatementTablePermission extends StatementPermission
{
	UUID tableUUID;
	int privType; // One of Authorizer.SELECT_PRIV, UPDATE_PRIV, etc.

	/**
	 * Constructor for StatementTablePermission. Creates an instance of
	 * table permission requested for the given access.
	 * 
	 * @param tableUUID	UUID of the table
	 * @param privType	Access privilege requested
	 *
	 */
	public StatementTablePermission(UUID tableUUID, int privType)
	{
		this.tableUUID = tableUUID;
		this.privType = privType;
	}

	/**
	 * Return privilege access requested for this access descriptor
	 *
	 * @return	Privilege access
	 */
	public int getPrivType()
	{
		return privType;
	}

	/**
	 * Return table UUID for this access descriptor
	 *
	 * @return	Table UUID
	 */
	public UUID getTableUUID()
	{
		return tableUUID;
	}

	/**
	 * Routine to check if another instance of access descriptor matches this.
	 * Used to ensure only one access descriptor for a table of given privilege is created.
	 * Otherwise, every column reference from a table may create a descriptor for that table.
	 *
	 * @param obj	Another instance of StatementPermission
	 *
	 * @return	true if match
	 */
	public boolean equals( Object obj)
	{
		if( obj == null)
			return false;
		if( getClass().equals( obj.getClass()))
		{
			StatementTablePermission other = (StatementTablePermission) obj;
			return privType == other.privType && tableUUID.equals( other.tableUUID);
		}
		return false;
	} // end of equals

	/**
	 * Return hash code for this instance
	 *
	 * @return	Hashcode
	 *
	 */
	public int hashCode()
	{
		return privType + tableUUID.hashCode();
	}
	
	/**
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   String authorizationId,
					   boolean forGrant)
		throws StandardException
	{
		DataDictionary dd = lcc.getDataDictionary();
	
		if( ! hasPermissionOnTable( dd, authorizationId, forGrant))
		{
			TableDescriptor td = getTableDescriptor( dd);
			throw StandardException.newException( forGrant ? SQLState.AUTH_NO_TABLE_PERMISSION_FOR_GRANT
												  : SQLState.AUTH_NO_TABLE_PERMISSION,
												  authorizationId,
												  getPrivName(),
												  td.getSchemaName(),
												  td.getName());
		}
	} // end of check

	protected TableDescriptor getTableDescriptor(DataDictionary dd)  throws StandardException
	{
		TableDescriptor td = dd.getTableDescriptor( tableUUID);
		if( td == null)
			throw StandardException.newException(SQLState.AUTH_INTERNAL_BAD_UUID, "table");
		return td;
	} // end of getTableDescriptor

	/*
	 * Check if authorizationId has permission on the table
	 */
	protected boolean hasPermissionOnTable(DataDictionary dd, String authorizationId, boolean forGrant)
		throws StandardException
	{
		return oneAuthHasPermissionOnTable( dd, Authorizer.PUBLIC_AUTHORIZATION_ID, forGrant)
		  || oneAuthHasPermissionOnTable( dd, authorizationId, forGrant);
	}

	protected boolean oneAuthHasPermissionOnTable(DataDictionary dd, String authorizationId, boolean forGrant)
		throws StandardException
	{
		TablePermsDescriptor perms = dd.getTablePermissions( tableUUID, authorizationId);
		if( perms == null)
			return false;
		
		String priv = null;
			
		switch( privType)
		{
		case Authorizer.SELECT_PRIV:
			priv = perms.getSelectPriv();
			break;
		case Authorizer.UPDATE_PRIV:
			priv = perms.getUpdatePriv();
			break;
		case Authorizer.REFERENCES_PRIV:
			priv = perms.getReferencesPriv();
			break;
		case Authorizer.INSERT_PRIV:
			priv = perms.getInsertPriv();
			break;
		case Authorizer.DELETE_PRIV:
			priv = perms.getDeletePriv();
			break;
		case Authorizer.TRIGGER_PRIV:
			priv = perms.getTriggerPriv();
			break;
		}

		return "Y".equals(priv) || (!forGrant) && "y".equals( priv);
	} // end of hasPermissionOnTable

	/**
	 * @see StatementPermission#getPermissionDescriptor
	 */
	public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		//if the required type of privilege exists for the given authorizer,
		//then pass the permission descriptor for it.
		if (oneAuthHasPermissionOnTable( dd, authid, false))
			return dd.getTablePermissions(tableUUID, authid);
		else return null;
	}

	/**
	 * Return privilege needed for this access as string
	 *
	 * @return	privilege string
	 */
	public String getPrivName( )
	{
		switch( privType)
		{
		case Authorizer.SELECT_PRIV:
			return "SELECT";
		case Authorizer.UPDATE_PRIV:
			return "UPDATE";
		case Authorizer.REFERENCES_PRIV:
			return "REFERENCES";
		case Authorizer.INSERT_PRIV:
			return "INSERT";
		case Authorizer.DELETE_PRIV:
			return "DELETE";
		case Authorizer.TRIGGER_PRIV:
			return "TRIGGER";
		}
		return "?";
	} // end of getPrivName
}
