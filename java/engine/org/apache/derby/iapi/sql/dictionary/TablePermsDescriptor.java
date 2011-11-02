/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.TablePermsDescriptor

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

import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.error.StandardException;

/**
 * This class describes a row in the SYS.SYSTABLEPERMS system table, which
 * stores the table permissions that have been granted but not revoked.
 */
public class TablePermsDescriptor extends PermissionsDescriptor
{
    private UUID tableUUID;
    private String tableName;
    private String selectPriv;
    private String deletePriv;
    private String insertPriv;
    private String updatePriv;
    private String referencesPriv;
    private String triggerPriv;
	
	public TablePermsDescriptor( DataDictionary dd,
                                 String grantee,
                                 String grantor,
                                 UUID tableUUID,
                                 String selectPriv,
                                 String deletePriv,
                                 String insertPriv,
                                 String updatePriv,
                                 String referencesPriv,
                                 String triggerPriv) throws StandardException
	{
		super (dd, grantee, grantor);
        this.tableUUID = tableUUID;
        this.selectPriv = selectPriv;
        this.deletePriv = deletePriv;
        this.insertPriv = insertPriv;
        this.updatePriv = updatePriv;
        this.referencesPriv = referencesPriv;
        this.triggerPriv = triggerPriv;
        //tableUUID can be null only if the constructor with tablePermsUUID
        //has been invoked.
        if (tableUUID != null)
        	tableName = dd.getTableDescriptor(tableUUID).getName();
	}

    /**
     * This constructor just sets up the key fields of a TablePermsDescriptor
     */
    public TablePermsDescriptor( DataDictionary dd,
                                 String grantee,
                                 String grantor,
                                 UUID tableUUID) throws StandardException
    {
        this( dd, grantee, grantor, tableUUID,
              (String) null, (String) null, (String) null, (String) null, (String) null, (String) null);
    }
    
    public TablePermsDescriptor( DataDictionary dd,
            UUID tablePermsUUID) throws StandardException
            {
        this( dd, null, null, null,
                (String) null, (String) null, (String) null, (String) null, (String) null, (String) null);
        this.oid = tablePermsUUID;
			}

    public int getCatalogNumber()
    {
        return DataDictionary.SYSTABLEPERMS_CATALOG_NUM;
    }
	
	/*----- getter functions for rowfactory ------*/
    public UUID getTableUUID() { return tableUUID;}
    public String getSelectPriv() { return selectPriv;}
    public String getDeletePriv() { return deletePriv;}
    public String getInsertPriv() { return insertPriv;}
    public String getUpdatePriv() { return updatePriv;}
    public String getReferencesPriv() { return referencesPriv;}
    public String getTriggerPriv() { return triggerPriv;}

	public String toString()
	{
		return "tablePerms: grantee=" + getGrantee() +
		",tablePermsUUID=" + getUUID() +
			",grantor=" + getGrantor() +
          ",tableUUID=" + getTableUUID() +
          ",selectPriv=" + getSelectPriv() +
          ",deletePriv=" + getDeletePriv() +
          ",insertPriv=" + getInsertPriv() +
          ",updatePriv=" + getUpdatePriv() +
          ",referencesPriv=" + getReferencesPriv() +
          ",triggerPriv=" + getTriggerPriv();
	}

    /**
     * @return true iff the key part of this permissions descriptor equals the key part of another permissions
     *         descriptor.
     */
    public boolean equals( Object other)
    {
        if( !( other instanceof TablePermsDescriptor))
            return false;
        TablePermsDescriptor otherTablePerms = (TablePermsDescriptor) other;
        return super.keyEquals( otherTablePerms) && tableUUID.equals( otherTablePerms.tableUUID);
    }
    
    /**
     * @return the hashCode for the key part of this permissions descriptor
     */
    public int hashCode()
    {
    	return super.keyHashCode() + tableUUID.hashCode();
    }
	
	/**
	 * @see PermissionsDescriptor#checkOwner
	 */
	public boolean checkOwner(String authorizationId) throws StandardException
	{
		TableDescriptor td = getDataDictionary().getTableDescriptor(tableUUID);
		if (td.getSchemaDescriptor().getAuthorizationId().equals(authorizationId))
			return true;
		else
			return false;
	}

	//////////////////////////////////////////////
	//
	// PROVIDER INTERFACE
	//
	//////////////////////////////////////////////

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		return "Table Privilege on " + tableName; 
	}

	/**
	 * Get the provider's type.
	 *
	 * @return char		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.TABLE_PERMISSION;
	}

	/**		
		@return the stored form of this provider

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder() 
	{
        return getDependableFinder(
                StoredFormatIds.TABLE_PERMISSION_FINDER_V01_ID);
	}
}
