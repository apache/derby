/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.TablePermsDescriptor

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * This class describes a row in the SYS.SYSTABLEPERMS system table, which
 * stores the table permissions that have been granted but not revoked.
 */
public class TablePermsDescriptor extends PermissionsDescriptor
{
    private final UUID tableUUID;
    private final String selectPriv;
    private final String deletePriv;
    private final String insertPriv;
    private final String updatePriv;
    private final String referencesPriv;
    private final String triggerPriv;
	
	public TablePermsDescriptor( DataDictionary dd,
                                 String grantee,
                                 String grantor,
                                 UUID tableUUID,
                                 String selectPriv,
                                 String deletePriv,
                                 String insertPriv,
                                 String updatePriv,
                                 String referencesPriv,
                                 String triggerPriv)
	{
		super (dd, grantee, grantor);
        this.tableUUID = tableUUID;
        this.selectPriv = selectPriv;
        this.deletePriv = deletePriv;
        this.insertPriv = insertPriv;
        this.updatePriv = updatePriv;
        this.referencesPriv = referencesPriv;
        this.triggerPriv = triggerPriv;
	}

    /**
     * This constructor just sets up the key fields of a TablePermsDescriptor
     */
    public TablePermsDescriptor( DataDictionary dd,
                                 String grantee,
                                 String grantor,
                                 UUID tableUUID)
    {
        this( dd, grantee, grantor, tableUUID,
              (String) null, (String) null, (String) null, (String) null, (String) null, (String) null);
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
}
