/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.RoutinePermsDescriptor

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
 * This class describes rows in the SYS.SYSROUTINEPERMS system table, which keeps track of the routine
 * (procedure and function) permissions that have been granted but not revoked.
 */
public class RoutinePermsDescriptor extends PermissionsDescriptor
{
    private final UUID routineUUID;
    private final boolean hasExecutePermission;
	
	public RoutinePermsDescriptor( DataDictionary dd,
                                   String grantee,
                                   String grantor,
                                   UUID routineUUID,
                                   boolean hasExecutePermission)
	{
		super (dd, grantor, grantee);
        this.routineUUID = routineUUID;
        this.hasExecutePermission = hasExecutePermission;
	}
	
	public RoutinePermsDescriptor( DataDictionary dd,
                                   String grantee,
                                   String grantor,
                                   UUID routineUUID)
	{
        this( dd, grantor, grantee, routineUUID, true);
	}

    /**
     * This constructor just sets up the key fields of a RoutinePermsDescriptor.
     */
	public RoutinePermsDescriptor( DataDictionary dd,
                                   String grantee,
                                   String grantor)
    {
        this( dd, grantee, grantor, (UUID) null);
    }
    
    public int getCatalogNumber()
    {
        return DataDictionary.SYSROUTINEPERMS_CATALOG_NUM;
    }
	
	/*----- getter functions for rowfactory ------*/
    public UUID getRoutineUUID() { return routineUUID;}
    public boolean getHasExecutePermission() { return hasExecutePermission;}

	public String toString()
	{
		return "routinePerms: grantor=" + getGrantee() + 
          ",grantor=" + getGrantor() +
          ",routineUUID=" + getRoutineUUID();
	}		

    /**
     * @return true iff the key part of this permissions descriptor equals the key part of another permissions
     *         descriptor.
     */
    public boolean equals( Object other)
    {
        if( !( other instanceof RoutinePermsDescriptor))
            return false;
        RoutinePermsDescriptor otherRoutinePerms = (RoutinePermsDescriptor) other;
        return super.keyEquals( otherRoutinePerms) &&
          routineUUID.equals( otherRoutinePerms.routineUUID);
    }
    
    /**
     * @return the hashCode for the key part of this permissions descriptor
     */
    public int hashCode()
    {
        return super.keyHashCode() + routineUUID.hashCode();
    }
}
