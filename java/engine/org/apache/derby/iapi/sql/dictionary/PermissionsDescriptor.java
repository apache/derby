/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.PermissionsDescriptor

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

import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * This class is used by rows in the SYS.SYSTABLEPERMS, SYS.SYSCOLPERMS, and SYS.SYSROUTINEPERMS
 * system tables.
 */
public abstract class PermissionsDescriptor extends TupleDescriptor 
	implements Cloneable, Provider
{
	protected UUID oid;
	private String grantee;
	private final String grantor;

	PermissionsDescriptor( DataDictionary dd,
								  String grantee,
								  String grantor)
	{
		super (dd);
		this.grantee = grantee;
		this.grantor = grantor;
	}

	public Object clone()
	{
		try
		{
			return super.clone();
		}
		catch( java.lang.CloneNotSupportedException cnse)
		{
			if( SanityManager.DEBUG)
				SanityManager.THROWASSERT("Could not clone a " +
										  getClass().getName(), cnse);
			return null;
		}
	}
	
	public abstract int getCatalogNumber();

	/**
	 * @return true iff the key part of this permissions descriptor equals the key part of another permissions
	 *		 descriptor.
	 */
	protected boolean keyEquals( PermissionsDescriptor other)
	{
		return grantee.equals( other.grantee);
	}
		   
	/**
	 * @return the hashCode for the key part of this permissions descriptor
	 */
	protected int keyHashCode()
	{
		return grantee.hashCode();
	}
	
	public void setGrantee( String grantee)
	{
		this.grantee = grantee;
	}
	
	/*----- getter functions for rowfactory ------*/
	public final String getGrantee() { return grantee;}
	public final String getGrantor() { return grantor;}

	/**
	 * Gets the UUID of the table.
	 *
	 * @return	The UUID of the table.
	 */
	public UUID	getUUID() { return oid;}

	/**
	 * Sets the UUID of the table
	 *
	 * @param oid	The UUID of the table to be set in the descriptor
	 */
	public void setUUID(UUID oid) {	this.oid = oid;}
	
	/**
	 * This method checks if the passed authorization id is same as the owner 
	 * of the object on which this permission is defined. This method gets
	 * called by create view/constraint/trigger to see if this permission 
	 * needs to be saved in dependency system for the view/constraint/trigger. 
	 * If the same user is the owner of the the object being accessed and the 
	 * newly created object, then no need to keep this privilege dependency 
	 *
	 * @return boolean	If passed authorization id is owner of the table
	 */
	public abstract boolean checkOwner(String authorizationId) throws StandardException;

	//////////////////////////////////////////////
	//
	// PROVIDER INTERFACE
	//
	////////////////////////////////////////////////////////////////////

	/**
	 * Get the provider's UUID
	 *
	 * @return 	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return oid;
	}

	/**
	 * Is this provider persistent?  A stored dependency will be required
	 * if both the dependent and provider are persistent.
	 *
	 * @return boolean              Whether or not this provider is persistent.
	 */
	public boolean isPersistent()
	{
		return true;
	}
}
