/*

   Derby - Class org.apache.derby.impl.sql.catalog.PermissionsCacheable

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.catalog.UUID;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.TablePermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.PermDescriptor;
import org.apache.derby.iapi.sql.dictionary.PermissionsDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColPermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.PrivilegedSQLObject;
import org.apache.derby.iapi.sql.dictionary.RoutinePermsDescriptor;

import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * This class implements a Cacheable for a DataDictionary cache of
 * permissions.
 */
class PermissionsCacheable implements Cacheable
{
	protected final DataDictionaryImpl dd;
	private PermissionsDescriptor permissions;
	
	PermissionsCacheable(DataDictionaryImpl dd)
	{
		this.dd = dd;
	}

	/* Cacheable interface */
	public Cacheable setIdentity(Object key) throws StandardException
	{
		// If the user does not have permission then cache an empty (no permission) descriptor in
		// case the same user asks again. That is particularly important for table permission because
		// we ask about table permission before column permissions. If a user has permission to use a
		// proper subset of the columns we will still ask about table permission every time he tries
		// to access that column subset.
		if( key instanceof TablePermsDescriptor)
		{
			TablePermsDescriptor tablePermsKey = (TablePermsDescriptor) key;
			permissions = dd.getUncachedTablePermsDescriptor( tablePermsKey);
			if( permissions == null)
			{
				// The owner has all privileges unless they have been revoked.
				TableDescriptor td = dd.getTableDescriptor( tablePermsKey.getTableUUID());
				SchemaDescriptor sd = td.getSchemaDescriptor();
				if( sd.isSystemSchema())
                {
					// RESOLVE The access to system tables is hard coded to SELECT only to everyone.
					// Is this the way we want Derby to work? Should we allow revocation of read access
					// to system tables? If so we must explicitly add a row to the SYS.SYSTABLEPERMISSIONS
					// table for each system table when a database is created.
					permissions = new TablePermsDescriptor( dd,
															tablePermsKey.getGrantee(),
															(String) null,
															tablePermsKey.getTableUUID(),
															"Y", "N", "N", "N", "N", "N");
                    // give the permission the same UUID as the system table
                    ((TablePermsDescriptor) permissions).setUUID( tablePermsKey.getTableUUID() );
                }
				else if( tablePermsKey.getGrantee().equals( sd.getAuthorizationId()))
                {
					permissions = new TablePermsDescriptor( dd,
															tablePermsKey.getGrantee(),
															Authorizer.SYSTEM_AUTHORIZATION_ID,
															tablePermsKey.getTableUUID(),
															"Y", "Y", "Y", "Y", "Y", "Y");
                }
				else
                {
					permissions = new TablePermsDescriptor( dd,
															tablePermsKey.getGrantee(),
															(String) null,
															tablePermsKey.getTableUUID(),
															"N", "N", "N", "N", "N", "N");
                }
			}
		}
		else if( key instanceof ColPermsDescriptor)
		{
			ColPermsDescriptor colPermsKey = (ColPermsDescriptor) key;
			permissions = dd.getUncachedColPermsDescriptor(colPermsKey );
			if( permissions == null)
				permissions = new ColPermsDescriptor( dd,
													  colPermsKey.getGrantee(),
													  (String) null,
													  colPermsKey.getTableUUID(),
													  colPermsKey.getType(),
													  (FormatableBitSet) null);
		}
		else if( key instanceof RoutinePermsDescriptor)
		{
			RoutinePermsDescriptor routinePermsKey = (RoutinePermsDescriptor) key;
			permissions = dd.getUncachedRoutinePermsDescriptor( routinePermsKey);
			if( permissions == null)
			{
				// The owner has all privileges unless they have been revoked.
				try
				{
					AliasDescriptor ad = dd.getAliasDescriptor( routinePermsKey.getRoutineUUID());
					SchemaDescriptor sd = dd.getSchemaDescriptor( ad.getSchemaUUID(),
											  ConnectionUtil.getCurrentLCC().getTransactionExecute());
//IC see: https://issues.apache.org/jira/browse/DERBY-464
					if (sd.isSystemSchema() && !sd.isSchemaWithGrantableRoutines())
						permissions = new RoutinePermsDescriptor( dd,
																  routinePermsKey.getGrantee(),
                                                                  (String) null,
																  routinePermsKey.getRoutineUUID(),
																  true);
					else if( routinePermsKey.getGrantee().equals( sd.getAuthorizationId()))
						permissions = new RoutinePermsDescriptor( dd,
																  routinePermsKey.getGrantee(),
																  Authorizer.SYSTEM_AUTHORIZATION_ID,
																  routinePermsKey.getRoutineUUID(),
//IC see: https://issues.apache.org/jira/browse/DERBY-464
																  true);
				}
				catch( java.sql.SQLException sqle)
				{
					throw StandardException.plainWrapException( sqle);
				}
			}
		}
		else if( key instanceof PermDescriptor)
		{
			PermDescriptor permKey = (PermDescriptor) key;
			permissions = dd.getUncachedGenericPermDescriptor( permKey);
			if( permissions == null)
			{
				// The owner has all privileges unless they have been revoked.
                String objectType = permKey.getObjectType();
                String privilege = permKey.getPermission();
                UUID protectedObjectsID = permKey.getPermObjectId();
                
                
                PrivilegedSQLObject pso = PermDescriptor.getProtectedObject( dd, protectedObjectsID, objectType );
                SchemaDescriptor sd = pso.getSchemaDescriptor();
                if( permKey.getGrantee().equals( sd.getAuthorizationId()))
                {
                    permissions = new PermDescriptor
                        (
                         dd,
                         null,
                         objectType,
                         pso.getUUID(),
                         privilege,
                         Authorizer.SYSTEM_AUTHORIZATION_ID,
                         permKey.getGrantee(),
                         true
                         );
                }
			}
		}
		else
		{
			if( SanityManager.DEBUG)
				SanityManager.NOTREACHED();
			return null;
		}
		if( permissions != null) { return this; }
    
		return null;
	} // end of setIdentity

	public Cacheable createIdentity(Object key, Object createParameter) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT( (key instanceof TablePermsDescriptor) ||
								  (key instanceof ColPermsDescriptor) ||
								  (key instanceof RoutinePermsDescriptor),
								  "Invalid class, " + key.getClass().getName()
								  + ", passed as key to PermissionsCacheable.createIdentity");
		}
		if( key == null)
			return null;
		permissions = (PermissionsDescriptor) ((PermissionsDescriptor)key).clone();
		return this;
	} // end of createIdentity

	public void clearIdentity()
	{
		permissions = null;
	}

	public Object getIdentity()
	{
		return permissions;
	}

	public boolean isDirty()
	{
		return false;
	}

	public void clean(boolean forRemove) throws StandardException
	{
	}
}
