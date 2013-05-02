/*

   Derby - Class org.apache.derby.impl.sql.execute.TablePrivilegeInfo

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TablePermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColPermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.reference.SQLState;

import java.util.List;
import java.util.Iterator;

public class TablePrivilegeInfo extends PrivilegeInfo
{
	// Action types
	public static final int SELECT_ACTION = 0;
	public static final int DELETE_ACTION = 1;
	public static final int INSERT_ACTION = 2;
	public static final int UPDATE_ACTION = 3;
	public static final int REFERENCES_ACTION = 4;
	public static final int TRIGGER_ACTION = 5;
	public static final int ACTION_COUNT = 6;

	private static final String YES_WITH_GRANT_OPTION = "Y";
	private static final String YES_WITHOUT_GRANT_OPTION = "y";
	private static final String NO = "N";

	private static final String[][] actionString =
	{{"s", "S"}, {"d", "D"}, {"i", "I"}, {"u", "U"}, {"r", "R"}, {"t", "T"}};

    private final TableDescriptor td;
    private final boolean[] actionAllowed;
    private final FormatableBitSet[] columnBitSets;
    private final List descriptorList;
	
	/**
	 * @param actionAllowed actionAllowed[action] is true if action is in the privilege set.
	 */
	public TablePrivilegeInfo( TableDescriptor td,
							   boolean[] actionAllowed,
							   FormatableBitSet[] columnBitSets,
							   List descriptorList)
	{
        // Copy the arrays so that modification outside doesn't change
        // the internal state.
        this.actionAllowed = ArrayUtil.copy(actionAllowed);
        this.columnBitSets = new FormatableBitSet[columnBitSets.length];
        for (int i = 0; i < columnBitSets.length; i++) {
            if (columnBitSets[i] != null) {
                this.columnBitSets[i] = new FormatableBitSet(columnBitSets[i]);
            }
        }

		this.td = td;
		this.descriptorList = descriptorList;
	}
	
	/**
	 * Determines whether a user is the owner of an object
	 * (table, function, or procedure). Note that the database 
	 * creator can access database objects without needing to be 
	 * their owner.
	 *
	 * @param user					authorizationId of current user
	 * @param td       		        table descriptor being checked against
	 * @param sd					SchemaDescriptor
	 * @param dd					DataDictionary
	 * @param lcc                   LanguageConnectionContext
	 * @param grant                 grant if true; revoke if false
	 *
	 * @exception StandardException if user does not own the object
	 */
	protected void checkOwnership( String user,
								   TableDescriptor td,
								   SchemaDescriptor sd,
								   DataDictionary dd,
								   LanguageConnectionContext lcc,
								   boolean grant)
		throws StandardException
	{
		super.checkOwnership(user, td, sd, dd);
		
		// additional check specific to this subclass
		if (grant)
		{
			checkPrivileges(user, td, sd, dd, lcc);
		}
	}
	
	/**
	 * Determines if the privilege is grantable by this grantor
	 * for the given view.
	 * 
	 * Note that the database owner can access database objects 
	 * without needing to be their owner.  This method should only 
	 * be called if it is a GRANT.
	 * 
	 * @param user					authorizationId of current user
	 * @param td		            TableDescriptor to be checked against
	 * @param sd					SchemaDescriptor
	 * @param dd					DataDictionary
	 * @param lcc                   LanguageConnectionContext
	 *
	 * @exception StandardException if user does not have permission to grant
	 */
	private void checkPrivileges( String user,
								   TableDescriptor td,
								   SchemaDescriptor sd,
								   DataDictionary dd,
								   LanguageConnectionContext lcc)
		throws StandardException
	{
		if (user.equals(dd.getAuthorizationDatabaseOwner())) return;
		
		//  check view specific
		if (td.getTableType() == TableDescriptor.VIEW_TYPE) 
		{
			if (descriptorList != null )
			{			    		   
				TransactionController tc = lcc.getTransactionExecute();
				int siz = descriptorList.size();
				for (int i=0; i < siz; i++)
				{
					TupleDescriptor p;
					SchemaDescriptor s = null;

					p = (TupleDescriptor)descriptorList.get(i);
					if (p instanceof TableDescriptor)
					{
						TableDescriptor t = (TableDescriptor)p;
						s = t.getSchemaDescriptor();
			    	}
					else if (p instanceof ViewDescriptor)
					{
						ViewDescriptor v = (ViewDescriptor)p;	
						s = dd.getSchemaDescriptor(v.getCompSchemaId(), tc);
					}
			    	else if (p instanceof AliasDescriptor)
			    	{
			    		AliasDescriptor a = (AliasDescriptor)p;
						s = dd.getSchemaDescriptor( a.getSchemaUUID(), tc);
			    	}
								
					if (s != null && !user.equals(s.getAuthorizationId()) ) 
					{
						throw StandardException.newException(
				    			   SQLState.AUTH_NO_OBJECT_PERMISSION,
				    			   user,
				    			   "grant",
				    			   sd.getSchemaName(),
								   td.getName());		  
					}
			    			   
			    	// FUTURE: if object is not own by grantor then check if 
			    	//         the grantor have grant option.
				}
			}
		}
	}
	
	/**
	 *	This is the guts of the Execution-time logic for GRANT/REVOKE of a table privilege
	 *
	 * @param activation
	 * @param grant true if grant, false if revoke
	 * @param grantees a list of authorization ids (strings)
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeGrantRevoke( Activation activation,
									boolean grant,
									List grantees)
		throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
        String currentUser = lcc.getCurrentUserId(activation);
		TransactionController tc = lcc.getTransactionExecute();
		SchemaDescriptor sd = td.getSchemaDescriptor();
		
		// Check that the current user has permission to grant the privileges.
		checkOwnership( currentUser, td, sd, dd, lcc, grant);
		
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		TablePermsDescriptor tablePermsDesc =
		  ddg.newTablePermsDescriptor( td,
									   getPermString( SELECT_ACTION, false),
									   getPermString( DELETE_ACTION, false),
									   getPermString( INSERT_ACTION, false),
									   getPermString( UPDATE_ACTION, false),
									   getPermString( REFERENCES_ACTION, false),
									   getPermString( TRIGGER_ACTION, false),
									   currentUser);
			
		ColPermsDescriptor[] colPermsDescs = new ColPermsDescriptor[ columnBitSets.length];
		for( int i = 0; i < columnBitSets.length; i++)
		{
			if( columnBitSets[i] != null ||
				// If it is a revoke and no column list is specified then revoke all column permissions.
				// A null column bitSet in a ColPermsDescriptor indicates that all the column permissions
				// should be removed.
				(!grant) && hasColumnPermissions(i) && actionAllowed[i]
				)
			{
				colPermsDescs[i] = ddg.newColPermsDescriptor( td,
															  getActionString(i, false),
															  columnBitSets[i],
															  currentUser);
			}
		}


		dd.startWriting(lcc);
		// Add or remove the privileges to/from the SYS.SYSTABLEPERMS and SYS.SYSCOLPERMS tables
		for( Iterator itr = grantees.iterator(); itr.hasNext();)
		{
			// Keep track to see if any privileges are revoked by a revoke 
			// statement. If a privilege is not revoked, we need to raise a 
			// warning. For table privileges, we do not check if privilege for 
			// a specific action has been revoked or not. Also, we do not check
			// privileges for specific columns. If at least one privilege has 
			// been revoked, we do not raise a warning. This has to be refined 
			// further to check for specific actions/columns and raise warning 
			// if any privilege has not been revoked.
			boolean privileges_revoked = false;
						
			String grantee = (String) itr.next();
			if( tablePermsDesc != null)
			{
				if (dd.addRemovePermissionsDescriptor( grant, tablePermsDesc, grantee, tc))
				{
					privileges_revoked = true;
					dd.getDependencyManager().invalidateFor
						(tablePermsDesc,
						 DependencyManager.REVOKE_PRIVILEGE, lcc);

					// When revoking a privilege from a Table we need to
					// invalidate all GPSs refering to it. But GPSs aren't
					// Dependents of TablePermsDescr, but of the
					// TableDescriptor itself, so we must send
					// INTERNAL_RECOMPILE_REQUEST to the TableDescriptor's
					// Dependents.
					dd.getDependencyManager().invalidateFor
						(td, DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);
				}
			}
			for( int i = 0; i < columnBitSets.length; i++)
			{
				if( colPermsDescs[i] != null)
				{
					if (dd.addRemovePermissionsDescriptor( grant, colPermsDescs[i], grantee, tc)) 
					{
						privileges_revoked = true;
						dd.getDependencyManager().invalidateFor(colPermsDescs[i], DependencyManager.REVOKE_PRIVILEGE, lcc);
						// When revoking a privilege from a Table we need to
						// invalidate all GPSs refering to it. But GPSs aren't
						// Dependents of colPermsDescs[i], but of the
						// TableDescriptor itself, so we must send
						// INTERNAL_RECOMPILE_REQUEST to the TableDescriptor's
						// Dependents.
						dd.getDependencyManager().invalidateFor
							(td,
							 DependencyManager.INTERNAL_RECOMPILE_REQUEST,
							 lcc);
					}
				}
			}
			
			addWarningIfPrivilegeNotRevoked(activation, grant, privileges_revoked, grantee);
		}
	} // end of executeConstantAction

	private String getPermString( int action, boolean forGrantOption)
	{
		if( actionAllowed[ action] && columnBitSets[action] == null)
			return forGrantOption ? YES_WITH_GRANT_OPTION : YES_WITHOUT_GRANT_OPTION;
		else
			return NO;
	} // end of getPermString

	private String getActionString( int action, boolean forGrantOption)
	{
		return actionString[action][forGrantOption ? 1 : 0];
	}

	private boolean hasColumnPermissions( int action)
	{
		return action == SELECT_ACTION || action == UPDATE_ACTION || action == REFERENCES_ACTION;
	}
}
