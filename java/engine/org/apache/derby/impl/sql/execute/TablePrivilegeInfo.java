/*

   Derby - Class org.apache.derby.impl.sql.execute.TablePrivilegeInfo

   Copyright 1998, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.PermissionsDescriptor;
import org.apache.derby.iapi.sql.dictionary.TablePermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColPermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;

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

	private TableDescriptor td;
	private boolean[] actionAllowed;
	private FormatableBitSet[] columnBitSets;
	
	/**
	 * @param actionAllowed actionAllowed[action] is true if action is in the privilege set.
	 */
	public TablePrivilegeInfo( TableDescriptor td,
							   boolean[] actionAllowed,
							   FormatableBitSet[] columnBitSets)
	{
		this.actionAllowed = actionAllowed;
		this.columnBitSets = columnBitSets;
		this.td = td;
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
		String currentUser = lcc.getAuthorizationId();
		TransactionController tc = lcc.getTransactionExecute();

		// Check that the current user has permission to grant the privileges.
		checkOwnership( currentUser, td, td.getSchemaDescriptor(), dd);
		
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
			String grantee = (String) itr.next();
			if( tablePermsDesc != null)
			{
				if (dd.addRemovePermissionsDescriptor( grant, tablePermsDesc, grantee, tc))
				{
	        		dd.getDependencyManager().invalidateFor(tablePermsDesc, DependencyManager.REVOKE_PRIVILEGE, lcc);
				}
			}
			for( int i = 0; i < columnBitSets.length; i++)
			{
				if( colPermsDescs[i] != null)
				{
					if (dd.addRemovePermissionsDescriptor( grant, colPermsDescs[i], grantee, tc))					
		        		dd.getDependencyManager().invalidateFor(colPermsDescs[i], DependencyManager.REVOKE_PRIVILEGE, lcc);
				}
			}
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
