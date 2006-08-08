/*

   Derby - Class org.apache.derby.impl.sql.compile.TablePrivilegesNode

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
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.sql.execute.PrivilegeInfo;
import org.apache.derby.impl.sql.execute.TablePrivilegeInfo;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

/**
 * This class represents a set of privileges on one table.
 */
public class TablePrivilegesNode extends QueryTreeNode
{
	private boolean[] actionAllowed = new boolean[ TablePrivilegeInfo.ACTION_COUNT];
	private ResultColumnList[] columnLists = new ResultColumnList[ TablePrivilegeInfo.ACTION_COUNT];
	private FormatableBitSet[] columnBitSets = new FormatableBitSet[ TablePrivilegeInfo.ACTION_COUNT];
	private TableDescriptor td;

	/**
	 * Add all actions
	 */
	public void addAll()
	{
		for( int i = 0; i < TablePrivilegeInfo.ACTION_COUNT; i++)
		{
			actionAllowed[i] = true;
			columnLists[i] = null;
		}
	} // end of addAll

	/**
	 * Add one action to the privileges for this table
	 *
	 * @param action The action type
	 * @param privilegeColumnList The set of privilege columns. Null for all columns
	 *
	 * @exception StandardException standard error policy.
	 */
	public void addAction( int action, ResultColumnList privilegeColumnList)
	{
		actionAllowed[ action] = true;
		if( privilegeColumnList == null)
			columnLists[ action] = null;
		else if( columnLists[ action] == null)
			columnLists[ action] = privilegeColumnList;
		else
			columnLists[ action].appendResultColumns( privilegeColumnList, false);
	} // end of addAction

	/**
	 * Bind.
	 *
	 * @param td The table descriptor
	 */
	public void bind( TableDescriptor td) throws StandardException
	{
		this.td = td;
		
		for( int action = 0; action < TablePrivilegeInfo.ACTION_COUNT; action++)
		{
			if( columnLists[ action] != null)
				columnBitSets[action] = columnLists[ action].bindResultColumnsByName( td, (DMLStatementNode) null);

			// Prevent granting non-SELECT privileges to views
			if (td.getTableType() == TableDescriptor.VIEW_TYPE && action != TablePrivilegeInfo.SELECT_ACTION)
				if (actionAllowed[action])
					throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED,
									td.getQualifiedName());
		}
	}
	
	/**
	 * @return PrivilegeInfo for this node
	 */
	public PrivilegeInfo makePrivilegeInfo()
	{
		return new TablePrivilegeInfo( td, actionAllowed, columnBitSets);
	}
}
