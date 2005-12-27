/*

   Derby - Class org.apache.derby.impl.sql.compile.GrantNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.impl.sql.execute.PrivilegeInfo;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * This class represents a GRANT statement.
 */
public class GrantNode extends MiscellaneousStatementNode
{
	private PrivilegeNode privileges;
	private List grantees;

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuffer sb = new StringBuffer();
			for( Iterator it = grantees.iterator(); it.hasNext();)
			{
				if( sb.length() > 0)
					sb.append( ",");
				sb.append( it.next().toString());
			}
			return super.toString() +
				   privileges.toString() +
				   "TO: \n" + sb.toString() + "\n";
		}
		else
		{
			return "";
		}
	} // end of toString

	public String statementToString()
	{
		return "GRANT";
	}

	
	/**
	 * Initialize a GrantNode.
	 *
	 * @param privileges PrivilegesNode
	 * @param grantees List
	 */
	public void init( Object privileges,
					  Object grantees)
	{
		this.privileges = (PrivilegeNode) privileges;
		this.grantees = (List) grantees;
	}

	/**
	 * Bind this GrantNode. Resolve all table, column, and routine references.
	 *
	 * @return the bound GrantNode
	 *
	 * @exception StandardException	Standard error policy.
	 */
	public QueryTreeNode bind() throws StandardException
	{
		privileges = (PrivilegeNode) privileges.bind( new HashMap());
		return this;
	} // end of bind


	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException	Standard error policy.
	 */
	public ConstantAction makeConstantAction() throws StandardException
	{
		return getGenericConstantActionFactory().getGrantConstantAction( privileges.makePrivilegeInfo(),
																		 grantees);
	}
}
