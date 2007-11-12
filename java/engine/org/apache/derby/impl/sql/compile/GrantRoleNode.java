/*

   Derby - Class org.apache.derby.impl.sql.compile.GrantRoleNode

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import java.util.Iterator;
import java.util.List;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.conn.Authorizer;

/**
 * This class represents a GRANT role statement.
 */
public class GrantRoleNode extends DDLStatementNode
{
    private List roles;
    private List grantees;

    /**
     * Initialize a GrantRoleNode.
     *
     * @param roles list of strings containing role name to be granted
     * @param grantees list of strings containing grantee names
     */
    public void init(Object roles,
					 Object grantees)
        throws StandardException
    {
        initAndCheck(null);
        this.roles = (List) roles;
        this.grantees = (List) grantees;
    }


    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @exception StandardException Standard error policy.
     */
    public ConstantAction makeConstantAction() throws StandardException
    {
        return getGenericConstantActionFactory().
            getGrantRoleConstantAction( roles, grantees);
    }


    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return  This object as a String
     */

    public String toString()
    {
        if (SanityManager.DEBUG) {
                StringBuffer sb1 = new StringBuffer();
                for( Iterator it = roles.iterator(); it.hasNext();) {
					if( sb1.length() > 0) {
						sb1.append( ", ");
					}
					sb1.append( it.next().toString());
				}

                StringBuffer sb2 = new StringBuffer();
                for( Iterator it = grantees.iterator(); it.hasNext();) {
					if( sb2.length() > 0) {
						sb2.append( ", ");
					}
					sb2.append( it.next().toString());
				}
                return (super.toString() +
                        sb1.toString() +
                        " TO: " +
                        sb2.toString() +
                        "\n");
		} else {
			return "";
		}
    } // end of toString

    public String statementToString()
    {
        return "GRANT role";
    }
}
