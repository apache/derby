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

import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 * This class represents a GRANT role statement.
 */
class GrantRoleNode extends DDLStatementNode
{
    private List<String> roles;
    private List<String> grantees;

    /**
     * Constructor for GrantRoleNode.
     *
     * @param roles list of strings containing role name to be granted
     * @param grantees list of strings containing grantee names
     * @param cm context manager
     */
    GrantRoleNode(List<String> roles,
                  List<String> grantees,
                  ContextManager cm) throws StandardException {
        super(null, cm);
        this.roles = roles;
        this.grantees = grantees;
    }


    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @exception StandardException Standard error policy.
     */
    @Override
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
    @Override
    public String toString()
    {
        if (SanityManager.DEBUG) {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                StringBuilder sb1 = new StringBuilder();
                for(String role : roles) {
					if( sb1.length() > 0) {
						sb1.append( ", ");
					}
                    sb1.append(role);
				}

                StringBuilder sb2 = new StringBuilder();
                for(String grantee : grantees) {
					if( sb2.length() > 0) {
						sb2.append( ", ");
					}
                    sb2.append(grantee);
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
