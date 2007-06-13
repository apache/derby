/*

   Derby - Class org.apache.derby.authentication.DatabasePrincipal

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.authentication;

import java.io.Serializable;
import java.security.Principal;

/**
 * This class represents Derby's notion of a principal, a concept of
 * user identity with controlled access to Derby-specific privileges.
 * An authenticated user may have other identities which make sense in
 * other code domains.
 * 
 * A principal represents an identity characterized by a user name.
 * The wildcard user name "*" can be used to declare a policy grant to
 * "all users".
 *
 * In future, database-scoped identities will be supported by appending
 * the user name with the distinguished character "@" followed by a
 * database name or the wildcard "*" meaning all databases.
 *
 * Note that the special characters "@" and "*" may be part of the user
 * or database names if escaped by a "\" character; the escaping "\"
 * character itself may be included in a name if doubled.
 */
public class DatabasePrincipal implements Principal, Serializable {

    /**
     * A principal with the distinguished name "*" representing a grant
     * for "all user names".
     */
    static public final DatabasePrincipal ANY_DATABASE_PRINCIPAL
        = new DatabasePrincipal("*");

    /**
     * The name of the principal.
     *
     * The distinguished "*" character means "all user names" if not
     * escaped by a "\" character. 
     */
    private final String userName;

    /**
     * Constructs a principal with the specified name.
     *
     * @param name the name of the principal
     * @throws NullPointerException if name is null
     * @throws IllegalArgumentException if name is not a legal Principal name
     */
    public DatabasePrincipal(String name) {
        parsePrincipalName(name);
        this.userName = name;
    }

    /**
     * Parses a principal name for a user name and an optional database name.
     *
     * @param name the name of the principal
     */
    private void parsePrincipalName(String name) {
        // note that exception messages on the Principal name aren't localized,
        // as is the general rule with runtime exceptions indicating
        // internal coding errors

        // analog to org.apache.derby.security.*Permission, we check that
        // the name is not null nor empty
	if (name == null) {
	    throw new NullPointerException("actions can't be null");
        }
	if (name.length() == 0) {
	    throw new IllegalArgumentException("actions can't be empty");
	}

        // handle the "*" wildcard meaning "all user names"
	if (name.equals("*")) {
	    return;
	}

        // future releases will support Database-scoped identities by
        // names of the form "userName@databaseName"; until then, however,
        // we throw an exception if we find a databaseName clause.
        for (int i = 0; i < name.length(); i++) {
            switch (name.charAt(i)) {
            case '\\' : {
                // ignore any escaped character
                i++;
                break;
            }
            case '*' : {
                // disallow unescaped special characters
                final String msg
                    = "unescaped '*' character not allowed in name";
                throw new IllegalArgumentException(msg);
            }
            case '@' : {
                // beginning of databaseName clause
                final String msg
                    = "unescaped '@' starting a databaseName not supported";
                throw new IllegalArgumentException(msg);
            }
            default:
                // ignore other character (including ' ')
            }
        }
    }

    /**
     * Compares this principal to the specified object. Returns true if
     * the object passed in matches the principal represented by the
     * implementation of this interface.
     *
     * @param other principal to compare with
     * @return true if the principal passed in is the same as that
     *         encapsulated by this principal, and false otherwise
     * @see Principal#equals
     */
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof DatabasePrincipal)) {
            return false;
        }
        final DatabasePrincipal that = (DatabasePrincipal)other;
        return this.getName().equals(that.getName());
    }

    /**
     * Returns the name of this principal.
     *
     * @return the name of this principal
     * @see Principal#getName()
     */
    public String getName() {
        return userName;
    }

    /**
     * Returns a hashcode for this principal.
     *
     * @return a hashcode for this principal
     * @see Principal#hashCode()
     */
    public int hashCode() {
        return getName().hashCode();
    }

    /**
     * Returns a string representation of this principal.
     *
     * @return a string representation of this principal
     * @see Principal#toString()
     */
    public String toString() {
        return this.getClass().getName() + "(" + getName() + ")";
    }
}
