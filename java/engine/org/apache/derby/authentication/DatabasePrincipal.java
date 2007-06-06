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
 */
public class DatabasePrincipal implements Principal, Serializable {

    /**
     * The name of the principal.
     */
    private final String userName;

    /**
     * Constructs a principal with the specified name.
     *
     * @param userName the name of the principal
     */
    public DatabasePrincipal(String userName) {
        this.userName = userName;
    }

    /**
     * Compares this principal to the specified object. Returns true if
     * the object passed in matches the principal represented by the
     * implementation of this interface.
     *
     * @param other principal to compare with
     * @return true if the principal passed in is the same as that
     *         encapsulated by this principal, and false otherwise
     * @see Principal.equal()
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
     * @see Principal.getName()
     */
    public String getName() {
        return userName;
    }

    /**
     * Returns a hashcode for this principal.
     *
     * @return a hashcode for this principal
     * @see Principal.hashCode()
     */
    public int hashCode() {
        return getName().hashCode();
    }

    /**
     * Returns a string representation of this principal.
     *
     * @return a string representation of this principal
     * @see Principal.toString()
     */
    public String toString() {
        return this.getClass().getName() + "(" + getName() + ")";
    }
}
