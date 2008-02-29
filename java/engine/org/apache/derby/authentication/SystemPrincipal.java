/*

   Derby - Class org.apache.derby.authentication.SystemPrincipal

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
 * user identity with controlled access to Derby System Privileges.
 * An authenticated user may have other identities which make sense in
 * other code domains.
 * <p>
 * Note that principal names do NOT follow Authorization Identifier rules.
 * For instance, although edward and edWard both match the normalized
 * authorization identifier EDWARD, the instances
 * <code>SystemPrincipal("edward")</code> and
 * <code>SystemPrincipal("edWard")</code> represent different principals
 * under the methods <code>getName()</code>, <code>equals()</code>, and
 * <code>hashCode()</code>.
 * <p>
 * According to JAASRefGuide, Principal classes must implement Serializable.
 *
 * @see Principal#name
 * @see <a href="http://java.sun.com/javase/6/docs/technotes/guides/security/jaas/JAASRefGuide.html#Principals">JAASRefGuide on Principals</a> 
 */
final public class SystemPrincipal implements Principal, Serializable {

    /**
     * BTW, this class currently does not require special handling during
     * serialization/deserialization, so, there's no need to define methods
     * <code>readObject(ObjectInputStream)</code> and 
     * <code>writeObject(ObjectOutputStream)</code>.
     */
    static final long serialVersionUID = 925380094921530190L;

    /**
     * The name of the principal.
     * <p>
     * Note that the name is not a "normalized" Authorization Identifier.
     * This is due to peculiarities of the Java Security Runtime, which
     * compares a <code>javax.security.auth.Subject</code>'s Principals
     * against the literal Principal name as declared in the policy files,
     * and not against the return value of method <code>getName()</code>.
     * So, a normalization of names within SystemPrincipal doesn't affect
     * permission checking by the SecurityManager.
     * <p>
     * In order for a <code>javax.security.auth.Subject</code> to be
     * granted permissions on the basis Authorization Identifier rules, e.g.,
     * for a Subject authenticated as edWard to fall under a policy clause
     * declared for EDWARD, the Subject has to be constructed (or augmented)
     * with both the literal name and the normalized Authorization Identifier.
     * <p>
     * As an alternative approach, class <code>SystemPrincipal</code> could
     * implement the non-standard interface
     * <code>com.sun.security.auth.PrincipalComparator</code>, which declares
     * a method <code>implies(Subject)<code> that would allow for Principals
     * to match Subjects on the basis of normalized Authorization Identifiers.
     * But then we'd be relying upon non-standard Security Runtime behaviour.
     *
     * @see <a href="http://wiki.apache.org/db-derby/UserIdentifiers">User Names & Authorization Identifiers in Derby</a>
     */
    private final String name;

    /**
     * Constructs a principal for a given name.
     *
     * @param name the name of the principal
     * @throws NullPointerException if name is null
     * @throws IllegalArgumentException if name is not a legal Principal name
     */
    public SystemPrincipal(String name) {
        // RuntimeException messages not localized
        if (name == null) {
            throw new NullPointerException("name can't be null");
        }
        if (name.length() == 0) {
            throw new IllegalArgumentException("name can't be empty");
        }
        this.name = name;
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
        if (!(other instanceof SystemPrincipal)) {
            return false;
        }
        final SystemPrincipal that = (SystemPrincipal)other;
        return name.equals(that.name);
    }

    /**
     * Returns the name of this principal.
     *
     * @return the name of this principal
     * @see Principal#getName()
     */
    public String getName() {
        return name;
    }

    /**
     * Returns a hashcode for this principal.
     *
     * @return a hashcode for this principal
     * @see Principal#hashCode()
     */
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * Returns a string representation of this principal.
     *
     * @return a string representation of this principal
     * @see Principal#toString()
     */
    public String toString() {
        return getClass().getName() + "(" + name + ")";
    }
}
