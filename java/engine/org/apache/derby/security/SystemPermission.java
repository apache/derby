/*

   Derby - Class org.apache.derby.security.SystemPermission

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

package org.apache.derby.security;

import java.security.BasicPermission;
import java.util.Set;
import java.util.HashSet;

/**
 * This class represents access to system-wide Derby privileges.
 */
public class SystemPermission extends BasicPermission {

    /**
     * The engine shutdown permission.
     */
    static public final String SHUTDOWN = "shutdownEngine";

    /**
     * The legal system permission names.
     */
    static protected final Set LEGAL_PERMISSIONS = new HashSet();    
    static {
        // when adding new permissions, check whether to override inherited
        // method: implies(Permission)
        LEGAL_PERMISSIONS.add(SHUTDOWN);
    };

    /**
     * Checks a name for denoting a legal SystemPermission.
     *
     * @param name the name of a SystemPermission
     * @throws IllegalArgumentException if name is not a legal SystemPermission
     */
    static protected void checkPermission(String name) {
        // superclass BasicPermission has checked that name isn't null
        // (NullPointerException) or empty (IllegalArgumentException)
        //assert(name != null);
        //assert(!name.equals(""));

        // note that exception messages on the name aren't localized,
        // as is the general rule with runtime exceptions indicating
        // internal coding errors
        if (!LEGAL_PERMISSIONS.contains(name)) {
            throw new IllegalArgumentException("Unknown permission " + name);
        }
    }
    
    /**
     * Creates a new SystemPermission with the specified name.
     *
     * @param name the name of the SystemPermission
     * @throws NullPointerException if name is null
     * @throws IllegalArgumentException if name is empty or not a legal SystemPermission
     * @see BasicPermission(String)
     */
    public SystemPermission(String name) {
        super(name);
        checkPermission(name);
    }
}
