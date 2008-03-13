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
import java.security.Permission;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * This class represents access to system-wide Derby privileges.
 */
final public class SystemPermission extends BasicPermission {
    
    /**
     * Permission target name for actions applicable
     * to the network server.
     */
    public static final String SERVER = "server";
    /**
     * Permission target name for actions applicable
     * to the core database engine.
     */
    public static final String ENGINE = "engine";
    /**
     * Permission target name for actions applicable
     * to management of Derby's JMX MBeans.
     */
    public static final String JMX = "jmx";

    /**
     * The server and engine shutdown action.
     */
    static public final String SHUTDOWN = "shutdown";
    
    /**
     * Permission to perform control actions through JMX
     * on engine, server or jmx.
     */
    public static final String CONTROL = "control";
    
    /**
     * Permission to perform monitoring actions through JMX
     * on engine and server.
     */
    public static final String MONITOR = "monitor";

    /**
     * The legal system permission names.
     */
    static private final Set LEGAL_NAMES = new HashSet();    
    static {
        // when adding new permissions, check whether to override inherited
        // method: implies(Permission)
        LEGAL_NAMES.add(SERVER);
        LEGAL_NAMES.add(ENGINE);
        LEGAL_NAMES.add(JMX);
    };
    
    /**
     * Set of legal actions in their canonical form.
     */
    static private final List LEGAL_ACTIONS = new ArrayList();
    static {
        LEGAL_ACTIONS.add(CONTROL);
        LEGAL_ACTIONS.add(MONITOR);
        LEGAL_ACTIONS.add(SHUTDOWN);
    }
    
    /**
     * Actions for this permission.
     */
    private final String actions;
    
    /**
     * Creates a new SystemPermission with the specified name.
     *
     * @param name the name of the SystemPermission
     * @throws NullPointerException if name is null
     * @throws IllegalArgumentException if name is empty or not a legal SystemPermission
     * @see BasicPermission#BasicPermission(String)
     */
    public SystemPermission(String name, String actions) {
        super(name);
            
        // superclass BasicPermission has checked that name isn't null
        // (NullPointerException) or empty (IllegalArgumentException)

        if (!LEGAL_NAMES.contains(name) ) {
            throw new IllegalArgumentException("Unknown permission " + name);
        }
      
        this.actions = getCanonicalForm(actions);   
    }
    
    /**
     * Return the permission's actions in a canonical form.
     */
    public String getActions() {
        return actions;
    }
    
    /**
     * Return a canonical form of the passed in actions.
     * Actions are lower-cased, in the order of LEGAL_ACTIONS
     * and on;ly appear once.
     */
    private static String getCanonicalForm(String actions) {
        actions = actions.trim().toLowerCase(Locale.ENGLISH);
        
        boolean[] seenAction = new boolean[LEGAL_ACTIONS.size()];
        StringTokenizer st = new StringTokenizer(actions, ",");
        while (st.hasMoreTokens()) {
            String action = st.nextToken().trim().toLowerCase(Locale.ENGLISH);
            int validAction = LEGAL_ACTIONS.indexOf(action);
            if (validAction != -1)
                seenAction[validAction] = true;
        }
        
        StringBuffer sb = new StringBuffer();
        for (int sa = 0; sa < seenAction.length; sa++)
        {
            if (seenAction[sa]) {
                if (sb.length() != 0)
                    sb.append(",");
                sb.append(LEGAL_ACTIONS.get(sa));
            }
        }
        
        return sb.toString();
    }

    /**
     * Does this permission equal another object.
     * True if its and identical class with same
     * name and (canonical) actions.
     */
    public boolean equals(Object other) {
        
        if (!super.equals(other))
            return false;
        
        SystemPermission osp = (SystemPermission) other;
        return getActions().equals(osp.getActions());
    }
    
    /**
     * Does this permission imply another. Only true
     * if the other permission is a SystemPermission
     * with the same name and all the actions
     * of the permission are present in this.
     * Note that none of the actions imply any other
     * with this SystemPermission.
     */
    public boolean implies(Permission permission)
    {
        if (!super.implies(permission))
            return false;
        
        int myActionMask = getActionMask(getActions());
        int permissionMask = getActionMask(permission.getActions());
        
        return
            (myActionMask & permissionMask) == permissionMask;
    }
    
    /**
     * Get a mask of bits that represents the actions
     * and can be used for the implies method.
     */
    private static int getActionMask(String actions) {
        
        int mask = 0;
        StringTokenizer st = new StringTokenizer(actions, ",");
        while (st.hasMoreTokens()) {
            int validAction = LEGAL_ACTIONS.indexOf(st.nextElement());
            if (validAction != -1)
                mask |= 1 << validAction;
        }
        
        return mask;
    }
    
    
}
