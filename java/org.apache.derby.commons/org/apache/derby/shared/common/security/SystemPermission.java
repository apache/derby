/*

   Derby - Class org.apache.derby.shared.common.security.SystemPermission

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

package org.apache.derby.shared.common.security;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.BasicPermission;
import java.security.Permission;
import java.security.PermissionCollection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * This class represents access to system-wide Derby privileges.
 * <P>
  <table border = "1">
  <tr> <th>Permission <th>Description <th>Risk </tr>
  <tr> <th> "jmx" "control" <td> Controls the ability of JMX clients to control
  Derby and view security sensitive attributes through Derby's MBeans.
     <td> JMX clients may be able to change the state of the running system </tr>
  <tr> <th> "jmx" "monitor" <td> Controls the ability of JMX clients to
      monitor Derby through Derby's MBeans, such as viewing number of current connections and
      configuration settings. <em> Note: security related settings require</em> <code>control</code>
      <em>action on</em> <code>jmx</code> <td> JMX clients can see information about a runing system
      including software versions. </tr>
 </table>
 */
final public class SystemPermission extends BasicPermission {
    
    private static final long serialVersionUID = 1965420504091489898L;
    
    /**
     * Permission target name (<code>"server"</code>) for actions applicable
     * to the network server.
     */
    public static final String SERVER = "server";
    /**
     * Permission target name (<code>"engine"</code>) for actions applicable
     * to the core database engine.
     */
    public static final String ENGINE = "engine";
    /**
     * Permission target name (<code>"jmx"</code>) for actions applicable
     * to management of Derby's JMX MBeans.
     */
    public static final String JMX = "jmx";

    /**
     * The server and engine shutdown action (<code>"shutdown"</code>).
     */
    static public final String SHUTDOWN = "shutdown";
    
    /**
     * Action (<code>"control"</code>) to perform control actions through JMX
     * on engine, server or jmx.
     * <P>
     * For JMX control permission is required to get
     * attributes that are deemed sensiive from a security
     * aspect, such as the network server's port number,
     * security mechanisms and any information about the
     * file system.
     */
    public static final String CONTROL = "control";
    
    /**
     * Action (<code>"monitor"</code>) to perform monitoring actions through JMX
     * on engine and server.
     */
    public static final String MONITOR = "monitor";

    /**
     * Action (<code>"useDerbyInternals"</code>) by the engine to lookup Derby contexts.
     */
    public static final String USE_DERBY_INTERNALS = "usederbyinternals";

    /**
     * The legal system permission names.
     */
    static private final Set<String> LEGAL_NAMES = new HashSet<String>();    
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
    static private final List<String> LEGAL_ACTIONS = new ArrayList<String>();
    static {
        LEGAL_ACTIONS.add(CONTROL);
        LEGAL_ACTIONS.add(MONITOR);
        LEGAL_ACTIONS.add(SHUTDOWN);
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        LEGAL_ACTIONS.add( USE_DERBY_INTERNALS );
    }
    
    /** Constant representing {@code SystemPermission("engine, "monitor")}. */
    public static final SystemPermission ENGINE_MONITOR =
            new SystemPermission(ENGINE, MONITOR);
//IC see: https://issues.apache.org/jira/browse/DERBY-6733

    /**
     * Actions for this permission.
     */
    private String actions;

    /**
     * Bit mask representing the actions. It is not serialized, and has
     * to be recalculated when the object is deserialized.
     */
    private transient int actionMask;
    
    /**
     * Creates a new SystemPermission with the specified name.
     *
     * @param name the name of the SystemPermission
     * @throws NullPointerException if name or actions is null
     * @throws IllegalArgumentException if name is empty or not a legal SystemPermission
     * @see BasicPermission#BasicPermission(String)
     */
    public SystemPermission(String name, String actions) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        super( name );
        validateNameAndActions(name, actions);
    }

    /**
     * Check if name and actions are valid, normalize the actions string,
     * and calculate the actions mask.
     *
     * @param name the name of the permission
     * @param actions the actions of the permission
     * @throws NullPointerException if actions is null
     * @throws IllegalArgumentException if name is not a legal SystemPermission
     */
    private void validateNameAndActions(String name, String actions) {
        // superclass BasicPermission has checked that name isn't null
        // (NullPointerException) or empty (IllegalArgumentException)

        if (!LEGAL_NAMES.contains(name) ) {
            throw new IllegalArgumentException("Unknown permission " + name);
        }
      
        this.actions = getCanonicalForm(actions);
        this.actionMask = getActionMask(this.actions);
    }
    
    /**
     * Return the permission's actions in a canonical form.
     */
    public String getActions() {
        return actions;
    }

    // DERBY-6717: Must override newPermissionCollection() since
    // BasicPermission's implementation ignores actions.
    @Override
    public PermissionCollection newPermissionCollection() {
        return new SystemPermissionCollection();
    }
    
    /**
     * Return a canonical form of the passed in actions.
     * Actions are lower-cased, in the order of LEGAL_ACTIONS
     * and only appear once.
     */
    private static String getCanonicalForm(String actions) {
        Set<String> actionSet = parseActions(actions);

        // Get all the legal actions that are in actionSet, in the order
        // of LEGAL_ACTIONS.
        List<String> legalActions = new ArrayList<String>(LEGAL_ACTIONS);

        legalActions.retainAll(actionSet);

        return buildActionsString(legalActions);
    }

    /**
     * Get a set of all actions specified in a string. Actions are transformed
     * to lower-case, and leading and trailing blanks are stripped off.
     *
     * @param actions the specified actions string
     * @return a set of all the specified actions
     */
    public static Set<String> parseActions(String actions) {
        HashSet<String> actionSet = new HashSet<String>();
        for (String s : actions.split(",", -1)) {
            actionSet.add(s.trim().toLowerCase(Locale.ENGLISH));
        }
        return actionSet;
    }

    /**
     * Build a comma-separated actions string suitable for returning from
     * {@code getActions()}.
     *
     * @param actions the list of actions
     * @return comma-separated string with the actions
     */
    public static String buildActionsString(Iterable<String> actions) {
        StringBuilder sb = new StringBuilder();

        for (String action : actions) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(action);
        }

        return sb.toString();
    }

    /**
     * Does this permission equal another object.
     * True if its and identical class with same
     * name and (canonical) actions.
     */
    public boolean equals(Object other) {
        // Check if the types and names match.
        if (!super.equals(other))
            return false;

        // Check if the actions match.
        SystemPermission osp = (SystemPermission) other;
        return actionMask == osp.actionMask;
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
        // Check if the types and names match.
        if (!super.implies(permission))
            return false;

        // Check if the actions match.
        int myActionMask = actionMask;
        int permissionMask = ((SystemPermission) permission).actionMask;
        
        return
            (myActionMask & permissionMask) == permissionMask;
    }
    
    /**
     * Get a mask of bits that represents the actions
     * and can be used for the implies method.
     */
    private static int getActionMask(String actions) {
        
        int mask = 0;
//IC see: https://issues.apache.org/jira/browse/DERBY-3462
//IC see: https://issues.apache.org/jira/browse/DERBY-3491
        StringTokenizer st = new StringTokenizer(actions, ",");
        while (st.hasMoreTokens()) {
            int validAction = LEGAL_ACTIONS.indexOf(st.nextElement());
            if (validAction != -1)
                mask |= 1 << validAction;
        }
        
        return mask;
    }

    /**
     * Called upon deserialization for restoring the state of this
     * SystemPermission from a stream.
     */
    private void readObject(ObjectInputStream s)
         throws IOException, ClassNotFoundException {
        // Read the fields from the stream.
        s.defaultReadObject();

        // Make sure the name and actions fields contain legal values.
        validateNameAndActions(getName(), getActions());
    }

    /**
     * A collection of {@code SystemPermission} objects. Instances of this
     * class must be thread-safe and serializable, per the specification of
     * {@code java.security.PermissionCollection}.
     */
    private static class SystemPermissionCollection
                                extends PermissionCollection {
        private static final long serialVersionUID = 0L;

        private HashMap<String, Permission> permissions
                = new HashMap<String, Permission>();

        @Override
        public void add(Permission permission) {
            // The contract of PermissionCollection.add() requires
            // IllegalArgumentException if permission is not SystemPermission.
            if (!(permission instanceof SystemPermission)) {
                throw new IllegalArgumentException();
            }

            // The contract of PermissionCollection.add() requires
            // SecurityException if the collection is read-only.
            if (isReadOnly()) {
                throw new SecurityException();
            }

            String name = permission.getName();

            synchronized (this) {
                Permission existing = permissions.get(name);
                if (existing == null) {
                    permissions.put(name, permission);
                } else {
                    String actions = existing.getActions() + ','
                                        + permission.getActions();
                    permissions.put(name, new SystemPermission(name, actions));
                }
            }
        }

        @Override
        public boolean implies(Permission permission) {
            if (!(permission instanceof SystemPermission)) {
                return false;
            }

            String name = permission.getName();
            Permission perm;

            synchronized (this) {
                perm = permissions.get(name);
            }

            return (perm != null) && perm.implies(permission);
        }

        @Override
        public synchronized Enumeration<Permission> elements() {
            return Collections.enumeration(permissions.values());
        }

        /**
         * Called upon Serialization for saving the state of this
         * SystemPermissionCollection to a stream.
         */
        private void writeObject(ObjectOutputStream s)
                throws IOException {
            // Only the values of the HashMap need to be serialized.
            // The keys can be reconstructed from the values during
            // deserialization.
            ArrayList<Permission> perms;
            synchronized (this) {
                perms = new ArrayList<Permission>(permissions.values());
            }

            ObjectOutputStream.PutField fields = s.putFields();
            fields.put("permissions", perms);
            s.writeFields();
        }

        /**
         * Called upon deserialization for restoring the state of this
         * SystemPermissionCollection from a stream.
         */
        private void readObject(ObjectInputStream s)
                throws IOException, ClassNotFoundException {
            ObjectInputStream.GetField fields = s.readFields();
            List perms = (List) fields.get("permissions", null);

            permissions = new HashMap<String, Permission>();

            // Insert the permissions one at a time, and verify that they
            // in fact are SystemPermissions by doing an explicit cast. If
            // a corrupted stream contains other kinds of permissions, a
            // ClassCastException is raised instead of returning an invalid
            // collection.
            for (Object p : perms) {
                SystemPermission sp = (SystemPermission) p;
                permissions.put(sp.getName(), sp);
            }
        }
    }

    @Override
    public String toString()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return getClass().getName() +
            "( " +
            doubleQuote( getName() ) +
            ", " +
            doubleQuote( actions ) +
            " )";
    }
    private String  doubleQuote( String raw )
    {
        return "\"" + raw + "\"";
    }
}
