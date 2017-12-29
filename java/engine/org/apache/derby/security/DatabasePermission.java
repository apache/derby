/*

   Derby - Class org.apache.derby.security.DatabasePermission

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

import java.security.Permission;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import java.security.AccessController;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.derby.shared.common.security.SystemPermission;

/**
 * This class represents access to database-scoped privileges.
 *
 * An example of database-scoped privileges is the permission to create
 * a database under a specified directory path.
 * <p>
 * A DatabasePermission is defined by two string attributes, similar to
 * a java.io.FilePermission:
 * <ul>
 * <li> <i>URL</i> - a location description of or for a Derby database
 * <li> <i>Actions</i> - a list of granted administrative actions
 * </ul>
 * The database location URL may contain certain wildcard characters.
 * The currently only supported database action is <i>create</i>.
 *
 * @see DatabasePermission#DatabasePermission(String,String)
 * @see SystemPermission
 * @see java.io.FilePermission
 */
final public class DatabasePermission extends Permission {

    /**
     * The URL protocol scheme specifying a directory location.
     */
    static public final String URL_PROTOCOL_DIRECTORY = "directory:";

    /**
     * The location text matching any database anywhere.
     */
    static public final String URL_PATH_INCLUSIVE_STRING = "<<ALL FILES>>";

    /**
     * The path type character matching any database anywhere.
     */
    static public final char URL_PATH_INCLUSIVE_CHAR = 'I';

    /**
     * The URL file path separator character.
     */
    static public final char URL_PATH_SEPARATOR_CHAR = '/';

    /**
     * The relative path character.
     */
    static public final char URL_PATH_RELATIVE_CHAR = '.';

    /**
     * The wildcard character matching any database in a directory.
     */
    static public final char URL_PATH_WILDCARD_CHAR = '*';

    /**
     * The wildcard character matching any database under a directory
     * or its subdirectories.
     */
    static public final char URL_PATH_RECURSIVE_CHAR = '-';

    // derived path type constants
    static public final String URL_PATH_SEPARATOR_STRING
        = String.valueOf(URL_PATH_SEPARATOR_CHAR);
    static public final String URL_PATH_RELATIVE_STRING
        = String.valueOf(URL_PATH_RELATIVE_CHAR);
    static public final String URL_PATH_RELATIVE_PREFIX
        = (URL_PATH_RELATIVE_STRING + URL_PATH_SEPARATOR_CHAR);
    static public final String URL_PATH_WILDCARD_STRING
        = String.valueOf(URL_PATH_WILDCARD_CHAR);
    static public final String URL_PATH_WILDCARD_SUFFIX
        = (URL_PATH_SEPARATOR_STRING + URL_PATH_WILDCARD_CHAR);
    static public final String URL_PATH_RECURSIVE_STRING
        = String.valueOf(URL_PATH_RECURSIVE_CHAR);
    static public final String URL_PATH_RECURSIVE_SUFFIX
        = (URL_PATH_SEPARATOR_STRING + URL_PATH_RECURSIVE_CHAR);

    /**
     * The create database permission.
     */
    static public final String CREATE = "create";

    /**
     * The legal database permission action names.
     */
    static protected final List<String> LEGAL_ACTIONS = new ArrayList<String>();
    static {
        // when adding new actions, check: implies(Permission), getActions()
        LEGAL_ACTIONS.add(CREATE);
    };

    /**
     * The actions of this permission, as returned by {@link #getActions()}.
     */
    private String actions;

    /**
     * This permission's canonical directory path.
     *
     * The path consists of a canonicalized form of the user-specified URL,
     * stripped off the protocol specification and any recursive/wildcard
     * characters, or {@code "<<ALL FILES>>"} for the "anywhere" permission.
     * The canonical path is used when testing permissions with implies(),
     * where real directory locations, not just notational differences,
     * ought to be compared.  Analog to java.io.FilePermission, the
     * canonical path is also used by equals() and hashCode() to support
     * hashing and mapping of permissions by their real directory locations.
     *
     * Because canonical file paths are platform dependent, this field
     * must not be serialized (hence transient) but be recomputed from
     * the original URL upon deserialization.
     */
    private transient String path;

    /**
     * The parent directory of this permission's canonical directory path,
     * or null if this permission's path does not have a parent directory.
     *
     * Because canonical file paths are platform dependent, this field
     * must not be serialized (hence transient) but be recomputed from
     * the original URL upon deserialization.
     */
    private transient String parentPath;

    /**
     * Indicates whether the path denotes an inclusive, recursive, wildcard,
     * or single location.
     *
     * If the path denotes an inclusive, recursive or wildcard location,
     * this field's value is URL_PATH_INCLUSIVE_CHAR, URL_PATH_RECURSIVE_CHAR,
     * or URL_PATH_WILDCARD_CHAR, respectively; otherwise, it's
     * URL_PATH_SEPARATOR_CHAR denoting a single location.
     *
     * This field gets recomputed upon deserialization.
     */
    private transient char pathType;

    /**
     * Creates a new DatabasePermission with the specified URL and actions.
     * <P>
     * <i>actions</i> contains a comma-separated list of the desired actions
     * granted on a database. Currently, the only supported action is
     * <code>create</code>.
     * <P>
     * <i>URL</i> denotes a database location URL, which, at this time, must
     * start with <code>directory:</code> followed by a directory pathname.
     * Note that in a URL, the separator character is always "/" rather than
     * the file separator of the operating-system.  The directory path may
     * be absolute or relative, in which case it is prefixed with the current
     * user directory. In addition, similar to java.io.FilePermission, the
     * directory pathname may end with a wildcard character to allow for
     * arbitrarily named databases under a path:
     * <ul>
     * <li> "directory:location" - refers to a database called
     *      <i>location</i>,
     * <li> "directory:location/*" - matches any database in the
     *      directory <i>location</i>,
     * <li> "directory:location/-" - matches any database under
     *      <i>location</i> or its subdirectories.
     * <li> "directory:*" - matches any database in the user's current
     *      working directory.
     * <li> "directory:-" - matches any database under the
     *      user's current working directory or its subdirectories.
     * <li> {@code "directory:<<ALL FILES>>"} matches any database anywhere.
     * </ul>
     *
     * @param url the database URL
     * @param actions the action string
     * @throws NullPointerException if an argument is null
     * @throws IllegalArgumentException if an argument is not legal
     * @throws IOException if the location URL cannot be canonicalized
     * @see Permission#Permission(String)
     * @see java.io.FilePermission#FilePermission(String,String)
     */
    public DatabasePermission(String url, String actions)
        throws IOException {
        super(url);
        initActions(actions);
        initLocation(url);
    }

    /**
     * Parses the list of database actions.
     *
     * @param actions the comma-separated action list
     * @throws NullPointerException if actions is null
     * @throws IllegalArgumentException if not a list of legal actions
     */
    protected void initActions(String actions) {
        // analog to java.security.BasicPermission, we check that actions
        // is not null nor empty
        if (actions == null) {
            throw new NullPointerException("actions can't be null");
        }
        if (actions.length() == 0) {
            throw new IllegalArgumentException("actions can't be empty");
        }

        // Get all the actions specified in the actions string
        Set<String> actionSet = SystemPermission.parseActions(actions);

        // check for any illegal actions
        for (String action : actionSet) {
            if (!LEGAL_ACTIONS.contains(action)) {
                // report illegal action
                final String msg = "Illegal action '" + action + "'";
                throw new IllegalArgumentException(msg);
            }
        }

        // Get all the legal actions that are in actionSet, in the order
        // of LEGAL_ACTIONS.
        List<String> legalActions = new ArrayList<String>(LEGAL_ACTIONS);
        legalActions.retainAll(actionSet);

        this.actions = SystemPermission.buildActionsString(legalActions);
    }

    /**
     * Parses the database location URL.
     *
     * @param url the database URL
     * @throws NullPointerException if the URL is null
     * @throws IllegalArgumentException if the URL is not well-formed
     * @throws IOException if the location URL cannot be canonicalized
     */
    protected void initLocation(String url)
        throws IOException {
        // analog to java.security.BasicPermission, we check that URL
        // is not null nor empty
        if (url == null) {
            throw new NullPointerException("URL can't be null");
        }
        if (url.length() == 0) {
            throw new IllegalArgumentException("URL can't be empty");
        }

        // check URL's protocol scheme and initialize path
        if (!url.startsWith(URL_PROTOCOL_DIRECTORY)) {
            final String msg = "Unsupported protocol in URL '" + url + "'";
            throw new IllegalArgumentException(msg);
        }
        String p = url.substring(URL_PROTOCOL_DIRECTORY.length());

        // check path for inclusive/relative/recursive/wildcard specifications,
        // split path into real pathname and the path type
        if (p.equals(URL_PATH_INCLUSIVE_STRING)) {
            // inclusive:  "<<ALL FILES>>" --> 'I', "<<ALL FILES>>"
            pathType = URL_PATH_INCLUSIVE_CHAR;
            // p = p;
        } else if (p.equals(URL_PATH_RECURSIVE_STRING)) {
            // relative & recursive:  "-" --> '-', "./"
            pathType = URL_PATH_RECURSIVE_CHAR;
            p = URL_PATH_RELATIVE_PREFIX;
        } else if (p.equals(URL_PATH_WILDCARD_STRING)) {
            // relative & wildcard:   "*" --> '*', "./"
            pathType = URL_PATH_WILDCARD_CHAR;
            p = URL_PATH_RELATIVE_PREFIX;
        } else if (p.endsWith(URL_PATH_RECURSIVE_SUFFIX)) {
            // absolute & recursive:  "<path>/-" --> '-', "<path>/"
            pathType = URL_PATH_RECURSIVE_CHAR;
            p = p.substring(0, p.length() - 1);
        } else if (p.endsWith(URL_PATH_WILDCARD_SUFFIX)) {
            // absolute & wildcard:   "<path>/*" --> '*', "<path>/"
            pathType = URL_PATH_WILDCARD_CHAR;
            p = p.substring(0, p.length() - 1);
        } else {
            // absolute | relative:   "<path>" --> '/', "<path>"
            pathType = URL_PATH_SEPARATOR_CHAR;
            // p = p;
        }

        // canonicalize the path and assign parentPath
        if (pathType == URL_PATH_INCLUSIVE_CHAR) {
            path = URL_PATH_INCLUSIVE_STRING;
            //assert(parentPath == null);
        } else {
            // resolve against user's working directory if relative pathname;
            // the read access to the system property is encapsulated in a
            // doPrivileged() block to allow for confined codebase permission
            // grants
            if (p.startsWith(URL_PATH_RELATIVE_PREFIX)) {
                final String cwd = AccessController.doPrivileged(
                    new PrivilegedAction<String>() {
                        public String run() {
                            return System.getProperty("user.dir");
                        }
                    });
                // concatenated path "<cwd>/./<path>" will be canonicalized
                p = cwd + URL_PATH_SEPARATOR_STRING + p;
            }
            final String absPath = p;

            // store canonicalized path as required for implies(Permission);
            // may throw IOException; canonicalization reads the "user.dir"
            // system property, which we encapsulate in a doPrivileged()
            // block to allow for confined codebase permission grants
            final File f;
            try {
                f = AccessController.doPrivileged(
                    new PrivilegedExceptionAction<File>() {
                        public File run() throws IOException {
                            return (new File(absPath)).getCanonicalFile();
                        }
                    });
            } catch (PrivilegedActionException pae) {
                // pae.getCause() should be an instance of IOException,
                // as only checked exceptions will be wrapped
                throw (IOException)pae.getCause();
            }
            path = f.getPath();

            // store canonicalized path of parent file as required for
            // implies(Permission); may throw IOException; note that
            // the path already denotes parent directory if of wildcard type:
            // for example, the parent of "/a/-" or "/a/*" is "/a"
            parentPath = ((pathType != URL_PATH_SEPARATOR_CHAR)
                          ? path : f.getParent());
        }

        //assert(pathType == URL_PATH_SEPARATOR_CHAR
        //       || pathType == URL_PATH_WILDCARD_CHAR
        //       || pathType == URL_PATH_RECURSIVE_CHAR
        //       || pathType == URL_PATH_INCLUSIVE_CHAR);
        //assert(path != null);
        //assert(parentPath == null || parentPath != null);
    }

    /**
     * Checks if this DatabasePermission implies a specified permission.
     * <P>
     * This method returns true if:<p>
     * <ul>
     * <li> <i>p</i> is an instanceof DatabasePermission and<p>
     * <li> <i>p</i>'s directory pathname is implied by this object's
     *      pathname. For example, "/tmp/*" implies "/tmp/foo", since
     *      "/tmp/*" encompasses the "/tmp" directory and all files in that
     *      directory, including the one named "foo".
     * </ul>
     * @param p the permission to check against
     * @return true if the specified permission is implied by this object,
     * false if not
     * @see Permission#implies(Permission)
     */
    public boolean implies(Permission p) {
        // can only imply other DatabasePermissions
        if (!(p instanceof DatabasePermission)) {
            return false;
        }
        final DatabasePermission that = (DatabasePermission)p;

        // an inclusive permission implies any other
        if (this.pathType == URL_PATH_INCLUSIVE_CHAR) {
            return true;
        }
        //assert(this.pathType != URL_PATH_INCLUSIVE_CHAR);

        // a non-inclusive permission cannot imply an inclusive one
        if (that.pathType == URL_PATH_INCLUSIVE_CHAR) {
            return false;
        }
        //assert(that.pathType != URL_PATH_INCLUSIVE_CHAR);

        // a recursive permission implies any other if a path prefix
        if (this.pathType == URL_PATH_RECURSIVE_CHAR) {
            return (that.parentPath != null
                    && that.parentPath.startsWith(this.path));
        }
        //assert(this.pathType != URL_PATH_RECURSIVE_CHAR);

        // a non-recursive permission cannot imply a recursive one
        if (that.pathType == URL_PATH_RECURSIVE_CHAR) {
            return false;
        }
        //assert(that.pathType != URL_PATH_RECURSIVE_CHAR);

        // a wildcard permission implies another if a parent directory
        if (this.pathType == URL_PATH_WILDCARD_CHAR) {
            return this.path.equals(that.parentPath);
        }
        //assert(this.pathType != URL_PATH_WILDCARD_CHAR);

        // a non-wildcard permission cannot imply a wildcard one
        if (that.pathType == URL_PATH_WILDCARD_CHAR) {
            return false;
        }
        //assert(that.pathType != URL_PATH_WILDCARD_CHAR);

        // non-recursive, non-wildcard permissions imply when paths are equal
        //assert(this.pathType == URL_PATH_SEPARATOR_CHAR);
        //assert(that.pathType == URL_PATH_SEPARATOR_CHAR);
        return this.path.equals(that.path);
    }

    /**
     * Checks two DatabasePermission objects for equality.
     * <P>
     * Checks that <i>obj</i> is a DatabasePermission and has the same
     * canonizalized URL and actions as this object.
     * <P>
     * @param obj the object we are testing for equality with this object
     * @return true if obj is a DatabasePermission, and has the same URL and
     * actions as this DatabasePermission object, false if not
     *
     * @see Permission#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof DatabasePermission)) {
            return false;
        }
        final DatabasePermission that = (DatabasePermission)obj;

        // compare canonicalized URLs
        return (pathType == that.pathType && path.equals(that.path));
    }

    /**
     * Returns the hash code value for this object.
     *
     * @return a hash code value for this object
     * @see Permission#hashCode()
     */
    public int hashCode() {
        // hash canonicalized URL
        return (path.hashCode() ^ pathType);
    }

    /**
     * Returns the "canonical string representation" of the actions.
     *
     * @return the canonical string representation of the actions
     * @see Permission#getActions()
     */
    public String getActions() {
        return actions;
    }


    /**
     * Called upon Serialization for saving the state of this
     * DatabasePermission to a stream.
     */
    private void writeObject(ObjectOutputStream s)
        throws IOException {
        // write the non-static and non-transient fields to the stream
        s.defaultWriteObject();
    }

    /**
     * Called upon Deserialization for restoring the state of this
     * DatabasePermission from a stream.
     */
    private void readObject(ObjectInputStream s)
         throws IOException, ClassNotFoundException
    {
        // read the non-static and non-transient fields from the stream
        s.defaultReadObject();

        // Validate the URL read from the object stream, and
        // restore the platform-dependent path from the original URL
        initLocation(getName());

        // Validate and normalize the actions read from the stream.
        initActions(getActions());
    }
}
