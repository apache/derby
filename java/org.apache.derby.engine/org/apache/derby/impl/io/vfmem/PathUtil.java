/*

   Derby - Class org.apache.derby.impl.io.vfmem.PathUtil

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

package org.apache.derby.impl.io.vfmem;

import java.io.File;

/**
 * Helper methods to deal with paths in the in-memory "file system".
 * <p>
 * These methods are similar to those in {@code java.io.File}.
 * <p>
 * <em>Note</em>: The system has been hardcoded to use the separator specified
 * by {@code java.io.File}.
 */
public class PathUtil {

    public static final char SEP = File.separatorChar;
    public static final String SEP_STR = String.valueOf(SEP);

    /** This class cannot be instantiated. */
    private PathUtil() {}

    private static void basicPathChecks(String path) {
        if (path == null) {
            throw new IllegalArgumentException("Path is null");
        }
        if (!path.equals(path.trim())) {
            throw new IllegalArgumentException("Path has not been trimmed: '" +
                    path + "'");
        }
    }

    /**
     * Returns the base name of the path.
     *
     * @param path the path to process
     * @return The base name of the path.
     */
    public static String getBaseName(String path) {
        basicPathChecks(path);
        int sepIndex = path.lastIndexOf(SEP);
        if (sepIndex != -1 && sepIndex != path.length() -1) {
            return path.substring(sepIndex +1);
        }
        return path;
    }

    /**
     * Returns the parent of the path.
     *
     * @param path the path to process
     * @return The parent path, which may be the empty string ({@code ""}) if
     *      the path is a relative path, or {@code null} if XXXX TODO
     */
    public static String getParent(String path) {
        basicPathChecks(path);
        if (path.equals(SEP_STR)) {
            return null;
        }
        // Remove the last separator, if it is the last char of the path.
        if (path.length() > 0 && path.charAt(path.length() -1) == SEP) {
            path = path.substring(0, path.length() -1);
        }
        // Look for the last separator.
        int sepIndex = path.lastIndexOf(SEP);
        if (sepIndex == 0) {
            return SEP_STR;
        } else if (sepIndex > 0) {
            return path.substring(0, sepIndex);
        } else {
            return null;
        }
    }

    /**
     * Joins the two paths by inserting the separator chararcter between them.
     *
     * @param parent parent directory
     * @param base file/directory name
     * @return A merged path.
     */
    public static String join(String parent, String base) {
        // It is not defined what happens if the base name starts with the
        // separator character. For now, just let it be, which will result in a
        // path with multiple separator chars next to eachother.
        if (parent.charAt(parent.length() -1) == SEP) {
            return parent + base;
        }
        return (parent + SEP + base);
    }
}
