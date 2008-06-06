/*

   Derby - Class org.apache.derby.iapi.util.PrivilegedFileOps

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
package org.apache.derby.iapi.util;

import java.io.File;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * A collection of operations on {$@link java.io.File} that wraps the
 * operations in privileged block of code.
 * <p>
 * Derby needs to use privileged blocks in some places to avoid
 * {@link SecurityException}s being thrown, as the required privileges are
 * often granted to Derby itself, but not the higher level application code.
 * <p>
 * Feel free to add new operations as they are needed. This class is not
 * intended to implement the full set of operations defined by
 * {@link java.io.File}.
 */
public final class PrivilegedFileOps {

    /**
     * Check if the file exists.
     *
     * @return <code>true</code> if file exists, <code>false</code> otherwise
     * @throws SecurityException if the required permissions to read the file,
     *      or the path it is in, are missing
     * @see File#exists
     */
    public static boolean exists(final File file)
            throws SecurityException {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        try {
            return ((Boolean)AccessController.doPrivileged(
                        new PrivilegedExceptionAction() {
                            public Object run() throws SecurityException {
                                return new Boolean(file.exists());
                            }
                        })).booleanValue();
        } catch (PrivilegedActionException pae) {
            throw (SecurityException)pae.getException();
        }
    }

    /**
     * Check if the pathname is a directory.
     *
     * @return <code>true</code> if pathname points to a directory,
     *      <code>false</code> otherwise
     * @throws SecurityException if the required permissions to access the path
     *      are missing
     * @see File#isDirectory
     */
    public static boolean isDirectory(final File file)
            throws SecurityException {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        try {
            return ((Boolean)AccessController.doPrivileged(
                        new PrivilegedExceptionAction() {
                            public Object run() throws SecurityException {
                                return new Boolean(file.isDirectory());
                            }
                        })).booleanValue();
        } catch (PrivilegedActionException pae) {
            throw (SecurityException)pae.getException();
        }
    }

    /**
     * Return a list of strings denoting the contents of the given directory.
     * <p>
     * Note the <code>null</code> is returned if a non-directory path is passed
     * to this method.
     *
     * @param directory the directory to list the contents of
     * @return A list of the contents in the directory. If
     *      <code>directory</code> is not denoting a directory, <code>null<code>
     *      is returned (as per {@link File#list}).
     * @throws SecurityException if the required permissions to access the path
     *      are missing
     * @see File#list
     */
    public static String[] list(final File directory)
            throws SecurityException {
        if (directory == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        try {
            return (String[])AccessController.doPrivileged(
                        new PrivilegedExceptionAction() {
                            public Object run() throws SecurityException {
                                return directory.list();
                            }
                        });
        } catch (PrivilegedActionException pae) {
            throw (SecurityException)pae.getException();
        }
    }
    
    /**
     * Creates the directory named by this abstract pathname and
     * parent directories
     * 
     * @param file   directory to create
     * @return  <code> true </true> if directory was created.
     */
    public static boolean mkdirs(final File file) {
     
        if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        try {
            return ((Boolean) AccessController.doPrivileged(
                        new PrivilegedExceptionAction() {
                            public Object run() throws SecurityException {
                                return new Boolean(file.mkdirs());
                            }
                        })).booleanValue();
        } catch (PrivilegedActionException pae) {
            throw (SecurityException)pae.getException();
        }
    }
    
} // End class PrivilegedFileOps
