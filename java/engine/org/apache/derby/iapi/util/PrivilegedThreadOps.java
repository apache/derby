/*

   Derby - Class org.apache.derby.iapi.util.PrivilegedThreadOps

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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * A collection of operations on {@link java.lang.Thread} that wraps the
 * operations in privileged block of code.
 * <p>
 * Derby needs to use privileged blocks in some places to avoid
 * {@link SecurityException}s being thrown, as the required privileges are
 * often granted to Derby itself, but not the higher level application code.
 * <p>
 * Feel free to add new operations as they are needed. This class is not
 * intended to implement the full set of operations defined by
 * {@link java.lang.Thread}.
 */

public class PrivilegedThreadOps {

    /**
     * Sets the context ClassLoader for this Thread. The context ClassLoader 
     * can be set when a thread is created, and allows the creator of the 
     * thread to provide the appropriate class loader to code running in 
     * the thread when loading classes 
     * and resources.
     * 
     * First, if there is a security manager, its <code> checkPermission </code>
     * method is called with a 
     * <code> RuntimePermission("setContextClassLoader") </code> permission 
     * to see if it's ok to set the context ClassLoader.. 
     * @param t  Thread for which we are setting the context class loader
     * @param cl the context class loader for t
     */
    public static void setContextClassLoader(final Thread t, final ClassLoader cl) {
            AccessController.doPrivileged(
                        new PrivilegedAction() {
                            public Object run()  {
                                t.setContextClassLoader(cl);
                                return null;
                            }
                        });
    }
    
    /**
     * Set the thread's context class loader if privileged.  If not ignore 
     * security exception and continue. 
     * @param t  Thread for which we are setting the context class loader
     * @param cl the context class loader for t
     */
    public static void setContextClassLoaderIfPrivileged(Thread t, ClassLoader cl) {
        try {
            setContextClassLoader(t,cl);
        } catch (SecurityException se) {
            // ignore security exception.  Earlier versions of Derby, before the 
            // DERBY-3745 fix did not require setContextClassloader permissions.
            // We may leak class loaders if we are not able to set this, but 
            // cannot just fail.
        }
    }
    
    public static ClassLoader getContextClassLoader(final Thread t) {
            return (ClassLoader)AccessController.doPrivileged(
                        new PrivilegedAction() {
                            public Object run()  {
                                return t.getContextClassLoader();
                            }
                        });
        
    }
    
   
       
}
