/*
 *
 * Derby - Class org.apache.derbyTesting.junit.ClasspathSetup
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import junit.extensions.TestSetup;
import junit.framework.Test;

/**
 * <p>
 * This decorator adds another resource to the classpath, removing
 * it at tearDown().
 * </p>
 */
public class ClasspathSetup extends TestSetup
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private URL             _resource;
    private ClassLoader _originalClassLoader;
    private URLClassLoader _newClassLoader;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Add the indicated URL to the classpath.
     * </p>
     */
    public ClasspathSetup(Test test, URL resource)
    {
        super( test );
        
        _resource = resource;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    protected void setUp()
    {
        AccessController.doPrivileged
            (
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
             new PrivilegedAction<Void>()
             {
                 public Void run()
                 { 
                     _originalClassLoader = Thread.currentThread().getContextClassLoader();

                     _newClassLoader = new URLClassLoader( new URL[] { _resource }, _originalClassLoader );

                     Thread.currentThread().setContextClassLoader( _newClassLoader );
                     
                     return null;
                 }
             }
             );
    }
    
    protected void tearDown() throws Exception
    {
        AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<Void>()
             {
                 public Void run() throws IOException
                 { 
                     Thread.currentThread().setContextClassLoader( _originalClassLoader );

                     // On Java 7 and higher, URLClassLoader implements the
                     // Closable interface and has a close() method. Use that
                     // method, if it's available, to free all resources
                     // associated with the class loader. DERBY-2162.
                     if (_newClassLoader instanceof Closeable) {
                        ((Closeable) _newClassLoader).close();
                     }

                     return null;
                 }
             }
             );

        _originalClassLoader = null;
        _newClassLoader = null;
        _resource = null;
    }

    /**
     * Check whether this platform supports closing a {@code URLClassLoader}.
     *
     * @return {@code true} if {@code URLClassLoader} has a {@code close()}
     * method (Java 7 and higher), or {@code false} otherwise
     */
    public static boolean supportsClose() {
        return Closeable.class.isAssignableFrom(URLClassLoader.class);
    }

}
