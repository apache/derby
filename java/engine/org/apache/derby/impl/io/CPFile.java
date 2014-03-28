/*

   Derby - Class org.apache.derby.impl.io.CPFile

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

package org.apache.derby.impl.io;

import org.apache.derby.io.StorageFile;

import java.io.InputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This class provides a class path based implementation of the StorageFile interface. It is used by the
 * database engine to access persistent data and transaction logs under the classpath subsubprotocol.
 */
class CPFile extends InputStreamFile
{

    private final CPStorageFactory storageFactory;
 
    CPFile( CPStorageFactory storageFactory, String path)
    {
        super( storageFactory, path);
        this.storageFactory = storageFactory;
    }

    CPFile( CPStorageFactory storageFactory, String parent, String name)
    {
        super( storageFactory, parent, name);
        this.storageFactory = storageFactory;
    }

    CPFile( CPFile dir, String name)
    {
        super( dir,name);
        this.storageFactory = dir.storageFactory;
    }

    private CPFile( CPStorageFactory storageFactory, String child, int pathLen)
    {
        super( storageFactory, child, pathLen);
        this.storageFactory = storageFactory;
    }

    /**
     * Tests whether the named file exists.
     *
     * @return <b>true</b> if the named file exists, <b>false</b> if not.
     */
    public boolean exists()
    {
    	return getURL() != null;
    } // end of exists

    /**
     * Get the parent of this file.
     *
     * @param pathLen the length of the parent's path name.
     */
    StorageFile getParentDir( int pathLen)
    {
        return new CPFile( storageFactory, path, pathLen);
    }
    
    /**
     * Creates an input stream from a file name.
     *
     * @return an input stream suitable for reading from the file.
     *
     * @exception FileNotFoundException if the file is not found.
     */
    public InputStream getInputStream( ) throws FileNotFoundException
    {
        URL url = getURL();

        if (url == null) {
            throw new FileNotFoundException(toString());
        }

        try {
            return openStream(url);
        } catch (FileNotFoundException fnf) {
            throw fnf;
        } catch (IOException ioe) {
            FileNotFoundException fnf = new FileNotFoundException(toString());
            fnf.initCause(ioe);
            throw fnf;
        }

    } // end of getInputStream
    
	/**
     * Return a URL for this file (resource).
     * 
     * @see org.apache.derby.io.StorageFile#getURL()
     */
    public URL getURL() {

        ClassLoader cl = getContextClassLoader(Thread.currentThread());
        URL myURL;
        if (cl != null) {
            myURL = getResource(cl, path);
            if (myURL != null)
                return myURL;
        }

        // don't assume the context class loader is tied
        // into the class loader that loaded this class.
        cl = getClass().getClassLoader();
        // Javadoc indicates implementations can use
        // null as a return from Class.getClassLoader()
        // to indicate the system/bootstrap classloader.
        if (cl != null) {
            return getResource(cl, path);
        } else {
            return getSystemResource(path);
        }
    }

    /** Privileged wrapper for {@code Thread.getContextClassLoader()}. */
    private static ClassLoader getContextClassLoader(final Thread thread) {
        return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
            public ClassLoader run() {
                return thread.getContextClassLoader();
            }
        });
    }

    /** Privileged wrapper for {@code ClassLoader.getResource(String)}. */
    private static URL getResource(
            final ClassLoader cl, final String name) {
        return AccessController.doPrivileged(new PrivilegedAction<URL>() {
            public URL run() {
                return cl.getResource(name);
            }
        });
    }

    /** Privileged wrapper for {@code ClassLoader.getSystemResource(String)}. */
    private static URL getSystemResource(final String name) {
        return AccessController.doPrivileged(new PrivilegedAction<URL>() {
            public URL run() {
                return ClassLoader.getSystemResource(name);
            }
        });
    }

    /** Privileged wrapper for {@code URL.openStream()}. */
    private static InputStream openStream(final URL url) throws IOException {
        try {
            return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<InputStream>() {
                public InputStream run() throws IOException {
                    return url.openStream();
                }
            });
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }
}
