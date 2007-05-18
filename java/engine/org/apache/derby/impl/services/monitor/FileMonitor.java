/*

   Derby - Class org.apache.derby.impl.services.monitor.FileMonitor

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

package org.apache.derby.impl.services.monitor;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.iapi.services.info.ProductVersionHolder;
import org.apache.derby.iapi.services.info.ProductGenusNames;

import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

/**
	Implementation of the monitor that uses the class loader
	that the its was loaded in for all class loading.

*/

public final class FileMonitor extends BaseMonitor implements java.security.PrivilegedExceptionAction
{

	/* Fields */
	private File home;

	private ProductVersionHolder engineVersion;

	public FileMonitor() {
		initialize(true);
		applicationProperties = readApplicationProperties();
	}

	public FileMonitor(java.util.Properties properties, java.io.PrintStream log) {
		runWithState(properties, log);
	}



	private InputStream PBapplicationPropertiesStream()
	  throws IOException {

		File sr = FileUtil.newFile(home, Property.PROPERTIES_FILE);

		if (!sr.exists())
			return null;

		return new FileInputStream(sr);
	}

	public Object getEnvironment() {
		return home;
	}



	/**
		SECURITY WARNING.

		This method is run in a privileged block in a Java 2 environment.

		Set the system home directory.  Returns false if it couldn't for
		some reason.

	**/
	private boolean PBinitialize(boolean lite)
	{
		if (!lite) {
			try {
				// Create a ThreadGroup and set the daemon property to
				// make sure the group is destroyed and garbage
				// collected when all its members have finished (i.e.,
				// when the driver is unloaded).
				daemonGroup = new ThreadGroup("derby.daemons");
				daemonGroup.setDaemon(true);
			} catch (SecurityException se) {
			}
		}

		InputStream versionStream = getClass().getResourceAsStream(ProductGenusNames.DBMS_INFO);

		engineVersion = ProductVersionHolder.getProductVersionHolderFromMyEnv(versionStream);

		String systemHome;
		// create the system home directory if it doesn't exist
		try {
			// SECURITY PERMISSION - OP2
			systemHome = System.getProperty(Property.SYSTEM_HOME_PROPERTY);
		} catch (SecurityException se) {
			// system home will be the current directory
			systemHome = null;
		}

		if (systemHome != null) {
			home = new File(systemHome);

			// SECURITY PERMISSION - OP2a
			if (home.exists()) {
				if (!home.isDirectory()) {
					report(Property.SYSTEM_HOME_PROPERTY + "=" + systemHome
						+ " does not represent a directory");
					return false;
				}
			} else if (!lite) {

				try {
					// SECURITY PERMISSION - OP2b
                    // Attempt to create just the folder initially
                    // which does not require read permission on
                    // the parent folder. This is to allow a policy
                    // file to limit file permissions for derby.jar
                    // to be contained under derby.system.home.
                    // If the folder cannot be created that way
                    // due to missing parent folder(s) 
                    // then mkdir() will return false and thus
                    // mkdirs will be called to create the
                    // intermediate folders. This use of mkdir()
                    // and mkdirs() retains existing (pre10.3) behaviour
                    // but avoids requiring read permission on the parent
                    // directory if it exists.
					boolean created = home.mkdir() || home.mkdirs();
				} catch (SecurityException se) {
					return false;
				}
			}
		}

		return true;
	}

	/**
		SECURITY WARNING.

		This method is run in a privileged block in a Java 2 environment.

		Return a property from the JVM's system set.
		In a Java2 environment this will be executed as a privileged block
		if and only if the property starts with 'derby.'.
		If a SecurityException occurs, null is returned.
	*/
	private String PBgetJVMProperty(String key) {

		try {
			// SECURITY PERMISSION - OP1
			return System.getProperty(key);
		} catch (SecurityException se) {
			return null;
		}
	}

	/*
	** Priv block code, moved out of the old Java2 version.
	*/

	private int action;
	private String key3;
	private Runnable task;
	private int intValue;

	/**
		Initialize the system in a privileged block.
	**/
	synchronized final boolean initialize(boolean lite)
	{
		action = lite ? 0 : 1;
		try {
			Object ret = java.security.AccessController.doPrivileged(this);

			return ((Boolean) ret).booleanValue();
        } catch (java.security.PrivilegedActionException pae) {
			throw (RuntimeException) pae.getException();
		}
	}

	synchronized final Properties getDefaultModuleProperties() {
		action = 2;
 		try {
			return (Properties) java.security.AccessController.doPrivileged(this);
        } catch (java.security.PrivilegedActionException pae) {
           throw (RuntimeException) pae.getException();
        }
    }

	public synchronized final String getJVMProperty(String key) {
		if (!key.startsWith("derby."))
			return PBgetJVMProperty(key);

		try {

			action = 3;
			key3 = key;
			String value  = (String) java.security.AccessController.doPrivileged(this);
			key3 = null;
			return value;
        } catch (java.security.PrivilegedActionException pae) {
			throw (RuntimeException) pae.getException();
		}
	}

	public synchronized final Thread getDaemonThread(Runnable task, String name, boolean setMinPriority) {

		action = 4;
		key3 = name;
		this.task = task;
		this.intValue = setMinPriority ? 1 : 0;

		try {

			Thread t = (Thread) java.security.AccessController.doPrivileged(this);

			key3 = null;
			task = null;

			return t;
        } catch (java.security.PrivilegedActionException pae) {
			throw (RuntimeException) pae.getException();
		}
	}

	public synchronized final void setThreadPriority(int priority) {
		action = 5;
		intValue = priority;
		try {
			java.security.AccessController.doPrivileged(this);
        } catch (java.security.PrivilegedActionException pae) {
			throw (RuntimeException) pae.getException();
		}
	}

	synchronized final InputStream applicationPropertiesStream()
	  throws IOException {
		action = 6;
		try {
			// SECURITY PERMISSION - OP3
			return (InputStream) java.security.AccessController.doPrivileged(this);
		}
        catch (java.security.PrivilegedActionException pae)
        {
			throw (IOException) pae.getException();
		}
	}


	public synchronized final Object run() throws IOException {
		switch (action) {
		case 0:
		case 1:
			// SECURITY PERMISSION - OP2, OP2a, OP2b
			return new Boolean(PBinitialize(action == 0));
		case 2: 
			// SECURITY PERMISSION - IP1
			return super.getDefaultModuleProperties();
		case 3:
			// SECURITY PERMISSION - OP1
			return PBgetJVMProperty(key3);
		case 4:
			return super.getDaemonThread(task, key3, intValue != 0);
		case 5:
			super.setThreadPriority(intValue);
			return null;
		case 6:
			// SECURITY PERMISSION - OP3
			return PBapplicationPropertiesStream();

		default:
			return null;
		}
	}

	public final ProductVersionHolder getEngineVersion() {
		return engineVersion;
	}
}
