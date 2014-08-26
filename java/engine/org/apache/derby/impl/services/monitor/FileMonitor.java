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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.info.ProductGenusNames;
import org.apache.derby.iapi.services.info.ProductVersionHolder;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.shared.common.reference.MessageId;

/**
	Implementation of the monitor that uses the class loader
	that the its was loaded in for all class loading.

*/

public final class FileMonitor extends BaseMonitor
{

	/* Fields */
	private File home;

	private ProductVersionHolder engineVersion;

	public FileMonitor() {
		initialize(true);
		applicationProperties = readApplicationProperties();
	}

	public FileMonitor(Properties properties, PrintWriter log) {
		runWithState(properties, log);
	}



	private InputStream PBapplicationPropertiesStream()
	  throws IOException {

        File sr = new File(home, Property.PROPERTIES_FILE);

		if (!sr.exists())
			return null;

		return new FileInputStream(sr);
	}

	public Object getEnvironment() {
		return home;
	}

    /**
     * Create a ThreadGroup and set the daemon property to make sure
     * the group is destroyed and garbage collected when all its
     * members have finished (i.e., either when the driver is
     * unloaded, or when the last database is shut down).
     *
     * @return the thread group "derby.daemons" or null if we saw
     * a SecurityException
     */
    private ThreadGroup createDaemonGroup() {
        try {
            ThreadGroup group = new ThreadGroup("derby.daemons");
            group.setDaemon(true);
            return group;
        } catch (SecurityException se) {
            // In case of a lacking privilege, issue a warning, return null and
            // let the daemon threads be created in the default thread group.
            // This can only happen if the current Derby thread is a part of
            // the root thread group "system".
            reportThread(se);
            return null;
        }
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
            daemonGroup = createDaemonGroup();
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
            report(se, Property.SYSTEM_HOME_PROPERTY);
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

                boolean created = false;
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
                    created = home.mkdir() || home.mkdirs();
                    if (created) {
                        FileUtil.limitAccessToOwner(home);
                    }
				} catch (SecurityException se) {
                    report(se, home);
					return false;
                } catch (IOException ioe) {
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
            report(se, key);
			return null;
		}
	}


    private final static Map<String, Void> securityProperties =
            new HashMap<String, Void>();
    static {
        securityProperties.put("derby.authentication.builtin.algorithm", null);
        securityProperties.put("derby.authentication.provider", null);
        securityProperties.put("derby.database.fullAccessUsers", null);
        securityProperties.put("derby.database.readOnlyAccessUsers", null);
        securityProperties.put("derby.database.sqlAuthorization", null);
        securityProperties.put("derby.connection.requireAuthentication", null);
        securityProperties.put("derby.database.defaultConnectionMode", null);
        securityProperties.put("derby.storage.useDefaultFilePermissions", null);
        securityProperties.put(Property.SYSTEM_HOME_PROPERTY, null);
    };

    private void report(SecurityException e, String key) {
         if (securityProperties.containsKey(key)) {
            report(MessageService.getTextMessage(
                MessageId.CANNOT_READ_SECURITY_PROPERTY, key, e.toString()));
         }
    }

    private void report(SecurityException e, File file) {
        report(MessageService.getTextMessage(
                MessageId.CANNOT_CREATE_FILE_OR_DIRECTORY,
                file.toString(),
                e.toString()));
    }

    private void reportThread(SecurityException e) {
        report(MessageService.getTextMessage(
                MessageId.CANNOT_SET_DAEMON, e.toString()));
    }

	/*
	** Priv block code, moved out of the old Java2 version.
	*/

	/**
		Initialize the system in a privileged block.
	**/
	final boolean initialize(final boolean lite)
	{
        // SECURITY PERMISSION - OP2, OP2a, OP2b
        return (AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
            public Boolean run() {
                return Boolean.valueOf(PBinitialize(lite));
            }
        })).booleanValue();
	}

	final Properties getDefaultModuleProperties() {
        // SECURITY PERMISSION - IP1
        return AccessController.doPrivileged(
                new PrivilegedAction<Properties>() {
            public Properties run() {
                return FileMonitor.super.getDefaultModuleProperties();
            }
        });
    }

	public final String getJVMProperty(final String key) {
		if (!key.startsWith("derby."))
			return PBgetJVMProperty(key);

        // SECURITY PERMISSION - OP1
        return AccessController.doPrivileged(new PrivilegedAction<String>() {
            public String run() {
                return PBgetJVMProperty(key);
            }
        });
	}

	public synchronized final Thread getDaemonThread(
            final Runnable task,
            final String name,
            final boolean setMinPriority) {
        return AccessController.doPrivileged(new PrivilegedAction<Thread>() {
            public Thread run() {
                try {
                    return FileMonitor.super.getDaemonThread(
                            task, name, setMinPriority);
                } catch (IllegalThreadStateException e) {
                    // We may get an IllegalThreadStateException if all the
                    // previously running daemon threads have completed and the
                    // daemon group has been automatically destroyed. If that's
                    // what happened, create a new daemon group and try again.
                    if (daemonGroup != null && daemonGroup.isDestroyed()) {
                        daemonGroup = createDaemonGroup();
                        return FileMonitor.super.getDaemonThread(
                                task, name, setMinPriority);
                    } else {
                        throw e;
                    }
                }
            }
        });
    }

	final InputStream applicationPropertiesStream()
	  throws IOException {
		try {
			// SECURITY PERMISSION - OP3
			return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<InputStream>() {
                public InputStream run() throws IOException {
                    return PBapplicationPropertiesStream();
                }
            });
		}
        catch (java.security.PrivilegedActionException pae)
        {
			throw (IOException) pae.getException();
		}
	}

	public final ProductVersionHolder getEngineVersion() {
		return engineVersion;
	}
}
