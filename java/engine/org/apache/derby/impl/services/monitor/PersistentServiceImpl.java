/*

   Derby - Class org.apache.derby.impl.services.monitor.PersistentServiceImpl

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.services.monitor;

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.data.DataFactory;

import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.Property;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;

import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.Properties;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;

/**
 * This class implements the PersistentService interface using a StorageFactory class.
 * It handles all subSubProtocols except for cache.
 */
public class PersistentServiceImpl implements PersistentService
{

    private String home; // the path of the database home directory. Can be null
    private String canonicalHome; // will be null if home is null
    private final String subSubProtocol;
    private final Class storageFactoryClass;
    private StorageFactory rootStorageFactory;
    private char separatorChar;

    public PersistentServiceImpl( String subSubProtocol, Class storageFactoryClass)
        throws StandardException
    {
        this.subSubProtocol = subSubProtocol;
        this.storageFactoryClass = storageFactoryClass;

        Object monitorEnv = Monitor.getMonitor().getEnvironment();
		if (monitorEnv instanceof File)
        {
            final File relativeRoot = (File) monitorEnv;
            try
            {
                AccessController.doPrivileged(
                    new java.security.PrivilegedExceptionAction()
                    {
                        public Object run() throws IOException, StandardException
                        {
                            home = relativeRoot.getPath();
                            canonicalHome = relativeRoot.getCanonicalPath();
                            rootStorageFactory = getStorageFactoryInstance( true, null, null, null);
                            if( home != null)
                            {
                                StorageFile rootDir = rootStorageFactory.newStorageFile( null);
                                rootDir.mkdirs();
                            }
                            return null;
                        }
                    }
                    );
            }
            catch( PrivilegedActionException pae)
            {
                home = null;
                canonicalHome = null;
            }
        }
        if( rootStorageFactory == null)
        {
            try
            {
                rootStorageFactory = getStorageFactoryInstance( true, null, null, null);
            }
            catch( IOException ioe){ throw Monitor.exceptionStartingModule(/*serviceName, */ ioe); }
        }
        AccessController.doPrivileged(
            new java.security.PrivilegedAction()
            {
                public Object run()
                {
                    separatorChar = rootStorageFactory.getSeparator();
                    return null;
                }
            }
            );
    } // end of constructor

    
	/*
	** Methods of PersistentService
	*/

    /**
     * @return true if the PersistentService has a StorageFactory, false if not.
     */
    public boolean hasStorageFactory()
    {
        return true;
    }
    
    /**
     * Get an initialized StorageFactoryInstance
     *
     * @param useHome If true and the database name is not absolute then the database directory will be
     *                relative to the home directory, if one is defined in the properties file.
     * @param databaseName The name of the database (directory). The name does not include the subSubProtocol.
     *                     If null then the storage factory will only be used to deal with the directory containing
     *                     the databases.
     * @param tempDirName The name of the temporary file directory set in properties. If null then a default
     *                    directory should be used. Each database should get a separate temporary file
     *                    directory within this one to avoid collisions.
     * @param uniqueName A unique name that can be used to create the temporary file directory for this database.
     *                   If null then temporary files will not be created in this StorageFactory instance.
     *
     * @return An initialized StorageFactory.
     *
     * @exception IOException if create, the database directory does not exist, and it cannot be created;
     *                        if !create and the database does not exist as a directory.
     */
    public StorageFactory getStorageFactoryInstance(final boolean useHome,
                                                    final String databaseName,
                                                    final String tempDirName,
                                                    final String uniqueName)
        throws StandardException, IOException
    {
        try
        {
            return (StorageFactory) AccessController.doPrivileged(
                new PrivilegedExceptionAction()
                {
                    public Object run() throws InstantiationException, IllegalAccessException, IOException
                    {
                        return privGetStorageFactoryInstance( useHome, databaseName, tempDirName, uniqueName);
                    }
                });
        }
        catch (PrivilegedActionException pae)
        {
            Exception e = pae.getException();
            throw StandardException.newException( SQLState.REGISTERED_CLASS_INSTANCE_ERROR,
                                                  e, subSubProtocol, storageFactoryClass);
        }
    } // end of getStorageFactoryInstance

    private StorageFactory privGetStorageFactoryInstance( boolean useHome,
                                                          String databaseName,
                                                          String tempDirName,
                                                          String uniqueName)
         throws InstantiationException, IllegalAccessException, IOException
    {
        StorageFactory storageFactory = (StorageFactory) storageFactoryClass.newInstance();
        String dbn;
        if( databaseName != null
            && subSubProtocol != null
            && databaseName.startsWith( subSubProtocol + ":"))
            dbn = databaseName.substring( subSubProtocol.length() + 1);
        else
            dbn = databaseName;
        storageFactory.init( useHome ? home : null, dbn, tempDirName, uniqueName);
        return storageFactory;
    } // end of privGetStorageFactoryInstance
        
	/**	
		The type of the service is 'directory'

		@see PersistentService#getType
	*/
	public String getType()
    {
        return subSubProtocol;
    }


	/**
	    Return a list of all the directoies in the system directory.

		@see PersistentService#getBootTimeServices
	*/
	public Enumeration getBootTimeServices()
    {
        if( home == null)
            return null;
        return new DirectoryList();
    }

    /**
		Open the service properties in the directory identified by the service name.

		The property SERVICE_ROOT (db2j.rt.serviceRoot) is added
		by this method and set to the service directory.

		@return A Properties object or null if serviceName does not represent a valid service.

		@exception StandardException Service appears valid but the properties cannot be created.
	*/
	public Properties getServiceProperties( final String serviceName, Properties defaultProperties)
		throws StandardException
    {
		if (SanityManager.DEBUG) {
			if (! serviceName.equals(getCanonicalServiceName(serviceName)))
			{
				SanityManager.THROWASSERT("serviceName (" + serviceName + 
										  ") expected to equal getCanonicalServiceName(serviceName) (" +
										  getCanonicalServiceName(serviceName) + ")");
			}
		}

		//recreate the service root  if requested by the user.
		final String recreateFrom = recreateServiceRoot(serviceName, defaultProperties);

        InputStream is = null;
		try
        {
            is = (InputStream) AccessController.doPrivileged(
                new PrivilegedExceptionAction()
                {
                    public Object run()
                        throws FileNotFoundException, IOException, StandardException,
                        InstantiationException, IllegalAccessException
                    {
                        if( recreateFrom != null) // restore from a file
                        {
                            File propFile = new File(recreateFrom, PersistentService.PROPERTIES_NAME);
                            return new FileInputStream(propFile);
                        }
                        else
                        {
                            StorageFactory storageFactory = privGetStorageFactoryInstance( true, serviceName, null, null);
                            StorageFile file = storageFactory.newStorageFile( PersistentService.PROPERTIES_NAME);
                            InputStream is1 = file.getInputStream();
                            storageFactory.shutdown();
                            return is1;
                        }
                    }
                }
                );

			Properties serviceProperties = new Properties(defaultProperties);
			serviceProperties.load(new BufferedInputStream(is));

			return serviceProperties;
		}
        catch (PrivilegedActionException pae)
        {
            if( pae.getException() instanceof FileNotFoundException)
                return null;
            throw Monitor.exceptionStartingModule( pae.getException());
        }
        catch (FileNotFoundException fnfe) {return null ;}
		catch (SecurityException se) { throw Monitor.exceptionStartingModule(/*serviceName, */ se);	}
        catch (IOException ioe) { throw Monitor.exceptionStartingModule(/*serviceName, */ ioe); }
        finally
        {
			if (is != null)
            {
				try
                {
					is.close();
				}
                catch (IOException ioe2) {}
			}
		}
	} // end of getServiceProperties


	/**
		@exception StandardException Properties cannot be saved.
	*/

	public void saveServiceProperties( final String serviceName,
                                       StorageFactory sf,
                                       final Properties properties,
                                       final boolean replace)
		throws StandardException
    {
		if (SanityManager.DEBUG)
        {
			SanityManager.ASSERT(serviceName.equals(getCanonicalServiceName(serviceName)), serviceName);
        }
        if( ! (sf instanceof WritableStorageFactory))
            throw StandardException.newException(SQLState.READ_ONLY_SERVICE);
        final WritableStorageFactory storageFactory = (WritableStorageFactory) sf;
        try
        {
            AccessController.doPrivileged(
                new PrivilegedExceptionAction()
                {
                    public Object run() throws StandardException
                    {
                        StorageFile backupFile = null;
                        StorageFile servicePropertiesFile = storageFactory.newStorageFile( PersistentService.PROPERTIES_NAME);

                        if (replace)
                        {
                            backupFile = storageFactory.newStorageFile( PersistentService.PROPERTIES_NAME.concat("old"));
                            try
                            {
                                if(!servicePropertiesFile.renameTo(backupFile))
                                    throw StandardException.newException(SQLState.UNABLE_TO_RENAME_FILE,
                                                                         servicePropertiesFile, backupFile);
                            }
                            catch (SecurityException se) { throw Monitor.exceptionStartingModule(se); }
                        }

                        OutputStream os = null;
                        try
                        {
                            os = servicePropertiesFile.getOutputStream();
                            properties.store( os, serviceName + MessageService.getTextMessage(MessageId.SERVICE_PROPERTIES_DONT_EDIT));
                            storageFactory.sync( os, false);
                            os.close();
                            os = null;
                        }
                        catch (IOException ioe)
                        {
                            if (os != null)
                            {
                                try
                                {
                                    os.close();
                                }
                                catch (IOException ioe2) {}
                                os = null;
                            }

                            if (backupFile != null)
                            {
                                // need to re-name the old properties file back again
                                try
                                {
                                    servicePropertiesFile.delete();
                                    backupFile.renameTo(servicePropertiesFile);
                                }
                                catch (SecurityException se) {}
                            }
                            throw Monitor.exceptionStartingModule(ioe);
                        }
		
                        if (backupFile != null)
                        {
                            try
                            {
                                backupFile.delete();
                                backupFile = null;
                            }
                            catch (SecurityException se) {}
                        }
                        return null;
                    }
                }
                );
        }
        catch( PrivilegedActionException pae) { throw (StandardException) pae.getException();}
	} // end of saveServiceProperties

	/**
       Save to a backup file
       
		@exception StandardException Properties cannot be saved.
	*/

	public void saveServiceProperties(String serviceName, Properties properties, boolean replace)
		throws StandardException {
		File backupFile = null;

		File servicePropertiesFile = new File(serviceName, PersistentService.PROPERTIES_NAME);
		
		if (replace) {
			backupFile = new File(serviceName, PersistentService.PROPERTIES_NAME.concat("old"));

			try {
				if(!servicePropertiesFile.renameTo(backupFile)) {
					throw StandardException.newException(SQLState.UNABLE_TO_RENAME_FILE, servicePropertiesFile, backupFile);
				}
			} catch (SecurityException se) {
				throw Monitor.exceptionStartingModule(se);
			}
		}

		FileOutputStream fos = null;
		try {

			fos = new FileOutputStream(servicePropertiesFile);
			properties.store(fos, serviceName + MessageService.getTextMessage(MessageId.SERVICE_PROPERTIES_DONT_EDIT));
			fos.getFD().sync();
			fos.close();
			fos = null;

			replace = false;


		} catch (IOException ioe) {

			if (fos != null) {
				try {
					fos.close();
				} catch (IOException ioe2) {
				}
				fos = null;
			}

			if (backupFile != null) {
				// need to re-name the old properties file back again
				try {
					servicePropertiesFile.delete();
					backupFile.renameTo(servicePropertiesFile);
				} catch (SecurityException se) {
				}
			}
			throw Monitor.exceptionStartingModule(ioe);
		}
		
		
		if (backupFile != null) {
			try {
				backupFile.delete();
				backupFile = null;
			} catch (SecurityException se) {
				// do nothing
			}
		}

	}

    /*
	**Recreates service root if required depending on which of the following
	**attribute is specified on the conection URL:
	** Attribute.CREATE_FROM (Create database from backup if it does not exist):
	** When a database not exist, the service(database) root is created
	** and the PersistentService.PROPERTIES_NAME (service.properties) file
	** is restored from the backup.
	** Attribute.RESTORE_FROM (Delete the whole database if it exists and then restore
	** it from backup)
	** Existing database root  is deleted and the new the service(database) root is created.
	** PersistentService.PROPERTIES_NAME (service.properties) file is restored from the backup.
	** Attribute.ROLL_FORWARD_RECOVERY_FROM:(Perform Rollforward Recovery;
	** except for the log directory everthing else is replced  by the copy  from
	** backup. log files in the backup are copied to the existing online log
	** directory.):
	** When a database not exist, the service(database) root is created.
	** PersistentService.PROPERTIES_NAME (service.properties) file is deleted
	** from the service dir and  recreated with the properties from backup.
	*/

	protected String recreateServiceRoot( final String serviceName,
										  Properties properties) throws StandardException
	{
		//if there are no propertues then nothing to do in this routine
		if(properties == null) {
			return null;
		}

		String restoreFrom; //location where backup copy of service properties available
		boolean createRoot = false;
		boolean deleteExistingRoot = false;
        
		//check if user wants to create a database from a backup copy
		restoreFrom = properties.getProperty(Attribute.CREATE_FROM);
		if(restoreFrom !=null)
		{
			//create root dicretory if it  does not exist.
			createRoot =true;
			deleteExistingRoot = false;
		}
        else
		{	//check if user requested a complete restore(version recovery) from backup
			restoreFrom = properties.getProperty(Attribute.RESTORE_FROM);
			//create root dir if it does not exists and  if there exists one already delete and recreate
			if(restoreFrom !=null)
            {
				createRoot =true;
				deleteExistingRoot = true;
			}
            else
			{
				//check if user has requested roll forward recovery using a backup
				restoreFrom = properties.getProperty(Attribute.ROLL_FORWARD_RECOVERY_FROM);
				if(restoreFrom !=null)
				{
					//if service root does not exist then only create one
					//This is useful when logDevice was on some other device
					//and the device on which data directorties existied has
					//failed and user is trying to restore it some other device.
                    try
                    {
                        if( AccessController.doPrivileged(
                                new PrivilegedExceptionAction()
                                {
                                    public Object run()
                                        throws IOException, StandardException, InstantiationException, IllegalAccessException
                                    {
                                        StorageFactory storageFactory
                                          = privGetStorageFactoryInstance( true, serviceName, null, null);
                                        try
                                        {
                                            StorageFile serviceDirectory = storageFactory.newStorageFile( null);
                                            return serviceDirectory.exists() ? this : null;
                                        }
                                        finally {storageFactory.shutdown();}
                                    }
                                }
                                ) == null)
                        {
                            createRoot =true;
                            deleteExistingRoot = false;
                        }
                        
                    }
                    catch( PrivilegedActionException pae)
                    {
                        throw Monitor.exceptionStartingModule( (IOException) pae.getException());
                    }
				}
			}
		}

		//restore the service properties from backup
		if(restoreFrom != null)
		{
			//First make sure backup service directory exists in the specified path
			File backupRoot = new File(restoreFrom);
			if(backupRoot.exists())
			{
				//First make sure backup have service.properties
				File bserviceProp = new File(restoreFrom, PersistentService.PROPERTIES_NAME);
				if(bserviceProp.exists())
				{
					//create service root if required
					if(createRoot)
						createServiceRoot(serviceName, deleteExistingRoot);
                    try
                    {
                        AccessController.doPrivileged(
                            new PrivilegedExceptionAction()
                            {
                                public Object run()
                                    throws IOException, StandardException, InstantiationException, IllegalAccessException
                                {
                                    WritableStorageFactory storageFactory =
                                      (WritableStorageFactory) privGetStorageFactoryInstance( true,
                                                                                              serviceName,
                                                                                              null,
                                                                                              null);
                                    try
                                    {
                                        StorageFile cserviceProp = storageFactory.newStorageFile( PersistentService.PROPERTIES_NAME);

                                        if(cserviceProp.exists())
                                            if(!cserviceProp.delete())
                                                throw StandardException.newException(SQLState.UNABLE_TO_DELETE_FILE,
                                                                                     cserviceProp);
                                        return null;
                                    }
                                    finally { storageFactory.shutdown();}
                                }
                            }
                            );
                    }
                    catch( PrivilegedActionException pae)
                    { throw Monitor.exceptionStartingModule( (IOException)pae.getException());}
				}
                else
					throw StandardException.newException(SQLState.PROPERTY_FILE_NOT_FOUND_IN_BACKUP, bserviceProp);
			}
            else
				throw StandardException.newException(SQLState.SERVICE_DIRECTORY_NOT_IN_BACKUP, backupRoot);

			properties.put(Property.IN_RESTORE_FROM_BACKUP,"True");
			if(createRoot)
				properties.put(Property.DELETE_ROOT_ON_ERROR, "True");
		}
		return restoreFrom;
	} // end of recreateServiceRoot

	/**
		Properties cannot be saved
	*/
	public String createServiceRoot(final String name, final boolean deleteExisting)
		throws StandardException
    {
        if( !( rootStorageFactory instanceof WritableStorageFactory))
            throw StandardException.newException(SQLState.READ_ONLY_SERVICE);
		// we need to create the directory before we can call
		// getCanonicalPath() on it, because if intermediate directories
		// need to be created the getCanonicalPath() will fail.

		Throwable t = null;
        try
        {
            return (String) AccessController.doPrivileged(
                new PrivilegedExceptionAction()
                {
                    public Object run()
                        throws StandardException, IOException, InstantiationException, IllegalAccessException
                    {
                        StorageFactory storageFactory = privGetStorageFactoryInstance( true, name, null, null);
                        try
                        {
                            StorageFile serviceDirectory = storageFactory.newStorageFile( null);

                            if (serviceDirectory.exists())
                            {
                                if (deleteExisting)
                                {
                                    if (!serviceDirectory.deleteAll())
                                        throw StandardException.newException(SQLState.SERVICE_DIRECTORY_REMOVE_ERROR,
                                                                             getDirectoryPath( name));
                                }
                                else
                                    throw StandardException.newException(SQLState.SERVICE_DIRECTORY_EXISTS_ERROR,
                                                                         getDirectoryPath( name));
                            }

                            if (serviceDirectory.mkdirs())
                            {
                                try
                                {
                                    return storageFactory.getCanonicalName();
                                }
                                catch (IOException ioe)
                                {
                                    serviceDirectory.deleteAll();
                                    throw ioe;
                                }
                            }
                            throw StandardException.newException(SQLState.SERVICE_DIRECTORY_CREATE_ERROR,
                                                                 serviceDirectory, null);
                        }
                        finally { storageFactory.shutdown(); }
                    }
                }
                );
		}
        catch (SecurityException se) { t = se; }
        catch (PrivilegedActionException pae)
        {
            t = pae.getException();
            if( t instanceof StandardException)
                throw (StandardException) t;
        }

        throw StandardException.newException(SQLState.SERVICE_DIRECTORY_CREATE_ERROR, name, t);
    } // end of createServiceRoot

    private String getDirectoryPath( String name)
    {
        StringBuffer sb = new StringBuffer();
        if( home != null)
        {
            sb.append( home);
            sb.append( separatorChar);
        }
        if( separatorChar != '/')
            sb.append( name.replace( '/', separatorChar));
        else
            sb.append( name);
        return sb.toString();
    } // end of getDirectoryPath

	public boolean removeServiceRoot(final String serviceName)
    {
        if( !( rootStorageFactory instanceof WritableStorageFactory))
            return false;
        try
        {
            return AccessController.doPrivileged(
                new PrivilegedExceptionAction()
                {
                    public Object run()
                        throws StandardException, IOException, InstantiationException, IllegalAccessException
                    {
                        StorageFactory storageFactory = privGetStorageFactoryInstance( true, serviceName, null, null);
                        try
                        {
                            if (SanityManager.DEBUG)
                                SanityManager.ASSERT(serviceName.equals( storageFactory.getCanonicalName()), serviceName);
                            StorageFile serviceDirectory = storageFactory.newStorageFile( null);
                            return serviceDirectory.deleteAll() ? this : null;
                        }
                        finally { storageFactory.shutdown(); }
                    }
                }
                ) != null;
        }
        catch( PrivilegedActionException pae){ return false;}
	} // end of removeServiceRoot

	public String getCanonicalServiceName(String name)
    {
		String protocolLeadIn = getType() + ":";
        int colon = name.indexOf( ':');
        if( colon > 1) // Subsubprotocols must be at least 2 characters long
        {
            if( ! name.startsWith( protocolLeadIn))
                return null; // It is not our database
            name = name.substring( colon + 1);
        }
        if( getType().equals( PersistentService.DIRECTORY)) // The default subsubprototcol
            protocolLeadIn = "";
        final String nm = name;

        try
        {
            return protocolLeadIn + (String) AccessController.doPrivileged(
                new PrivilegedExceptionAction()
                {
                    public Object run()
                        throws StandardException, IOException, InstantiationException, IllegalAccessException
                    {
                        StorageFactory storageFactory = privGetStorageFactoryInstance( true, nm, null, null);
                        try
                        {
                            return storageFactory.getCanonicalName();
                        }
                        finally { storageFactory.shutdown();}
                    }
                }
                );
        }
		catch (PrivilegedActionException pae)
        {
            if( SanityManager.DEBUG)
            {
                Exception ex = pae.getException();
                SanityManager.THROWASSERT( ex.getClass().getName()
                                           + " thrown while getting the canonical name: "
                                           + ex.getMessage());
            }
            return null;
        }
	} // end of getCanonicalServiceName

	public String getUserServiceName(String serviceName)
    {
		if (home != null)
        {
			// allow for file separatorChar by adding 1 to the length
			if ((serviceName.length() > (canonicalHome.length() + 1)) && serviceName.startsWith(canonicalHome))
            {
				serviceName = serviceName.substring(canonicalHome.length());
				if (serviceName.charAt(0) == separatorChar)
					serviceName = serviceName.substring(1);
			}
		}

		return serviceName.replace( separatorChar, '/');
	}

	public boolean isSameService(String serviceName1, String serviceName2)
    {
		if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(serviceName1.equals(getCanonicalServiceName(serviceName1)), serviceName1);
			SanityManager.ASSERT(serviceName2.equals(getCanonicalServiceName(serviceName2)), serviceName2);
		}
		return serviceName1.equals(serviceName2);
	} // end of isSameService


    /**
     * Get the StorageFactory implementation for this PersistentService
     *
     * @return the StorageFactory class.
     */
    public Class getStorageFactoryClass()
    {
        return storageFactoryClass;
    }
    
    class DirectoryList implements Enumeration, PrivilegedAction
    {
        private String[] contents;
        private StorageFile systemDirectory;	 

        private int      index;
        private boolean  validIndex;

        private int actionCode;
        private static final int INIT_ACTION = 0;
        private static final int HAS_MORE_ELEMENTS_ACTION = 1;

        DirectoryList()
        {
            actionCode = INIT_ACTION;
            AccessController.doPrivileged( this);
        }

        public boolean hasMoreElements()
        {
            if (validIndex)
                return true;

            actionCode = HAS_MORE_ELEMENTS_ACTION;
            return AccessController.doPrivileged( this) != null;
        } // end of hasMoreElements

        public Object nextElement() throws NoSuchElementException
        {
            if (!hasMoreElements())
                throw new NoSuchElementException();

            validIndex = false;
            return contents[index++];
        } // end of nextElement

        // PrivilegedAction method
        public Object run()
        {
            switch( actionCode)
            {
            case INIT_ACTION:
                systemDirectory = rootStorageFactory.newStorageFile( null);
                contents = systemDirectory.list();
                return null;

            case HAS_MORE_ELEMENTS_ACTION:
                for (; index < contents.length; contents[index++] = null)
                {
                    try
                    {
                        StorageFactory storageFactory = privGetStorageFactoryInstance( true, contents[index], null, null);
                        try
                        {
                            StorageFile properties = storageFactory.newStorageFile( PersistentService.PROPERTIES_NAME);
                            if (!properties.exists())
                                continue;
                            // convert to a canonical name while we are here.
                            contents[index] = storageFactory.getCanonicalName();
                            validIndex = true;

                            return this;
                        }
                        finally { storageFactory.shutdown(); }
                    }
                    catch (Exception se) { continue; }
                }
                return null;
            }
            return null;
        } // end of run
    } // end of class DirectoryList
}
