/*

   Derby - Class org.apache.derby.impl.services.monitor.StorageFactoryService

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

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageRandomAccessFile;

import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.iapi.reference.Property;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;

import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.Properties;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import org.apache.derby.iapi.services.io.FileUtil;

/**
 * This class implements the PersistentService interface using a StorageFactory class.
 * It handles all subSubProtocols except for cache.
 */
final class StorageFactoryService implements PersistentService
{
    /** Marker printed as the last line of the service properties file. */
    private static final String SERVICE_PROPERTIES_EOF_TOKEN =
            "#--- last line, don't put anything after this line ---";

    private String home; // the path of the database home directory. Can be null
    private String canonicalHome; // will be null if home is null
    private final String subSubProtocol;
    private final Class<?> storageFactoryClass;
    private StorageFactory rootStorageFactory;
    private char separatorChar;

    StorageFactoryService( String subSubProtocol, Class storageFactoryClass)
        throws StandardException
    {
        this.subSubProtocol = subSubProtocol;
        this.storageFactoryClass = storageFactoryClass;

        Object monitorEnv = getMonitor().getEnvironment();
		if (monitorEnv instanceof File)
        {
            final File relativeRoot = (File) monitorEnv;
            try
            {
                AccessController.doPrivileged(
                    new java.security.PrivilegedExceptionAction<Object>()
                    {
                        public Object run() throws IOException, StandardException
                        {
                            home = relativeRoot.getPath();
                            canonicalHome = relativeRoot.getCanonicalPath();
                            rootStorageFactory = getStorageFactoryInstance( true, null, null, null);

                            if( home != null)
                            {
                                StorageFile rootDir = rootStorageFactory.newStorageFile( null);
                                boolean created = rootDir.mkdirs();
                                if (created) {
                                    rootDir.limitAccessToOwner();
                                }
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
            new java.security.PrivilegedAction<Object>()
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
            return AccessController.doPrivileged(
                new PrivilegedExceptionAction<StorageFactory>()
                {
                  public StorageFactory run() throws InstantiationException,
                      IllegalAccessException, IOException, NoSuchMethodException, InvocationTargetException
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
      throws InstantiationException,
             IllegalAccessException,
             IOException,
             NoSuchMethodException,
             InvocationTargetException
    {
        StorageFactory storageFactory = (StorageFactory) storageFactoryClass.getConstructor().newInstance();
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

        final Properties serviceProperties = new Properties(defaultProperties);
		try
        {
            AccessController.doPrivileged(
                new PrivilegedExceptionAction<Object>()
                {
                    public Object run()
                        throws IOException, StandardException,
                        InstantiationException, IllegalAccessException,
                        NoSuchMethodException, InvocationTargetException
                    {
                        if( recreateFrom != null) // restore from a file
                        {
                            File propFile = new File(recreateFrom, PersistentService.PROPERTIES_NAME);
                            InputStream is = new FileInputStream(propFile);
                            try {
                                serviceProperties.load(new BufferedInputStream(is));
                            } finally {
                                is.close();
                            }
                        }
                        else
                        {
                            StorageFactory storageFactory = privGetStorageFactoryInstance( true, serviceName, null, null);
                            StorageFile file = storageFactory.newStorageFile( PersistentService.PROPERTIES_NAME);
                            resolveServicePropertiesFiles(storageFactory, file);
                            try {
                                InputStream is = file.getInputStream();
                                try {
                                    // Need to load the properties before closing the
                                    // StorageFactory.
                                    serviceProperties.load(new BufferedInputStream(is));
                                } finally {
                                    is.close();
                                }
                            } finally {
                               storageFactory.shutdown();
                            }
                        }
                        return null;
                    }
                }
                );

			return serviceProperties;
		}
        catch (PrivilegedActionException pae)
        {
            if( pae.getException() instanceof FileNotFoundException)
                return null;
            throw Monitor.exceptionStartingModule( pae.getException());
        }
		catch (SecurityException se) { throw Monitor.exceptionStartingModule(/*serviceName, */ se);	}
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
        // Write the service properties to file.
        try
        {
            AccessController.doPrivileged(
                new PrivilegedExceptionAction<Object>()
                {
                    public Object run() throws StandardException
                    {
                        StorageFile backupFile = replace
                            ? storageFactory.newStorageFile(
                                PersistentService.PROPERTIES_NAME.concat("old"))
                            : null;
                        StorageFile servicePropertiesFile = storageFactory.newStorageFile( PersistentService.PROPERTIES_NAME);
                        FileOperationHelper foh = new FileOperationHelper();

                        if (replace)
                        {
                            foh.renameTo(
                                    servicePropertiesFile, backupFile, true);
                        }

                        OutputStream os = null;
                        try
                        {
                            os = servicePropertiesFile.getOutputStream();
                            properties.store(os, serviceName +
                                MessageService.getTextMessage(
                                    MessageId.SERVICE_PROPERTIES_DONT_EDIT));
                            // The eof token should match the ISO-8859-1 encoding 
                            // of the rest of the properties file written with store.
                            BufferedWriter bOut = new BufferedWriter(
                                    new OutputStreamWriter(os,"ISO-8859-1"));
                            bOut.write(SERVICE_PROPERTIES_EOF_TOKEN);
                            bOut.newLine();
                            storageFactory.sync( os, false);
                            bOut.close();
                            os.close();
                            os = null; 
                        }
                        catch (IOException ioe)
                        {
                            if (backupFile != null)
                            {
                                // Rename the old properties file back again.
                                foh.renameTo(backupFile, servicePropertiesFile,
                                        false);
                            }
                            if (replace)
                            {
                                throw StandardException.newException(
                                        SQLState.SERVICE_PROPERTIES_EDIT_FAILED,
                                        ioe);
                            }
                            else
                            {
                                throw Monitor.exceptionStartingModule(ioe);
                            }
                        }
                        finally
                        {
                            if (os != null)
                            {
                                try
                                {
                                    os.close();
                                }
                                catch (IOException ioe)
                                {
                                    // Ignore exception on close
                                }
                            }
                        }
		
                        if (backupFile != null)
                        {
                            if (!foh.delete(backupFile, false))
                            {
                                Monitor.getStream().printlnWithHeader(
                                    MessageService.getTextMessage(
                                        MessageId.SERVICE_PROPERTIES_BACKUP_DEL_FAILED,
                                        getMostAccuratePath(backupFile)));
                                
                            }
                        }
                        return null;
                    }
                }
                );
        }
        catch( PrivilegedActionException pae) { throw (StandardException) pae.getException();}
	} // end of saveServiceProperties

	
    /** @see PersistentService#createDataWarningFile */
    public void createDataWarningFile(StorageFactory sf) throws StandardException {
        if( ! (sf instanceof WritableStorageFactory))
            throw StandardException.newException(SQLState.READ_ONLY_SERVICE);
        final WritableStorageFactory storageFactory = (WritableStorageFactory) sf;
        try
        {
            AccessController.doPrivileged(
            	    new PrivilegedExceptionAction<Object>()
                    {
                        public Object run() throws StandardException
                        {
                            OutputStreamWriter osw=null; 
                            try 
                            {
                                StorageFile fileReadMe = storageFactory.newStorageFile(
                                    PersistentService.DB_README_FILE_NAME);
                                osw = new OutputStreamWriter(fileReadMe.getOutputStream(),"UTF8");
                                osw.write(MessageService.getTextMessage(
                                        MessageId.README_AT_DB_LEVEL));
                            }
                            catch (IOException ioe)
                            {
                            }
                            finally
                            {
                                if (osw != null)
                                {
                                    try
                                    {
                                        osw.close();
                                    }
                                    catch (IOException ioe)
                                    {
                                        // Ignore exception on close
                                    }
                                }
                            }
                            return null;
                        }
                    }
                );
        }
        catch( PrivilegedActionException pae) { throw (StandardException) pae.getException();}
    } // end of createDataWarningFile

    /**
     * Save service.properties during backup
     *
     * @param serviceName backup location of the service
     * @param properties the properties to save
     *
     * @exception StandardException Properties cannot be saved.
     */

	public void saveServiceProperties(final String serviceName, 
                                      final Properties properties)
		throws StandardException {

        try
        {
            AccessController.doPrivileged(
                new PrivilegedExceptionAction<Object>()
                {
                    public Object run() throws StandardException
                    {
                        // Since this is the backup location, we cannot use
                        // storageFactory.newStorageFile as in the other
                        // variant of this method:
                        File servicePropertiesFile = 
                            new File(serviceName, PersistentService.PROPERTIES_NAME);

                        FileOutputStream fos = null;
                        try {

                            fos = new FileOutputStream(servicePropertiesFile);
                            FileUtil.limitAccessToOwner(servicePropertiesFile);

                            properties.store(fos, 
                                             serviceName + 
                                             MessageService.getTextMessage(
                                                  MessageId.SERVICE_PROPERTIES_DONT_EDIT));
                            fos.getFD().sync();
                            fos.close();
                            fos = null;
                        } catch (IOException ioe) {
                            if (fos != null) {
                                try {
                                    fos.close();
                                } catch (IOException ioe2) {
                                }
                                fos = null;
                            }

                            throw Monitor.exceptionStartingModule(ioe);
                        }
		
                        return null;
                    }
                }
                );
        }catch( PrivilegedActionException pae) { throw (StandardException) pae.getException();}
    }
    
    /**
     * Resolves situations where a failure condition left the service properties
     * file, and/or the service properties file backup, in an inconsistent
     * state.
     * <p>
     * Note that this method doesn't resolve the situation where both the
     * current service properties file and the backup file are missing.
     *
     * @param sf the storage factory for the service
     * @param spf the service properties file
     * @throws StandardException if a file operation on a service properties
     *      file fails
     */
    private void resolveServicePropertiesFiles(StorageFactory sf,
                                               StorageFile spf)
            throws StandardException {
        StorageFile spfOld = sf.newStorageFile(PROPERTIES_NAME.concat("old"));
        FileOperationHelper foh = new FileOperationHelper();
        boolean hasCurrent = foh.exists(spf, true);
        boolean hasBackup = foh.exists(spfOld, true);
        // Shortcut the normal case.
        if (hasCurrent && !hasBackup) {
            return;
        }

        // Backup file, but no current file.
        if (hasBackup && !hasCurrent) {
            // Writing the new service properties file must have failed during
            // an update. Rename the backup to be the current file.
            foh.renameTo(spfOld, spf, true);
            Monitor.getStream().printlnWithHeader(
                                MessageService.getTextMessage(
                                    MessageId.SERVICE_PROPERTIES_RESTORED));
        // Backup file and current file.
        } else if (hasBackup && hasCurrent) {
            // See if the new (current) file is valid. If so delete the backup,
            // if not, rename the backup to be the current.
            BufferedReader bin = null;
            String lastLine = null;
            try {
                //service.properties always in ISO-8859-1 because written with Properties.store()
                bin = new BufferedReader(new InputStreamReader(
                        new FileInputStream(spf.getPath()),"ISO-8859-1"));
                String line;
                while ((line = bin.readLine()) != null) {
                    if (line.trim().length() != 0) {
                        lastLine = line;
                    }
                }
            } catch (IOException ioe) {
                throw StandardException.newException(
                        SQLState.UNABLE_TO_OPEN_FILE, ioe,
                        spf.getPath(), ioe.getMessage());
            } finally {
                try {
                    if (bin != null) {
                        bin.close();
                    }
                } catch (IOException ioe) {
                    // Ignore exception during close
                }
            }
            if (lastLine != null &&
                    lastLine.startsWith(SERVICE_PROPERTIES_EOF_TOKEN)) {
                // Being unable to delete the backup file is fine as long as
                // the current file appears valid.
                String msg;
                if (foh.delete(spfOld, false)) {
                    msg = MessageService.getTextMessage(
                            MessageId.SERVICE_PROPERTIES_BACKUP_DELETED);    
                } else {
                    // Include path so the user can delete file manually.
                    msg = MessageService.getTextMessage(
                            MessageId.SERVICE_PROPERTIES_BACKUP_DEL_FAILED,
                            getMostAccuratePath(spfOld));
                }
                Monitor.getStream().printlnWithHeader(msg);
            } else {
                foh.delete(spf, false);
                foh.renameTo(spfOld, spf, true);
                Monitor.getStream().printlnWithHeader(
                                MessageService.getTextMessage(
                                    MessageId.SERVICE_PROPERTIES_RESTORED));
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
                                new PrivilegedExceptionAction<Object>()
                                {
                                    public Object run()
                                        throws IOException, StandardException,
                                      InstantiationException, IllegalAccessException,
                                      NoSuchMethodException, InvocationTargetException
                                      
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
			if (fileExists(backupRoot))
			{
				//First make sure backup have service.properties
				File bserviceProp = new File(restoreFrom, PersistentService.PROPERTIES_NAME);
				if(fileExists(bserviceProp))
				{
					//create service root if required
					if(createRoot)
						createServiceRoot(serviceName, deleteExistingRoot);
                    try
                    {
                        AccessController.doPrivileged(
                            new PrivilegedExceptionAction<Object>()
                            {
                                public Object run()
                                    throws IOException, StandardException,
                                    InstantiationException, IllegalAccessException,
                                    NoSuchMethodException, InvocationTargetException
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
            return getProtocolLeadIn() + (String) AccessController.doPrivileged(
                new PrivilegedExceptionAction<Object>()
                {
                    public Object run()
                        throws StandardException, IOException,
                        InstantiationException, IllegalAccessException,
                        NoSuchMethodException, InvocationTargetException
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
                                {
                                    vetService( storageFactory, name );
                                    throw StandardException.newException(SQLState.SERVICE_DIRECTORY_EXISTS_ERROR,
                                                                         getDirectoryPath( name));
                                }
                            }

                            if (serviceDirectory.mkdirs())
                            {
                                serviceDirectory.limitAccessToOwner();
                                // DERBY-5096. The storageFactory canonicalName may need to be adjusted
                                // for casing after the directory is created. Just reset it after making the 
                                // the directory to make sure.
                                String serviceDirCanonicalPath = serviceDirectory.getCanonicalPath();
                                storageFactory.setCanonicalName(serviceDirCanonicalPath);
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
                            throw StandardException.newException(SQLState.SERVICE_DIRECTORY_CREATE_ERROR, serviceDirectory);
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

        throw StandardException.newException(SQLState.SERVICE_DIRECTORY_CREATE_ERROR, t, name);
    } // end of createServiceRoot

    /**
       Verify that the service directory looks ok before objecting that the database
       already exists.
    */
    private void    vetService( StorageFactory storageFactory, String serviceName ) throws StandardException
    {
        // check for existence of service.properties descriptor file
        StorageFile    service_properties = storageFactory.newStorageFile( PersistentService.PROPERTIES_NAME );

        if ( !service_properties.exists() )
        {
            // DERBY-5526 Try to roughly determine if this was a partially created database by 
            // seeing if the seg0 directory exists.
            StorageFile seg0 = storageFactory.newStorageFile("seg0");
            if (seg0.exists()) {
            throw StandardException.newException
                ( SQLState.SERVICE_PROPERTIES_MISSING, serviceName, PersistentService.PROPERTIES_NAME );
            }
        }
    }

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
                new PrivilegedExceptionAction<Object>()
                {
                    public Object run()
                        throws StandardException, IOException,
                        InstantiationException, IllegalAccessException,
                        NoSuchMethodException, InvocationTargetException
                    {
                        StorageFactory storageFactory = privGetStorageFactoryInstance( true, serviceName, null, null);
                        try
                        {
                            if (SanityManager.DEBUG)
                            {
                                // Run this through getCanonicalServiceName as
                                // an extra sanity check. Prepending the
                                // protocol lead in to the canonical name from
                                // the storage factory should be enough.
                                String tmpCanonical = getCanonicalServiceName(
                                        getProtocolLeadIn() +
                                        storageFactory.getCanonicalName());
                                // These should give the same result.
                                SanityManager.ASSERT(
                                        tmpCanonical.equals(getProtocolLeadIn()
                                        + storageFactory.getCanonicalName()));
                                SanityManager.ASSERT(
                                    serviceName.equals(tmpCanonical),
                                    "serviceName = " + serviceName +
                                    " ; protocolLeadIn + " +
                                    "storageFactory.getCanoicalName = " +
                                    tmpCanonical);
                            }
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
		throws StandardException
    {
        int colon = name.indexOf( ':');
        // If no subsubprotocol is specified and the storage factory type isn't
        // the default one, abort. We have to deal with Windows drive
        // specifications here, which contain a colon (i.e. 'C:').
        // The logic in this method may break in some cases if a colon is used
        // in the directory or database name.
        if (colon < 2 && !getType().equals(PersistentService.DIRECTORY)) {
            return null;
        }
        if( colon > 1) // Subsubprotocols must be at least 2 characters long
        {
            if( ! name.startsWith(getType() + ":"))
                return null; // It is not our database
            name = name.substring( colon + 1);
        }
        final String nm = name;

        try
        {
            return getProtocolLeadIn() + AccessController.doPrivileged(
                new PrivilegedExceptionAction<String>()
                {
                    public String run()
                        throws StandardException, IOException,
                        InstantiationException, IllegalAccessException,
                        NoSuchMethodException, InvocationTargetException
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
			throw Monitor.exceptionStartingModule(pae.getException());
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
			try {
            SanityManager.ASSERT(serviceName1.equals(getCanonicalServiceName(serviceName1)), serviceName1);
			SanityManager.ASSERT(serviceName2.equals(getCanonicalServiceName(serviceName2)), serviceName2);
			} catch (StandardException se)
			{
				return false;
			}
		}
		return serviceName1.equals(serviceName2);
	} // end of isSameService

    /**
     * Checks if the specified file exists.
     *
     * @param file the file to check
     * @return {@code true} if the file exists, {@code false} if not.
     * @throws SecurityException if the required privileges are missing
     */
    private final boolean fileExists(final File file) {
        return (AccessController.doPrivileged(
                new PrivilegedAction<Boolean>() {
                    public Boolean run() {
                        return file.exists();
                    }
            })).booleanValue();
    }

    /**
     * Get the StorageFactory implementation for this PersistentService
     *
     * @return the StorageFactory class.
     */
    public Class getStorageFactoryClass()
    {
        return storageFactoryClass;
    }

    /**
     * Returns the protocol lead in for this service.
     *
     * @return An empty string if the protocol is the default one
     *      (PersistentService.DIRECTORY), the subsub protocol name followed by
     *      colon otherwise.
     */
    private String getProtocolLeadIn() {
        // We prepend the subsub protocol name to the storage factory canonical
        // name to form the service name, except in case of the default
        // subsub prototcol (which is PersistentService.DIRECTORY).
        if (getType().equals(PersistentService.DIRECTORY)) {
            return "";
        } else {
            return getType() + ":";
        }
    }
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

    final class DirectoryList implements Enumeration, PrivilegedAction<DirectoryList>
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
            if (contents == null)
                return false;
            
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
        public final DirectoryList run()
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
                        String dirname = contents[index];
                        StorageFile dir = rootStorageFactory.newStorageFile(dirname);
                        if (!dir.isDirectory())
                            continue;
                        
                        // Look to see if service.properties is in this
                        // directory.
                        StorageFile properties =
                            rootStorageFactory.newStorageFile(dir,
                                    PersistentService.PROPERTIES_NAME);
                        
                        if (!properties.exists())
                            continue;
                        
                        // convert to a canonical name while we are here.
                        contents[index] = dir.getCanonicalPath();
                        validIndex = true;

                        return this;
                    }
                    catch (Exception se) { continue; }
                }
                return null;
            }
            return null;
        } // end of run
    } // end of class DirectoryList

    /**
     * Helper method returning the "best-effort-most-accurate" path.
     *
     * @param file the file to get the path to
     * @return The file path, either ala {@code File.getCanonicalPath} or
     *      {@code File.getPath}.
     */
    private static String getMostAccuratePath(StorageFile file) {
        String path = file.getPath();
        try {
            path = file.getCanonicalPath();
        } catch (IOException ioe) {
            // Ignore this, use path from above.
        }
        return path;
    }

    /**
     * Helper class for common file operations on the service properties files.
     * <p>
     * Introduced to take care of error reporting for common file operations
     * carried out in StorageFactoryService.
     */
    //@NotThreadSafe
    private static class FileOperationHelper {
        /** Name of the most recently performed operation. */
        private String operation;

        boolean exists(StorageFile file, boolean mustSucceed)
                throws StandardException {
            operation = "exists";
            boolean ret = false;
            try {
                ret = file.exists();
            } catch (SecurityException se) {
                handleSecPrivException(file, mustSucceed, se);
            }
            return ret;
        }

        boolean delete(StorageFile file, boolean mustSucceed)
                throws StandardException {
            operation = "delete";
            boolean deleted = false;
            try {
                deleted = file.delete();
            } catch (SecurityException se) {
                handleSecPrivException(file, mustSucceed, se);
            }
            if (mustSucceed && !deleted) {
                throw StandardException.newException(
                        SQLState.UNABLE_TO_DELETE_FILE, file.getPath());   
            }
            return deleted;
        }

        boolean renameTo(StorageFile from, StorageFile to, boolean mustSucceed)
                throws StandardException {
            operation = "renameTo";
            // Even if the explicit delete fails, the rename may succeed.
            delete(to, false);
            boolean renamed = false;
            try {
                renamed = from.renameTo(to);
            } catch (SecurityException se) {
                StorageFile file = to;
                try {
                    // We got a security exception, assume a secman is present.
                    System.getSecurityManager().checkWrite(from.getPath());
                } catch (SecurityException se1) {
                    file = from;
                }
                handleSecPrivException(file, mustSucceed, se);
            }
            if (mustSucceed && !renamed) {
                throw StandardException.newException(
                        SQLState.UNABLE_TO_RENAME_FILE,
                        from.getPath(), to.getPath());
            }
            return renamed;
        }
        
        /**
         * Handles security exceptions caused by missing privileges on the
         * files being accessed.
         *
         * @param file the file that was accessed
         * @param mustSucceed if {@code true} a {@code StandardException} will
         *      be thrown, if {@code false} a warning is written to the log
         * @param se the security exception raised
         * @throws StandardException if {@code mustSucceed} is {@code true}
         * @throws NullPointerException if {@code file} or {@code se} is null
         */
        private void handleSecPrivException(StorageFile file,
                                            boolean mustSucceed,
                                            SecurityException se)
                throws StandardException {
            if (mustSucceed) {
                throw StandardException.newException(
                        SQLState.MISSING_FILE_PRIVILEGE, se, operation,
                        file.getName(), se.getMessage());
            } else {
                Monitor.getStream().printlnWithHeader(
                        MessageService.getTextMessage(
                        SQLState.MISSING_FILE_PRIVILEGE, operation,
                        getMostAccuratePath(file), se.getMessage())); 
            }
        }
    } // End of static class FileOperationHelper
}
