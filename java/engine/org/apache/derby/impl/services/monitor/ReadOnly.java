/*

   Derby - Class org.apache.derby.impl.services.monitor.ReadOnly

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.store.raw.data.DataFactory;

import org.apache.derby.io.StorageFactory;

import java.util.Properties;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.IOException;

public abstract class ReadOnly implements PersistentService {

	protected final Properties readProperties(InputStream is, Properties defaultProperties) throws StandardException {

		try {

			Properties serviceProperties = new Properties(defaultProperties);
			serviceProperties.load(new BufferedInputStream(is, 1024));

			return serviceProperties;

		} catch (IOException ioe) {
			throw Monitor.exceptionStartingModule(ioe);
		} finally {
			try {
				is.close();
			} catch (IOException ioe) {
			}
		}
	}

	/**
		@exception StandardException Properties cannot be saved.
	*/

	public void saveServiceProperties(String serviceName, StorageFactory storageFactory,Properties properties, boolean replace)
		throws StandardException {
		throw StandardException.newException(SQLState.READ_ONLY_SERVICE);
	}

	/**
       Save to a backup file.
       
		@exception StandardException Properties cannot be saved.
	*/
	public void saveServiceProperties(String serviceName,
                                      Properties properties,
                                      boolean replace)
		throws StandardException {
		throw StandardException.newException(SQLState.READ_ONLY_SERVICE);
	}

	/**
		Properties cannot be saved
	*/
	public String createServiceRoot(String name, boolean deleteExisting)
		throws StandardException {
		throw StandardException.newException(SQLState.READ_ONLY_SERVICE);
	}

	public boolean removeServiceRoot(String serviceName)  {
		return false;
	}

    /**
     * @return true if the PersistentService has a StorageFactory, false if not.
     */
    public boolean hasStorageFactory()
    {
        return false;
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
     */
    public StorageFactory getStorageFactoryInstance(boolean useHome,
                                                    String databaseName,
                                                    String tempDirName,
                                                    String uniqueName)
    {
        return null;
    }
}
