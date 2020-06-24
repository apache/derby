/*

   Derby - Class org.apache.impl.storeless.StorelessService

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
package org.apache.derby.impl.storeless;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.EngineType;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.io.StorageFactory;

/**
 * PersistentService for the storeless engine.
 * Type is 'storeless' which will correspond to
 * the JDBC URL 'jdbc:derby:storeless'.
 *
 */
public class StorelessService implements PersistentService {
	
	public StorelessService()
	{
	}
	
	public String getType() {
		return "storeless";
	}

	public Enumeration getBootTimeServices() {
		return null;
	}

	public Properties getServiceProperties(String serviceName, Properties defaultProperties) throws StandardException {
		
		Properties service = new Properties(defaultProperties);
		service.setProperty(Property.SERVICE_PROTOCOL,
                "org.apache.derby.database.Database");
		service.setProperty(EngineType.PROPERTY,
                Integer.toString(getEngineType()));
		return service;
	}

	public void saveServiceProperties(String serviceName, StorageFactory storageFactory, Properties properties, boolean replace) throws StandardException {
		// Auto-generated method stub
		
	}

    public void saveServiceProperties(String serviceName,
//IC see: https://issues.apache.org/jira/browse/DERBY-5260
                                      Properties properties)
            throws StandardException {
		// Auto-generated method stub
		
	}

	public String createServiceRoot(String name, boolean deleteExisting) throws StandardException {
		// Auto-generated method stub
		return null;
	}

	public boolean removeServiceRoot(String serviceName) {
		// Auto-generated method stub
		return false;
	}

	public String getCanonicalServiceName(String name) {
		return name;
	}

	public String getUserServiceName(String serviceName) {
		// Auto-generated method stub
		return null;
	}

	public boolean isSameService(String serviceName1, String serviceName2) {
		// Auto-generated method stub
		return serviceName1.equals(serviceName2);
	}

	public boolean hasStorageFactory() {
		// Auto-generated method stub
		return false;
	}

	public StorageFactory getStorageFactoryInstance(boolean useHome, String databaseName, String tempDirName, String uniqueName) throws StandardException, IOException {
		// Auto-generated method stub
		return null;
	}

    protected int getEngineType() {
        return EngineType.STORELESS_ENGINE;
    }

    /** @see PersistentService#createDataWarningFile */
    public void createDataWarningFile(StorageFactory sf) 
            throws StandardException {
        // Auto-generated method stub
    }
}
