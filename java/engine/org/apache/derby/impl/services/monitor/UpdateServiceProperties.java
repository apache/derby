/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.monitor
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.monitor;

import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Properties;
import java.util.Hashtable;
import org.apache.derby.io.WritableStorageFactory;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.PassThroughException;
import org.apache.derby.iapi.reference.Property;

/**
*/
public class UpdateServiceProperties extends Properties {

	private PersistentService serviceType;
	private String serviceName;
    private WritableStorageFactory storageFactory;
    
	/*
	Fix for bug 3668: Following would allow user to change properties while in the session
	in which the database was created.
	While the database is being created, serviceBooted would be false. What that means
	is, don't save changes into services.properties file from here until the database
	is created. Instead, let BaseMonitor save the properties at the end of the database
  creation and also set serviceBooted to true at that point. From then on, the
  services.properties file updates will be made here.
	*/
	private boolean serviceBooted;

	public UpdateServiceProperties(PersistentService serviceType, String serviceName,
	Properties actualSet, boolean serviceBooted) {
		super(actualSet);
		this.serviceType = serviceType;
		this.serviceName = serviceName;
		this.serviceBooted = serviceBooted;
	}

	//look at the comments for serviceBooted at the top to understand this.
	public void setServiceBooted() {
		serviceBooted = true;
	}

    public void setStorageFactory( WritableStorageFactory storageFactory)
    {
        this.storageFactory = storageFactory;
    }

    public WritableStorageFactory getStorageFactory()
    {
        return storageFactory;
    }
    
	/*
	** Methods of Hashtable (overridden)
	*/

	/**	
		Put the key-value pair in the Properties set and
		mark this set as modified.

		@see Hashtable#put
	*/
	public Object put(Object key, Object value) {
		Object ref = defaults.put(key, value);
		if (!((String) key).startsWith(Property.PROPERTY_RUNTIME_PREFIX))
			update();
		return ref;
	}

	/**	
		Remove the key-value pair from the Properties set and
		mark this set as modified.

		@see Hashtable#remove
	*/
	public Object remove(Object key) {
		Object ref = defaults.remove(key);
		if ((ref != null) &&
			(!((String) key).startsWith(Property.PROPERTY_RUNTIME_PREFIX)))
			update();
		return ref;
	}

	/**
	   Saves the service properties to the disk.
	 */
	public void saveServiceProperties()
	{
        if( SanityManager.DEBUG)
            SanityManager.ASSERT( storageFactory != null,
                                  "UpdateServiceProperties.saveServiceProperties() called before storageFactory set.");
		try{
			serviceType.saveServiceProperties(serviceName, storageFactory,
					BaseMonitor.removeRuntimeProperties(defaults), false);
		} catch (StandardException mse) {
			throw new PassThroughException(mse);
		}
	}

	/*
	** Class specific methods.
	*/

	private void update() {

		try {
			//look at the comments for serviceBooted at the top to understand this if.
			if (serviceBooted)
				serviceType.saveServiceProperties(serviceName, storageFactory,
					BaseMonitor.removeRuntimeProperties(defaults), true);
		} catch (StandardException mse) {
			throw new PassThroughException(mse);
		}
	}

}
