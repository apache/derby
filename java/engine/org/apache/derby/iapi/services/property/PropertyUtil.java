/*

   Derby - Class org.apache.derby.iapi.services.property.PropertyUtil

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

package org.apache.derby.iapi.services.property;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.EngineType;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.util.StringUtil;

import java.util.Properties;
import java.io.Serializable;
import java.util.Dictionary;

/**
	There are 5 property objects within a JBMS system.

	1) JVM - JVM set - those in System.getProperties
	2) APP - Application set - derby.properties file
	3) SRV - Persistent Service set - Those stored in service.properties
	4) TRAN - Persistent Transactional set - Those stored via the AccessManager interface
	5) BOOT - Set by a boot method (rare)

	This class has a set of static methods to find a property using a consistent search order
	from the above set.
	<BR>
	getSystem*() methods use the search order.
	<OL>
	<LI> JVM
	<LI> APP
	</OL>
	<BR>
	getService* methods use the search order
	<OL>
	<LI> JVM
	<LI> TRAN
	<LI> SRV
	<LI> APP
	</OL>

*/
public class PropertyUtil {

	// List of properties that are stored in the service.properties file
	public static final String[] servicePropertyList = {
		EngineType.PROPERTY,
		Property.NO_AUTO_BOOT,
		Property.STORAGE_TEMP_DIRECTORY,
        Attribute.CRYPTO_PROVIDER,
        Attribute.CRYPTO_ALGORITHM,
		Attribute.RESTORE_FROM,
		Attribute.LOG_DEVICE,
		Property.LOG_ARCHIVE_MODE
	};

	/**
		Property is set in JVM set
	*/
	public static final int SET_IN_JVM = 0;	
	/**
		Property is set in DATABASE set
	*/
	public static final int SET_IN_DATABASE = 1;
	/**
		Property is set in APPLICATION (derby.properties) set
	*/
	public static final int SET_IN_APPLICATION = 2;

	/**
		Property is not set.
	*/
	public static final int NOT_SET = -1;


	static int whereSet(String key, Dictionary set) {

		boolean dbOnly = isDBOnly(set);

		if (!dbOnly) {
			if (Monitor.getMonitor().getJVMProperty(key) != null) {
				return SET_IN_JVM;
			}
		}
		
		if ((set != null) && (set.get(key) != null))
				return SET_IN_DATABASE;

		if (!dbOnly) {
			if (PropertyUtil.getSystemProperty(key) != null)
				return SET_IN_APPLICATION;
		}

		return NOT_SET;
	}

	public static boolean isDBOnly(Dictionary set) {

		if (set == null)
			return false;

		String value = (String) set.get(Property.DATABASE_PROPERTIES_ONLY);

		boolean dbOnly = Boolean.valueOf(
                    (value != null ? value.trim() : value)).booleanValue();

		return dbOnly;
	}

	public static boolean isDBOnly(Properties set) {

		if (set == null)
			return false;

		String value = set.getProperty(Property.DATABASE_PROPERTIES_ONLY);

		boolean dbOnly = Boolean.valueOf(
                    (value != null ? value.trim() : value)).booleanValue();

		return dbOnly;
	}
	
	/**
		Find a system wide property. Search order is

		@return the value of the property or null if it does not exist.
	*/
	public static String getSystemProperty(String key) {
		return PropertyUtil.getSystemProperty(key, (String) null);
	}

	/**
		Find a system wide property with a default. Search order is

		<OL>
		<LI> JVM property
		<LI> derby.properties
		</OL>

		<P>
		This method can be used by a system that is not running Cloudscape,
		just to maintain the same lookup logic and security manager concerns
		for finding derby.properties and reading system properties.

		@return the value of the property or defaultValue if it does not exist.
	*/
	public static String getSystemProperty(String key, String defaultValue) {

		ModuleFactory monitor = Monitor.getMonitorLite();

		String value = monitor.getJVMProperty(key);

		if (value == null) {

			Properties applicationProperties =
				monitor.getApplicationProperties();

			if (applicationProperties != null)
				value = applicationProperties.getProperty(key);
		}
		return value == null ? defaultValue : value;
	}


	/**
		Get a property from the passed in set. The passed in set is
		either:
		
		  <UL>
		  <LI> The properties object passed into ModuleControl.boot()
		  after the database has been booted. This set will be a DoubleProperties
		  object with the per-database transaction set as the read set
		  and the service.properties as the write set.
		  <LI>
		  The Dictionary set returned/passed in by a method of BasicService.Properties.
		  </UL>
		<BR>
		This method uses the same search order as the getService() calls.

	*/
	public static String getPropertyFromSet(Properties set, String key) {
	
		boolean dbOnly = set != null ? isDBOnly(set) : false;

		return PropertyUtil.getPropertyFromSet(dbOnly, set, key);
	}

	public static Serializable getPropertyFromSet(Dictionary set, String key) {
	
		boolean dbOnly = set != null ? isDBOnly(set) : false;

		return PropertyUtil.getPropertyFromSet(dbOnly, set, key);
	}

	public static Serializable getPropertyFromSet(boolean dbOnly, Dictionary set, String key) {

		if (set != null) {

			Serializable value;

			if (!dbOnly) {
				value = Monitor.getMonitor().getJVMProperty(key);
				if (value != null)
					return value;
			}
		
			value = (Serializable) set.get(key);
			if (value != null)
				return value;

			if (dbOnly)
				return null;
		}

		return PropertyUtil.getSystemProperty(key);
	}

	public static String getPropertyFromSet(boolean dbOnly, Properties set, String key) {

		if (set != null) {

			String value;

			if (!dbOnly) {
				value = Monitor.getMonitor().getJVMProperty(key);
				if (value != null)
					return value;
			}
		
			value = set.getProperty(key);
			if (value != null)
				return value;

			if (dbOnly)
				return null;
		}

		return PropertyUtil.getSystemProperty(key);
	}

	/**
		Get a property only looking in the Persistent Transactional (database) set.

		@exception StandardException Standard Cloudscape error handling. 
	*/
	public static String getDatabaseProperty(PersistentSet set, String key) 
		throws StandardException {

		if (set == null)
			return null;

		Object obj = set.getProperty(key);
 		if (obj == null) { return null; }
 		return obj.toString();
	}

	/**
		Find a service wide property with a default. Search order is

		The service is the persistent service associated with the
		current context stack.

		@return the value of the property or defaultValue if it does not exist.

		@exception StandardException Standard Cloudscape error handling. 
	*/
	public static String getServiceProperty(PersistentSet set, String key, String defaultValue) 
		throws StandardException {


		String value =
			PropertyUtil.getDatabaseProperty(
                set, Property.DATABASE_PROPERTIES_ONLY);

		boolean dbOnly = 
            Boolean.valueOf(
                (value != null ? value.trim() : value)).booleanValue();

		if (!dbOnly) {
			value = Monitor.getMonitor().getJVMProperty(key);
			if (value != null)
				return value;
		}

		value = PropertyUtil.getDatabaseProperty(set, key);
		if (value != null)
			return value;

		if (dbOnly) {
			return defaultValue;
		}

		return PropertyUtil.getSystemProperty(key, defaultValue);
	}


	/**
		Find a service wide property. 

		The service is the persistent service associated with the
		current context stack.

		@return the value of the property or null if it does not exist.

			@exception StandardException Standard Cloudscape error handling. 
	*/
	public static String getServiceProperty(PersistentSet set, String key)
		throws StandardException {
		return PropertyUtil.getServiceProperty(set, key, (String) null);
	}

	/**
		Get a system wide property as a boolean.

		@return true of the property is set to 'true, TRUE', false otherwise
	*/
	public static boolean getSystemBoolean(String key) {

        String value = PropertyUtil.getSystemProperty(key);

		return( 
            Boolean.valueOf(
                (value != null ? value.trim() : value)).booleanValue());
	}

	/**
		Get a service wide property as a boolean.

		@return true of the property is set to 'true, TRUE', false otherwise

		@exception StandardException Standard Cloudscape error handling. 
	*/
	public static boolean getServiceBoolean(PersistentSet set, String key, boolean defValue) 
		throws StandardException {

        String value = PropertyUtil.getServiceProperty(set, key);

		return booleanProperty(key, value, defValue);
	}

	/**s
		Get a system wide property as a int.

		@return value of the property if set subject to min and max, defaultValue if
		it is not set or set to a non-integer value.
	*/
	public static int getSystemInt(String key, int min, int max, int defaultValue) {
		return PropertyUtil.handleInt(PropertyUtil.getSystemProperty(key), min, max, defaultValue);
	}

	/**
		Get a service wide property as a int.

		@return value of the property if set subject to min and max, defaultValue if
		it is not set or set to a non-integer value.

		@exception StandardException Standard Cloudscape error handling. 

	*/
	public static int getServiceInt(PersistentSet set, String key, int min, int max, int defaultValue)
		throws StandardException {
		//return PropertyUtil.intPropertyValue(key, PropertyUtil.getServiceProperty(set, key), min, max, defaultValue);
		return PropertyUtil.handleInt(PropertyUtil.getServiceProperty(set, key), min, max, defaultValue);
	}

	/**
		Get a service wide property as a int. The passed in Properties
		set overrides any system, applcation or per-database properties.

		@return value of the property if set subject to min and max, defaultValue if
		it is not set or set to a non-integer value.

		@exception StandardException Standard Cloudscape error handling. 

	*/
	public static int getServiceInt(PersistentSet set, Properties props, String key, int min, int max, int defaultValue)
		throws StandardException {

		String value = null;

		if (props != null)
			value = props.getProperty(key);

		if (value == null)
			value = PropertyUtil.getServiceProperty(set, key);

		return PropertyUtil.handleInt(value, min, max, defaultValue);
	}

	/**
		Get a system wide property as a int.

		@return value of the property if, defaultValue if
		it is not set or set to a non-integer value.
	*/
	public static int getSystemInt(String key, int defaultValue) {
		return PropertyUtil.getSystemInt(key, 0, Integer.MAX_VALUE, defaultValue);
	}

	/**
		Parse an string as an int based property value.
	*/
	public static int handleInt(String value, int min, int max, int defaultValue) {

		if (value == null)
			return defaultValue;

		try {
			int intValue = Integer.parseInt(value);
			if ((intValue >= min) && (intValue <= max))
				return intValue;
		}
		catch (NumberFormatException nfe)
		{
			// just leave the default.
		}
		return defaultValue;
	}

	/**
	  Parse and validate and return a boolean property value. If the value is invalid
	  raise an exception.

	  <P>
	  The following are valid property values.
	  <UL>
	  <LI> null - returns defaultValue
	  <LI> "true" - returns true (in any case without the quotes)
	  <LI> "false" - return true (in any case without the quotes)
	  </UL>
	  @exception StandardException Oops
	  */
	public static boolean booleanProperty(String p, Serializable v, boolean defaultValue)
		 throws StandardException
	{
		if (v==null)
			return defaultValue;

		String vS = ((String) v).trim();
		if (StringUtil.SQLToLowerCase(vS).equals("true"))
			return true;
		if (StringUtil.SQLToLowerCase(vS).equals("false"))
			return false;

		throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE, p,vS);
	}

	/**
	  Parse, validate and return an integer property value. If the value is invalid
	  raise an exception. If the value passed in is null return a default value.

	  @exception StandardException Oops
	  */
	public static int intPropertyValue(String p, Serializable v,
									   int minValue, int maxValue, int defaultValue)
		 throws StandardException
	{
		if (v==null)
			return defaultValue;

		String vs = ((String)v).trim();
		try {
			int result = Integer.parseInt(vs);
			if (result < minValue || result > maxValue)
				throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE, p,vs);
			return result;
		}
		catch (NumberFormatException nfe) {
			throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE, p,vs);
		}
	}

	/**
	  Return true iff the key is the name of a database property that is 
	  stored in services.properties.
	  */ 
	public static boolean isServiceProperty(String key)
	{
		for (int i = 0; i < PropertyUtil.servicePropertyList.length; i++) 
			if (key.equals(PropertyUtil.servicePropertyList[i])) return true;
		return false;
	}
}

