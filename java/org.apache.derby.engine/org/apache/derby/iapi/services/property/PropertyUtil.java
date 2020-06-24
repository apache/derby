/*

   Derby - Class org.apache.derby.iapi.services.property.PropertyUtil

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

package org.apache.derby.iapi.services.property;

import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.EngineType;
import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.iapi.util.IdUtil;

import java.util.Properties;
import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Dictionary;
import java.util.Enumeration;

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
	private static final String[] servicePropertyList = {
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
			if (getMonitor().getJVMProperty(key) != null) {
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
                    (value != null ? value.trim() : null)).booleanValue();
//IC see: https://issues.apache.org/jira/browse/DERBY-673

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
     * Get the list of properties which are normally stored in service.properties
     *
     * @return the list of properties which are normally stored in service.propertie
     */
    public  static  String[]    getServicePropertyList()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6166
        return ArrayUtil.copy( servicePropertyList );
    }
	
	/**
		Find a system wide property.
//IC see: https://issues.apache.org/jira/browse/DERBY-623

        @param key The key to lookup
		@return the value of the property or null if it does not exist.
		@see #getSystemProperty(String,String)
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2400
		This method can be used by a system that is not running Derby,
		just to maintain the same lookup logic and security manager concerns
		for finding derby.properties and reading system properties.

        @param key The propertykey
        @param defaultValue A default value
		@return the value of the property or defaultValue if it does not exist.
	*/
	public static String getSystemProperty(String key, String defaultValue) {

		ModuleFactory monitor = getMonitorLite();
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

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

        @param set The property set to inspect
        @param key The key to lookup
        @return the value bound to the key
	*/
	public static String getPropertyFromSet(Properties set, String key) {
	
		boolean dbOnly = set != null ? isDBOnly(set) : false;

        //
        // Once NATIVE authentication has been set in the database, it cannot
        // be overridden.
        //
        if ( Property.AUTHENTICATION_PROVIDER_PARAMETER.equals( key ) )
        {
            String  dbValue = PropertyUtil.getPropertyFromSet( true, set, key );

            if ( nativeAuthenticationEnabled( dbValue ) ) { return dbValue; }
        }

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
				value = getMonitor().getJVMProperty(key);
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
				value = getMonitor().getJVMProperty(key);
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

        @param set The property set to inspect
        @param key The key to lookup
        @return the value bound to the key
		@exception StandardException Standard Derby error handling. 
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

        @param set The property set to inspect
        @param key The key to lookup
        @param defaultValue The default value to use
		@return the value of the property or defaultValue if it does not exist.

		@exception StandardException Standard Derby error handling. 
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
			value = getMonitor().getJVMProperty(key);
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

        @param set The property set to inspect
        @param key The key to lookup
		@return the value of the property or null if it does not exist.

        @exception StandardException Standard Derby error handling. 
	*/
	public static String getServiceProperty(PersistentSet set, String key)
		throws StandardException {
		return PropertyUtil.getServiceProperty(set, key, (String) null);
	}

	/**
		Get a system wide property as a boolean.

		@param key The name of the system property
		@return true of the property is set to 'true, TRUE', false otherwise
	*/
	public static boolean getSystemBoolean(String key) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3388
		return getSystemBoolean(key, false);
	}

    /**
     * Get a system wide property as a boolean.
     *
     * @param key The name of the system property
     * @param defaultValue The value to use if the property has not been set
     * @return return defaultValue if the property is not set, returns true if
     *  the property is set to 'true, TRUE', returns false otherwise.
     */
    public static boolean getSystemBoolean(String key, boolean defaultValue) {

        String value = PropertyUtil.getSystemProperty(key);
        if (value == null) {
            return defaultValue;
        } else {
            return (Boolean.valueOf(value.trim()).booleanValue());
        }
    }

	/**
		Get a service wide property as a boolean.

        @param set The property set to inspect
        @param key The key to lookup
        @param defValue The default value to use
		@return true of the property is set to 'true, TRUE', false otherwise

		@exception StandardException Standard Derby error handling. 
	*/
	public static boolean getServiceBoolean(PersistentSet set, String key, boolean defValue) 
		throws StandardException {

        String value = PropertyUtil.getServiceProperty(set, key);

		return booleanProperty(key, value, defValue);
	}

	/**s
		Get a system wide property as a int.

        @param key The key to lookup
        @param min The min acceptable value
        @param max The max acceptable value
        @param defaultValue The value to use if the key has no match
		@return value of the property if set subject to min and max, defaultValue if
		it is not set or set to a non-integer value.
	*/
	public static int getSystemInt(String key, int min, int max, int defaultValue) {
		return PropertyUtil.handleInt(PropertyUtil.getSystemProperty(key), min, max, defaultValue);
	}

	/**
		Get a service wide property as a int.

        @param set The property set
        @param key The key to lookup
        @param min The min acceptable value
        @param max The max acceptable value
        @param defaultValue The value to use if the key has no match
		@return value of the property if set subject to min and max, defaultValue if
		it is not set or set to a non-integer value.

		@exception StandardException Standard Derby error handling. 

	*/
	public static int getServiceInt(PersistentSet set, String key, int min, int max, int defaultValue)
		throws StandardException {
		//return PropertyUtil.intPropertyValue(key, PropertyUtil.getServiceProperty(set, key), min, max, defaultValue);
		return PropertyUtil.handleInt(PropertyUtil.getServiceProperty(set, key), min, max, defaultValue);
	}

	/**
		Get a service wide property as a int. The passed in Properties
		set overrides any system, applcation or per-database properties.

        @param set The target property set
        @param props The properties
        @param key The key to lookup
        @param min The min acceptable value
        @param max The max acceptable value
        @param defaultValue The value to use if the key has no match
		@return value of the property if set subject to min and max, defaultValue if
		it is not set or set to a non-integer value.

		@exception StandardException Standard Derby error handling. 

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

        @param key The key to lookup
        @param defaultValue The value to use if the key has no match
		@return value of the property if, defaultValue if
		it is not set or set to a non-integer value.
	*/
	public static int getSystemInt(String key, int defaultValue) {
		return PropertyUtil.getSystemInt(key, 0, Integer.MAX_VALUE, defaultValue);
	}

	/**
		Parse an string as an int based property value.

        @param value The string value to be parsed as an int
        @param min The minimum legal value
        @param max The maximum legal value
        @param defaultValue The defaultValue to set
        @return the final value
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

      @param p The property name
      @param v The desired value
      @param defaultValue The default value to use
      @return The final value decided on
	  @exception StandardException Oops
	  */
	public static boolean booleanProperty(String p, Serializable v, boolean defaultValue)
		 throws StandardException
	{
		if (v==null)
			return defaultValue;

		String vS = ((String) v).trim();

//IC see: https://issues.apache.org/jira/browse/DERBY-3147
		if ("TRUE".equals(StringUtil.SQLToUpperCase(vS)))
			return true;
        if ("FALSE".equals(StringUtil.SQLToUpperCase(vS)))
			return false;

		throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE, p,vS);
	}

	/**
	  Parse, validate and return an integer property value. If the value is invalid
	  raise an exception. If the value passed in is null return a default value.

      @param p The property name
      @param v The desired value
      @param minValue The minimum legal value
      @param maxValue The maximum legal value
      @param defaultValue The default value to use
      @return The final value decided on
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

      @param key The name of a property
      @return true if that property is stored in service.properties
	  */ 
	public static boolean isServiceProperty(String key)
	{
		for (int i = 0; i < PropertyUtil.servicePropertyList.length; i++) 
			if (key.equals(PropertyUtil.servicePropertyList[i])) return true;
		return false;
	}


	/**
	 * Return {@code true} if {@code username} is defined as a built-in user
	 * i.e. there exists a property {@code derby.user.}&lt;userid&gt; in the
	 * database (or, possibly, in system properties if not forbidden by {@code
	 * derby.database.propertiesOnly}). Note that &lt;userid&gt; found in a
	 * property will be normalized to case normal form before comparison is
	 * performed against username, which is presumed normalized already.
	 *
	 * @param set object which implements PersistentSet interface
	 *        (TransactionController)
	 * @param username Normalized authorization identifier
	 *
	 * @return {@code true} if match found
	 *
	 * @exception StandardException on error
	 */
	public static boolean existsBuiltinUser (
//IC see: https://issues.apache.org/jira/browse/DERBY-3673
		PersistentSet set,
		String username)
			throws StandardException
	{
		if (propertiesContainsBuiltinUser(set.getProperties(), username)) {
			return true;
		}
		
		// check system level propery, if allowed by
		// derby.database.propertiesOnly
		boolean dbOnly = false;
		dbOnly = Boolean.valueOf(
			PropertyUtil.getDatabaseProperty(
				set,
				Property.DATABASE_PROPERTIES_ONLY)).booleanValue();

		if (!dbOnly &&
				systemPropertiesExistsBuiltinUser(username)){
			return true;
		}

		return false;
	}

	/**
     * Return true if NATIVE authentication has been enabled in the passed-in properties.
     *
     * @param properties A set of properties
     * @return true if NATIVE authentication has been enabled in the passed-in properties
     */
	public static boolean nativeAuthenticationEnabled( Properties properties )
    {
		String authenticationProvider = getPropertyFromSet
            (
             properties,
             Property.AUTHENTICATION_PROVIDER_PARAMETER
             );

        return nativeAuthenticationEnabled( authenticationProvider );
	}

	/**
     * Return true if NATIVE authentication is turned on for the passed-in
     * value of Property.AUTHENTICATION_PROVIDER_PARAMETER.
     *
     * @param authenticationProvider An authentication service spec
     * @return true if NATIVE authentication is turned in that spec
     */
	private static boolean nativeAuthenticationEnabled( String authenticationProvider )
    {
        if ( authenticationProvider ==  null ) { return false; }

        return StringUtil.SQLToUpperCase( authenticationProvider ).startsWith( Property.AUTHENTICATION_PROVIDER_NATIVE );
    }
    
	/**
		Return true if the passed-in properties specify NATIVE authentication using LOCAL credentials.

        @param properties A set of properties
        @return true if the passed-in properties specify NATIVE authentication using LOCAL credentials
	*/
	public static boolean localNativeAuthenticationEnabled( Properties properties )
    {
        if ( ! nativeAuthenticationEnabled( properties ) ) { return false; }
        
		String authenticationProvider = getPropertyFromSet
            (
             properties,
             Property.AUTHENTICATION_PROVIDER_PARAMETER
             );

        return StringUtil.SQLToUpperCase( authenticationProvider ).endsWith
            ( Property.AUTHENTICATION_PROVIDER_LOCAL_SUFFIX );
	}


	/**
	 * Return true if username is defined as a system property
	 * i.e. there exists a property {@code derby.user.}&lt;userid&gt;
	 * in the system properties. Note that &lt;userid&gt; will be
	 * normalized to case normal form before comparison is performed
	 * against username, which is presumed normalized already.
	 * @param username Normalized authorization identifier
	 * @return {@code true} if match found
	 */
	private static boolean systemPropertiesExistsBuiltinUser(String username)
	{
		ModuleFactory monitor = getMonitorLite();
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

		try {
			Properties JVMProperties = System.getProperties();

			if (propertiesContainsBuiltinUser(JVMProperties, username)) {
				return true;
			}
		} catch (SecurityException e) {
			// Running with security manager and we can't get at all
			// JVM properties, to try to map the back the authid to
			// how the user may have specified a matching id (1->many,
			// since userids are subject to SQL up-casing).
			String key= Property.USER_PROPERTY_PREFIX +
				IdUtil.SQLIdentifier2CanonicalPropertyUsername(username);

			if (monitor.getJVMProperty(key) != null) {
				return true;
			}
		}

		Properties applicationProperties = monitor.getApplicationProperties();

		return propertiesContainsBuiltinUser(applicationProperties, username);
	}

	private static boolean propertiesContainsBuiltinUser(Properties props,
														 String username)
	{
		if (props != null) {
			Enumeration e = props.propertyNames();
		
			while (e.hasMoreElements()) {
				String p = (String)e.nextElement();

				if (p.startsWith(Property.USER_PROPERTY_PREFIX)) {
					String userAsSpecified = StringUtil.normalizeSQLIdentifier(
						p.substring(Property.USER_PROPERTY_PREFIX.length()));

					if (username.equals(userAsSpecified)) {
						return true;
					}
				}
			}
		}

		return false;
	}
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
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

    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitorLite()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitorLite();
                 }
             }
             );
    }

}

