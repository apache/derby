/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.property
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.property;

import java.util.Properties;

import java.io.Serializable;

import org.apache.derby.iapi.error.StandardException;

public interface PersistentSet
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
    /**
     * Gets a value for a stored property. The returned value will be:
	 *
	 * <OL>
	 * <LI> the de-serialized object associated with the key
	 *      using setProperty if such a value is defined or
	 * <LI> the default de-serialized object associated with
	 *      the key using setPropertyDefault if such a value
	 *      is defined or
	 * <LI> null
	 * </OL>
	 *      
     * <p>
     * The Store provides a transaction protected list of database properties.
     * Higher levels of the system can store and retrieve these properties
     * once Recovery has finished. Each property is a serializable object
     * and is stored/retrieved using a String key.
     * <p>
     *
     * @param key     The "key" of the property that is being requested.
     *
	 * @return object The requested object or null.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Serializable getProperty(
    String key) 
        throws StandardException;

    /**
     * Gets a default value for a stored property. The returned
	 * value will be:
	 *
	 * <OL>
	 * <LI> the default de-serialized object associated with
	 *      the key using setPropertyDefault if such a value
	 *      is defined or
	 * <LI> null
	 * </OL>
	 *      
     * <p>
     * The Store provides a transaction protected list of database properties.
     * Higher levels of the system can store and retrieve these properties
     * once Recovery has finished. Each property is a serializable object
     * and is stored/retrieved using a String key.
     * <p>
     *
     * @param key     The "key" of the property that is being requested.
     *
	 * @return object The requested object or null.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Serializable getPropertyDefault(
    String key) 
        throws StandardException;


    /**
     * Return true if the default property is visible. A default
	 * is visible as long as the property is not set.
     *
     * @param key     The "key" of the property that is being requested.
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean propertyDefaultIsVisible(String key) throws StandardException;

    /**
     * Sets the Serializable object associated with a property key.
     * <p>
     * See the discussion of getProperty().
     * <p>
     * The value stored may be a Formatable object or a Serializable object
	 * whose class name starts with java.*. This stops arbitary objects being
	 * stored in the database by class name, which will cause problems in
	 * obfuscated/non-obfuscated systems.
     *
	 * @param	key		The key used to lookup this property.
	 * @param	value	The value to be associated with this key. If null, 
     *                  delete the property from the properties list.
	   @param   dbOnlyProperty True if property is only ever searched for int the database properties.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void	setProperty(
    String          key, 
    Serializable    value,
	boolean dbOnlyProperty) 
        throws StandardException;

    /**
     * Sets the Serializable object default value associated with a property
	 * key.
     * <p>
     * See the discussion of getProperty().
     * <p>
     * The value stored may be a Formatable object or a Serializable object
	 * whose class name starts with java.*. This stops arbitary objects being
	 * stored in the database by class name, which will cause problems in
	 * obfuscated/non-obfuscated systems.
     *
	 * @param	key		The key used to lookup this propertyDefault.
	 * @param	value	The default value to be associated with this key. 
     *                  If null, delete the property default from the
	 *                  properties list.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void	setPropertyDefault(
    String          key, 
    Serializable    value) 
        throws StandardException;

    /**
     * Get properties that can be stored in a java.util.Properties object.
     * <p>
	 * Get the sub-set of stored properties that can be stored in a 
     * java.util.Properties object. That is all the properties that have a
     * value of type java.lang.String.  Changes to this properties object are
     * not reflected in any persisent storage.
     * <p>
     * Code must use the setProperty() method call.
     *
	 * @return The sub-set of stored properties that can be stored in a 
     *         java.util.Propertes object.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public Properties getProperties() 
        throws StandardException;
}
