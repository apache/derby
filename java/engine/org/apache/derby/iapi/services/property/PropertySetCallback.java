/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.property
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.property;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.daemon.Serviceable;
import java.io.Serializable;
import java.util.Dictionary;

public interface PropertySetCallback { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	/**
		Initialize the properties for this callback.
		Called when addPropertySetNotification() is called
		with a non-null transaction controller.
		This allows code to set read its initial property
		values at boot time.

		<P>
		Code within an init() method should use the 3 argument
		PropertyUtil method getPropertyFromSet() to obtain a property's value.

		@param dbOnly true if only per-database properties are to be looked at
		@param p the complete set of per-database properties.
	*/ 
	void init(boolean dbOnly, Dictionary p);

	/**
	  Validate a property change.
	  @param key Property key for the property being set
	  @param value proposed new value for the property being set or null if
	         the property is being dropped.
	  @param p Property set before the change. SettingProperty may read but
	         must never change p.

	  @return true if this object was interested in this property, false otherwise.
	  @exception StandardException Oh well.
	*/
    boolean validate(String key, Serializable value, Dictionary p)
		 throws StandardException;
	/**
	  Apply a property change. Will only be called after validate has been called
	  and only if validate returned true. If this method is called then the
	  new value is the value to be used, ie. the property is not set in the
	  overriding JVM system set.

	  @param key Property key for the property being set
	  @param value proposed new value for the property being set or null if
	         the property is being dropped.
	  @param p Property set before the change. SettingProperty may read but
	         must never change p.
	  @return post commit work for the property change.
	  @exception StandardException Oh well.
	*/
    Serviceable apply(String key, Serializable value, Dictionary p)
		 throws StandardException;
	
	/**

	  Map a proposed new value for a property to an official value.

	  Will only be called after apply() has been called.
	  @param key Property key for the property being set
	  @param value proposed new value for the property being set or null if
	         the property is being dropped.
	  @param p Property set before the change. SettingProperty may read but
	         must never change p.
	  @return new value for the change
	  @exception StandardException Oh well.
	*/
    Serializable map(String key, Serializable value, Dictionary p)
		 throws StandardException;
}
