/*

   Derby - Class org.apache.derby.iapi.services.property.PropertySetCallback

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.daemon.Serviceable;
import java.io.Serializable;
import java.util.Dictionary;

public interface PropertySetCallback {

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
