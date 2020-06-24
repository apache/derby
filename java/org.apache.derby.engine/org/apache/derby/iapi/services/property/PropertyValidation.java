/*

   Derby - Class org.apache.derby.iapi.services.property.PropertyValidation

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

import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.property.PropertySetCallback;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import java.io.Serializable;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

public class PropertyValidation implements PropertyFactory
{
	private Vector<PropertySetCallback>  notifyOnSet;

    /* Constructors for This class: */
	public PropertyValidation()
	{

	}

	public Serializable doValidateApplyAndMap(TransactionController tc,
											 String key, Serializable value,
											 Dictionary d, boolean dbOnlyProperty)
		 throws StandardException
	{
		Serializable mappedValue = null;
 		if (notifyOnSet != null) {
			synchronized (this) {

				for (int i = 0; i < notifyOnSet.size() ; i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
					PropertySetCallback psc = notifyOnSet.get(i);
					if (!psc.validate(key, value, d))
						continue;

					if (mappedValue == null)
 						mappedValue = psc.map(key, value, d);

					// if this property should not be used then
					// don't call apply. This depends on where
					// the old value comes from
					// SET_IN_JVM - property will not be used
					// SET_IN_DATABASE - propery will be used
					// SET_IN_APPLICATION - will become SET_IN_DATABASE
					// NOT_SET - will become SET_IN_DATABASE

					if (!dbOnlyProperty && key.startsWith("derby.")) {
						if (PropertyUtil.whereSet(key, d) == PropertyUtil.SET_IN_JVM)
							continue;
					}

					Serviceable s;
					if ((s = psc.apply(key,value,d)) != null)
						((TransactionManager) tc).addPostCommitWork(s);
				}
			}
		}
		return mappedValue;
	}
	/**
	  Call the property set callbacks to map a proposed property value
	  to a value to save.
	  <P>
	  The caller must run this in a block synchronized on this
	  to serialize validations with changes to the set of
	  property callbacks
	  */
	public Serializable doMap(String key,
							 Serializable value,
							 Dictionary set)
		 throws StandardException
	{
		Serializable mappedValue = null;
 		if (notifyOnSet != null) {
			for (int i = 0; i < notifyOnSet.size() && mappedValue == null; i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
				PropertySetCallback psc = notifyOnSet.get(i);
				mappedValue = psc.map(key, value, set);
			}
		}

		if (mappedValue == null)
			return value;
		else
			return mappedValue;
	}

	public void validateSingleProperty(String key,
						  Serializable value,
						  Dictionary set)
		 throws StandardException
	{
		// RESOLVE: log device cannot be changed on the fly right now
		if (key.equals(Attribute.LOG_DEVICE))
        {
			throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_CHANGE_LOGDEVICE);
        }

 		if (notifyOnSet != null) {
			for (int i = 0; i < notifyOnSet.size(); i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
				PropertySetCallback psc = notifyOnSet.get(i);
				psc.validate(key, value, set);
			}
		}
	}

	public synchronized void addPropertySetNotification(PropertySetCallback who){

		if (notifyOnSet == null)
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			notifyOnSet = new Vector<PropertySetCallback>(1,1);
		notifyOnSet.add(who);
//IC see: https://issues.apache.org/jira/browse/DERBY-5060

	}

	public synchronized void verifyPropertySet(Properties p,Properties ignore)
		 throws StandardException
	{
		for (Enumeration e = p.propertyNames(); e.hasMoreElements();)
		{
			String pn = (String)e.nextElement();
			//
			//Ignore the ones we are told to ignore.
			if (ignore.getProperty(pn) != null) continue;
			Serializable pv = p.getProperty(pn);
			validateSingleProperty(pn,pv,p);
		}
	}
}



