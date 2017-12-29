/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_CachedInteger

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

package org.apache.derbyTesting.unitTests.services;

import org.apache.derby.iapi.services.cache.*;

import org.apache.derby.shared.common.error.StandardException;

/**

*/
public class T_CachedInteger extends T_Cacheable {

	protected T_Key		keyValue;
		
	public T_CachedInteger() {
	}

	/*
	** Cacheable methods
	*/


	/**
		@exception StandardException  Standard Derby Error policy
	*/
	public Cacheable setIdentity(Object key) throws StandardException {

		super.setIdentity(key);

		T_Key tkey = (T_Key) key;	// instanceof check provided by superclass

		if (!(tkey.getValue() instanceof Integer)) {

			return getCorrectObject(tkey.getValue()).setIdentity(key);
		}

		// potentially pretend to wait and potentally behave as not found.
		if (!dummySet(tkey))
			return null;
		keyValue = tkey;

		return this;
	}

	/**
		@exception StandardException  Standard Derby Error policy
	*/
	public Cacheable createIdentity(Object key, Object createParameter) throws StandardException {
		super.createIdentity(key, createParameter);

		T_Key tkey = (T_Key) key;	// instanceof check provided by superclass

		if (!(tkey.getValue() instanceof Integer)) {

			return getCorrectObject(tkey.getValue()).createIdentity(key, createParameter);
		}


		// potentially pretend to wait and potentally behave as not found.
		if (!dummySet(tkey))
			return null;

		keyValue = tkey;

		return this;
	}



	/**
		Put the object into the No Identity state. 

		<BR> MT - single thread required - Method must only be called be cache manager
		and the cache manager will guarantee only one thread can be calling it.

	*/
	public void clearIdentity() {
		keyValue = null;
	}

	/**
		Get the identity of this object.

		<BR> MT - thread safe.

	*/
	public Object getIdentity() {
		return keyValue;
	}

	/**
		@exception StandardException  Standard Derby Error policy
	*/
	public void clean(boolean forRemove) throws StandardException {
		super.clean(forRemove);
	}
}

