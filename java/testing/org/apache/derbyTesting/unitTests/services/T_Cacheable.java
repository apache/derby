/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_Cacheable

   Copyright 1997, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.unitTests.services;

import org.apache.derby.iapi.services.cache.*;

import org.apache.derby.iapi.error.StandardException;

/**

*/
public abstract class T_Cacheable implements Cacheable {

	protected boolean	isDirty;

	protected Thread       owner;
		
	public T_Cacheable() {
	}

	/*
	** Cacheable methods
	*/

	public Cacheable setIdentity(Object key) throws StandardException {
		// we expect a key of type Object[]
		if (!(key instanceof T_Key)) {
			throw T_CacheException.invalidKey();
		}

		owner = null;

		return null; // must be overriden by super class	
	}



	public Cacheable createIdentity(Object key, Object createParameter) throws StandardException {

		// we expect a key of type Object[]
		if (!(key instanceof T_Key)) {
			throw T_CacheException.invalidKey();
		}

		owner = (Thread) createParameter;

		return null; // must be overriden by super class
	}

	/**
		Returns true of the object is dirty. Will only be called when the object is unkept.

		<BR> MT - thread safe 

	*/
	public boolean isDirty() {
		synchronized (this) {
			return isDirty;
		}
	}



	public void clean(boolean forRemove) throws StandardException {
		synchronized (this) {
			isDirty = false;
		}
	}

	/*
	** Implementation specific methods
	*/

	protected Cacheable getCorrectObject(Object keyValue) throws StandardException {

		Cacheable correctType;

		if (keyValue instanceof Integer) {

			correctType = new T_CachedInteger();
		//} else if (keyValue instanceof String) {
			//correctType = new T_CachedString();
		} else {

			throw T_CacheException.invalidKey();
		}

		return correctType;
	}

	protected boolean dummySet(T_Key tkey) throws StandardException {

		// first wait
		if (tkey.getWait() != 0) {
			synchronized (this) {

				try {
					wait(tkey.getWait());
				} catch (InterruptedException ie) {
					// RESOLVE
				}
			}
		}

		if (!tkey.canFind())
			return false;

		if (tkey.raiseException())
			throw T_CacheException.identityFail();

		return true;
	}

	public void setDirty() {
		synchronized (this) {
			isDirty = true;
		}
	}

	public boolean canRemove() {

		synchronized (this) {
			if (owner == null)
				owner = Thread.currentThread();

			if (owner == Thread.currentThread())
				return true;
			return false;
		}
	}
}

