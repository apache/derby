/*

   Derby - Class org.apache.derby.iapi.services.cache.Cacheable

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.cache;

import org.apache.derby.iapi.error.StandardException;

/**
	Any object that implements this interface can be cached using the services of
	the CacheManager/CacheFactory. In addition to implementing this interface the
	class must be public and it must have a public no-arg constructor. This is because
	the cache manager will construct objects itself and then set their identity
	by calling the setIdentity method.
	<P>
	A Cacheable object has five states:
	<OL>
	<OL>
	<LI> No identity - The object is only accessable by the cache manager
	<LI> Identity, clean, unkept - The object has an identity, is clean but is only accessable by the cache manager
	<LI> Identity, clean, kept - The object has an identity, is clean, and is in use by one or more threads 
	<LI> Identity, kept, dirty - The object has an identity, is dirty, and is in use by one or more threads 
	<LI> Identity, unkept, dirty - The object has an identity, is dirty but is only accessable by the cache manager
	</OL>
	</OL>
	<BR>
	While the object is kept it is guaranteed
	not to change identity. While it is unkept no-one outside of the
	cache manager can have a reference to the object.
	The cache manager returns kept objects and they return to the unkept state
	when all the current users of the object have released it.
	<BR>
	It is required that the object can only move into a dirty state while it is kept.

	<BR> MT - Mutable : thread aware - Calls to Cacheable method must only be made by the
	cache manager or the object itself.

	@see CacheManager
	@see CacheFactory
	@see Class#newInstance
*/
public interface Cacheable  {

	/**
		Set the identity of the object to represent an item that already exists,
		e.g. an existing container.
		The object will be in the No Identity state,
		ie. it will have just been created or clearIdentity() was just called. 
		<BR>
		The object must copy the information out of key, not just store a reference to key.
		After this call the expression getIdentity().equals(key) must return true.
		<BR>
		If the class of the object needs to change (e.g. to support a different format)
		then the object should create a new object, call its initParameter() with the parameters
		the original object was called with, set its identity and return a reference to it. The cache
		manager will discard the reference to the old object. 
		<BR>
		If an exception is thrown the object must be left in the no-identity state.

		<BR> MT - single thread required - Method must only be called be cache manager
		and the cache manager will guarantee only one thread can be calling it.

		@return an object reference if the object can take on the identity, null otherwise.

		@exception StandardException Standard Cloudscape Policy

		@see CacheManager#find

	*/
	public Cacheable setIdentity(Object key) throws StandardException;

	/**
		Create a new item and set the identity of the object to represent it.
		The object will be in the No Identity state,
		ie. it will have just been created or clearIdentity() was just called. 
		<BR>
		The object must copy the information out of key, not just store a reference to key
		if the key is not immutable.
		After this call the expression getIdentity().equals(key) must return true.
		<BR>
		<BR>
		If the class of the object needs to change (e.g. to support a different format)
		then the object should create a new object, call its initParameter() with the parameters
		the original object was called with, set its identity and return a reference to it. The cache
		manager will discard the reference to the old object. 
		<BR>
		If an exception is thrown the object must be left in the no-identity state.

		<BR> MT - single thread required - Method must only be called be cache manager
		and the cache manager will guarantee only one thread can be calling it.

		@return an object reference if the object can take on the identity, null otherwise.

		@exception StandardException If forCreate is true and the object cannot be created.

		@see CacheManager#create

	*/
	public Cacheable createIdentity(Object key, Object createParameter) throws StandardException;


	/**
		Put the object into the No Identity state. 

		<BR> MT - single thread required - Method must only be called be cache manager
		and the cache manager will guarantee only one thread can be calling it.

	*/
	public void clearIdentity();

	/**
		Get the identity of this object.

		<BR> MT - thread safe.

	*/
	public Object getIdentity();


	/**
		Returns true of the object is dirty. 
		May be called when the object is kept or unkept.

		<BR> MT - thread safe 

	*/
	public boolean isDirty();

	/**
		Clean the object.
		It is up to the object to ensure synchronization of the isDirty()
		and clean() method calls.
		<BR>
		If forRemove is true then the 
		object is being removed due to an explict remove request, in this case
		the cache manager will have called this method regardless of the
		state of the isDirty() 

		<BR>
		If an exception is thrown the object must be left in the clean state.

		<BR> MT - thread safe - Can be called at any time by the cache manager, it is the
		responsibility of the object implementing Cacheable to ensure any users of the
		object do not conflict with the clean call.

		@exception StandardException Standard Cloudscape error policy.

	*/
	public void clean(boolean forRemove) throws StandardException;
}

