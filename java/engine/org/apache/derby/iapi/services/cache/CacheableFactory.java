/*

   Derby - Class org.apache.derby.iapi.services.cache.CacheableFactory

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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
public interface CacheableFactory  {

	public Cacheable newCacheable(CacheManager cm);
}

