/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.cache
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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

