/*

   Derby - Class org.apache.derby.iapi.services.cache.CacheManager

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
import org.apache.derby.iapi.services.daemon.DaemonService;

import org.apache.derby.iapi.util.Matchable;
import org.apache.derby.iapi.util.Operator;

public interface CacheManager {

    /**
     * @return the current maximum size of the cache.
     */
	public long getMaximumSize();

    /**
     * Change the maximum size of the cache. If the size is decreased then cache entries
     * will be thrown out.
     *
     * @param newSize the new maximum cache size
     *
     * @exception StandardException Cloudscape Standard error policy
     */
	public void resize( long newSize) throws StandardException;

	/**
		Find an object in the cache that matches the key provided using the equals()
		method, i.e. the return Cacheable will have getIdentifier.equals(key) true.
		If the object does not exist in the cache it will be added by one of:
		<UL>
		<LI>creating a new holder object and calling its initParameter() method and then its
		setIdentity() method with key as the parameter.
		<LI>Calling clearIdentity() on an holder object in the clean state and then calling its
		setIdentity() method with key as the parameter.
		<LI>Calling clean() on a dirty holder object and then calling clearIdentity() on an
		holder object in the clean state and then calling its setIdentity() method with key
		as the parameter.
		</UL>
		In all cases the setIdentity() method is called with forCreate set to false. 
		<BR>
		The returned object is kept, i.e. its identity will not change, until the release()
		method is called. The release() method must be called after the caller is finished
		with the object and throw away the reference to it, e.g.
		<PRE>
			Page p = (Page) pageCache.find(pageKey);

			// do stuff with p

			// release p
			pageCache.release(p);
			p = null;
			
		</PRE>

		@return A reference to an object in the cache, or null if the object cannot be found.

		@exception StandardException Standard Cloudscape error policy.

		@see Cacheable#setIdentity

	*/
	public Cacheable find(Object key) throws StandardException;

	/**
		Find an object in the cache that matches the key provided using the equals()
		method, i.e. the return Cacheable will have getIdentifier.equals(key) true.
		If a matching object does not exist in the cache, null is returned.
		<BR>
		The returned object is kept, i.e. its identity will not change, until the release()
		method is called. The release() method must be called after the caller is finished
		with the object and throw away the reference to it, e.g.
		<PRE>
			Page p = (Page) pageCache.findCached(pageKey);
			if (p != null) {

				// do stuff with p

				// release p
				pageCache.release(p);
				p = null;
			}
			
		</PRE>
		@exception StandardException Standard Cloudscape error policy.
	*/
	public Cacheable findCached(Object key) throws StandardException;

    /**
     * Determine whether a key is in the cache.
     *
     * <b>WARNING:</b> This method does not keep a lock on the entry or the cache, so
     * the return value could be made incorrect by the time that this method returns.
     * Therefore this method should only be used for statistical purposes.
     */
    public boolean containsKey( Object key);
    
    /**
     * Mark a set of entries as having been used. Normally this is done as a side effect
     * of find() or findCached. If the entry has been replaced then this method
     * does nothing.
     *
     * @param key the key of the used entry.
     */
    public void setUsed( Object[] keys);
    
	/**
		Create an object in the cache. The resulting object will match the key provided using the equals()
		method, i.e. the return Cacheable will have getIdentifier.equals(key) true.
		If an object that matches the key already exists in the cache then
		an exception is thrown. 
		<BR>
		The object will be added by one of:
		<UL>
		<LI>creating a new holder object and calling its initParameter() method and then its
		createIdentity() method with key as the parameter.
		<LI>Calling clearIdentity() on an holder object in the clean state and then calling its
		createIdentity() method with key as the parameter.
		<LI>Calling clean() on a dirty holder object and then calling clearIdentity() on an
		holder object in the clean state and then calling its createIdentity() method with key
		as the parameter.
		</UL>
		In all cases the setIdentity() method is called with the createParameter as the second
		argument.
		If the object cannot be created then an exception is thrown by createIdentity.
		<BR>
		The returned object is kept, i.e. its identity will not change, until the release()
		method is called. The release() method must be called after the caller is finished
		with the object and throw away the reference to it, e.g.
		<PRE>
			Page p = (Page) pageCache.create(pageKey, createType);

			// do stuff with p

			// release p
			pageCache.release(p);
			p = null;
			
		</PRE>

		@return A reference to an object in the cache.

		@exception StandardException Standard Cloudscape error policy.

		@see Cacheable#createIdentity

	*/
	public Cacheable create(Object key, Object createParameter) throws StandardException;

	/**
		Release a Cacheable object previously found with find() or findCached().
		After this call the caller must throw away the reference to item.

	*/
	public void release(Cacheable entry);

	/**
		Delete and remove an object from the cache. It is up to the user of the cache
		to provide synchronization of some form that ensures that only one caller
		executes remove() on a cached object.
		<BR>
		The object must have previously been found with find() or findCached().
		The item will be placed into the NoIdentity
		state through clean(true) (if required) and clearIdentity(). The removal of the
		object will be delayed until it is not kept by anyone. Objects that are in the
		to be removed state can still be found through find() and findCached()
		until their keep count drops to zero. This call waits until the object
		has been removed.
		<BR>
		After this call the caller must throw away the reference to item.

		@exception StandardException Standard Cloudscape error policy.
	*/
	public void remove(Cacheable entry) throws StandardException;

	/**
		Place all objects in their clean state by calling their clean method
		if they are dirty. This method guarantees that all objects that existed
		in the cache at the time of the call are placed in the clean state sometime
		during this call. Objects that are added to the cache during this call or
		objects that are dirtied during this call (by other callers) are not guaranteed
		to be clean once this call returns.

		@see Cacheable#clean
		@see Cacheable#isDirty

		@exception StandardException Standard Cloudscape error policy.
	*/
	public void cleanAll() throws StandardException;

	/**
		Clean all objects that match the partialKey (or exact key).
		Any cached object that results in the partialKey.equals(Object)
		method returning true when passed the cached object will be cleaned. 
		<P>
		In order to clean more than one object the Cacheable equals method must be able to handle
		a partial key, e.g. a page has PageKey but a clean may pass a ContainerKey which will discard
		all pages in that container.

		@exception StandardException Standard Cloudscape error policy.
	*/
	public void clean(Matchable partialKey) throws StandardException;

	/**
		Age as many objects as possible out of the cache.
		This call is guaranteed not to block.
		It is not guaranteed to leave the cache empty.

		<BR>
		It is guaranteed that all unkept, clean objects will be
		removed from the cache.

		@see Cacheable#clean
		@see Cacheable#clearIdentity


	*/
	public void ageOut();

	/**
		Shutdown the cache. This call stops the cache returning
		any more valid references on a find() or findCached() call,
		and then cleanAll() and ageOut() are called. The cache remains
		in existence until the last kept object has been unkept.

		@exception StandardException Standard Cloudscape error policy.

	*/
	public void shutdown() throws StandardException;

	/**
		This cache can use this DaemonService if it needs some work to be done
		int he background 
	*/
	public void useDaemonService(DaemonService daemon);


	/**
		Discard all objects that match the partialKey (or exact key).
		Any cached object that results in the partialKey.equals(Object)
		method returning true when passed the cached object will be thrown out of the cache
		if and only if it is not in use. The Cacheable
		will be discarded without its clean method being called.
		<P>
		If partialKey is null, it matches all objects.  This is a way to
		discard all objects from the cache in case of emergency shutdown.
		<P>
		In order to discard more than one object the Cacheable equals method must be able to handle
		a partial key, e.g. a page has PageKey but a discard may pass a ContainerKey which will discard
		all pages in that container.
		<P>
		@return true if discard has successful gotten rid of all objects that
		match the partial or exact key.  False if some objects that matches
		were not gotten rid of because it was kept.
	*/
	public boolean discard(Matchable partialKey);

	/**
		Report the number of items in use (with Identity) in this cache.
	 */
	public int getNumberInUse();

	/**
		Return statistics about cache that may be implemented.
	**/
	public long[] getCacheStats();

	/**
		reset the cache statistics to 0.
	**/
	public void resetCacheStats();

    /**
     * Perform an operation on (approximately) all entries that matches the filter,
     * or all entries if the filter is null.  Entries that are added while the
     * cache is being scanned might or might not be missed.
     *
     * @param filter
     * @param operator
     */
    public void scan( Matchable filter, Operator operator);
}
