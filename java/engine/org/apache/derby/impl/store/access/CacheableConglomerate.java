/*

   Derby - Class org.apache.derby.impl.store.access.CacheableConglomerate

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

package org.apache.derby.impl.store.access;

import java.util.Properties;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;

/**
The CacheableConglomerate implements a single item in the cache used by
the Conglomerate directory to cache Conglomerates.  It is simply a wrapper
object for the conglomid and Conglomerate object that is read from the
Conglomerate Conglomerate.   It is a wrapper rather than extending 
the conglomerate implementations because we want to cache all conglomerate
implementatations: (ie. Heap, B2I, ...).

References to the Conglomerate objects cached by this wrapper will be handed
out to callers.  When this this object goes out of cache callers may still
have references to the Conglomerate objects, which we are counting on java
to garbage collect.  The Conglomerate Objects never change after they are
created.

**/

class CacheableConglomerate implements Cacheable
{
    private Long            conglomid;
    private Conglomerate    conglom;

    /* Constructor */
    CacheableConglomerate()
    {
    }

	/*
	** protected Methods of CacheableConglomerate:
	*/
    protected Conglomerate getConglom()
    {
        return(this.conglom);
    }

	/*
	** Methods of Cacheable:
	*/

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
	public Cacheable setIdentity(Object key) throws StandardException
    {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("not supported.");
		}

        return(null);
    }

	/**
     * Create a new item and set the identity of the object to represent it.
	 * The object will be in the No Identity state,
	 * ie. it will have just been created or clearIdentity() was just called. 
	 * <BR>
	 * The object must copy the information out of key, not just store a 
     * reference to key.  After this call the expression 
     * getIdentity().equals(key) must return true.
	 * <BR>
	 * If the class of the object needs to change (e.g. to support a different 
     * format) then the object should create a new object, call its 
     * initParameter() with the parameters the original object was called with,
     * set its identity and return a reference to it. The cache manager will 
     * discard the reference to the old object. 
	 * <BR>
	 * If an exception is thrown the object must be left in the no-identity 
     * state.
	 * <BR> MT - single thread required - Method must only be called be cache 
     * manager and the cache manager will guarantee only one thread can be 
     * calling it.
     *
	 * @return an object reference if the object can take on the identity, 
     * null otherwise.
     *
	 * @exception StandardException If forCreate is true and the object cannot 
     * be created.
     *
	 * @see CacheManager#create
	 **/
	public Cacheable createIdentity(Object key, Object createParameter) 
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                key instanceof Long, "key is not instanceof Long");
            SanityManager.ASSERT(
                createParameter instanceof Conglomerate, 
                "createParameter is not instanceof Conglomerate");
        }

        this.conglomid = (Long) key;
        this.conglom   = ((Conglomerate) createParameter);

        return(this);
    }

	/**
		Put the object into the No Identity state. 

		<BR> MT - single thread required - Method must only be called be cache manager
		and the cache manager will guarantee only one thread can be calling it.

	*/
	public void clearIdentity()
    {
        this.conglomid = null;
        this.conglom   = null;
    }

	/**
		Get the identity of this object.

		<BR> MT - thread safe.

	*/
	public Object getIdentity()
    {
        return(this.conglomid);
    }


	/**
		Returns true of the object is dirty. Will only be called when the object is unkept.

		<BR> MT - thread safe 

	*/
	public boolean isDirty()
    {
        return(false);
    }

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
	public void clean(boolean forRemove) throws StandardException
    {
    }
}
