/*

   Derby - Class org.apache.derby.impl.services.cache.Clock

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

package org.apache.derby.impl.services.cache;

import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheableFactory;
import org.apache.derby.iapi.services.cache.SizedCacheable;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.daemon.Serviceable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.util.Matchable;
import org.apache.derby.iapi.util.Operator;
import org.apache.derby.iapi.reference.SQLState;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Properties;


/**
	A cache manager that uses a Hashtable and a ArrayList. The ArrayList holds
	CachedItem objects, each with a holder object. The Hashtable is keyed
	by the identity of the holder object (Cacheable.getIdentity()) and
	the data portion is a pointer to the CachedItem. CachedItems that have
	holder objects with no identity do not have entries in the hashtable.
	<P>
	CachedItems can in various state.
	<UL>
	<LI>isValid - the entry has a valid identity
	<LI>inCreate - the entry is being created or being faulted in from persistent store
	<LI>inClean - the entry is being written out to persistent store
	<LI>isKept - the entry is currently being looked at/updated, do not remove or
				clean it.
	</OL>

	<P>Multithreading considerations:<BR>
	A clock cache manager must be MT-safe.
	All member variables are accessed single threaded (synchronized on this) or
	set once or readonly. Assumptions: holders size() and addElement must be
	synchronized.
	<BR>
	CachedItem is never passed out of the clock cache manager, only the
	Cacheable object is.  The cachedItem is responsible for the setting and
	clearing of its own member fields (RESOLVE: now they are done in cache
	manager, need to be moved to the cachedItem).  The cache manager will
	following the following rules while accessing a cacheditem:
	<UL>
	<LI>invalid item is never returned from the cache
	<LI>setValidState and isValid() is only called single threaded through the cache manager.
	<LI>keep() and isKept() is only called single threaded through the cache
	manager once the item has been added to the holders array
	<LI>item that isKept() won't be cleaned or removed or invalidated from the cache.
	<LI>item that is inClean() or inCreate(), the cache manager
	will wait on the cachedItem to finish cleaning or creating before it
	returns the cached item outside of the cache.
	</UL>
	<BR>
	The cacheable must be cleaned thru the cache if it is managed by a cache.
	On CacheItem, a inClean state is maintain to stablelize the content of the
	cacheable while it is being cleaned.  Only unkept items are cleaned.  If an
	item is found to be inClean, it will wait until it exits the inClean state.
	If a cached item calls it own clean method without notifying the cache, it
	has to stablize its content for the duration of the clean.
	<BR>
	It is assumed that the cacheable object maintain its own MT-safeness.<BR>

	@see CachedItem
	@see Cacheable

*/

final class Clock extends Hashtable implements CacheManager, Serviceable {

	/*
	** Fields
	*/
	public final CacheStat			stat;
	private DaemonService		cleaner;	// the background worker thread who is going to
									// do pre-flush for this cache. 
	private final ArrayList		holders;
    private int validItemCount = 0;
	private long			maximumSize;
    private boolean useByteCount; // regulate the total byte count or the entry count
    private long currentByteCount = 0;
    /* currentByteCount should be the sum of entry.getSize() for all entries in the cache.
     * That is, it should be the sum of getItemSize( item, false) for each item in the holders
     * vector.
     */

    private static final int ITEM_OVERHEAD = ClassSize.estimateBaseFromCatalog( CachedItem.class)
        + ClassSize.getRefSize() // one ref per item in the holder ArrayList
        + ClassSize.estimateHashEntrySize();

	private final CacheableFactory holderFactory;

	private boolean		active;		// true if active for find/create
	private String		name;		// name of the cache, mainly for debugging purposes.
	private int			clockHand;             // the sweep of the clock hand

	
	private	int			myClientNumber;	// use this number to talk to cleaner service
	private boolean	wokenToClean;	// true if the client was woken to clean, false if to shrink
	private boolean   cleanerRunning;
	private boolean	  needService;

	/**
		Construct a new clock cache manager.

		<P>MT - not needed for constructor.

		@param holderClass the cacheable object class
		@param name the name of the cache
		@param initParam parameter to call the cacheable object the first time
		it is being initialized.
		@param initialSize the initial number of cachable object this cache
		holds.
		@param maximumSize the maximum size of the cache.  The cache may grow
		from initialSize to maximumSize if the cache policy notices that there
		is not enough free buffers availiable.  Once the cache hits maximumSize
		it will not grow.  If the cache is full, an exception will be thrown

	*/
	Clock(CacheableFactory holderFactory,
                  String name,
                  int initialSize,
                  long maximumSize,
                  boolean useByteCount)
    {
        super(initialSize, (float) 0.95);
		this.maximumSize = maximumSize;
		this.holderFactory = holderFactory;
        this.useByteCount = useByteCount;

		if (SanityManager.DEBUG) {
		  if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace)) {
			SanityManager.DEBUG(ClockFactory.CacheTrace, "initializing " + name + " cache to size " + initialSize);
		  }
		}

		//int delta = initialSize / 2;
		//if (delta < 5)
		//	delta = 5;

		holders = new ArrayList(initialSize);
		this.name = name;
		active = true;

		this.stat = new CacheStat();
		stat.initialSize = initialSize;
		stat.maxSize = maximumSize;
    }

	/**
		Find the object or materialize one in the cache.  If it has not been
		created in the persistent store yet, return null.

		<P>MT - must be MT-safe.  The cache is single threaded through finding
		the item in cache and finding a free item if it is not in cache, thus
		preventing another thread from creating the same item while is is being
		faulted in.  (RESOLVE - this is really low performance if the cache
		cleaner cannot keep a steady supply of free items and we have to do an
		I/O while blocking the cache).   If it needs to be faulted in, the
		inCreate bit is set.  The item is kept before it exits the sync block. 
		<BR>
		If the item is in cache but in the middle of being faulted in or
		cleaned, it needs to wait until this is done being before returning.
		<BR>
		The keep status prevents other threads from removing this item.  
		The inCreate status prevents other threads from looking at or writing
		out this item while it is being faulted in.
		(RESOLVE: need to handle the case where the object is marked for
		removal and being waited on)

		@param key the key to the object
		@return a cacheable object that is kept in the cache.
		@exception StandardException Cloudscape Standard error policy
	*/
	public Cacheable find(Object key) throws StandardException {
		CachedItem item;
		boolean		add;

		/*
		** We will only loop if someone else tried to add the
		** same key as we did and they failed.  In this case
		** we start all over.  An example of this would be an
		** attempt to cache an object that failed due to a 
		** transient error (e.g. deadlock), which should not
		** prevent another thread from trying to add the 
		** key to the cache (e.g. it might be the one holding
		** the lock that caused the other thread to deadlock).
		*/
		while (true)
		{
			add = false;

			synchronized (this) {
	
				if (!active)
					return null;
	
				item = (CachedItem) get(key);
	
				if (item != null) {
					item.keepAfterSearch();
					
					stat.findHit++;

					if (SanityManager.DEBUG) {
					  if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace)) 
					  {
						SanityManager.DEBUG(ClockFactory.CacheTrace, name + ": Found key " +
											key + " already in cache, item " + item);
					  }
					}
				}
			}
	
			// no entry was found, need to add one
			if (item == null) {
	
				// get a free item
				item = findFreeItem();

				stat.findMiss++;
	
				if (SanityManager.DEBUG) {
				  if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace)) 
				  {
					SanityManager.DEBUG(ClockFactory.CacheTrace, name + ": Find key " +
										key + " Not in cache, get free item " + item);
				  }
				}
	
	
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(item != null, "found null item");
	
				synchronized (this) {
					CachedItem inCacheItem = (CachedItem) get(key);
	
					if (inCacheItem != null) {
						// some-one beat us to adding an item into the cache,
						// just use that one
						item.unkeepForCreate();
	
						item = inCacheItem;
						item.keepAfterSearch();
					} else {
						// yes, we really are the ones to add it
						put(key, item);
						add = true;
						if (SanityManager.DEBUG) {

							if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

								if (size() > ((11 * maximumSize) / 10))
									System.out.println("memoryLeakTrace:Cache:" + name + " " + size());
							}
						}
					}
				}
			}
			
			if (add) {
	
				if (SanityManager.DEBUG) {
				  if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace))
				  {
					SanityManager.DEBUG(ClockFactory.CacheTrace, name + " Added " + 
										key + " to cache, item " + item);
				  }
				}
	
				stat.findFault++;

				return addEntry(item, key, false, (Object) null);
			}
				
			Cacheable entry = item.use();
			if (entry == null) {
				// item was not added by the other user successfully ...
				synchronized (this) {
					item.unkeep();
				}

				// try to hash the key again (see
				// comment at head of loop)
				continue;
			}
	
			return entry;
		}
	}


	/**
		Find an object in the cache.  Do not fault in or create the object if
		is is not found in the cache.

		<P>MT - must be MT-safe.  The cache is single threaded through finding
		the item in cache.  If it needs to wait for it to be faulted in or
		cleaned it is synchronized/waited on the cached item itself.

		@param key the key to the object
		@return a cacheable object that is kept in the cache.
	*/

	public Cacheable findCached(Object key) throws StandardException {


		CachedItem item;

		synchronized (this) {

			if (!active)
				return null;
		
			item = (CachedItem) get(key);

			if (item == null) {
				stat.findCachedMiss++;
				return null;
			} else
				stat.findCachedHit++;
			
			item.keepAfterSearch();
		}

		Cacheable entry = item.use();
		if (entry == null) {
			// item was not added by the other user successfully ...
			synchronized (this) {
				item.unkeep();
			}
		}

		return entry;
	}


    /**
     * Mark a set of  entries as having been used. Normally this is done as a side effect
     * of find() or findCached. Entries that are no longer in the cache are ignored.
     *
     * @param key the key of the used entry.
     */
    public void setUsed( Object[] keys)
    {
		CachedItem item;

        for( int i = 0; i < keys.length;)
        {
            // Do not hold the synchronization lock for too long.
            synchronized (this)
            {
                if (!active)
                    return;

                int endIdx = i + 32;
                if( endIdx > keys.length)
                    endIdx = keys.length;
                for( ; i < endIdx; i++)
                {
                    if( keys[i] == null)
                        return;
                    
                    item = (CachedItem) get(keys[i]);
                    if( null != item)
                        item.setUsed( true);
                }
            }
        }
    } // end of setUsed

	/**
		Create a new object with the said key.

		<P>MT - must be MT-safe.  Single thread thru verifying no such item
		exist in cache and finding a free item, keep the item and set inCreate
		state.  The actual creating of the  object is done outside
		the sync block and is protected by the isKept and inCreate bits.

		@param key the key to the object
		@return a cacheable object that is kept in the cache.  

		@exception StandardException Cloudscape Standard error policy
	*/
	public Cacheable create(Object key, Object createParameter) throws StandardException {

		// assume the item is not already in the cache
		CachedItem item = findFreeItem();

		stat.create++;

		synchronized (this) {

			if (!active)
				return null;

			if (get(key) != null) {

				item.unkeepForCreate();

				throw StandardException.newException(SQLState.OBJECT_EXISTS_IN_CACHE, this.name, key);
			}

			put(key, item);

			if (SanityManager.DEBUG) {

				if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

					if (size() > ((11 * maximumSize) / 10))
						System.out.println("memoryLeakTrace:Cache:" + name + " " + size());
				}
			}
		}

		Cacheable entry = addEntry(item, key, true, createParameter);
	
		if (SanityManager.DEBUG) {
			if (entry != null)
				SanityManager.ASSERT(item.getEntry() == entry);
		}

		return entry;
	}


	/**
		The caller is no longer looking at or updating the entry.  Since there
		could be more than one piece of code looking at this entry, release
		does not mean nobody is looking at or updating the entry, just one
		less.  If the cacheable is marked for remove (someone is waiting to
		remove the persistent object once nobody is looking at it), then notify
		the waiter if this is the last one looking at it.
		<BR>
		Unless there is a good reason to do otherwise, release should be used
		to release a cachable and not directly call cachedItem unkeep, since
		unkeep does not handle the case of remove.


		<P>MT - must be MT-safe.  Getting and deleteing item from the hashtable
		is in the same synchronized block.  If the cacheable object is waiting
		to be removed, that is synchronized thru the cachedItem itself
		(RESOLVE: need to move this sync block to cachedItem instead)

		@param entry the cached entry

	 */
	public void release(Cacheable entry)  {
		boolean removeItem;
		CachedItem item;
		long toShrink = 0;

		synchronized (this) {

			item = (CachedItem) get(entry.getIdentity());

			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(item != null, "item null");
				SanityManager.ASSERT(item.getEntry() == entry, "entry not equals keyed entry");
				SanityManager.ASSERT(item.isKept(), "item is not kept in release(Cachable)");
			}

			removeItem = item.unkeep();

			if (removeItem) {
				
				remove(entry.getIdentity());

				// we keep the item here to stop another thread trying to evict it
				// while we are destroying it.
				item.keepForClean();
			}

			if (cleaner == null) {
				// try to shrink the cache on a release
				toShrink = shrinkSize( getCurrentSize());
			}
		}

		if (removeItem) {

			item.notifyRemover();
		}

		if (toShrink > 0)
			performWork(true /* shrink only */);
	}

	protected void release(CachedItem item) {

		boolean removeItem;

		synchronized (this) {

			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(item.isKept(), "item is not kept in released(CachedItem)");
			}

			removeItem = item.unkeep();

			if (removeItem) {
				
				remove(item.getEntry().getIdentity());

				// we keep the item here to stop another thread trying to evict it
				// while we are destroying it.
				item.keepForClean();
			}
		}

		if (removeItem) {

			item.notifyRemover();
		}
	}

	/**
		Remove an object from the cache. The item will be placed into the NoIdentity
		state through clean() (if required) and clearIdentity(). The removal of the
		object will be delayed until it is not kept by anyone.

		After this call the caller must throw away the reference to item.

		<P>MT - must be MT-safe.  Single thread thru finding and setting the
		remove state of the item, the actual removal of the cacheable is
		synchronized on the cachedItem itself.

		@exception StandardException Standard Cloudscape error policy.
	*/
	public void remove(Cacheable entry) throws StandardException {

		boolean removeNow;
		CachedItem item;
        long origItemSize = 0;

		stat.remove++;

		synchronized (this) {

			

			item = (CachedItem) get(entry.getIdentity());

			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(item != null);
				SanityManager.ASSERT(item.getEntry() == entry);
				SanityManager.ASSERT(item.isKept());
			}
            if( useByteCount)
                origItemSize = getItemSize( item);

			item.setRemoveState();
			removeNow = item.unkeep();	

			if (removeNow) {
				remove(entry.getIdentity());
				item.keepForClean();
			}
		}

		try {
			// if removeNow is false then this thread may sleep
			item.remove(removeNow);

		} finally {

			synchronized (this)
			{
				// in the case where this thread didn't call keepForClean() the thread
				// that woke us would have called keepForClean.
				item.unkeep();
				item.setValidState(false);
                validItemCount--;
				item.getEntry().clearIdentity();
                if( useByteCount)
                    currentByteCount += getItemSize( item) - origItemSize;
			}
		}

	}

	/**
		Clean all objects in the cache.
	*/
	public void cleanAll() throws StandardException {
		stat.cleanAll++;
		cleanCache((Matchable) null);
	}

	/**
		Clean all objects that match a partial key.
	*/
	public void clean(Matchable partialKey) throws StandardException {

		cleanCache(partialKey);
	}

	/**
		Age as many objects as possible out of the cache.

  		<BR>MT - thread safe

		@see CacheManager#ageOut
	*/
	public void ageOut() {

		stat.ageOut++;
		synchronized (this) {

			int size = holders.size();
			long toShrink = shrinkSize( getCurrentSize());
			boolean shrunk = false;

			for (int position = 0; position < size; position++) {
				CachedItem item = (CachedItem) holders.get(position);

				if (item.isKept())
					continue;
				if (!item.isValid())
					continue;

				if (item.getEntry().isDirty()) {
					continue;
				}

				long itemSize = removeIdentity(item);

				if (toShrink > 0) {

					if (SanityManager.DEBUG) {
						if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace)) {
						SanityManager.DEBUG(ClockFactory.CacheTrace, name + 
											" shrinking item " + item + " at position " + position);
						}
					}
					
					toShrink -= itemSize;
					shrunk = true;
				}

			} // end of for loop

			if (shrunk)
				trimToSize();

		} // out of sync block
	} // end of ageOut

	/**
		MT - synchronization provided by caller

		@exception StandardException Standard Cloudscape error policy.
	*/
	public void shutdown() throws StandardException {

		if (cleaner != null) {
			cleaner.unsubscribe(myClientNumber);
			cleaner = null;
		}

		synchronized (this) {
			active = false;
		}

		ageOut();
		cleanAll();
		ageOut();
	}

	/**
		MT - synchronization provided by caller

		can use this Daemomn service if needed
	*/
	public void useDaemonService(DaemonService daemon)
	{
		// if we were using another cleaner, unsubscribe first
		if (cleaner != null)
			cleaner.unsubscribe(myClientNumber);

		cleaner = daemon;
		myClientNumber = cleaner.subscribe(this, true /* onDemandOnly */);
	}
	/**
		Discard all objects that match the partial key.

		<BR>MT - thread safe
	*/
	public boolean discard(Matchable partialKey) {

		// we miss something because it was kept
		boolean noMisses = true;

		synchronized (this) {

			int size = holders.size();
			long toShrink = shrinkSize( getCurrentSize());
			boolean shrunk = false;

			for (int position = 0; position < size; position++) {
				CachedItem item = (CachedItem) holders.get(position);

				if (!item.isValid())
					continue;

				Object key = item.getEntry().getIdentity();

				if (partialKey != null && !partialKey.match(key))
					continue;

				if (item.isKept())
				{
					noMisses = false;
					continue;
				}

				long itemSize = removeIdentity(item);

				if (toShrink > 0) {

					if (SanityManager.DEBUG) {
						if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace)) {
						SanityManager.DEBUG(ClockFactory.CacheTrace, name + 
											" shrinking item " + item + " at position " + position);
						}
					}

					// and we shrunk one item
					toShrink -= itemSize;
					shrunk = true;
				}
			}

			if (shrunk)
				trimToSize();
		}

		return noMisses;
	}

	/**
		Add a new CachedItem and a holder object to the cache. The holder object
		is returned kept.

		<P>MT - need to be MT-safe.  The insertion of the key into the hash
		table is synchronized on this.

	*/
	private Cacheable addEntry(CachedItem item, Object key, boolean forCreate, Object createParameter)
		throws StandardException {

		Cacheable entry = null;
        long origEntrySize = 0;
        if( useByteCount)
            origEntrySize = getItemSize( item);

		try
		{
			if (SanityManager.DEBUG) {
			  if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace))
			  {
				SanityManager.DEBUG(ClockFactory.CacheTrace, name + 
									" item " + item + " take on identity " + key);
			  }
			}
			
			// tell the object it needs to create itself
			entry = item.takeOnIdentity(this, holderFactory, key, forCreate, createParameter);
		}
		finally
		{
			boolean	notifyWaiters;
			synchronized (this) {

				Object removed = remove(key);
				if (SanityManager.DEBUG) {
					SanityManager.ASSERT(removed == item);
				}

				if (entry != null) {
					// put the actual key into the hash table, not the one that was passed in
					// for the find or create. This is because the caller may re-use the key
					// for another cache operation, which would corrupt our hashtable
					put(entry.getIdentity(), item);
                    if( useByteCount)
                        currentByteCount += ((SizedCacheable) entry).getSize() - origEntrySize;
					item.setValidState(true);
                    validItemCount++;
					notifyWaiters = true;
				} else {
					item.unkeep();
					notifyWaiters = item.isKept();
				}
			}

			// whatever the outcome, we have to notify waiters ...
			if (notifyWaiters)
				item.settingIdentityComplete();
		}

		return entry;
	}

   
	protected CachedItem findFreeItem() throws StandardException {

		// Need to avoid thrashing the cache when we start out
		// so if the cache is smaller than its maximum size
		// then that's a good indication we should grow.

		long currentSize = getCurrentSize();


		if (currentSize >= maximumSize) {
			// look at 20%
			CachedItem item = rotateClock(0.2f);
			if (item != null)
				return item;
		}

		// However, if the cache contains a large number of invalid
		// items then we should see if we can avoid growing.
		// This avoids simple use of Cloudscape looking like
		// a memory leak, as the page cache fills the holders array
		// with page objects including the 4k (or 32k) pages.
		// size() is the number of valid entries in the hash table


		// no need to sync on getting the sizes since the if
		// they are wrong we will just not find a invalid entry in
		// the lookup below.
		if (validItemCount < holders.size()) {

			synchronized (this) {

				for (int i = holders.size() - 1; i >= 0 ; i--) {
					CachedItem item = (CachedItem) holders.get(i);

					if (item.isKept())
						continue;

					// found a free item, just use it
					if (!item.isValid()) {
						item.keepForCreate();
						return item;
					}
				}
			}
		}


		return growCache();
	}

	/**
		Go through the list of holder objects and find a free one.
		<P>MT - must be MT-safe.  The moving of the clockHand and finding of an
		eviction candidate is synchronized.  The cleaning of the cachable is
		handled by the cacheable itself.
	*/
	protected CachedItem rotateClock(float percentOfClock) throws StandardException
	{
		// statistics -- only used in debug
		int evictions = 0;
		int cleaned = 0;
		int resetUsed = 0;
		int iskept = 0;

        // When we are managing the entry count (useByteCount == false) this method just
        // has to find or manufacture an available item (a cache slot). When we are managing
        // the total byte count then this method must find both available space and an
        // available item.
        CachedItem availableItem = null;

		boolean kickCleaner = false;

		try {


			// this can be approximate
			int itemCount = holders.size();
			int itemsToCheck;
			if (itemCount < 20)
				itemsToCheck = 2 * itemCount;
			else
				itemsToCheck = (int) (((float) itemCount) * percentOfClock);


			// if we can grow then shrinking is OK too, if we can't grow
			// then shrinking the cache won't help us find an item.
			long toShrink = shrinkSize(getCurrentSize());

restartClock:
			for (; itemsToCheck > 0;) {

				CachedItem item = null;

				synchronized (this) {

					if (SanityManager.DEBUG) {
					  if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace))
					  {
						SanityManager.DEBUG(ClockFactory.CacheTrace, name + " rotateClock starting " +
											clockHand + " itemsToCheck " + itemsToCheck);
					  }
					}

					// size of holders cannot change while in the synchronized block.
					int size = holders.size();
					for (; itemsToCheck > 0; item = null, itemsToCheck--, incrClockHand())
					{
						//
						// This uses a very simple clock algorithm.
						//
						// The cache consist of a circular list of cachedItems.  Each cached item
						// has a 'recentlyUsed' bit which is set every time that item is kept.
						// Each clock cache manager keeps a global variable clockHand which
						// refers to the item that is most recently replaced.
						//
						// to find a free item, the clock Hand moves to the next cached Item.
						// If it is kept, or in the middle of being created, the clock hand
						// moves on.  
						// If it is recentlyUsed, clear the recently used bit and moves on. 
						// If it is not recentlyUsed, clean the item and use
						//
						// If all the cached item is kept, then create a new entry.
						// So it is possible, although very unlikely,  that, in time, the cache
						// will grow beyond the maximum size.

						

						if (clockHand >= size) {
							if (size == 0)
								break;
							clockHand = 0;
						}

						item = (CachedItem) holders.get(clockHand);

						if (item.isKept())
						{
							if (SanityManager.DEBUG) // stats only in debug mode
								iskept++;
							continue;
						}

						if (!item.isValid()) // found a free item, just use it
						{
                            if( null != availableItem)
                                // We have found an available item, now we are looking for bytes
                                continue;
							if (SanityManager.DEBUG) {
							  if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace))
							  {
								SanityManager.DEBUG(ClockFactory.CacheTrace,
													name + " found free item at " + clockHand + " item " + item);
							  }
							}

							item.keepForCreate();
							if( useByteCount && getCurrentSize() > maximumSize)
                            {
                                availableItem = item;
                                // now look for bytes.
                                continue;
                            }
							// since we are using this item, move the clock past it.
							incrClockHand();

							return item;
						}

						if (item.recentlyUsed())
						{

							if (SanityManager.DEBUG) // stats only in debug mode
								resetUsed++;
							item.setUsed(false);
							continue;
						}


						if (toShrink > 0) {
							if (!cleanerRunning) {

								// try an get the cleaner to shrink the cache
								kickCleaner = true;
								cleanerRunning = true;
								needService = true;
							}
						}

						// we are seeing valid, not recently used buffers. Evict this.
						if (SanityManager.DEBUG) {
							evictions++;

							if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace))
							{
								SanityManager.DEBUG(ClockFactory.CacheTrace,
													name + " evicting item at " +
													clockHand + " item " + item);
							}
						}

						if (!item.getEntry().isDirty()) {

							if (SanityManager.DEBUG) {
							  if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace)) 
							  {
								SanityManager.DEBUG(ClockFactory.CacheTrace,
													name + " Evicting Item " +
													item + ", not dirty");
							  }
							}

							// a valid, unkept, clean item, clear its identity
							// and use it.
                            long itemSize = removeIdentity(item);

                            if( useByteCount)
                            {
                                toShrink -= itemSize;
                                if( getCurrentSize() > maximumSize && 0 < toShrink)
                                {
                                    if( null == availableItem)
                                    {
                                        item.keepForCreate();
                                        availableItem = item;
                                    }
                                    continue;
                                }
                            }
							// since we are using it move the clock past it
							incrClockHand();

                            if( null != availableItem)
                                return availableItem;

							// item is kept but not valid when returned
							item.keepForCreate();
							return item;
						}
						// item is valid, unkept, and dirty. clean it.
						if ((cleaner != null) && !cleanerRunning) {
							kickCleaner = true;
							wokenToClean = true;
							cleanerRunning = true; // at least it soon will be
						}
						item.keepForClean();

						// leave the clock hand where it is so that we will pick it
						// up if no-one else uses the cache. Other hunters will
						// skip over it as it is kept, and thus move the clock
						// hand past it.
						break;
					}
					if (item == null) {
						return availableItem;
					}

				} // out of synchronized block

				// clean the entry outside of a sync block				    
				try 
				{
					if ( SanityManager.DEBUG) {
						if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace)) {
						SanityManager.DEBUG(ClockFactory.CacheTrace,name + " cleaning item " + item);
						}
					}

					item.clean(false);

					if (SanityManager.DEBUG) // stats only in debug mode
					{
						cleaned++;
					}
				}
				finally {
					release(item);
					item = null;
				}

				// at this point the item we cleaned could be in any state
				// so we can't just re-use it. Continue searching
				continue restartClock;
			}
			return availableItem;
		} finally {


			if (SanityManager.DEBUG)
			{
				// report statistics
				if (
					SanityManager.DEBUG_ON(ClockFactory.CacheTrace))
					SanityManager.DEBUG(ClockFactory.CacheTrace, name + " evictions " + evictions +
										", cleaned " + cleaned + 
										", resetUsed " + resetUsed +
										", isKept " + iskept +
										", size " + holders.size());
			}

			if (kickCleaner && (cleaner != null))
			{
				if (SanityManager.DEBUG) {
					if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace)) {
					SanityManager.DEBUG(DaemonService.DaemonTrace, name + " client # " + myClientNumber + " calling cleaner ");
					}
				}

				cleaner.serviceNow(myClientNumber);

				if (SanityManager.DEBUG) {
				  if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace)) {
					SanityManager.DEBUG(DaemonService.DaemonTrace, name + Thread.currentThread().getName() + " cleaner called");
				  }
				}
			}
		}
    } // end of rotateClock

	/**
		Synchronously increment clock hand position
	*/
	private int incrClockHand()
	{
		if (++clockHand >= holders.size())
			clockHand = 0;
		return clockHand;
	}

	/*
	 * Serviceable methods
	 */

	public int performWork(ContextManager contextMgr /* ignored */) {

		int ret = performWork(false);
		synchronized (this) {
			cleanerRunning = false;
		}
		return ret;
	}

	
	/**
		<P>MT - read only. 
	*/
	public boolean serviceASAP()
	{
		return needService;
	}	


	// @return true, if this work needs to be done on a user thread immediately
	public boolean serviceImmediately()
	{
		return false;
	}	


	public synchronized int getNumberInUse() {

			int size = holders.size();
			int inUse = 0;

			for (int position = 0; position < size; position++) {

				CachedItem item = (CachedItem) holders.get(position);

				if (item.isValid()) {
					inUse++;
				}

			}
			return inUse;
	}
/*
	public int getNumberKept() {

		synchronized (this) {

			int size = holders.size();
			int inUse = 0;

			for (int position = 0; position < size; position++) {

				CachedItem item = (CachedItem) holders.get(position);

				if (item.isValid() && item.isKept()) {
					inUse++;
				}

			}
			return inUse;
		}
	}
*/

	/**
		Grow the cache and return a unused, kept item.

		@exception StandardException Thrown if the cache cannot be grown.
	*/

	private CachedItem growCache()  {

		CachedItem item = new CachedItem();
        item.keepForCreate();

		// if we run out of memory below here we don't
		// know what state the holders could be in
		// so don't trap it
		synchronized (this) {
			holders.add(item);
            // Do not adjust currentByteCount until we put the entry into the CachedItem.
		}

		return item;
	}


	/**
		Clear an item's identity. Item must be 
		unkept and valid. This is called for
		dirty items from the discard code.

		Caller must hold the cache synchronization.

        @return the amount by which this shrinks the cache.
	*/
	protected long removeIdentity(CachedItem item) {

        long shrink = 1;
        
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(!item.isKept(), "item is kept");
			SanityManager.ASSERT(item.isValid(), "item is not valid");

		}

        if( useByteCount)
            shrink = ((SizedCacheable) item.getEntry()).getSize();
		remove(item.getEntry().getIdentity());				
		item.setValidState(false);
        validItemCount--;
		item.getEntry().clearIdentity();
        if( useByteCount)
        {
            shrink -= ((SizedCacheable) item.getEntry()).getSize();
            currentByteCount -= shrink;
        }
        return shrink;
	}

	/**
		Write out all dirty buffers.

		<P>MT - must be MT safe.
		Single thread on the part that finds the next dirty buffer to write
		out, the synchronization of cleaning of the individual cachable is
		provided by the cacheable itself.
	 */
	protected void cleanCache(Matchable partialKey) throws StandardException {
	
		int position;

		synchronized(this)
		{
			// this is at many dirty buffers as the cleaner is ever going to
			// see 
			position = holders.size() - 1;
		}


outerscan:
		for (;;) {

			CachedItem item = null;

			synchronized (this) {

				// the cache may have shrunk by quite a bit since we last came
				// in here
				int size = holders.size();
				if (position >= size)
					position = size - 1;

innerscan:
				// go from position (the last cached item in the holder array
				// to 0 (the first).  Otherwise, if we go from 0 to
				// position, some other thread may come in and shrink items
				// which is between 0 and position.  Since a shrink move all
				// items up, we may skip some items without cleaning.
				for ( ;  position >= 0; position--, item = null) {

					item = (CachedItem) holders.get(position);

					if (!item.isValid())
						continue innerscan;

					if (!item.getEntry().isDirty())
						continue innerscan;

					if (partialKey != null) {

						Object key = item.getEntry().getIdentity();

						if (!partialKey.match(key))
							continue;
					}

					item.keepForClean();
					break innerscan;
				}
			} // end of synchronized block

			if (position < 0)
			{
				return;
			}

			try {

				item.clean(false);
			} finally {
				release(item);
			}
			position--;
			
		} // for (;;)
	}


	protected long shrinkSize(long currentSize) {

		long maxSize = getMaximumSize();

		long toShrink = currentSize - maxSize;
		if (toShrink <= 0)
			return 0;

		// only shrink 10% of the maximum size
		long shrinkLimit = maxSize / 10;
		if (shrinkLimit == 0)
			shrinkLimit = 2;

		if (toShrink < shrinkLimit)
			return toShrink;
		else
			return shrinkLimit;
	}

	/**
		The background cleaner tries to make sure that there are serveral
		cleaned or invalied buffers ahead of the clock hand so that when they
		are evicted, they don't need to be cleaned.

		The way this routine work is as follows, starting at the current clock
		hand position, go forward around the cache buffers, moving the same
		route that the clock hand moves.  It keep tracks of the number of
		invalid or not recently used buffers it sees along the way.  If it sees
		a not recently used buffer, it will clean it.  After it has seen N
		invalid or not recently used buffers, or it has gone around and visited
		all buffers in the cache, it finished.

		It does not clean recently used buffers.

		<P>MT - must be MT-safe.  It takes a snapshot of the current clock hand
		position (a synchronous call).  Getting and looking at the next
		serveral cached item is synchronized on this (RESOLVE: probably doesn't
		need to be).  Cleaning of the cacheable is handle by the cacheable itself.

	*/
	protected int performWork(boolean shrinkOnly)
	{
		long target;
		long toShrink;
        int maxLooks;

		synchronized(this)
		{
			if (!active) {
				needService = false;
				return Serviceable.DONE;
			}
			else {
				long currentSize = getCurrentSize();
				target = currentSize / 20;  // attempt to get 5% of the cache clean
				toShrink = wokenToClean ? 0 : shrinkSize(currentSize);
			}

			if (target == 0) {
				wokenToClean = false;
				needService = false;
				return Serviceable.DONE;
			}

			if (!wokenToClean && (toShrink <= 0)) {
				needService = false;
				return Serviceable.DONE;
			}

            maxLooks = useByteCount ? (holders.size()/10) : (int) (target * 2);
		}

		// try to clean the next N (target) cached item, 
		long clean = 0;
		int cleaned = 0; // only used in debug
		CachedItem item = null;
        int currentPosition = 0;

		String ThreadName = null;

		if (SanityManager.DEBUG) {
		  if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
		  {
			ThreadName = Thread.currentThread().getName();
			SanityManager.DEBUG(DaemonService.DaemonTrace, ThreadName + " Cleaning " + name + " clientNumber " + myClientNumber);
		  }
		}


		synchronized(this)
		{
			int itemCount = holders.size();
			currentPosition = clockHand;
					
			// see if the cache needs to shrink
			boolean shrunk = false;
            long currentSize = getCurrentSize();

			for (; shrinkOnly ? (currentSize > maximumSize && toShrink > 0) : (clean < target); item = null)
			{				
				if (++currentPosition >= itemCount) {
					if (itemCount == 0)
						break;

					currentPosition = 0;

				}

				if (maxLooks-- <= 0)
				{
					if (SanityManager.DEBUG) {
						if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace)) {
						SanityManager.DEBUG(DaemonService.DaemonTrace, ThreadName + " done one round of " + name);
						}
					}

					break;			// done one round
				}

				item = (CachedItem) holders.get(currentPosition);

				if (item.isKept())
					continue;

				if (!item.isValid())
				{
					if (toShrink > 0) {

						if (SanityManager.DEBUG) {
							if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace)) {
						SanityManager.DEBUG(ClockFactory.CacheTrace, name + 
											" shrinking item " + item + " at position " + currentPosition);
							}
						}

                        toShrink -= currentSize;
						holders.remove(currentPosition);
                        if( useByteCount)
                            currentByteCount -= getItemSize( item);
                        currentSize = getCurrentSize();
                        toShrink += currentSize;
                        itemCount--;

						// account for the fact all the items have shifted down
						currentPosition--;

						shrunk = true;
					}		
					continue;
				}

				if (item.recentlyUsed())
					continue;

				// found a valid, not kept, and not recently used item
				// this item will be cleaned
                int itemSize = getItemSize( item);
				clean += itemSize;
				if (!item.getEntry().isDirty()) {

					if (toShrink > 0) {
						if (SanityManager.DEBUG) {
							if (SanityManager.DEBUG_ON(ClockFactory.CacheTrace)) {
							SanityManager.DEBUG(ClockFactory.CacheTrace, name + 
											" shrinking item " + item + " at position " + currentPosition);
							}
						}

                        toShrink -= currentSize;
						removeIdentity(item);
						holders.remove(currentPosition);
                        if( useByteCount)
                            currentByteCount -= getItemSize( item);
                        currentSize = getCurrentSize();
                        toShrink += currentSize;
                        itemCount--;
                        shrunk = true;

						// account for the fact all the items have shifted down
						currentPosition--;
					}		
					continue;
				}

				if (shrinkOnly)
					continue;

				// found one that needs cleaning, keep it to clean
				item.keepForClean();
				break;
			} // end of for loop

			if (shrunk)
				trimToSize();

			if (item == null) {
				wokenToClean = false;
				needService = false;
				return Serviceable.DONE;
			}
		} // end of sync block

		try
		{
			if (SanityManager.DEBUG) {
				if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace)) {
				SanityManager.DEBUG(DaemonService.DaemonTrace, ThreadName + " cleaning entry  in " + name);
				}
			}

			item.clean(false);
			if (SanityManager.DEBUG) // only need stats for debug
				   cleaned++;
				
		} catch (StandardException se) {
			// RESOLVE - should probably throw the error into the log.
		}
		finally
		{
			release(item);
			item = null;
		}

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace)) {
			SanityManager.DEBUG(DaemonService.DaemonTrace, ThreadName + " Found " + clean + " clean items, cleaned " +
								cleaned + " items in " + name );
			}
		}

		needService = true;
		return Serviceable.REQUEUE; // return is actually ignored.
	} // end of performWork

    private int getItemSize( CachedItem item)
    {
        if( ! useByteCount)
            return 1;
        SizedCacheable entry = (SizedCacheable) item.getEntry();
        if( null == entry)
            return 0;
        return entry.getSize();
    } // end of getItemSize
    
	/**
		Return statistics about cache that may be implemented.
	**/
	public synchronized long[] getCacheStats()
    {
		stat.currentSize = getCurrentSize();
		return stat.getStats();
    }

	/**
		Reset the statistics to 0.
	**/
	public void resetCacheStats()
    {
		stat.reset();
	}

     /**
     * @return the current maximum size of the cache.
     */
	public synchronized long getMaximumSize()
    {
        return maximumSize;
    }
   
    /**
     * Change the maximum size of the cache. If the size is decreased then cache entries
     * will be thrown out.
     *
     * @param newSize the new maximum cache size
     *
     * @exception StandardException Cloudscape Standard error policy
     */
	public void resize( long newSize) throws StandardException
    {
        boolean shrink;

        synchronized( this)
        {
            maximumSize = newSize;
            stat.maxSize = maximumSize;
            shrink = ( shrinkSize( getCurrentSize()) > 0);
        }
        if( shrink)
        {
            performWork(true /* shrink only */);
            /* performWork does not remove recently used entries nor does it mark them as
             * not recently used. Therefore if the cache has not shrunk enough we will call rotateClock
             * to free up some entries.
             */
            if( shrinkSize( getCurrentSize()) > 0)
            {
                CachedItem freeItem = rotateClock( (float) 2.0);
                /* rotateClock(2.0) means that the clock will rotate through the cache as much as
                 * twice.  If it does not find sufficient unused items the first time through it
                 * will almost certainly find enough of them the second time through, because it
                 * marked all the items as not recently used in the first pass.
                 *
                 * If the cache is very heavily used by other threads then a lot of the items marked as
                 * unused in the first pass may be used before rotateClock passes over them again. In this
                 * unlikely case rotateClock( 2.0) may not be able to clear out enough space to bring the
                 * current size down to the maximum. However the cache size should come down as rotateClock
                 * is called in the normal course of operation.
                 */
                if( freeItem != null)
                    freeItem.unkeepForCreate();
            }
        }
                
    } // end of resize;
    
    private synchronized long getCurrentSize()
    {
        if( ! useByteCount)
            return holders.size();
        return currentByteCount + holders.size()*ITEM_OVERHEAD;
    }

    /**
     * Perform an operation on (approximately) all entries that matches the filter,
     * or all entries if the filter is null.  Entries that are added while the
     * cache is being scanned might or might not be missed.
     *
     * @param filter
     * @param operator
     */
    public void scan( Matchable filter, Operator operator)
    {
        int itemCount = 1;
        Cacheable entry = null;
        CachedItem item = null;

        // Do not call the operator while holding the synchronization lock.
        // However we cannot access an item's links without holding the synchronization lock,
        // nor can we assume that an item is still in the cache unless we hold the synchronization
        // lock or the item is marked as kept.
        for( int position = 0;; position++)
        {
            synchronized( this)
            {
                if( null != item)
                {
                    release( item);
                    item = null;
                }
                    
                for( ; position < holders.size(); position++)
                {
                    item = (CachedItem) holders.get( position);
                    if( null != item)
                    {
                        try
                        {
                            entry = item.use();
                        }
                        catch( StandardException se)
                        {
                            continue;
                        }
                    
                        if( null != entry && (null == filter || filter.match( entry)))
                        {
                            item.keepForClean();
                            break;
                        }
                    }
                }
                if( position >= holders.size())
                    return;

            } // end of synchronization
            operator.operate( entry);
            // Do not release the item until we have re-acquired the synchronization lock.
            // Otherwise the item may be removed and its next link invalidated.
        }
    } // end of scan

    private int trimRequests = 0;
    
    /* Trim out invalid items from holders if there are a lot of them. This is expensive if
     * holders is large.
     * The caller must hold the cache synchronization lock.
     */
    private void trimToSize()
    {
        int size = holders.size();

        // Trimming is expensive, don't do it often.
        trimRequests++;
        if( trimRequests < size/8)
            return;
        trimRequests = 0;
        
		// move invalid items to the end.
		int endPosition = size - 1;

		int invalidCount = 0;
        for (int i = 0; i <= endPosition; i++)
        {
            CachedItem item = (CachedItem) holders.get(i);

			if (item.isKept())
				continue;

            if (item.isValid())
				continue;

			invalidCount++;

			// swap with an item later in the list
			// try to keep free items at the end of the holders array.
			for (; endPosition > i; endPosition--) {
				CachedItem last = (CachedItem) holders.get(endPosition);
				if (last.isValid()) {
					holders.set(i, last);
					holders.set(endPosition, item);
					endPosition--;
					break;
				}
			}
		}
		// small cache - don't shrink.
		if (size < 32)
			return;
		
		// now decide if we need to shrink the holder array or not.
		int validItems = size - invalidCount;

		// over 75% entries used, don't shrink.
		if (validItems > ((3 * size) / 4))
			return;

		// keep 10% new items.
		int newSize = validItems + (validItems / 10);

		if (newSize >= size)
			return;

		// remove items, starting at the end,  where
		// hopefully most of the free items are.
		for (int r = size - 1; r > newSize; r--) {
			CachedItem remove = (CachedItem) holders.get(r);
			if (remove.isKept() || remove.isValid()) {
				continue;
			}

			if (useByteCount) {
				currentByteCount -= getItemSize(remove);
			}

			holders.remove(r);
		}

		holders.trimToSize();
		// move the clock hand to the start of the invalid items.
		clockHand = validItems + 1;

    } // end of trimToSize
}
