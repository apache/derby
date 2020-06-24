/*

   Derby - Class org.apache.derby.impl.store.access.RAMAccessManager

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

package org.apache.derby.impl.store.access;

import org.apache.derby.iapi.security.Securable;
import org.apache.derby.iapi.security.SecurityUtil;

import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheableFactory;
import org.apache.derby.iapi.services.cache.CacheFactory;
import org.apache.derby.iapi.services.cache.CacheManager;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.iapi.services.property.PropertySetCallback;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ConglomerateFactory;
import org.apache.derby.iapi.store.access.conglomerate.MethodFactory;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.services.property.PropertyFactory;

import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.TransactionInfo;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.data.DataFactory;

import org.apache.derby.catalog.UUID;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.Attribute;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.AccessController;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

import java.io.Serializable;


public abstract class RAMAccessManager
    implements AccessFactory, 
               CacheableFactory, 
               ModuleControl, 
               PropertySetCallback
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**
    The raw store that this access manager uses.
    **/
    private RawStoreFactory rawstore;

    /**
    Hash table on primary implementation type.
    **/
    private Hashtable<String,MethodFactory> implhash;

    /**
    Hash table on primary format.
    **/
    private Hashtable<UUID,MethodFactory> formathash;

	/**
	Service properties.  These are supplied from ModuleControl.boot(),
	and ultimately come from the service.properties file.
	By convention, these properties are passed down to all modules
	booted by this one.  If this module needs to pass specific instructions
	to its sub-modules, it should create a new Properties object with
	serviceProperties as its default (so that the rest of the modules
	that are looking at it don't see the properties that this module
	needs to add).
	**/
	private Properties serviceProperties;

    /**
     * Default locking policy for the entire system.
     **/
    LockingPolicy system_default_locking_policy;

	/**	
		The object providing the properties like behaviour
		that is transactional.
	*/
	private PropertyConglomerate xactProperties;
	private PropertyFactory 	pf;

    protected LockingPolicy table_level_policy[];
    protected LockingPolicy record_level_policy[];


    /**
     * A map of the implementation specific id to conglomerate object.
     * <p>
     * A map of the implementation specific id to conglomerate object.
     * The id is encoded into the conglomerate number, and then used to
     * pick the right implementation of the conglomerate.  It is then
     * up to the conglomerate implementation to retrieve it's stored 
     * representation from disk.
     *
     * An internal mapping of the encoding of conglomerate identity in the
     * conglomerate number to the actual conglomerate implementation.  Encoding
     * this means that we can't dynamically add conglomerate implementations
     * into the system, so when we want to do that this mapping will have to
     * be more dynamic - but for now store knows exactly what implementations
     * there are.
     **/
    protected ConglomerateFactory conglom_map[];

    /**
     * Cache of Conglomerate objects, keyed by conglom id.  Used to speed up
     * subsquent open of conglomerates, first open will need to call the 
     * conglomerate to read and return it's description.
     **/
    private CacheManager    conglom_cache;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    public RAMAccessManager()
    {
        // Intialize the hash tables that hold the access methods that
        // this access manager knows about.
        implhash   = new Hashtable<String,MethodFactory>();
        formathash = new Hashtable<UUID,MethodFactory>();
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**
     * Return the default locking policy for this access manager.
     *
	 * @return the default locking policy for this accessmanager.
     **/
    protected LockingPolicy getDefaultLockingPolicy()
    {
        return(system_default_locking_policy);
    }


    RawStoreFactory getRawStore()
    {
        return rawstore;
    }


	PropertyConglomerate getTransactionalProperties()
    {
		return xactProperties;
	}

    private void boot_load_conglom_map()
        throws StandardException
    {
        // System.out.println("before new code.");

        conglom_map = new ConglomerateFactory[2];

		// Find the appropriate factory for the desired implementation.
		MethodFactory mfactory = findMethodFactoryByImpl("heap");

		if (mfactory == null || !(mfactory instanceof ConglomerateFactory))
        {
			throw StandardException.newException(
                    SQLState.AM_NO_SUCH_CONGLOMERATE_TYPE, "heap");
        }

        conglom_map[ConglomerateFactory.HEAP_FACTORY_ID] = 
            (ConglomerateFactory) mfactory;

		// Find the appropriate factory for the desired implementation.
		mfactory = findMethodFactoryByImpl("BTREE");

		if (mfactory == null || !(mfactory instanceof ConglomerateFactory))
        {
			throw StandardException.newException(
                    SQLState.AM_NO_SUCH_CONGLOMERATE_TYPE, "BTREE");
        }
        conglom_map[ConglomerateFactory.BTREE_FACTORY_ID] = 
            (ConglomerateFactory) mfactory;

        // System.out.println("conglom_map[0] = " + conglom_map[0]);
        // System.out.println("conglom_map[1] = " + conglom_map[1]);
    }




    /***************************************************************************
    ** Abstract Methods of RAMAccessManager, interfaces that control locking
    ** level of the system.
    ****************************************************************************
    */

    /**
     * Return the locking level of the system.
     * <p>
     * This routine controls the lowest level of locking enabled for all locks
     * for all tables accessed through this accessmanager.  The concrete 
     * implementation may set this value always to table level locking for
     * a client configuration, or it may set it to row level locking for a
     * server configuration.
     * <p>
     * If TransactionController.MODE_RECORD is returned table may either be
     * locked at table or row locking depending on the type of access expected
     * (ie. level 3 will require table locking for heap scans.)
     *
	 * @return TransactionController.MODE_TABLE if only table locking allowed,
     *         else returns TransactionController.MODE_RECORD.
     *
     **/
    abstract protected int getSystemLockLevel();

    /**
     * Query property system to get the System lock level.
     * <p>
     * This routine will be called during boot after access has booted far 
     * enough, to allow access to the property conglomerate.  This routine
     * will call the property system and set the value to be returned by
     * getSystemLockLevel().
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    abstract protected void bootLookupSystemLockLevel(
    TransactionController tc)
		throws StandardException;

    /**************************************************************************
     * Routines to map to/from conglomid/containerid:
     **************************************************************************
     */
    private long conglom_nextid = 0;

    /**
     * Return next conglomid to try to add the container with.
     * <p>
     * The conglomerate number has 2 parts.  The low 4 bits are used to 
     * encode the factory which "owns" the conglomerate.  The high 60 bits
     * are used as a normal unique id mechanism.
     * <p>
     * So for example if the next id to assign is 0x54 the following will
     * be the conglomid:
     *     if a HEAP  (factory 0) - 0x540
     *     if a BTREE (factory 1) - 0x541
     *
     * And the next id assigned will be:
     *     if a HEAP  (factory 0) - 0x550
     *     if a BTREE (factory 1) - 0x551
     *
     * @param factory_type factory id as gotten from getConglomerateFactoryId()
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected long getNextConglomId(int   factory_type)
		throws StandardException
    {
        long    conglomid;

        if (SanityManager.DEBUG)
        {
            // current code depends on this range, if we ever need to expand the
            // range we can claim bits from the high order of the long.

            SanityManager.ASSERT(factory_type >= 0x00 && factory_type <= 0x0f);
        }

        synchronized (conglom_cache)
        {
            if (conglom_nextid == 0)
            {
                // shift out the factory id and then add 1.
                conglom_nextid = (rawstore.getMaxContainerId() >> 4) + 1;
            }

            conglomid = conglom_nextid++;
        }

        // shift in the factory id and then return the conglomid.
        
        return((conglomid << 4) | factory_type);
    }

    /**
     * Bump the conglomid.
     * <p>
     * For some reason we have found that the give conglomid already exists
     * in the directory so just bump the next conglomid to greater than this
     * one.  The algorithm to store and retrieve the last conglomid is not
     * transactional as we don't want to pay the overhead for such an algorithm
     * on every ddl statement - so it is possible to "lose" an update to the
     * counter if we crash at an inopportune moment.  In general the upper
     * level store code will just handle the error from addContainer which 
     * says there already exists a conglom with that id, update the next
     * conglomid and then try again.
     * <p>
     *
     * @param conglomid The conglomid which already exists.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    // currently not used, but this is one idea on how to handle 
    // non-transactional update of the nextid field, just handle the error
    // if we try to create a conglom and find the container already exists.
    /*
    private void handleConglomidExists(
    long   conglomid)
		throws StandardException
    {
        synchronized (conglom_cache)
        {
            conglom_nextid = ((conglomid >> 4) + 1);
        }
    }
    */

    /**
     * Given a conglomid, return the factory which "owns" it.
     * <p>
     * A simple lookup on the boot time built table which maps the low order
     * 4 bits into which factory owns the conglomerate.
     * <p>
     *
     * @param conglom_id The conglomerate id of the conglomerate to look up.
     *
	 * @return The ConglomerateFactory which "owns" this conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    ConglomerateFactory getFactoryFromConglomId(
    long    conglom_id)
		throws StandardException
    {
        try
        {
            return(conglom_map[((int) (0x0f & conglom_id))]);
        }
        catch (java.lang.ArrayIndexOutOfBoundsException e)
        {
            // just in case language passes in a bad factory id.
			throw StandardException.newException(
                SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST, 
                conglom_id);
        }
    }


    /**************************************************************************
     * Conglomerate Cache routines:
     **************************************************************************
     */

    /**
     * ACCESSMANAGER CONGLOMERATE CACHE - 
     * <p>
     * Every conglomerate in the system is described by an object which 
     * implements Conglomerate.  This object basically contains the parameters
     * which describe the metadata about the conglomerate that store needs
     * to know - like types of columns, number of keys, number of columns, ...
     * <p>
     * It is up to each conglomerate to maintain it's own description, and
     * it's factory must be able to read this info from disk and return it
     * from the ConglomerateFactory.readConglomerate() interface.
     * <p>
     * This cache simply maintains an in memory copy of these conglomerate
     * objects, key'd by conglomerate id.  By caching, this avoids the cost
     * of reading the conglomerate info from disk on each subsequent query
     * which accesses the conglomerate.
     * <p>
     * The interfaces and internal routines which deal with this cache are:
     * conglomCacheInit() - initializes the cache at boot time.
     *
     *
     *
     **/

    /**
     * Initialize the conglomerate cache.
     * <p>
     * Simply calls the cache manager to create the cache with some hard
     * coded defaults for size.
     * <p>
	 * @exception  StandardException  Standard exception policy.
     **/
    private void conglomCacheInit()
        throws StandardException
    {
        // Get a cache factory to create the conglomerate cache.
		CacheFactory cf = 
            (CacheFactory) startSystemModule(
                 org.apache.derby.shared.common.reference.Module.CacheFactory);

        // Now create the conglomerate cache.

	    conglom_cache =
           cf.newCacheManager(
               this, AccessFactoryGlobals.CFG_CONGLOMDIR_CACHE, 200, 300);

    }

    /**
     * Find a conglomerate by conglomid in the cache.
     * <p>
     * Look for a conglomerate given a conglomid.  If in cache return it,
     * otherwise fault in an entry by asking the owning factory to produce
     * an entry.
     * <p>
     *
	 * @return The conglomerate object identified by "conglomid".
     *
     * @param conglomid The conglomerate id of the conglomerate to look up.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    /* package */ Conglomerate conglomCacheFind(long conglomid)
        throws StandardException
    {
        Conglomerate conglom       = null;
        Long         conglomid_obj = conglomid;

        CacheableConglomerate cache_entry =
            (CacheableConglomerate) conglom_cache.find(conglomid_obj);

        if (cache_entry != null) {
            conglom = cache_entry.getConglom();
            conglom_cache.release(cache_entry);
        }

        return(conglom);
    }

    /**
     * Invalide the current Conglomerate Cache.
     * <p>
     * Abort of certain operations will invalidate the contents of the 
     * cache.  Longer term we could just invalidate those entries, but
     * for now just invalidate the whole cache.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    /* package */ protected void conglomCacheInvalidate()
        throws StandardException
    {
        conglom_cache.ageOut();
    }

    /**
     * Add a newly created conglomerate to the cache.
     * <p>
     *
     * @param conglomid   The conglomid of conglomerate to replace.
     * @param conglom     The Conglom to add.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    /* package */ void conglomCacheAddEntry(
    long            conglomid,
    Conglomerate    conglom)
        throws StandardException
    {
        // Insert the new entry.
        CacheableConglomerate conglom_entry = (CacheableConglomerate)
            conglom_cache.create(conglomid, conglom);
        conglom_cache.release(conglom_entry);
    }

    /**
     * Remove an entry from the cache.
     * <p>
     *
     * @param conglomid   The conglomid of conglomerate to replace.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    /* package */ void conglomCacheRemoveEntry(long conglomid)
        throws StandardException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5632
        CacheableConglomerate conglom_entry = (CacheableConglomerate)
            conglom_cache.findCached(conglomid);
//IC see: https://issues.apache.org/jira/browse/DERBY-6856

        if (conglom_entry != null) {
            conglom_cache.remove(conglom_entry);
        }
    }

    /**
     * <p>
     * Get the current transaction context.
     * </p>
     *
     * <p>
     * If there is an internal transaction on the context stack, return the
     * internal transaction. Otherwise, if there is a nested user transaction
     * on the context stack, return the nested transaction. Otherwise,
     * return the current user transaction.
     * </p>
     *
     * @return a context object referencing the current transaction
     */
    RAMTransactionContext getCurrentTransactionContext() {
        RAMTransactionContext rtc =
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
            (RAMTransactionContext) getContext(
                AccessFactoryGlobals.RAMXACT_INTERNAL_CONTEXT_ID);

        if (rtc == null) {
            rtc = (RAMTransactionContext) getContext(
                    AccessFactoryGlobals.RAMXACT_CHILD_CONTEXT_ID);
        }

        if (rtc == null) {
            rtc = (RAMTransactionContext) getContext(
                    AccessFactoryGlobals.RAMXACT_CONTEXT_ID);
        }

        return rtc;
    }

    /**************************************************************************
     * Public Methods implementing AccessFactory Interface:
     **************************************************************************
     */

	/**
	Database creation finished.  Tell RawStore.
	@exception StandardException standard Derby error policy
	*/
	public void createFinished() throws StandardException
	{
		rawstore.createFinished();
	}

    /**
    Find an access method that implements a format type.
    @see AccessFactory#findMethodFactoryByFormat
    **/
    public MethodFactory findMethodFactoryByFormat(UUID format)
    {
        MethodFactory factory;
        
        // See if there's an access method that supports the desired
        // format type as its primary format type.
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        factory = formathash.get(format);
        if (factory != null)
            return factory;

        // No primary format.  See if one of the access methods
        // supports it as a secondary format.
        Enumeration<MethodFactory> e = formathash.elements();
        while (e.hasMoreElements())
        {
            factory = e.nextElement();
            if (factory.supportsFormat(format))
                return factory;
        }

        // No such implementation.
        return null;
    }

    /**
    Find an access method that implements an implementation type.
    @see AccessFactory#findMethodFactoryByImpl
    **/
    public MethodFactory findMethodFactoryByImpl(String impltype)
        throws StandardException
    {
        // See if there's an access method that supports the desired
        // implementation type as its primary implementation type.
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        MethodFactory factory = implhash.get(impltype);
        if (factory != null)
				return factory;

        // No primary implementation.  See if one of the access methods
        // supports the implementation type as a secondary.
        Enumeration<MethodFactory> e = implhash.elements();
        while (e.hasMoreElements())
        {
            factory = e.nextElement();
            if (factory.supportsImplementation(impltype))
                return factory;
        }
		factory = null;

		// try and load an implementation.  a new properties object needs
		// to be created to hold the conglomerate type property, since
		// that value is specific to the conglomerate we want to boot, not
		// to the service as a whole
		Properties conglomProperties = new Properties(serviceProperties);
		conglomProperties.put(AccessFactoryGlobals.CONGLOM_PROP, impltype);

		try {
			factory = 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
               (MethodFactory) bootServiceModule(
                    false, this, MethodFactory.MODULE, 
                    impltype, conglomProperties);
		} catch (StandardException se) {
			if (!se.getMessageId().equals(SQLState.SERVICE_MISSING_IMPLEMENTATION))
				throw se;
		}

		conglomProperties = null;

		if (factory != null) {
			registerAccessMethod(factory);
			return factory;
		}

        // No such implementation.
        return null;
    }

	public LockFactory getLockFactory() {
		return rawstore.getLockFactory();
	}


    public TransactionController getTransaction(
    ContextManager cm)
        throws StandardException
    {
        return getAndNameTransaction(cm, AccessFactoryGlobals.USER_TRANS_NAME);
    }

    public TransactionController getAndNameTransaction(
    ContextManager cm, String transName)
        throws StandardException
    {
        if (cm == null)
            return null;  // XXX (nat) should throw exception

        // See if there's already a transaction context.
        RAMTransactionContext rtc = (RAMTransactionContext)
            cm.getContext(AccessFactoryGlobals.RAMXACT_CONTEXT_ID);

        if (rtc == null)
        {
            // No transaction context.  Create or find a raw store transaction,
            // make a context for it, and push the context.  Note this puts the
            // raw store transaction context above the access context, which is
            // required for error handling assumptions to be correct.
            Transaction rawtran = rawstore.findUserTransaction(cm, transName);
            RAMTransaction rt      = new RAMTransaction(this, rawtran, null);

            rtc = 
                new RAMTransactionContext(
                    cm, 
                    AccessFactoryGlobals.RAMXACT_CONTEXT_ID,
                    rt, false /* abortAll */);

			TransactionController tc = rtc.getTransaction();

			if (xactProperties != null)
            {
				rawtran.setup(tc);
				tc.commit();
			}

            rawtran.setDefaultLockingPolicy(system_default_locking_policy);

			tc.commit();

			return tc;
        }
        return rtc.getTransaction();
    }

	/**
     * Start a global transaction.
     * <p>
	 * Get a transaction controller with which to manipulate data within
	 * the access manager.  Implicitly creates an access context.
     * <p>
     * Must only be called if no other transaction context exists in the
     * current context manager.  If another transaction exists in the context
     * an exception will be thrown.
     * <p>
     * The (format_id, global_id, branch_id) triplet is meant to come exactly
     * from a javax.transaction.xa.Xid.  We don't use Xid so that the system
     * can be delivered on a non-1.2 vm system and not require the javax classes
     * in the path.  
     *
     * @param cm        The context manager for the current context.
     * @param format_id the format id part of the Xid - ie. Xid.getFormatId().
     * @param global_id the global transaction identifier part of XID - ie.
     *                  Xid.getGlobalTransactionId().
     * @param branch_id The branch qualifier of the Xid - ie. 
     *                  Xid.getBranchQaulifier()
     * 	
	 * @exception StandardException Standard exception policy.
	 * @see TransactionController
	 **/
	public /* XATransactionController */ Object startXATransaction(
    ContextManager  cm, 
    int             format_id,
    byte[]          global_id,
    byte[]          branch_id)
		throws StandardException
    {
        RAMTransaction xa_tc = null;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(global_id != null);
            SanityManager.ASSERT(branch_id != null);
        }

        if (cm == null)
            return null;  // XXX (nat) should throw exception

        // See if there's already a transaction context.
        RAMTransactionContext rtc = (RAMTransactionContext) 
            cm.getContext(AccessFactoryGlobals.RAMXACT_CONTEXT_ID);

        if (rtc == null)
        {
            // No transaction context.  Create or find a raw store transaction,
            // make a context for it, and push the context.  Note this puts the
            // raw store transaction context above the access context, which is
            // required for error handling assumptions to be correct.
            Transaction rawtran = 
                rawstore.startGlobalTransaction(
                    cm, format_id, global_id, branch_id);

            xa_tc                    = new RAMTransaction(this, rawtran, null);

            rtc = 
                new RAMTransactionContext(
                    cm, 
                    AccessFactoryGlobals.RAMXACT_CONTEXT_ID,
                    xa_tc, false /* abortAll */);

            // RESOLVE - an XA transaction can only commit once so, if we
            // acquire readlocks.

			if (xactProperties != null) 
            {
				rawtran.setup(xa_tc);

                // HACK - special support has been added to the commitNoSync
                // of a global xact, to allow committing of read only xact, 
                // which will allow subsequent activity on the xact keeping
                // the same global transaction id.
                xa_tc.commitNoSync(
                    TransactionController.RELEASE_LOCKS |
                    TransactionController.READONLY_TRANSACTION_INITIALIZATION);
			}

            rawtran.setDefaultLockingPolicy(system_default_locking_policy);

            // HACK - special support has been added to the commitNoSync
            // of a global xact, to allow committing of read only xact, 
            // which will allow subsequent activity on the xact keeping
            // the same global transaction id.
            xa_tc.commitNoSync(
                TransactionController.RELEASE_LOCKS |
                TransactionController.READONLY_TRANSACTION_INITIALIZATION);
        }
        else
        {
            // throw an error.
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT(
                    "RAMTransactionContext found on stack.");
        }

        return(xa_tc);
    }


    /**
     * Return the XAResourceManager associated with this AccessFactory.
     * <p>
     * Returns an object which can be used to implement the "offline" 
     * 2 phase commit interaction between the accessfactory and outstanding
     * transaction managers taking care of in-doubt transactions.
     *
     * @return The XAResourceManager associated with this accessfactory.
     *
     **/
	public /* XAResourceManager */ Object getXAResourceManager()
        throws StandardException
    {
        return(rawstore.getXAResourceManager());
    }

    public void registerAccessMethod(MethodFactory factory)
    {
        // Put the access method's primary implementation type in
        // a hash table so we can find it quickly.
        implhash.put(factory.primaryImplementationType(), factory);

        // Put the access method's primary format in a hash table
        // so we can find it quickly.
        formathash.put(factory.primaryFormat(), factory);
    }

	public boolean isReadOnly()
	{
		return rawstore.isReadOnly();
	}

    /**
     * DERBY-5996(Create readme files (cautioning users against modifying 
     *  database files) at database hard upgrade time)
     * This gets called during hard upgrade. It will create 3 readme files
     *  one in database directory, one in "seg0" directory and one in log
     *  directory. These readme files warn users against touching any of
     *  files associated with derby database 
     */
    public void createReadMeFiles()
        throws StandardException
    {
        //creating readme in "seg0" directory
        rawstore.createDataWarningFile();

        //creating readme in log directory
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        LogFactory logFactory =(LogFactory) findServiceModule(this, rawstore.getLogFactoryModule());
        logFactory.createDataWarningFile();

        //creating readme in root database directory
        DataFactory dataFactory =(DataFactory) findServiceModule(this, rawstore.getDataFactoryModule());
        PersistentService ps = getMonitor().getServiceType(rawstore);
        ps.createDataWarningFile(dataFactory.getStorageFactory());
    }
	
	private void addPropertySetNotification(PropertySetCallback who, TransactionController tc) {

		pf.addPropertySetNotification(who);
		
		// set up the initial values by calling the validate and apply methods.
		// the map methods are not called as they will have been called
		// at runtime when the user set the property.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        Dictionary<Object, Object> d = new Hashtable<Object, Object>();
		try {
			xactProperties.getProperties(tc,d,false/*!stringsOnly*/,false/*!defaultsOnly*/);
		} catch (StandardException se) {
			return;
		}

		boolean dbOnly = PropertyUtil.isDBOnly(d);

		who.init(dbOnly, d);
	}

	public TransactionInfo[] getTransactionInfo()
	{
		return rawstore.getTransactionInfo();
	}

    /**
     * Start the replication master role for this database.
     * @param dbmaster The master database that is being replicated.
     * @param host The hostname for the slave
     * @param port The port the slave is listening on
     * @param replicationMode The type of replication contract.
     * Currently only asynchronous replication is supported, but
     * 1-safe/2-safe/very-safe modes may be added later.
     * @exception StandardException Standard Derby exception policy,
     * thrown on error.
     */
    public void startReplicationMaster(String dbmaster, String host, int port,
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
                                       String replicationMode)
        throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3189
        rawstore.startReplicationMaster(dbmaster, host, port, replicationMode);
    }
    
    /**
     * @see org.apache.derby.iapi.store.access.AccessFactory#failover(String dbname).
     */
    public void failover(String dbname) throws StandardException {
        rawstore.failover(dbname);
    }

    /**
     * Stop the replication master role for this database.
     * 
     * @exception StandardException Standard Derby exception policy,
     * thrown on error.
     */
    public void stopReplicationMaster() throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3189
        rawstore.stopReplicationMaster();
    }

	public void freeze() throws StandardException
	{
        // make sure that application code doesn't bypass security checks
        // by calling this public entry point
//IC see: https://issues.apache.org/jira/browse/DERBY-6616
        SecurityUtil.authorize( Securable.FREEZE_DATABASE );
            
		rawstore.freeze();
	}

	public void unfreeze() throws StandardException
	{
        // make sure that application code doesn't bypass security checks
        // by calling this public entry point
        SecurityUtil.authorize( Securable.UNFREEZE_DATABASE );
            
		rawstore.unfreeze();
	}

    public void backup(
//IC see: https://issues.apache.org/jira/browse/DERBY-239
//IC see: https://issues.apache.org/jira/browse/DERBY-523
    String  backupDir, 
    boolean wait) 
        throws StandardException
	{
        // make sure that application code doesn't bypass security checks
        // by calling this public entry point
//IC see: https://issues.apache.org/jira/browse/DERBY-6616
        SecurityUtil.authorize
            (
             wait ?
             Securable.BACKUP_DATABASE :
             Securable.BACKUP_DATABASE_NOWAIT
             );
		rawstore.backup(backupDir, wait);
	}


    public void backupAndEnableLogArchiveMode(
    String  backupDir, 
    boolean deleteOnlineArchivedLogFiles,
    boolean wait)
		throws StandardException 
	{
        // make sure that application code doesn't bypass security checks
        // by calling this public entry point
//IC see: https://issues.apache.org/jira/browse/DERBY-6616
        SecurityUtil.authorize
            (
             wait ?
             Securable.BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE :
             Securable.BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT
             );
		rawstore.backupAndEnableLogArchiveMode(backupDir, 
                                               deleteOnlineArchivedLogFiles, 
                                               wait);
	}

	public void disableLogArchiveMode(boolean deleteOnlineArchivedLogFiles)
		throws StandardException
	{
        // make sure that application code doesn't bypass security checks
        // by calling this public entry point
//IC see: https://issues.apache.org/jira/browse/DERBY-6616
        SecurityUtil.authorize( Securable.DISABLE_LOG_ARCHIVE_MODE );
            
		rawstore.disableLogArchiveMode(deleteOnlineArchivedLogFiles);
	}



	public void checkpoint() throws StandardException
	{
        // make sure that application code doesn't bypass security checks
        // by calling this public entry point
//IC see: https://issues.apache.org/jira/browse/DERBY-6616
        SecurityUtil.authorize( Securable.CHECKPOINT_DATABASE );
            
		rawstore.checkpoint();
	}

	public void waitForPostCommitToFinishWork()
	{
		rawstore.getDaemon().waitUntilQueueIsEmpty();
	}

    /**************************************************************************
     * Public Methods implementing ModuleControl Interface:
     **************************************************************************
     */
	public void boot(boolean create, Properties startParams)
		throws StandardException
	{
		this.serviceProperties = startParams;

        boot_load_conglom_map();

        if (create)
        {
            // if we are creating the db, then just start the conglomid's at
            // 1, and proceed from there.  If not create, we delay 
            // initialization of this until the first ddl which needs a new
            // id.
            conglom_nextid = 1;
        }

        // Access depends on a Raw Store implementations.  Load it.
        //
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        rawstore = (RawStoreFactory) bootServiceModule(
            create, this, RawStoreFactory.MODULE, serviceProperties);

        // initialize handler with raw store to be called in the event of
        // aborted inserts.  Store will use the call back to reclaim space
        // when these events happen.  See DERBY-4057.
        rawstore.setUndoInsertEventHandler(new RAMAccessUndoHandler(this));

		// Note: we also boot this module here since we may start Derby
		// system from store access layer, as some of the unit test case,
		// not from JDBC layer.(See
		// /protocol/Database/Storage/Access/Interface/T_AccessFactory.java)
		// If this module has already been booted by the JDBC layer, this will 
		// have no effect at all.
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
		bootServiceModule(
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            create, this, org.apache.derby.shared.common.reference.Module.PropertyFactory, 
            startParams);

        // Create the in-memory conglomerate directory

        conglomCacheInit();

        // Read in the conglomerate directory from the conglom conglom
        // Create the conglom conglom from within a separate system xact
        RAMTransaction tc =
            (RAMTransaction) getAndNameTransaction(
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                getContextService().getCurrentContextManager(),
                AccessFactoryGlobals.USER_TRANS_NAME);

        // looking up lock_mode is dependant on access booting, but
        // some boot routines need lock_mode and
        // system_default_locking_policy, so during boot do table level
        // locking and then look up the "right" locking level.

        int lock_mode = LockingPolicy.MODE_CONTAINER;

        system_default_locking_policy =
            tc.getRawStoreXact().newLockingPolicy(
                lock_mode,
                TransactionController.ISOLATION_SERIALIZABLE, true);


        // RESOLVE - code reduction - get rid of this table, and somehow
        // combine it with the raw store one.

        table_level_policy = new LockingPolicy[6];

        table_level_policy[TransactionController.ISOLATION_NOLOCK] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_CONTAINER,
                TransactionController.ISOLATION_NOLOCK, true);

        table_level_policy[TransactionController.ISOLATION_READ_UNCOMMITTED] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_CONTAINER,
                TransactionController.ISOLATION_READ_UNCOMMITTED, true);

        table_level_policy[TransactionController.ISOLATION_READ_COMMITTED] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_CONTAINER,
                TransactionController.ISOLATION_READ_COMMITTED, true);

        table_level_policy[TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_CONTAINER,
                TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK,
                true);

        table_level_policy[TransactionController.ISOLATION_REPEATABLE_READ] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_CONTAINER,
                TransactionController.ISOLATION_REPEATABLE_READ, true);

        table_level_policy[TransactionController.ISOLATION_SERIALIZABLE] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_CONTAINER,
                TransactionController.ISOLATION_SERIALIZABLE, true);

        record_level_policy = new LockingPolicy[6];

        record_level_policy[TransactionController.ISOLATION_NOLOCK] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_RECORD,
                TransactionController.ISOLATION_NOLOCK, true);

        record_level_policy[TransactionController.ISOLATION_READ_UNCOMMITTED] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_RECORD,
                TransactionController.ISOLATION_READ_UNCOMMITTED, true);

        record_level_policy[TransactionController.ISOLATION_READ_COMMITTED] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_RECORD,
                TransactionController.ISOLATION_READ_COMMITTED, true);

        record_level_policy[TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_RECORD,
                TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK,
                true);

        record_level_policy[TransactionController.ISOLATION_REPEATABLE_READ] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ, true);

        record_level_policy[TransactionController.ISOLATION_SERIALIZABLE] =
            tc.getRawStoreXact().newLockingPolicy(
                LockingPolicy.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE, true);

        if (SanityManager.DEBUG)
        {
            for (int i = 0;
                 i < TransactionController.ISOLATION_SERIALIZABLE;
                 i++)
            {
                SanityManager.ASSERT(
                    table_level_policy[i] != null,
                    "table_level_policy[" + i + "] is null");
                SanityManager.ASSERT(
                    record_level_policy[i] != null,
                    "record_level_policy[" + i + "] is null");
            }
        }

        tc.commit();

        // set up the property validation
        pf = (PropertyFactory) 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
            findServiceModule(
                this, org.apache.derby.shared.common.reference.Module.PropertyFactory);
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

        // set up the transaction properties.  On J9, over NFS, runing on a
        // power PC coprossor, the directories were created fine, but create
        // db would fail when trying to create this first file in seg0.
        xactProperties = new PropertyConglomerate(tc, create, startParams, pf);

        //Put a readme file in seg0 directory, alerting users to not 
        // touch or remove any of the files there 
        if(create) {
            rawstore.createDataWarningFile();
        }

        // see if there is any properties that raw store needs to know
        // about
        rawstore.getRawStoreProperties(tc);

        // now that access and raw store are booted, do the property lookup
        // which may do conglomerate access.
        bootLookupSystemLockLevel(tc);

        lock_mode =
            (getSystemLockLevel() == TransactionController.MODE_TABLE ?
                 LockingPolicy.MODE_CONTAINER : LockingPolicy.MODE_RECORD);

        system_default_locking_policy =
            tc.getRawStoreXact().newLockingPolicy(
                lock_mode,
                TransactionController.ISOLATION_SERIALIZABLE, true);

        // set up the callbacl for the lock manager with initialization
        addPropertySetNotification(getLockFactory(), tc);

        // make sure user cannot change these properties
        addPropertySetNotification(this,tc);

        tc.commit();

        tc.destroy();
        tc = null;

        if (SanityManager.DEBUG)
        {
            // RESOLVE - (mikem) currently these constants need to be the
            // same, but for modularity reasons there are 2 sets.  Probably
            // should only be one set.  For now just make sure they are the
            // same value.
            SanityManager.ASSERT(
                TransactionController.OPENMODE_USE_UPDATE_LOCKS ==
                ContainerHandle.MODE_USE_UPDATE_LOCKS);
            SanityManager.ASSERT(
                TransactionController.OPENMODE_SECONDARY_LOCKED ==
                ContainerHandle.MODE_SECONDARY_LOCKED);
            SanityManager.ASSERT(
                TransactionController.OPENMODE_BASEROW_INSERT_LOCKED ==
                ContainerHandle.MODE_BASEROW_INSERT_LOCKED);
            SanityManager.ASSERT(
                TransactionController.OPENMODE_FORUPDATE ==
                ContainerHandle.MODE_FORUPDATE);
            SanityManager.ASSERT(
                TransactionController.OPENMODE_FOR_LOCK_ONLY ==
                ContainerHandle.MODE_OPEN_FOR_LOCK_ONLY);
        }
	}

    public void stop()
    {
    }

    /* Methods of the PropertySetCallback interface */

    // This interface is implemented to ensure the user cannot change the
    // encryption provider or algorithm.

	public void init(boolean dbOnly, Dictionary p)
    {
    }

    public boolean validate(String key, Serializable value, Dictionary p)
		 throws StandardException
    {
        if (key.equals(Attribute.CRYPTO_ALGORITHM))
        {
            throw StandardException.newException(SQLState.ENCRYPTION_NOCHANGE_ALGORITHM);
		}
        if (key.equals(Attribute.CRYPTO_PROVIDER))
        {
            throw StandardException.newException(SQLState.ENCRYPTION_NOCHANGE_PROVIDER);
		}
        return true;
    }

    public Serviceable apply(String key, Serializable value, Dictionary p)
		 throws StandardException
    {
        return null;
    }

    public Serializable map(String key, Serializable value, Dictionary p)
		 throws StandardException
    {
        return null;
    }

    // ///////////////////////////////////////////////////////////////

	/*
	** CacheableFactory interface
	*/

	public Cacheable newCacheable(CacheManager cm) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5632
		return new CacheableConglomerate(this);
	}

    // ///////////////////////////////////////////////////////////////

    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private static  ContextService    getContextService()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getFactory();
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<ContextService>()
                 {
                     public ContextService run()
                     {
                         return ContextService.getFactory();
                     }
                 }
                 );
        }
    }

    /**
     * Privileged lookup of a Context. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Context    getContext( final String contextID )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Context>()
             {
                 public Context run()
                 {
                     return ContextService.getContext( contextID );
                 }
             }
             );
    }

    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

    
    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object  startSystemModule( final String factoryInterface )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.startSystemModule( factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object bootServiceModule
        (
         final boolean create, final Object serviceModule,
         final String factoryInterface, final Properties properties
         )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.bootServiceModule( create, serviceModule, factoryInterface, properties );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }


    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object bootServiceModule
        (
         final boolean create, final Object serviceModule,
         final String factoryInterface, final String identifier, final Properties properties
         )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.bootServiceModule( create, serviceModule, factoryInterface, identifier, properties );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object findServiceModule( final Object serviceModule, final String factoryInterface)
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.findServiceModule( serviceModule, factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

}
