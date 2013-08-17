/*

   Derby - Class org.apache.derby.impl.sql.depend.BasicDependencyManager

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

package org.apache.derby.impl.sql.depend;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.depend.ProviderList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DependencyDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * The dependency manager tracks needs that dependents have of providers.
 * <p>
 * A dependency can be either persistent or non-persistent. Persistent
 * dependencies are stored in the data dictionary, and non-persistent
 * dependencies are stored within the dependency manager itself (in memory).
 * <p>
 * <em>Synchronization:</em> The need for synchronization is different depending
 * on whether the dependency is an in-memory dependency or a stored dependency.
 * When accessing and modifying in-memory dependencies, Java synchronization
 * must be used (specifically, we synchronize on {@code this}). When accessing
 * and modifying stored dependencies, which are stored in the data dictionary,
 * we expect that the locking protocols will provide the synchronization needed.
 * Note that stored dependencies should not be accessed while holding the
 * monitor of {@code this}, as this may result in deadlocks. So far the need
 * for synchronization across both in-memory and stored dependencies hasn't
 * occurred.
 */
public class BasicDependencyManager implements DependencyManager {
  
    /**
     * DataDictionary for this database.
     */
    private final DataDictionary dd;
    
    /**
     * Map of in-memory dependencies for Dependents.
     * In-memory means that one or both of the Dependent
     * or Provider are non-persistent (isPersistent() returns false).
     * 
     * Key is the UUID of the Dependent (from getObjectID()).
     * Value is a List containing Dependency objects, each
     * of whihc links the same Dependent to a Provider.
     * Dependency objects in the List are unique.
     * 
     */
    //@GuardedBy("this")
    private final Map<UUID,List<Dependency>> dependents = new HashMap<UUID,List<Dependency>>();
    
    /**
     * Map of in-memory dependencies for Providers.
     * In-memory means that one or both of the Dependent
     * or Provider are non-persistent (isPersistent() returns false).
     * 
     * Key is the UUID of the Provider (from getObjectID()).
     * Value is a List containing Dependency objects, each
     * of which links the same Provider to a Dependent.
     * Dependency objects in the List are unique.
     * 
     */    
    //@GuardedBy("this")
    private final Map<UUID,List<Dependency>> providers = new HashMap<UUID,List<Dependency>>();


	//
	// DependencyManager interface
	//

	/**
		adds a dependency from the dependent on the provider.
		This will be considered to be the default type of
		dependency, when dependency types show up.
		<p>
		Implementations of addDependency should be fast --
		performing alot of extra actions to add a dependency would
		be a detriment.

		@param d the dependent
		@param p the provider

		@exception StandardException thrown if something goes wrong
	 */
	public void addDependency(Dependent d, Provider p, ContextManager cm) 
		throws StandardException {
		addDependency(d, p, cm, null);
	}

    /**
     * Adds the dependency to the data dictionary or the in-memory dependency
     * map.
     * <p>
     * The action taken is detmermined by whether the dependent and/or the
     * provider are persistent.
     *
     * @param d the dependent
     * @param p the provider
     * @param cm context manager
     * @param tc transaction controller, used to determine if any transactional
     *      operations should be attempted carried out in a nested transaction.
     *      If {@code tc} is {@code null}, the user transaction is used.
     * @throws StandardException if adding the dependency fails
     */
    private void addDependency(Dependent d, Provider p,
                               ContextManager cm, TransactionController tc)
            throws StandardException {
        // Dependencies are either in-memory or stored, but not both.
        if (! d.isPersistent() || ! p.isPersistent()) {
            addInMemoryDependency(d, p, cm);
        } else {
            addStoredDependency(d, p, cm, tc);
        }
    }

    /**
     * Adds the dependency as an in-memory dependency.
     *
     * @param d the dependent
     * @param p the provider
     * @param cm context manager
     * @throws StandardException if adding the dependency fails
     * @see #addStoredDependency
     */
    private synchronized void addInMemoryDependency(Dependent d, Provider p,
                                                    ContextManager cm)
            throws StandardException {
        Dependency dy = new BasicDependency(d, p);

        // Duplicate dependencies are not added to the lists.
        // If we find that the dependency we are trying to add in
        // one list is a duplicate, then it should be a duplicate in the
        // other list.
        boolean addedToProvs = false;
        boolean addedToDeps =
                addDependencyToTable(dependents, d.getObjectID(), dy);

        if (addedToDeps) {
            addedToProvs = addDependencyToTable(providers, p.getObjectID(), dy);
        } else if (SanityManager.DEBUG) {
            addedToProvs = addDependencyToTable(providers, p.getObjectID(), dy);
        }

        // Dependency should have been added to both or neither.
        if (SanityManager.DEBUG) {
            if (addedToDeps != addedToProvs) {
                SanityManager.THROWASSERT(
                    "addedToDeps (" + addedToDeps +
                    ") and addedToProvs (" +
                    addedToProvs + ") are expected to agree");
            }
        }

        // Add the dependency to the StatementContext, so that
        // it can be cleared on a pre-execution error.
        StatementContext sc = (StatementContext) cm.getContext(
                org.apache.derby.iapi.reference.ContextId.LANG_STATEMENT);
        sc.addDependency(dy);
    }

    /**
     * Adds the dependency as a stored dependency.
     * <p>
     * We expect that transactional locking (in the data dictionary) is enough
     * to protect us from concurrent changes when adding stored dependencies.
     * Adding synchronization here and accessing the data dictionary (which is
     * transactional) may cause deadlocks.
     *
     * @param d the dependent
     * @param p the provider
     * @param cm context manager
     * @param tc transaction controller (may be {@code null})
     * @throws StandardException if adding the dependency fails
     * @see #addInMemoryDependency
     */
    private void addStoredDependency(Dependent d, Provider p,
                                     ContextManager cm,
                                     TransactionController tc)
            throws StandardException {
        LanguageConnectionContext lcc = getLanguageConnectionContext(cm);
        // tc == null means do it in the user transaction
        TransactionController tcToUse =
                (tc == null) ? lcc.getTransactionExecute() : tc;

        // Call the DataDictionary to store the dependency.
        dd.addDescriptor(new DependencyDescriptor(d, p), null,
                         DataDictionary.SYSDEPENDS_CATALOG_NUM, true,
                         tcToUse);
    }

	/**
		drops a single dependency

		@param d the dependent
		@param p the provider

		@exception StandardException thrown if something goes wrong
	 */
	private	void dropDependency(LanguageConnectionContext lcc, Dependent d, Provider p) throws StandardException
	{
		if (SanityManager.DEBUG) {
			// right now, this routine isn't called for in-memory dependencies
			if (! d.isPersistent() || ! p.isPersistent())
			{
				SanityManager.NOTREACHED();
			}
		}

		DependencyDescriptor dependencyDescriptor = new DependencyDescriptor(d, p);

		dd.dropStoredDependency( dependencyDescriptor, 
								 lcc.getTransactionExecute() );
	}


	/**
		mark all dependencies on the named provider as invalid.
		When invalidation types show up, this will use the default
		invalidation type. The dependencies will still exist once
		they are marked invalid; clearDependencies should be used
		to remove dependencies that a dependent has or provider gives.
		<p>
		Implementations of this can take a little time, but are not
		really expected to recompile things against any changes
		made to the provider that caused the invalidation. The
		dependency system makes no guarantees about the state of
		the provider -- implementations can call this before or
		after actually changing the provider to its new state.
		<p>
		Implementations should throw StandardException
		if the invalidation should be disallowed.

		@param p the provider
		@param action	The action causing the invalidate

		@exception StandardException thrown if unable to make it invalid
	 */
	public void invalidateFor(Provider p, int action,
				  LanguageConnectionContext lcc)
		 throws StandardException
	{
		/*
		** Non-persistent dependencies are stored in memory, and need to
		** use "synchronized" to ensure their lists don't change while
		** the invalidation is taking place.  Persistent dependencies are
		** stored in the data dictionary, and we should *not* do anything
		** transactional (like reading from a system table) from within
		** a synchronized method, as it could cause deadlock.
		**
		** Presumably, the transactional locking in the data dictionary
		** is enough to protect us, so that we don't have to put any
		** synchronization in the DependencyManager.
		*/
		if (p.isPersistent())
			coreInvalidateFor(p, action, lcc);
		else {
			synchronized (this) {
				coreInvalidateFor(p, action, lcc);
			}
		}
	}

    /**
     * A version of invalidateFor that does not provide synchronization among
     * invalidators.
     *
     * @param p the provider
     * @param action the action causing the invalidation
     * @param lcc language connection context
     *
     * @throws StandardException if something goes wrong
     */
	private void coreInvalidateFor(Provider p, int action, LanguageConnectionContext lcc)
		throws StandardException
	{
		List<Dependency> list = getDependents(p);

        if (list.isEmpty()) {
			return;
		}


		// affectedCols is passed in from table descriptor provider to indicate
		// which columns it cares; subsetCols is affectedCols' intersection
		// with column bit map found in the provider of SYSDEPENDS line to
		// find out which columns really matter.  If SYSDEPENDS line's
		// dependent is view (or maybe others), provider is table, yet it 
		// doesn't have column bit map because the view was created in a
		// previous version of server which doesn't support column dependency,
		// and we really want it to have (such as in drop column), in any case
		// if we passed in table descriptor to this function with a bit map,
		// we really need this, we generate the bitmaps on the fly and update
		// SYSDEPENDS
        //
        // Note: Since the "previous version of server" mentioned above must
        // be a version that predates Derby, and we don't support upgrade from
        // those versions, we no longer have code to generate the column
        // dependency list on the fly. Instead, an assert has been added to
        // verify that we always have a column bitmap in SYSDEPENDS if the
        // affectedCols bitmap is non-null.

		FormatableBitSet affectedCols = null, subsetCols = null;
		if (p instanceof TableDescriptor)
		{
			affectedCols = ((TableDescriptor) p).getReferencedColumnMap();
			if (affectedCols != null)
				subsetCols = new FormatableBitSet(affectedCols.getLength());
		}

		{
			StandardException noInvalidate = null;
			// We cannot use an iterator here as the invalidations can remove
			// entries from this list. 
			for (int ei = list.size() - 1; ei >= 0; ei--)
			{
				if (ei >= list.size())
					continue;
				Dependency dependency = list.get(ei);

				Dependent dep = dependency.getDependent();

				if (affectedCols != null)
				{
					TableDescriptor td = (TableDescriptor) dependency.getProvider();
					FormatableBitSet providingCols = td.getReferencedColumnMap();
					if (providingCols == null)
					{
						if (dep instanceof ViewDescriptor)
						{
                            // If the table descriptor that was passed in had a
                            // column bit map, so should the provider's table
                            // descriptor. Views that were created with a
                            // database version that predates Derby could lack
                            // a bitmap in the provider and needed to
                            // reconstruct it here by parsing and binding the
                            // original CREATE VIEW statement. However, since
                            // we don't support upgrade from pre-Derby versions,
                            // this code was removed as part of DERBY-6169.
                            if (SanityManager.DEBUG)
                            {
                                SanityManager.THROWASSERT("Expected view to " +
                                        "have referenced column bitmap");
                            }
						}	// if dep instanceof ViewDescriptor
						else
							((TableDescriptor) p).setReferencedColumnMap(null);
					}	// if providingCols == null
					else
					{
                        subsetCols.copyFrom( affectedCols );
						subsetCols.and(providingCols);
						if (subsetCols.anySetBit() == -1)
							continue;
						((TableDescriptor) p).setReferencedColumnMap(subsetCols);
					}
				}

				// generate a list of invalidations that fail.
				try {
					dep.prepareToInvalidate(p, action, lcc);
				} catch (StandardException sqle) {

					if (noInvalidate == null) {
						noInvalidate = sqle;
					} else {
						try {
							sqle.initCause(noInvalidate);
							noInvalidate = sqle;
						} catch (IllegalStateException ise) {
							// We weren't able to chain the exceptions. That's
							// OK, since we always have the first exception we
							// caught. Just skip the current exception.
						}
					}
				}
				if (noInvalidate == null) {

					if (affectedCols != null)
						((TableDescriptor) p).setReferencedColumnMap(affectedCols);

					// REVISIT: future impl will want to mark the individual
					// dependency as invalid as well as the dependent...
					dep.makeInvalid(action, lcc);
				}
			}

			if (noInvalidate != null)
				throw noInvalidate;
		}
	}

	/**
		Erases all of the dependencies the dependent has, be they
		valid or invalid, of any dependency type.  This action is
		usually performed as the first step in revalidating a
		dependent; it first erases all the old dependencies, then
		revalidates itself generating a list of new dependencies,
		and then marks itself valid if all its new dependencies are
		valid.
		<p>
		There might be a future want to clear all dependencies for
		a particular provider, e.g. when destroying the provider.
		However, at present, they are assumed to stick around and
		it is the responsibility of the dependent to erase them when
		revalidating against the new version of the provider.
		<p>
		clearDependencies will delete dependencies if they are
		stored; the delete is finalized at the next commit.

		@param d the dependent
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void clearDependencies(LanguageConnectionContext lcc, Dependent d) throws StandardException {
		clearDependencies(lcc, d, null);
	}

	/**
	 * @inheritDoc
	 */
	public void clearDependencies(LanguageConnectionContext lcc, 
									Dependent d, TransactionController tc) throws StandardException {

        UUID id = d.getObjectID();
        // Remove all the stored dependencies.
        if (d.isPersistent()) {
            boolean wait = (tc == null);
            dd.dropDependentsStoredDependencies(id,
                            ((wait)?lcc.getTransactionExecute():tc),
                            wait);
        }

        // Now remove the in-memory dependencies, if any.
        synchronized(this) {
            List deps = (List) dependents.get(id);
            if (deps != null) {
                Iterator depsIter = deps.iterator();
                // go through the list notifying providers to remove
                // the dependency from their lists
                while (depsIter.hasNext()) {
                    Dependency dy = (Dependency)depsIter.next();
                    clearProviderDependency(dy.getProviderKey(), dy);
                }
                dependents.remove(id);
            }
        }
    }

	/**
	 * Clear the specified in memory dependency.
	 * This is useful for clean-up when an exception occurs.
	 * (We clear all in-memory dependencies added in the current
	 * StatementContext.)
	 */
    public synchronized void clearInMemoryDependency(Dependency dy) {
        final UUID deptId = dy.getDependent().getObjectID();
        final UUID provId = dy.getProviderKey();
        List deps = (List) dependents.get(deptId);

        // NOTE - this is a NEGATIVE Sanity mode check, in sane mode we continue
        // to ensure the dependency manager is consistent.
        if (!SanityManager.DEBUG) {
            // dependency has already been removed
            if (deps == null)
                return;
        }

        List provs = (List) providers.get(provId);

        if (SanityManager.DEBUG)
        {
            // if both are null then everything is OK
            if ((deps != null) || (provs != null)) {

                // ensure that the Dependency dy is either
                // in both lists or in neither. Even if dy
                // is out of the list we can have non-null
                // deps and provs here because other dependencies
                // with the the same providers or dependents can exist

                //
                int depCount = 0;
                if (deps != null) {
                    for (int ci = 0; ci < deps.size(); ci++) {
                        if (dy.equals(deps.get(ci)))
                            depCount++;
                    }
                }

                int provCount = 0;
                if (provs != null) {
                    for (int ci = 0; ci < provs.size(); ci++) {
                        if (dy.equals(provs.get(ci)))
                            provCount++;
                    }
                }

                SanityManager.ASSERT(depCount == provCount,
                        "Dependency count mismatch count in deps: " + depCount +
                        ", count in provs " + provCount +
                        ", dy.getDependent().getObjectID() = " + deptId +
                        ", dy.getProvider().getObjectID() = " + provId);
            }

            // dependency has already been removed,
            // matches code that is protected by !DEBUG above
            if (deps == null)
                return;
        }

        // dependency has already been removed
        if (provs == null)
            return;


        deps.remove(dy);
        if (deps.isEmpty())
            dependents.remove(deptId);
        provs.remove(dy);
        if (provs.isEmpty())
            providers.remove(provId);
    }

	/**
	 * @see DependencyManager#getPersistentProviderInfos
	 *
	 * @exception StandardException		Thrown on error
	 */
    public ProviderInfo[] getPersistentProviderInfos(Dependent dependent)
            throws StandardException {
        List<Provider> provs = getProviders(dependent);

        if (provs.isEmpty()) {
			return EMPTY_PROVIDER_INFO;
		}

        List<ProviderInfo> pih = new ArrayList<ProviderInfo>();

        for (Provider p : provs) {
            if (p.isPersistent()) {
				pih.add(new BasicProviderInfo(
                                        p.getObjectID(),
                                        p.getDependableFinder(),
                                        p.getObjectName()
									));
			}
		}

		return (ProviderInfo[]) pih.toArray(EMPTY_PROVIDER_INFO);
	}

	private static final ProviderInfo[] EMPTY_PROVIDER_INFO = new ProviderInfo[0];

	/**
	 * @see DependencyManager#getPersistentProviderInfos
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ProviderInfo[] getPersistentProviderInfos(ProviderList pl)
							throws StandardException
	{
		Enumeration e = pl.elements();
		int			numProviders = 0;
		ProviderInfo[]	retval;

		/*
		** We make 2 passes - the first to count the number of persistent
 		** providers and the second to populate the array of ProviderInfos.
		*/
		while (e != null && e.hasMoreElements())
		{
			Provider prov = (Provider) e.nextElement();

			if (prov.isPersistent())
			{
				numProviders++;
			}
		}

		e = pl.elements();
		retval = new ProviderInfo[numProviders];
		int piCtr = 0;
		while (e != null && e.hasMoreElements())
		{
			Provider prov = (Provider) e.nextElement();

			if (prov.isPersistent())
			{
				retval[piCtr++] = new BasicProviderInfo(
									prov.getObjectID(),
									prov.getDependableFinder(),
									prov.getObjectName()
									);
			}
		}

		return retval;
	}

	/**
	 * @see DependencyManager#clearColumnInfoInProviders
	 *
	 * @param pl		provider list
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void clearColumnInfoInProviders(ProviderList pl)
					throws StandardException
	{
		Enumeration e = pl.elements();
		while (e.hasMoreElements())
		{
			Provider pro = (Provider) e.nextElement();
			if (pro instanceof TableDescriptor)
				((TableDescriptor) pro).setReferencedColumnMap(null);
		}
	}

	/**
 	 * Copy dependencies from one dependent to another.
	 *
	 * @param copy_From the dependent to copy from	
	 * @param copyTo the dependent to copy to
	 * @param persistentOnly only copy persistent dependencies
	 * @param cm		Current ContextManager
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public void copyDependencies(Dependent	copy_From, 
								Dependent	copyTo,
								boolean		persistentOnly,
								ContextManager cm) throws StandardException
	{
		copyDependencies(copy_From, copyTo, persistentOnly, cm, null);
	}
	
	/**
	 * @inheritDoc 
	 */
    public void copyDependencies(
									Dependent	copy_From, 
									Dependent	copyTo,
									boolean		persistentOnly,
									ContextManager cm,
									TransactionController tc)
		throws StandardException
	{

		List list = getProviders(copy_From);
        Iterator depsIter = list.iterator();
        while (depsIter.hasNext()) {
            Provider provider = (Provider)depsIter.next();
			if (!persistentOnly || provider.isPersistent())
			{
				this.addDependency(copyTo, provider, cm, tc);
			}
		}
	}

					
	/**
	 * Returns a string representation of the SQL action, hence no
	 * need to internationalize, which is causing the invokation
	 * of the Dependency Manager.
	 *
	 * @param action		The action
	 *
	 * @return String	The String representation
	 */
	public String getActionString(int action)
	{
		switch (action)
		{
			case ALTER_TABLE:
				return "ALTER TABLE";

			case RENAME: //for rename table and column
				return "RENAME";

			case RENAME_INDEX:
				return "RENAME INDEX";

			case COMPILE_FAILED:
				return "COMPILE FAILED";

			case DROP_TABLE:
				return "DROP TABLE";

			case DROP_INDEX:
				return "DROP INDEX";

			case DROP_VIEW:
				return "DROP VIEW";

			case CREATE_INDEX:
				return "CREATE INDEX";

			case ROLLBACK:
				return "ROLLBACK";

			case CHANGED_CURSOR:
				return "CHANGED CURSOR";

			case CREATE_CONSTRAINT:
				return "CREATE CONSTRAINT";

			case DROP_CONSTRAINT:
				return "DROP CONSTRAINT";

			case DROP_METHOD_ALIAS:
				return "DROP ROUTINE";

			case PREPARED_STATEMENT_RELEASE:
				return "PREPARED STATEMENT RELEASE";

			case DROP_SPS:
				return "DROP STORED PREPARED STATEMENT";

			case USER_RECOMPILE_REQUEST:
				return "USER REQUESTED INVALIDATION";

			case BULK_INSERT:
				return "BULK INSERT";

		    case CREATE_VIEW:
				return "CREATE_VIEW";
 
			case DROP_JAR:
				return "DROP_JAR";

			case REPLACE_JAR:
				return "REPLACE_JAR";

			case SET_CONSTRAINTS_ENABLE:
				return "SET_CONSTRAINTS_ENABLE";

			case SET_CONSTRAINTS_DISABLE:
				return "SET_CONSTRAINTS_DISABLE";

			case INTERNAL_RECOMPILE_REQUEST:
				return "INTERNAL RECOMPILE REQUEST";

			case CREATE_TRIGGER:
				return "CREATE TRIGGER";

			case DROP_TRIGGER:
				return "DROP TRIGGER";

			case SET_TRIGGERS_ENABLE:
				return "SET TRIGGERS ENABLED";

			case SET_TRIGGERS_DISABLE:
				return "SET TRIGGERS DISABLED";

			case MODIFY_COLUMN_DEFAULT:
				return "MODIFY COLUMN DEFAULT";

			case COMPRESS_TABLE:
				return "COMPRESS TABLE";

			case DROP_COLUMN:
				return "DROP COLUMN";

			case DROP_COLUMN_RESTRICT:
				return "DROP COLUMN RESTRICT";

		    case DROP_STATISTICS:
				return "DROP STATISTICS";

			case UPDATE_STATISTICS:
				return "UPDATE STATISTICS";

		    case TRUNCATE_TABLE:
			    return "TRUNCATE TABLE";

		    case DROP_SYNONYM:
			    return "DROP SYNONYM";

		    case REVOKE_PRIVILEGE:
			    return "REVOKE PRIVILEGE";

		    case REVOKE_PRIVILEGE_RESTRICT:
			    return "REVOKE PRIVILEGE RESTRICT";

		    case REVOKE_ROLE:
				return "REVOKE ROLE";

		    case RECHECK_PRIVILEGES:
				return "RECHECK PRIVILEGES";

            case DROP_SEQUENCE:
				return "DROP SEQUENCE";

            case DROP_UDT:
				return "DROP TYPE";

            case DROP_AGGREGATE:
				return "DROP DERBY AGGREGATE";

            default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("getActionString() passed an invalid value (" + action + ")");
				}
				// NOTE: This is not internationalized because we should never
				// reach here.
				return "UNKNOWN";
		}
	}

	/**
	 * Count the number of active dependencies, both stored and in memory,
	 * in the system.
	 *
	 * @return int		The number of active dependencies in the system.

		@exception StandardException thrown if something goes wrong
	 */
	public int countDependencies()
		throws StandardException
	{
        // Add the stored dependencies.
        List<?> storedDeps = dd.getAllDependencyDescriptorsList();
        int numDependencies = storedDeps.size();
        synchronized(this) {
            Iterator<List<Dependency>> deps = dependents.values().iterator();
            Iterator<List<Dependency>> provs = providers.values().iterator();

            // Count the in memory dependencies.
            while (deps.hasNext()) {
                numDependencies += deps.next().size();
            }

            while (provs.hasNext()) {
                numDependencies += provs.next().size();
            }
        }
        return numDependencies;
	}

	//
	// class interface
	//
	public BasicDependencyManager(DataDictionary dd) {
        this.dd = dd;
	}

	//
	// class implementation
	//

	/**
	 * Add a new dependency to the specified table if it does not
	 * already exist in that table.
	 *
	 * @return boolean		Whether or not the dependency get added.
	 */
    private boolean addDependencyToTable(
            Map<UUID, List<Dependency>> table, UUID key, Dependency dy) {

        List<Dependency> deps = table.get(key);

		if (deps == null) {
            deps = new ArrayList<Dependency>();
			deps.add(dy);
			table.put(key, deps);
		}
		else {
			/* Make sure that we're not adding a duplicate dependency */
			UUID	provKey = dy.getProvider().getObjectID();
			UUID	depKey = dy.getDependent().getObjectID();

			for (ListIterator<Dependency> depsIT = deps.listIterator();  depsIT.hasNext(); )
			{
				Dependency curDY = depsIT.next();
				if (curDY.getProvider().getObjectID().equals(provKey) &&
					curDY.getDependent().getObjectID().equals(depKey))
				{
					return false;
				}
			}

			deps.add(dy);
		}

		if (SanityManager.DEBUG) {

			if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

				if (table.size() > 100)
					System.out.println("memoryLeakTrace:BasicDependencyManager:table " + table.size());
				if (deps.size() > 50)
					System.out.println("memoryLeakTrace:BasicDependencyManager:deps " + deps.size());
			}
		}

		return true;
	}

	/**
	 * removes a dependency for a given provider. assumes
	 * that the dependent removal is being dealt with elsewhere.
	 * Won't assume that the dependent only appears once in the list.
	 */
    //@GuardedBy("this")
    private void clearProviderDependency(UUID p, Dependency d) {
        List<?> deps = providers.get(p);

		if (deps == null)
			return;

		deps.remove(d);

        if (deps.isEmpty()) {
			providers.remove(p);
        }
	}

	/**
	 * Turn a list of DependencyDescriptors into a list of Dependencies.
	 *
	 * @param storedList	The List of DependencyDescriptors representing
	 *						stored dependencies.
	 * @param providerForList The provider if this list is being created
	 *                        for a list of dependents. Null otherwise.
	 * 
	 * @return List		The converted List
	 *
	 * @exception StandardException thrown if something goes wrong
	 */
    private List<Dependency> getDependencyDescriptorList(
            List<DependencyDescriptor> storedList,
			Provider providerForList)
		throws StandardException
	{
        List<Dependency>    retval = new ArrayList<Dependency>();
        
        if (!storedList.isEmpty())
		{
			/* For each DependencyDescriptor, we need to instantiate
			 * object descriptors of the appropriate type for both
			 * the dependent and provider, create a Dependency with
			 * that Dependent and Provider and substitute the Dependency
			 * back into the same place in the List
			 * so that the call gets an enumerations of Dependencys.
			 */
            for (DependencyDescriptor depDesc : storedList)
			{
				Dependent 			tempD;
				Provider  			tempP;
                DependableFinder    finder = depDesc.getDependentFinder();
                tempD = (Dependent) finder.getDependable(dd, depDesc.getUUID());

                if (providerForList != null)
                {
                    // Use the provider being passed in.
                    tempP = providerForList;

                    // Sanity check the object identifiers match.
                    if (SanityManager.DEBUG) {
                        if (!tempP.getObjectID().equals(depDesc.getProviderID()))
                        {
                            SanityManager.THROWASSERT("mismatch providers");
                        }
                    }
                }
                else
                {
                    finder = depDesc.getProviderFinder();
                    tempP = (Provider) finder.getDependable(dd, depDesc.getProviderID() );

                }

				retval.add( new BasicDependency( tempD, tempP ) );
			}
		}

		return retval;
	}

	/**
	 * Returns the LanguageConnectionContext to use.
	 *
	 * @param cm	Current ContextManager
	 *
	 * @return LanguageConnectionContext	The LanguageConnectionContext to use.
	 */
	private LanguageConnectionContext getLanguageConnectionContext(ContextManager cm)
	{
		// find the language context.
		return (LanguageConnectionContext) cm.getContext(LanguageConnectionContext.CONTEXT_ID);
	}

	/**
     * Returns a list of all providers that this dependent has (even invalid
     * ones). Includes all dependency types.
     *
     * @param d the dependent
     * @return A list of providers (possibly empty).
     * @throws StandardException thrown if something goes wrong
     */
    private List<Provider> getProviders (Dependent d) throws StandardException {
        List<Provider> provs = new ArrayList<Provider>();
        synchronized (this) {
            List deps = (List) dependents.get(d.getObjectID());
            if (deps != null) {
                Iterator depsIter = deps.iterator();
                while (depsIter.hasNext()) {
                    provs.add(((Dependency)depsIter.next()).getProvider());
                }
            }
        }

        // If the dependent is persistent, we have to take stored dependencies
        // into consideration as well.
        if (d.isPersistent()) {
			List<Dependency> storedList = getDependencyDescriptorList
                (
                 dd.getDependentsDescriptorList( d.getObjectID().toString() ),
                 (Provider) null
                 );
            Iterator<Dependency> depIter = storedList.iterator();
            while (depIter.hasNext()) {
                provs.add((depIter.next()).getProvider());
            }
		}
        return provs;
	}

    /**
     * Returns an enumeration of all dependencies that this
     * provider is supporting for any dependent at all (even
     * invalid ones). Includes all dependency types.
     *
     * @param p the provider
     * @return A list of dependents (possibly empty).
     * @throws StandardException if something goes wrong
	 */
	private List<Dependency> getDependents (Provider p) 
			throws StandardException {
        List<Dependency> deps = new ArrayList<Dependency>();
        synchronized (this) {
            List<Dependency> memDeps = providers.get(p.getObjectID());
            if (memDeps != null) {
                deps.addAll(memDeps);
            }
        }

        // If the provider is persistent, then we have to add providers for
        // stored dependencies as well.
        if (p.isPersistent()) {
			List<Dependency> storedList = getDependencyDescriptorList
                (
                 dd.getProvidersDescriptorList( p.getObjectID().toString() ),
                 p
                 );
            deps.addAll(storedList);
        }
        return deps;
	}
}
