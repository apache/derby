/*

   Derby - Class org.apache.derby.impl.sql.depend.BasicDependencyManager

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

package org.apache.derby.impl.sql.depend;

import	org.apache.derby.catalog.Dependable;
import	org.apache.derby.catalog.DependableFinder;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.impl.sql.compile.CreateViewNode;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.depend.ProviderList;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DependencyDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.impl.sql.catalog.DDColumnDependableFinder;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.reference.MessageId;

import org.apache.derby.iapi.error.StandardException;

import java.util.Hashtable;
import java.util.Enumeration;
import java.util.ListIterator;
import java.util.List;

/**
	The dependency manager tracks needs that dependents have of providers.
 */

public class BasicDependencyManager implements DependencyManager {

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
	public void addDependency(Dependent d, Provider p, ContextManager cm) throws StandardException {

		synchronized(this)
		{
			Dependency dy = new BasicDependency(d, p);

			/* Dependencies are either in-memory or stored, but not both */
			if (! d.isPersistent() || ! p.isPersistent())
			{
				/* Duplicate dependencies are not added to the lists.
				 * If we find that the dependency we are trying to add in
				 * one list is a duplicate, then it should be a duplicate in the
				 * other list.
				 */
				boolean addedToDeps = false;
				boolean addedToProvs = false;
				
				addedToDeps = addDependencyToTable(dependents, d.getObjectID(), dy);
				if (addedToDeps)
				{
					addedToProvs = addDependencyToTable(providers, p.getObjectID(), dy);
				}
				else if (SanityManager.DEBUG)
				{
					addedToProvs = addDependencyToTable(providers, p.getObjectID(), dy);
				}

				/* Dependency should have been added to both or neither */
				if (SanityManager.DEBUG)
				{
                    if (addedToDeps != addedToProvs)
                    {
                        SanityManager.THROWASSERT(
                            "addedToDeps (" + addedToDeps + 
                            ") and addedToProvs (" +
                            addedToProvs + ") are expected to agree");
                    }
				}

				/* Add the dependency to the StatementContext, so that
			 	* it can be cleared on a pre-execution error.
			 	*/
				StatementContext sc = (StatementContext) cm.getContext(org.apache.derby.iapi.reference.ContextId.LANG_STATEMENT);
				sc.addDependency(dy);
			}
			else
			{
				/* Add a stored dependency */
				LanguageConnectionContext	lcc = getLanguageConnectionContext(cm);
				DataDictionary				dd = getDataDictionary();
				DependencyDescriptor		dependencyDescriptor;
			
				dependencyDescriptor = new DependencyDescriptor(d, p);

				/* We can finally call the DataDictionary to store the dependency */
				dd.addDescriptor(dependencyDescriptor, null,
								 DataDictionary.SYSDEPENDS_CATALOG_NUM, true,
								 lcc.getTransactionExecute());
			}
		}
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

		DataDictionary				dd = getDataDictionary();
		
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
	 * invalidators.  If parameter "forSync" is true, it also provides
	 * synchronization on the dependents.  Currently, this means synchronizing
	 * on the prepared statements, which might be being executed on other
	 * threads.
	 * @param p			provider
	 * @param action	The action causing the invalidate
	 * @param lcc		Language connection context
	 *
	 * @return		array of locked dependents (to be unlocked by caller later)
	 *
	 * @exception StandardException		Thrown on error.
	 */
	private void coreInvalidateFor(Provider p, int action, LanguageConnectionContext lcc)
		throws StandardException
	{
		List list = getDependents(p);
		if (list == null)
		{
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
				Dependency dependency = (Dependency) list.get(ei);

				Dependent dep = dependency.getDependent();

				if (affectedCols != null)
				{
					TableDescriptor td = (TableDescriptor) dependency.getProvider();
					FormatableBitSet providingCols = td.getReferencedColumnMap();
					if (providingCols == null)
					{
						if (dep instanceof ViewDescriptor)
						{
							ViewDescriptor vd = (ViewDescriptor) dep;
							DataDictionary dd = getDataDictionary();
							SchemaDescriptor compSchema;
							compSchema = dd.getSchemaDescriptor(vd.getCompSchemaId(), null);
							CompilerContext newCC = lcc.pushCompilerContext(compSchema);
							Parser	pa = newCC.getParser();
							LanguageConnectionFactory	lcf = lcc.getLanguageConnectionFactory();

							// Since this is always nested inside another SQL
							// statement, so topLevel flag should be false
							CreateViewNode cvn = (CreateViewNode)pa.parseStatement(
												vd.getViewText());

							// need a current dependent for bind
							newCC.setCurrentDependent(dep);
							cvn = (CreateViewNode) cvn.bind();
							ProviderInfo[] providerInfos = cvn.getProviderInfo();
							lcc.popCompilerContext(newCC);

							boolean		interferent = false;
							for (int i = 0; i < providerInfos.length; i++)
							{
								Provider provider = null;
								try
								{
									provider = (Provider) providerInfos[i].
													getDependableFinder().
													getDependable(
													providerInfos[i].getObjectId());
								}
								catch(java.sql.SQLException te)
								{
									if (SanityManager.DEBUG)
									{
										SanityManager.THROWASSERT("unexpected java.sql.SQLException - " + te);
									}
								}
								if (provider instanceof TableDescriptor)
								{
									TableDescriptor tab = (TableDescriptor)provider;
									FormatableBitSet colMap = tab.getReferencedColumnMap();
									if (colMap == null)
										continue;
									// if later on an error is raised such as in
									// case of interference, this dependency line
									// upgrade will not happen due to rollback
									tab.setReferencedColumnMap(null);
									dropDependency(lcc, vd, tab);
									tab.setReferencedColumnMap(colMap);
									addDependency(vd, tab, lcc.getContextManager());

									if (tab.getObjectID().equals(td.getObjectID()))
									{
										System.arraycopy(affectedCols.getByteArray(), 0, 
											subsetCols.getByteArray(), 0, 
											affectedCols.getLengthInBytes());
										subsetCols.and(colMap);
										if (subsetCols.anySetBit() != -1)
										{
											interferent = true;
											((TableDescriptor) p).setReferencedColumnMap(subsetCols);
										}
									}
								}	// if provider instanceof TableDescriptor
							}	// for providerInfos
							if (! interferent)
								continue;
						}	// if dep instanceof ViewDescriptor
						else
							((TableDescriptor) p).setReferencedColumnMap(null);
					}	// if providingCols == null
					else
					{
						System.arraycopy(affectedCols.getByteArray(), 0, subsetCols.getByteArray(), 0, affectedCols.getLengthInBytes());
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

					if (noInvalidate != null)
						sqle.setNestedException(noInvalidate);

					noInvalidate = sqle;
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
		@param p the provider
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void clearDependencies(LanguageConnectionContext lcc, Dependent d) throws StandardException {
		List deps = (List) dependents.get(d.getObjectID());

		synchronized(this)
		{
			/* Remove all the stored dependencies */
			if (d.isPersistent())
			{
				DataDictionary			  dd = getDataDictionary();

				dd.dropDependentsStoredDependencies(d.getObjectID(),
													lcc.getTransactionExecute());
			}

			/* Now remove the in-memory dependencies */

			if (deps == null) return; // already removed

			// go through the list notifying providers to remove
			// the dependency from their lists
			for (ListIterator depsIterator = deps.listIterator();
				depsIterator.hasNext(); ) {

				Dependency dy = (Dependency)depsIterator.next();
				clearProviderDependency(dy.getProviderKey(), dy);
			}

			dependents.remove(d.getObjectID());
		}
	}

	/**
	 * Clear the specified in memory dependency.
	 * This is useful for clean-up when an exception occurs.
	 * (We clear all in-memory dependencies added in the current
	 * StatementContext.)
	 */
	public void clearInMemoryDependency(Dependency dy)
	{
		synchronized(this)
		{
			List deps =
				(List) dependents.get(dy.getDependent().getObjectID());

			// NOTE - this is a NEGATIVE Sanity mode check, in sane mode we continue
			// to ensure the dependency manager is consistent.
			if (!SanityManager.DEBUG) {
				// dependency has already been removed
				if (deps == null)
					return;
			}

			List provs =
				(List) providers.get(dy.getProvider().getObjectID());

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

					if (depCount != provCount) {
						SanityManager.THROWASSERT("Dependency count mismatch count in deps: " + depCount +
							", count in provs " + provCount +
							", dy.getDependent().getObjectID() = " + dy.getDependent().getObjectID() +
							", dy.getProvider().getObjectID() = " + dy.getProvider().getObjectID());
					}
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
			if (deps.size() == 0)
				dependents.remove(dy.getDependent().getObjectID());
			provs.remove(dy);
			if (provs.size() == 0)
				providers.remove(dy.getProvider().getObjectID());
		}
	}


	/**
	 * @see DependencyManager#getAllProviders
	 *
	 * @exception StandardException		Thrown on error
	 */
//	public SList getAllProviders(Dependent dependent)
//							throws StandardException
//	{
//		synchronized(this)
//		{
//			SList list = getProviders(dependent);
//			return list;
//		}
//	}

	/**
	 * @see DependencyManager#getAllProviderInfos
	 *
	 * @exception StandardException		Thrown on error
	 */
/*	public ProviderInfo[] getAllProviderInfos(Dependent dependent)
							throws StandardException
	{
		synchronized(this)
		{
			ProviderInfo[]	retval;
			SList list = getProviders(dependent);
			retval = new ProviderInfo[list.size()];
			if (list == null)
			{
				return retval;
			}
			
			int piCtr = 0;

			Enumeration enum = list.elements();
			while (enum != null && enum.hasMoreElements())
			{
				Dependency dep = (Dependency) enum.nextElement();

				retval[piCtr++] = new BasicProviderInfo(
										dep.getProvider().getObjectID(),
										dep.getProvider().getDependableFinder(),
										dep.getProvider().getObjectName()
										);
			}

			return retval;
		}
	}
*/

	/**
	 * @see DependencyManager#getPersistentProviderInfos
	 *
	 * @exception StandardException		Thrown on error
	 */
	public synchronized ProviderInfo[] getPersistentProviderInfos(Dependent dependent)
							throws StandardException
	{
		List list = getProviders(dependent);
		if (list == null)
		{
			return EMPTY_PROVIDER_INFO;
		}

		java.util.ArrayList pih = new java.util.ArrayList();

		for (ListIterator depsIterator = list.listIterator();
					depsIterator.hasNext(); ) 
		{
			Dependency dep = (Dependency) depsIterator.next();

			if (dep.getProvider().isPersistent())
			{
				pih.add(new BasicProviderInfo(
									dep.getProvider().getObjectID(),
									dep.getProvider().getDependableFinder(),
									dep.getProvider().getObjectName()
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
		Enumeration enum = pl.elements();
		int			numProviders = 0;
		ProviderInfo[]	retval;

		/*
		** We make 2 passes - the first to count the number of persistent
 		** providers and the second to populate the array of ProviderInfos.
		*/
		while (enum != null && enum.hasMoreElements())
		{
			Provider prov = (Provider) enum.nextElement();

			if (prov.isPersistent())
			{
				numProviders++;
			}
		}

		enum = pl.elements();
		retval = new ProviderInfo[numProviders];
		int piCtr = 0;
		while (enum != null && enum.hasMoreElements())
		{
			Provider prov = (Provider) enum.nextElement();

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
	 * @return void
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void clearColumnInfoInProviders(ProviderList pl)
					throws StandardException
	{
		Enumeration enum = pl.elements();
		while (enum != null && enum.hasMoreElements())
		{
			Provider pro = (Provider) enum.nextElement();
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
	public synchronized void copyDependencies(
									Dependent	copy_From, 
									Dependent	copyTo,
									boolean		persistentOnly,
									ContextManager cm)
		throws StandardException
	{

		List list = getProviders(copy_From);
		if (list == null)
			return;

		for (ListIterator depsIterator = list.listIterator(); depsIterator.hasNext(); ) 
		{
			Provider provider = ((Dependency) depsIterator.next()).getProvider();
				
			if (!persistentOnly || provider.isPersistent())
			{
				this.addDependency(copyTo, provider, cm);
			}
		}
	}

					
	/**
	 * Returns a string representation of the SQL action, hence no
	 * need to internationalize, which is causing the invokation
	 * of the Dependency Manager.
	 *
	 * @param int		The action
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

			case DROP_TABLE_CASCADE:
				return "DROP TABLE CASCADE";

			case DROP_VIEW_CASCADE:
				return "DROP VIEW CASCADE";

			case DROP_COLUMN:
				return "DROP COLUMN";

			case DROP_COLUMN_CASCADE:
				return "DROP COLUMN CASCADE";

		    case DROP_STATISTICS:
				return "DROP STATISTICS";

			case UPDATE_STATISTICS:
				return "UPDATE STATISTICS";

		    case TRUNCATE_TABLE:
			    return "TRUNCATE TABLE";

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
		synchronized(this)
		{
			int numDependencies = 0;
			Enumeration deps = dependents.elements();
			Enumeration provs = providers.elements();
			List storedDeps = getDataDictionary().
									getAllDependencyDescriptorsList();

			/* Count the in memory dependencies */
			while (deps.hasMoreElements())
			{
				numDependencies += ((List) deps.nextElement()).size();
			}

			while (provs.hasMoreElements())
			{
				numDependencies += ((List) provs.nextElement()).size();
			}

			/* Add in the stored dependencies */
			numDependencies += storedDeps.size();

			return numDependencies;
		}
	}

	/**
	 * Dump out debugging info on all of the dependencies currently
	 * within the system.
	 *
	 * @return String	Debugging info on the dependencies.
	 *					(null if SanityManger.DEBUG is false)

	 * @exception StandardException thrown if something goes wrong
	 * @exception java.sql.SQLException thrown if something goes wrong
	 */
	public String dumpDependencies() throws StandardException, java.sql.SQLException
	{
		synchronized(this)
		{
			boolean foundInMemory = false;
			boolean foundStored = false;
			StringBuffer debugBuf = new StringBuffer();

			if (SanityManager.DEBUG)
			{
				Enumeration deps = dependents.keys();
				UUID[]		depKeys = new UUID[dependents.size()];

				/* Record the in memory dependencies */
				for (int i = 0; deps.hasMoreElements(); i++)
				{
					/*
					** Get all the keys and sort them, so that they will always
					** be printed in the same order (we have tests that canonize
					** the order of printing the dependencies, and since the key
					** is a UUID, the order they are returned from
					** hasMoreElements() changes from run to run).
					*/
					depKeys[i] = (UUID) deps.nextElement();
				}

				/* Do a bubble sort - there aren't likely to be many elements */
				bubbleSort(depKeys);

				/* Iterate through the sorted keys */
				for (int i = 0; i < depKeys.length; i++)
				{
					List depsSList = (List) dependents.get(depKeys[i]);

					for (ListIterator depsIterator = depsSList.listIterator();
					 	depsIterator.hasNext(); ) 
					{
						Dependency dy = (Dependency)depsIterator.next();

						if (! foundInMemory)
						{
							debugBuf.append("In Memory Dependencies:\n");
							foundInMemory = true;
						}

						debugBuf.append(dy.getDependent().toString() +
										", type " + 
										dy.getDependent().getClassType() +
										", " +
										" is dependent on " +
										dy.getProvider().getObjectName() + 
										", type " +
										dy.getProvider().getClassType() +
										"\n");
					}
				}

				/* Record the in memory dependencies */
				Enumeration provs = providers.keys();
				UUID[]		provKeys = new UUID[providers.size()];
				for (int i = 0; provs.hasMoreElements(); i++)
				{
					/*
					** Get all the keys and sort them, so that they will always
					** be printed in the same order (we have tests that canonize
					** the order of printing the dependencies, and since the key
					** is a UUID, the order they are returned from
					** hasMoreElements() changes from run to run).
					*/
					provKeys[i] = (UUID) provs.nextElement();
				}

				/* Do a bubble sort - there aren't likely to be many elements */
				bubbleSort(provKeys);

				/* Iterate through the sorted keys */
				for (int i = 0; i < provKeys.length; i++)
				{
					List depsSList = (List) providers.get(provKeys[i]);

					for (ListIterator depsIterator = depsSList.listIterator();
						depsIterator.hasNext(); ) 
					{

						Dependency dy = (Dependency)depsIterator.next();

						if (! foundInMemory)
						{
							debugBuf.append("In Memory Dependencies:\n");
							foundInMemory = true;
						}

						debugBuf.append( 
										dy.getProvider().toString() + 
										", type " +
										dy.getProvider().getClassType() +
										", provides for " +
										dy.getDependent().getObjectName() +
										", type " +
										dy.getDependent().getClassType() +
										"\n");
					}
				}
				/* Record the stored dependencies in sorted order to avoid
				   ordering problems in canons. Also the dependencyDescriptor.getUUID()
				   in this list is not unique, hence the sort on the output string values instead
				*/
				List storedDeps =
							getDataDictionary().getAllDependencyDescriptorsList();

				String[] dependStr = new String[storedDeps.size()];

				int i = 0;
				for (ListIterator depsIterator = storedDeps.listIterator();
					 depsIterator.hasNext(); )
				{
					DependencyDescriptor dd = (DependencyDescriptor)depsIterator.next();

					if (! foundStored)
					{
						debugBuf.append("Stored Dependencies:\n");
						foundStored = true;
					}

					dependStr[i++] = new String(
									dd.getProviderFinder().getSQLObjectName(
										dd.getProviderID().toString()) +
									", type " +
									dd.getProviderFinder().getSQLObjectType() +
									", provides for " +
									dd.getDependentFinder().getSQLObjectName(
										dd.getUUID().toString()) +
									", type " +
									dd.getDependentFinder().getSQLObjectType() +
									"\n");
				}

				// sort stored dependencies; dependStr
				for (i = 0; i < dependStr.length; i++)
				{
					for (int j = i + 1; j < dependStr.length; j++)
					{
						if (dependStr[i].compareTo(dependStr[j]) > 0)
						{
							String save = dependStr[i];
							dependStr[i] = dependStr[j];
							dependStr[j] = save;
						}
					}
				}

				for(i=0; i < dependStr.length; i++)
						debugBuf.append(dependStr[i]);


			}

			return debugBuf.toString();
		}
	}

	//
	// class interface
	//
	public BasicDependencyManager() {
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
	private boolean addDependencyToTable(Hashtable table, 
		Object key, Dependency dy) {

		List deps = (List) table.get(key);
		if (deps == null) {
			deps = newSList();
			deps.add(dy);
			table.put(key, deps);
		}
		else {
			/* Make sure that we're not adding a duplicate dependency */
			UUID	provKey = dy.getProvider().getObjectID();
			UUID	depKey = dy.getDependent().getObjectID();

			for (ListIterator depsIT = deps.listIterator();  depsIT.hasNext(); )
			{
				Dependency curDY = (Dependency)depsIT.next();
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
	protected void clearProviderDependency(UUID p, Dependency d) {
		List deps = (List) providers.get(p);

		if (deps == null)
			return;

		deps.remove(d);

		if (deps.size() == 0)
			providers.remove(p);
	}

	/**
	 * Replace the DependencyDescriptors in an List with Dependencys.
	 *
	 * @param storedList	The List of DependencyDescriptors representing
	 *						stored dependencies.
	 * 
	 * @return List		The converted List
	 *
	 * @exception StandardException thrown if something goes wrong
	 */
	private List getDependencyDescriptorList(List storedList)
		throws StandardException
	{
		DataDictionary		 dd = getDataDictionary();

		if (storedList.size() != 0)
		{
			/* For each DependencyDescriptor, we need to instantiate
			 * object descriptors of the appropriate type for both
			 * the dependent and provider, create a Dependency with
			 * that Dependent and Provider and substitute the Dependency
			 * back into the same place in the List
			 * so that the call gets an enumerations of Dependencys.
			 */
			for (ListIterator depsIterator = storedList.listIterator();
				 depsIterator.hasNext(); ) 
			{
				Dependent 			tempD;
				Provider  			tempP;
				DependableFinder	finder = null;

				DependencyDescriptor depDesc = (DependencyDescriptor) depsIterator.next();

				try {
					finder = depDesc.getDependentFinder();
					tempD = (Dependent) finder.getDependable( depDesc.getUUID() );

					finder = depDesc.getProviderFinder();
					tempP = (Provider) finder.getDependable( depDesc.getProviderID() );
/*					if (finder instanceof DDColumnDependableFinder)
						((TableDescriptor)tempP).setReferencedColumnMap(
							new FormatableBitSet(((DDColumnDependableFinder) finder).
										getColumnBitMap()));
*/
				} catch (java.sql.SQLException te) {
					throw StandardException.newException(SQLState.DEP_UNABLE_TO_RESTORE, finder.getClass().getName(), te.getMessage());

				}

				depsIterator.set(new BasicDependency(tempD, tempP));
			}
		}

		return storedList;
	}

	/**
	 * Returns the DataDictionary to use.
	 *
	 * @return DataDictionary	The DataDictionary to use.
	 */
	private DataDictionary getDataDictionary()
	{
		if (dataDictionary == null)
		{
			DataDictionaryContext	  ddc;
			
			ddc = (DataDictionaryContext)
						(ContextService.getContext(DataDictionaryContext.CONTEXT_ID));


			dataDictionary = ddc.getDataDictionary();
		}

		return dataDictionary;
	}

	/**
	 * Returns the LanguageConnectionContext to use.
	 *
	 * @return LanguageConnectionContext	The LanguageConnectionContext to use.
	 */
	private LanguageConnectionContext getLanguageConnectionContext()
	{
		// find the language context.
		return (LanguageConnectionContext) 
				ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
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
	 * Do a bubble sort on the given array of UUIDs.  This sorts by the
	 * String values of the UUIDs.  It's slow, but it doesn't matter
	 * because this is only for testing and debugging.  Sorting by
	 * UUID.toString() always gives the same order because, within a
	 * single boot of the system, UUIDs are distinguished only by a
	 * sequence number.
	 *
	 * @param uuids		The array of UUIDs to sort.
	 */
	private void bubbleSort(UUID[] uuids)
	{
		if (SanityManager.DEBUG)
		{
			for (int i = 0; i < uuids.length; i++)
			{
				for (int j = i + 1; j < uuids.length; j++)
				{
					if (uuids[i].toString().compareTo(uuids[j].toString()) > 0)
					{
						UUID	save = uuids[i];
						uuids[i] = uuids[j];
						uuids[j] = save;
					}
				}
			}
		}
	}

	/**
		Returns an enumeration of all dependencies that this
		dependent has with any provider (even
		invalid ones). Includes all dependency types.

		@param d the dependent

		@exception StandardException thrown if something goes wrong
	 */
	protected List getProviders (Dependent d) throws StandardException {

		List deps = (List) dependents.get(d.getObjectID());

		/* If the Dependent is not persistent, then we only have to
	 	* worry about in-memory dependencies.  Otherwise, we have to
	 	* integrate the 2.
	 	*/
		if (! d.isPersistent())
		{
			return (deps == null? null : deps);
		}
		else
		{
			if (deps == null)
			{
				deps = newSList();
			}
			else
			{
				deps = newSList(deps);
			}

			/* Now we need to add any persistent dependencies to the
		 	* list before returning
		 	*/
			List storedList = getDependencyDescriptorList(
							getDataDictionary().
								getDependentsDescriptorList(
												d.getObjectID().toString()
															)
													);

			if (storedList.size() > 0)
			{
				deps.addAll(0, storedList);
			}

			return deps;
		}
	}

	/**
		Returns an enumeration of all dependencies that this
		provider is supporting for any dependent at all (even
		invalid ones). Includes all dependency types.

		@param p the provider

		@exception StandardException thrown if something goes wrong
	 */
	protected List getDependents (Provider p) 
			throws StandardException {

		List deps = (List) providers.get(p.getObjectID());

		/* If the Provider is not persistent, then we only have to
		* worry about in-memory dependencies.  Otherwise, we have to
		* integrate the 2.
		*/
		if (! p.isPersistent())
		{
			return (deps == null? null : deps);
		}
		else
		{
			if (deps == null)
			{
				deps = newSList();
			}
			else
			{
				deps = newSList(deps);
			}

			/* Now we need to add any persistent dependencies to the
		 	* list before returning
		 	*/
			List storedList = getDependencyDescriptorList(
							getDataDictionary().
								getProvidersDescriptorList(
												p.getObjectID().toString()
															)
													);
			if (storedList.size() > 0)
			{
				deps.addAll(0, storedList);
			}

			return deps;
		}
	}

	private static List newSList() {
		return java.util.Collections.synchronizedList(new java.util.LinkedList());
	}
	private static List newSList(List list) {
		return java.util.Collections.synchronizedList(new java.util.LinkedList(list));
	}

	private	DataDictionary dataDictionary = null;
	protected Hashtable dependents = new Hashtable();
	protected Hashtable providers = new Hashtable();
}
