/*

   Derby - Class org.apache.derby.iapi.sql.depend.DependencyManager

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

package org.apache.derby.iapi.sql.depend;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;


/**
	Dependency Manager Interface
	<p>
	The dependency manager tracks needs that dependents have of providers. This
	is a general purpose interface interface which is associated with a
	DataDictinary object; infact the dependencymanager is really the
	datadictionary keeping track of dependcies between objects that it handles
	(descriptors) as well as prepared statements.
	<p>
	The primary example of this is a prepared statement's needs of 
	schema objects such as tables.
	<p>
	Dependencies are used so that we can determine when we
	need to recompile a statement; compiled statements depend
	on schema objects like tables and constraints, and may
	no longer be executable when those tables or constraints are
	altered. For example, consider an insert statement.
	<p>
	An insert statement is likely to have dependencies on the table it
	inserts into, any tables it selects from (including
	subqueries), the authorities it uses to do this,
	and any constraints or triggers it needs to check.
	<p>
	A prepared insert statement has a dependency on the target table 
	of the insert. When it is compiled, that dependency is registered 
	from the prepared statement on the data dictionary entry for the
	table. This dependency is added to the prepared statement's dependency
	list, which is also accessible from an overall dependency pool.
	<p>
	A DDL statement will mark invalid any prepared statement that
	depends on the schema object the DDL statement is altering or
	dropping.  We tend to want to track at the table level rather than
	the column or constraint level, so that we are not overburdened
	with dependencies.  This does mean that we may invalidate when in
	fact we do not need to; for example, adding a column to a table may
	not actually cause an insert statement compiled for that table
	to stop working; but our level of granularity may force us to
	invalidate the insert because it has to invalidate all statements
	that depend on the table due to some of them actually no longer
	being valid.

	It is up to the user of the dependency system at what granularity
	to track dependencies, where to hang them, and how to identify when
	objects become invalid.  The dependency system is basically supplying
	the ability to find out who is interested in knowing about
	other, distinct operations.  The primary user is the language system,
	and its primary use is for invalidating prepared statements when
	DDL occurs.
	<p>
	The insert will recompile itself when its next execution
	is requested (not when it is invalidated). We don't want it to
	recompile when the DDL is issued, as that would increase the time
	of execution of the DDL command unacceptably.  Note that the DDL 
	command is also allowed to proceed even if it would make the 
	statement no longer compilable.  It can be useful to have a way
	to recompile invalid statements during idle time in the system,
	but our first implementation will simply recompile at the next
	execution.
	<p>
	The start of a recompile will release the connection to
	all dependencies when it releases the activation class and 
	generates a new one.
	<p>
	The Dependency Manager is capable of storing dependencies to
	ensure that other D.M.s can see them and invalidate them
	appropriately. The dependencies in memory only the current
	D.M. can see; the stored dependencies are visible to other D.M.s
	once the transaction in which they were stored is committed.
	<p>
	REVISIT: Given that statements are compiled in a separate top-transaction
	from their execution, we may need/want some intermediate memory
	storage that makes the dependencies visible to all D.M.s in the
	system, without requiring that they be stored.
	<p>
	To ensure that dependencies are cleaned up when a statement is undone,
	the compiler context needs to keep track of what dependent it was
	creating dependencies for, and if it is informed of a statement
	exception that causes it to throw out the statement it was compiling,
	it should also call the dependency manager to have the
	dependencies removed.
	<p>
	Several expansions of the basic interface may be desirable:
	<ul>
	<li> to note a type of dependency, and to invalidate or perform
	  an invalidation action based on dependency type
	<li> to note a type of invalidation, so the revalidation could 
	  actually take some action other than recompilation, such as 
	  simply ensuring the provider objects still existed.
	<li> to control the order of invalidation, so that if (for example)
	  the invalidation action actually includes the revalidation attempt,
	  revalidation is not attempted until all invalidations have occurred.
	<li> to get a list of dependencies that a Dependent or 
	  a Provider has (this is included in the above, although the
	  basic system does not need to expose the list).
	<li> to find out which of the dependencies for a dependent were marked 
	  invalid.
	</ul>
	<p>
	To provide a simple interface that satisfies the basic need,
	and yet supply more advanced functionality as well, we will present
	the simple functionality as defaults and provide ways to specify the
	more advanced functionality.

	<pre>
	interface Dependent {
		boolean isValid();
		InvalidType getInvalidType(); // returns what it sees
						// as the "most important"
						// of its invalid types.
		void makeInvalid( );
		void makeInvalid( DependencyType dt, InvalidType it );
		void makeValid();
	}

	interface Provider() {
	}

	interface Dependency() {
		Provider getProvider();
		Dependent getDependent();
		DependencyType getDependencyType();
		boolean isValid();
		InvalidType getInvalidType(); // returns what it sees
						// as the "most important"
						// of its invalid types.
	}

	interface DependencyManager() {
		void addDependency(Dependent d, Provider p, ContextManager cm);
		void invalidateFor(Provider p);
		void invalidateFor(Provider p, DependencyType dt, InvalidType it);
		void clearDependencies(Dependent d);
		void clearDependencies(Dependent d, DependencyType dt);
		Enumeration getProviders (Dependent d);
		Enumeration getProviders (Dependent d, DependencyType dt);
		Enumeration getInvalidDependencies (Dependent d, 
			DependencyType dt, InvalidType it);
		Enumeration getDependents (Provider p);
		Enumeration getDependents (Provider p, DependencyType dt);
		Enumeration getInvalidDependencies (Provider p, 
			DependencyType dt, InvalidType it);
	}
	</pre>
	<p>
	The simplest things for DependencyType and InvalidType to be are 
	integer id's or strings, rather than complex objects.
	<p>
	In terms of ensuring that no makeInvalid calls are made until we have 
	identified all objects that could be, so that the calls will be made 
	from "leaf" invalid objects (those not in turn relied on by other 
	dependents) to dependent objects upon which others depend, the 
	dependency manager will need to maintain an internal queue of 
	dependencies and make the calls once it has completes its analysis 
	of the dependencies of which it is aware.  Since it is much simpler 
	and potentially faster for makeInvalid calls to be made as soon
	as the dependents are identified, separate implementations may be
	called for, or separate interfaces to trigger the different
	styles of invalidation.
	<p>
	In terms of separate interfaces, the DependencyManager might have
	two methods,
	<pre>
		void makeInvalidImmediate();
		void makeInvalidOrdered();
	</pre>
	or a flag on the makeInvalid method to choose the style to use.
	<p>
	In terms of separate implementations, the ImmediateInvalidate
	manager might have simpler internal structures for 
	tracking dependencies than the OrderedInvalidate manager.
	<p>
	The language system doesn't tend to suffer from this ordering problem,
	as it tends to handle the impact of invalidation by simply deferring
	recompilation until the next execution.  So, a prepared statement
	might be invalidated several times by a transaction that contains
	several DDL operations, and only recompiled once, at its next 
	execution.  This is sufficient for the common use of a system, where
	DDL changes tend to be infrequent and clustered.
	<p>
	There could be ways to push this "ordering problem" out of the 
	dependency system, but since it knows when it starts and when it
	finished finding all of the invalidating actions, it is likely
	the best home for this.
	<p>
	One other problem that could arise is multiple invalidations occurring
	one after another.  The above design of the dependency system can 
	really only react to each invalidation request as a unit, not 
	to multiple invalidation requests.
	<p>
	Another extension that might be desired is for the dependency manager
	to provide for cascading invalidations -- that is, if it finds
	and marks one Dependent object as invalid, if that object can also
	be a provider, to look for its dependent objects and cascade the
	dependency on to them.  This can be a way to address the 
	multiple-invalidation request need, if it should arise.  The simplest
	way to do this is to always cascade the same invalidation type;
	otherwise, dependents need to be able to say what a certain type
	of invalidation type gets changed to when it is handed on.
	<p>
	The basic language system does not need support for cascaded 
	dependencies -- statements do not depend on other statements
	in a way that involves the dependency system.
	<p>
	I do not know if it would be worthwhile to consider using the 
	dependency manager to aid in the implementation of the SQL DROP
	statements or not. SQL DROP statements tend to have CASCADE or
	RESTRICT actions, where they either also DROP all objects that
	somehow use or depend on the object being dropped, or refuse
	to drop the object if any such objects exist.  Past implementations
	of database systems have not used the dependency system to implement
	this functionality, but have instead hard-coded the lookups like so:

	<pre>
		in DropTable:
			scan the TableAuthority table looking for authorities on
		this table; drop any that are found.
			scan the ColumnAuthority table looking for authorities on
		this table; drop any that are found.
			scan the View table looking for views on
		this table; drop any that are found.
			scan the Column table looking for rows for columns of
		this table; drop any that are found.
			scan the Constraint table looking for rows for constraints of
		this table; drop any that are found.
			scan the Index table looking for rows for indexes of
		this table; drop the indexes, and any rows that are found.
		drop the table's conglomerate
		drop the table's row in the Table table.
		</pre>
	<p>
	The direct approach such as that outlined in the example will
	probably be quicker and is definitely "known technology" over
	the use of a dependency system in this area.
 */

public interface DependencyManager {

	/* NOTE - every value in this group (actions) must have a matching
	 * String in the implementation of getActionString().
	 */
	public static final int COMPILE_FAILED = 0;
	public static final int DROP_TABLE = 1;
	public static final int DROP_INDEX = 2;
	public static final int CREATE_INDEX = 3;
	public static final int ROLLBACK = 4;
	public static final int CHANGED_CURSOR = 5;
	public static final int DROP_METHOD_ALIAS = 6;
	public static final int DROP_VIEW = 9;
	public static final int CREATE_VIEW = 10;
	public static final int PREPARED_STATEMENT_RELEASE = 11;
	public static final int ALTER_TABLE = 12;
	public static final int DROP_SPS = 13;
	public static final int USER_RECOMPILE_REQUEST = 14; 
	public static final int BULK_INSERT = 15; 
	public static final int DROP_JAR = 17;
	public static final int REPLACE_JAR = 18;
	public static final int DROP_CONSTRAINT = 19; 
	public static final int SET_CONSTRAINTS_ENABLE = 20;
	public static final int SET_CONSTRAINTS_DISABLE = 21;
	public static final int CREATE_CONSTRAINT = 22;
	public static final int INTERNAL_RECOMPILE_REQUEST = 23;
	public static final int DROP_TRIGGER = 27;
	public static final int CREATE_TRIGGER = 28;
	public static final int SET_TRIGGERS_ENABLE = 29;
	public static final int SET_TRIGGERS_DISABLE = 30;
	public static final int MODIFY_COLUMN_DEFAULT = 31;
	public static final int DROP_SCHEMA = 32;
	public static final int COMPRESS_TABLE = 33;
	//using same action for rename table/column
	public static final int RENAME = 34;
	public static final int DROP_TABLE_CASCADE = 35;
	public static final int DROP_VIEW_CASCADE = 36;
	public static final int DROP_COLUMN = 37;
	public static final int DROP_COLUMN_CASCADE = 38;
	public static final int DROP_STATISTICS = 39;
	public static final int UPDATE_STATISTICS = 40;
	//rename index dependency behavior is not as stringent as rename table and column and
	//hence we need a different action for rename index. Rename index tries to imitate the
	//drop index behavior for dependency which is not very strict.
	public static final int RENAME_INDEX = 41;

	public static final int TRUNCATE_TABLE = 42;

    /**
     * Extensions to this interface may use action codes > MAX_ACTION_CODE without fear of
     * clashing with action codes in this base interface.
     */
    public static final int MAX_ACTION_CODE = 0XFFFF;

	/**
		adds a dependency from the dependent on the provider.
		This will be considered to be the default type of
		dependency, when dependency types show up.
		<p>
		Implementations of addDependency should be fast --
		performing alot of extra actions to add a dependency would
		be a detriment.

		@param d	the dependent
		@param p	the provider
		@param cm	Current ContextManager

		@exception StandardException thrown if something goes wrong
	 */
	void addDependency(Dependent d, Provider p, ContextManager cm) throws StandardException; 

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
		Implementations should throw DependencyStatementException
		if the invalidation should be disallowed.

		@param p the provider
		@param action	The action causing the invalidate
		@param lcc		The LanguageConnectionContext

		@exception StandardException thrown if unable to make it invalid
	 */
	void invalidateFor(Provider p, int action, LanguageConnectionContext lcc) 
		throws StandardException;



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
	void clearDependencies(LanguageConnectionContext lcc, Dependent d) throws StandardException;

	/**
	 * Clear the specified in memory dependency.
	 * This is useful for clean-up when an exception occurs.
	 * (We clear all in-memory dependencies added in the current
	 * StatementContext.)
	   This method will handle Dependency's that have already been
	   removed from the DependencyManager.
	 */
	public void clearInMemoryDependency(Dependency dy);

	/**
	 * Get a new array of ProviderInfos representing all the persistent
	 * providers for the given dependent.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public ProviderInfo[] getPersistentProviderInfos(Dependent dependent)
			throws StandardException;

	/**
	 * Get a new array of ProviderInfos representing all the persistent
	 * providers from the given list of providers.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public ProviderInfo[] getPersistentProviderInfos(ProviderList pl)
			throws StandardException;

	/**
	 * Clear the in memory column bit map information in any table descriptor
	 * provider in a provider list.  This function needs to be called before
	 * the table descriptor is reused as provider in column dependency.  For
	 * example, this happens in "create publication" statement with target-only
	 * DDL where more than one views are defined and they all reference one
	 * table.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public void clearColumnInfoInProviders(ProviderList pl)
			throws StandardException;


	/**
 	 * Copy dependencies from one dependent to another.
	 *
	 * @param copy_From the dependent to copy from	
	 * @param copyTo the dependent to copy to
	 * @param persistentOnly only copy persistent dependencies
	 * @param cm			Current ContextManager
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public void copyDependencies(
									Dependent	copy_From, 
									Dependent	copyTo,
									boolean		persistentOnly,
									ContextManager cm)
			throws StandardException;
	
	/**
	 * Returns a string representation of the SQL action, hence no
	 * need to internationalize, which is causing the invokation
	 * of the Dependency Manager.
	 *
	 * @param int		The action
	 *
	 * @return String	The String representation
	 */
	String getActionString(int action);

	/**
	 * Count the number of active dependencies, both stored and in memory,
	 * in the system.
	 *
	 * @return int		The number of active dependencies in the system.

		@exception StandardException thrown if something goes wrong
	 */
	public int countDependencies() 		throws StandardException;

	/**
	 * Dump out debugging info on all of the dependencies currently
	 * within the system.
	 *
	 * @return String	Debugging info on the dependencies.
	 *					(null if SanityManger.DEBUG is false)

		@exception StandardException thrown if something goes wrong
		@exception java.sql.SQLException thrown if something goes wrong
	 */
	public String dumpDependencies() throws StandardException, java.sql.SQLException;
}
