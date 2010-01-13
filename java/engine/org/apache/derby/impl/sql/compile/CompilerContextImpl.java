
/*

   Derby - Class org.apache.derby.impl.sql.compile.CompilerContextImpl

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;

import org.apache.derby.iapi.sql.depend.ProviderList;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.sql.compile.Parser;

import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.PermDescriptor;
import org.apache.derby.iapi.sql.dictionary.PrivilegedSQLObject;
import org.apache.derby.iapi.sql.dictionary.StatementGenericPermission;
import org.apache.derby.iapi.sql.dictionary.StatementTablePermission;
import org.apache.derby.iapi.sql.dictionary.StatementSchemaPermission;
import org.apache.derby.iapi.sql.dictionary.StatementColumnPermission;
import org.apache.derby.iapi.sql.dictionary.StatementRoutinePermission;
import org.apache.derby.iapi.sql.dictionary.StatementRolePermission;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.compile.TypeCompilerFactory;

import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.sql.ParameterValueSet;

import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.SortCostController;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.compiler.JavaFactory;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.util.ReuseFactory;

import java.sql.SQLWarning;
import java.util.Vector;
import java.util.Properties;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.BitSet;
import java.util.List;
import java.util.Stack;
import java.util.ArrayList;

/**
 *
 * CompilerContextImpl, implementation of CompilerContext.
 * CompilerContext and hence CompilerContextImpl objects are private to a LanguageConnectionContext.
 *
 */
public class CompilerContextImpl extends ContextImpl
	implements CompilerContext {

	//
	// Context interface       
	//

	/**
		@exception StandardException thrown by makeInvalid() call
	 */
	public void cleanupOnError(Throwable error) throws StandardException {

		setInUse(false);
		resetContext();

		if (error instanceof StandardException) {

			StandardException se = (StandardException) error;
			// if something went wrong with the compile,
			// we need to mark the statement invalid.
			// REVISIT: do we want instead to remove it,
			// so the cache doesn't get full of garbage input
			// that won't even parse?
            
            int severity = se.getSeverity();

			if (severity < ExceptionSeverity.SYSTEM_SEVERITY) 
			{
				if (currentDependent != null)
				{
					currentDependent.makeInvalid(DependencyManager.COMPILE_FAILED,
												 lcc);
				}
				closeStoreCostControllers();
				closeSortCostControllers();
			}
			// anything system or worse, or non-DB errors,
			// will cause the whole system to shut down.
            
            if (severity >= ExceptionSeverity.SESSION_SEVERITY)
                popMe();
		}

	}

	/**
	  *	Reset compiler context (as for instance, when we recycle a context for
	  *	use by another compilation.
	  */
	public	void	resetContext()
	{
		nextColumnNumber = 1;
		nextTableNumber = 0;
		nextSubqueryNumber = 0;
		resetNextResultSetNumber();
		nextEquivalenceClass = -1;
		compilationSchema = null;
		parameterList = null;
		parameterDescriptors = null;
		scanIsolationLevel = ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL;
		warnings = null;
		savedObjects = null;
		reliability = CompilerContext.SQL_LEGAL;
		returnParameterFlag = false;
		initRequiredPriv();
		defaultSchemaStack = null;
	}

	//
	// CompilerContext interface
	//
	// we might want these to refuse to return
	// anything if they are in-use -- would require
	// the interface provide a 'done' call, and
	// we would mark them in-use whenever a get happened.
	public Parser getParser() {
		return parser;
	}

	/**
	  *	Get the NodeFactory for this context
	  *
	  *	@return	The NodeFactory for this context.
	  */
	public	NodeFactory	getNodeFactory()
	{	return lcf.getNodeFactory(); }


	public int getNextColumnNumber()
	{
		return nextColumnNumber++;
	}

	public int getNextTableNumber()
	{
		return nextTableNumber++;
	}

	public int getNumTables()
	{
		return nextTableNumber;
	}

	/**
	 * Get the current next subquery number from this CompilerContext.
	 *
	 * @return int	The next subquery number for the current statement.
	 *
	 */

	public int getNextSubqueryNumber()
	{
		return nextSubqueryNumber++;
	}

	/**
	 * Get the number of subquerys in the current statement from this CompilerContext.
	 *
	 * @return int	The number of subquerys in the current statement.
	 *
	 */

	public int getNumSubquerys()
	{
		return nextSubqueryNumber;
	}

	public int getNextResultSetNumber()
	{
		return nextResultSetNumber++;
	}

	public void resetNextResultSetNumber()
	{
		nextResultSetNumber = 0;
	}

	public int getNumResultSets()
	{
		return nextResultSetNumber;
	}

	public String getUniqueClassName()
	{
		// REMIND: should get a new UUID if we roll over...
		if (SanityManager.DEBUG)
		{
    		SanityManager.ASSERT(nextClassName <= Long.MAX_VALUE);
    	}
		return classPrefix.concat(Long.toHexString(nextClassName++));
	}

	/**
	 * Get the next equivalence class for equijoin clauses.
	 *
	 * @return The next equivalence class for equijoin clauses.
	 */
	public int getNextEquivalenceClass()
	{
		return ++nextEquivalenceClass;
	}

	public ClassFactory getClassFactory()
	{
		return lcf.getClassFactory();
	}

	public JavaFactory getJavaFactory()
	{
		return lcf.getJavaFactory();
	}

	public void setCurrentDependent(Dependent d) {
		currentDependent = d;
	}

	/**
	 * Get the current auxiliary provider list from this CompilerContext.
	 *
	 * @return	The current AuxiliaryProviderList.
	 *
	 */

	public ProviderList getCurrentAuxiliaryProviderList()
	{
		return currentAPL;
	}

	/**
	 * Set the current auxiliary provider list for this CompilerContext.
	 *
	 * @param apl	The new current AuxiliaryProviderList.
	 */

	public void setCurrentAuxiliaryProviderList(ProviderList apl)
	{
		currentAPL = apl;
	}

	public void createDependency(Provider p) throws StandardException {
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(currentDependent != null,
				"no current dependent for compilation");

		if (dm == null)
			dm = lcc.getDataDictionary().getDependencyManager();
		dm.addDependency(currentDependent, p, getContextManager());
		addProviderToAuxiliaryList(p);
	}

	/**
	 * Add a dependency between two objects.
	 *
	 * @param d	The Dependent object.
	 * @param p	The Provider of the dependency.
	 * @exception StandardException thrown on failure.
	 *
	 */
	public	void createDependency(Dependent d, Provider p) throws StandardException
	{
		if (dm == null)
			dm = lcc.getDataDictionary().getDependencyManager();

		dm.addDependency(d, p, getContextManager());
		addProviderToAuxiliaryList(p);
	}

	/**
	 * Add a Provider to the current AuxiliaryProviderList, if one exists.
	 *
	 * @param p		The Provider to add.
	 */
	private void addProviderToAuxiliaryList(Provider p)
	{
		if (currentAPL != null)
		{
			currentAPL.addProvider(p);
		}
	}

	public int addSavedObject(Object obj) {
		if (savedObjects == null) savedObjects = new Vector();

		savedObjects.addElement(obj);
		return savedObjects.size()-1;
	}

	public Object[] getSavedObjects() {
		if (savedObjects == null) return null;

		Object[] retVal = new Object[savedObjects.size()];
		savedObjects.copyInto(retVal);
		savedObjects = null; // erase to start over
		return retVal;
	}

	/** @see CompilerContext#setSavedObjects */
	public void setSavedObjects(Object[] objs) 
	{
		if (objs == null)
		{
			return;
		}

		for (int i = 0; i < objs.length; i++)
		{
			addSavedObject(objs[i]);
		}		
	}

	/** @see CompilerContext#setCursorInfo */
	public void setCursorInfo(Object cursorInfo)
	{
		this.cursorInfo = cursorInfo;
	}

	/** @see CompilerContext#getCursorInfo */
	public Object getCursorInfo()
	{
		return cursorInfo;
	}

	
	/** @see CompilerContext#firstOnStack */
	public void firstOnStack()
	{
		firstOnStack = true;
	}

	/** @see CompilerContext#isFirstOnStack */
	public boolean isFirstOnStack()
	{
		return firstOnStack;
	}

	/**
	 * Set the in use state for the compiler context.
	 *
	 * @param inUse	 The new inUse state for the compiler context.
	 */
	public void setInUse(boolean inUse)
	{
		this.inUse = inUse;

		/*
		** Close the StoreCostControllers associated with this CompilerContext
		** when the context is no longer in use.
		*/
		if ( ! inUse)
		{
			closeStoreCostControllers();
			closeSortCostControllers();
		}
	}

	/**
	 * Return the in use state for the compiler context.
	 *
	 * @return boolean	The in use state for the compiler context.
	 */
	public boolean getInUse()
	{
		return inUse;
	}

	/**
	 * Sets which kind of query fragments are NOT allowed. Basically,
	 * these are fragments which return unstable results. CHECK CONSTRAINTS
	 * and CREATE PUBLICATION want to forbid certain kinds of fragments.
	 *
	 * @param reliability	bitmask of types of query fragments to be forbidden
	 *						see the reliability bitmasks in CompilerContext.java
	 *
	 */
	public void	setReliability(int reliability) { this.reliability = reliability; }

	/**
	 * Return the reliability requirements of this clause. See setReliability()
	 * for a definition of clause reliability.
	 *
	 * @return a bitmask of which types of query fragments are to be forbidden
	 */
	public int getReliability() { return reliability; }

	/**
	 * @see CompilerContext#getStoreCostController
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StoreCostController getStoreCostController(long conglomerateNumber)
			throws StandardException
	{
		/*
		** Try to find the given conglomerate number in the array of
		** conglom ids.
		*/
		for (int i = 0; i < storeCostConglomIds.size(); i++)
		{
			Long conglomId = (Long) storeCostConglomIds.elementAt(i);
			if (conglomId.longValue() == conglomerateNumber)
				return (StoreCostController) storeCostControllers.elementAt(i);
		}

		/*
		** Not found, so get a StoreCostController from the store.
		*/
		StoreCostController retval =
						lcc.getTransactionCompile().openStoreCost(conglomerateNumber);

		/* Put it in the array */
		storeCostControllers.insertElementAt(retval,
											storeCostControllers.size());

		/* Put the conglomerate number in its array */
		storeCostConglomIds.insertElementAt(
								new Long(conglomerateNumber),
								storeCostConglomIds.size());

		return retval;
	}

	/**
	 *
	 */
	private void closeStoreCostControllers()
	{
		for (int i = 0; i < storeCostControllers.size(); i++)
		{
			StoreCostController scc =
				(StoreCostController) storeCostControllers.elementAt(i);
			try {
				scc.close();
			} catch (StandardException se) {
			}
		}

		storeCostControllers.removeAllElements();
		storeCostConglomIds.removeAllElements();
	}

	/**
	 * @see CompilerContext#getSortCostController
	 *
	 * @exception StandardException		Thrown on error
	 */
	public SortCostController getSortCostController() throws StandardException
	{
		/*
		** Re-use a single SortCostController for each compilation
		*/
		if (sortCostController == null)
		{
			/*
			** Get a StoreCostController from the store.
			*/

			sortCostController =
				lcc.getTransactionCompile().openSortCostController((Properties) null);
		}

		return sortCostController;
	}

	/**
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void closeSortCostControllers()
	{
		if (sortCostController != null)
		{
			sortCostController.close();
			sortCostController = null;	
		}
	}

	/**
	 * Get the compilation schema descriptor for this compilation context.
	   Will be null if no default schema lookups have occured. Ie.
	   the statement is independent of the current schema.
	 * 
	 * @return the compilation schema descirptor
	 */
	public SchemaDescriptor getCompilationSchema()
	{
		return compilationSchema;
	}

	/**
	 * Set the compilation schema descriptor for this compilation context.
	 *
	 * @param newDefault	the compilation schema
	 * 
	 * @return the previous compilation schema descirptor
	 */
	public SchemaDescriptor setCompilationSchema(SchemaDescriptor newDefault)
	{
		SchemaDescriptor tmpSchema = compilationSchema;
		compilationSchema = newDefault;
		return tmpSchema;
	}

	/**
	 * @see CompilerContext#pushCompilationSchema
	 */
	public void pushCompilationSchema(SchemaDescriptor sd)
	{
		if (defaultSchemaStack == null) {
			defaultSchemaStack = new ArrayList(2);
		}

		defaultSchemaStack.add(defaultSchemaStack.size(),
							   getCompilationSchema());
		setCompilationSchema(sd);
	}

	/**
	 * @see CompilerContext#popCompilationSchema
	 */
	public void popCompilationSchema()
	{
		SchemaDescriptor sd =
			(SchemaDescriptor)defaultSchemaStack.remove(
				defaultSchemaStack.size() - 1);
		setCompilationSchema(sd);
	}

	/**
	 * @see CompilerContext#setParameterList
	 */
	public void setParameterList(Vector parameterList)
	{
		this.parameterList = parameterList;

		/* Don't create param descriptors array if there are no params */
		int numberOfParameters = (parameterList == null) ? 0 : parameterList.size();

		if (numberOfParameters > 0)
		{
			parameterDescriptors = new DataTypeDescriptor[numberOfParameters];
		}
	}

	/**
	 * @see CompilerContext#getParameterList
	 */
	public Vector getParameterList()
	{
		return parameterList;
	}

	/**
	 * @see CompilerContext#setReturnParameterFlag
	 */
	public void setReturnParameterFlag()
	{
		returnParameterFlag = true;
	}

	/**
	 * @see CompilerContext#getReturnParameterFlag
	 */
	public boolean getReturnParameterFlag()
	{
		return returnParameterFlag;
	}

	/**
	 * @see CompilerContext#getParameterTypes
	 */
	public DataTypeDescriptor[] getParameterTypes()
	{
		return parameterDescriptors;
	}

	/**
	 * @see CompilerContext#setScanIsolationLevel
	 */
	public void setScanIsolationLevel(int isolationLevel)
	{
		scanIsolationLevel = isolationLevel;
	}

	/**
	 * @see CompilerContext#getScanIsolationLevel
	 */
	public int getScanIsolationLevel()
	{
		return scanIsolationLevel;
	}

	/**
	 * @see CompilerContext#getTypeCompilerFactory
	 */
	public TypeCompilerFactory getTypeCompilerFactory()
	{
		return typeCompilerFactory;
	}


	/**
		Add a compile time warning.
	*/
	public void addWarning(SQLWarning warning) {
		if (warnings == null)
			warnings = warning;
		else
			warnings.setNextWarning(warning);
	}

	/**
		Get the chain of compile time warnings.
	*/
	public SQLWarning getWarnings() {
		return warnings;
	}

	/////////////////////////////////////////////////////////////////////////////////////
	//
	// class interface
	//
	// this constructor is called with the parser
	// to be saved when the context
	// is created (when the first statement comes in, likely).
	//
	/////////////////////////////////////////////////////////////////////////////////////

	public CompilerContextImpl(ContextManager cm,
			LanguageConnectionContext lcc,
		TypeCompilerFactory typeCompilerFactory )
	{
		super(cm, CompilerContext.CONTEXT_ID);

		this.lcc = lcc;
		lcf = lcc.getLanguageConnectionFactory();
		this.parser = lcf.newParser(this);
		this.typeCompilerFactory = typeCompilerFactory;

		// the prefix for classes in this connection
		classPrefix = "ac"+lcf.getUUIDFactory().createUUID().toString().replace('-','x');

		initRequiredPriv();
	}

	private void initRequiredPriv()
	{
		currPrivType = Authorizer.NULL_PRIV;
		privTypeStack.clear();
		requiredColumnPrivileges = null;
		requiredTablePrivileges = null;
		requiredSchemaPrivileges = null;
		requiredRoutinePrivileges = null;
		requiredUsagePrivileges = null;
		requiredRolePrivileges = null;
		LanguageConnectionContext lcc = (LanguageConnectionContext)
		getContextManager().getContext(LanguageConnectionContext.CONTEXT_ID);
		if( lcc.usesSqlAuthorization())
		{
			requiredColumnPrivileges = new HashMap();
			requiredTablePrivileges = new HashMap();
			requiredSchemaPrivileges = new HashMap();
			requiredRoutinePrivileges = new HashMap();
			requiredUsagePrivileges = new HashMap();
			requiredRolePrivileges = new HashMap();
		}
	} // end of initRequiredPriv

	/**
	 * Sets the current privilege type context. Column and table nodes do not know
	 * how they are being used. Higher level nodes in the query tree do not know what
	 * is being referenced.
	 * Keeping the context allows the two to come together.
	 *
	 * @param privType One of the privilege types in org.apache.derby.iapi.sql.conn.Authorizer.
	 */
	public void pushCurrentPrivType( int privType)
	{
		privTypeStack.push( ReuseFactory.getInteger( currPrivType));
		currPrivType = privType;
	}

	public void popCurrentPrivType( )
	{
		currPrivType = ((Integer) privTypeStack.pop()).intValue();
	}

	/**
	 * Add a column privilege to the list of used column privileges.
	 *
	 * @param column The column whose privileges we're interested in.
	 */
	public void addRequiredColumnPriv( ColumnDescriptor column)
	{
		if( requiredColumnPrivileges == null // Using old style authorization
			|| currPrivType == Authorizer.NULL_PRIV
			|| currPrivType == Authorizer.DELETE_PRIV // Table privilege only
			|| currPrivType == Authorizer.INSERT_PRIV // Table privilege only
			|| currPrivType == Authorizer.TRIGGER_PRIV // Table privilege only
			|| currPrivType == Authorizer.EXECUTE_PRIV
			|| column == null)
			return;
		/*
		* Note that to look up the privileges for this column,
		* we need to know what table the column is in. However,
		* not all ColumnDescriptor objects are associated with
		* a table object. Sometimes a ColumnDescriptor
		* describes a column but doesn't specify the table. An
		* example of this occurs in the set-clause of the
		* UPDATE statement in SQL, where we may have a
		* ColumnDescriptor which describes the expression that
		* is being used in the UPDATE statement to provide the
		* new value that will be computed by the UPDATE. In such a
		* case, there is no column privilege to be added, so we
		* just take an early return. DERBY-1583 has more details.
		*/
		TableDescriptor td = column.getTableDescriptor();
		if (td == null)
			return;

		if (td.getTableType() ==
				TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE) {
			return; // no priv needed, it is per session anyway
		}

		UUID tableUUID = td.getUUID();

		//DERBY-4191
		if( currPrivType == Authorizer.MIN_SELECT_PRIV){
			//If we are here for MIN_SELECT_PRIV requirement, then first
			//check if there is already a SELECT privilege requirement on any 
			//of the columns in the table. If yes, then we do not need to add 
			//MIN_SELECT_PRIV requirement for the table because that 
			//requirement is already getting satisfied with the already
			//existing SELECT privilege requirement
			StatementTablePermission key = new StatementTablePermission( 
					tableUUID, Authorizer.SELECT_PRIV);
			StatementColumnPermission tableColumnPrivileges
			  = (StatementColumnPermission) requiredColumnPrivileges.get( key);
			if( tableColumnPrivileges != null)
				return;
		}
		if( currPrivType == Authorizer.SELECT_PRIV){
			//If we are here for SELECT_PRIV requirement, then first check
			//if there is already any MIN_SELECT_PRIV privilege required
			//on this table. If yes, then that requirement will be fulfilled
			//by the SELECT_PRIV requirement we are adding now. Because of
			//that, remove the MIN_SELECT_PRIV privilege requirement
			StatementTablePermission key = new StatementTablePermission( 
					tableUUID, Authorizer.MIN_SELECT_PRIV);
			StatementColumnPermission tableColumnPrivileges
			  = (StatementColumnPermission) requiredColumnPrivileges.get( key);
			if( tableColumnPrivileges != null)
				requiredColumnPrivileges.remove(key);
		}
		
		StatementTablePermission key = new StatementTablePermission( tableUUID, currPrivType);
		StatementColumnPermission tableColumnPrivileges
		  = (StatementColumnPermission) requiredColumnPrivileges.get( key);
		if( tableColumnPrivileges == null)
		{
			tableColumnPrivileges = new StatementColumnPermission( tableUUID,
																   currPrivType,
																   new FormatableBitSet( td.getNumberOfColumns()));
			requiredColumnPrivileges.put(key, tableColumnPrivileges);
		}
		tableColumnPrivileges.getColumns().set(column.getPosition() - 1);
	} // end of addRequiredColumnPriv

	/**
	 * Add a table or view privilege to the list of used table privileges.
	 *
	 * @see CompilerContext#addRequiredRoutinePriv
	 */
	public void addRequiredTablePriv( TableDescriptor table)
	{
		if( requiredTablePrivileges == null || table == null)
			return;

		if (table.getTableType() ==
				TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE) {
			return; // no priv needed, it is per session anyway
		}

		if( currPrivType == Authorizer.SELECT_PRIV){
			//DERBY-4191
			//Check if there is any MIN_SELECT_PRIV select privilege required
			//on this table. If yes, then that requirement will be fulfilled
			//by the SELECT_PRIV requirement we are adding now. Because of
			//that, remove the MIN_SELECT_PRIV privilege requirement
			StatementTablePermission key = new StatementTablePermission( 
					table.getUUID(), Authorizer.MIN_SELECT_PRIV);
			StatementColumnPermission tableColumnPrivileges
			  = (StatementColumnPermission) requiredColumnPrivileges.get( key);
			if( tableColumnPrivileges != null)
				requiredColumnPrivileges.remove(key);
		}

		StatementTablePermission key = new StatementTablePermission( table.getUUID(), currPrivType);
		requiredTablePrivileges.put(key, key);
	}

	/**
	 * Add a routine execute privilege to the list of used routine privileges.
	 *
	 * @see CompilerContext#addRequiredRoutinePriv
	 */
	public void addRequiredRoutinePriv( AliasDescriptor routine)
	{
		// routine == null for built in routines
		if( requiredRoutinePrivileges == null || routine == null)
			return;

		// Ignore SYSFUN routines for permission scheme
		if (routine.getSchemaUUID().toString().equals(SchemaDescriptor.SYSFUN_SCHEMA_UUID))
			return;

 		if (requiredRoutinePrivileges.get(routine.getUUID()) == null)
 			requiredRoutinePrivileges.put(routine.getUUID(), ReuseFactory.getInteger(1));
	}

	/**
	 * @see CompilerContext#addRequiredUsagePriv
	 */
	public void addRequiredUsagePriv( PrivilegedSQLObject usableObject )
    {
		if( requiredUsagePrivileges == null || usableObject == null) { return; }

        UUID objectID = usableObject.getUUID();
        String objectType = usableObject.getObjectTypeName();

 		if (requiredUsagePrivileges.get( objectID ) == null)
        { requiredUsagePrivileges.put( objectID, objectType ); }
    }
    
	/**
	 * Add a required schema privilege to the list privileges.
	 *
	 * @see CompilerContext#addRequiredSchemaPriv
	 */
	public void addRequiredSchemaPriv(String schemaName, String aid, int privType)
	{
		if( requiredSchemaPrivileges == null || schemaName == null)
			return;

		StatementSchemaPermission key = new 
				StatementSchemaPermission(schemaName, aid, privType);

		requiredSchemaPrivileges.put(key, key);
	}


	/**
	 * Add a required role privilege to the list privileges.
	 *
	 * @see CompilerContext#addRequiredRolePriv
	 */
	public void addRequiredRolePriv(String roleName, int privType)
	{
		if( requiredRolePrivileges == null)
			return;

		StatementRolePermission key = new
			StatementRolePermission(roleName, privType);

		requiredRolePrivileges.put(key, key);
	}


	/**
	 * @return The list of required privileges.
	 */
	public List getRequiredPermissionsList()
	{
		int size = 0;
		if( requiredRoutinePrivileges != null)
        { size += requiredRoutinePrivileges.size(); }
		if( requiredUsagePrivileges != null)
        { size += requiredUsagePrivileges.size(); }
		if( requiredTablePrivileges != null)
        { size += requiredTablePrivileges.size(); }
		if( requiredSchemaPrivileges != null)
        { size += requiredSchemaPrivileges.size(); }
		if( requiredColumnPrivileges != null)
        { size += requiredColumnPrivileges.size(); }
		if( requiredRolePrivileges != null)
        { size += requiredRolePrivileges.size(); }
		
		ArrayList list = new ArrayList( size);
		if( requiredRoutinePrivileges != null)
		{
			for( Iterator itr = requiredRoutinePrivileges.keySet().iterator(); itr.hasNext();)
			{
				UUID routineUUID = (UUID) itr.next();
				
				list.add( new StatementRoutinePermission( routineUUID));
			}
		}
		if( requiredUsagePrivileges != null)
		{
			for( Iterator itr = requiredUsagePrivileges.keySet().iterator(); itr.hasNext();)
			{
				UUID objectID = (UUID) itr.next();
				
				list.add( new StatementGenericPermission( objectID, (String) requiredUsagePrivileges.get( objectID ), PermDescriptor.USAGE_PRIV ) );
			}
		}
		if( requiredTablePrivileges != null)
		{
			for( Iterator itr = requiredTablePrivileges.values().iterator(); itr.hasNext();)
			{
				list.add( itr.next());
			}
		}
		if( requiredSchemaPrivileges != null)
		{
			for( Iterator itr = requiredSchemaPrivileges.values().iterator(); itr.hasNext();)
			{
				list.add( itr.next());
			}
		}
		if( requiredColumnPrivileges != null)
		{
			for( Iterator itr = requiredColumnPrivileges.values().iterator(); itr.hasNext();)
			{
				list.add( itr.next());
			}
		}
		if( requiredRolePrivileges != null)
		{
			for( Iterator itr = requiredRolePrivileges.values().iterator();
				 itr.hasNext();)
			{
				list.add( itr.next());
			}
		}
		return list;
	} // end of getRequiredPermissionsList

	/*
	** Context state must be reset in restContext()
	*/

	private final Parser 		parser;
	private final LanguageConnectionContext lcc;
	private final LanguageConnectionFactory lcf;
	private TypeCompilerFactory	typeCompilerFactory;
	private Dependent			currentDependent;
	private DependencyManager	dm;
	private boolean				firstOnStack;
	private boolean				inUse;
	private int					reliability = CompilerContext.SQL_LEGAL;
	private	int					nextColumnNumber = 1;
	private int					nextTableNumber;
	private int					nextSubqueryNumber;
	private int					nextResultSetNumber;
	private int					scanIsolationLevel;
	private int					nextEquivalenceClass = -1;
	private long				nextClassName;
	private Vector				savedObjects;
	private String				classPrefix;
	private SchemaDescriptor	compilationSchema;

	/**
	 * Saved execution time default schema, if we need to change it
	 * temporarily.
	 */
	private ArrayList        	defaultSchemaStack;

	private ProviderList		currentAPL;
	private boolean returnParameterFlag;

	private Vector				storeCostControllers = new Vector();
	private Vector				storeCostConglomIds = new Vector();

	private SortCostController	sortCostController;

	private Vector parameterList;

	/* Type descriptors for the ? parameters */
	private DataTypeDescriptor[]	parameterDescriptors;

	private Object				cursorInfo;

	private SQLWarning warnings;

	private Stack privTypeStack = new Stack();
	private int currPrivType = Authorizer.NULL_PRIV;
	private HashMap requiredColumnPrivileges;
	private HashMap requiredTablePrivileges;
	private HashMap requiredSchemaPrivileges;
	private HashMap requiredRoutinePrivileges;
	private HashMap requiredUsagePrivileges;
	private HashMap requiredRolePrivileges;
} // end of class CompilerContextImpl
