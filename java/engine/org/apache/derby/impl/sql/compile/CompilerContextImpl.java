/*

   Derby - Class org.apache.derby.impl.sql.compile.CompilerContextImpl

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;

import org.apache.derby.iapi.sql.depend.ProviderList;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.sql.compile.Parser;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.context.ContextImpl;

import java.sql.SQLWarning;
import java.util.Vector;
import java.util.Properties;

/*
 *
 * CompilerContextImpl
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

			if (se.getSeverity() < ExceptionSeverity.SYSTEM_SEVERITY) 
			{
				if (currentDependent != null)
				{
					LanguageConnectionContext lcc;

					/* Find the LanguageConnectionContext */
					lcc = (LanguageConnectionContext)
						getContextManager().getContext(LanguageConnectionContext.CONTEXT_ID);
					currentDependent.makeInvalid(DependencyManager.COMPILE_FAILED,
												 lcc);
				}
				closeStoreCostControllers();
				closeSortCostControllers();
			}
			// anything system or worse, or non-DB errors,
			// will cause the whole system to shut down.
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


	public Dependent getCurrentDependent() {
		return currentDependent;
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
	 * @param adl	The new current AuxiliaryProviderList.
	 *
	 * @return Nothing.
	 */

	public void setCurrentAuxiliaryProviderList(ProviderList apl)
	{
		currentAPL = apl;
	}

	public void createDependency(Provider p) throws StandardException {
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(getCurrentDependent() != null,
				"no current dependent for compilation");

		LanguageConnectionContext	lcc = (LanguageConnectionContext)
			getContextManager().getContext(LanguageConnectionContext.CONTEXT_ID);
		DependencyManager dm = lcc.getDataDictionary().getDependencyManager();
		dm.addDependency(getCurrentDependent(), p, getContextManager());
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
		LanguageConnectionContext lcc = (LanguageConnectionContext)
			getContextManager().getContext(LanguageConnectionContext.CONTEXT_ID);
		DependencyManager dm = lcc.getDataDictionary().getDependencyManager();

		dm.addDependency(d, p, getContextManager());
		addProviderToAuxiliaryList(p);
	}

	/**
	 * Add a Provider to the current AuxiliaryProviderList, if one exists.
	 *
	 * @param p		The Provider to add.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException thrown on failure.
	 */
	private void addProviderToAuxiliaryList(Provider p)
		throws StandardException
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

	/** @see CompilerContext#setParams */
	public void setParams(ParameterValueSet params)
	{
		this.params = params;
	}

	/** @see CompilerContext#getParams */
	public ParameterValueSet getParams()
	{
		ParameterValueSet tmpParams = this.params;
		this.params = null;
		return tmpParams;
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
	 *
	 * @return Nothing.
	 *
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
	public StoreCostController getStoreCostController(long conglomerateNumber,
													  LanguageConnectionContext lcc)
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
			LanguageConnectionContext lcc;

			/* Find the LanguageConnectionContext */
			lcc = (LanguageConnectionContext)
				getContextManager().getContext(LanguageConnectionContext.CONTEXT_ID);

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
	 * @param the compilation schema
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
	 * @see CompilerContext#getNextParameterNumber
	 */
	public int getNextParameterNumber()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(parameterList != null,
				"parameterList is expected to be non-null");
		}
		
		// Parameter #s are 0-based
		return parameterList.size();
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
	 * @see CompilerContext#setEntryIsolationLevel
	 */
	public void setEntryIsolationLevel(int isolationLevel)
	{
		this.entryIsolationLevel = isolationLevel;
	}

	/**
	 * @see CompilerContext#getScanIsolationLevel
	 */
	public int getEntryIsolationLevel()
	{
		return entryIsolationLevel;
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

	public CompilerContextImpl(ContextManager cm, LanguageConnectionFactory lcf,
		TypeCompilerFactory typeCompilerFactory )
	{
		super(cm, CompilerContext.CONTEXT_ID);

		this.parser = lcf.newParser(this);
		this.lcf = lcf;
		this.typeCompilerFactory = typeCompilerFactory;

		// the prefix for classes in this connection
		classPrefix = "ac"+lcf.getUUIDFactory().createUUID().toString().replace('-','x');
	}

	/*
	** Context state must be reset in restContext()
	*/

	private final Parser 		parser;
	private LanguageConnectionFactory lcf;
	private TypeCompilerFactory	typeCompilerFactory;
	private Dependent			currentDependent;
	private DependencyManager	dmgr;
	private boolean				firstOnStack;
	private boolean				inUse;
	private int					reliability = CompilerContext.SQL_LEGAL;
	private	int					nextColumnNumber = 1;
	private int					nextTableNumber;
	private int					nextSubqueryNumber;
	private int					nextResultSetNumber;
	private int					entryIsolationLevel;
	private int					scanIsolationLevel;
	private int					nextEquivalenceClass = -1;
	private long				nextClassName;
	private Vector				savedObjects;
	private String				classPrefix;
	private ParameterValueSet	params;
	private SchemaDescriptor	compilationSchema;
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
}
