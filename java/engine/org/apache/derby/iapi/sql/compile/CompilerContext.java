/*

   Derby - Class org.apache.derby.iapi.sql.compile.CompilerContext

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.compiler.JavaFactory;
import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.ParameterValueSet;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderList;

import org.apache.derby.iapi.sql.compile.TypeCompilerFactory;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.SortCostController;

import java.util.Vector;
import java.sql.SQLWarning;

/**
 * CompilerContext stores the parser and type id factory to be used by
 * the compiler.  Stack compiler contexts when a new, local parser is needed
 * (if calling the compiler recursively from within the compiler,
 * for example).
 *
 * @author ames
 *
 * History:
 *	5/22/97 Moved getExternalInterfaceFactory() to LanguageConnectionContext
 *			because it had to be used at execution. - Jeff
 */
public interface CompilerContext extends Context
{
	/////////////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////////////////////////////

	/**
	 * this is the ID we expect compiler contexts
	 * to be stored into a context manager under.
	 */
	String CONTEXT_ID = "CompilerContext";

	// bit masks for query fragments which are potentially unreliable. these are used
	// by setReliability() and checkReliability().

	public	static	final	int			DATETIME_ILLEGAL			=	0x00000001;	
	// NOTE: getCurrentConnection() is currently legal everywhere
	public	static	final	int			CURRENT_CONNECTION_ILLEGAL	=	0x00000002;	
	public	static	final	int			FUNCTION_CALL_ILLEGAL		=	0x00000004;	
	public	static	final	int			UNNAMED_PARAMETER_ILLEGAL	=	0x00000008;	
	public	static	final	int			DIAGNOSTICS_ILLEGAL			=	0x00000010;	
	public	static	final	int			SUBQUERY_ILLEGAL			=	0x00000020;	
	public	static	final	int			USER_ILLEGAL				=	0x00000040;	
	public	static	final	int			COLUMN_REFERENCE_ILLEGAL	=	0x00000080;
	public	static	final	int			IGNORE_MISSING_CLASSES		=	0x00000100;
	public	static	final	int			SCHEMA_ILLEGAL				=	0x00000200;
	public  static  final   int			INTERNAL_SQL_ILLEGAL		=	0x00000400;

	/** Standard SQL is legal */
	public	static	final	int			SQL_LEGAL					=	(INTERNAL_SQL_ILLEGAL);

	/** Any SQL we support is legal */
	public	static	final	int			INTERNAL_SQL_LEGAL			=	0;

	public	static	final	int			CHECK_CONSTRAINT		= (
		                                                                    DATETIME_ILLEGAL |
																		    UNNAMED_PARAMETER_ILLEGAL |
																		    DIAGNOSTICS_ILLEGAL |
																		    SUBQUERY_ILLEGAL |
																			USER_ILLEGAL |
																			SCHEMA_ILLEGAL |
																			INTERNAL_SQL_ILLEGAL
																		  );

	public	static	final	int			DEFAULT_RESTRICTION		= (
		                                                                    SUBQUERY_ILLEGAL |
																			UNNAMED_PARAMETER_ILLEGAL |
																			COLUMN_REFERENCE_ILLEGAL |
																			INTERNAL_SQL_ILLEGAL
																			);


	/////////////////////////////////////////////////////////////////////////////////////
	//
	//	BEHAVIOR
	//
	/////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Get the Parser from this CompilerContext.
	 *	 *
	 * @return	The parser associated with this CompilerContext
	 *
	 */

	Parser getParser();

	/**
	 * Get the NodeFactory from this CompilerContext.
	 *
	 * @return	The NodeFactory associated with this CompilerContext
	 *
	 */

	NodeFactory getNodeFactory();

	/**
	 * Get the TypeCompilerFactory from this CompilerContext.
	 *
	 * @return	The TypeCompilerFactory associated with this CompilerContext
	 *
	 */
	TypeCompilerFactory getTypeCompilerFactory();

	/**
		Return the class factory to use in this compilation.
	*/
	ClassFactory getClassFactory();

	/**
	 * Get the JavaFactory from this CompilerContext.
	 *
	 * @return	The JavaFactory associated with this CompilerContext
	 *
	 */

	JavaFactory getJavaFactory();

	/**
	 * Get the current next column number (for generated column names)
	 * from this CompilerContext.
	 *
	 * @return int	The next column number for the current statement.
	 *
	 */

	int getNextColumnNumber();

	/**
	  *	Reset compiler context (as for instance, when we recycle a context for
	  *	use by another compilation.
	  */
	void	resetContext();

	/**
	 * Get the current next table number from this CompilerContext.
	 *
	 * @return int	The next table number for the current statement.
	 *
	 */

	int getNextTableNumber();

	/**
	 * Get the number of tables in the current statement from this CompilerContext.
	 *
	 * @return int	The number of tables in the current statement.
	 *
	 */

	int getNumTables();

	/**
	 * Get the current next subquery number from this CompilerContext.
	 *
	 * @return int	The next subquery number for the current statement.
	 *
	 */

	int getNextSubqueryNumber();

	/**
	 * Get the number of subquerys in the current statement from this CompilerContext.
	 *
	 * @return int	The number of subquerys in the current statement.
	 *
	 */

	int getNumSubquerys();

	/**
	 * Get the current next ResultSet number from this CompilerContext.
	 *
	 * @return int	The next ResultSet number for the current statement.
	 *
	 */

	int getNextResultSetNumber();

	/**
	 * Reset the next ResultSet number from this CompilerContext.
	 *
	 * @return None.
	 *
	 */

	void resetNextResultSetNumber();

	/**
	 * Get the number of Results in the current statement from this CompilerContext.
	 *
	 * @return int	The number of ResultSets in the current statement.
	 *
	 */

	int getNumResultSets();

	/**
	 * Get a unique Class name from this CompilerContext.
	 * Ensures it is globally unique for this JVM.
	 *
	 * @return String	A unique-enough class name.
	 *
	 */

	String getUniqueClassName();

	/**
	 * Get the current dependent from this CompilerContext.
	 *
	 * @return	The Dependent currently being compiled.
	 *
	 */

	Dependent getCurrentDependent();

	/**
	 * Set the current dependent from this CompilerContext.
	 * This should be called at the start of a compile to
	 * register who has the dependencies needed for the compilation.
	 *
	 * @param d	The Dependent currently being compiled.
	 *
	 */

	void setCurrentDependent(Dependent d);

	/**
	 * Get the current auxiliary provider list from this CompilerContext.
	 *
	 * @return	The current AuxiliaryProviderList.
	 *
	 */

	ProviderList getCurrentAuxiliaryProviderList();

	/**
	 * Set the current auxiliary provider list for this CompilerContext.
	 *
	 * @param apl	The new current AuxiliaryProviderList.
	 *
	 */

	void setCurrentAuxiliaryProviderList(ProviderList apl);

	/**
	 * Add a dependency for the current dependent.
	 *
	 * @param p	The Provider of the dependency.
	 * @exception StandardException thrown on failure.
	 *
	 */
	void createDependency(Provider p) throws StandardException;

	/**
	 * Add a dependency between two objects.
	 *
	 * @param d	The Dependent object.
	 * @param p	The Provider of the dependency.
	 * @exception StandardException thrown on failure.
	 *
	 */
	public	void createDependency(Dependent d, Provider p) throws StandardException;

	/**
	 * Add an object to the pool that is created at compile time
	 * and used at execution time.  Use the integer to reference it
	 * in execution constructs.  Execution code will have to generate:
	 *	<pre>
	 *	(#objectType) (this.getPreparedStatement().getSavedObject(#int))
	 *  <\pre>
	 *
	 * @param the object to add to the pool of saved objects
	 * @return the entry # for the object
	 */
	int	addSavedObject(Object o);

	/**
	 *	Get the saved object pool (for putting into the prepared statement).
	 *  This turns it into its storable form, an array of objects.
	 *
	 * @return the saved object pool.
	 */
	Object[] getSavedObjects(); 

	/**
	 *	Set the saved object pool (for putting into the prepared statement).
	 *
	 * @param objs	 The new saved objects
	 */
	public void setSavedObjects(Object[] objs); 

	/**
	 * Set the in use state for the compiler context.
	 *
	 * @param inUse	 The new inUse state for the compiler context.
	 *
	 * @return Nothing.
	 *
	 */
	public void setInUse(boolean inUse);

	/**
	 * Return the in use state for the compiler context.
	 *
	 * @return boolean	The in use state for the compiler context.
	 */
	public boolean getInUse();

	/**
	 * Mark this CompilerContext as the first on the stack, so we can avoid
	 * continually popping and pushing a CompilerContext.
	 */
	public void firstOnStack();

	/**
	 * Is this the first CompilerContext on the stack?
	 */
	public boolean isFirstOnStack();

	/**
	 * Sets which kind of query fragments are NOT allowed. Basically,
	 * these are fragments which return unstable results. CHECK CONSTRAINTS
	 * and CREATE PUBLICATION want to forbid certain kinds of fragments.
	 *
	 * @param reliability	bitmask of types of query fragments to be forbidden
	 *						see the reliability bitmasks above
	 *
	 */
	public void	setReliability(int reliability);

	/**
	 * Return the reliability requirements of this clause. See setReliability()
	 * for a definition of clause reliability.
	 *
	 * @return a bitmask of which types of query fragments are to be forbidden
	 */
	public int getReliability();

	/**
	 * Get the compilation schema descriptor for this compilation context.
	   Will be null if no default schema lookups have occured. Ie.
	   the statement is independent of the current schema.
	 * 
	 * @return the compilation schema descirptor
	 */
	public SchemaDescriptor getCompilationSchema();

	/**
	 * Set the compilation schema descriptor for this compilation context.
	 *
	 * @param the compilation schema
	 * 
	 * @return the previous compilation schema descirptor
	 */
	public SchemaDescriptor setCompilationSchema(SchemaDescriptor newDefault);

	/**
	 * Get a StoreCostController for the given conglomerate.
	 *
	 * @param conglomerateNumber	The conglomerate for which to get a
	 *								StoreCostController.
	 * @param lcc					Current LanguageConnectionContext.
	 *
	 * @return	The appropriate StoreCostController.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StoreCostController getStoreCostController(long conglomerateNumber,
													  LanguageConnectionContext lcc)
			throws StandardException;

	/**
	 * Get a SortCostController.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public SortCostController getSortCostController() throws StandardException;

	/**
	 * Set the parameter list.
	 *
	 * @param parameterList	The parameter list.
	 *
	 * @return Nothing.
	 */
	public void setParameterList(Vector parameterList);

	/**
	 * Get the parameter list.
	 *
	 * @return	The parameter list.
	 */
	public Vector getParameterList();

	/**
	 * If callable statement uses ? = form
	 *
	 *
	 * @return Nothing.
	 */
	public void setReturnParameterFlag();

	/**
	 * Is the callable statement uses ? for return parameter.
	 *
	 * @return	true if ? = call else false
	 */
	public boolean getReturnParameterFlag();

	/**
	 * Get the array of DataTypeDescriptor representing the types of
	 * the ? parameters.
	 *
	 * @return	The parameter descriptors
	 */

	public DataTypeDescriptor[] getParameterTypes();

	/**
	 * Get the next parameter number.
	 * Useful when generating "associated" 
	 * parameters.
	 *
	 * @return The next parameter number for the statement.
	 */
	public int getNextParameterNumber();

	/**
	 * Get the params stored vai setParams.
	 * WARNING: nulls out the params value before
	 * returning it, so a subsequent get will
	 * get a null.
	 *
	 * @return the parameters
	 */
	public ParameterValueSet getParams();
	
	/**
	 * Set params
	 *
	 * @param params the params
	 */
	public void setParams(ParameterValueSet params);

	/**
	 * Get the cursor info stored in the context.
	 *
	 * @return the cursor info
	 */
	public Object getCursorInfo();
	
	/**
	 * Set params
	 *
	 * @param cursorInfo the cursor info
	 */
	public void setCursorInfo(Object cursorInfo);

	/**
	 * Set the isolation level for the scans in this query.
	 *
	 * @param isolationLevel	The isolation level to use.
	 *
	 * @return Nothing.
	 */
	public void setScanIsolationLevel(int isolationLevel);

	/**
	 * Get the isolation level for the scans in this query.
	 *
	 * @return	The isolation level for the scans in this query.
	 */
	public int getScanIsolationLevel();

	/**
	 * Set the isolation level on entry to this CC so that it can be restored on exit.
	 *
	 * @param isolationLevel	The isolation level on entry.
	 *
	 * @return Nothing.
	 */
	public void setEntryIsolationLevel(int isolationLevel);

	/**
	 * Get the entry isolation level from this CC.
	 *
	 * @return	The entry isolation level.
	 */
	public int getEntryIsolationLevel();

	/**
	 * Get the next equivalence class for equijoin clauses.
	 *
	 * @return The next equivalence class for equijoin clauses.
	 */
	public int getNextEquivalenceClass();

	/**
		Add a compile time warning.
	*/
	public void addWarning(SQLWarning warning);

	/**
		Get the chain of compile time warnings.
	*/
	public SQLWarning getWarnings();
}
