/*

   Derby - Class org.apache.derby.impl.sql.GenericPreparedStatement

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

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.reference.JDBC20Translation;

import	org.apache.derby.catalog.Dependable;
import	org.apache.derby.catalog.DependableFinder;

import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.cache.Cacheable;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.util.ByteArray;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;

import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecCursorTableReference;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.impl.sql.compile.QueryTreeNode;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.loader.ClassFactory;

import java.sql.Timestamp;
import java.sql.SQLWarning;
import java.util.Vector;

/**
 * Basic implementation of prepared statement.
 * relies on implementation of ResultDescription and Statement that
 * are also in this package.
 * <p>
 * These are both dependents (of the schema objects and prepared statements
 * they depend on) and providers.  Prepared statements that are providers
 * are cursors that end up being used in positioned delete and update
 * statements (at present).
 * <p>
 * This is impl with the regular prepared statements; they will never
 * have the cursor info fields set.
 * <p>
 * Stored prepared statements extend this implementation
 *
 * @author ames
 */
public class GenericPreparedStatement
	implements ExecPreparedStatement
{
	///////////////////////////////////////////////
	//
	// WARNING: when adding members to this class, be
	// sure to do the right thing in getClone(): if
	// it is PreparedStatement specific like finished,
	// then it shouldn't be copied, but stuff like parameters 
	// must be copied.	
	//
	////////////////////////////////////////////////

	////////////////////////////////////////////////
	// STATE that is copied by getClone()
	////////////////////////////////////////////////
	public Statement statement;
	protected GeneratedClass activationClass; // satisfies Activation
	protected ResultDescription resultDesc;
	protected DataTypeDescriptor[] paramTypeDescriptors;
	private String			spsName;
	private SQLWarning		warnings;

	//If the query node for this statement references SESSION schema tables, mark it so in the boolean below
	//This information will be used by EXECUTE STATEMENT if it is executing a statement that was created with NOCOMPILE. Because
	//of NOCOMPILE, we could not catch SESSION schema table reference by the statement at CREATE STATEMENT time. Need to catch
	//such statements at EXECUTE STATEMENT time when the query is getting compiled.
	//This information will also be used to decide if the statement should be cached or not. Any statement referencing SESSION
	//schema tables will not be cached.
	private boolean		referencesSessionSchema;

	// fields used for cursors
	protected ExecCursorTableReference	targetTable; 
	protected ResultColumnDescriptor[]	targetColumns; 
	protected String[] 					updateColumns; 
	protected int 						updateMode;

	protected ConstantAction	executionConstants;
	protected Object[]	savedObjects;

	// fields for dependency tracking
	protected String UUIDString;
	protected UUID   UUIDValue;

	private ParameterValueSet params;
	private boolean needsSavepoint;

	private String execStmtName;
	private String execSchemaName;
	protected boolean isAtomic;
	protected String sourceTxt;

	private int inUseCount;

	// true if the statement is being compiled.
	boolean compilingStatement;


	////////////////////////////////////////////////
	// STATE that is not copied by getClone()
	////////////////////////////////////////////////
	// fields for run time stats
	protected long parseTime;
	protected long bindTime;
	protected long optimizeTime;
	protected long generateTime;
	protected long compileTime;
	protected Timestamp beginCompileTimestamp;
	protected Timestamp endCompileTimestamp;

	//private boolean finished;
	protected boolean isValid;
	protected boolean spsAction;

	// state for caching.
	/**
		If non-null then this object is the cacheable
		that holds us in the cache.
	*/
	private Cacheable cacheHolder;

	//
	// constructors
	//

	protected GenericPreparedStatement() {
		/* Get the UUID for this prepared statement */
		UUIDFactory uuidFactory = 
			Monitor.getMonitor().getUUIDFactory();

		UUIDValue = uuidFactory.createUUID();
		UUIDString = UUIDValue.toString();
		spsAction = false;
	}

	/**
	 */
	public GenericPreparedStatement(Statement st)
	{
		this();

		statement = st;
	}

	//
	// PreparedStatement interface
	//
	public synchronized boolean	upToDate()
		throws StandardException
	{
		boolean	upToDate =  isValid && (activationClass != null) && !compilingStatement;

		// this if for the Plugin
		if ( executionConstants != null )
	    {
			boolean		constantsUpToDate = executionConstants.upToDate();
			upToDate = upToDate && constantsUpToDate;
		}

		return upToDate;
	}

	public void rePrepare(LanguageConnectionContext lcc) 
		throws StandardException {
		if (!upToDate())
		    makeValid(lcc);
	}

	/**
	 * Get a new activation instance.
	 *
	 * @exception StandardException thrown if finished.
	 */
	public synchronized Activation	getActivation(LanguageConnectionContext lcc, boolean scrollable) throws StandardException 
	{
		GeneratedClass gc = getActivationClass();

		if (gc == null) {
			rePrepare(lcc);
			gc = getActivationClass();
		}

		Activation ac = new GenericActivationHolder(lcc, gc, this, scrollable);

		if (params != null)
		{
			ac.setParameters(params, null);
		}

		inUseCount++;

		return ac;
	}

	public ResultSet execute(LanguageConnectionContext lcc, boolean rollbackParentContext)
		throws StandardException
	{
		Activation a = getActivation(lcc, false);
		a.setSingleExecution();
		return execute(a, false, false, rollbackParentContext);
	}

	/**
	  *	The guts of execution.
	  *
	  *	@param	activation					the activation to run.
	  * @param	executeQuery				Called via executeQuery
	  * @param	executeUpdate				Called via executeUpdate
	  * @param rollbackParentContext True if 1) the statement context is
	  *  NOT a top-level context, AND 2) in the event of a statement-level
	  *	 exception, the parent context needs to be rolled back, too.
	  *	@return	the result set to be pawed through
	  *
	  *	@exception	StandardException thrown on error
	  */

	public ResultSet execute
	(Activation activation, boolean executeQuery, boolean executeUpdate,
		boolean rollbackParentContext) throws StandardException 
	{
		boolean				needToClearSavePoint = false;

		if (activation == null || activation.getPreparedStatement() != this)
		{
			throw StandardException.newException(SQLState.LANG_WRONG_ACTIVATION, "execute");
		}

recompileOutOfDatePlan:
		while (true) {
			// verify the activation is for me--somehow.  NOTE: This is
			// different from the above check for whether the activation is
			// associated with the right PreparedStatement - it's conceivable
			// that someone could construct an activation of the wrong type
			// that points to the right PreparedStatement.
			//
			//SanityManager.ASSERT(activation instanceof activationClass, "executing wrong activation");

			/* This is where we set and clear savepoints around each individual
			 * statement which needs one.  We don't set savepoints for cursors because
			 * they're not needed and they wouldn't work in a read only database.
			 * We can't set savepoints for commit/rollback because they'll get
			 * blown away before we try to clear them.
			 */

			LanguageConnectionContext lccToUse = activation.getLanguageConnectionContext();

 			if (lccToUse.getLogStatementText())
			{
				HeaderPrintWriter istream = Monitor.getStream();
				String xactId = lccToUse.getTransactionExecute().getActiveStateTxIdString();
				String pvsString = "";
				ParameterValueSet pvs = activation.getParameterValueSet();
				if (pvs != null && pvs.getParameterCount() > 0)
				{
					pvsString = " with " + pvs.getParameterCount() +
							" parameters " + pvs.toString();
				}
				istream.printlnWithHeader(LanguageConnectionContext.xidStr + 
										  xactId + 
										  "), " +
										  LanguageConnectionContext.lccStr +
										  lccToUse.getInstanceNumber() +
										  "), " +
										  LanguageConnectionContext.dbnameStr +
										  lccToUse.getDbname() +
										  "), " +
										  LanguageConnectionContext.drdaStr +
										  lccToUse.getDrdaID() +
										  "), Executing prepared statement: " +
										  getSource() +
										  " :End prepared statement" +
										  pvsString);
			}

			ParameterValueSet pvs = activation.getParameterValueSet();

			/* put it in try block to unlock the PS in any case
			 */
			rePrepare(lccToUse);

			StatementContext statementContext = lccToUse.pushStatementContext(
				isAtomic, getSource(), pvs, rollbackParentContext);

			if (needsSavepoint())
			{
				/* Mark this position in the log so that a statement
				* rollback will undo any changes.
				*/
				statementContext.setSavePoint();
				needToClearSavePoint = true;
			}

			if (executionConstants != null)
			{
				lccToUse.validateStmtExecution(executionConstants);
			}

			ResultSet resultSet = null;
			try {
	
				resultSet = activation.execute();

				resultSet.open();
			} catch (StandardException se) {
				/* Cann't handle recompiling SPS action recompile here */
				if (!se.getMessageId().equals(SQLState.LANG_STATEMENT_NEEDS_RECOMPILE)
						 || spsAction)
					throw se;
				statementContext.cleanupOnError(se);
				continue recompileOutOfDatePlan;

			}


			if (needToClearSavePoint)
			{
				/* We're done with our updates */
				statementContext.clearSavePoint();
			}

			lccToUse.popStatementContext(statementContext, null);					

			if (activation.isSingleExecution() && resultSet.isClosed())
			{
				// if the result set is 'done', i.e. not openable,
				// then we can also release the activation.
				// Note that a result set with output parameters 
				// or rows to return is explicitly finished 
				// by the user.
				activation.close();
			}


			/* executeQuery() not allowed on statements
			 * that return a row count,
			 * executeUpdate() not allowed on statements
			 * that return a ResultSet.
			 * We need to do the test here so that any
			 * exeception will rollback to the statement
			 * savepoint.
			 */
			if ( (! resultSet.returnsRows()) && executeQuery)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_CALL_TO_EXECUTE_QUERY);
			}

			if ( resultSet.returnsRows() && executeUpdate)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_CALL_TO_EXECUTE_UPDATE);
			}
			return resultSet;
			
		}
	}

	public ResultDescription	getResultDescription()	{
		return resultDesc;
	}

	public DataTypeDescriptor[]	getParameterTypes()	{
		return paramTypeDescriptors;
	}

	public String getSource() {
		return (sourceTxt != null) ?
			sourceTxt : 
			(statement == null) ? 
				"null" : 
				statement.getSource();
	}

	public void setSource(String text)
	{
		sourceTxt = text;
	}

	public final void setSPSName(String name) {
		spsName = name;
	}

	public String getSPSName() {
		return spsName;
	}


	/**
	 * Get the total compile time for the associated query in milliseconds.
	 * Compile time can be divided into parse, bind, optimize and generate times.
	 * 
	 * @return long		The total compile time for the associated query in milliseconds.
	 */
	public long getCompileTimeInMillis()
	{
		return compileTime;
	}

	/**
	 * Get the parse time for the associated query in milliseconds.
	 * 
	 * @return long		The parse time for the associated query in milliseconds.
	 */
	public long getParseTimeInMillis()
	{
		return parseTime;
	}

	/**
	 * Get the bind time for the associated query in milliseconds.
	 * 
	 * @return long		The bind time for the associated query in milliseconds.
	 */
	public long getBindTimeInMillis()
	{
		return bindTime;
	}

	/**
	 * Get the optimize time for the associated query in milliseconds.
	 * 
	 * @return long		The optimize time for the associated query in milliseconds.
	 */
	public long getOptimizeTimeInMillis()
	{
		return optimizeTime;
	}

	/**
	 * Get the generate time for the associated query in milliseconds.
	 * 
	 * @return long		The generate time for the associated query in milliseconds.
	 */
	public long getGenerateTimeInMillis()
	{
		return generateTime;
	}

	/**
	 * Get the timestamp for the beginning of compilation
	 *
	 * @return Timestamp	The timestamp for the beginning of compilation.
	 */
	public Timestamp getBeginCompileTimestamp()
	{
		return beginCompileTimestamp;
	}

	/**
	 * Get the timestamp for the end of compilation
	 *
	 * @return Timestamp	The timestamp for the end of compilation.
	 */
	public Timestamp getEndCompileTimestamp()
	{
		return endCompileTimestamp;
	}

	void setCompileTimeWarnings(SQLWarning warnings) {
		this.warnings = warnings;
	}

	public final SQLWarning getCompileTimeWarnings() {
		return warnings;
	}

	/**
	 * Set the compile time for this prepared statement.
	 *
	 * @param compileTime	The compile time
	 *
	 * @return Nothing.
	 */
	protected void setCompileTimeMillis(long parseTime, long bindTime,
										long optimizeTime, 
										long generateTime,
										long compileTime,
										Timestamp beginCompileTimestamp,
										Timestamp endCompileTimestamp)
	{
		this.parseTime = parseTime;
		this.bindTime = bindTime;
		this.optimizeTime = optimizeTime;
		this.generateTime = generateTime;
		this.compileTime = compileTime;
		this.beginCompileTimestamp = beginCompileTimestamp;
		this.endCompileTimestamp = endCompileTimestamp;
	}


	/**
		Finish marks a statement as totally unusable.
	 */
	public void finish(LanguageConnectionContext lcc) {

		synchronized (this) {
			inUseCount--;

			if (cacheHolder != null)
				return;

			if (inUseCount != 0) {
				//if (SanityManager.DEBUG) {
				//	if (inUseCount < 0)
				//		SanityManager.THROWASSERT("inUseCount is negative " + inUseCount + " for " + this);
				//}
				return; 
			}
		}
			
		// invalidate any prepared statements that
		// depended on this statement (including this one)
		// prepareToInvalidate(this, DependencyManager.PREPARED_STATEMENT_INVALID);
		try
		{
			/* NOTE: Since we are non-persistent, we "know" that no exception
			 * will be thrown under us.
			 */
			makeInvalid(DependencyManager.PREPARED_STATEMENT_RELEASE, lcc);
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
			{
				se.printStackTrace(System.out);
				SanityManager.THROWASSERT(
					"Unexpected exception - " + se);
			}
		}
	}

	/**
	 *	Set the Execution constants. This routine is called as we Prepare the
	 *	statement.
	 *
	 *	@param	ConstantAction	The big structure enclosing the Execution constants.
	 */
	public	final void	setConstantAction( ConstantAction constantAction )
	{
		executionConstants = constantAction;
	}


	/**
	 *	Get the Execution constants. This routine is called at Execution time.
	 *
	 *	@return	ConstantAction	The big structure enclosing the Execution constants.
	 */
	public	final ConstantAction	getConstantAction()
	{
		return	executionConstants;
	}

	/**
	 *	Set the saved objects. Called when compilation completes.
	 *
	 *	@param	objects	The objects to save from compilation
	 */
	public	final void	setSavedObjects( Object[] objects )
	{
		savedObjects = objects;
	}

	/**
	 *	Get the specified saved object.
	 *
	 *	@param	objectNum	The object to get.
	 *	@return	the requested saved object.
	 */
	public final Object	getSavedObject(int objectNum)
	{
		if (SanityManager.DEBUG) {
			if (!(objectNum>=0 && objectNum<savedObjects.length))
			SanityManager.THROWASSERT(
				"request for savedObject entry "+objectNum+" invalid; "+
				"savedObjects has "+savedObjects.length+" entries");
		}
		return	savedObjects[objectNum];
	}

	/**
	 *	Get the saved objects.
	 *
	 *	@return all the saved objects
	 */
	public	final Object[]	getSavedObjects()
	{
		return	savedObjects;
	}

	//
	// Dependent interface
	//
	/**
		Check that all of the dependent's dependencies are valid.

		@return true if the dependent is currently valid
	 */
	public boolean isValid() {
		return isValid;
	}

	/**
	 * set this prepared statement to be valid, currently used by
	 * GenericTriggerExecutor.
	 */
	public void setValid()
	{
		isValid = true;
	}

	/**
	 * Indicate this prepared statement is an SPS action, currently used
	 * by GenericTriggerExecutor.
	 */
	public void setSPSAction()
	{
		spsAction = true;
	}

	/**
		Prepare to mark the dependent as invalid (due to at least one of
		its dependencies being invalid).

		@param	action	The action causing the invalidation
		@param	p		the provider

		@exception StandardException thrown if unable to make it invalid
	 */
	public void prepareToInvalidate(Provider p, int action, 
									LanguageConnectionContext lcc) 
		throws StandardException {

		/*
			this statement can have other open result sets
			if another one is closing without any problems.

			It is not a problem to create an index when there is an open
			result set, since it doesn't invalidate the access path that was
			chosen for the result set.
		*/
		switch (action) {
		case DependencyManager.CHANGED_CURSOR:
		case DependencyManager.CREATE_INDEX:
			return;
		}

		/* Verify that there are no activations with open result sets
		 * on this prepared statement.
		 */
		lcc.verifyNoOpenResultSets(this, p, action);
	}


	/**
		Mark the dependent as invalid (due to at least one of
		its dependencies being invalid).

		@param	action	The action causing the invalidation

	 	@exception StandardException Standard Cloudscape error policy.
	 */
	public void makeInvalid(int action, LanguageConnectionContext lcc)
		 throws StandardException
	{

		boolean alreadyInvalid;
		
		synchronized (this) {

			if (compilingStatement)
				return;

			alreadyInvalid = !isValid;
		
			// make ourseleves invalid
			isValid = false;

			// block compiles while we are invalidating
			compilingStatement = true;
		}

		try {

			DependencyManager dm = lcc.getDataDictionary().getDependencyManager();

			if (!alreadyInvalid)
			{
				dm.invalidateFor(this, action, lcc);
			}

			/* Clear out the old dependencies on this statement as we
			 * will build the new set during the reprepare in makeValid().
			 */
			dm.clearDependencies(lcc, this);

			/*
			** If we are invalidating an EXECUTE STATEMENT because of a stale
			** plan, we also need to invalidate the stored prepared statement.
			*/
			if (execStmtName != null) {
				switch (action) {
				case DependencyManager.INTERNAL_RECOMPILE_REQUEST:
				case DependencyManager.CHANGED_CURSOR:
				{
					/*
					** Get the DataDictionary, so we can get the descriptor for
					** the SPP to invalidate it.
					*/
					DataDictionary dd = lcc.getDataDictionary();
						
					SchemaDescriptor sd = dd.getSchemaDescriptor(execSchemaName, lcc.getTransactionCompile(), true);
					SPSDescriptor spsd = dd.getSPSDescriptor(execStmtName, sd);
					spsd.makeInvalid(action, lcc);
					break;
				}
				}
			}
		} finally {
			synchronized (this) {
				compilingStatement = false;
				notifyAll();
			}
		}
	}

	/**
		Attempt to revalidate the dependent. For prepared statements,
		this could go through its dependencies and check that they
		are up to date; if not, it would recompile the statement.
		Any failure during this attempt should throw
		StandardException.unableToRevalidate().

		@exception StandardException thrown if unable to make it valid
	 */
	public void makeValid(LanguageConnectionContext lcc) 
		throws StandardException 
	{
		PreparedStatement ps;

		// REMIND: will want to go through dependency list
		// and check if we can make it valid just on faith,
		// i.e. when it was marked 'possibly invalid' due
		// to a rollback or some similar action.

		// this ends up calling makeValid(qt, ac) below:
		ps = statement.prepare(lcc);
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(ps == this, "ps != this");
	}

	/**
	 * Is this dependent persistent?  A stored dependency will be required
	 * if both the dependent and provider are persistent.
	 *
	 * @return boolean		Whether or not this dependent is persistent.
	 */
	public boolean isPersistent()
	{
		/* Non-stored prepared statements are not persistent */
		return false;
	}

	//
	// Dependable interface
	//

	/**		
		@return the stored form of this Dependable

		@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder()
	{
	    return null;
	}

	/**
	 * Return the name of this Dependable.  (Useful for errors.)
	 *
	 * @return String	The name of this Dependable..
	 */
	public String getObjectName()
	{
		return UUIDString;
	}

	/**
	 * Get the Dependable's UUID String.
	 *
	 * @return String	The Dependable's UUID String.
	 */
	public UUID getObjectID()
	{
		return UUIDValue;
	}

	/**
	 * Get the Dependable's class type.
	 *
	 * @return String		Classname that this Dependable belongs to.
	 */
	public String getClassType()
	{
		return Dependable.PREPARED_STATEMENT;
	}

	/**
	 * Return true if the query node for this statement references SESSION schema tables.
	 *
	 * @return	true if references SESSION schema tables, else false
	 */
	public boolean referencesSessionSchema()
	{
		return referencesSessionSchema;
	}

	//
	// class interface
	//

	/**
		Makes the prepared statement valid, assigning
		values for its query tree, generated class,
		and associated information.

		@param qt the query tree for this statement
		@param dtd	The DataTypeDescriptors for the parameters, if any
		@param ac the generated class for this statement

		@return	true if there is a reference to SESSION schema tables, else false

		@exception StandardException thrown on failure.
	 */
	boolean completeCompile(QueryTreeNode qt)
						throws StandardException {
		//if (finished)
		//	throw StandardException.newException(SQLState.LANG_STATEMENT_CLOSED, "completeCompile()");

		paramTypeDescriptors = qt.getParameterTypes();

		//If the query references a SESSION schema table (temporary or permanent), then mark so in this statement
		//This information will be used by EXECUTE STATEMENT if it is executing a statement that was created with NOCOMPILE. Because
		//of NOCOMPILE, we could not catch SESSION schema table reference by the statement at CREATE STATEMENT time. Need to catch
		//such statements at EXECUTE STATEMENT time when the query is getting compiled.
		referencesSessionSchema = qt.referencesSessionSchema();

		// erase cursor info in case statement text changed
		if (targetTable!=null) {
			targetTable = null;
			updateMode = 0;
			updateColumns = null;
			targetColumns = null;
		}

		// get the result description (null for non-cursor statements)
		// would we want to reuse an old resultDesc?
		// or do we need to always replace in case this was select *?
		resultDesc = qt.makeResultDescription();

		// would look at resultDesc.getStatementType() but it
		// doesn't call out cursors as such, so we check
		// the root node type instead.

		if (resultDesc != null)
		{
			/*
				For cursors, we carry around some extra information.
			 */
			CursorInfo cursorInfo = (CursorInfo)qt.getCursorInfo();
			if (cursorInfo != null)
			{
				targetTable = cursorInfo.targetTable;
				targetColumns = cursorInfo.targetColumns;
				updateColumns = cursorInfo.updateColumns;
				updateMode = cursorInfo.updateMode;
			}
		}
		isValid = true;

		//if this statement is referencing session schema tables, then we do not want cache it. 
		return referencesSessionSchema;
	}

	public GeneratedClass getActivationClass()
		throws StandardException
	{
		return activationClass;
	}

	void setActivationClass(GeneratedClass ac)
	{
		activationClass = ac;
	}

	//
	// ExecPreparedStatement
	//

	/**
	 * the update mode of the cursor
	 *
	 * @return	The update mode of the cursor
	 */
	public int	getUpdateMode() {
		return updateMode;
	}

	/**
	 * the target table of the cursor
	 *
	 * @return	target table of the cursor
	 */
	public ExecCursorTableReference getTargetTable() 
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(targetTable!=null, "Not a cursor, no target table");
		}
		return targetTable;
	}

	/**
	 * the target columns of the cursor as a result column list
	 *
	 * @return	target columns of the cursor as a result column list
	 */
	public ResultColumnDescriptor[]	getTargetColumns() {
		return targetColumns;
	}

	/**
	 * the update columns of the cursor as a update column list
	 *
	 * @return	update columns of the cursor as a array of strings
	 */
	public String[]	getUpdateColumns() 
	{
		return updateColumns;
	}

	/**
	 * Return the cursor info in a single chunk.  Used
	 * by StrorablePreparedStatement
	 */
	public Object getCursorInfo()
	{
		return new CursorInfo(
			updateMode,
			targetTable, 
			targetColumns,
			updateColumns);
	}

	void setCursorInfo(CursorInfo cursorInfo)
	{
		if (cursorInfo != null)
		{
			updateMode = cursorInfo.updateMode;
			targetTable = cursorInfo.targetTable;
			targetColumns = cursorInfo.targetColumns;
			updateColumns = cursorInfo.updateColumns;
		}
	}


	//
	// class implementation
	//

	/**
	 * Get the byte code saver for this statement.
	 * Overridden for StorablePreparedStatement.  We
	 * don't want to save anything
	 *
	 * @return a byte code saver (null for us)
	 */
	ByteArray getByteCodeSaver()
	{
		return null;
	}

	/**
	 * Set parameters to be associated with this
	 * statement.  Used to process EXECUTE STATMENT <name>
	 * USING <resultSet> statements -- the <resultSet>
	 * is evaluated and a parameter list is generated.
	 * That list is saved using this call.  Parameters
	 * are set in the activation when it is created
	 * (see getActivation). 
	 * 
	 * @param params the parameters
	 */
	protected void setParams(ParameterValueSet params)
	{
		this.params = params;
	}

	/**
	 * Does this statement need a savepoint?  
	 * 
	 * @return true if this statement needs a savepoint.
	 */
	public boolean needsSavepoint()
	{
		return needsSavepoint;
	}

	/**
	 * Set the stmts 'needsSavepoint' state.  Used
	 * by an SPS to convey whether the underlying stmt
	 * needs a savepoint or not.
	 * 
	 * @param needsSavepoint true if this statement needs a savepoint.
	 */
	void setNeedsSavepoint(boolean needsSavepoint)
	{
	 	this.needsSavepoint = needsSavepoint;
	}

	/**
	 * Set the stmts 'isAtomic' state.  
	 * 
	 * @param isAtomic true if this statement must be atomic
	 * (i.e. it is not ok to do a commit/rollback in the middle)
	 */
	void setIsAtomic(boolean isAtomic)
	{
	 	this.isAtomic = isAtomic;
	}

	/**
	 * Returns whether or not this Statement requires should
	 * behave atomically -- i.e. whether a user is permitted
	 * to do a commit/rollback during the execution of this
	 * statement.
	 *
	 * @return boolean	Whether or not this Statement is atomic
	 */
	public boolean isAtomic()
	{
		return isAtomic;
	}

	/**
	 * Set the name of the statement and schema for an "execute statement"
	 * command.
	 */
	void setExecuteStatementNameAndSchema(String execStmtName,
												 String execSchemaName)
	{
		this.execStmtName = execStmtName;
		this.execSchemaName = execSchemaName;
	}

	/**
	 * Get a new prepared statement that is a shallow copy
	 * of the current one.
	 *
	 * @return a new prepared statement
	 * 
	 * @exception StandardException on error
	 */
	public ExecPreparedStatement getClone() throws StandardException
	{

		GenericPreparedStatement clone = new GenericPreparedStatement(statement);

		clone.activationClass = getActivationClass();
		clone.resultDesc = resultDesc;
		clone.paramTypeDescriptors = paramTypeDescriptors;
		clone.executionConstants = executionConstants;
		clone.UUIDString = UUIDString;
		clone.UUIDValue = UUIDValue;
		clone.savedObjects = savedObjects;
		clone.execStmtName = execStmtName;
		clone.execSchemaName = execSchemaName;
		clone.isAtomic = isAtomic;
		clone.sourceTxt = sourceTxt;
		clone.targetTable = targetTable;
		clone.targetColumns = targetColumns;
		clone.updateColumns = updateColumns;
		clone.updateMode = updateMode;	
		clone.params = params;
		clone.needsSavepoint = needsSavepoint;

		return clone;
	}

	// cache holder stuff.
	public void setCacheHolder(Cacheable cacheHolder) {

		this.cacheHolder = cacheHolder;

		if (cacheHolder == null) {

			// need to invalidate the statement
			if (!isValid || (inUseCount != 0))
				return;

			ContextManager cm = ContextService.getFactory().getCurrentContextManager();
			LanguageConnectionContext lcc = 
				(LanguageConnectionContext) 
				(cm.getContext(LanguageConnectionContext.CONTEXT_ID));

			// invalidate any prepared statements that
			// depended on this statement (including this one)
			// prepareToInvalidate(this, DependencyManager.PREPARED_STATEMENT_INVALID);
			try
			{
				/* NOTE: Since we are non-persistent, we "know" that no exception
				 * will be thrown under us.
				 */
				makeInvalid(DependencyManager.PREPARED_STATEMENT_RELEASE, lcc);
			}
			catch (StandardException se)
			{
				if (SanityManager.DEBUG)
				{
					se.printStackTrace(System.out);
					SanityManager.THROWASSERT(
						"Unexpected exception - " + se);
				}
			}
		}
	}

	public String toString() {
		return getObjectName();
	}

	public boolean isStorable() {
		return false;
	}
}
