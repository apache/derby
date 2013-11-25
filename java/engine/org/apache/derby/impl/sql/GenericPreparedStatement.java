/*

   Derby - Class org.apache.derby.impl.sql.GenericPreparedStatement

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

package org.apache.derby.impl.sql;

import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.SQLSessionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.StatementPermission;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecCursorTableReference;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataTypeUtilities;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.impl.sql.compile.CursorNode;
import org.apache.derby.impl.sql.compile.StatementNode;

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
    protected List<String>           updateColumns;
	protected int 						updateMode;

	protected ConstantAction	executionConstants;
	protected Object[]	savedObjects;
    protected List<StatementPermission> requiredPermissionsList;

	// fields for dependency tracking
	protected String UUIDString;
	protected UUID   UUIDValue;

	private boolean needsSavepoint;

	private String execStmtName;
	private String execSchemaName;
	protected boolean isAtomic;
	protected String sourceTxt;

	private int inUseCount;

	// true if the statement is being compiled.
	boolean compilingStatement;

    /** True if the statement was invalidated while it was being compiled. */
    boolean invalidatedWhileCompiling;

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

    /**
     * Incremented for each (re)compile.
     */
    private long versionCounter;

    /**
     * Holder for row counts and execution count. Used for determining
     * whether the statement should be recompiled.
     */
    private RowCountStatistics rowCountStats = new RowCountStatistics();

	//
	// constructors
	//

	GenericPreparedStatement() {
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
		return isUpToDate();
	}

    /**
     * Check whether this statement is up to date and its generated class is
     * identical to the supplied class object.
     * @see ExecPreparedStatement#upToDate(GeneratedClass)
     */
    public synchronized boolean upToDate(GeneratedClass gc) {
        return (activationClass == gc) && isUpToDate();
    }

    /**
     * Unsynchronized helper method for {@link #upToDate()} and {@link
     * #upToDate(GeneratedClass)}. Checks whether this statement is up to date.
     *
     * @return {@code true} if this statement is up to date, {@code false}
     * otherwise
     */
    private boolean isUpToDate() {
        return isValid && (activationClass != null) && !compilingStatement;
    }

	public void rePrepare(LanguageConnectionContext lcc) 
		throws StandardException {
		if (!upToDate()) {
			PreparedStatement ps = statement.prepare(lcc);

			if (SanityManager.DEBUG)
				SanityManager.ASSERT(ps == this, "ps != this");
		}
	}

	/**
	 * Get a new activation instance.
	 *
	 * @exception StandardException thrown if finished.
	 */
	public Activation getActivation(LanguageConnectionContext lcc,
									boolean scrollable)
		throws StandardException
	{
		Activation ac;
		synchronized (this) {
			GeneratedClass gc = getActivationClass();

			if (gc == null) {
				rePrepare(lcc);
				gc = getActivationClass();
			}

			ac = new GenericActivationHolder(lcc, gc, this, scrollable);

			inUseCount++;
		}
		// DERBY-2689. Close unused activations-- this method should be called
		// when I'm not holding a lock on a prepared statement to avoid
		// deadlock.
		lcc.closeUnusedActivations();

		Activation parentAct = null;
		StatementContext stmctx = lcc.getStatementContext();

		if (stmctx != null) {
			// If not null, parentAct represents one of 1) the activation of a
			// calling statement and this activation corresponds to a statement
			// inside a stored procedure or function, and 2) the activation of
			// a statement that performs a substatement, e.g. trigger body
			// execution.
			parentAct = stmctx.getActivation();
		}

		ac.setParentActivation(parentAct);

		return ac;
	}

	/**
	 * @see PreparedStatement#executeSubStatement(LanguageConnectionContext, boolean, long)
	 */
    public ResultSet executeSubStatement(LanguageConnectionContext lcc,
										 boolean rollbackParentContext,
										 long timeoutMillis)
		throws StandardException
	{
		Activation parent = lcc.getLastActivation();
		Activation a = getActivation(lcc, false);
		a.setSingleExecution();
		lcc.setupSubStatementSessionContext(parent);
		return executeStmt(a, rollbackParentContext, timeoutMillis);
	}

	/**
	 * @see PreparedStatement#executeSubStatement(Activation, Activation, boolean, long)
	 */
    public ResultSet executeSubStatement(Activation parent,
										 Activation activation,
										 boolean rollbackParentContext,
										 long timeoutMillis)
		throws StandardException
	{
		parent.getLanguageConnectionContext().
			setupSubStatementSessionContext(parent);
		return executeStmt(activation, rollbackParentContext, timeoutMillis);
	}


	/**
	 * @see PreparedStatement#execute
	 */
	public ResultSet execute(Activation activation,
							 long timeoutMillis)
			throws StandardException
	{
		return executeStmt(activation, false, timeoutMillis);
	}


	/**
	  *	The guts of execution.
	  *
	  *	@param	activation					the activation to run.
	  * @param rollbackParentContext True if 1) the statement context is
	  *  NOT a top-level context, AND 2) in the event of a statement-level
	  *	 exception, the parent context needs to be rolled back, too.
      * @param timeoutMillis timeout value in milliseconds.
	  *	@return	the result set to be pawed through
	  *
	  *	@exception	StandardException thrown on error
	  */
    private ResultSet executeStmt(Activation activation,
								  boolean rollbackParentContext,
								  long timeoutMillis)
        throws
            StandardException 
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
			if (!spsAction) {
			// only re-prepare if this isn't an SPS for a trigger-action;
			// if it _is_ an SPS for a trigger action, then we can't just
			// do a regular prepare because the statement might contain
			// internal SQL that isn't allowed in other statements (such as a
			// static method call to get the trigger context for retrieval
			// of "new row" or "old row" values).  So in that case we
			// skip the call to 'rePrepare' and if the statement is out
			// of date, we'll get a NEEDS_COMPILE exception when we try
			// to execute.  That exception will be caught by the executeSPS()
			// method of the GenericTriggerExecutor class, and at that time
			// the SPS action will be recompiled correctly.
                rePrepare(lccToUse);
			}

			StatementContext statementContext = lccToUse.pushStatementContext(
				isAtomic, updateMode==CursorNode.READ_ONLY, getSource(), pvs, rollbackParentContext, timeoutMillis);

			statementContext.setActivation(activation);

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

            ResultSet resultSet;
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

            if (activation.getSQLSessionContextForChildren() != null) {
                lccToUse.popNestedSessionContext(activation);
            }

			if (activation.isSingleExecution() && resultSet.isClosed())
			{
				// if the result set is 'done', i.e. not openable,
				// then we can also release the activation.
				// Note that a result set with output parameters 
				// or rows to return is explicitly finished 
				// by the user.
				activation.close();
			}

			return resultSet;
			
		}
	}

	public ResultDescription	getResultDescription()	{
		return resultDesc;
	}

	public DataTypeDescriptor[]	getParameterTypes()	{
        return ArrayUtil.copy(paramTypeDescriptors);
	}

    /** Return the type of the parameter (0-based indexing) */
    public DataTypeDescriptor  getParameterType( int idx ) throws StandardException
    {
		if ( paramTypeDescriptors == null )
        {
			throw StandardException.newException( SQLState.NO_INPUT_PARAMETERS );
		}

		/* Check that the parameterIndex is in range. */
		if ( (idx < 0) || (idx >= paramTypeDescriptors.length) )
        {
			/* This message matches the one used by the DBMS */
			throw StandardException.newException
                (
                 SQLState.LANG_INVALID_PARAM_POSITION, 
                 new Integer( idx+1 ),
                 new Integer( paramTypeDescriptors.length )
                 );
		}

        return paramTypeDescriptors[ idx ];
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
		return DataTypeUtilities.clone( beginCompileTimestamp );
	}

	/**
	 * Get the timestamp for the end of compilation
	 *
	 * @return Timestamp	The timestamp for the end of compilation.
	 */
	public Timestamp getEndCompileTimestamp()
	{
		return DataTypeUtilities.clone( endCompileTimestamp );
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
				SanityManager.THROWASSERT("Unexpected exception", se);
			}
		}
	}

	/**
	 *	Set the Execution constants. This routine is called as we Prepare the
	 *	statement.
	 *
	 *	@param constantAction The big structure enclosing the Execution constants.
	 */
	final void	setConstantAction( ConstantAction constantAction )
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
	final void	setSavedObjects( Object[] objects )
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
    public final List<Object> getSavedObjects()
	{
        // Return an unmodifiable view of the underlying array, so that
        // the caller cannot modify the internal state.
        return ArrayUtil.asReadOnlyList(savedObjects);
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
			// Used by activations only:
		case DependencyManager.RECHECK_PRIVILEGES:
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

	 	@exception StandardException Standard Derby error policy.
	 */
	public void makeInvalid(int action, LanguageConnectionContext lcc)
		 throws StandardException
	{

		boolean alreadyInvalid;

		switch (action) {
		case DependencyManager.RECHECK_PRIVILEGES:
			return;
		}

		synchronized (this) {

			if (compilingStatement)
            {
                // Since the statement is in the process of being compiled,
                // and at the end of the compilation it will set isValid to
                // true and overwrite whatever we set it to here, set another
                // flag to indicate that an invalidation was requested. A
                // re-compilation will be triggered if this flag is set, but
                // not until the current compilation is done.
                invalidatedWhileCompiling = true;
				return;
            }

			alreadyInvalid = !isValid;
		
			// make ourseleves invalid
			isValid = false;

			// block compiles while we are invalidating
			compilingStatement = true;
		}

		try {

			DependencyManager dm = lcc.getDataDictionary().getDependencyManager();

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
	 * Return true if the query node for this statement references SESSION schema
	 * tables/views.
	 * This method gets called at the very beginning of the compile phase of any statement.
	 * If the statement which needs to be compiled is already found in cache, then there is
	 * no need to compile it again except the case when the statement is referencing SESSION
	 * schema objects. There is a small window where such a statement might get cached 
	 * temporarily (a statement referencing SESSION schema object will be removed from the
	 * cache after the bind phase is over because that is when we know for sure that the 
	 * statement is referencing SESSION schema objects.)
	 *
	 * @return	true if references SESSION schema tables, else false
	 */
	public boolean referencesSessionSchema()
	{
		return referencesSessionSchema;
	}
	
	/**
	 * Return true if the QueryTreeNode references SESSION schema tables/views.
	 * The return value is also saved in the local field because it will be 
	 * used by referencesSessionSchema() method. 
	 * This method gets called when the statement is not found in cache and 
	 * hence it is getting compiled.
	 * At the beginning of compilation for any statement, first we check if
	 * the statement's plan already exist in the cache. If not, then we add
	 * the statement to the cache and continue with the parsing and binding.
	 * At the end of the binding, this method gets called to see if the 
	 * QueryTreeNode references a SESSION schema object. If it does, then
	 * we want to remove it from the cache, since any statements referencing
	 * SESSION schema objects should never get cached.  
	 *
	 * @return	true if references SESSION schema tables/views, else false
	 */
	public boolean referencesSessionSchema(StatementNode qt)
	throws StandardException {
		//If the query references a SESSION schema table (temporary or permanent), then
		// mark so in this statement
		referencesSessionSchema = qt.referencesSessionSchema();
		return(referencesSessionSchema);
	}

	//
	// class interface
	//

	/**
		Makes the prepared statement valid, assigning
		values for its query tree, generated class,
		and associated information.

		@param qt the query tree for this statement

		@exception StandardException thrown on failure.
	 */
	void completeCompile(StatementNode qt)
						throws StandardException {
		//if (finished)
		//	throw StandardException.newException(SQLState.LANG_STATEMENT_CLOSED, "completeCompile()");

		paramTypeDescriptors = qt.getParameterTypes();

		// erase cursor info in case statement text changed
		if (targetTable!=null) {
			targetTable = null;
			updateMode = 0;
			updateColumns = null;
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
            setCursorInfo((CursorInfo) qt.getCursorInfo());
		}
		isValid = true;

        rowCountStats.reset();
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

    public boolean hasUpdateColumns() {
        return updateColumns != null && !updateColumns.isEmpty();
    }

    public boolean isUpdateColumn(String columnName) {
        return updateColumns != null && updateColumns.contains(columnName);
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
			updateColumns);
	}

	void setCursorInfo(CursorInfo cursorInfo)
	{
		if (cursorInfo != null)
		{
			updateMode = cursorInfo.updateMode;
			targetTable = cursorInfo.targetTable;
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
		clone.updateColumns = updateColumns;
		clone.updateMode = updateMode;	
		clone.needsSavepoint = needsSavepoint;
        clone.rowCountStats = rowCountStats;

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
					SanityManager.THROWASSERT("Unexpected exception", se);
				}
			}
		}
	}

    @Override
	public String toString() {
		return getObjectName();
	}

	public boolean isStorable() {
		return false;
	}

    public void setRequiredPermissionsList(
            List<StatementPermission> requiredPermissionsList)
	{
		this.requiredPermissionsList = requiredPermissionsList;
	}

    public List<StatementPermission> getRequiredPermissionsList()
	{
		return requiredPermissionsList;
	}

    public final long getVersionCounter() {
        return versionCounter;
    }

    public final void incrementVersionCounter() {
        ++versionCounter;
    }

    // Stale plan checking.

    /**
     * This class holds information about stale plan check interval,
     * execution count and row count statistics for a GenericPreparedStatement.
     *
     * The fields and methods should ideally live in GenericPreparedStatement,
     * not in a separate class. However, triggers clone the GPS on each
     * execution, which means the statistics would be reset on each execution
     * if they lived directly inside GPS. Instead, keep the statistics in an
     * object that can be shared between multiple GPS instances when they
     * are cloned.
     */
    private static class RowCountStatistics {
        private int stalePlanCheckInterval;
        private int executionCount;
        private ArrayList<Long> rowCounts;

        // No synchronization for executionCount. Since it's accessed on
        // every execution, we want to avoid synchronization. Nothing serious
        // happens if the execution count is off, we just risk checking for
        // stale plans at a different frequency than specified by
        // derby.language.stalePlanCheckInterval.
        //
        // We might want to use a java.util.concurrent.atomic.AtomicInteger
        // and its atomic incrementAndGet() method once support for pre-Java 5
        // JVMs is dropped.

        /** @see ExecPreparedStatement#incrementExecutionCount() */
        int incrementExecutionCount() {
            return ++executionCount;
        }

        /** @see ExecPreparedStatement#getInitialRowCount(int, long) */
        synchronized long getInitialRowCount(int rsNum, long rowCount) {
            // Allocate the list of row counts lazily.
            if (rowCounts == null) {
                rowCounts = new ArrayList<Long>();
            }

            // Make sure the list is big enough to hold the row count for
            // the specified result set number.
            if (rsNum >= rowCounts.size()) {
                int newSize = rsNum + 1;
                rowCounts.addAll(
                                 Collections.nCopies(newSize - rowCounts.size(), (Long) null));
            }

            // Get the initial row count for the specified result set, and
            // set it if it is not already set.
            Long initialCount = rowCounts.get(rsNum);
            if (initialCount == null) {
                rowCounts.set(rsNum, ReuseFactory.getLong(rowCount));
                return rowCount;
            } else {
                return initialCount.longValue();
            }
        }

        // No synchronization for stale plan check interval. Same reason as
        // stated above for executionCount. Since int accesses are guaranteed
        // atomic, the worst that could happen is that one thread sees it as
        // uninitialized (zero) when another thread in fact has initialized it,
        // and we end up doing the initialization work twice.

        /** @see ExecPreparedStatement#setStalePlanCheckInterval(int) */
        void setStalePlanCheckInterval(int interval) {
            stalePlanCheckInterval = interval;
        }

        /** @see ExecPreparedStatement#getStalePlanCheckInterval() */
        int getStalePlanCheckInterval() {
            return stalePlanCheckInterval;
        }

        /** Reset all the row count statistics. */
        synchronized void reset() {
            stalePlanCheckInterval = 0;
            executionCount = 0;
            rowCounts = null;
        }
    }

    /** @see ExecPreparedStatement#incrementExecutionCount() */
    public int incrementExecutionCount() {
        return rowCountStats.incrementExecutionCount();
    }

    /** @see ExecPreparedStatement#setStalePlanCheckInterval(int) */
    public void setStalePlanCheckInterval(int interval) {
        rowCountStats.setStalePlanCheckInterval(interval);
    }

    /** @see ExecPreparedStatement#getStalePlanCheckInterval() */
    public int getStalePlanCheckInterval() {
        return rowCountStats.getStalePlanCheckInterval();
    }

    /** @see ExecPreparedStatement#getInitialRowCount(int, long) */
    public long getInitialRowCount(int rsNum, long currentRowCount) {
        return rowCountStats.getInitialRowCount(rsNum, currentRowCount);
    }
}
