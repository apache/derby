/*

   Derby - Class org.apache.derby.impl.sql.GenericStatement

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

import java.sql.Timestamp;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.daemon.IndexStatisticsDaemon;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.ASTVisitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.impl.sql.compile.StatementNode;
import org.apache.derby.impl.sql.conn.GenericLanguageConnectionContext;
import org.apache.derby.iapi.util.InterruptStatus;

public class GenericStatement
	implements Statement {

	// these fields define the identity of the statement
	private final SchemaDescriptor compilationSchema;
	private final String			statementText;
        private final boolean isForReadOnly;
	private int                      prepareIsolationLevel;
	private GenericPreparedStatement preparedStmt;

	/**
	 * Constructor for a Statement given the text of the statement in a String
	 * @param compilationSchema schema
	 * @param statementText	The text of the statement
	 * @param isForReadOnly if the statement is opened with level CONCUR_READ_ONLY
	 */

	public GenericStatement(SchemaDescriptor compilationSchema, String statementText, boolean isForReadOnly)
	{
		this.compilationSchema = compilationSchema;
		this.statementText = statementText;
		this.isForReadOnly = isForReadOnly;
	}

	/*
	 * Statement interface
	 */

	
	/* RESOLVE: may need error checking, debugging code here */
	public PreparedStatement prepare(LanguageConnectionContext lcc) throws StandardException
	{
		/*
		** Note: don't reset state since this might be
		** a recompilation of an already prepared statement.
		*/ 
		return prepare(lcc, false);
	}
	public PreparedStatement prepare(LanguageConnectionContext lcc, boolean forMetaData) throws StandardException
	{
		/*
		** Note: don't reset state since this might be
		** a recompilation of an already prepared statement.
		*/ 

        final int depth = lcc.getStatementDepth();
        String prevErrorId = null;
        while (true) {
            boolean recompile = false;
            try {
                return prepMinion(lcc, true, (Object[]) null,
                                  (SchemaDescriptor) null, forMetaData);
            } catch (StandardException se) {
                // There is a chance that we didn't see the invalidation
                // request from a DDL operation in another thread because
                // the statement wasn't registered as a dependent until
                // after the invalidation had been completed. Assume that's
                // what has happened if we see a conglomerate does not exist
                // error, and force a retry even if the statement hasn't been
                // invalidated.
                if (SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST.equals(
                        se.getMessageId())) {
                    // STORE_CONGLOMERATE_DOES_NOT_EXIST has exactly one
                    // argument: the conglomerate id
                    String conglomId = String.valueOf(se.getArguments()[0]);

                    // Request a recompile of the statement if a conglomerate
                    // disappears while we are compiling it. But if we have
                    // already retried once because the same conglomerate was
                    // missing, there's probably no hope that yet another retry
                    // will help, so let's break out instead of potentially
                    // looping infinitely.
                    if (!conglomId.equals(prevErrorId)) {
                        recompile = true;
                    }

                    prevErrorId = conglomId;
                }
                throw se;
            } finally {
                // Check if the statement was invalidated while it was
                // compiled. If so, the newly compiled plan may not be
                // up to date anymore, so we recompile the statement
                // if this happens. Note that this is checked in a finally
                // block, so we also retry if an exception was thrown. The
                // exception was probably thrown because of the changes
                // that invalidated the statement. If not, recompiling
                // will also fail, and the exception will be exposed to
                // the caller.
                //
                // invalidatedWhileCompiling and isValid are protected by
                // synchronization on the prepared statement.
                synchronized (preparedStmt) {
                    if (recompile || preparedStmt.invalidatedWhileCompiling) {
                        preparedStmt.isValid = false;
                        preparedStmt.invalidatedWhileCompiling = false;
                        recompile = true;
                    }
                }

                if (recompile) {
                    // A new statement context is pushed while compiling.
                    // Typically, this context is popped by an error
                    // handler at a higher level. But since we retry the
                    // compilation, the error handler won't be invoked, so
                    // the stack must be reset to its original state first.
                    while (lcc.getStatementDepth() > depth) {
                        lcc.popStatementContext(
                                lcc.getStatementContext(), null);
                    }

                    // Don't return yet. The statement was invalidated, so
                    // we must retry the compilation.
                    continue;
                }
            }
        }
	}

	private PreparedStatement prepMinion(LanguageConnectionContext lcc, boolean cacheMe, Object[] paramDefaults,
		SchemaDescriptor spsSchema, boolean internalSQL)
		throws StandardException
	{
						  
		long				beginTime = 0;
		long				parseTime = 0;
		long				bindTime = 0;
		long				optimizeTime = 0;
		long				generateTime = 0;
		Timestamp			beginTimestamp = null;
		Timestamp			endTimestamp = null;
		StatementContext	statementContext = null;

		// verify it isn't already prepared...
		// if it is, and is valid, simply return that tree.
		// if it is invalid, we will recompile now.
		if (preparedStmt != null) {
			if (preparedStmt.upToDate())
				return preparedStmt;
		}

		// Clear the optimizer trace from the last statement
		if (lcc.getOptimizerTrace())
			lcc.setOptimizerTraceOutput(getSource() + "\n");

		beginTime = getCurrentTimeMillis(lcc);
		/* beginTimestamp only meaningful if beginTime is meaningful.
		 * beginTime is meaningful if STATISTICS TIMING is ON.
		 */
		if (beginTime != 0)
		{
			beginTimestamp = new Timestamp(beginTime);
		}

		/** set the prepare Isolaton from the LanguageConnectionContext now as 
		 * we need to consider it in caching decisions
		 */
		prepareIsolationLevel = lcc.getPrepareIsolationLevel();

		/* a note on statement caching:
		 * 
		 * A GenericPreparedStatement (GPS) is only added it to the cache if the
		 * parameter cacheMe is set to TRUE when the GPS is created.
		 * 
		 * Earlier only CacheStatement (CS) looked in the statement cache for a
		 * prepared statement when prepare was called. Now the functionality 
		 * of CS has been folded into GenericStatement (GS). So we search the
		 * cache for an existing PreparedStatement only when cacheMe is TRUE.
		 * i.e if the user calls prepare with cacheMe set to TRUE:
		 * then we 
		 *         a) look for the prepared statement in the cache.
		 *         b) add the prepared statement to the cache.
		 *
		 * In cases where the statement cache has been disabled (by setting the
		 * relevant Derby property) then the value of cacheMe is irrelevant.
		 */ 
		boolean foundInCache = false;
		if (preparedStmt == null) 
		{
			if (cacheMe)
				preparedStmt = (GenericPreparedStatement)((GenericLanguageConnectionContext)lcc).lookupStatement(this);

			if (preparedStmt == null) 
			{
				preparedStmt = new GenericPreparedStatement(this);
			}
			else
			{
				foundInCache = true;
			}
		}

		// if anyone else also has this prepared statement,
		// we don't want them trying to compile with it while
		// we are.  So, we synchronize on it and re-check
		// its validity first.
		// this is a no-op if and until there is a central
		// cache of prepared statement objects...
		synchronized (preparedStmt) 
		{

			for (;;) {

				if (foundInCache) {
					if (preparedStmt.referencesSessionSchema()) {
						// cannot use this state since it is private to a connection.
						// switch to a new statement.
						foundInCache = false;
						preparedStmt = new GenericPreparedStatement(this);
						break;
					}
				}

				// did it get updated while we waited for the lock on it?
				if (preparedStmt.upToDate()) {
					return preparedStmt;
				}

				if (!preparedStmt.compilingStatement) {
					break;
				}

				try {
					preparedStmt.wait();
				} catch (InterruptedException ie) {
                    InterruptStatus.setInterrupted();
				}
			}

			preparedStmt.compilingStatement = true;
			preparedStmt.setActivationClass(null);
		}

		try {

			HeaderPrintWriter istream = lcc.getLogStatementText() ? Monitor.getStream() : null;

			/*
			** For stored prepared statements, we want all
			** errors, etc in the context of the underlying
			** EXECUTE STATEMENT statement, so don't push/pop
			** another statement context unless we don't have
			** one.  We won't have one if it is an internal
			** SPS (e.g. jdbcmetadata).
			*/
			if (!preparedStmt.isStorable() || lcc.getStatementDepth() == 0)
			{
				// since this is for compilation only, set atomic
				// param to true and timeout param to 0
				statementContext = lcc.pushStatementContext(true, isForReadOnly, getSource(),
                                                            null, false, 0L);
			}



			/*
			** RESOLVE: we may ultimately wish to pass in
			** whether we are a jdbc metadata query or not to
			** get the CompilerContext to make the createDependency()
			** call a noop.
			*/
			CompilerContext cc = lcc.pushCompilerContext(compilationSchema);
			
			if (prepareIsolationLevel != 
				ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL)
			{
				cc.setScanIsolationLevel(prepareIsolationLevel);
			}


			// Look for stored statements that are in a system schema
			// and with a match compilation schema. If so, allow them
			// to compile using internal SQL constructs.

			if (internalSQL ||
				(spsSchema != null) && (spsSchema.isSystemSchema()) &&
					(spsSchema.equals(compilationSchema))) {
						cc.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);
			}

			try 
			{
				// Statement logging if lcc.getLogStatementText() is true
				if (istream != null)
				{
					String xactId = lcc.getTransactionExecute().getActiveStateTxIdString();
					istream.printlnWithHeader(LanguageConnectionContext.xidStr + 
											  xactId + 
											  "), " +
											  LanguageConnectionContext.lccStr +
												  lcc.getInstanceNumber() +
											  "), " +
											  LanguageConnectionContext.dbnameStr +
												  lcc.getDbname() +
											  "), " +
											  LanguageConnectionContext.drdaStr +
												  lcc.getDrdaID() +
											  "), Begin compiling prepared statement: " + 
											  getSource() +
											  " :End prepared statement");
				}

				Parser p = cc.getParser();

				cc.setCurrentDependent(preparedStmt);

				//Only top level statements go through here, nested statement
				//will invoke this method from other places
				StatementNode qt = (StatementNode)
                        p.parseStatement(statementText, paramDefaults);

				parseTime = getCurrentTimeMillis(lcc);

                // Call user-written tree-printer if it exists
                walkAST( lcc, qt, ASTVisitor.AFTER_PARSE);

				if (SanityManager.DEBUG) 
				{
					if (SanityManager.DEBUG_ON("DumpParseTree")) 
					{
						SanityManager.GET_DEBUG_STREAM().print(
							"\n\n============PARSE===========\n\n");
						qt.treePrint();
						lcc.getPrintedObjectsMap().clear();
					}

					if (SanityManager.DEBUG_ON("StopAfterParsing")) 
					{
                        lcc.setLastQueryTree( qt );
                        
						throw StandardException.newException(SQLState.LANG_STOP_AFTER_PARSING);
					}
				}

				/*
				** Tell the data dictionary that we are about to do
				** a bunch of "get" operations that must be consistent with
				** each other.
				*/
				
				DataDictionary dataDictionary = lcc.getDataDictionary();

				int ddMode = dataDictionary == null ? 0 : dataDictionary.startReading(lcc);

				try
				{
					// start a nested transaction -- all locks acquired by bind
					// and optimize will be released when we end the nested
					// transaction.
					lcc.beginNestedTransaction(true);

					qt.bindStatement();
					bindTime = getCurrentTimeMillis(lcc);

                    // Call user-written tree-printer if it exists
                    walkAST( lcc, qt, ASTVisitor.AFTER_BIND);

					if (SanityManager.DEBUG) 
					{
						if (SanityManager.DEBUG_ON("DumpBindTree")) 
						{
							SanityManager.GET_DEBUG_STREAM().print(
								"\n\n============BIND===========\n\n");
							qt.treePrint();
							lcc.getPrintedObjectsMap().clear();
						}

						if (SanityManager.DEBUG_ON("StopAfterBinding")) {
							throw StandardException.newException(SQLState.LANG_STOP_AFTER_BINDING);
						}
					}

					//Derby424 - In order to avoid caching select statements referencing
					// any SESSION schema objects (including statements referencing views
					// in SESSION schema), we need to do the SESSION schema object check
					// here.  
					//a specific eg for statement referencing a view in SESSION schema 
					//CREATE TABLE t28A (c28 int)
					//INSERT INTO t28A VALUES (280),(281)
					//CREATE VIEW SESSION.t28v1 as select * from t28A
					//SELECT * from SESSION.t28v1 should show contents of view and we
					// should not cache this statement because a user can later define
					// a global temporary table with the same name as the view name.
					//Following demonstrates that
					//DECLARE GLOBAL TEMPORARY TABLE SESSION.t28v1(c21 int, c22 int) not
					//     logged
					//INSERT INTO SESSION.t28v1 VALUES (280,1),(281,2)
					//SELECT * from SESSION.t28v1 should show contents of global temporary
					//table and not the view.  Since this select statement was not cached
					// earlier, it will be compiled again and will go to global temporary
					// table to fetch data. This plan will not be cached either because
					// select statement is using SESSION schema object.
					//
					//Following if statement makes sure that if the statement is
					// referencing SESSION schema objects, then we do not want to cache it.
					// We will remove the entry that was made into the cache for 
					//this statement at the beginning of the compile phase.
					//The reason we do this check here rather than later in the compile
					// phase is because for a view, later on, we loose the information that
					// it was referencing SESSION schema because the reference
					//view gets replaced with the actual view definition. Right after
					// binding, we still have the information on the view and that is why
					// we do the check here.
					if (preparedStmt.referencesSessionSchema(qt)) {
						if (foundInCache)
							((GenericLanguageConnectionContext)lcc).removeStatement(this);
					}
					
					qt.optimizeStatement();

					optimizeTime = getCurrentTimeMillis(lcc);

                    // Call user-written tree-printer if it exists
                    walkAST( lcc, qt, ASTVisitor.AFTER_OPTIMIZE);

					// Statement logging if lcc.getLogStatementText() is true
					if (istream != null)
					{
						String xactId = lcc.getTransactionExecute().getActiveStateTxIdString();
						istream.printlnWithHeader(LanguageConnectionContext.xidStr + 
												  xactId + 
												  "), " +
												  LanguageConnectionContext.lccStr +
												  lcc.getInstanceNumber() +
												  "), " +
												  LanguageConnectionContext.dbnameStr +
												  lcc.getDbname() +
												  "), " +
												  LanguageConnectionContext.drdaStr +
												  lcc.getDrdaID() +
												  "), End compiling prepared statement: " + 
												  getSource() +
												  " :End prepared statement");
					}
				}

				catch (StandardException se)
				{
					lcc.commitNestedTransaction();

					// Statement logging if lcc.getLogStatementText() is true
					if (istream != null)
					{
						String xactId = lcc.getTransactionExecute().getActiveStateTxIdString();
						istream.printlnWithHeader(LanguageConnectionContext.xidStr + 
												  xactId + 
												  "), " +
												  LanguageConnectionContext.lccStr +
												  lcc.getInstanceNumber() +
												  "), " +
												  LanguageConnectionContext.dbnameStr +
												  lcc.getDbname() +
												  "), " +
												  LanguageConnectionContext.drdaStr +
												  lcc.getDrdaID() +
												  "), Error compiling prepared statement: " + 
												  getSource() +
												  " :End prepared statement");
					}
					throw se;
				}

				finally
				{
					/* Tell the data dictionary that we are done reading */
					if (dataDictionary != null)
					dataDictionary.doneReading(ddMode, lcc);
				}

				/* we need to move the commit of nested sub-transaction
				 * after we mark PS valid, during compilation, we might need
				 * to get some lock to synchronize with another thread's DDL
				 * execution, in particular, the compilation of insert/update/
				 * delete vs. create index/constraint (see Beetle 3976).  We
				 * can't release such lock until after we mark the PS valid.
				 * Otherwise we would just erase the DDL's invalidation when
				 * we mark it valid.
				 */
				try		// put in try block, commit sub-transaction if bad
				{
					if (SanityManager.DEBUG) 
					{
						if (SanityManager.DEBUG_ON("DumpOptimizedTree")) 
						{
							SanityManager.GET_DEBUG_STREAM().print(
								"\n\n============OPT===========\n\n");
							qt.treePrint();
							lcc.getPrintedObjectsMap().clear();
						}

						if (SanityManager.DEBUG_ON("StopAfterOptimizing")) 
						{
							throw StandardException.newException(SQLState.LANG_STOP_AFTER_OPTIMIZING);
						}
					}

					GeneratedClass ac = qt.generate(preparedStmt.getByteCodeSaver());

					generateTime = getCurrentTimeMillis(lcc);
					/* endTimestamp only meaningful if generateTime is meaningful.
					 * generateTime is meaningful if STATISTICS TIMING is ON.
					 */
					if (generateTime != 0)
					{
						endTimestamp = new Timestamp(generateTime);
					}

					if (SanityManager.DEBUG) 
					{
						if (SanityManager.DEBUG_ON("StopAfterGenerating")) 
						{
							throw StandardException.newException(SQLState.LANG_STOP_AFTER_GENERATING);
						}
					}

					/*
						copy over the compile-time created objects
						to the prepared statement.  This always happens
						at the end of a compile, so there is no need
						to erase the previous entries on a re-compile --
						this erases as it replaces.  Set the activation
						class in case it came from a StorablePreparedStatement
					*/
					preparedStmt.setConstantAction( qt.makeConstantAction() );
					preparedStmt.setSavedObjects( cc.getSavedObjects() );
					preparedStmt.setRequiredPermissionsList(cc.getRequiredPermissionsList());
                    preparedStmt.incrementVersionCounter();
					preparedStmt.setActivationClass(ac);
					preparedStmt.setNeedsSavepoint(qt.needsSavepoint());
					preparedStmt.setCursorInfo((CursorInfo)cc.getCursorInfo());
					preparedStmt.setIsAtomic(qt.isAtomic());
					preparedStmt.setExecuteStatementNameAndSchema(
												qt.executeStatementName(),
												qt.executeSchemaName()
												);
					preparedStmt.setSPSName(qt.getSPSName());
					preparedStmt.completeCompile(qt);
					preparedStmt.setCompileTimeWarnings(cc.getWarnings());

                    // Schedule updates of any stale index statistics we may
                    // have detected when creating the plan.
                    TableDescriptor[] tds = qt.updateIndexStatisticsFor();
                    if (tds.length > 0) {
                        IndexStatisticsDaemon isd = lcc.getDataDictionary().
                            getIndexStatsRefresher(true);
                        if (isd != null) {
                            for (int i=0; i < tds.length; i++) {
                                isd.schedule(tds[i]);
                            }
                        }
                    }
                }
				catch (StandardException e) 	// hold it, throw it
				{
					lcc.commitNestedTransaction();
					throw e;
				}

				if (lcc.getRunTimeStatisticsMode())
				{
					preparedStmt.setCompileTimeMillis(
						parseTime - beginTime, //parse time
						bindTime - parseTime, //bind time
						optimizeTime - bindTime, //optimize time
						generateTime - optimizeTime, //generate time
						generateTime - beginTime, //total compile time
						beginTimestamp,
						endTimestamp);
				}

			}
			finally // for block introduced by pushCompilerContext()
			{
				lcc.popCompilerContext( cc );
			}
		}
		catch (StandardException se)
		{
			if (foundInCache)
				((GenericLanguageConnectionContext)lcc).removeStatement(this);

			throw se;
		}
		finally
		{
			synchronized (preparedStmt) {
				preparedStmt.compilingStatement = false;
				preparedStmt.notifyAll();
			}
		}

		lcc.commitNestedTransaction();

		if (statementContext != null)
			lcc.popStatementContext(statementContext, null);

		return preparedStmt;
	}

    /** Walk the AST, using a (user-supplied) Visitor */
    private void walkAST( LanguageConnectionContext lcc, Visitable queryTree, int phase ) throws StandardException
    {
        ASTVisitor visitor = lcc.getASTVisitor();
        if ( visitor != null )
        {
            visitor.begin( statementText, phase );
            queryTree.accept( visitor );
            visitor.end( phase );
        }
    }

	/**
	 * Generates an execution plan given a set of named parameters.
	 * Does so for a storable prepared statement.
	 *
	 * @param	paramDefaults		Parameter defaults
	 *
	 * @return A PreparedStatement that allows execution of the execution
	 *	   plan.
	 * @exception StandardException	Thrown if this is an
	 *	   execution-only version of the module (the prepare() method
	 *	   relies on compilation).
	 */
	public	PreparedStatement prepareStorable(
				LanguageConnectionContext lcc,
				PreparedStatement ps,
				Object[]			paramDefaults,
				SchemaDescriptor	spsSchema,
				boolean internalSQL)
		throws StandardException
	{
		if (ps == null)
			ps = new GenericStorablePreparedStatement(this);
		else
			((GenericPreparedStatement) ps).statement = this;

		this.preparedStmt = (GenericPreparedStatement) ps;
		return prepMinion(lcc, false, paramDefaults, spsSchema, internalSQL);
	}

	public String getSource() {
		return statementText;
	}

	public String getCompilationSchema() {
		return compilationSchema.getDescriptorName();
	}

	private static long getCurrentTimeMillis(LanguageConnectionContext lcc)
	{
		if (lcc.getStatisticsTiming())
		{
			return System.currentTimeMillis();
		}
		else
		{
			return 0;
		}
	}

    /**
     * Return the {@link PreparedStatement} currently associated with this
     * statement.
     *
     * @return the prepared statement that is associated with this statement
     */
    public PreparedStatement getPreparedStatement() {
        return preparedStmt;
    }

	/*
	** Identity
	*/

	public boolean equals(Object other) {

		if (other instanceof GenericStatement) {

			GenericStatement os = (GenericStatement) other;

			return statementText.equals(os.statementText) && isForReadOnly==os.isForReadOnly
				&& compilationSchema.equals(os.compilationSchema) &&
				(prepareIsolationLevel == os.prepareIsolationLevel);
		}

		return false;
	}

	public int hashCode() {

		return statementText.hashCode();
	}
}
