/*

   Derby - Class org.apache.derby.impl.sql.GenericStatement

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

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import org.apache.derby.iapi.reference.JDBC20Translation;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.ParameterValueSet;

import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.sql.depend.Dependent;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.sql.compile.Parser;

import org.apache.derby.impl.sql.compile.QueryTreeNode;
import org.apache.derby.impl.sql.conn.GenericLanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.services.compiler.JavaFactory;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.util.ByteArray;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.loader.GeneratedClass;

import java.sql.Timestamp;
import java.sql.SQLWarning;

public class GenericStatement
	implements Statement {

	// these fields define the identity of the statement
	private final SchemaDescriptor compilationSchema;
	private final String			statementText;
	private int                      prepareIsolationLevel;
	private GenericPreparedStatement preparedStmt;

	/**
	 * Constructor for a Statement given the text of the statement in a String
	 *
	 * @param statementText	The text of the statement
	 */

	public GenericStatement(SchemaDescriptor compilationSchema, String statementText)
	{
		this.compilationSchema = compilationSchema;
		this.statementText = statementText;
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
		return prepMinion(lcc, true, (Object[]) null, (SchemaDescriptor) null, false); 
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
		 * relevant cloudscape property) then the value of cacheMe is irrelevant.
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
					throw StandardException.interrupt(ie);
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
				// param to true
				statementContext = lcc.pushStatementContext(true, getSource(), null, false);
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
				QueryTreeNode qt = p.parseStatement(statementText, paramDefaults);

				parseTime = getCurrentTimeMillis(lcc);

				if (SanityManager.DEBUG) 
				{
					if (SanityManager.DEBUG_ON("DumpParseTree")) 
					{
						qt.treePrint();
					}

					if (SanityManager.DEBUG_ON("StopAfterParsing")) 
					{
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

					qt = qt.bind();
					bindTime = getCurrentTimeMillis(lcc);

					if (SanityManager.DEBUG) 
					{
						if (SanityManager.DEBUG_ON("DumpBindTree")) 
						{
							qt.treePrint();
						}

						if (SanityManager.DEBUG_ON("StopAfterBinding")) {
							throw StandardException.newException(SQLState.LANG_STOP_AFTER_BINDING);
						}
					}

					qt = qt.optimize();

					optimizeTime = getCurrentTimeMillis(lcc);

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
					if (foundInCache)
						((GenericLanguageConnectionContext)lcc).removeStatement(this);


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
							qt.treePrint();
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
					preparedStmt.setActivationClass(ac);
					preparedStmt.setParams(cc.getParams());
					preparedStmt.setNeedsSavepoint(qt.needsSavepoint());
					preparedStmt.setCursorInfo((CursorInfo)cc.getCursorInfo());
					preparedStmt.setIsAtomic(qt.isAtomic());
					preparedStmt.setExecuteStatementNameAndSchema(
												qt.executeStatementName(),
												qt.executeSchemaName()
												);
					preparedStmt.setSPSName(qt.getSPSName());

					//if this statement is referencing session schema tables, then we do not want cache it. Following will remove the
					//entry that was made into the cache for this statement at the beginning of the compile phase
					if (preparedStmt.completeCompile(qt)) {
						if (foundInCache)
							((GenericLanguageConnectionContext)lcc).removeStatement(this);
					}

					preparedStmt.setCompileTimeWarnings(cc.getWarnings());
				}
				catch (StandardException e) 	// hold it, throw it
				{
					lcc.commitNestedTransaction();
					if (foundInCache)
						((GenericLanguageConnectionContext)lcc).removeStatement(this);
					throw e;
				}

				if (lcc.getRunTimeStatisticsMode())
				{
					preparedStmt.setCompileTimeMillis(
						parseTime - beginTime, //parse time
						bindTime - parseTime, //bind time
						optimizeTime - bindTime, //optimize time
						generateTime - optimizeTime, //generate time
						getElapsedTimeMillis(beginTime),
						beginTimestamp,
						endTimestamp);
				}

			}
			finally // for block introduced by pushCompilerContext()
			{
				lcc.popCompilerContext( cc );
			}
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

	/**
	 * Generates an execution plan given a set of named parameters.
	 * Does so for a storable prepared statement.
	 *
	 * @param 	compilationSchema	the schema to compile against
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

	public boolean getUnicode() {
		return true;
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

	private static long getElapsedTimeMillis(long beginTime)
	{
		if (beginTime != 0)
		{
			return System.currentTimeMillis() - beginTime;
		}
		else
		{
			return 0;
		}
	}

	/*
	** Identity
	*/

	public boolean equals(Object other) {

		if (other instanceof GenericStatement) {

			GenericStatement os = (GenericStatement) other;

			return statementText.equals(os.statementText)
				&& compilationSchema.equals(os.compilationSchema) &&
				(prepareIsolationLevel == os.prepareIsolationLevel);
		}

		return false;
	}

	public int hashCode() {

		return statementText.hashCode();
	}
}
