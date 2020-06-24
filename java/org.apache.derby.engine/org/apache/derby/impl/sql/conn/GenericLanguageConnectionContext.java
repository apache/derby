/*

   Derby - Class org.apache.derby.impl.sql.conn.GenericLanguageConnectionContext

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

package org.apache.derby.impl.sql.conn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.db.TriggerExecutionContext;
import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ContextId;
import org.apache.derby.shared.common.reference.Limits;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.shared.common.stream.HeaderPrintWriter;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.compile.ASTVisitor;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.OptTrace;
import org.apache.derby.iapi.sql.compile.OptimizerFactory;
import org.apache.derby.iapi.sql.compile.TypeCompilerFactory;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;
import org.apache.derby.iapi.sql.conn.SQLSessionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.dictionary.CheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.CursorActivation;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecutionStmtValidator;
import org.apache.derby.iapi.sql.execute.RunTimeStatistics;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.XATransactionController;
import org.apache.derby.iapi.transaction.TransactionControl;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.InterruptStatus;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.derby.impl.sql.GenericStatement;
import org.apache.derby.impl.sql.compile.CompilerContextImpl;
import org.apache.derby.impl.sql.execute.AutoincrementCounter;
import org.apache.derby.impl.sql.execute.DeferredConstraintsMemory.ValidationInfo;
import org.apache.derby.shared.common.sanity.SanityManager;
/**
 * LanguageConnectionContext keeps the pool of prepared statements,
 * activations, and cursors in use by the current connection.
 * <p>
 * The generic impl does not provide statement caching.
 *
 *
 */
public class GenericLanguageConnectionContext
    extends ContextImpl 
    implements LanguageConnectionContext
{

    // make sure these are not zeros
    private final static int NON_XA = 0;
    private final static int XA_ONE_PHASE = 1;
    private final static int XA_TWO_PHASE = 2;

    /*
        fields
     */

    private final ArrayList<Activation> acts;
    private volatile boolean unusedActs=false;
    /** The maximum size of acts since the last time it was trimmed. Used to
     * determine whether acts should be trimmed to reclaim space. */
    private int maxActsSize;
    protected int bindCount;
    private boolean ddWriteMode;
    private boolean runTimeStatisticsSetting ;
    private boolean statisticsTiming;

    /**
     * If xplainOnlyMode is set (via SYSCS_SET_XPLAIN_MODE), then the
     * connection does not actually execute statements, but only
     * compiles them, and emits the query plan information into the
     * XPLAIN tables.
     */
    private boolean xplainOnlyMode = false;
    
    /** the current xplain schema. Is usually NULL. Can be set via
     * SYSCS_SET_XPLAIN_SCHEMA, in which case it species the schema into
     * which XPLAIN information should be stored in user tables.
     */
    private String xplain_schema = null;
    /**
     * For each XPLAIN table, this map stores a SQL INSERT statement which
     * can be prepared and used to insert rows into the table during the
     * capturing of statistics data into the user XPLAIN tables.
     */
    private Map<Object,Object> xplain_statements = new HashMap<Object,Object>();
    
    //all the temporary tables declared for this connection
    private ArrayList<TempTableInfo> allDeclaredGlobalTempTables;

    //The currentSavepointLevel is used to provide the rollback behavior of 
    //temporary tables.  At any point, this variable has the total number of 
    //savepoints defined for the transaction.
    private int currentSavepointLevel = 0;

    protected long  nextCursorId;

    protected int   nextSavepointId;

    private RunTimeStatistics runTimeStatisticsObject;
    private StringBuffer sb;

    private Database db;

    private final int instanceNumber;
    private String drdaID;
    private String dbname;

    private Object lastQueryTree; // for debugging
    
    /**
    The transaction to use within this language connection context.  It may
    be more appropriate to have it in a separate context (LanguageTransactionContext?).
    REVISIT (nat): I shoehorned the transaction context that
    the language uses into this class.  The main purpose is so
    that the various language code can find out what its
    transaction is.
    **/
    private final TransactionController tran;

    /**
     * If non-null indicates that a read-only nested 
     * user transaction is in progress.
     */
    private TransactionController readOnlyNestedTransaction;
    
    /**
     * queryNestingDepth is a counter used to keep track of how many calls 
     * have been made to begin read-only nested transactions. Only the first call 
     * actually starts a Nested User Transaction with the store. Subsequent
     * calls simply increment this counter. commitNestedTransaction only
     * decrements the counter and when it drops to 0 actually commits the 
     * nested user transaction.
     */
    private int queryNestingDepth;

    protected DataValueFactory dataFactory;
    protected LanguageFactory langFactory;
    protected TypeCompilerFactory tcf;
    protected OptimizerFactory of;
    protected LanguageConnectionFactory connFactory;
    
    /* 
     * A statement context is "pushed" and "popped" at the beginning and
     * end of every statement so that only that statement is cleaned up
     * on a Statement Exception.  As a performance optimization, we only push
     * the outermost statement context once, and never pop it.  Also, we
     * save off a 2nd StatementContext for speeding server side method
     * invocation, though we still push and pop it as needed.  All other
     * statement contexts will allocated and pushed and popped on demand.
     */
    private final StatementContext[] statementContexts = new StatementContext[2];
    private int     statementDepth;
    protected int     outermostTrigger = -1;

    protected Authorizer authorizer;
    protected String userName = null; //The name the user connects with.
                                      //May still be quoted.
    /**
     * The top SQL session context stack frame (SQL 2003, section
     * 4.37.3), is kept in topLevelSSC. For nested session contexts,
     * the SQL session context is held by the activation of the
     * calling statement, cf. pushNestedSessionContext and it is
     * accessible through the current statement context
     * (compile-time), or via the current activation (execution-time).
     * @see GenericLanguageConnectionContext#getTopLevelSQLSessionContext
     */
    private SQLSessionContext topLevelSSC;

    /**
     * Used to hold the computed value of the initial default schema,
     * cf logic in initDefaultSchemaDescriptor.
     */
    private SchemaDescriptor cachedInitialDefaultSchemaDescr = null;

    // RESOLVE - How do we want to set the default.
    private int defaultIsolationLevel = TransactionControl.READ_COMMITTED_ISOLATION_LEVEL;
    protected int isolationLevel = defaultIsolationLevel;

    private boolean isolationLevelExplicitlySet = false;
    // Isolation level can be changed using JDBC api Connection.setTransactionIsolation
    // or it can be changed using SQL "set current isolation = NEWLEVEL".
    // 
    // In XA transactions, BrokeredConnection keeps isolation state information.
    // When isolation is changed in XA transaction using JDBC, that state gets
    // correctly set in BrokeredConnection.setTransactionIsolation method. But
    // when SQL is used to set the isolation level, the code path is different
    // and it does not go through BrokeredConnection's setTransactionIsolation
    // method and hence the state is not maintained correctly when coming through
    // SQL. To get around this, I am adding following flag which will get set
    // everytime the isolation level is set using JDBC or SQL. This flag will be
    // checked at global transaction start and end time. If the flag is set to true
    // then BrokeredConnection's isolation level state will be brought upto date
    // with Real Connection's isolation level and this flag will be set to false
    // after that.
    private boolean isolationLevelSetUsingSQLorJDBC = false;

    // isolation level to when preparing statements.
    // if unspecified, the statement won't be prepared with a specific 
    // scan isolationlevel
    protected int prepareIsolationLevel = TransactionControl.UNSPECIFIED_ISOLATION_LEVEL;

    // Whether or not to write executing statement info to db2j.log
    private boolean logStatementText;
    private boolean logQueryPlan;
    private HeaderPrintWriter istream;

    // this used to be computed in OptimizerFactoryContextImpl; i.e everytime a
    // connection was made. To keep the semantics same I'm putting it out here
    // instead of in the OptimizerFactory which is only initialized when the
    // database is booted.
    private int lockEscalationThreshold; 

    private ArrayList<ExecutionStmtValidator> stmtValidators;
    private ArrayList<TriggerExecutionContext> triggerExecutionContexts;
    private ArrayList <TableDescriptor> triggerTables;

    // OptimizerTrace
    private OptTrace    optimizerTracer;

    //// Support for AUTOINCREMENT

    /**
     * To support lastAutoincrementValue: This is a hashtable which maps
     * schemaName,tableName,columnName to a Long value.
     */
    private HashMap<String,Long> autoincrementHT;
    /**
     * whether to allow updates or not. 
     */
    private boolean autoincrementUpdate;
    private long identityVal;   //support IDENTITY_VAL_LOCAL function
    private boolean identityNotNull;    //frugal programmer

    // cache of ai being handled in memory (bulk insert + alter table).
    private HashMap<String,AutoincrementCounter> autoincrementCacheHashtable;

    // User-written inspector to print out query tree
    private ASTVisitor astWalker;

    /**
     * Interrupt status flag of this session's thread, in the form of an
     * exception created where an interrupt was (last) detected during operation,
     * null if no interrupt has been seen.
     */
    private StandardException interruptedException;

    /**
     * Connection local state for cached {@code TableDescriptor}s used
     * for keeping track of referenced columns for a table during DDL
     * operations.
     */
    private WeakHashMap<TableDescriptor,FormatableBitSet> referencedColumnMap;

    /**
     * The set of disk backed hash tables containing any index rows
     * saved for deferred constraints in this transaction, keyed by the
     * conglomerate id. Checked at commit time, then discarded.
     */
    private HashMap<UUID, ValidationInfo> deferredHashTables;

    /*
       constructor
    */
    public GenericLanguageConnectionContext
    (
     ContextManager cm,
     TransactionController tranCtrl,

     LanguageFactory lf,
     LanguageConnectionFactory lcf,
     Database db,
     String userName,
     int instanceNumber,
     String drdaID,
     String dbname)
         throws StandardException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        super(cm, org.apache.derby.shared.common.reference.ContextId.LANG_CONNECTION);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        acts = new ArrayList<Activation>();
        tran = tranCtrl;

        dataFactory = lcf.getDataValueFactory();
        tcf = lcf.getTypeCompilerFactory();
        of = lcf.getOptimizerFactory();
        langFactory =  lf;
        connFactory =  lcf;
        this.db = db;
        this.userName = userName;
        this.instanceNumber = instanceNumber;
        this.drdaID = drdaID;
        this.dbname = dbname;

        /* Find out whether or not to log info on executing statements to error log
         */
        String logStatementProperty = PropertyUtil.getServiceProperty(getTransactionCompile(),
                    "derby.language.logStatementText");
        logStatementText = Boolean.valueOf(logStatementProperty).booleanValue();

        String logQueryPlanProperty = PropertyUtil.getServiceProperty(getTransactionCompile(),
                    "derby.language.logQueryPlan");
        logQueryPlan = Boolean.valueOf(logQueryPlanProperty).booleanValue();

        setRunTimeStatisticsMode(logQueryPlan);

        lockEscalationThreshold = 
            PropertyUtil.getServiceInt(tranCtrl,
                                       Property.LOCKS_ESCALATION_THRESHOLD,
                                       Property.MIN_LOCKS_ESCALATION_THRESHOLD,
                                       Integer.MAX_VALUE,
                                       Property.DEFAULT_LOCKS_ESCALATION_THRESHOLD);                                                             
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        stmtValidators = new ArrayList<ExecutionStmtValidator>();
        triggerExecutionContexts = new ArrayList<TriggerExecutionContext>();
        triggerTables = new ArrayList<TableDescriptor>();
    }

    /**
     * In contrast to current user id, which may change (inside a routine
     * executing with definer's rights), the sessionUser is constant in a
     * session.
     */
    private String sessionUser = null;

    public void initialize() throws StandardException
    {
        interruptedException = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
        sessionUser = IdUtil.getUserAuthorizationId(userName);
        //
        //Creating the authorizer authorizes the connection.
        authorizer = new GenericAuthorizer(this);

        /*
        ** Set the authorization id.  User shouldn't
        ** be null or else we are going to blow up trying
        ** to create a schema for this user.
        */
        if (SanityManager.DEBUG)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
            if (getSessionUserId() == null)
            {
                SanityManager.THROWASSERT("User name is null," +
                    " check the connection manager to make sure it is set" +
                    " reasonably");
            }
        }

        setDefaultSchema(initDefaultSchemaDescriptor());
        referencedColumnMap = new WeakHashMap<TableDescriptor,FormatableBitSet>();
    }

    /**
     * Compute the initial default schema and set
     * cachedInitialDefaultSchemaDescr accordingly.
     *
     * @return computed initial default schema value for this session
     * @throws StandardException
     */
    protected SchemaDescriptor initDefaultSchemaDescriptor()
        throws StandardException {
        /*
        ** - If the database supports schemas and a schema with the
        ** same name as the user's name exists (has been created using
        ** create schema already) the database will set the users
        ** default schema to the the schema with the same name as the
        ** user.
        ** - Else Set the default schema to APP.
        */
        if (cachedInitialDefaultSchemaDescr == null) {
            DataDictionary dd = getDataDictionary();

            SchemaDescriptor sd =
                dd.getSchemaDescriptor(
                    getSessionUserId(), getTransactionCompile(), false);
//IC see: https://issues.apache.org/jira/browse/DERBY-4551

            if (sd == null) {
                sd = new SchemaDescriptor(
                    dd,
                    getSessionUserId(),
                    getSessionUserId(),
                    (UUID) null,
                    false);
            }

            cachedInitialDefaultSchemaDescr = sd;
        }
        return cachedInitialDefaultSchemaDescr;
    }

    /**
     * Get the computed value for the initial default schema.
     * @return the schema descriptor of the computed initial default schema
     */
    private SchemaDescriptor getInitialDefaultSchemaDescriptor() {
        return cachedInitialDefaultSchemaDescr;
    }
    

    //
    // LanguageConnectionContext interface
    //
    /**
     * @see LanguageConnectionContext#getLogStatementText
     */
    public boolean getLogStatementText()
    {
        return logStatementText;
    }

    /**
     * @see LanguageConnectionContext#setLogStatementText
     */
    public void setLogStatementText(boolean logStatementText)
    {
        this.logStatementText = logStatementText;
    }

    /**
     * @see LanguageConnectionContext#getLogQueryPlan
     */
    public boolean getLogQueryPlan()
    {
        return logQueryPlan;
    }

    /**
     * @see LanguageConnectionContext#usesSqlAuthorization
     */
    public boolean usesSqlAuthorization()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-464
        return getDataDictionary().usesSqlAuthorization();
    }

    /**
     * get the lock escalation threshold.
     */
    public int getLockEscalationThreshold()
    {
        return lockEscalationThreshold;
    }

    /**
     * Add the activation to those known about by this connection.
     */
    public void addActivation(Activation a) 
//IC see: https://issues.apache.org/jira/browse/DERBY-418
//IC see: https://issues.apache.org/jira/browse/DERBY-1142
        throws StandardException {
        acts.add(a);
//IC see: https://issues.apache.org/jira/browse/DERBY-2150

        if (acts.size() > maxActsSize) {
            maxActsSize = acts.size();
        }
    }

    public void closeUnusedActivations()
            throws StandardException
    {
        // DERBY-418. Activations which are marked unused,
        // are closed here. Activations Vector is iterated 
        // to identify and close unused activations, only if 
        // unusedActs flag is set to true and if the total 
        // size exceeds 20.
        if( (unusedActs) && (acts.size() > 20) ) {
            unusedActs = false;

            for (int i = acts.size() - 1; i >= 0; i--) {

                // it maybe the case that a Activation's reset() ends up
                // closing one or more activation leaving our index beyond
                // the end of the array
                if (i >= acts.size())
                    continue;

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
                Activation a1 = acts.get(i);
                if (!a1.isInUse()) {
                    a1.close();
                }
            }
        }

        if (SanityManager.DEBUG) {

            if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

                if (acts.size() > 20)
                    System.out.println("memoryLeakTrace:GenericLanguageContext:activations " + acts.size());
            }
        }
    }

    /**
     * Make a note that some activations are marked unused
     */
    public void notifyUnusedActivation() {
//IC see: https://issues.apache.org/jira/browse/DERBY-418
//IC see: https://issues.apache.org/jira/browse/DERBY-1142
        unusedActs = true;
    }

    /**
     * @see LanguageConnectionContext#checkIfAnyDeclaredGlobalTempTablesForThisConnection
     */
    public boolean checkIfAnyDeclaredGlobalTempTablesForThisConnection() {
        return (allDeclaredGlobalTempTables == null ? false : true);
    }

    /**
     * @see LanguageConnectionContext#addDeclaredGlobalTempTable
     */
    public void addDeclaredGlobalTempTable(TableDescriptor td)
        throws StandardException 
    {

        if (findDeclaredGlobalTempTable(td.getName()) != null) 
        {
            //if table already declared, throw an exception
            throw StandardException.newException(
                SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
                "Declared global temporary table",
                td.getName(),
                "Schema",
                SchemaDescriptor.STD_DECLARED_GLOBAL_TEMPORARY_TABLES_SCHEMA_NAME);
        }

        //save all the information about temp table in this special class
        TempTableInfo tempTableInfo = 
            new TempTableInfo(td, currentSavepointLevel);

        // Rather than exist in a catalog, a simple array is kept of the 
        // tables currently active in the transaction.

        if (allDeclaredGlobalTempTables == null)
            allDeclaredGlobalTempTables = new ArrayList<TempTableInfo>();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

        allDeclaredGlobalTempTables.add(tempTableInfo);
    }

    /**
     * @see LanguageConnectionContext#dropDeclaredGlobalTempTable
     */
    public boolean dropDeclaredGlobalTempTable(String tableName) 
    {
        TempTableInfo tempTableInfo = findDeclaredGlobalTempTable(tableName);

        if (tempTableInfo != null)
        {
            if (SanityManager.DEBUG)
            {
                if (tempTableInfo.getDeclaredInSavepointLevel() > 
                        currentSavepointLevel)
                {
                    SanityManager.THROWASSERT(
                        "declared in savepoint level (" + 
                            tempTableInfo.getDeclaredInSavepointLevel() + 
                        ") can not be higher than current savepoint level (" +
                            currentSavepointLevel +
                        ").");
                }
            }

            // check if the table was declared in the current unit of work.
            if (tempTableInfo.getDeclaredInSavepointLevel() == 
                    currentSavepointLevel)
            {
                // since the table was declared in this unit of work, the drop 
                // table method should remove it from the valid list of temp 
                // table for this unit of work
                allDeclaredGlobalTempTables.remove(
                        allDeclaredGlobalTempTables.indexOf(tempTableInfo));

//IC see: https://issues.apache.org/jira/browse/DERBY-673
                if (allDeclaredGlobalTempTables.isEmpty()) {
                    allDeclaredGlobalTempTables = null;
                }
            }
            else
            {
                // since the table was not declared in this unit of work, the
                // drop table method will just mark the table as dropped
                // in the current unit of work. This information will be used 
                // at rollback time.

                tempTableInfo.setDroppedInSavepointLevel(currentSavepointLevel);
            }

            return true;
        } 
        else
        {
            return false;
        }
    }

    /**
     * After a release of a savepoint, we need to go through our temp tables 
     * list. If there are tables with their declare or drop or modified in 
     * savepoint levels set to savepoint levels higher than the current 
     * savepoint level, then we should change them to the current savepoint
     * level
     */
    private void tempTablesReleaseSavepointLevels() 
    {
        // unlike rollback, here we check for dropped in / declared in / 
        // modified in savepoint levels > current savepoint level only.
        // This is because the temp tables with their savepoint levels same as 
        // currentSavepointLevel have correct value assigned to them and
        // do not need to be changed and hence no need to check for >=

        for (int i = 0; i < allDeclaredGlobalTempTables.size(); i++) 
        {
            TempTableInfo tempTableInfo = 
                allDeclaredGlobalTempTables.get(i);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

            if (tempTableInfo.getDroppedInSavepointLevel() > 
                    currentSavepointLevel)
            {
                tempTableInfo.setDroppedInSavepointLevel(currentSavepointLevel);
            }

            if (tempTableInfo.getDeclaredInSavepointLevel() > 
                    currentSavepointLevel)
            {
                tempTableInfo.setDeclaredInSavepointLevel(
                    currentSavepointLevel);
            }

            if (tempTableInfo.getModifiedInSavepointLevel() > 
                    currentSavepointLevel)
            {
                tempTableInfo.setModifiedInSavepointLevel(
                    currentSavepointLevel);
            }
        }
    }

    /**
     * Do the necessary work at commit time for temporary tables
     * <p>
     * 1)If a temporary table was marked as dropped in this transaction, then 
     *   remove it from the list of temp tables for this connection
     * 2)If a temporary table was not dropped in this transaction, then mark 
     *   it's declared savepoint level and modified savepoint level as -1
     * 3)After savepoint fix up, then handle all ON COMMIT DELETE ROWS with
     *   no open held cursor temp tables.
     * <p>
     *
     * @param in_xa_transaction if true, then transaction is an XA transaction,
     *                          and special nested transaction may be necessary
     *                          to cleanup internal containers supporting the
     *                          temp tables at commit time.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    private void tempTablesAndCommit(boolean in_xa_transaction) 
        throws StandardException
    {
        // loop through all declared global temporary tables and determine
        // what to do at commit time based on if they were dropped during
        // the current savepoint level.
        for (int i = allDeclaredGlobalTempTables.size()-1; i >= 0; i--) 
        {
            TempTableInfo tempTableInfo = 
                allDeclaredGlobalTempTables.get(i);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

            if (tempTableInfo.getDroppedInSavepointLevel() != -1)
            {
                // this means table was dropped in this unit of work and hence 
                // should be removed from valid list of temp tables

                allDeclaredGlobalTempTables.remove(i);
            } 
            else 
            {
                //this table was not dropped in this unit of work, hence set 
                //its declaredInSavepointLevel as -1 and also mark it as not 
                //modified 

                tempTableInfo.setDeclaredInSavepointLevel(-1);
                tempTableInfo.setModifiedInSavepointLevel(-1);
            }
        }

        // at commit time, for all the temp tables declared with 
        // ON COMMIT DELETE ROWS, make sure there are no held cursor open
        // on them.
        // If there are no held cursors open on ON COMMIT DELETE ROWS, 
        // drop those temp tables and redeclare them to get rid of all the 
        // data in them

        // in XA use nested user updatable transaction.  Delay creating
        // the transaction until loop below finds one it needs to 
        // process.
        
        for (int i=0; i<allDeclaredGlobalTempTables.size(); i++)
        {
            TableDescriptor td = 
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
                allDeclaredGlobalTempTables.
                                      get(i).getTableDescriptor();
            if (td.isOnCommitDeleteRows() == false) 
            {
                // do nothing for temp table with ON COMMIT PRESERVE ROWS
                continue;
            }
            else if (checkIfAnyActivationHasHoldCursor(td.getName()) == 
                        false)
            {
                // temp tables with ON COMMIT DELETE ROWS and 
                // no open held cursors
                getDataDictionary().getDependencyManager().invalidateFor(
                    td, DependencyManager.DROP_TABLE, this);

                if (!in_xa_transaction)
                {
                    // delay physical cleanup to after the commit for XA
                    // transactions.   In XA the transaction is likely in
                    // prepare state at this point and physical changes to
                    // store are not allowed until after the commit.
                    // Do the work here for non-XA so that fast path does
                    // have to do the 2 commits that the XA path will.
                    cleanupTempTableOnCommitOrRollback(td, true);
                }
            }
        }
    }

    private void tempTablesXApostCommit() 
        throws StandardException
    {
        TransactionController tc = getTransactionExecute();

        // at commit time for an XA transaction drop all temporary tables.
        // A transaction context may not be maintained from one
        // XAResource.xa_commit to the next in the case of XA with
        // network server and thus there is no way to get at the temp
        // tables again.  To provide consistent behavior in embedded vs
        // network server, consistently remove temp tables at XA commit
        // transaction boundary.
        for (int i=0; i < allDeclaredGlobalTempTables.size(); i++)
        {
            // remove all temp tables from this context.
            TableDescriptor td = 
                allDeclaredGlobalTempTables.get(i).getTableDescriptor();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

            //remove the conglomerate created for this temp table
            tc.dropConglomerate(td.getHeapConglomerateId()); 

            //remove it from the list of temp tables
            allDeclaredGlobalTempTables.remove(i); 
        }

        tc.commit();
    }

    /**
        Reset the connection before it is returned (indirectly) by
        a PooledConnection object. See EmbeddedConnection.
     */
    public void resetFromPool()
         throws StandardException
    {
        interruptedException = null;

        // Reset IDENTITY_VAL_LOCAL
        identityNotNull = false;

        // drop all temp tables.
        dropAllDeclaredGlobalTempTables();

        // Reset the current schema (see DERBY-3690).
        setDefaultSchema(null);
//IC see: https://issues.apache.org/jira/browse/DERBY-3690

        // Reset the current role
        getCurrentSQLSessionContext().setRole(null);
//IC see: https://issues.apache.org/jira/browse/DERBY-3859

        // Reset the current user
        getCurrentSQLSessionContext().setUser(getSessionUserId());
//IC see: https://issues.apache.org/jira/browse/DERBY-4551

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        referencedColumnMap = new WeakHashMap<TableDescriptor,FormatableBitSet>();
    }

    // debug methods
    public  void    setLastQueryTree( Object queryTree ) { lastQueryTree = queryTree; }
    public  Object    getLastQueryTree() { return lastQueryTree; }

    /**
     * Drop all the declared global temporary tables associated with this 
     * connection. This gets called when a getConnection() is done on a 
     * PooledConnection. This will ensure all the temporary tables declared on
     * earlier connection handle associated with this physical database 
     * connection are dropped before a new connection handle is issued on that 
     * same physical database connection.
     */
    private void dropAllDeclaredGlobalTempTables() throws StandardException 
    {
        if (allDeclaredGlobalTempTables == null)
            return;
    
        DependencyManager dm = getDataDictionary().getDependencyManager();
        StandardException topLevelStandardException = null;

        // collect all the exceptions we might receive while dropping the 
        // temporary tables and throw them as one chained exception at the end.
        for (int i = 0; i < allDeclaredGlobalTempTables.size(); i++) 
        {
            try 
            {
                TempTableInfo tempTableInfo = 
                    allDeclaredGlobalTempTables.get(i);

                TableDescriptor td = tempTableInfo.getTableDescriptor();

                // the following 2 lines of code has been copied from 
                // DropTableConstantAction. If there are any changes made there
                // in future, we should check if they need to be made here too.
                dm.invalidateFor(td, DependencyManager.DROP_TABLE, this);
                tran.dropConglomerate(td.getHeapConglomerateId());

            } 
            catch (StandardException e) 
            {
                if (topLevelStandardException == null) 
                {
                    // always keep the first exception unchanged
                    topLevelStandardException = e;
                } 
                else 
                {
                    try 
                    {
                        // Try to create a chain of exceptions. If successful,
                        // the current exception is the top-level exception,
                        // and the previous exception the cause of it.
                        e.initCause(topLevelStandardException);
                        topLevelStandardException = e;
                    } 
                    catch (IllegalStateException ise) 
                    {
                        // initCause() has already been called on e. We don't
                        // expect this to happen, but if it happens, just skip
                        // the current exception from the chain. This is safe
                        // since we always keep the first exception.
                    }
                }
            }
        }
    
        allDeclaredGlobalTempTables = null;
        try 
        {
            internalCommit(true);
        } 
        catch (StandardException e) 
        {
            // do the same chaining as above
            if (topLevelStandardException == null) 
            {
                topLevelStandardException = e;
            } 
            else 
            {
                try 
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
                    e.initCause(topLevelStandardException);
                    topLevelStandardException = e;
                } 
                catch (IllegalStateException ise) 
                { 
                    /* ignore */ 
                }
            }
        }

        if (topLevelStandardException != null) 
            throw topLevelStandardException;
    }

    /**
     * do the necessary work at rollback time for temporary tables
     * 1)If a temp table was declared in the UOW, then drop it and remove it 
     *   from list of temporary tables.
     * 2)If a temp table was declared and dropped in the UOW, then remove it 
     *   from list of temporary tables.
     * 3)If an existing temp table was dropped in the UOW, then recreate it 
     *   with no data.
     * 4)If an existing temp table was modified in the UOW, then get rid of 
     *   all the rows from the table.
     */
    private void tempTablesAndRollback()
        throws StandardException 
    {
        for (int i = allDeclaredGlobalTempTables.size()-1; i >= 0; i--) 
        {
            TempTableInfo tempTableInfo = 
                allDeclaredGlobalTempTables.get(i);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

            if (tempTableInfo.getDeclaredInSavepointLevel() >= 
                    currentSavepointLevel)
            {
                if (tempTableInfo.getDroppedInSavepointLevel() == -1)
                {
                    // the table was declared but not dropped in the unit of 
                    // work getting rolled back and hence we will remove it 
                    // from valid list of temporary tables and drop the 
                    // conglomerate associated with it

                    TableDescriptor td = tempTableInfo.getTableDescriptor();
                    invalidateCleanupDroppedTable( td );

                    //remove the conglomerate created for this temp table
                    tran.dropConglomerate(td.getHeapConglomerateId()); 

                    //remove it from the list of temp tables
                    allDeclaredGlobalTempTables.remove(i); 

                } 
                else if (tempTableInfo.getDroppedInSavepointLevel() >= 
                            currentSavepointLevel)
                {
                    // the table was declared and dropped in the unit of work 
                    // getting rolled back
                    allDeclaredGlobalTempTables.remove(i);
                }
            } 
            else if (tempTableInfo.getDroppedInSavepointLevel() >= 
                        currentSavepointLevel) 
            {
                // this means the table was declared in an earlier savepoint 
                // unit / transaction and then dropped in current UOW 

                // restore the old definition of temp table because drop is 
                // being rolledback
                TableDescriptor td = tempTableInfo.getTableDescriptor();
                td = cleanupTempTableOnCommitOrRollback(td, false);

                // In order to store the old conglomerate information for the 
                // temp table, we need to replace the existing table descriptor
                // with the old table descriptor which has the old conglomerate 
                // information
                tempTableInfo.setTableDescriptor(td);
                tempTableInfo.setDroppedInSavepointLevel(-1);

                // following will mark the table as not modified. This is 
                // because the table data has been deleted as part of the 
                // current rollback
                tempTableInfo.setModifiedInSavepointLevel(-1);
                allDeclaredGlobalTempTables.set(i, tempTableInfo);
                
            } 
            else if (tempTableInfo.getModifiedInSavepointLevel() >= 
                    currentSavepointLevel) 
            {
                // this means the table was declared in an earlier savepoint 
                // unit / transaction and modified in current UOW

                // following will mark the table as not modified. This is 
                // because the table data will be deleted as part of the 
                // current rollback
                tempTableInfo.setModifiedInSavepointLevel(-1);
                TableDescriptor td = tempTableInfo.getTableDescriptor();

                invalidateCleanupDroppedTable( td );
            } 
            // there is no else here because there is no special processing 
            // required for temp tables declares in earlier work of 
            // unit/transaction and not modified
        }
    
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        if (allDeclaredGlobalTempTables.isEmpty())
        {
            allDeclaredGlobalTempTables = null;
        }
    }

    /** Invalidate a dropped temp table */
    private void    invalidateCleanupDroppedTable( TableDescriptor td )
        throws StandardException
    {
        getDataDictionary().getDependencyManager().invalidateFor( td, DependencyManager.DROP_TABLE, this );
        cleanupTempTableOnCommitOrRollback( td, true );
    }

    /**
     * This is called at the commit time for temporary tables with 
     * ON COMMIT DELETE ROWS
     *
     * If a temp table with ON COMMIT DELETE ROWS doesn't have any held cursor 
     * open on them, we delete the data from them by dropping the conglomerate 
     * and recreating the conglomerate. In order to store the new conglomerate
     * information for the temp table, we need to replace the existing table 
     * descriptor with the new table descriptor which has the new conglomerate
     * information
     *
     * @param tableName Temporary table name whose table descriptor is 
     *                  getting changed
     * @param td        New table descriptor for the temporary table
     */
    private void replaceDeclaredGlobalTempTable(
    String          tableName, 
    TableDescriptor td) 
    {
        TempTableInfo tempTableInfo = findDeclaredGlobalTempTable(tableName);
        
        tempTableInfo.setDroppedInSavepointLevel(-1);
        tempTableInfo.setDeclaredInSavepointLevel(-1);
        tempTableInfo.setTableDescriptor(td);

        allDeclaredGlobalTempTables.set(
            allDeclaredGlobalTempTables.indexOf(tempTableInfo), tempTableInfo);
    }

    /**
     * @see LanguageConnectionContext#getTableDescriptorForDeclaredGlobalTempTable
     */
    public TableDescriptor getTableDescriptorForDeclaredGlobalTempTable(String tableName) {
    TempTableInfo tempTableInfo = findDeclaredGlobalTempTable(tableName);
        if (tempTableInfo == null)
            return null;
        else
            return tempTableInfo.getTableDescriptor();
    }

    /**
     * Find the declared global temporary table in the list of temporary tables known by this connection.
     * @param tableName look for this table name in the saved list
     * @return data structure defining the temporary table if found. Else, return null 
     *
     */
    private TempTableInfo findDeclaredGlobalTempTable(String tableName) {
        if (allDeclaredGlobalTempTables == null)
            return null;

        for (int i = 0; i < allDeclaredGlobalTempTables.size(); i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            if ((allDeclaredGlobalTempTables.get(i)).matches(tableName))
                return allDeclaredGlobalTempTables.get(i);
        }
        return null;
    }

    /**
     * @see LanguageConnectionContext#markTempTableAsModifiedInUnitOfWork
     */
    public void markTempTableAsModifiedInUnitOfWork(String tableName) {
    TempTableInfo tempTableInfo = findDeclaredGlobalTempTable(tableName);
    tempTableInfo.setModifiedInSavepointLevel(currentSavepointLevel);
    }

        /**
     * @see LanguageConnectionContext#prepareInternalStatement
     */
        public PreparedStatement prepareInternalStatement(SchemaDescriptor compilationSchema, String sqlText, boolean isForReadOnly, boolean forMetaData) 
//IC see: https://issues.apache.org/jira/browse/DERBY-231
        throws StandardException 
        {
            if (forMetaData) {
                //DERBY-2946
                //Make sure that metadata queries always run with SYS as 
                //compilation schema. This will make sure that the collation
                //type of character string constants will be UCS_BASIC which
                //is also the collation of character string columns belonging
                //to system tables. 
                compilationSchema = getDataDictionary().getSystemSchemaDescriptor(); 
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-573
        return connFactory.getStatement(compilationSchema, sqlText, isForReadOnly).prepare(this, forMetaData);
        }

        /**
         * @see LanguageConnectionContext#prepareInternalStatement
         */
        public PreparedStatement prepareInternalStatement(String sqlText) 
        throws StandardException 
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-3327
//IC see: https://issues.apache.org/jira/browse/DERBY-1331
            return connFactory.
                getStatement(getDefaultSchema(), sqlText, true).prepare(this);
        }      

    /**
     * Remove the activation to those known about by this connection.
     *
     */
    public void removeActivation(Activation a) 
    {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(a.isClosed(), "Activation is not closed");
        }

        acts.remove(a);
//IC see: https://issues.apache.org/jira/browse/DERBY-2150

        if (maxActsSize > 20 && (maxActsSize > 2 * acts.size())) {
            acts.trimToSize();
            maxActsSize = acts.size();
        }
    }

    /**
     * Return the number of activations known for this connection.
     * Note that some of these activations may not be in use
     * (when a prepared statement is finalized, its activations
     * are marked as unused and later closed and removed on
     * the next commit/rollback).
     */
    public int getActivationCount() {
        return acts.size();
    }

    /**
     * See if a given cursor is available for use.
     * if so return its activation. Returns null if not found.
     * For use in execution.
     *
     * @return the activation for the given cursor, null
     *  if none was found.
     */
    public CursorActivation lookupCursorActivation(String cursorName) {

        int size = acts.size();
        if (size > 0)
        {
            int cursorHash = cursorName.hashCode();

            for (int i = 0; i < size; i++) {
                 Activation a = acts.get(i);

                 if (!a.isInUse())
                 {
                    continue;
                 }



                String executingCursorName = a.getCursorName();

                // If the executing cursor has no name, or if the hash code of
                // its name is different from the one we're looking for, it
                // can't possibly match. Since java.lang.String caches the
                // hash code (at least in the most common implementations),
                // checking the hash code is cheaper than comparing the names
                // with java.lang.String.equals(), especially if there are many
                // open statements associated with the connection. See
                // DERBY-3882. Note that we can only use the hash codes to
                // determine that the names don't match. Even if the hash codes
                // are equal, we still need to call equals() to verify that the
                // two names actually are equal.
                if (executingCursorName == null ||
                        executingCursorName.hashCode() != cursorHash) {
                    continue;
                }

                 if (cursorName.equals(executingCursorName)) {

                    ResultSet rs = a.getResultSet();
                    if (rs == null)
                        continue;

                     // if the result set is closed, the the cursor doesn't exist
                     if (rs.isClosed()) {                   
                        continue;
                     }

                    return (CursorActivation)a;
                 }
            }
        }
        return null;
    }

    /**
    *  This method will remove a statement from the  statement cache.
    *  It should only be called if there is an exception preparing
    *  the statement. The caller must have set the flag
    *  {@code preparedStmt.compilingStatement} in the {@code GenericStatement}
    *  before calling this method in order to prevent race conditions when
    *  calling {@link CacheManager#remove(Cacheable)}.
    *
    *  @param statement Statement to remove
    *  @exception StandardException thrown if lookup goes wrong.
    */  
    public void removeStatement(GenericStatement statement)
        throws StandardException {
        
        CacheManager statementCache =
            getLanguageConnectionFactory().getStatementCache();

        if (statementCache == null)
            return;
 
        Cacheable cachedItem = statementCache.findCached(statement);
        // No need to do anything if the statement is already removed
        if (cachedItem != null) {
            CachedStatement cs = (CachedStatement) cachedItem;
            if (statement.getPreparedStatement() != cs.getPreparedStatement()) {
                // DERBY-3786: Someone else has removed the statement from
                // the cache, probably because of the same error that brought
                // us here. In addition, someone else has recreated the
                // statement. Since the recreated statement is not the same
                // object as the one we are working on, we don't have the
                // proper guarding (through the synchronized flag
                // GenericStatement.preparedStmt.compilingStatement) to ensure
                // that we're the only ones calling CacheManager.remove() on
                // this statement. Therefore, just release the statement here
                // so that we don't get in the way for the other thread that
                // is trying to compile the same query.
                statementCache.release(cachedItem);
            } else {
                // The statement object that we were trying to compile is still
                // in the cache. Since the compilation failed, remove it.
                statementCache.remove(cachedItem);
            }
        }
    }

    /**
     * See if a given statement has already been compiled for this user, and
     * if so use its prepared statement. Returns null if not found.
     *
     * @exception StandardException thrown if lookup goes wrong.
     * @return the prepared statement for the given string, null
     *  if none was found.
     */
    public PreparedStatement lookupStatement(GenericStatement statement)
        throws StandardException {

//IC see: https://issues.apache.org/jira/browse/DERBY-2772
//IC see: https://issues.apache.org/jira/browse/DERBY-2772
        CacheManager statementCache =
            getLanguageConnectionFactory().getStatementCache();
            
        if (statementCache == null)
            return null;

        // statement caching disable when in DDL mode
        if (dataDictionaryInWriteMode()) {
            return null;
        }

        Cacheable cachedItem = statementCache.find(statement);

        CachedStatement cs = (CachedStatement) cachedItem;


        GenericPreparedStatement ps = cs.getPreparedStatement();

        synchronized (ps) {
            if (ps.upToDate()) {
                GeneratedClass ac = ps.getActivationClass();

                // Check to see if the statement was prepared before some change
                // in the class loading set. If this is the case then force it to be invalid
                int currentClasses =
                        getLanguageConnectionFactory().getClassFactory().getClassLoaderVersion();

                if (ac.getClassLoaderVersion() != currentClasses) {
                    ps.makeInvalid(DependencyManager.INTERNAL_RECOMPILE_REQUEST, this);
                }

                // note that the PreparedStatement is not kept in the cache. This is because
                // having items kept in the cache that ultimately are held onto by
                // user code is impossible to manage. E.g. an open ResultSet would hold onto
                // a PreparedStatement (through its activation) and the user can allow
                // this object to be garbage collected. Pushing a context stack is impossible
                // in garbage collection as it may deadlock with the open connection and
                // the context manager assumes a singel current thread per context stack
            }
        }

        statementCache.release(cachedItem);
        return ps;
    }

    /**
        Get a connection unique system generated name for a cursor.
    */
    public String getUniqueCursorName() 
    {
        return getNameString("SQLCUR", nextCursorId++);
    }

    /**
        Get a connection unique system generated name for an unnamed savepoint.
    */
    public String getUniqueSavepointName()
    {
        return getNameString("SAVEPT", nextSavepointId++);
    }

    /**
        Get a connection unique system generated id for an unnamed savepoint.
    */
    public int getUniqueSavepointID()
    {
        return nextSavepointId-1;
    }

    /**
     * Build a String for a statement name.
     *
     * @param prefix    The prefix for the statement name.
     * @param number    The number to append for uniqueness
     *
     * @return  A unique String for a statement name.
     */
    private String getNameString(String prefix, long number)
    {
        if (sb != null)
        {
            sb.setLength(0);
        }
        else
        {
            sb = new StringBuffer();
        }
        sb.append(prefix).append(number);

        return sb.toString();
    }

    /**
     * Do a commit as appropriate for an internally generated
     * commit (e.g. as needed by sync, or autocommit).
     *
     * @param   commitStore true if we should commit the Store transaction
     *
     * @exception StandardException thrown on failure
     */
    public void internalCommit(boolean commitStore) throws StandardException
    {
        doCommit(commitStore, true, NON_XA, false);
    }

    /**
     * Do a commmit as is appropriate for a user requested
     * commit (e.g. a java.sql.Connection.commit() or a language
     * 'COMMIT' statement.  Does some extra checking to make
     * sure that users aren't doing anything bad.
     *
     * @exception StandardException thrown on failure
     */
    public void userCommit() throws StandardException
    {
        doCommit(true, true, NON_XA, true);
    }


    /**
        Commit the language transaction by doing a commitNoSync()
        on the store's TransactionController.

        <p>
        Do *NOT* tell the data dictionary that the transaction is
        finished. The reason is that this would allow other transactions
        to see comitted DDL that could be undone in the event of a
        system crash.

        @param  commitflag  the flags to pass to commitNoSync in the store's
                            TransactionController

        @exception StandardException thrown on failure
     */
    public final void internalCommitNoSync(int commitflag) 
        throws StandardException
    {
        doCommit(true, false, commitflag, false);
    }


    /**
        Same as userCommit except commit a distributed transaction.   
        This commit always commit store and sync the commit.

        @param onePhase if true, allow it to commit without first going thru a
        prepared state.

        @exception StandardException    thrown if something goes wrong
     */
    public final void xaCommit(boolean onePhase) throws StandardException
    {
        // further overload internalCommit to make it understand 2 phase commit
        doCommit(true /* commit store */,
                                  true /* sync */,
                                  onePhase ? XA_ONE_PHASE : XA_TWO_PHASE,
                                  true);
    }


    /**
     * This is where the work on internalCommit(), userCOmmit() and 
     * internalCommitNoSync() actually takes place.
     * <p>
     * When a commit happens, the language connection context
     * will close all open activations/cursors and commit the
     * Store transaction.
     * <p>
     * REVISIT: we talked about having a LanguageTransactionContext,
     * but since store transaction management is currently in flux
     * and our context might want to delegate to that context,
     * for now all commit/rollback actions are handled directly by
     * the language connection context.
     * REVISIT: this may need additional alterations when
     * RELEASE SAVEPOINT/ROLLBACK TO SAVEPOINT show up.
     * <P>
     * Since the access manager's own context takes care of its own
     * resources on commit, and the transaction stays open, there is
     * nothing that this context has to do with the transaction controller.
     * <p>
     * Also, tell the data dictionary that the transaction is finished,
     * if necessary (that is, if the data dictionary was put into
     * DDL mode in this transaction.
     *
     *
     * @param   commitStore true if we should commit the Store transaction
     * @param   sync        true means do a synchronized commit,
     *                      false means do an unsynchronized commit
     * @param   commitflag  if this is an unsynchronized commit, the flags to
     *                      pass to commitNoSync in the store's
     *                      TransactionController.  If this is a synchronized
     *                      commit, this flag is overloaded for xacommit.
     * @param   requestedByUser    False iff the commit is for internal use and
     *                      we should ignore the check to prevent commits
     *                      in an atomic statement.
     *
     * @exception StandardException     Thrown on error
     */

    protected void doCommit(boolean commitStore,
                                               boolean sync,
                                               int commitflag,
                                               boolean requestedByUser)
         throws StandardException
    {
        StatementContext statementContext = getStatementContext();

        if (requestedByUser  &&
            (statementContext != null) &&
            statementContext.inUse() &&
            statementContext.isAtomic())
        {
            throw StandardException.newException(
                    SQLState.LANG_NO_COMMIT_IN_NESTED_CONNECTION);
        }

        checkIntegrity();
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419

        // Log commit to error log, if appropriate
        if (logStatementText)
        {
            if (istream == null)
            {
                istream = Monitor.getStream();
            }
            String xactId = tran.getTransactionIdString();
            istream.printlnWithHeader(
                LanguageConnectionContext.xidStr + 
                xactId + 
                "), " +
                LanguageConnectionContext.lccStr +
                instanceNumber +
                "), " + LanguageConnectionContext.dbnameStr +
                dbname +
                "), " +
                LanguageConnectionContext.drdaStr +
                drdaID +
                "), Committing");
        }

        endTransactionActivationHandling(false);

        // Do clean up work required for temporary tables at commit time.  
        if (allDeclaredGlobalTempTables != null)
        {
            tempTablesAndCommit(commitflag != NON_XA);
        }

        //reset the current savepoint level for the connection to 0 at the end 
        //of commit work for temp tables
        currentSavepointLevel = 0; 

        // Do *NOT* tell the DataDictionary to start using its cache again
        // if this is an unsynchronized commit. The reason is that it
        // would allow other transactions to see this transaction's DDL,
        // which could be rolled back in case of a system crash.
        if (sync)
        {
            finishDDTransaction();
        }
        
        // Check that any nested transaction has been destoyed
        // before a commit.
        if (SanityManager.DEBUG)
        {
            if (readOnlyNestedTransaction != null)
            {
                SanityManager.THROWASSERT("Nested transaction active!");
            }
        }

        // now commit the Store transaction
        TransactionController tc = getTransactionExecute();
        if ( tc != null && commitStore ) 
        { 
            if (sync)
            {
                if (commitflag == NON_XA)
                {
                    // regular commit
                    tc.commit();
                }
                else
                {
                    // This may be a xa_commit, check overloaded commitflag.

                    if (SanityManager.DEBUG)
                        SanityManager.ASSERT(commitflag == XA_ONE_PHASE ||
                                             commitflag == XA_TWO_PHASE,
                                               "invalid commit flag");

                    ((XATransactionController)tc).xa_commit(
                            commitflag == XA_ONE_PHASE);

                }
            }
            else
            {
                tc.commitNoSync(commitflag);
            }

            // reset the savepoints to the new
            // location, since any outer nesting
            // levels expect there to be a savepoint
            resetSavepoints();

            // Do post commit XA temp table cleanup if necessary.
            if ((allDeclaredGlobalTempTables != null) &&
                (commitflag != NON_XA))
            {
                tempTablesXApostCommit();
            }
        }
    }

    /**
     * If dropAndRedeclare is true, that means we have come here for temp 
     * tables with on commit delete rows and no held curosr open on them. We 
     * will drop the existing conglomerate and redeclare a new conglomerate
     * similar to old conglomerate. This is a more efficient way of deleting 
     * all rows from the table.
     *
     * If dropAndRedeclare is false, that means we have come here for the 
     * rollback cleanup work. We are trying to restore old definition of the 
     * temp table (because the drop on it is being rolled back).
     */
    private TableDescriptor cleanupTempTableOnCommitOrRollback(
    TableDescriptor         td, 
    boolean                 dropAndRedeclare)
         throws StandardException
    {
        TransactionController tc = getTransactionExecute();

        //create new conglomerate with same properties as the old conglomerate 
        //and same row template as the old conglomerate
        long conglomId = 
            tc.createConglomerate(
                "heap", // we're requesting a heap conglomerate
//IC see: https://issues.apache.org/jira/browse/DERBY-2661
                td.getEmptyExecRow().getRowArray(), // row template
                null, //column sort order - not required for heap
                td.getColumnCollationIds(),  // same ids as old conglomerate
                null, // properties
                (TransactionController.IS_TEMPORARY | 
                 TransactionController.IS_KEPT));

        long cid = td.getHeapConglomerateId();

        //remove the old conglomerate descriptor from the table descriptor
        ConglomerateDescriptor cgd = td.getConglomerateDescriptor(cid);
        td.getConglomerateDescriptorList().dropConglomerateDescriptorByUUID(
            cgd.getUUID());

        //add the new conglomerate descriptor to the table descriptor
        cgd = getDataDictionary().getDataDescriptorGenerator().newConglomerateDescriptor(conglomId, null, false, null, false, null, td.getUUID(),
        td.getSchemaDescriptor().getUUID());
        ConglomerateDescriptorList conglomList = 
            td.getConglomerateDescriptorList();
        conglomList.add(cgd);

        //reset the heap conglomerate number in table descriptor to -1 so it 
        //will be refetched next time with the new value
        td.resetHeapConglomNumber();

        if (dropAndRedeclare)
        {
            //remove the old conglomerate from the system
            tc.dropConglomerate(cid); 

            replaceDeclaredGlobalTempTable(td.getName(), td);
        }

        return(td);
    }

    /**
      Do a rollback as appropriate for an internally generated
      rollback (e.g. as needed by sync, or autocommit).
     
      When a rollback happens, we 
      close all open activations and invalidate their
      prepared statements.  We then tell the cache to
      age out everything else, which effectively invalidates
      them.  Thus, all prepared statements will be
      compiled anew on their 1st execution after
      a rollback.
      <p>
      The invalidated statements can revalidate themselves without
      a full recompile if they verify their dependencies' providers still
      exist unchanged. REVISIT when invalidation types are created.
      <p>
      REVISIT: this may need additional alterations when
      RELEASE SAVEPOINT/ROLLBACK TO SAVEPOINT show up.
      <p>
      Also, tell the data dictionary that the transaction is finished,
      if necessary (that is, if the data dictionary was put into
      DDL mode in this transaction.

      @exception StandardException thrown on failure
     */

    public void internalRollback() throws StandardException 
    {
        doRollback(false /* non-xa */, false);
    }

    /**
     * Do a rollback as is appropriate for a user requested
     * rollback (e.g. a java.sql.Connection.rollback() or a language
     * 'ROLLBACk' statement.  Does some extra checking to make
     * sure that users aren't doing anything bad.
     *
     * @exception StandardException thrown on failure
     */
    public void userRollback() throws StandardException
    {
        doRollback(false /* non-xa */, true);
    }

    /**
        Same as userRollback() except rolls back a distrubuted transaction.

        @exception StandardException    thrown if something goes wrong
     */
    public void xaRollback() throws StandardException
    {
        doRollback(true /* xa */, true);
    }

    /**
     * When a rollback happens, the language connection context
     * will close all open activations and invalidate
     * their prepared statements. Then the language will abort the
     * Store transaction.
     * <p>
     * The invalidated statements can revalidate themselves without
     * a full recompile if they verify their dependencies' providers still
     * exist unchanged. REVISIT when invalidation types are created.
     * <p>
     * REVISIT: this may need additional alterations when
     * RELEASE SAVEPOINT/ROLLBACK TO SAVEPOINT show up.
     * <p>
     * Also, tell the data dictionary that the transaction is finished,
     * if necessary (that is, if the data dictionary was put into
     * DDL mode in this transaction.
     *
     * @param xa    true if this is an xa rollback
     * @param requestedByUser   true if requested by user
     *
     * @exception StandardException thrown on failure
     */
    private void doRollback(boolean xa, boolean requestedByUser) throws StandardException
    {
        StatementContext statementContext = getStatementContext();
        if (requestedByUser &&
            (statementContext != null) &&
            statementContext.inUse() &&
            statementContext.isAtomic())
        {
            throw StandardException.newException(SQLState.LANG_NO_ROLLBACK_IN_NESTED_CONNECTION);
        }

        clearDeferreds();
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419

        // Log rollback to error log, if appropriate
        if (logStatementText)
        {
            if (istream == null)
            {
                istream = Monitor.getStream();
            }
            String xactId = tran.getTransactionIdString();
            istream.printlnWithHeader(LanguageConnectionContext.xidStr +
                                      xactId + 
                                      "), " +
                                      LanguageConnectionContext.lccStr +
                                      instanceNumber +
                                      "), " + LanguageConnectionContext.dbnameStr +
                                          dbname +
                                          "), " +
                                          LanguageConnectionContext.drdaStr +
                                          drdaID +
                                      "), Rolling back");
        }

        endTransactionActivationHandling(true);

        currentSavepointLevel = 0; //reset the current savepoint level for the connection to 0 at the beginning of rollback work for temp tables
        if (allDeclaredGlobalTempTables != null)
            tempTablesAndRollback();

        finishDDTransaction();
        
        // If a nested transaction is active then
        // ensure it is destroyed before working
        // with the user transaction.
        if (readOnlyNestedTransaction != null)
        {
            readOnlyNestedTransaction.destroy();
            readOnlyNestedTransaction = null;
            queryNestingDepth = 0;
        }

        // now rollback the Store transaction
        TransactionController tc = getTransactionExecute();
        if (tc != null) 
        {   
            if (xa)
                ((XATransactionController)tc).xa_rollback();
            else
                tc.abort(); 
            
            // reset the savepoints to the new
            // location, since any outer nesting
            // levels expet there to be a savepoint
//IC see: https://issues.apache.org/jira/browse/DERBY-938
            resetSavepoints();
        }
    }

    /**
     * Reset all statement savepoints. Traverses the StatementContext
     * stack from bottom to top, calling resetSavePoint()
     * on each element.
     *
     * @exception StandardException thrown if something goes wrong
     */
    private void resetSavepoints() throws StandardException 
    {
        final ContextManager cm = getContextManager();
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        final List<Context>
                stmts = cm.getContextStack(ContextId.LANG_STATEMENT);

        for (Context c : stmts) {
            ((StatementContext)c).resetSavePoint();
        }
    }

    /**
     * Let the context deal with a rollback to savepoint
     *
     * @param   savepointName   Name of the savepoint that needs to be rolled back
     * @param   refreshStyle    boolean indicating whether or not the controller should close
     * open conglomerates and scans. Also used to determine if language should close
     * open activations.
     * @param   kindOfSavepoint  A NULL value means it is an internal savepoint (ie not a user defined savepoint)
     * Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
     *   A String value for kindOfSavepoint would mean it is SQL savepoint
     *   A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint
     *
     * @exception StandardException thrown if something goes wrong
     */
    public void internalRollbackToSavepoint( String savepointName, boolean refreshStyle, Object kindOfSavepoint )
        throws StandardException
    {
        // now rollback the Store transaction to the savepoint
        TransactionController tc = getTransactionExecute();
        if (tc != null)
        {
            boolean     closeConglomerates;

            if ( refreshStyle ) 
            {
                closeConglomerates = true;
                // bug 5145 - don't forget to close the activations while rolling
                // back to a savepoint
                endTransactionActivationHandling(true);
            }
            else { closeConglomerates = false; }

            currentSavepointLevel = tc.rollbackToSavePoint( savepointName, closeConglomerates, kindOfSavepoint );
        }

        if (tc != null && refreshStyle && allDeclaredGlobalTempTables != null)
            tempTablesAndRollback();
    }

    /**
      Let the context deal with a release of a savepoint

      @param    savepointName   Name of the savepoint that needs to be released
      @param    kindOfSavepoint  A NULL value means it is an internal savepoint (ie not a user defined savepoint)
      Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
      A String value for kindOfSavepoint would mean it is SQL savepoint
      A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint

      @exception StandardException thrown if something goes wrong
     */
    public  void    releaseSavePoint( String savepointName, Object kindOfSavepoint )  throws StandardException
    {
        TransactionController tc = getTransactionExecute();
        if (tc != null)
        {
            currentSavepointLevel = tc.releaseSavePoint( savepointName, kindOfSavepoint );
            //after a release of a savepoint, we need to go through our temp tables list.
            if (allDeclaredGlobalTempTables != null)
                tempTablesReleaseSavepointLevels();
        }
    }

    /**
      Sets a savepoint. Causes the Store to set a savepoint.

      @param    savepointName   name of savepoint
      @param    kindOfSavepoint  A NULL value means it is an internal savepoint (ie not a user defined savepoint)
      Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
      A String value for kindOfSavepoint would mean it is SQL savepoint
      A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint

        @exception StandardException thrown if something goes wrong
      */
    public  void    languageSetSavePoint( String savepointName, Object kindOfSavepoint )  throws StandardException
    {
        TransactionController tc = getTransactionExecute();
        if (tc != null)
        {
            currentSavepointLevel = tc.setSavePoint( savepointName, kindOfSavepoint );
        }
    }

    /**
     * Start a Nested User Transaction (NUT) with the store. If a NUT is 
     * already active simply increment a counter, queryNestingDepth, to keep
     * track of how many times we have tried to start a NUT.
     */
    public void beginNestedTransaction(boolean readOnly) throws StandardException
    {
        // DERBY-2490 incremental rework, currently this is only called
        // with read-only true. Future changes will have this
        // method support read-write nested transactions as well
        // instead of callers using the startNestedUserTransaction
        // directly on tran.
        if (SanityManager.DEBUG)
        {
            // if called for update transactions, compile would start using
            // non-readonly xacts for compile.  For now, throw an error if
            // someone tries to use this call to make non readonly transaction.
            SanityManager.ASSERT(
                readOnly, 
                "Routine not yet coded to support non-readonly transactions.");
        }

        if (readOnlyNestedTransaction == null)
            readOnlyNestedTransaction = 
                tran.startNestedUserTransaction(readOnly, true);

        queryNestingDepth++;
    }

    public void commitNestedTransaction()
        throws StandardException
    {
        if (--queryNestingDepth == 0)
        {
            readOnlyNestedTransaction.commit();
            readOnlyNestedTransaction.destroy();
            readOnlyNestedTransaction = null;
        }
    }

    /**
     * Get the transaction controller to use at compile time with this language
     * connection context. If a NUT is active then return NUT else return parent
     * transaction.
     */
    public final TransactionController getTransactionCompile()
    {
        return (readOnlyNestedTransaction != null) ? readOnlyNestedTransaction : tran;
    }

    public TransactionController getTransactionExecute()
    {
        return tran;
    }

 /** Get the data value factory to use with this language connection
        context.
     */
    public DataValueFactory getDataValueFactory() {
        return dataFactory;
    }
    
    /**
        Get the language factory to use with this language connection
        context.
     */
    public LanguageFactory getLanguageFactory() {
        return langFactory;
    }
        
    public OptimizerFactory getOptimizerFactory() {
        return of;
    }

    /**
        Get the language connection factory to use with this language connection
        context.
     */
    public LanguageConnectionFactory getLanguageConnectionFactory() {
        return connFactory;
    }

    /**
     * check if there are any activations that reference this temporary table
     * @param tableName look for any activations referencing this table name
     * @return boolean  false if found no activations
     */
    private boolean checkIfAnyActivationHasHoldCursor(String tableName)
            throws StandardException
    {
        for (int i = acts.size() - 1; i >= 0; i--) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            Activation a = acts.get(i);
            if (a.checkIfThisActivationHasHoldCursor(tableName))
                return true;
    }
    return false;
    }


    /**
     * Verify that there are no activations with open held result sets.
     *
     * @return boolean  Found no open (held) resultsets.
     *
     * @exception StandardException thrown on failure
     */
    /* This gets used in case of hold cursors. If there are any hold cursors open
     * then user can't change the isolation level without closing them. At the
     * execution time, set transaction isolation level calls this method before
     * changing the isolation level.
     */
    public boolean verifyAllHeldResultSetsAreClosed()
            throws StandardException
    {
        boolean seenOpenResultSets = false;

        /* For every activation */
        for (int i = acts.size() - 1; i >= 0; i--) {

            Activation a = acts.get(i);

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(a instanceof CursorActivation, "a is not a CursorActivation");
            }

            if (!a.isInUse())
            {
                continue;
            }

            if (!a.getResultSetHoldability())
            {
                continue;
            }

            ResultSet rs = ((CursorActivation) a).getResultSet();

            /* is there an open result set? */
            if ((rs != null) && !rs.isClosed() && rs.returnsRows())
            {
                seenOpenResultSets = true;
                break;
            }
        }

        if (!seenOpenResultSets)
            return(true);

        // There may be open ResultSet's that are yet to be garbage collected
        // let's try and force these out rather than throw an error
        System.gc();
        System.runFinalization();


        /* For every activation */
        for (int i = acts.size() - 1; i >= 0; i--) {
                
            Activation a = acts.get(i);

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(a instanceof CursorActivation, "a is not a CursorActivation");
            }

            if (!a.isInUse())
            {
                continue;
            }

            if (!a.getResultSetHoldability())
            {
                continue;
            }

            ResultSet rs = ((CursorActivation) a).getResultSet();

            /* is there an open held result set? */
            if ((rs != null) && !rs.isClosed() && rs.returnsRows())
            {
                return(false);
            }
        }
        return(true);
    }

    /**
     * Verify that there are no activations with open result sets
     * on the specified prepared statement.
     *
     * @param pStmt     The prepared Statement
     * @param provider  The object precipitating a possible invalidation
     * @param action    The action causing the possible invalidation
     *
     * @return Nothing.
     *
     * @exception StandardException thrown on failure
     */
    public boolean verifyNoOpenResultSets(PreparedStatement pStmt, Provider provider,
                                       int action)
            throws StandardException
    {
        /*
        ** It is not a problem to create an index when there is an open
        ** result set, since it doesn't invalidate the access path that was
        ** chosen for the result set.
        */
        boolean seenOpenResultSets = false;

        /* For every activation */

        // synchronize on acts as other threads may be closing activations
        // in this list, thus invalidating the Enumeration
        for (int i = acts.size() - 1; i >= 0; i--) {
                
            Activation a = acts.get(i);

            if (!a.isInUse())
            {
                continue;
            }
            
            /* for this prepared statement */
            if (pStmt == a.getPreparedStatement()) {
                ResultSet rs = a.getResultSet();

                /* is there an open result set? */
                if (rs != null && ! rs.isClosed())
                {
                    if (!rs.returnsRows())
                        continue;
                    seenOpenResultSets = true;
                    break;
                }
                
            }
        }

        if (!seenOpenResultSets)
            return false;

        // There may be open ResultSet's that are yet to be garbage collected
        // let's try and force these out rather than throw an error
        System.gc();
        System.runFinalization();


        /* For every activation */
        // synchronize on acts as other threads may be closing activations
        // in this list, thus invalidating the Enumeration
        for (int i = acts.size() - 1; i >= 0; i--) {
                
            Activation a = acts.get(i);

            if (!a.isInUse())
            {
                continue;
            }

            /* for this prepared statement */
            if (pStmt == a.getPreparedStatement()) {
                ResultSet rs = a.getResultSet();

                /* is there an open result set? */
                if (rs != null && ! rs.isClosed())
                {
                    if ((provider != null) && rs.returnsRows()) {
                    DependencyManager dmgr = getDataDictionary().getDependencyManager();

                    throw StandardException.newException(SQLState.LANG_CANT_INVALIDATE_OPEN_RESULT_SET, 
                                    dmgr.getActionString(action), 
                                    provider.getObjectName());

                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get the session user
     *
     * @return String the authorization id of the session user.
     */
    public String getSessionUserId()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
        return sessionUser;
    }

    /**
     * @see LanguageConnectionContext#getDefaultSchema
     */
    public SchemaDescriptor getDefaultSchema() 
    { 
//IC see: https://issues.apache.org/jira/browse/DERBY-3327
//IC see: https://issues.apache.org/jira/browse/DERBY-1331
        return getCurrentSQLSessionContext().getDefaultSchema();
    }

    /**
     * @see LanguageConnectionContext#getDefaultSchema(Activation a)
     */
    public SchemaDescriptor getDefaultSchema(Activation a) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3897
        return getCurrentSQLSessionContext(a).getDefaultSchema();
    }

    /**
     * @see LanguageConnectionContext#getCurrentSchemaName()
     */
    public String getCurrentSchemaName()
    {
        // getCurrentSchemaName with no arg is used even
        // at run-time but only in places(*) where the statement context
        // can be relied on, AFAICT.
        //
        // (*) SpaceTable#getConglomInfo,
        //     SystemProcedures#{INSTALL|REPLACE|REMOVE}_JAR

        SchemaDescriptor s = getDefaultSchema();
        if( null == s)
            return null;
        return s.getSchemaName();
    }


    /**
     * @see LanguageConnectionContext#getCurrentSchemaName(Activation a)
     */
    public String getCurrentSchemaName(Activation a)
    {
        SchemaDescriptor s = getDefaultSchema(a);
        if( null == s)
            return null;
        return s.getSchemaName();
    }


    /**
     * @see LanguageConnectionContext#isInitialDefaultSchema
     */
    public boolean isInitialDefaultSchema(String schemaName) {
//IC see: https://issues.apache.org/jira/browse/DERBY-48
        return cachedInitialDefaultSchemaDescr.getSchemaName().
            equals(schemaName);
    }


    /**
     * @see LanguageConnectionContext#setDefaultSchema(SchemaDescriptor sd)
     */
    public void setDefaultSchema(SchemaDescriptor sd)
        throws StandardException
    {   
        if (sd == null) {   
            sd = getInitialDefaultSchemaDescriptor();
        }

        getCurrentSQLSessionContext().setDefaultSchema(sd);
    }

    /**
     * @see LanguageConnectionContext#setDefaultSchema(Activation a,
     * SchemaDescriptor sd)
     */
    public void setDefaultSchema(Activation a, SchemaDescriptor sd)
        throws StandardException
    {
        if (sd == null) {
            sd = getInitialDefaultSchemaDescriptor();
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-3897
        getCurrentSQLSessionContext(a).setDefaultSchema(sd);
    }


    /**
     * @see LanguageConnectionContext#resetSchemaUsages(Activation activation,
     *      String schemaName)
     */
    public void resetSchemaUsages(Activation activation, String schemaName)
            throws StandardException {

//IC see: https://issues.apache.org/jira/browse/DERBY-3897
        Activation parent = activation.getParentActivation();
        SchemaDescriptor defaultSchema = getInitialDefaultSchemaDescriptor();

        // walk SQL session context chain
        while (parent != null) {
            SQLSessionContext ssc = parent.getSQLSessionContextForChildren();
            SchemaDescriptor s = ssc.getDefaultSchema();

            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(s != null, "s should not be empty here");
            }

            if (schemaName.equals(s.getSchemaName())) {
                ssc.setDefaultSchema(defaultSchema);
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-3897
            parent = parent.getParentActivation();
        }

        // finally top level
        SQLSessionContext top = getTopLevelSQLSessionContext();
        SchemaDescriptor sd = top.getDefaultSchema();

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(sd != null, "sd should not be empty here");
        }

        if (schemaName.equals(sd.getSchemaName())) {
            top.setDefaultSchema(defaultSchema);
        }
    }

    /**
     * Get the identity column value most recently generated.
     *
     * @return the generated identity column value
     */
    public Long getIdentityValue()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        return identityNotNull ? Long.valueOf(identityVal) : null;
    }

    /**
     * Set the field of most recently generated identity column value.
     *
     * @param val the generated identity column value
     */
    public void setIdentityValue(long val)
    {
        identityVal = val;
        identityNotNull = true;
    }

    /**
     * Push a CompilerContext on the context stack with
     * the current default schema as the default schema
     * which we compile against.
     *
     * @return the compiler context
     */
    public  final CompilerContext pushCompilerContext()
    {
        return pushCompilerContext((SchemaDescriptor)null);
    }

    /**
     * Push a CompilerContext on the context stack with
     * the passed in schema sd as the default schema
     * we compile against.
     *
     * @param sd the default schema 
     *
     * @return the compiler context
     *
     * For the parameter sd, there are 3 possible values(of interest) that can 
     * get passed into this method:
     * 
     * a) A null SchemaDescriptor which indicates to the system to use the 
     *    CURRENT SCHEMA as the compilation schema.
     *    
     * b) A SchemaDescriptor with its UUID == null, this indicates that either 
     *    the schema has not been physically created yet or that the LCC's 
     *    getDefaultSchema() is not yet up-to-date with its actual UUID. 
     *    The system will use the CURRENT SCHEMA as the compilation schema. 
     *    
     * c) A SchemaDescriptor with its UUID != null, this means that the schema 
     *    has been physically created.  The system will use this schema as the 
     *    compilation schema (e.g.: for trigger or view recompilation cases, 
     *    etc.). 
     *    
     * The compiler context's compilation schema will be set accordingly based 
     * on the given input above.   
     */
    public  CompilerContext pushCompilerContext(SchemaDescriptor sd)
    {
        CompilerContext cc;
        boolean         firstCompilerContext = false;

        //  DEBUG   END

        cc = (CompilerContext) (getContextManager().getContext(CompilerContext.CONTEXT_ID));

        /*
        ** If there is no compiler context, this is the first one on the
        ** stack, so don't pop it when we're done (saves time).
        */
        if (cc == null) { firstCompilerContext = true; }

        if (cc == null || cc.getInUse())
        {
            cc = new CompilerContextImpl(getContextManager(), this, tcf);
            if (firstCompilerContext) { cc.firstOnStack(); }
        }
        else
        {
            /* Reset the next column,table, subquery and ResultSet numbers at 
            * the beginning of each statement 
            */
            cc.resetContext();
        }

        cc.setInUse(true);

        StatementContext sc = getStatementContext();
        if (sc.getSystemCode())
            cc.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);

        /*
         * Set the compilation schema when its UUID is available.
         * i.e.:  Schema may not have been physically created yet, so
         *        its UUID will be null.
         * 
         * o For trigger SPS recompilation, the system must use its
         *   compilation schema to recompile the statement. 
         * 
         * o For view recompilation, we set the compilation schema
         *   for this compiler context if its UUID is available.
         *   Otherwise, the compilation schema will be determined
         *   at execution time of view creation.
         */
        if (sd != null && sd.getUUID() != null)
        {
            cc.setCompilationSchema(sd);
        }
        
        return  cc;
    }


    /**
     * Pop a CompilerContext off the context stack.
     *
     * @param cc  The compiler context.
     */
    public void popCompilerContext(CompilerContext cc)
    {
        cc.setCurrentDependent(null);

        cc.setInUse(false);

        /*
        ** Only pop the compiler context if it's not the first one
        ** on the stack.
        */
        if (! cc.isFirstOnStack()) 
        { 
            cc.popMe(); 
        }
        else
        {
            cc.setCompilationSchema((SchemaDescriptor)null);
        }
    }

    /**
     * Push a StatementContext on the context stack.
     *
     * Inherit SQL session state a priori (statementContext will get
     * its own SQL session state if this statement executes a call,
     * cf. pushNestedSessionContext.

     * @param isAtomic whether this statement is atomic or not
     * @param isForReadOnly whether this statement is for a read only resultset
     * @param stmtText the text of the statement.  Needed for any language
     *  statement (currently, for any statement that can cause a trigger
     *  to fire).  Please set this unless you are some funky jdbc setXXX
     *  method or something.
     * @param pvs parameter value set, if it has one
     * @param rollbackParentContext True if 1) the statement context is
     *  NOT a top-level context, AND 2) in the event of a statement-level
     *  exception, the parent context needs to be rolled back, too.
     * @param timeoutMillis timeout value for this statement, in milliseconds.
     *  The value 0 means that no timeout is set.
     *
     * @return StatementContext  The statement context.
     *
     */
    public StatementContext pushStatementContext (boolean isAtomic, boolean isForReadOnly, 
//IC see: https://issues.apache.org/jira/browse/DERBY-231
                              String stmtText, ParameterValueSet pvs, 
                              boolean rollbackParentContext, 
                              long timeoutMillis)
    {
        int                 parentStatementDepth = statementDepth;
        boolean             inTrigger = false;
        boolean             parentIsAtomic = false;

        // by default, assume we are going to use the outermost statement context
        StatementContext    statementContext = statementContexts[0];

        /*
        ** If we haven't allocated any statement contexts yet, allocate
        ** the outermost stmt context now and push it.
        */

        if (statementContext == null)
        {
            statementContext = statementContexts[0] = new GenericStatementContext(this);
//IC see: https://issues.apache.org/jira/browse/DERBY-3327
//IC see: https://issues.apache.org/jira/browse/DERBY-1331
            statementContext.
                setSQLSessionContext(getTopLevelSQLSessionContext());
        }
        else if (statementDepth > 0)
        {
            StatementContext    parentStatementContext;
            /*
            ** We also cache a 2nd statement context, though we still
            ** push and pop it. Note, new contexts are automatically pushed.
            */
            if (statementDepth == 1)
            {
                statementContext = statementContexts[1];

                if (statementContext == null)
                    statementContext = statementContexts[1] = new GenericStatementContext(this);
                else
                    statementContext.pushMe();

                parentStatementContext = statementContexts[0];
            }
            else
            {
                parentStatementContext = getStatementContext();
                statementContext = new GenericStatementContext(this);
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-3327
//IC see: https://issues.apache.org/jira/browse/DERBY-1331
            statementContext.setSQLSessionContext(
                parentStatementContext.getSQLSessionContext());

            inTrigger = parentStatementContext.inTrigger() || (outermostTrigger == parentStatementDepth);
            parentIsAtomic = parentStatementContext.isAtomic();
            statementContext.setSQLAllowed(parentStatementContext.getSQLAllowed(), false);
            if (parentStatementContext.getSystemCode())
                statementContext.setSystemCode();
        } else {
            statementContext.
                setSQLSessionContext(getTopLevelSQLSessionContext());
        }

        incrementStatementDepth();

//IC see: https://issues.apache.org/jira/browse/DERBY-231
        statementContext.setInUse(inTrigger, isAtomic || parentIsAtomic, isForReadOnly, stmtText, pvs, timeoutMillis);
        if (rollbackParentContext)
            statementContext.setParentRollback();
        return statementContext;
    }

    /**
     * Pop a StatementContext of the context stack.
     *
     * @param statementContext  The statement context.
     * @param error             The error, if any  (Only relevant for DEBUG)
     */
    public void popStatementContext(StatementContext statementContext,
                                    Throwable error) 
    {
        if ( statementContext != null ) 
        { 
            /*
            ** If someone beat us to the punch, then it is ok,
            ** just silently ignore it.  We probably got here
            ** because we had a try catch block around a push/pop
            ** statement context, and we already got the context
            ** on a cleanupOnError.
            */
            if (!statementContext.inUse())
            {
                return;
            }
            statementContext.clearInUse(); 
        }

        decrementStatementDepth();
        if (statementDepth == -1)
        {
            /*
             * Only ignore the pop request for an already
             * empty stack when dealing with a session exception.
             */
            if (SanityManager.DEBUG)
            {
                int severity = (error instanceof StandardException) ?
                                    ((StandardException)error).getSeverity() :
                                0;
                SanityManager.ASSERT(error != null,
                    "Must have error to try popStatementContext with 0 depth");
                SanityManager.ASSERT(
                    (severity == ExceptionSeverity.SESSION_SEVERITY),
                    "Must have session severity error to try popStatementContext with 0 depth");
                SanityManager.ASSERT(statementContext == statementContexts[0],
                    "statementContext is expected to equal statementContexts[0]");
            }
            resetStatementDepth(); // pretend we did nothing.
        }
        else if (statementDepth == 0)
        {
            if (SanityManager.DEBUG)
            {
                /* Okay to pop last context on a session exception.
                 * (We call clean up on error when exiting connection.)
                 */
                int severity = (error instanceof StandardException) ?
                                    ((StandardException)error).getSeverity() :
                                0;
                if ((error == null) || 
                    (severity != ExceptionSeverity.SESSION_SEVERITY))
                {
                    SanityManager.ASSERT(statementContext == statementContexts[0],
                        "statementContext is expected to equal statementContexts[0]");
                }
            }
        }
        else
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(statementContext != statementContexts[0],
                    "statementContext is not expected to equal statementContexts[0]");
                if (statementDepth <= 0)
                    SanityManager.THROWASSERT(
                        "statement depth expected to be >0, was "+statementDepth);
                
//IC see: https://issues.apache.org/jira/browse/DERBY-1095
                if (getContextManager().getContext(statementContext.getIdName()) != statementContext)
                {
                    SanityManager.THROWASSERT("trying to pop statement context from middle of stack");
                }
            }

            statementContext.popMe();       
        }

    }

    /**
     * Push a new execution statement validator.  An execution statement 
     * validator is an object that validates the current statement to
     * ensure that it is permitted given the current execution context.
     * An example of a validator a trigger ExecutionStmtValidator that
     * doesn't allow ddl on the trigger target table.
     * <p>
     * Multiple ExecutionStmtValidators may be active at any given time.
     * This mirrors the way there can be multiple connection nestings
     * at a single time.  The validation is performed by calling each
     * validator's validateStatement() method.  This yields the union
     * of all validations.
     *
     * @param validator the validator to add
     */
    public void pushExecutionStmtValidator(ExecutionStmtValidator validator)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2150
        stmtValidators.add(validator);
    }

    /**
     * Remove the validator.  Does an object identity (validator == validator)
     * comparison.  Asserts that the validator is found.
     *
     * @param validator the validator to remove
     *
     * @exception StandardException on error
     */
    public void popExecutionStmtValidator(ExecutionStmtValidator validator)
        throws StandardException
    {
        boolean foundElement = stmtValidators.remove(validator);
        if (SanityManager.DEBUG)
        {
            if (!foundElement)
            {
                SanityManager.THROWASSERT("statement validator "+validator+" not found");
            }
        }
    }

    /**
     * Push a new trigger execution context.
     * <p>
     * Multiple TriggerExecutionContexts may be active at any given time.
     *
     * @param tec the trigger execution context
     *
     * @exception StandardException on trigger recursion error
     */
    public void pushTriggerExecutionContext(TriggerExecutionContext tec) throws StandardException
    {
        if (outermostTrigger == -1) 
        {
            outermostTrigger = statementDepth; 
        }

        /* Maximum 16 nesting levels allowed */
//IC see: https://issues.apache.org/jira/browse/DERBY-104
        if (triggerExecutionContexts.size() >= Limits.DB2_MAX_TRIGGER_RECURSION)
        {
            throw StandardException.newException(SQLState.LANG_TRIGGER_RECURSION_EXCEEDED);
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-2150
        triggerExecutionContexts.add(tec);
    }

    /**
     * Remove the tec.  Does an object identity (tec == tec)
     * comparison.  Asserts that the tec is found.
     *
     * @param tec the tec to remove
     *
     * @exception StandardException on error
     */
    public void popTriggerExecutionContext(TriggerExecutionContext tec)
        throws StandardException
    {
        if (outermostTrigger == statementDepth)
        {
            outermostTrigger = -1; 
        }

        boolean foundElement = triggerExecutionContexts.remove(tec);
        if (SanityManager.DEBUG)
        {
            if (!foundElement)
            {
                SanityManager.THROWASSERT("trigger execution context "+tec+" not found");
            }
        }
    }

    /**
     * Get the topmost tec.  
     *
     * @return the tec
     */
    public TriggerExecutionContext getTriggerExecutionContext()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        return triggerExecutionContexts.isEmpty() ?
                null :
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
                triggerExecutionContexts.get( triggerExecutionContexts.size() - 1 );   
    }

    /**
     * Validate a statement.  Does so by stepping through all the validators
     * and executing them.  If a validator throws and exception, then the
     * checking is stopped and the exception is passed up.
     *
     * @param constantAction the constantAction that is about to be executed (and
     *  should be validated
     *
     * @exception StandardException on validation failure
     */
    public void validateStmtExecution(ConstantAction constantAction)
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(constantAction!=null, "constantAction is null");
        }

        if (stmtValidators.size() > 0)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            for (Iterator<ExecutionStmtValidator> it = stmtValidators.iterator(); it.hasNext(); )
            {
                it.next()
                    .validateStatement(constantAction);
            }
        }
    }
    
    /**
     * Set the trigger table descriptor.  Used to compile
     * statements that may special trigger pseudo tables.
     *
     * @param td the table that the trigger is 
     * defined upon
     *
     */
    public void pushTriggerTable(TableDescriptor td)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2150
        triggerTables.add(td);
    }

    /**
     * Remove the trigger table descriptor.
     *
     * @param td the table to remove from the stack.
     */
    public void popTriggerTable(TableDescriptor td)
    {
        boolean foundElement = triggerTables.remove(td);
        if (SanityManager.DEBUG)
        {
            if (!foundElement)
            {
                SanityManager.THROWASSERT("trigger table not found: "+td);
            }
        }
    }

    /**
     * Get the topmost trigger table descriptor
     *
     * @return the table descriptor, or null if we
     * aren't in the middle of compiling a create
     * trigger.
     */
    public TableDescriptor getTriggerTable()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        return triggerTables.isEmpty() ?
            (TableDescriptor)null :
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            triggerTables.get(triggerTables.size() - 1);
    }
    /**
     * @see LanguageConnectionContext#getDatabase
     */
    public Database
    getDatabase()
    {
        return db;
    }

    /** @see LanguageConnectionContext#incrementBindCount */
    public int incrementBindCount()
    {
        bindCount++;
        return bindCount;
    }

    
    /** @see LanguageConnectionContext#decrementBindCount */
    public int decrementBindCount()
    {
        bindCount--;

        if (SanityManager.DEBUG)
        {
            if (bindCount < 0)
                SanityManager.THROWASSERT(
                    "Level of nested binding == " + bindCount);
        }

        return bindCount;
    }

    /** @see LanguageConnectionContext#getBindCount */
    public int getBindCount()
    {
        return bindCount;
    }

    /** @see LanguageConnectionContext#setDataDictionaryWriteMode */
    public final void setDataDictionaryWriteMode()
    {
        ddWriteMode = true;
    }

    /** @see LanguageConnectionContext#dataDictionaryInWriteMode */
    public final boolean dataDictionaryInWriteMode()
    {
        return ddWriteMode;
    }

    /** @see LanguageConnectionContext#setRunTimeStatisticsMode */
    public final void setRunTimeStatisticsMode(boolean onOrOff)
    {
        runTimeStatisticsSetting = onOrOff;
    }

    /** @see LanguageConnectionContext#getRunTimeStatisticsMode */
    public boolean getRunTimeStatisticsMode()
    {
        return runTimeStatisticsSetting;
    }

    /** @see LanguageConnectionContext#setStatisticsTiming */
    public void setStatisticsTiming(boolean onOrOff)
    {
        statisticsTiming = onOrOff;
    }

    /** @see LanguageConnectionContext#getStatisticsTiming */
    public boolean getStatisticsTiming()
    {
        return statisticsTiming;
    }

    /** @see LanguageConnectionContext#setRunTimeStatisticsObject */
    public void setRunTimeStatisticsObject(RunTimeStatistics runTimeStatisticsObject)
    {
        this.runTimeStatisticsObject = runTimeStatisticsObject;
    }

    /** @see LanguageConnectionContext#getRunTimeStatisticsObject */
    public RunTimeStatistics getRunTimeStatisticsObject()
    {
        return runTimeStatisticsObject;
    }


    /**
      * Reports how many statement levels deep we are.
      *
      * @return a statement level &gt;= OUTERMOST_STATEMENT
      */
    public  int     getStatementDepth()
    { return statementDepth; }

    /**
     * @see LanguageConnectionContext#isIsolationLevelSetUsingSQLorJDBC
     */
    public boolean isIsolationLevelSetUsingSQLorJDBC()
    {
        return isolationLevelSetUsingSQLorJDBC;
    }

    /**
     * @see LanguageConnectionContext#resetIsolationLevelFlagUsedForSQLandJDBC
     */
    public void resetIsolationLevelFlagUsedForSQLandJDBC()
    {
        isolationLevelSetUsingSQLorJDBC = false;
    }

    /**
     * @see LanguageConnectionContext#setIsolationLevel
     */
    public void setIsolationLevel(int isolationLevel) throws StandardException
    {
        StatementContext stmtCtxt = getStatementContext();
        if (stmtCtxt!= null && stmtCtxt.inTrigger())
            throw StandardException.newException(SQLState.LANG_NO_XACT_IN_TRIGGER, getTriggerExecutionContext().toString());

        // find if there are any held cursors from previous isolation level.
        // if yes, then throw an exception that isolation change not allowed until
        // the held cursors are closed.
        // I had to move this check outside of transaction idle check because if a
        // transactions creates held cursors and commits the transaction, then
        // there still would be held cursors but the transaction state would be idle.
        // In order to check the above mentioned case, the held cursor check
        // shouldn't rely on transaction state.
        if (this.isolationLevel != isolationLevel)
        {
            if (!verifyAllHeldResultSetsAreClosed())
            {
                throw StandardException.newException(SQLState.LANG_CANT_CHANGE_ISOLATION_HOLD_CURSOR);
            }
        }

        /* Commit and set to new isolation level.
         * NOTE: We commit first in case there's some kind
         * of error, like can't commit within a server side jdbc call.
         */
        TransactionController tc = getTransactionExecute();
        if (!tc.isIdle())
        {
            // If this transaction is in progress, commit it.
            // However, do not allow commit to happen if this is a global
            // transaction.
            if (tc.isGlobal())
                throw StandardException.newException(SQLState.LANG_NO_SET_TRAN_ISO_IN_GLOBAL_CONNECTION);

            userCommit();
        }
        this.isolationLevel = isolationLevel;
        this.isolationLevelExplicitlySet = true;
        this.isolationLevelSetUsingSQLorJDBC = true;
    }

    /**
     * @see LanguageConnectionContext#getCurrentIsolationLevel
     */
    public int getCurrentIsolationLevel()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
        return (isolationLevel == TransactionControl.UNSPECIFIED_ISOLATION_LEVEL) ? defaultIsolationLevel : isolationLevel;
    }

    /**
     * @see LanguageConnectionContext#getCurrentIsolationLevel
     */
    public String getCurrentIsolationLevelStr()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
        if( isolationLevel >= 0 && isolationLevel < TransactionControl.isolationMapCount() )
        {
            return TransactionControl.isolationTextNames( isolationLevel )[0];
        }
        return TransactionControl.isolationTextNames( TransactionControl.UNSPECIFIED_ISOLATION_LEVEL )[0];
    }

    /**
     * @see LanguageConnectionContext#setPrepareIsolationLevel
     */
    public void setPrepareIsolationLevel(int level) 
    {
            prepareIsolationLevel = level;
    }

    /**
     * @see LanguageConnectionContext#getPrepareIsolationLevel
     */
    public int getPrepareIsolationLevel()
    {
        if (!isolationLevelExplicitlySet)
            return prepareIsolationLevel;
        else
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
            return TransactionControl.UNSPECIFIED_ISOLATION_LEVEL;
    }

    /**
     * @see LanguageConnectionContext#getStatementContext
     */
    public StatementContext getStatementContext()
    {   
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        return (StatementContext) getContextManager().getContext(org.apache.derby.shared.common.reference.ContextId.LANG_STATEMENT);
    }

    /**
     * @see LanguageConnectionContext#setOptimizerTracer
     */
    public void setOptimizerTracer( OptTrace tracer )
    {
        optimizerTracer = tracer;
    }

    /**
     * @see LanguageConnectionContext#getOptimizerTracer
     */
	public OptTrace getOptimizerTracer() { return optimizerTracer; }
    
    /**
     * @see LanguageConnectionContext#optimizerTracingIsOn
     */
    public boolean optimizerTracingIsOn()
    {
        return (optimizerTracer != null);
    }

    /**
      * Reports whether there is any outstanding work in the transaction.
      *
      * @return true if there is outstanding work in the transaction
      *             false otherwise
      */
    public  boolean isTransactionPristine()
    {
        return getTransactionExecute().isPristine();
    }


    //
    // Context interface
    //
    /**
        If worse than a transaction error, everything goes; we
        rely on other contexts to kill the context manager
        for this session.
        <p>
        If a transaction error, act like we saw a rollback.
        <p>
        If more severe or a java error, the outer cleanup
        will shutdown the connection, so we don't have to clean up.
        <p>
        REMIND: connection should throw out all contexts and start
        over when the connection is closed... perhaps by throwing
        out the context manager?
        <p>
        REVISIT: If statement error, should we do anything?
        <P>
        Since the access manager's own context takes care of its own
        resources on errors, there is nothing that this context has
        to do with the transaction controller.

        @exception StandardException thrown on error. REVISIT: don't want
        cleanupOnError's to throw exceptions.
     */
    public void cleanupOnError(Throwable error) throws StandardException {

        /*
        ** If it isn't a StandardException, then assume
        ** session severity. It is probably an unexpected
        ** java error somewhere in the language.
        ** Store layer treats JVM error as session severity, 
        ** hence to be consistent and to avoid getting rawstore
        ** protocol violation errors, we treat java errors here
        ** to be of session severity.
        */  

        int severity = (error instanceof StandardException) ?
            ((StandardException) error).getSeverity() :
//IC see: https://issues.apache.org/jira/browse/DERBY-1732
            ExceptionSeverity.SESSION_SEVERITY;
 
        if (statementContexts[0] != null)
        {
            statementContexts[0].clearInUse();
            
            // Force the StatementContext that's normally
            // left on the stack for optimization to be popped
            // when the session is closed. Ensures full cleanup
            // and no hanging refrences in the ContextManager.
//IC see: https://issues.apache.org/jira/browse/DERBY-1095
            if (severity >= ExceptionSeverity.SESSION_SEVERITY)
                statementContexts[0].popMe();
        }
        if (statementContexts[1] != null)
        {
            statementContexts[1].clearInUse();                
        }

        // closing the activations closes all the open cursors.
        // the activations are, for all intents and purposes, the
        // cursors.
        if (severity >= ExceptionSeverity.SESSION_SEVERITY) 
        {
            for (int i = acts.size() - 1; i >= 0; i--) {
                // it maybe the case that a reset()/close() ends up closing
                // one or more activation leaving our index beyond
                // the end of the array
                if (i >= acts.size())
                    continue;
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
                Activation a = acts.get(i);
                a.reset();
                a.close();
            }
                       
            popMe();

//IC see: https://issues.apache.org/jira/browse/DERBY-5152
            InterruptStatus.saveInfoFromLcc(this);
        }

        /*
        ** We have some global state that we need
        ** to clean up no matter what.  Be sure
        ** to do so.
        */
        else if (severity >= ExceptionSeverity.TRANSACTION_SEVERITY) 
        {
            internalRollback();
        }
    }

    /**
     * @see org.apache.derby.iapi.services.context.Context#isLastHandler
     */
    @Override
    public boolean isLastHandler(int severity)
    {
        return false;
    }

    //
    // class implementation
    //

    /**
        If we are called as part of rollback code path, then we will reset all 
        the activations that have resultset returning rows associated with 
        them. DERBY-3304 Resultsets that do not return rows should be left 
        alone when the rollback is through the JDBC Connection object. If the 
        rollback is caused by an exception, then at that time, all kinds of
        resultsets should be closed. 
        
        If we are called as part of commit code path, then we will do one of 
        the following if the activation has resultset assoicated with it. Also,
        we will clear the conglomerate used while scanning for update/delete
        1)Close result sets that return rows and are not held across commit.
        2)Clear the current row of the resultsets that return rows and are
        held across commit.
        3)Leave the result sets untouched if they do not return rows
        
        Additionally, clean up (close()) activations that have been
        marked as unused during statement finalization.

        @exception StandardException thrown on failure
     */
    private void endTransactionActivationHandling(boolean forRollback) throws StandardException {

        // don't use an enumeration as the activation may remove
        // itself from the list, thus invalidating the Enumeration
        for (int i = acts.size() - 1; i >= 0; i--) {

            // it maybe the case that a reset() ends up closing
            // one or more activation leaving our index beyond
            // the end of the array
            if (i >= acts.size())
                continue;

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            Activation a = acts.get(i);
            /*
            ** Look for stale activations.  Activations are
            ** marked as unused during statement finalization.
            ** Here, we sweep and remove this inactive ones.
            */  
            if (!a.isInUse())
            {
                a.close();
                continue;
            }

            //Determine if the activation has a resultset and if that resultset
            //returns rows. For such an activation, we need to take special
            //actions during commit and rollback as explained in the comments
            //below.
            ResultSet activationResultSet = a.getResultSet();
            boolean resultsetReturnsRows =
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                    activationResultSet != null &&
                    activationResultSet.returnsRows();

            if (forRollback) { 
                if (resultsetReturnsRows)
                    //Since we are dealing with rollback, we need to reset 
                    //the activation no matter what the holdability might 
                    //be provided that resultset returns rows. An example
                    //where we do not want to close a resultset that does
                    //not return rows would be a java procedure which has
                    //user invoked rollback inside of it. That rollback
                    //should not reset the activation associated with
                    //the call to java procedure because that activation
                    //is still being used.
                    a.reset();
                // Only invalidate statements if we performed DDL.
                if (dataDictionaryInWriteMode()) {
                    ExecPreparedStatement ps = a.getPreparedStatement();
                    if (ps != null) {
                        ps.makeInvalid(DependencyManager.ROLLBACK, this);
                    }
                }
            } else {
                //We are dealing with commit here. 
                if (resultsetReturnsRows){
                    if (a.getResultSetHoldability() == false)
                        //Close result sets that return rows and are not held 
                        //across commit. This is to implement closing JDBC 
                        //result sets that are CLOSE_CURSOR_ON_COMMIT at commit 
                        //time. 
                        activationResultSet.close();
                    else 
                        //Clear the current row of the result sets that return
                        //rows and are held across commit. This is to implement
                        //keeping JDBC result sets open that are 
                        //HOLD_CURSORS_OVER_COMMIT at commit time and marking
                        //the resultset to be not on a valid row position. The 
                        //user will need to reposition within the resultset 
                        //before doing any row operations.
                        activationResultSet.clearCurrentRow();                          
                }
                a.clearHeapConglomerateController();
            }
        }
    }

    /**
        Finish the data dictionary transaction, if any.

        @exception StandardException    Thrown on error
     */
    private void finishDDTransaction() throws StandardException {

        /* Was the data dictionary put into write mode? */
        if (ddWriteMode) {
            DataDictionary dd = getDataDictionary();

            /* Tell the data dictionary that the transaction is finished */
            dd.transactionFinished();

            /* The data dictionary isn't in write mode any more */
            ddWriteMode = false;
        }
    }

    ////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    ////////////////////////////////////////////////////////////////////

    /**
      * Increments the statement depth.
      */
    private void    incrementStatementDepth() { statementDepth++; }

    /**
      * Decrements the statement depth
      */
    private void    decrementStatementDepth()
    {
        statementDepth--;
    }

    /**
      * Resets the statementDepth.
      */
    protected   void    resetStatementDepth()
    {
        statementDepth = 0;
    }

    public DataDictionary getDataDictionary()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2138
        return getDatabase().getDataDictionary();
    }

    /**
      @see LanguageConnectionContext#setReadOnly
      @exception StandardException The operation is disallowed.
      */
    public void setReadOnly(boolean on) throws StandardException
    {
        if (!tran.isPristine())
            throw StandardException.newException(SQLState.AUTH_SET_CONNECTION_READ_ONLY_IN_ACTIVE_XACT);
        authorizer.setReadOnlyConnection(on,true);
    }

    /**
      @see LanguageConnectionContext#isReadOnly
      */
    public boolean isReadOnly()
    {
        return authorizer.isReadOnlyConnection();
    }

    /**
      @see LanguageConnectionContext#getAuthorizer
     */
    public Authorizer getAuthorizer()
    {
        return authorizer;
    }

    /**
     * lastAutoincrementValue searches for the last autoincrement value inserted
     * into a column specified by the user. The search for the "last" value
     * supports nesting levels caused by triggers (Only triggers cause nesting,
     * not server side JDBC). 
     * If lastAutoincrementValue is called from within a trigger, the search
     * space for ai-values are those values that are inserted by this trigger as
     * well as previous triggers; 
     * i.e if a SQL statement fires trigger T1, which in turn does something
     * that fires trigger t2, and if lastAutoincrementValue is called from
     * within t2, then autoincrement values genereated by t1 are visible to
     * it. By the same logic, if it is called from within t1, then it does not
     * see values inserted by t2.
     *
     * @see LanguageConnectionContext#lastAutoincrementValue
     */
    public Long lastAutoincrementValue(String schemaName, String tableName,
                                       String columnName)
    {
        String aiKey = AutoincrementCounter.makeIdentity(schemaName, tableName, columnName);
        
        int size = triggerExecutionContexts.size();
        //      System.out.println(" searching for " + aiKey);
        for (int i = size - 1; i >= 0; i--)
        {
            // first loop through triggers.
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            TriggerExecutionContext itec = triggerExecutionContexts.get(i);
            Long value = itec.getAutoincrementValue(aiKey);
            if (value == null)
                continue;

            return value;
        }
        if (autoincrementHT == null)
            return null;
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        return autoincrementHT.get(aiKey);
    }   

    /**
     * @see LanguageConnectionContext#setAutoincrementUpdate
     */
    public void setAutoincrementUpdate(boolean flag)
    {
        autoincrementUpdate = flag;
    }
    
    /**
     * @see LanguageConnectionContext#getAutoincrementUpdate
     */
    public boolean getAutoincrementUpdate()
    {
        return autoincrementUpdate;
    }
    
    /**
     * @see LanguageConnectionContext#autoincrementCreateCounter
     */
    public void autoincrementCreateCounter(String s, String t, String c,
                                           Long initialValue, long increment,
                                           int position)
    {
        String key = AutoincrementCounter.makeIdentity(s,t,c);
        
        if (autoincrementCacheHashtable == null)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            autoincrementCacheHashtable = new HashMap<String,AutoincrementCounter>();
        }

        AutoincrementCounter aic = 
            autoincrementCacheHashtable.get(key);
        if (aic != null)
        {
            if (SanityManager.DEBUG)            
            {
                SanityManager.THROWASSERT(
                              "Autoincrement Counter already exists:" + key);
            }
            return;
        }
        
        aic = new AutoincrementCounter(initialValue, 
                                       increment, 0, s, t, c, position);
        autoincrementCacheHashtable.put(key, aic);
    }

    /**
     * returns the <b>next</b> value to be inserted into an autoincrement col.
     * This is used internally by the system to generate autoincrement values
     * which are going to be inserted into a autoincrement column. This is
     * used when as autoincrement column is added to a table by an alter 
     * table statemenet and during bulk insert.
     *
     * @param schemaName
     * @param tableName
     * @param columnName identify the column uniquely in the system.
     */
    public long nextAutoincrementValue(String schemaName, String tableName,
                                       String columnName)
            throws StandardException                           
    {
        String key = AutoincrementCounter.makeIdentity(schemaName,tableName,
                                                       columnName);
        
        AutoincrementCounter aic = 
            autoincrementCacheHashtable.get(key);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

        if (aic == null)
        {
            if (SanityManager.DEBUG)            
            {
                SanityManager.THROWASSERT("counter doesn't exist:" + key);
            }
            return 0;
        }
        else
        {
            return aic.update();
        }
    }
    
    /**
     * Flush the cache of autoincrement values being kept by the lcc.
     * This will result in the autoincrement values being written to the
     * SYSCOLUMNS table as well as the mapping used by lastAutoincrementValue
     * 
     * @exception StandardException thrown on error.
     * @see LanguageConnectionContext#lastAutoincrementValue
     * @see GenericLanguageConnectionContext#lastAutoincrementValue
     */
    public void autoincrementFlushCache(UUID tableUUID)
        throws StandardException
    {
        if (autoincrementCacheHashtable == null)
            return;

        if (autoincrementHT == null)
            autoincrementHT = new HashMap<String,Long>();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

        DataDictionary dd = getDataDictionary();
        for (Iterator<String> it = autoincrementCacheHashtable.keySet().iterator();
             it.hasNext(); )
        {
            String key = it.next();
            AutoincrementCounter aic = 
                autoincrementCacheHashtable.get(key);
            Long value = aic.getCurrentValue();
            aic.flushToDisk(getTransactionExecute(), dd, tableUUID);
            if (value != null)
            {
                autoincrementHT.put(key, value);
            }
        }
        autoincrementCacheHashtable.clear();
    }

    /**
     * Copies an existing autoincrement mapping
     * into autoincrementHT, the cache of autoincrement values 
     * kept in the languageconnectioncontext.
     */
    public void copyHashtableToAIHT(Map<String,Long> from)
    {
        if (from.isEmpty())
            return;
        if (autoincrementHT == null)
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            autoincrementHT = new HashMap<String,Long>();
        
        autoincrementHT.putAll(from);
    }
    
    /**
     * @see LanguageConnectionContext#getInstanceNumber
     */
    public int getInstanceNumber()
    {
        return instanceNumber;
    }

    /**
     * @see LanguageConnectionContext#getDrdaID
     */
    public String getDrdaID()
    {
        return drdaID;
    }

    /**
     * @see LanguageConnectionContext#setDrdaID
     */
    public void setDrdaID(String drdaID)
    {
        this.drdaID = drdaID;
    }

    /**
     * @see LanguageConnectionContext#getDbname
     */
    public String getDbname()
    {
        return dbname;
    }

    /**
     * @see LanguageConnectionContext#getLastActivation
     */
    public Activation getLastActivation()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        return acts.get(acts.size() - 1);
    }

    @Override
    public StringBuffer appendErrorInfo() {

        TransactionController tc = getTransactionExecute();
        if (tc == null)
            return null;

        StringBuffer sb = new StringBuffer(200);

        sb.append(LanguageConnectionContext.xidStr);
        sb.append(tc.getTransactionIdString());
        sb.append("), ");

        sb.append(LanguageConnectionContext.lccStr);
        sb.append(Integer.toString(getInstanceNumber()));
        sb.append("), ");

        sb.append(LanguageConnectionContext.dbnameStr);
        sb.append(getDbname());
        sb.append("), ");

        sb.append(LanguageConnectionContext.drdaStr);
        sb.append(getDrdaID());
        sb.append("), ");

        return sb;
    }


    /**
     * @see LanguageConnectionContext#setCurrentRole(Activation a, String role)
     */
    public void setCurrentRole(Activation a, String role) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3897
        getCurrentSQLSessionContext(a).setRole(role);
    }


    /**
     * @see LanguageConnectionContext#getCurrentRoleId(Activation a)
     */
    public String getCurrentRoleId(Activation a) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3897
        return getCurrentSQLSessionContext(a).getRole();
    }


    /**
     * @see LanguageConnectionContext#getCurrentUserId(Activation a)
     */
    public String getCurrentUserId(Activation a) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
        return getCurrentSQLSessionContext(a).getCurrentUser();
    }


    /**
     * @see LanguageConnectionContext#getCurrentRoleIdDelimited(Activation a)
     */
    public String getCurrentRoleIdDelimited(Activation a)
            throws StandardException {

        String role = getCurrentSQLSessionContext(a).getRole();
//IC see: https://issues.apache.org/jira/browse/DERBY-3897

        if (role != null) {
            beginNestedTransaction(true);

            try {
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
                if (!roleIsSettable(a, role)) {
                    // invalid role, so lazily reset it.
                    setCurrentRole(a, null);
                    role = null;
                }
            } finally {
                commitNestedTransaction();
            }
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-3137
        if (role != null) {
            role = IdUtil.normalToDelimited(role);
        }

        return role;
    }


    /**
     * @see LanguageConnectionContext#roleIsSettable(Activation a, String role)
     */
    public boolean roleIsSettable(Activation a, String role)
            throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4551

        DataDictionary dd = getDataDictionary();
        String dbo = dd.getAuthorizationDatabaseOwner();

//IC see: https://issues.apache.org/jira/browse/DERBY-673
        RoleGrantDescriptor grantDesc;
        String currentUser = getCurrentUserId(a);

        if (currentUser.equals(dbo)) {
            grantDesc = dd.getRoleDefinitionDescriptor(role);
        } else {
            grantDesc = dd.getRoleGrantDescriptor
                (role, currentUser, dbo);

            if (grantDesc == null) {
                // or if not, via PUBLIC?
                grantDesc = dd.getRoleGrantDescriptor
                    (role, Authorizer.PUBLIC_AUTHORIZATION_ID,dbo);
            }
        }
        return grantDesc != null;
    }

    /**
     * Return the current SQL session context of the activation
     *
     * @param activation the activation
     */
    public SQLSessionContext getCurrentSQLSessionContext(
        Activation activation) {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419

        SQLSessionContext curr;

        Activation parent = activation.getParentActivation();
//IC see: https://issues.apache.org/jira/browse/DERBY-3897

        if (parent == null ) {
            // top level
            curr = getTopLevelSQLSessionContext();
        } else {
            // inside a nested connection (stored procedure/function), or when
            // executing a substatement the SQL session context is maintained
            // in the activation of the parent
            curr = parent.getSQLSessionContextForChildren();
        }

        return curr;
    }


    /**
     * Return the current SQL session context based on statement context
     */
    private SQLSessionContext getCurrentSQLSessionContext() {
        StatementContext ctx = getStatementContext();
        SQLSessionContext curr;

        if (ctx == null || !ctx.inUse()) {
            curr = getTopLevelSQLSessionContext();
        } else {
            // We are inside a nested connection in a procedure of
            // function.
            curr = ctx.getSQLSessionContext();

            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(
                    curr != null,
                    "SQL session context should never be empty here");
            }
        }

        return curr;
    }


    /**
     * {@inheritDoc}
     */
    public void pushNestedSessionContext(
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
        Activation a,
        boolean definersRights,
        String definer) throws StandardException {

        setupSessionContextMinion(a, true, definersRights, definer);
    }

    private void setupSessionContextMinion(
        Activation a,
        boolean push,
        boolean definersRights,
        String definer) throws StandardException {

        if (SanityManager.DEBUG) {
            if (definersRights) {
                SanityManager.ASSERT(push);
            }
        }

        SQLSessionContext sc = a.setupSQLSessionContextForChildren(push);

        if (definersRights) {
            sc.setUser(definer);
        } else {
            // A priori: invoker's rights: Current user
            sc.setUser(getCurrentUserId(a));
        }


        if (definersRights) {
            // No role a priori. Cf. SQL 2008, section 10.4 <routine
            // invocation>, GR 5 j) i) 1) B) "If the external security
            // characteristic of R is DEFINER, then the top cell of the
            // authorization stack of RSC is set to contain only the routine
            // authorization identifier of R.

            sc.setRole(null);
        } else {
            // Semantics for roles dictate (SQL 4.34.1.1 and 4.27.3.) that the
            // role is initially inherited from the current session context
            // when we run with INVOKER security characteristic.
            sc.setRole(getCurrentRoleId(a));
        }


        if (definersRights) {
            SchemaDescriptor sd = getDataDictionary().getSchemaDescriptor(
                definer,
                getTransactionExecute(),
                false);

            if (sd == null) {
                sd = new SchemaDescriptor(
                    getDataDictionary(), definer, definer, (UUID) null, false);
            }

            sc.setDefaultSchema(sd);
        } else {
            // Inherit current default schema. The initial value of the
            // default schema is implementation defined. In Derby we
            // inherit it when we invoke stored procedures and functions.
            sc.setDefaultSchema(getDefaultSchema(a));
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419
        final SQLSessionContext ssc = getCurrentSQLSessionContext(a);
        sc.setDeferredAll(ssc.getDeferredAll());
        sc.setConstraintModes(ssc.getConstraintModes());
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665

        StatementContext stmctx = getStatementContext();

        // Since the statement is an invocation (iff push=true), it will now be
        // associated with the pushed SQLSessionContext (and no longer just
        // share that of its caller (or top).  The statement contexts of nested
        // connection statements will inherit statement context so the SQL
        // session context is available through it when nested statements are
        // compiled (and executed, for the most part).  However, for dynamic
        // result sets, the relevant statement context (originating result set)
        // is no longer available for execution time references to the SQL
        // session context, so we rely on the activation of the caller for
        // accessing it, cf. e.g. overload variants of
        // getDefaultSchema/setDefaultSchema.  If such nested connections
        // themselves turn out to be invocations, they in turn get a new
        // SQLSessionContext associated with them etc.
        stmctx.setSQLSessionContext(sc);
    }

    /**
     * @param a {@inheritDoc}
     * @throws StandardException {@inheritDoc}
     */
    public void popNestedSessionContext(Activation a) throws StandardException {
        SQLSessionContext nested = a.getSQLSessionContextForChildren();
        SQLSessionContext caller = getCurrentSQLSessionContext(a);

        compareConstraintModes(nested, caller);
    }

    private void compareConstraintModes(
            SQLSessionContext nested,
            SQLSessionContext caller) throws StandardException {

        if (deferredHashTables == null) {
            // Nothing to do
            return;
        }

        // Check all constraints that were deferred inside the routine
        // but whose constraint mode is immediate on the outside. If
        // any of these violate the constraints, roll back.
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
        for (ValidationInfo info : deferredHashTables.values()) {
            info.possiblyValidateOnReturn(this, nested, caller);
        }
    }

    public boolean isEffectivelyDeferred(SQLSessionContext sc, UUID constraintId)
            throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-532

        final Boolean deferred = sc.isDeferred(constraintId);
        final boolean effectivelyDeferred;
        final DataDictionary dd = getDataDictionary();

        if (deferred != null) {
            effectivelyDeferred = deferred.booleanValue();
        } else {
            // no explicit setting applicable, use initial constraint mode
            final ConstraintDescriptor conDesc =
                    dd.getConstraintDescriptor(constraintId);
            effectivelyDeferred = conDesc.initiallyDeferred();
        }

        return effectivelyDeferred;
    }


    /**
     * @see LanguageConnectionContext#setupSubStatementSessionContext(Activation a)
     */
    public void setupSubStatementSessionContext(Activation a)
            throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4551

        setupSessionContextMinion(a, false, false, null);
    }


    /**
     * @see GenericLanguageConnectionContext#topLevelSSC
     */
    public SQLSessionContext getTopLevelSQLSessionContext() {
        if (topLevelSSC == null) {
            topLevelSSC = new SQLSessionContextImpl(
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
                getInitialDefaultSchemaDescriptor(),
                getSessionUserId());
        }
        return topLevelSSC;
    }


    /**
     * @see LanguageConnectionContext#createSQLSessionContext
     */
    public SQLSessionContext createSQLSessionContext() {
        return new SQLSessionContextImpl(
            getInitialDefaultSchemaDescriptor(),
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419
            getSessionUserId()); /* a priori */
    }

    /**
     * This holds a map of AST nodes that have already been printed during a
     * compiler phase, so as to be able to avoid printing a node more than once.
     * @see org.apache.derby.impl.sql.compile.QueryTreeNode#treePrint(int)
     */
    Map<Object,Object> printedObjectsMap = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

    /**
     * @see org.apache.derby.iapi.sql.conn.LanguageConnectionContext#getPrintedObjectsMap
     */
    public Map<Object,Object> getPrintedObjectsMap() {
//IC see: https://issues.apache.org/jira/browse/DERBY-4087
        if (printedObjectsMap == null) {
            printedObjectsMap = new IdentityHashMap<Object,Object>();
        }
        return printedObjectsMap;
    }

    /** @see LanguageConnectionContext#getXplainOnlyMode() */
    public boolean getXplainOnlyMode() {
        return xplainOnlyMode;
    }
    
    /** @see LanguageConnectionContext#setXplainOnlyMode(boolean) */
    public void setXplainOnlyMode(boolean onOrOff) {
        xplainOnlyMode = onOrOff;
    }
    
    /** @see LanguageConnectionContext#getXplainSchema() */
    public String getXplainSchema() {
        return xplain_schema;
    }
    
    /** @see LanguageConnectionContext#setXplainSchema(String) */
    public void setXplainSchema(String s) {
        xplain_schema = s;
    }
    public void setXplainStatement(Object key, Object stmt)
    {
        xplain_statements.put(key, stmt);
    }
    public Object getXplainStatement(Object key)
    {
        return xplain_statements.get(key);
    }
    
    public void setASTVisitor( ASTVisitor visitor )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4415
        astWalker = visitor;
    }
    public ASTVisitor getASTVisitor( )
    {
        return astWalker;
    }

    public void setInterruptedException(StandardException e) {
        interruptedException = e;
    }

    public StandardException getInterruptedException() {
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
        return interruptedException;
    }

    public FormatableBitSet getReferencedColumnMap(TableDescriptor td) {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        return referencedColumnMap.get(td);
    }

    public void setReferencedColumnMap(TableDescriptor td,
//IC see: https://issues.apache.org/jira/browse/DERBY-4895
                                       FormatableBitSet map) {
        referencedColumnMap.put(td, map);
    }

    /**
     * Set the constraint mode to deferred for the specified constraint.
     *
     * @param a             activation
     * @param cd            the constraint descriptor
     * @param deferred      the constraint mode
     * @throws StandardException standard error policy
     */
    public void setConstraintDeferred(
            final Activation a,
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            ConstraintDescriptor cd,
            final boolean deferred) throws StandardException {

        if (!deferred) {
            // Moving to immediate, check what's done in this transaction first
            validateDeferredConstraint(cd);
        }

        getCurrentSQLSessionContext(a).setDeferred(cd.getUUID(), deferred);
    }

    public void checkIntegrity() throws StandardException {
        validateDeferredConstraints(true);
        clearDeferreds();
    }

    /**
     * Clear deferred information for this transaction.
     */
    private void clearDeferreds() {
        deferredHashTables = null;
        getCurrentSQLSessionContext().resetConstraintModes();
    }

    public void setDeferredAll(final Activation a, final boolean deferred)
            throws StandardException {
        if (!deferred) {
            validateDeferredConstraints(false);

            // No violations, bug can't forget since we might roll back to a
            // savepoint that migh re-introduce the violations
            // DERBY-6670
            // deferredHashTables = null;
        }
        getCurrentSQLSessionContext(a).setDeferredAll(
            Boolean.valueOf(deferred));
    }

    public HashMap<UUID, ValidationInfo> getDeferredHashTables() {
        if (deferredHashTables == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            deferredHashTables = new HashMap<UUID, ValidationInfo>();
        }
        return deferredHashTables;
    }

    /**
     * Validate all deferred constraints.
     */
    private void validateDeferredConstraints(final boolean rollbackOnError)
            throws StandardException {

        if (deferredHashTables == null) {
            // Nothing to do
            return;
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
        for (ValidationInfo info : deferredHashTables.values()) {
            info.validateConstraint(this, null, rollbackOnError);
        }
    }

    /**
     * Validate a deferred constraint.
     *
     * @param cd the descriptor of the constraint to validate
     */
    private void validateDeferredConstraint(ConstraintDescriptor cd)
            throws StandardException {

        if (deferredHashTables == null) {
            // Nothing to do.
            return;
        }

        // For CHECK constraints, the key is the table id. All other
        // constraints use the constraint id as key.
        UUID key = cd.hasBackingIndex() ? cd.getUUID() : cd.getTableId();

        ValidationInfo vi = deferredHashTables.get(key);

        if (vi == null) {
            // Nothing to do
            return;
        }

        vi.validateConstraint(this, cd.getUUID(), false);

        // No violations, bug can't forget since we might roll back to a
        // savepoint that migh re-introduce the violations
        // DERBY-6670-
        //
        // deferredHashTables.remove(key);
    }
}
