/*

   Derby - Class org.apache.derby.impl.sql.execute.xplain.XPLAINSystemTableVisitor

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute.xplain;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Stack;

import org.apache.derby.catalog.UUID;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.jdbc.ConnectionContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.iapi.services.io.FormatableProperties;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetTimingsDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINScanPropsDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINSortPropsDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINStatementDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINStatementTimingsDescriptor;
import org.apache.derby.iapi.sql.execute.RunTimeStatistics;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.compile.IntersectOrExceptNode;
import org.apache.derby.impl.sql.execute.rts.RealAnyResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealDeleteCascadeResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealDeleteResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealDeleteVTIResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealDistinctScalarAggregateStatistics;
import org.apache.derby.impl.sql.execute.rts.RealDistinctScanStatistics;
import org.apache.derby.impl.sql.execute.rts.RealGroupedAggregateStatistics;
import org.apache.derby.impl.sql.execute.rts.RealHashJoinStatistics;
import org.apache.derby.impl.sql.execute.rts.RealHashLeftOuterJoinStatistics;
import org.apache.derby.impl.sql.execute.rts.RealHashScanStatistics;
import org.apache.derby.impl.sql.execute.rts.RealHashTableStatistics;
import org.apache.derby.impl.sql.execute.rts.RealIndexRowToBaseRowStatistics;
import org.apache.derby.impl.sql.execute.rts.RealInsertResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealInsertVTIResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealLastIndexKeyScanStatistics;
import org.apache.derby.impl.sql.execute.rts.RealMaterializedResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealNestedLoopJoinStatistics;
import org.apache.derby.impl.sql.execute.rts.RealNestedLoopLeftOuterJoinStatistics;
import org.apache.derby.impl.sql.execute.rts.RealNormalizeResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealOnceResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealProjectRestrictStatistics;
import org.apache.derby.impl.sql.execute.rts.RealRowResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealScalarAggregateStatistics;
import org.apache.derby.impl.sql.execute.rts.RealScrollInsensitiveResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealSetOpResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealSortStatistics;
import org.apache.derby.impl.sql.execute.rts.RealTableScanStatistics;
import org.apache.derby.impl.sql.execute.rts.RealUnionResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealUpdateResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealVTIStatistics;
import org.apache.derby.impl.sql.execute.rts.ResultSetStatistics;

/**
 * This is the Visitor, which explains the information and stores the statistics in 
 * the system catalogs. It traverses the result set statistics tree and extracts the
 * information.  
 *
 */
public class XPLAINSystemTableVisitor implements XPLAINVisitor {
    
    private boolean no_call_stmts = true;
    
    
    // ---------------------------------------------------------
    // member variables
    // ---------------------------------------------------------
    
    // the needed system objects for writing to the dictionary
    private LanguageConnectionContext lcc;
    private DataDictionary dd;
    private TransactionController tc;
    private DataDescriptorGenerator ddg;
    
    // the stmt activation object
    private Activation activation;

    // a flag which is used to reflect if the statistics timings is on
    private boolean considerTimingInformation = false;
    
    // the different tuple descriptors describing the query characteristics
    // regarding the stmt
    private XPLAINStatementDescriptor stmt;
    private XPLAINStatementTimingsDescriptor stmtTimings = null;
    private UUID stmtUUID; // the UUID to save for the resultsets
    
    // now the lists of descriptors regarding the resultsets
    private List rsets; // for the resultset descriptors
    private List rsetsTimings; // for the resultset timings descriptors
    private List sortrsets; // for the sort props descriptors
    private List scanrsets; // fot the scan props descriptors
    
    // the number of children of the current explained node
    private int noChildren;
    
    // this stack keeps track of the result set UUIDs, which get popped by the
    // children of the current explained node
    private Stack UUIDStack;
    
    // ---------------------------------------------------------
    // Constructor
    // ---------------------------------------------------------
    
    public XPLAINSystemTableVisitor(){
        // System.out.println("System Table Visitor created...");
        // initialize lists
        rsets        = new ArrayList();
        rsetsTimings = new ArrayList();
        sortrsets    = new ArrayList();
        scanrsets    = new ArrayList();
        
        // init UUIDStack
        UUIDStack    = new Stack();
        
    }

    
    /** helper method, which pushes the UUID,
     *  "number of Children" times onto the UUIDStack.
     * @param uuid the UUID to push
     */
    private void pushUUIDnoChildren(UUID uuid){
        for (int i=0;i<noChildren;i++){
            UUIDStack.push(uuid);
        }
    }
    
    // ---------------------------------------------------------
    // XPLAINVisitor Implementation
    // ---------------------------------------------------------
    
    /**
     * this method only stores the current number of children of the current explained node.
     * The child nodes then can re-use this information.
     */
    public void setNumberOfChildren(int noChildren) {
        this.noChildren = noChildren;
    }
    
    /** 
      * Visit this node, calling back to it to get details.
      *
      * This method visits the RS Statisitcs node, calling back to the
      * node to get detailed descriptor information about it.
      */
    public void visit(ResultSetStatistics statistics)
    {
        UUID timingID = null;
        
        if(considerTimingInformation){
            timingID = dd.getUUIDFactory().createUUID();
            rsetsTimings.add( 
                    statistics.getResultSetTimingsDescriptor(timingID));
        }
        
        UUID sortID = dd.getUUIDFactory().createUUID();
        Object sortRSDescriptor = statistics.getSortPropsDescriptor(sortID);
        if (sortRSDescriptor != null)
            sortrsets.add(sortRSDescriptor);
        else
            sortID = null;
        
        UUID scanID = dd.getUUIDFactory().createUUID();
        Object scanRSDescriptor = statistics.getScanPropsDescriptor(scanID);
        if (scanRSDescriptor != null)
            scanrsets.add(scanRSDescriptor);
        else
            scanID = null;

        UUID rsID = dd.getUUIDFactory().createUUID();
        rsets.add(statistics.getResultSetDescriptor(rsID,
           UUIDStack.empty()?  null: (UUID)UUIDStack.pop(),
           scanID, sortID, stmtUUID, timingID));
        
        pushUUIDnoChildren(rsID);
    }
    
    /**
     * This method resets the visitor. Gets called right before explanation
     * to make sure all needed objects exist and are up to date and the lists are cleared
     */
    public void reset() {
        lcc = activation.getLanguageConnectionContext();
        dd  = lcc.getDataDictionary();
        tc  = lcc.getTransactionExecute();
        ddg = dd.getDataDescriptorGenerator(); 
    }
    
    /** the interface method, which gets called by the Top-ResultSet, which starts
     *  the tree traversal. 
     */
    public void doXPLAIN(RunTimeStatistics rss, Activation activation)
        throws StandardException
    {
         // save this activation
         this.activation = activation;
         
         // reset this visitor
         reset();
         
         // get the timings settings
         considerTimingInformation = lcc.getStatisticsTiming();
         
         // placeholder for the stmt timings UUID 
         UUID stmtTimingsUUID = null;
         
         //1. create new stmt timings descriptor 
         if (considerTimingInformation){
             stmtTimingsUUID = dd.getUUIDFactory().createUUID();
             Timestamp endExeTS   = rss.getEndExecutionTimestamp();
             Timestamp beginExeTS = rss.getBeginExecutionTimestamp();
             long exeTime;
             if (endExeTS!=null && beginExeTS!=null){
                 exeTime = endExeTS.getTime() - beginExeTS.getTime();
             } else {
                 exeTime = 0;
             }
             
             stmtTimings = new XPLAINStatementTimingsDescriptor(
                 stmtTimingsUUID,                    // the Timing UUID
                 new Long(rss.getParseTimeInMillis()),         // the Parse Time
                 new Long(rss.getBindTimeInMillis()),          // the Bind Time
                 new Long(rss.getOptimizeTimeInMillis()),      // the Optimize Time
                 new Long(rss.getGenerateTimeInMillis()),      // the Generate Time
                 new Long(rss.getCompileTimeInMillis()),       // the Compile Time
                 new Long(exeTime),                            // the Execute Time, TODO resolve why getExecutionTime() returns 0
                 rss.getBeginCompilationTimestamp(), // the Begin Compilation TS
                 rss.getEndCompilationTimestamp(),   // the End   Compilation TS
                 rss.getBeginExecutionTimestamp(),   // the Begin Execution   TS
                 rss.getEndExecutionTimestamp()      // the End   Execution   TS
             );
         }
         
         // 2. create new Statement Descriptor 

         // create new UUID
         stmtUUID = dd.getUUIDFactory().createUUID();
         // extract stmt type
         String type = XPLAINUtil.getStatementType(rss.getStatementText());
         
         // don`t explain CALL Statements, quick implementation
         // TODO improve usability to switch between call stmt explanation on or off
         if (type.equalsIgnoreCase("C") && no_call_stmts) return;
         
         // get transaction ID
         String xaID = lcc.getTransactionExecute().getTransactionIdString();
         // get session ID
         String sessionID = Integer.toString(lcc.getInstanceNumber());
         // get the JVM ID
         String jvmID = Integer.toString(JVMInfo.JDK_ID); 
         // get the OS ID 
         String osID  = System.getProperty("os.name"); 
         // the current system time
         long current = System.currentTimeMillis();
         // the xplain type
         String XPLAINtype = lcc.getXplainOnlyMode() ?
             XPLAINUtil.XPLAIN_ONLY : XPLAINUtil.XPLAIN_FULL;
         // the xplain time
         Timestamp time = new Timestamp(current);
         // the thread id
         String threadID = Thread.currentThread().toString();
         
         stmt = new XPLAINStatementDescriptor(
            stmtUUID,               // unique statement UUID
            rss.getStatementName(), // the statement name
            type,                   // the statement type
            rss.getStatementText(), // the statement text
            jvmID,                  // the JVM ID
            osID,                   // the OS ID
            XPLAINtype,             // the EXPLAIN tpye
            time,                   // the EXPLAIN Timestamp
            threadID,               // the Thread ID
            xaID,                   // the transaction ID
            sessionID,              // the Session ID
            lcc.getDbname(),        // the Database name
            lcc.getDrdaID(),        // the DRDA ID
            stmtTimingsUUID         // Timing ID, if available
            );
         
        try {
         // add it to system catalog
         addStmtDescriptorsToSystemCatalog();                 
         
         // get TopRSS and start the traversal of the RSS-tree
         rss.acceptFromTopResultSet(this);
         
         // add the filled lists to the dictionary
         addArraysToSystemCatalogs();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            throw StandardException.plainWrapException(e);
        }
         
         // clean up to free kept resources
         clean();
    }

    // ---------------------------------------------------------
    // helper methods
    // ---------------------------------------------------------
    
    /**
     * This method cleans up things after explanation. It frees kept 
     * resources and still holded references.
     */
    private void clean(){
        
        
        // forget about all the system objects
        activation = null;
        lcc = null;
        dd = null;
        tc = null;
        
        // forget about the stmt descriptors and the Stmt UUID
        stmtUUID = null;
        stmt = null;
        stmtTimings = null;
        
        // reset the descriptor lists to keep memory low
        rsets.clear();
        rsetsTimings.clear();
        sortrsets.clear();
        scanrsets.clear();
        
        // clear stack, although it must be already empty...
        UUIDStack.clear();
    }
    
    /**
      * Open a nested Connection with which to execute INSERT statements.
      */
    private Connection getDefaultConn()throws SQLException
    {
        ConnectionContext cc = (ConnectionContext)
            lcc.getContextManager().getContext(ConnectionContext.CONTEXT_ID);
        return cc.getNestedConnection(true);
    }
    /**
     * This method writes only the stmt and its timing descriptor
     * to the dataDictionary
     *
     */
    private void addStmtDescriptorsToSystemCatalog()
        throws StandardException, SQLException
    {
        boolean statsSave = lcc.getRunTimeStatisticsMode();
        lcc.setRunTimeStatisticsMode(false);
        Connection conn = getDefaultConn();
        PreparedStatement ps = conn.prepareStatement(
            (String)lcc.getXplainStatement("SYSXPLAIN_STATEMENTS"));
        stmt.setStatementParameters(ps);
        ps.executeUpdate();
        ps.close();
            
        if(considerTimingInformation)
        {
            ps = conn.prepareStatement(
                (String)lcc.getXplainStatement("SYSXPLAIN_STATEMENT_TIMINGS"));
            stmtTimings.setStatementParameters(ps);
            ps.executeUpdate();
            ps.close();
        }
        conn.close();
        lcc.setRunTimeStatisticsMode(statsSave);
    }
    
    /**
     * This method writes the created descriptor arrays 
     * to the cooresponding system catalogs.
     */
    private void addArraysToSystemCatalogs()
        throws StandardException, SQLException
    {
        Iterator iter;
        boolean statsSave = lcc.getRunTimeStatisticsMode();
        lcc.setRunTimeStatisticsMode(false);
        Connection conn = getDefaultConn();

        PreparedStatement ps = conn.prepareStatement(
            (String)lcc.getXplainStatement("SYSXPLAIN_RESULTSETS"));
        iter = rsets.iterator();
        while (iter.hasNext())
        {
            XPLAINResultSetDescriptor rset =
                (XPLAINResultSetDescriptor)iter.next();
            rset.setStatementParameters(ps);
            ps.executeUpdate();
        }
        ps.close();

        // add the resultset timings descriptors, if timing is on
        if(considerTimingInformation)
        {
            ps = conn.prepareStatement(
                (String)lcc.getXplainStatement("SYSXPLAIN_RESULTSET_TIMINGS"));
            iter = rsetsTimings.iterator();
            while (iter.hasNext())
            {
                XPLAINResultSetTimingsDescriptor rsetT =
                    (XPLAINResultSetTimingsDescriptor)iter.next();
                rsetT.setStatementParameters(ps);
                ps.executeUpdate();
            }
            ps.close();
        }
        ps = conn.prepareStatement(
            (String)lcc.getXplainStatement("SYSXPLAIN_SCAN_PROPS"));
        iter = scanrsets.iterator();
        while (iter.hasNext())
        {
            XPLAINScanPropsDescriptor scanProps =
                (XPLAINScanPropsDescriptor)iter.next();
            scanProps.setStatementParameters(ps);
            ps.executeUpdate();
        }
        ps.close();

        ps = conn.prepareStatement(
            (String)lcc.getXplainStatement("SYSXPLAIN_SORT_PROPS"));
        iter = sortrsets.iterator();
        while (iter.hasNext())
        {
            XPLAINSortPropsDescriptor sortProps =
                (XPLAINSortPropsDescriptor)iter.next();
            sortProps.setStatementParameters(ps);
            ps.executeUpdate();
        }
        ps.close();

        conn.close();
        lcc.setRunTimeStatisticsMode(statsSave);
    }

}
