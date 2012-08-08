 /*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealTableScanStatistics

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

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.util.PropertyUtil;
import org.apache.derby.iapi.util.StringUtil;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableProperties;
import org.apache.derby.catalog.UUID;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetTimingsDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINScanPropsDescriptor;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Enumeration;
import java.util.Properties;


/**
  ResultSetStatistics implemenation for TableScanResultSet.


*/
public class RealTableScanStatistics 
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public boolean isConstraint;
	public boolean coarserLock;
	public int		fetchSize;
	public String isolationLevel;
	public String tableName;
	public String userSuppliedOptimizerOverrides;
	public String indexName;
	public String lockString;
	public String qualifiers;
	public String startPosition;
	public String stopPosition;
	public FormatableProperties scanProperties;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealTableScanStatistics(int numOpens,
									int rowsSeen,
									int rowsFiltered,
									long constructorTime,
									long openTime,
									long nextTime,
									long closeTime,
									int resultSetNumber,
									String tableName,
									String userSuppliedOptimizerOverrides,
									String indexName,
									boolean isConstraint,
									String qualifiers,
									Properties scanProperties,
									String startPosition,
									String stopPosition,
									String isolationLevel,
									String lockString,
									int fetchSize,
									boolean coarserLock,
									double optimizerEstimatedRowCount,
									double optimizerEstimatedCost
									)
	{
		super(
			numOpens,
			rowsSeen,
			rowsFiltered,
			constructorTime,
			openTime,
			nextTime,
			closeTime,
			resultSetNumber,
			optimizerEstimatedRowCount,
			optimizerEstimatedCost
			);
		this.tableName = tableName;
		this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
		this.indexName = indexName;
		this.isConstraint = isConstraint;
		this.qualifiers = qualifiers;
		this.scanProperties = new FormatableProperties();
		for (Enumeration e = scanProperties.keys(); e.hasMoreElements(); )
		{
			String key = (String)e.nextElement();
			this.scanProperties.put(key, scanProperties.get(key));
		}
		this.startPosition = startPosition;
		this.stopPosition = stopPosition;
		this.isolationLevel = isolationLevel;
		this.lockString = lockString;
		this.fetchSize = fetchSize;
		this.coarserLock = coarserLock;
	}

	// ResultSetStatistics methods

	/**
	 * Return the statement execution plan as a String.
	 *
	 * @param depth	Indentation level
	 *
	 * @return String	The statement executio plan as a String.
	 */
	public String getStatementExecutionPlanText(int depth)
	{
		String header = "";
		String isolationString = null;

		initFormatInfo(depth);

		if (userSuppliedOptimizerOverrides != null)
		{ 
			header = 
				indent + MessageService.getTextMessage(SQLState.RTS_USER_SUPPLIED_OPTIMIZER_OVERRIDES_FOR_TABLE,
						tableName, userSuppliedOptimizerOverrides);
			header = header + "\n";
		}
		if (indexName != null)
		{
            // note that the "constraint" and "index" literals are names of SQL
            // objects and so do not need to be internationalized
			header = header +
				indent + MessageService.getTextMessage(
											SQLState.RTS_IS_RS_USING,
											tableName,
                                            isConstraint ? "constraint" : "index",
											indexName);
				
		}
		else
		{
			header = header +
				indent + MessageService.getTextMessage(
											SQLState.RTS_TS_RS_FOR,
											tableName);
		}

		header = header + " " + MessageService.getTextMessage(
											SQLState.RTS_LOCKING_OPTIMIZER,
											isolationLevel,
											lockString);

		/* Did we get (or already have) a coarser lock then requested
		 * due to a covering lock, lock escalation or configuration
		 * for table locking.
		 */
		if (coarserLock)
		{
			header = header + " (" + MessageService.getTextMessage(
													SQLState.RTS_ACTUAL_TABLE) +
								")";
		}

		header = header + "\n";

		String scanInfo =
			indent + MessageService.getTextMessage(SQLState.RTS_SCAN_INFO) +
						":\n" +
						PropertyUtil.sortProperties(scanProperties, subIndent);

		return
			header +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
				" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_SEEN) +
				" = " + rowsSeen + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_FILTERED) +
				" = " + rowsFiltered + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_FETCH_SIZE) +
				" = " + fetchSize + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
			((rowsSeen > 0) 
				?
					subIndent + MessageService.getTextMessage(
													SQLState.RTS_NEXT_TIME) +
								" = " + (nextTime / rowsSeen) + "\n"
				: 
					"") + "\n" +
			scanInfo +
			subIndent + MessageService.getTextMessage(
												SQLState.RTS_START_POSITION) +
			":\n" + StringUtil.ensureIndent(startPosition, depth + 2) + "\n" +
			subIndent + MessageService.getTextMessage(
												SQLState.RTS_STOP_POSITION) +
			":\n" + StringUtil.ensureIndent(stopPosition, depth + 2) + "\n" +
			subIndent + MessageService.getTextMessage(SQLState.RTS_QUALS) +
			":\n" + StringUtil.ensureIndent(qualifiers, depth + 2) + "\n" +

			// RESOLVE - estimated row count and cost will eventually 
			// be displayed for all nodes
			dumpEstimatedCosts(subIndent);
	}

	/**
	 * Return information on the scan nodes from the statement execution 
	 * plan as a String.
	 *
	 * @param depth	Indentation level.
	 * @param tableName if not NULL then print information for this table only
	 *
	 * @return String	The information on the scan nodes from the 
	 *					statement execution plan as a String.
	 */
	public String getScanStatisticsText(String tableName, int depth)
	{
		if ((tableName == null) || (tableName.equals(this.tableName)))
			return getStatementExecutionPlanText(depth);
		else
			return "";
	}


	// Class implementation
	
	public String toString()
	{
		return getStatementExecutionPlanText(0);
	}

	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
	return MessageService.getTextMessage(
				indexName == null ?
				SQLState.RTS_TABLE_SCAN :
				SQLState.RTS_INDEX_SCAN);
  }

	/**
	 * If this node is on a database item (like a table or an index), then provide a
   * string that describes the on item.
   *
	 */
  public String getNodeOn(){
    if (indexName == null)
      return MessageService.getTextMessage(SQLState.RTS_ON, tableName);
    else
      return MessageService.getTextMessage(SQLState.RTS_ON_USING,
												tableName,
												indexName);
  }
  
  // -----------------------------------------------------
  // XPLAINable Implementation
  // -----------------------------------------------------
  
    public void accept(XPLAINVisitor visitor) {
        // I have no children
        visitor.setNumberOfChildren(0);
        // pre-order, depth-first traversal
        // me first
        visitor.visit(this);
        // I'm a leaf node, I have no children ...
    }

    public String getRSXplainType()
    {
        if (this.indexName!=null)
            return this.isConstraint ? XPLAINUtil.OP_CONSTRAINTSCAN :
                                       XPLAINUtil.OP_INDEXSCAN;
        else
            return XPLAINUtil.OP_TABLESCAN;
    }
    public String getRSXplainDetails()
    {
        if (this.indexName!=null)
            return (this.isConstraint ? "C: " : "I: ") + this.indexName;
        else
            return "T: " + this.tableName;
    }
    public Object getScanPropsDescriptor(Object scanPropsID)
    {
        String scanObjectType, scanObjectName;

        if (this.indexName!=null)
        {
            if (this.isConstraint)
            {
                scanObjectType = "C";  // constraint
                scanObjectName = this.indexName;
            }
            else
            {
                scanObjectType = "I";  // index
                scanObjectName = this.indexName;
            }
        }
        else
        {
            scanObjectType = "T";      // table
            scanObjectName = this.tableName;
        }
        
        String isoLevel = XPLAINUtil.getIsolationLevelCode(this.isolationLevel);
        
        XPLAINScanPropsDescriptor scanRSDescriptor =            
              new XPLAINScanPropsDescriptor(
              (UUID)scanPropsID,
              scanObjectName,
              scanObjectType,
              null,             // the scan type: heap, btree, sort
              isoLevel,         // the isolation level
              null,             // the number of visited pages
              null,             // the number of visited rows
              null,             // the number of qualified rows
              null,             // the number of visited deleted rows
              null,             // the number of fetched columns
              null,             // the bitset of fetched columns
              null,             // the btree height
              new Integer(this.fetchSize),
              this.startPosition,
              this.stopPosition,
              this.qualifiers,
              null,             // the next qualifiers
              null,             // the hash key column numbers
              null                 // the hash table size
            );
        
        FormatableProperties props = this.scanProperties;

        return XPLAINUtil.extractScanProps(scanRSDescriptor,props);
    }
    public Object getResultSetDescriptor(Object rsID, Object parentID,
            Object scanID, Object sortID, Object stmtID, Object timingID)
    {
        String lockMode = XPLAINUtil.getLockModeCode(this.lockString);
        String lockGran = XPLAINUtil.getLockGranularityCode(this.lockString);
        
        return new XPLAINResultSetDescriptor(
           (UUID)rsID,
           getRSXplainType(),
           getRSXplainDetails(),
           new Integer(this.numOpens),
           null,                           // the number of index updates 
           lockMode,                       // lock mode
           lockGran,                       // lock granularity
           (UUID)parentID,
           new Double(this.optimizerEstimatedRowCount),
           new Double(this.optimizerEstimatedCost),
           null,                              // the affected rows
           null,                              // the deferred rows
           null,                              // the input rows
           new Integer(this.rowsSeen),            // the seen rows
           null,                              // the seen rows right
           new Integer(this.rowsFiltered),        // the filtered rows
           new Integer(this.rowsSeen-this.rowsFiltered),// the returned rows
           null,                              // the empty right rows
           null,                           // index key optimization
           (UUID)scanID,
           (UUID)sortID,
           (UUID)stmtID,                       // the stmt UUID
           (UUID)timingID);
    }
}
