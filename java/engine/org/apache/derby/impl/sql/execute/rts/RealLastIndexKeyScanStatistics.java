/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealLastIndexKeyScanStatistics

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

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

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
  ResultSetStatistics implemenation for RealLastIndexKeyScanResultSet.


*/
public class RealLastIndexKeyScanStatistics 
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public String isolationLevel;
	public String tableName;
	public String indexName;
	public String lockString;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealLastIndexKeyScanStatistics(int numOpens,
									long constructorTime,
									long openTime,
									long nextTime,
									long closeTime,
									int resultSetNumber,
									String tableName,
									String indexName,
									String isolationLevel,
									String lockString,
									double optimizerEstimatedRowCount,
									double optimizerEstimatedCost
									)
	{
		super(
			numOpens,
			1,
			0,
			constructorTime,
			openTime,
			nextTime,
			closeTime,
			resultSetNumber,
			optimizerEstimatedRowCount,
			optimizerEstimatedCost
			);
		this.tableName = tableName;
		this.indexName = indexName;
		this.isolationLevel = isolationLevel;
		this.lockString = lockString;
	}

	// ResultSetStatistics methods

	/**
	 * Return the statement execution plan as a String.
	 *
	 * @param depth	Indentation level.
	 *
	 * @return String	The statement executio plan as a String.
	 */
	public String getStatementExecutionPlanText(int depth)
	{
		String header;
		String isolationString = null;

		initFormatInfo(depth);

		header =
				indent + MessageService.getTextMessage(
												SQLState.RTS_LKIS_RS,
												tableName,
												indexName);

		header = header + MessageService.getTextMessage(
												SQLState.RTS_LOCKING_OPTIMIZER,
												isolationLevel,
												lockString);

		header = header + "\n";

		return
			header +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
					" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_SEEN) +
					" = " + numOpens + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
			((rowsSeen > 0) 
				?
					subIndent + MessageService.getTextMessage(
													SQLState.RTS_NEXT_TIME) +
						" = " + (nextTime / numOpens) + "\n"
				: 
					"") + "\n" +
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
			return (String)"";
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
      return MessageService.getTextMessage(
										SQLState.RTS_ON_USING,
										tableName,
										indexName);
  }
  
  // -----------------------------------------------------
  // XPLAINable Implementation
  // -----------------------------------------------------
  
    public void accept(XPLAINVisitor visitor) {
        //inform the visitor
        visitor.setNumberOfChildren(0);

        // pre-order, depth-first traversal
        // me first
        visitor.visit(this);
        // I'm a leaf node and I have no children ...
        
    }
    public String getRSXplainType() { return XPLAINUtil.OP_LASTINDEXKEYSCAN; }
    public String getRSXplainDetails()
    {
        return "I: " + this.indexName + ", T: " + this.tableName;
    }
    public Object getScanPropsDescriptor(Object scanPropsID)
    {
        String isoLevel = XPLAINUtil.getIsolationLevelCode(this.isolationLevel);
        
        XPLAINScanPropsDescriptor scanRSDescriptor =            
              new XPLAINScanPropsDescriptor(
              (UUID)scanPropsID,      // the scan props UUID
              this.indexName,
              "I",
              null,             // the scan type: heap, btree, sort
              isoLevel,         // the isolation level
              null,             // the number of visited pages
              null,             // the number of visited rows
              null,             // the number of qualified rows
              null,             // the number of visited deleted rows
              null,             // the number of fetched columns
              null,             // the bitset of fetched columns
              null,             // the btree height
              null,             // the fetch size
              null,                          // the start position, internal encoding
              null,                          // the stop position, internal encoding
              null,                          // the scan qualifiers
              null,     // the next qualifiers
              null,               // the hash key column numbers
              null // the hash table size
            );
        
        return scanRSDescriptor;
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
           this.numOpens,
           null,                           // the number of index updates 
           lockMode,
           lockGran,
           (UUID)parentID,
           this.optimizerEstimatedRowCount,
           this.optimizerEstimatedCost,
           null,                              // the affected rows
           null,                              // the deferred rows
           null,                              // the input rows
           this.rowsSeen,            // the seen rows
           null,                              // the seen rows right
           this.rowsFiltered,        // the filtered rows
           this.rowsSeen-this.rowsFiltered,// the returned rows
           null,                              // the empty right rows
           null,                           // index key optimization
           (UUID)scanID,
           (UUID)sortID,
           (UUID)stmtID,                       // the stmt UUID
           (UUID)timingID);
    }
}
