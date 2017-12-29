/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealHashScanStatistics

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

import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.util.PropertyUtil;
import org.apache.derby.iapi.util.StringUtil;

import org.apache.derby.shared.common.i18n.MessageService;
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
import java.util.Hashtable;
import java.util.Properties;


/**
  ResultSetStatistics implemenation for HashScanResultSet.


*/
public class RealHashScanStatistics
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public boolean isConstraint;
	public int hashtableSize;
	public int[] hashKeyColumns;
	public String isolationLevel;
	public String lockString;
	public String tableName;
	public String indexName;
	public String nextQualifiers;
	public String scanQualifiers;
	public String startPosition = null;
	public String stopPosition = null;
	public FormatableProperties scanProperties;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealHashScanStatistics(
									int numOpens,
									int rowsSeen,
									int rowsFiltered,
									long constructorTime,
									long openTime,
									long nextTime,
									long closeTime,
									int resultSetNumber,
									String tableName,
									String indexName,
									boolean isConstraint,
									int hashtableSize,
									int[] hashKeyColumns,
									String scanQualifiers,
									String nextQualifiers,
									Properties scanProperties,
									String startPosition,
									String stopPosition,
									String isolationLevel,
									String lockString,
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
		this.indexName = indexName;
		this.isConstraint = isConstraint;
		this.hashtableSize = hashtableSize;
		this.hashKeyColumns = ArrayUtil.copy( hashKeyColumns );
		this.scanQualifiers = scanQualifiers;
		this.nextQualifiers = nextQualifiers;
		this.scanProperties = new FormatableProperties();
		if (scanProperties != null)
		{
			for (Enumeration e = scanProperties.keys(); e.hasMoreElements(); )
			{
				String key = (String)e.nextElement();
				this.scanProperties.put(key, scanProperties.get(key));
			}
		}
		this.startPosition = startPosition;
		this.stopPosition = stopPosition;
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

		if (indexName != null)
		{
            // note that the "constraint" and "index" literals are names of SQL
            // objects and so do not need to be internationalized
			header =
				indent +
					MessageService.getTextMessage(
										SQLState.RTS_HASH_SCAN_RS_USING,
										tableName,
                                        isConstraint ? "constraint" : "index",
										indexName);
		}
		else
		{
			header =
				indent +
					MessageService.getTextMessage(SQLState.RTS_HASH_SCAN_RS,
														tableName);
		}

		header = header + " " +
					MessageService.getTextMessage(
										SQLState.RTS_LOCKING,
										isolationLevel,
										lockString) +
					": \n";

		String scanInfo =
			indent +
					MessageService.getTextMessage(SQLState.RTS_SCAN_INFO) +
					": \n" +
					PropertyUtil.sortProperties(scanProperties, subIndent);
		
		String hashKeyColumnString;
		if (hashKeyColumns.length == 1)
		{
			hashKeyColumnString = MessageService.getTextMessage(
														SQLState.RTS_HASH_KEY) +
									" " + hashKeyColumns[0];
		}
		else
		{
			hashKeyColumnString = MessageService.getTextMessage(
													SQLState.RTS_HASH_KEYS) +
									" (" + hashKeyColumns[0];
			for (int index = 1; index < hashKeyColumns.length; index++)
			{
				hashKeyColumnString = hashKeyColumnString + "," + hashKeyColumns[index];
			}
			hashKeyColumnString = hashKeyColumnString + ")";
		}

		return
			header +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
						" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_HASH_TABLE_SIZE) +
						" = " + hashtableSize + "\n" +
			indent + hashKeyColumnString + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_SEEN) +
						" = " + rowsSeen + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_FILTERED) +
						" = " + rowsFiltered + "\n" +
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
			subIndent + MessageService.getTextMessage(
													SQLState.RTS_SCAN_QUALS) +
			":\n" + StringUtil.ensureIndent(scanQualifiers, depth + 2) + "\n" +
			subIndent + MessageService.getTextMessage(
													SQLState.RTS_NEXT_QUALS) +
			":\n" + StringUtil.ensureIndent(nextQualifiers, depth + 2) + "\n" +

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
	 * If this node is on a database item (like a table or an index), then provide a
   * string that describes the on item.
   *
	 */
  public String getNodeOn(){
    return MessageService.getTextMessage(
										SQLState.RTS_ON_USING,
										tableName,
										indexName);
  }
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_HASH_SCAN);
  }
  
  // -----------------------------------------------------
  // XPLAINable Implementation
  // -----------------------------------------------------
  
    public void accept(XPLAINVisitor visitor) {
        //inform the visitor about my children
        visitor.setNumberOfChildren(0);
        
        // pre-order, depth-first traversal
        // me first
        visitor.visit(this);
        // I'm a leaf node, I have no children ...
        
    }
    public String getRSXplainType() { return XPLAINUtil.OP_HASHSCAN; }
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

        if(this.indexName!=null){
            if(this.isConstraint){
                scanObjectType = "C";  // constraint
                scanObjectName = this.indexName;
            } else {
                scanObjectType = "I";  // index
                scanObjectName = this.indexName;
            }
        } else {
            scanObjectType = "T";      // table
            scanObjectName = this.tableName;
        }
        
        String isoLevel = XPLAINUtil.getIsolationLevelCode(this.isolationLevel);
        String hashkey_columns =
            XPLAINUtil.getHashKeyColumnNumberString(this.hashKeyColumns);
        
        XPLAINScanPropsDescriptor scanRSDescriptor =            
              new XPLAINScanPropsDescriptor(
              (UUID)scanPropsID,      // the scan props UUID
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
              null,             // the fetch size
              this.startPosition,
              this.stopPosition,
              this.scanQualifiers,
              this.nextQualifiers,
              hashkey_columns,
              this.hashtableSize
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
           this.numOpens,            // the number of opens
           null,                           // the number of index updates 
           lockMode,                       // lock mode
           lockGran,                       // lock granularity
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
           (UUID)stmtID,
           (UUID)timingID);
    }
}
