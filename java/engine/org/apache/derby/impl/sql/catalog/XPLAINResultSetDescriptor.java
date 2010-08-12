/*

   Derby - Class org.apache.derby.impl.sql.catalog.XPLAINResultSetDescriptor

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

package org.apache.derby.impl.sql.catalog;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;
import java.sql.Types;

public class XPLAINResultSetDescriptor extends XPLAINTableDescriptor 
{

    private UUID rs_id           ; // the result set UUID identifier
    private String op_identifier ; // the operator code identifier
    private String op_details     ;     // the operator details, operator-specific information
    private Integer no_opens      ; // the number of open calls of this resultset 
    private Integer no_index_updates; // the number of index updates, executed by this dml write operation
    private String lock_granularity; // the lock granularity, either (T)able or (R)ow locking
    private String lock_mode;  // the lock mode, either instant share, share or instant exclusive, exclusive 
    private UUID parent_rs_id; // the parent UUID of this resultset, null if root (top) resultset
    private Double est_row_count; // the estimated row count, forwarded by the optimizer
    private Double est_cost; // the estimated costs, forwarded by the optimizer
    private Integer affected_rows; // the affected rows, specific for insert/update/delete stmts
    private String  deferred_rows; // the deferred rows, specific for insert/update/delete stmts
    private Integer input_rows; // the number of input rows
    private Integer seen_rows; // the seen rows from this operator 
    private Integer seen_rows_right; // the seen right rows from this operator, only filled by a join operator, seen_rows has then the rows from the outer(left) partner of the join 
    private Integer filtered_rows; // the filtered rows
    private Integer returned_rows; // the returned rows
    private Integer empty_right_rows; // the number of empty right rows 
    private String index_key_optimization; // does this node use index key optimization
    private UUID scan_rs_id; // the UUID of the scan info properties of this node, if this node is a scan node, otherwise null
    private UUID sort_rs_id; // the UUID of the sort info properties of this node. if this node is a groupby or sort node, otherwise null
    private UUID stmt_id; // the UUID of the statement, which this resultset belongs to
    private UUID timing_id; // the UUID of the resultset timing information, if statistics timing was on, otherwise null
    
    public XPLAINResultSetDescriptor() {}
    public XPLAINResultSetDescriptor
    (
             UUID rs_id,
             String op_identifier,
             String op_details,
             Integer no_opens,
             Integer no_index_updates,
             String lock_mode,
             String lock_granularity,
             UUID parent_rs_id,
             Double est_row_count,
             Double est_cost,
             Integer affected_rows,
             String deferred_rows,
             Integer input_rows,
             Integer seen_rows,
             Integer seen_rows_right,
             Integer filtered_rows,
             Integer returned_rows,
             Integer empty_right_rows,
             String index_key_optimization,
             UUID scan_rs_id,
             UUID sort_rs_id,
             UUID stmt_id,
             UUID timing_id
    )
    {

        this.rs_id=  rs_id;
        this.op_identifier = op_identifier;
        this.op_details = op_details;
        this.no_opens = no_opens;
        this.no_index_updates = no_index_updates;
        this.lock_granularity = lock_granularity;
        this.lock_mode = lock_mode;
        this.parent_rs_id = parent_rs_id;
        this.est_row_count = est_row_count;
        this.est_cost = est_cost;
        this.affected_rows = affected_rows;
        this.deferred_rows = deferred_rows;
        this.input_rows = input_rows;
        this.seen_rows = seen_rows;
        this.seen_rows_right = seen_rows_right;
        this.filtered_rows = filtered_rows;
        this.returned_rows = returned_rows;
        this.empty_right_rows = empty_right_rows;
        this.index_key_optimization = index_key_optimization;
        this.scan_rs_id = scan_rs_id;
        this.sort_rs_id = sort_rs_id;
        this.stmt_id = stmt_id;
        this.timing_id = timing_id;
        
    }
    public void setStatementParameters(PreparedStatement ps)
        throws SQLException
    {
        ps.setString(1, rs_id.toString());
        ps.setString(2, op_identifier);
        ps.setString(3, op_details);
        if (no_opens != null)
            ps.setInt(4, no_opens.intValue());
        else
            ps.setNull(4, Types.INTEGER);
        if (no_index_updates != null)
            ps.setInt(5, no_index_updates.intValue());
        else
            ps.setNull(5, Types.INTEGER);
        ps.setString(6, lock_mode);
        ps.setString(7, lock_granularity);
        ps.setString(8, (parent_rs_id != null ? parent_rs_id.toString():null));
        if (est_row_count != null)
            ps.setDouble(9, est_row_count.doubleValue());
        else
            ps.setNull(9, Types.DOUBLE);
        if (est_cost != null)
            ps.setDouble(10, est_cost.doubleValue());
        else
            ps.setNull(10, Types.DOUBLE);
        if (affected_rows != null)
            ps.setInt(11, affected_rows.intValue());
        else
            ps.setNull(11, Types.INTEGER);
        ps.setString(12, deferred_rows);
        if (input_rows != null)
            ps.setInt(13, input_rows.intValue());
        else
            ps.setNull(13, Types.INTEGER);
        if (seen_rows != null)
            ps.setInt(14, seen_rows.intValue());
        else
            ps.setNull(14, Types.INTEGER);
        if (seen_rows_right != null)
            ps.setInt(15, seen_rows_right.intValue());
        else
            ps.setNull(15, Types.INTEGER);
        if (filtered_rows != null)
            ps.setInt(16, filtered_rows.intValue());
        else
            ps.setNull(16, Types.INTEGER);
        if (returned_rows != null)
            ps.setInt(17, returned_rows.intValue());
        else
            ps.setNull(17, Types.INTEGER);
        if (empty_right_rows != null)
            ps.setInt(18, empty_right_rows.intValue());
        else
            ps.setNull(18, Types.INTEGER);
        ps.setString(19, index_key_optimization);
        ps.setString(20, (scan_rs_id != null ? scan_rs_id.toString():null));
        ps.setString(21, (sort_rs_id != null ? sort_rs_id.toString():null));
        ps.setString(22, (stmt_id != null ? stmt_id.toString():null));
        ps.setString(23, (timing_id != null ? timing_id.toString():null));
    }
    
    public String getCatalogName() { return TABLENAME_STRING; }
    static final String             TABLENAME_STRING = "SYSXPLAIN_RESULTSETS";

    private static final String[][] indexColumnNames =
    {
        {"RS_ID"}
    };

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList() {
        
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("RS_ID", false),
            SystemColumnImpl.getColumn("OP_IDENTIFIER",Types.VARCHAR,false,30),
            SystemColumnImpl.getColumn("OP_DETAILS", Types.VARCHAR, true, 256),
            SystemColumnImpl.getColumn("NO_OPENS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("NO_INDEX_UPDATES", Types.INTEGER, true),
            SystemColumnImpl.getColumn("LOCK_MODE", Types.CHAR, true, 2),
            SystemColumnImpl.getColumn("LOCK_GRANULARITY", Types.CHAR, true, 1),
            SystemColumnImpl.getUUIDColumn("PARENT_RS_ID", true),
            SystemColumnImpl.getColumn("EST_ROW_COUNT", Types.DOUBLE, true),
            SystemColumnImpl.getColumn("EST_COST", Types.DOUBLE, true),
            SystemColumnImpl.getColumn("AFFECTED_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("DEFERRED_ROWS", Types.CHAR, true, 1),
            SystemColumnImpl.getColumn("INPUT_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("SEEN_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("SEEN_ROWS_RIGHT", Types.INTEGER, true),
            SystemColumnImpl.getColumn("FILTERED_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("RETURNED_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("EMPTY_RIGHT_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("INDEX_KEY_OPT", Types.CHAR, true, 1),
            SystemColumnImpl.getUUIDColumn("SCAN_RS_ID", true),
            SystemColumnImpl.getUUIDColumn("SORT_RS_ID", true),
            SystemColumnImpl.getUUIDColumn("STMT_ID", false),
            SystemColumnImpl.getUUIDColumn("TIMING_ID", true),
        };
    }

}
