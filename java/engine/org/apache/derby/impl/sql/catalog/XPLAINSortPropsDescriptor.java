package org.apache.derby.impl.sql.catalog;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;
import java.sql.Types;
/**
 * This class describes a Tuple for the XPLAIN_SORT_PROPS System Table.
 *
 */
public class XPLAINSortPropsDescriptor extends XPLAINTableDescriptor 
{

    private UUID sort_rs_id;  // the sort props UUID
    private String sort_type; // the sort type: internal or external
    private Integer no_input_rows; // the number of input rows of this sort
    private Integer no_output_rows; // the number of output rows of this sort
    private Integer no_merge_runs; // the number of merge sort runs
    private String merge_run_details; // merge run details, internal encoding
    private String eliminate_dups; // eliminate duplicates during sort
    private String in_sort_order; // is already in sorted order
    private String distinct_aggregate; // has distinct aggregates
    
    public XPLAINSortPropsDescriptor() {}
    public XPLAINSortPropsDescriptor
    (
             UUID sort_rs_id,
             String sort_type,
             Integer no_input_rows,
             Integer no_output_rows,
             Integer no_merge_runs,
             String merge_run_details,
             String eliminate_dups,
             String in_sort_order,
             String distinct_aggregate
    )
    {

        this.sort_rs_id = sort_rs_id;
        this.sort_type = sort_type;
        this.no_input_rows = no_input_rows;
        this.no_output_rows = no_output_rows;
        this.no_merge_runs = no_merge_runs;
        this.merge_run_details = merge_run_details;
        this.eliminate_dups = eliminate_dups;
        this.in_sort_order = in_sort_order;
        this.distinct_aggregate = distinct_aggregate;
        
    }
    public void setStatementParameters(PreparedStatement ps)
        throws SQLException
    {
        ps.setString(1, sort_rs_id.toString());
        ps.setString(2, sort_type);
        if (no_input_rows != null)
            ps.setInt(3, no_input_rows.intValue());
        else
            ps.setNull(3, Types.INTEGER);
        if (no_output_rows != null)
            ps.setInt(4, no_output_rows.intValue());
        else
            ps.setNull(4, Types.INTEGER);
        if (no_merge_runs != null)
            ps.setInt(5, no_merge_runs.intValue());
        else
            ps.setNull(5, Types.INTEGER);
        ps.setString(6, merge_run_details);
        ps.setString(7, eliminate_dups);
        ps.setString(8, in_sort_order);
        ps.setString(9, distinct_aggregate);
    }
    
    public void setSort_type(String sort_type) {
        this.sort_type = sort_type;
    }

    public void setNo_input_rows(Integer no_input_rows) {
        this.no_input_rows = no_input_rows;
    }

    public void setNo_output_rows(Integer no_output_rows) {
        this.no_output_rows = no_output_rows;
    }

    public void setNo_merge_runs(Integer no_merge_runs) {
        this.no_merge_runs = no_merge_runs;
    }

    public void setMerge_run_details(String merge_run_details) {
        this.merge_run_details = merge_run_details;
    }


    public String getCatalogName() { return TABLENAME_STRING; }
    static  final   String  TABLENAME_STRING = "SYSXPLAIN_SORT_PROPS";

    private static final String[][] indexColumnNames =
    {
        {"SORT_RS_ID"}
    };

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList() {
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("SORT_RS_ID", false),
            SystemColumnImpl.getColumn("SORT_TYPE", Types.CHAR, true, 2),
            SystemColumnImpl.getColumn("NO_INPUT_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("NO_OUTPUT_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("NO_MERGE_RUNS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("MERGE_RUN_DETAILS", Types.VARCHAR, true, 256),
            SystemColumnImpl.getColumn("ELIMINATE_DUPLICATES", Types.CHAR, true, 1),
            SystemColumnImpl.getColumn("IN_SORT_ORDER", Types.CHAR, true, 1),
            SystemColumnImpl.getColumn("DISTINCT_AGGREGATE", Types.CHAR, true, 1),

        };
    }

}
