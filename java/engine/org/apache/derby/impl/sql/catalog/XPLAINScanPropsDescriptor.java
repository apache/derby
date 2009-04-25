package org.apache.derby.impl.sql.catalog;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;
import java.sql.Types;

public class XPLAINScanPropsDescriptor extends XPLAINTableDescriptor 
{

    private UUID scan_rs_id; // the UUID of this scan info tuple
    private String scan_object_name; // the name of the scanned object
    private String scan_object_type; // the object type, either index, constraint or table
    private String scan_type; // the type of the scan: heap, btree or sort
    private String isolation_level; // the isolation level
    private Integer no_visited_pages; // the number of visited pages during this scan
    private Integer no_visited_rows; // the number of visited rows during this scan
    private Integer no_qualified_rows; // the number of qualified rows, during the scan
    private Integer no_visited_deleted_rows; // the number of visited rows, marked for delete
    private Integer no_fetched_columns; // the number of fetched columns of this scan from the object
    private String bitset_of_fetched_columns; // the bitset of the fetched columns
    private Integer btree_height; // the btree height, if this is a btree scan
    private Integer fetch_size; // the fetch size, for bulk scans
    private String start_position; // the start positioner info, internal encoding
    private String stop_position; // the stop positioner info, internal encoding
    private String scan_qualifiers; // the scan qualifiers, in internal encoding (conjunctive normal form)
    private String next_qualifiers; // the next qualifiers, in internal encoding
    private String hash_key_column_numbers; // the hash key column numbers
    private Integer hash_table_size; // the hash table size of the constructed hash table during the scan
    
    public XPLAINScanPropsDescriptor() {}
    public XPLAINScanPropsDescriptor 
    (
             UUID scan_rs_id,
             String scan_object_name,
             String scan_object_type,
             String scan_type,
             String isolation_level,
             Integer no_visited_pages,
             Integer no_visited_rows,
             Integer no_qualified_rows,
             Integer no_visited_deleted_rows,
             Integer no_fetched_columns,
             String bitset_of_fetched_columns,
             Integer btree_height,
             Integer fetch_size,
             String start_position,
             String stop_position,
             String scan_qualifiers,
             String next_qualifiers,
             String hash_key_column_numbers,
             Integer hash_table_size
    )
    {
        
        this.scan_rs_id = scan_rs_id;
        this.scan_object_name = scan_object_name;
        this.scan_object_type = scan_object_type;
        this.scan_type = scan_type;
        this.isolation_level = isolation_level;
        this.no_visited_pages = no_visited_pages;
        this.no_visited_rows  = no_visited_rows;
        this.no_qualified_rows = no_qualified_rows;
        this.no_visited_deleted_rows = no_visited_deleted_rows;
        this.no_fetched_columns = no_fetched_columns;
        this.bitset_of_fetched_columns = bitset_of_fetched_columns;
        this.btree_height = btree_height;
        this.fetch_size = fetch_size;
        this.start_position = start_position;
        this.stop_position = stop_position;
        this.scan_qualifiers = scan_qualifiers;
        this.next_qualifiers = next_qualifiers;
        this.hash_key_column_numbers = hash_key_column_numbers;
        this.hash_table_size = hash_table_size;
    }
    public void setStatementParameters(PreparedStatement ps)
        throws SQLException
    {
        ps.setString(1, scan_rs_id.toString());
        ps.setString(2, scan_object_name);
        ps.setString(3, scan_object_type);
        ps.setString(4, scan_type);
        ps.setString(5, isolation_level);
        if (no_visited_pages != null)
            ps.setInt(6, no_visited_pages.intValue());
        else
            ps.setNull(6, Types.INTEGER);
        if (no_visited_rows != null)
            ps.setInt(7, no_visited_rows.intValue());
        else
            ps.setNull(7, Types.INTEGER);
        if (no_qualified_rows != null)
            ps.setInt(8, no_qualified_rows.intValue());
        else
            ps.setNull(8, Types.INTEGER);
        if (no_visited_deleted_rows != null)
            ps.setInt(9, no_visited_deleted_rows.intValue());
        else
            ps.setNull(9, Types.INTEGER);
        if (no_fetched_columns != null)
            ps.setInt(10, no_fetched_columns.intValue());
        else
            ps.setNull(10, Types.INTEGER);
        ps.setString(11, bitset_of_fetched_columns);
        if (btree_height != null)
            ps.setInt(12, btree_height.intValue());
        else
            ps.setNull(12, Types.INTEGER);
        if (fetch_size != null)
            ps.setInt(13, fetch_size.intValue());
        else
            ps.setNull(13, Types.INTEGER);
        ps.setString(14, start_position);
        ps.setString(15, stop_position);
        ps.setString(16, scan_qualifiers);
        ps.setString(17, next_qualifiers);
        ps.setString(18, hash_key_column_numbers);
        if (hash_table_size != null)
            ps.setInt(19, hash_table_size.intValue());
        else
            ps.setNull(19, Types.INTEGER);
    }

    public void setScan_type(String scan_type) {
        this.scan_type = scan_type;
    }

    public void setNo_visited_pages(Integer no_visited_pages) {
        this.no_visited_pages = no_visited_pages;
    }

    public void setNo_visited_rows(Integer no_visited_rows) {
        this.no_visited_rows = no_visited_rows;
    }

    public void setNo_qualified_rows(Integer no_qualified_rows) {
        this.no_qualified_rows = no_qualified_rows;
    }

    public void setNo_fetched_columns(Integer no_fetched_columns) {
        this.no_fetched_columns = no_fetched_columns;
    }

    public void setNo_visited_deleted_rows(Integer no_visited_deleted_rows) {
        this.no_visited_deleted_rows = no_visited_deleted_rows;
    }

    public void setBtree_height(Integer btree_height) {
        this.btree_height = btree_height;
    }

    public void setBitset_of_fetched_columns(String bitset_of_fetched_columns) {
        this.bitset_of_fetched_columns = bitset_of_fetched_columns;
    }


    public String getCatalogName() { return TABLENAME_STRING; }
    static final   String  TABLENAME_STRING = "SYSXPLAIN_SCAN_PROPS";
    
    private static final String[][] indexColumnNames =
    {
        {"SCAN_RS_ID"}
    };


    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList() {
        
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("SCAN_RS_ID", false),
            SystemColumnImpl.getIdentifierColumn("SCAN_OBJECT_NAME", false),
            SystemColumnImpl.getIndicatorColumn("SCAN_OBJECT_TYPE"),
            SystemColumnImpl.getColumn("SCAN_TYPE", Types.CHAR, false, 8),
            SystemColumnImpl.getColumn("ISOLATION_LEVEL", Types.CHAR, true, 3),
            SystemColumnImpl.getColumn("NO_VISITED_PAGES", Types.INTEGER, true),
            SystemColumnImpl.getColumn("NO_VISITED_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("NO_QUALIFIED_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("NO_VISITED_DELETED_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("NO_FETCHED_COLUMNS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("BITSET_OF_FETCHED_COLUMNS", Types.VARCHAR, true, 256),
            SystemColumnImpl.getColumn("BTREE_HEIGHT", Types.INTEGER, true),
            SystemColumnImpl.getColumn("FETCH_SIZE", Types.INTEGER, true),
            SystemColumnImpl.getColumn("START_POSITION", Types.VARCHAR, true, 512),
            SystemColumnImpl.getColumn("STOP_POSITION", Types.VARCHAR, true, 512),
            SystemColumnImpl.getColumn("SCAN_QUALIFIERS", Types.VARCHAR, true, 512),
            SystemColumnImpl.getColumn("NEXT_QUALIFIERS", Types.VARCHAR, true, 512),
            SystemColumnImpl.getColumn("HASH_KEY_COLUMN_NUMBERS", Types.VARCHAR, true, 512),
            SystemColumnImpl.getColumn("HASH_TABLE_SIZE", Types.INTEGER, true),
        };
    }

}
