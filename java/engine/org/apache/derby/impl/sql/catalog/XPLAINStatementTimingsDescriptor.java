package org.apache.derby.impl.sql.catalog;

import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;
import java.sql.Types;

public class XPLAINStatementTimingsDescriptor extends XPLAINTableDescriptor 
{

    private UUID timing_id;  // the Timing UUID, which is saved in the xplain_Statements table, if statistics timing is switched on
    private Long parse_time; // the time needed for parsing the stmt text 
    private Long bind_time;  // the time needed for binding the node tree
    private Long optimize_time; // time needed for optimizing the node tree
    private Long generate_time; // time needed for class generation
    private Long compile_time; // time needed for parse+bind+optimize+generate
    private Long execute_time; // time needed for execution of class 
    private Timestamp begin_comp_time; // the begin compilation timestamp
    private Timestamp end_comp_time;   // the end   compilation timestamp
    private Timestamp begin_exe_time;  // the begin execution timestamp
    private Timestamp end_exe_time;    // the end   execution timestamp
    
    public XPLAINStatementTimingsDescriptor() {}
    public XPLAINStatementTimingsDescriptor
    (
            UUID timing_id,
            Long parse_time,
            Long bind_time,
            Long optimize_time,
            Long generate_time,
            Long compile_time,
            Long execute_time,
            Timestamp begin_comp_time,
            Timestamp end_comp_time,
            Timestamp begin_exe_time,
            Timestamp end_exe_time
    )
    {
        this.timing_id       = timing_id;
        this.parse_time      = parse_time;
        this.bind_time       = bind_time;
        this.optimize_time   = optimize_time;
        this.generate_time   = generate_time;
        this.compile_time    = compile_time;
        this.execute_time    = execute_time;
        this.begin_comp_time = begin_comp_time;
        this.end_comp_time   = end_comp_time;
        this.begin_exe_time  = begin_exe_time;
        this.end_exe_time    = end_exe_time;
        
    }
    public void setStatementParameters(PreparedStatement ps)
        throws SQLException
    {
        ps.setString(1, timing_id.toString());
        if (parse_time != null)
            ps.setLong(2, parse_time.longValue());
        else
            ps.setNull(2, Types.BIGINT);
        if (bind_time != null)
            ps.setLong(3, bind_time.longValue());
        else
            ps.setNull(3, Types.BIGINT);
        if (optimize_time != null)
            ps.setLong(4, optimize_time.longValue());
        else
            ps.setNull(4, Types.BIGINT);
        if (generate_time != null)
            ps.setLong(5, generate_time.longValue());
        else
            ps.setNull(5, Types.BIGINT);
        if (compile_time != null)
            ps.setLong(6, compile_time.longValue());
        else
            ps.setNull(6, Types.BIGINT);
        if (execute_time != null)
            ps.setLong(7, execute_time.longValue());
        else
            ps.setNull(7, Types.BIGINT);
        ps.setTimestamp(8, begin_comp_time);
        ps.setTimestamp(9, end_comp_time);
        ps.setTimestamp(10, begin_exe_time);
        ps.setTimestamp(11, end_exe_time);
    }
    
    public String getCatalogName() { return TABLENAME_STRING; }
    static final String             TABLENAME_STRING = "SYSXPLAIN_STATEMENT_TIMINGS";

    private static final String[][] indexColumnNames =
    {
        {"TIMING_ID"}
    };

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList() {
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("TIMING_ID", false),
            SystemColumnImpl.getColumn("PARSE_TIME", Types.BIGINT, false),
            SystemColumnImpl.getColumn("BIND_TIME", Types.BIGINT, false),
            SystemColumnImpl.getColumn("OPTIMIZE_TIME", Types.BIGINT, false),
            SystemColumnImpl.getColumn("GENERATE_TIME", Types.BIGINT, false),
            SystemColumnImpl.getColumn("COMPILE_TIME", Types.BIGINT, false),
            SystemColumnImpl.getColumn("EXECUTE_TIME", Types.BIGINT, false),
            SystemColumnImpl.getColumn("BEGIN_COMP_TIME", Types.TIMESTAMP, false),
            SystemColumnImpl.getColumn("END_COMP_TIME", Types.TIMESTAMP, false),
            SystemColumnImpl.getColumn("BEGIN_EXE_TIME", Types.TIMESTAMP, false),
            SystemColumnImpl.getColumn("END_EXE_TIME", Types.TIMESTAMP, false),
        };
    }

}
