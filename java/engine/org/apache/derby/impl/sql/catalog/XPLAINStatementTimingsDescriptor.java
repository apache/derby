/*

   Derby - Class org.apache.derby.impl.sql.catalog.XPLAINStatementTimingsDescriptor

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

import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.types.DataTypeUtilities;

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
        this.begin_comp_time = DataTypeUtilities.clone( begin_comp_time );
        this.end_comp_time   = DataTypeUtilities.clone( end_comp_time );
        this.begin_exe_time  = DataTypeUtilities.clone( begin_exe_time );
        this.end_exe_time    = DataTypeUtilities.clone( end_exe_time );
        
    }
    public void setStatementParameters(PreparedStatement ps)
        throws SQLException
    {
        ps.setString(1, timing_id.toString());
        ps.setObject(2, parse_time, Types.BIGINT);
        ps.setObject(3, bind_time, Types.BIGINT);
        ps.setObject(4, optimize_time, Types.BIGINT);
        ps.setObject(5, generate_time, Types.BIGINT);
        ps.setObject(6, compile_time, Types.BIGINT);
        ps.setObject(7, execute_time, Types.BIGINT);
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
