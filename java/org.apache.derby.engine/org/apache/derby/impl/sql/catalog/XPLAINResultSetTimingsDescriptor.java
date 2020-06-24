/*

   Derby - Class org.apache.derby.impl.sql.catalog.XPLAINResultSetTimingsDescriptor

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
import java.sql.Types;

public class XPLAINResultSetTimingsDescriptor extends XPLAINTableDescriptor 
{

    private UUID timing_id; // the timuing UUID for the result set timing information
    private Long constructor_time; // the time needed to create an object, through a call of the constructor
    private Long open_time; // the time needed to process all open calls
    private Long next_time; // the time needed to process all next calls
    private Long close_time; // the time needed to close the resultset
    private Long execute_time; // the time needed for overall execution
    private Long avg_next_time_per_row; // the avarage time needed for a next call per row
    private Long projection_time; // the time needed by a ProjectRestrictResultSet to do the projection
    private Long restriction_time; // the time needed by a ProjectRestrictResultSet to do the restriction
    private Long temp_cong_create_time; //  the timestamp of th creation of a temporary conglomerate
    private Long temp_cong_fetch_time; // the time needed to do a fetch from this temporary conglomerate
    

    public XPLAINResultSetTimingsDescriptor() {}
    public XPLAINResultSetTimingsDescriptor
    (
            UUID timing_id,
            Long constructor_time,
            Long open_time,
            Long next_time,
            Long close_time,
            Long execute_time,
            Long avg_next_time_per_row,
            Long projection_time,
            Long restriction_time,
            Long temp_cong_create_time,
            Long temp_cong_fetch_time
    )
    {
        
        this.timing_id = timing_id;
        this.constructor_time = constructor_time;
        this.open_time = open_time;
        this.next_time = next_time;
        this.close_time = close_time;
        this.execute_time = execute_time;
        this.avg_next_time_per_row = avg_next_time_per_row;
        this.projection_time = projection_time;
        this.restriction_time = restriction_time;
        this.temp_cong_create_time = temp_cong_create_time;
        this.temp_cong_fetch_time = temp_cong_fetch_time;
    }

    public void setStatementParameters(PreparedStatement ps)
        throws SQLException
    {
        ps.setString(1, timing_id.toString());
//IC see: https://issues.apache.org/jira/browse/DERBY-6318
        ps.setObject(2, constructor_time, Types.BIGINT);
        ps.setObject(3, open_time, Types.BIGINT);
        ps.setObject(4, next_time, Types.BIGINT);
        ps.setObject(5, close_time, Types.BIGINT);
        ps.setObject(6, execute_time, Types.BIGINT);
        ps.setObject(7, avg_next_time_per_row, Types.BIGINT);
        ps.setObject(8, projection_time, Types.BIGINT);
        ps.setObject(9, restriction_time, Types.BIGINT);
        ps.setObject(10, temp_cong_create_time, Types.BIGINT);
        ps.setObject(11, temp_cong_fetch_time, Types.BIGINT);
    }

    public String getCatalogName() { return TABLENAME_STRING; }
    static final String             TABLENAME_STRING = "SYSXPLAIN_RESULTSET_TIMINGS";

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
            SystemColumnImpl.getColumn("CONSTRUCTOR_TIME", Types.BIGINT, true),
            SystemColumnImpl.getColumn("OPEN_TIME", Types.BIGINT, true),
            SystemColumnImpl.getColumn("NEXT_TIME", Types.BIGINT, true),
            SystemColumnImpl.getColumn("CLOSE_TIME", Types.BIGINT, true),
            SystemColumnImpl.getColumn("EXECUTE_TIME", Types.BIGINT, true),
            SystemColumnImpl.getColumn("AVG_NEXT_TIME_PER_ROW", Types.BIGINT, true),
            SystemColumnImpl.getColumn("PROJECTION_TIME", Types.BIGINT, true),
            SystemColumnImpl.getColumn("RESTRICTION_TIME", Types.BIGINT, true),
            SystemColumnImpl.getColumn("TEMP_CONG_CREATE_TIME", Types.BIGINT, true),
            SystemColumnImpl.getColumn("TEMP_CONG_FETCH_TIME", Types.BIGINT, true),
        };
    }

}
