/*

   Derby - Class org.apache.derby.impl.sql.catalog.XPLAINStatementDescriptor

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
import java.sql.Timestamp;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.types.TypeId;
import java.sql.Types;

public class XPLAINStatementDescriptor extends XPLAINTableDescriptor 
{
    
    // implementation
    private UUID stmt_id;           // the statement UUID
    private String stmt_name;       // the statement name, if available
    private String stmt_type;       // the statement type, e.g. select,insert, update, etc. 
    private String stmt_text;       // the statement text 
    private String jvm_id;          // the virtual machine identifier, only the code
    private String os_id;           // the operating system identifier, from the os.home vm system variable
    private String xplain_mode;     // the explain mode, either (F)ull explain or explain (O)nly
    private Timestamp xplain_time;  // the explain timestamp, useful if statistics timing was off
    private String thread_id;       // the explaining thread identifier      
    private String xa_id;           // the transaction identifier
    private String session_id;      // the session identifier (instance)
    private String db_name;         // the database name
    private String drda_id;         // the drda identifier
    private UUID timing_id;         // the UUID of the associated timing tuple
    
    public XPLAINStatementDescriptor() {}
    public XPLAINStatementDescriptor (
            UUID stmt_id,
            String stmt_name,
            String stmt_type,
            String stmt_text,
            String jvm_id,
            String os_id,
            String xplain_mode,
            Timestamp xplain_time,
            String thread_id,
            String xa_id,
            String session_id,
            String db_name,
            String drda_id,
            UUID timing_id
    ){
        
        this.stmt_id     =  stmt_id;
        this.stmt_name   =  stmt_name;
        this.stmt_type   =  stmt_type;
        this.stmt_text   = stmt_text;
        this.jvm_id      = jvm_id;
        this.os_id       = os_id;
        this.xplain_mode = xplain_mode;
        this.xplain_time = xplain_time;
        this.thread_id   = thread_id;
        this.xa_id       = xa_id;
        this.session_id  = session_id;
        this.db_name     = db_name;
        this.drda_id     = drda_id;
        this.timing_id   = timing_id;
       
    }
    public void setStatementParameters(PreparedStatement ps)
        throws SQLException
    {
        ps.setString(1, stmt_id.toString());
        ps.setString(2, stmt_name);
        ps.setString(3, stmt_type);
        ps.setString(4, stmt_text);
        ps.setString(5, jvm_id);
        ps.setString(6, os_id);
        ps.setString(7, xplain_mode);
        ps.setTimestamp(8, xplain_time);
        ps.setString(9, thread_id);
        ps.setString(10, xa_id);
        ps.setString(11, session_id);
        ps.setString(12, db_name);
        ps.setString(13, drda_id);
        ps.setString(14, (timing_id != null ? timing_id.toString():null));
    }
    
    public String getCatalogName() { return TABLENAME_STRING; }
    static final String             TABLENAME_STRING = "SYSXPLAIN_STATEMENTS";

    private static final String[][] indexColumnNames =
    {
        {"STMT_ID"}
    };

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList() {
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("STMT_ID", false),
            SystemColumnImpl.getIdentifierColumn("STMT_NAME", true),
            SystemColumnImpl.getColumn("STMT_TYPE", Types.CHAR, false, 3),
            SystemColumnImpl.getColumn("STMT_TEXT", Types.VARCHAR, false, TypeId.VARCHAR_MAXWIDTH),
            SystemColumnImpl.getColumn("JVM_ID", Types.VARCHAR, false,
                    TypeId.VARCHAR_MAXWIDTH),
            SystemColumnImpl.getColumn("OS_IDENTIFIER", Types.VARCHAR, false,
                    TypeId.VARCHAR_MAXWIDTH),
            SystemColumnImpl.getColumn("XPLAIN_MODE", Types.CHAR, true, 1),
            SystemColumnImpl.getColumn("XPLAIN_TIME", Types.TIMESTAMP, true),
            SystemColumnImpl.getColumn("XPLAIN_THREAD_ID", Types.VARCHAR, false,
                    TypeId.VARCHAR_MAXWIDTH),
            SystemColumnImpl.getColumn("TRANSACTION_ID", Types.VARCHAR, false,
                    TypeId.VARCHAR_MAXWIDTH),
            SystemColumnImpl.getColumn("SESSION_ID", Types.VARCHAR, false,
                    TypeId.VARCHAR_MAXWIDTH),
            SystemColumnImpl.getIdentifierColumn("DATABASE_NAME", false),
            SystemColumnImpl.getColumn("DRDA_ID", Types.VARCHAR, true,
                    TypeId.VARCHAR_MAXWIDTH),
            SystemColumnImpl.getUUIDColumn("TIMING_ID", true),
        };
    }

}
