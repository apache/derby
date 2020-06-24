/*

   Derby - Class org.apache.derby.client.am.ColumnMetaData

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

package org.apache.derby.client.am;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Hashtable;
import org.apache.derby.shared.common.reference.DRDAConstants;
import org.apache.derby.shared.common.reference.JDBC40Translation;
import org.apache.derby.shared.common.reference.SQLState;

// Under JDBC 2, we must new up our parameter meta data as column meta data instances
// Once we move to JDK 1.4 pre-req, create a ResultSetMetaData class and make this class abstract

public class ColumnMetaData implements ResultSetMetaData {

    public int columns_;

    public boolean[] nullable_;

    // All of the following state data comes from the SQLDA reply.

    //Data from SQLDHGRP
    public short sqldHold_;
    public short sqldReturn_;
    public short sqldScroll_;
    public short sqldSensitive_;
    public short sqldFcode_;
    public short sqldKeytype_;
    public String sqldRdbnam_; // catalog name, not used by driver, placeholder only
    public String sqldSchema_; // schema name, not used by driver, placeholder only

    //data comes from SQLDAGRP
    public int[] sqlPrecision_; // adjusted sqllen;
    public int[] sqlScale_;
    public long[] sqlLength_;  // This is maximum length for varchar fields
    // These are the derby sql types, for use only by ResultSetMetaData, other code should use jdbcTypes_.
    // sqlTypes_ is currently not set for input column meta data.
    public int[] sqlType_;
    public int[] sqlCcsid_;

    // With the exception of sqlNames_ and sqlxParmmode_, the following members are only allocated when needed

    //Data from SQLDOPTGRP
    public String[] sqlName_;   // column name, pre-allocated
    public String[] sqlLabel_;  // column label
    public short[] sqlUnnamed_;
    public String[] sqlComment_;
    public String[] sqlUDTname_;
    public String[] sqlUDTclassName_;

    //Data from SQLDXGRP
    public short[] sqlxKeymem_;
    public short[] sqlxGenerated_;
    public short[] sqlxParmmode_; // pre-allocated
    public String[] sqlxCorname_;
    public String[] sqlxName_;
    public String[] sqlxBasename_;  // table name
    public int[] sqlxUpdatable_;
    public String[] sqlxSchema_;    // schema name
    public String[] sqlxRdbnam_;    // catalog name

    //-----------------------------transient state--------------------------------

    // For performance only, not part of logical model.
    public transient int[][] protocolTypesCache_ = null;

    public transient int[] types_;
    public transient int[] clientParamtertype_;

    private transient LogWriter logWriter_;

    // only set on execute replies, this is not describe information.
    // only used for result set meta data.

    transient int resultSetConcurrency_;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

    transient private Hashtable<String, Integer> columnNameToIndexCache_;
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

    transient private boolean statementClosed_ = false;

    void markClosed() {
        statementClosed_ = true;
        nullDataForGC();
    }

    private void checkForClosedStatement() throws SqlException {
        // agent_.checkForDeferredExceptions();
        if (statementClosed_) {
            throw new SqlException(logWriter_, 
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                    new ClientMessageId (SQLState.LANG_STATEMENT_CLOSED_NO_REASON));
        }
    }


    //---------------------navigational members-----------------------------------

    //---------------------constructors/finalizer---------------------------------

    // Called by NETColumnMetaData constructor before #columns is parsed out yet.
    public ColumnMetaData(LogWriter logWriter) {
        logWriter_ = logWriter;
    }

    // For creating column meta data when describe input is not available.
    // The upper bound that is passed in is determined by counting the number of parameter markers.
    // Called by PreparedStatement.flowPrepareStatement() and flowDescribeInputOutput()
    // only when describe input is not available.
    public ColumnMetaData(LogWriter logWriter, int upperBound) {
        logWriter_ = logWriter;
        initializeCache(upperBound);
    }


    public void initializeCache(int upperBound) {
        columns_ = upperBound;
        nullable_ = new boolean[upperBound];
        types_ = new int[upperBound];
        clientParamtertype_ = new int[upperBound];

        sqlPrecision_ = new int[upperBound];
        sqlScale_ = new int[upperBound];
        sqlLength_ = new long[upperBound];
        sqlType_ = new int[upperBound];
        sqlCcsid_ = new int[upperBound];

        sqlName_ = new String[upperBound];
        sqlxParmmode_ = new short[upperBound];

        sqlUDTname_ = new String[upperBound];
        sqlUDTclassName_ = new String[upperBound];
    }

    //--------------------Abstract material layer call-down methods-----------------

    //------------------material layer event callback methods-----------------------

    // ---------------------------jdbc 1------------------------------------------

    public int getColumnCount() throws SQLException {
        try
        {            
            checkForClosedStatement();
            return columns_;
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
//IC see: https://issues.apache.org/jira/browse/DERBY-1355
            if( sqlxGenerated_[column - 1] == 2) {
                return true;
            }
            return false;
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            //return true if the SQLTYPE is CHAR, VARCHAR, LOGVARCHAR or CLOB
            int type = types_[column - 1];
            return
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                    type == ClientTypes.CHAR ||
                    type == ClientTypes.VARCHAR ||
                    type == ClientTypes.LONGVARCHAR ||
                    type == ClientTypes.CLOB;
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    // all searchable except distinct
    public boolean isSearchable(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            return true;
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public boolean isCurrency(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            return false;
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public int isNullable(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            if (nullable_[column - 1]) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                return ResultSetMetaData.columnNullable;
            } else {
                return ResultSetMetaData.columnNoNulls;
            }
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public boolean isSigned(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            //return true only if the SQLType is SMALLINT, INT, BIGINT, FLOAT, REAL, DOUBLE, NUMERIC OR DECIMAL
            int type = types_[column - 1];
            return
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                    type == ClientTypes.SMALLINT ||
                    type == ClientTypes.INTEGER ||
                    type == ClientTypes.BIGINT ||
                    type == Types.FLOAT ||
                    type == ClientTypes.REAL ||
                    type == ClientTypes.DOUBLE ||
                    type == Types.NUMERIC ||
                    type == ClientTypes.DECIMAL;
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
}

    public int getColumnDisplaySize(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            int jdbcType = types_[column - 1];
            switch (jdbcType) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            case ClientTypes.BOOLEAN:
                return 5;
            case ClientTypes.INTEGER:
                return 11;
            case ClientTypes.SMALLINT:
                return 6;
            case ClientTypes.BIGINT:
                return 20;
            case ClientTypes.REAL:
//IC see: https://issues.apache.org/jira/browse/DERBY-3398
                return 15;
            case ClientTypes.DOUBLE:
            case Types.FLOAT:
                return 24;
            case ClientTypes.DECIMAL:
            case Types.NUMERIC:
        // There are 3 possible cases with respect to finding the correct max width for DECIMAL type.
        // 1. If scale = 0, only sign should be added to precision.
        // 2. scale = precision, 3 should be added to precision for sign, decimal and an additional char '0'.
        // 3. precision > scale > 0, 2 should be added to precision for sign and decimal.
        int scale = getScale(column);
        int precision = getPrecision(column);
//IC see: https://issues.apache.org/jira/browse/DERBY-836
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        return (scale == 0) ? (precision + 1) : ((scale == precision) ? (precision + 3) : (precision + 2));
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            case ClientTypes.CHAR:
            case ClientTypes.VARCHAR:
            case ClientTypes.LONGVARCHAR:
            case ClientTypes.CLOB:
                return (int) sqlLength_[column - 1];
            case ClientTypes.DATE:
                return 10;
            case ClientTypes.TIME:
                return 8;
            case ClientTypes.TIMESTAMP:
                return 29;
            case ClientTypes.JAVA_OBJECT:
//IC see: https://issues.apache.org/jira/browse/DERBY-2438
                return JDBC40Translation.DEFAULT_COLUMN_DISPLAY_SIZE;
            case ClientTypes.BINARY:
            case ClientTypes.VARBINARY:
            case ClientTypes.LONGVARBINARY:
            case ClientTypes.BLOB:
        // Derby-2425. For long length values, size overflows the int 
        // range. In such cases, the size is limited to the max. int value
        // This behavior is consistent with the same in Embedded mode.
        int size = (int) (2 * sqlLength_[column - 1]);  // eg. "FF" represents just one byte
//IC see: https://issues.apache.org/jira/browse/DERBY-2425
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        if ( size < 0 )
            size = Integer.MAX_VALUE;
                return size;
            default:
                throw new SqlException(logWriter_, 
                        new ClientMessageId (SQLState.UNSUPPORTED_TYPE));
            }
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public String getColumnLabel(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            // return labels if label is turned on, otherwise, return column name
            if (sqlLabel_ != null && sqlLabel_[column - 1] != null) {
                return sqlLabel_[column - 1];
            }
            if (sqlName_ == null || sqlName_[column - 1] == null) {
                assignColumnName(column);
            }
            return sqlName_[column - 1];
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public String getColumnName(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            // The Javadoc and Jdbc book explicitly state that the empty string ("") is returned if "not applicable"
            // for the following methods:
            //   getSchemaName()
            //   getTableName()
            //   getCatalogName()
            // Since the empty string is a valid string and is not really a proper table name, schema name, or catalog name,
            // we're not sure why the empty string was chosen over null, except possibly to be friendly to lazy jdbc apps
            // that may not be checking for nulls, thereby minimizing potential NPE's.
            // By induction, it would make sense to return the empty string when column name is not available/applicable.
            //
            // The JDBC specification contains blanket statements about SQL compliance levels,
            // so elaboration within the JDBC specification is often bypassed.
            // Personally, I would prefer to return Java null for all the not-applicable cases,
            // but it appears that we have precedent for the empty ("") string.
            //
            // We assume a straightforward induction from jdbc spec that the column name be "" (empty)
            // in preference to null or NULL for the not applicable case.
            //
            if (sqlName_ == null || sqlName_[column - 1] == null) {
                assignColumnName(column);
            }
            return sqlName_[column - 1];
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public String getSchemaName(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            if (sqlxSchema_ == null || sqlxSchema_[column - 1] == null) {
                return ""; // Per jdbc spec
            }
            return sqlxSchema_[column - 1];
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public int getPrecision(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            int jdbcType = types_[column - 1];

            switch (jdbcType) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            case ClientTypes.BOOLEAN:
                return 1;
            case Types.NUMERIC:
            case ClientTypes.DECIMAL:
                return sqlPrecision_[column - 1];
            case ClientTypes.SMALLINT:
                return 5;
            case ClientTypes.INTEGER:
                return 10;
            case ClientTypes.BIGINT:
                return 19;
            case Types.FLOAT:
                return 15;
            case ClientTypes.REAL:
                return 7;  // This is the number of signed digits for IEEE float with mantissa 24, ie. 2^24
            case ClientTypes.DOUBLE:
                return 15; // This is the number of signed digits for IEEE float with mantissa 24, ie. 2^24
            case ClientTypes.CHAR:
            case ClientTypes.VARCHAR:
            case ClientTypes.LONGVARCHAR:
            case ClientTypes.BINARY:
            case ClientTypes.VARBINARY:
            case ClientTypes.LONGVARBINARY:
            case ClientTypes.CLOB:
            case ClientTypes.BLOB:
                return (int) sqlLength_[column - 1];
            case ClientTypes.DATE:
                return 10;
            case ClientTypes.TIME:
                return 8;
            case ClientTypes.TIMESTAMP:
//IC see: https://issues.apache.org/jira/browse/DERBY-4614
//IC see: https://issues.apache.org/jira/browse/DERBY-4614
                return 29;
            case ClientTypes.JAVA_OBJECT:
//IC see: https://issues.apache.org/jira/browse/DERBY-2438
                return JDBC40Translation.UNKNOWN_PRECISION;
            default:
                throw new SqlException(logWriter_, 
                        new ClientMessageId (SQLState.UNSUPPORTED_TYPE));
            }
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public int getScale(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);

            return sqlScale_[column - 1];
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public String getTableName(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            if (sqlxBasename_ == null || sqlxBasename_[column - 1] == null) {
                return ""; // Per jdbc spec
            }
            return sqlxBasename_[column - 1];
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    /**
     * What's a column's table's catalog name?
     *
     * @param column the first column is 1, the second is 2, ...
     *
     * @return column name or "" if not applicable.
     *
     * @throws SQLException thrown on failure
     */
    public String getCatalogName(int column) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            return "";
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public int getColumnType(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);

            return types_[column - 1];
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public String getColumnTypeName(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);

            int jdbcType = types_[column - 1];
            // So these all come back zero for downlevel servers in PROTOCOL.
            // John is going to write some code to construct the sqlType_ array
            // based on the protocol types from the query descriptor.
            int sqlType = sqlType_[column - 1];

            switch (sqlType) {
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DB2_SQLTYPE_BOOLEAN:
            case DRDAConstants.DB2_SQLTYPE_NBOOLEAN:
                return "BOOLEAN";
            case DRDAConstants.DB2_SQLTYPE_DATE:
            case DRDAConstants.DB2_SQLTYPE_NDATE:
                return "DATE";
            case DRDAConstants.DB2_SQLTYPE_TIME:
            case DRDAConstants.DB2_SQLTYPE_NTIME:
                return "TIME";
            case DRDAConstants.DB2_SQLTYPE_TIMESTAMP:
            case DRDAConstants.DB2_SQLTYPE_NTIMESTAMP:
                return "TIMESTAMP";
            case DRDAConstants.DB2_SQLTYPE_BLOB:
            case DRDAConstants.DB2_SQLTYPE_NBLOB:
                return "BLOB";
            case DRDAConstants.DB2_SQLTYPE_CLOB:
            case DRDAConstants.DB2_SQLTYPE_NCLOB:
                return "CLOB";
            case DRDAConstants.DB2_SQLTYPE_VARCHAR:
            case DRDAConstants.DB2_SQLTYPE_NVARCHAR:
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                if (jdbcType == ClientTypes.VARBINARY) {
                    return "VARCHAR FOR BIT DATA";
                } else {
                    return "VARCHAR";
                }
            case DRDAConstants.DB2_SQLTYPE_CHAR:
            case DRDAConstants.DB2_SQLTYPE_NCHAR:
                if (jdbcType == ClientTypes.BINARY) {
                    return "CHAR FOR BIT DATA";
                } else {
                    return "CHAR";
                }
            case DRDAConstants.DB2_SQLTYPE_LONG:
            case DRDAConstants.DB2_SQLTYPE_NLONG:
                if (jdbcType == ClientTypes.LONGVARBINARY) {
                    return "LONG VARCHAR FOR BIT DATA";
                } else {
                    return "LONG VARCHAR";
                }
            case DRDAConstants.DB2_SQLTYPE_CSTR:
            case DRDAConstants.DB2_SQLTYPE_NCSTR:
                return "SBCS";
            case DRDAConstants.DB2_SQLTYPE_FLOAT:
            case DRDAConstants.DB2_SQLTYPE_NFLOAT:
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                if (jdbcType == ClientTypes.DOUBLE) {
                    return "DOUBLE";
                }
                if (jdbcType == ClientTypes.REAL) {
                    return "REAL";
                }
            case DRDAConstants.DB2_SQLTYPE_DECIMAL:
            case DRDAConstants.DB2_SQLTYPE_NDECIMAL:
                return "DECIMAL";
            case DRDAConstants.DB2_SQLTYPE_BIGINT:
            case DRDAConstants.DB2_SQLTYPE_NBIGINT:
                return "BIGINT";
            case DRDAConstants.DB2_SQLTYPE_INTEGER:
            case DRDAConstants.DB2_SQLTYPE_NINTEGER:
                return "INTEGER";
            case DRDAConstants.DB2_SQLTYPE_SMALL:
            case DRDAConstants.DB2_SQLTYPE_NSMALL:
                return "SMALLINT";
            case DRDAConstants.DB2_SQLTYPE_NUMERIC:
            case DRDAConstants.DB2_SQLTYPE_NNUMERIC:
                return "NUMERIC";
            case DRDAConstants.DB2_SQLTYPE_FAKE_UDT:
            case DRDAConstants.DB2_SQLTYPE_FAKE_NUDT:
                return sqlUDTname_[ column - 1 ];
            default:
                throw new SqlException(logWriter_, 
                        new ClientMessageId (SQLState.UNSUPPORTED_TYPE));
            }
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public boolean isReadOnly(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            if (sqlxUpdatable_ == null) {
                // If no extended describe, return resultSet's concurrency
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                return resultSetConcurrency_ ==
                       ResultSet.CONCUR_READ_ONLY;
            }
            return sqlxUpdatable_[column - 1] == 0; // PROTOCOL 0 means not updatable, 1 means updatable
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public boolean isWritable(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            if (sqlxUpdatable_ == null) {
                // If no extended describe, return resultSet's concurrency
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                return resultSetConcurrency_ ==
                       ResultSet.CONCUR_UPDATABLE;
            }
            return sqlxUpdatable_[column - 1] == 1; // PROTOCOL 0 means not updatable, 1 means updatable
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);
            if (sqlxUpdatable_ == null) {
                return false;
            }
            return sqlxUpdatable_[column - 1] == 1; // PROTOCOL 0 means not updatable, 1 means updatable
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    //--------------------------jdbc 2.0-----------------------------------

    public String getColumnClassName(int column) throws SQLException {
        try
        {
            checkForClosedStatement();
            checkForValidColumnIndex(column);

            int jdbcType = types_[column - 1];
            switch (jdbcType) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            case Types.BOOLEAN:
                return "java.lang.Boolean";
            case Types.BIT:
                return "java.lang.Boolean";
            case Types.TINYINT:
                return "java.lang.Integer";
            case ClientTypes.SMALLINT:
                return "java.lang.Integer";
            case ClientTypes.INTEGER:
                return "java.lang.Integer";
            case ClientTypes.BIGINT:
                return "java.lang.Long";
            case Types.FLOAT:
                return "java.lang.Double";
            case ClientTypes.REAL:
                return "java.lang.Float";
            case ClientTypes.DOUBLE:
                return "java.lang.Double";
            case Types.NUMERIC:
            case ClientTypes.DECIMAL:
                return "java.math.BigDecimal";
            case ClientTypes.CHAR:
            case ClientTypes.VARCHAR:
            case ClientTypes.LONGVARCHAR:
                return "java.lang.String";
            case ClientTypes.DATE:
                return "java.sql.Date";
            case ClientTypes.TIME:
                return "java.sql.Time";
            case ClientTypes.TIMESTAMP:
                return "java.sql.Timestamp";
            case ClientTypes.BINARY:
            case ClientTypes.VARBINARY:
            case ClientTypes.LONGVARBINARY:
                return "byte[]";
            case Types.STRUCT:
                return "java.sql.Struct";
            case Types.ARRAY:
                return "java.sql.Array";
            case ClientTypes.BLOB:
                return "java.sql.Blob";
            case ClientTypes.CLOB:
                return "java.sql.Clob";
            case Types.REF:
                return "java.sql.Ref";
            case Types.JAVA_OBJECT:
                return sqlUDTclassName_[ column - 1 ];
            default:
                throw new SqlException(logWriter_, 
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                        new ClientMessageId (SQLState.UNSUPPORTED_TYPE));
            }
        }
        catch ( SqlException e )
        {
            throw e.getSQLException();
        }
    }

    //----------------------------helper methods----------------------------------


    void checkForValidColumnIndex(int column) throws SqlException {
        if (column < 1 || column > columns_) {
            throw new SqlException(logWriter_, 
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                    new ClientMessageId (SQLState.LANG_INVALID_COLUMN_POSITION),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                    column, columns_);
        }
    }

    public void setLogWriter(LogWriter logWriter) {
        logWriter_ = logWriter;
    }

    private void nullDataForGC() {
        columns_ = 0;
        nullable_ = null;
        types_ = null;
        sqldRdbnam_ = null;
        sqldSchema_ = null;
        sqlPrecision_ = null;
        sqlScale_ = null;
        sqlLength_ = null;
        sqlType_ = null;
        sqlCcsid_ = null;
        sqlName_ = null;
        sqlLabel_ = null;
        sqlUnnamed_ = null;
        sqlComment_ = null;
        sqlUDTname_ = null;
        sqlUDTclassName_ = null;
        sqlxKeymem_ = null;
        sqlxGenerated_ = null;
        sqlxParmmode_ = null;
        sqlxCorname_ = null;
        sqlxName_ = null;
        sqlxBasename_ = null;
        sqlxUpdatable_ = null;
        sqlxSchema_ = null;
        sqlxRdbnam_ = null;
        clientParamtertype_ = null;
        types_ = null;
    }

    boolean hasLobColumns() {
        for (int i = 0; i < columns_; i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            switch (Utils.getNonNullableSqlType(sqlType_[i])) {
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DB2_SQLTYPE_BLOB:
            case DRDAConstants.DB2_SQLTYPE_CLOB:
                return true;
            default:
                break;
            }
        }
        return false;
    }

    // Cache the hashtable in ColumnMetaData.
    int findColumnX(String columnName) throws SqlException {
        // Create cache if it doesn't exist
        if (columnNameToIndexCache_ == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            columnNameToIndexCache_ = new Hashtable<String, Integer>();
        } else { // Check cache for mapping
            Integer index = columnNameToIndexCache_.get(columnName);
            if (index != null) {
                return index.intValue();
            }
        }

        // Ok, we'll have to search the metadata
        for (int col = 0; col < this.columns_; col++) {
            if (this.sqlName_ != null && // sqlName comes from an optional group
                    this.sqlName_[col] != null &&
                    this.sqlName_[col].equalsIgnoreCase(columnName)) {
                // Found it, add it to the cache
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                columnNameToIndexCache_.put(columnName, col + 1);
                return col + 1;
            }
        }
        throw new SqlException(logWriter_, 
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                new ClientMessageId (SQLState.INVALID_COLUMN_NAME), columnName);
    }

    // assign ordinal position as the column name if null.
    private void assignColumnName(int column) {
        if (columnNameToIndexCache_ == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            columnNameToIndexCache_ = new Hashtable<String, Integer>();
        }
        String columnName = Integer.toString(column);
        columnNameToIndexCache_.put(columnName, column);
        sqlName_[column - 1] = columnName;
    }

    // JDBC 4.0 java.sql.Wrapper interface methods

    /**
     * Check whether this instance wraps an object that implements the interface
     * specified by {@code iface}.
     *
     * @param iface a class defining an interface
     * @return {@code true} if this instance implements {@code iface}, or
     * {@code false} otherwise
     * @throws SQLException if an error occurs while determining if this
     * instance implements {@code iface}
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    /**
     * Returns {@code this} if this class implements the specified interface.
     *
     * @param  iface a class defining an interface
     * @return an object that implements the interface
     * @throws SQLException if no object is found that implements the
     * interface
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(null,
                    new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                    iface).getSQLException();
        }
    }
}
