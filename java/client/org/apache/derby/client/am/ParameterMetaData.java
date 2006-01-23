/*

   Derby - Class org.apache.derby.client.am.ParameterMetaData

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.client.am;

import java.sql.SQLException;

// Parameter meta data as used internally by the driver is always a column meta data instance.
// We will only create instances of this class when getParameterMetaData() is called.
// This class simply wraps a column meta data instance.
//
// Once we go to JDK 1.4 as runtime pre-req, we can extend ColumnMetaData and new up ParameterMetaData instances directly,
// and we won't have to wrap column meta data instances directly.

public class ParameterMetaData implements java.sql.ParameterMetaData {
    ColumnMetaData columnMetaData_;

    // This is false unless for parameterMetaData for a call statement with return clause
    boolean escapedProcedureCallWithResult_ = false;

    public ParameterMetaData(ColumnMetaData columnMetaData) {
        columnMetaData_ = columnMetaData;
    }

    public int getParameterCount() throws SQLException {
        if (escapedProcedureCallWithResult_) {
            return columnMetaData_.columns_++;
        }
        return columnMetaData_.columns_;
    }

    public int getParameterType(int param) throws SQLException {
        if (escapedProcedureCallWithResult_) {
            param--;
            if (param == 0) {
                return java.sql.Types.INTEGER;
            }
        }
        return columnMetaData_.getColumnType(param);
    }

    public String getParameterTypeName(int param) throws SQLException {
        if (escapedProcedureCallWithResult_) {
            param--;
            if (param == 0) {
                return "INTEGER";
            }
        }
        return columnMetaData_.getColumnTypeName(param);
    }

    public String getParameterClassName(int param) throws SQLException {
        if (escapedProcedureCallWithResult_) {
            param--;
            if (param == 0) {
                return "java.lang.Integer";
            }
        }
        return columnMetaData_.getColumnClassName(param);
    }

    public int getParameterMode(int param) throws SQLException {
        try
        {
            if (escapedProcedureCallWithResult_) {
                param--;
                if (param == 0) {
                    return java.sql.ParameterMetaData.parameterModeOut;
                }
            }
            columnMetaData_.checkForValidColumnIndex(param);
            if (columnMetaData_.sqlxParmmode_[param - 1] == java.sql.ParameterMetaData.parameterModeUnknown) {
                return java.sql.ParameterMetaData.parameterModeUnknown;
            } else if (columnMetaData_.sqlxParmmode_[param - 1] == java.sql.ParameterMetaData.parameterModeIn) {
                return java.sql.ParameterMetaData.parameterModeIn;
            } else if (columnMetaData_.sqlxParmmode_[param - 1] == java.sql.ParameterMetaData.parameterModeOut) {
                return java.sql.ParameterMetaData.parameterModeOut;
            } else {
                return java.sql.ParameterMetaData.parameterModeInOut;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int isNullable(int param) throws SQLException {
        if (escapedProcedureCallWithResult_) {
            param--;
            if (param == 0) {
                return java.sql.ResultSetMetaData.columnNoNulls;
            }
        }
        return columnMetaData_.isNullable(param);
    }

    public boolean isSigned(int param) throws SQLException {
        if (escapedProcedureCallWithResult_) {
            param--;
            if (param == 0) {
                return true;
            }
        }
        return columnMetaData_.isSigned(param);
    }

    public int getPrecision(int param) throws SQLException {
        if (escapedProcedureCallWithResult_) {
            param--;
            if (param == 0) {
                return 10;
            }
        }
        return columnMetaData_.getPrecision(param);
    }

    public int getScale(int param) throws SQLException {
        if (escapedProcedureCallWithResult_) {
            param--;
            if (param == 0) {
                return 0;
            }
        }
        return columnMetaData_.getScale(param);
    }

}


