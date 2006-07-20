/*

   Derby - Class org.apache.derby.client.am.Types

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

import org.apache.derby.iapi.reference.DRDAConstants;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.JDBC40Translation;

// This enumeration of types represents the typing scheme used by our jdbc driver.
// Once this is finished, we need to review our switches to make sure they are exhaustive

public class Types {
    // -------------------------------- Driver types -------------------------------------------------

    // Not currently supported as a DERBY column type.  Mapped to SMALLINT.
    // public final static int BIT        =  java.sql.Types.BIT;          // -7;

    // Not currently supported as a DERBY column type.  Mapped to SMALLINT.
    //public final static int TINYINT 	= java.sql.Types.TINYINT;       // -6;

    public final static int BOOLEAN = JDBC30Translation.BOOLEAN;       // 16;

    public final static int SMALLINT = java.sql.Types.SMALLINT;      // 5;

    public final static int INTEGER = java.sql.Types.INTEGER;       // 4;

    public final static int BIGINT = java.sql.Types.BIGINT;        // -5;

    // We type using DOUBLE
    //public final static int FLOAT 	= java.sql.Types.FLOAT;         // 6;

    public final static int REAL = java.sql.Types.REAL;          // 7;

    public final static int DOUBLE = java.sql.Types.DOUBLE;        // 8;

    // We type using DECIMAL
    //public final static int NUMERIC 	= java.sql.Types.NUMERIC;       // 2;

    public final static int DECIMAL = java.sql.Types.DECIMAL;       // 3;

    public final static int CHAR = java.sql.Types.CHAR;          // 1;

    public final static int VARCHAR = java.sql.Types.VARCHAR;       // 12;

    public final static int LONGVARCHAR = java.sql.Types.LONGVARCHAR;   // -1;

    public final static int DATE = java.sql.Types.DATE;          // 91;

    public final static int TIME = java.sql.Types.TIME;          // 92;

    public final static int TIMESTAMP = java.sql.Types.TIMESTAMP;     // 93;

    public final static int BINARY = java.sql.Types.BINARY;        // -2;

    public final static int VARBINARY = java.sql.Types.VARBINARY;     // -3;

    public final static int LONGVARBINARY = java.sql.Types.LONGVARBINARY; // -4;

    public final static int BLOB = java.sql.Types.BLOB;          // 2004;

    public final static int CLOB = java.sql.Types.CLOB;          // 2005;

    // hide the default constructor
    private Types() {
   }
    
    static public String getTypeString(int type)
    {
        switch (type )
        {
            case BIGINT:        return "BIGINT";
            case BINARY:        return "BINARY";
            case BLOB:          return "BLOB";
            case BOOLEAN:       return "BOOLEAN";
            case CHAR:          return "CHAR";
            case CLOB:          return "CLOB";
            case DATE:          return "DATE";
            case DECIMAL:       return "DECIMAL";
            case DOUBLE:        return "DOUBLE";
            case INTEGER:       return "INTEGER";
            case LONGVARBINARY: return "LONGVARBINARY";
            case LONGVARCHAR:   return "LONGVARCHAR";
            case REAL:          return "REAL";
            case SMALLINT:      return "SMALLINT";
            case TIME:          return "TIME";
            case TIMESTAMP:     return "TIMESTAMP";
            case VARBINARY:     return "VARBINARY";
            case VARCHAR:       return "VARCHAR";
            // Types we don't support:
            case java.sql.Types.ARRAY: return "ARRAY";
            case java.sql.Types.DATALINK: return "DATALINK";
            case JDBC40Translation.NCHAR: return "NATIONAL CHAR";
            case JDBC40Translation.NCLOB: return "NCLOB";
            case JDBC40Translation.NVARCHAR: return "NATIONAL CHAR VARYING";
            case JDBC40Translation.LONGNVARCHAR: return "LONG NVARCHAR";
            case java.sql.Types.REF: return "REF";
            case JDBC40Translation.ROWID: return "ROWID";
            case JDBC40Translation.SQLXML: return "SQLXML";
            case java.sql.Types.STRUCT: return "STRUCT";
            // Unknown type:
            default:            return "<UNKNOWN>";
        }
    }

    static public int mapDERBYTypeToDriverType(boolean isDescribed, int sqlType, long length, int ccsid) {
        switch (Utils.getNonNullableSqlType(sqlType)) { // mask the isNullable bit
        case DRDAConstants.DB2_SQLTYPE_SMALL:
            return SMALLINT;
        case DRDAConstants.DB2_SQLTYPE_INTEGER:
            return INTEGER;
        case DRDAConstants.DB2_SQLTYPE_BIGINT:
            return BIGINT;
        case DRDAConstants.DB2_SQLTYPE_FLOAT:
            if (length == 16)                  // can map to either NUMERIC or DECIMAL
            {
                return DECIMAL;
            } else if (length == 8)              // can map to either DOUBLE or FLOAT
            {
                return DOUBLE;
            } else if (length == 4) {
                return REAL;
            } else {
                return 0;
            }
        case DRDAConstants.DB2_SQLTYPE_DECIMAL:            // can map to either NUMERIC or DECIMAL
        case DRDAConstants.DB2_SQLTYPE_NUMERIC:            // can map to either NUMERIC or DECIMAL
            return DECIMAL;
        case DRDAConstants.DB2_SQLTYPE_CHAR:    // mixed and single byte
            if (isDescribed && (ccsid == 0xffff || ccsid == 0)) {
                return BINARY;
            } else {
                return CHAR;
            }
        case DRDAConstants.DB2_SQLTYPE_CSTR:    // null terminated SBCS/Mixed
            return CHAR;
            // use ccsid to distinguish between BINARY and CHAR, VARBINARY and VARCHAR, LONG...
        case DRDAConstants.DB2_SQLTYPE_VARCHAR:   // variable character SBCS/Mixed
            if (isDescribed && (ccsid == 0xffff || ccsid == 0)) {
                return VARBINARY;
            } else {
                return VARCHAR;
            }
        case DRDAConstants.DB2_SQLTYPE_LONG:      // long varchar SBCS/Mixed
            if (isDescribed && (ccsid == 0xffff || ccsid == 0)) {
                return LONGVARBINARY;
            } else {
                return LONGVARCHAR;
            }
        case DRDAConstants.DB2_SQLTYPE_DATE:
            return DATE;
        case DRDAConstants.DB2_SQLTYPE_TIME:
            return TIME;
        case DRDAConstants.DB2_SQLTYPE_TIMESTAMP:
            return TIMESTAMP;
        case DRDAConstants.DB2_SQLTYPE_CLOB:    // large object character SBCS/Mixed
            return Types.CLOB;
        case DRDAConstants.DB2_SQLTYPE_BLOB:    // large object bytes
            return java.sql.Types.BLOB;
        default:
            return 0;
        }
    }
}
