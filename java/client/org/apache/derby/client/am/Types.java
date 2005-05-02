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

// This enumeration of types represents the typing scheme used by our jdbc driver.
// Once this is finished, we need to review our switches to make sure they are exhaustive

public class Types {
    // -------------------------------- Driver types -------------------------------------------------

    // Not currently supported as a DERBY column type.  Mapped to SMALLINT.
    // public final static int BIT        =  java.sql.Types.BIT;          // -7;

    // Not currently supported as a DERBY column type.  Mapped to SMALLINT.
    //public final static int TINYINT 	= java.sql.Types.TINYINT;       // -6;

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

    // -------------------------------- DERBY types ----------------------------------------------------

    public final static int DERBY_SQLTYPE_DATE = 384;        // DATE
    public final static int DERBY_SQLTYPE_NDATE = 385;
    public final static int DERBY_SQLTYPE_TIME = 388;        // TIME
    public final static int DERBY_SQLTYPE_NTIME = 389;
    public final static int DERBY_SQLTYPE_TIMESTAMP = 392;   // TIMESTAMP
    public final static int DERBY_SQLTYPE_NTIMESTAMP = 393;

    public final static int DERBY_SQLTYPE_BLOB = 404;        // BLOB
    public final static int DERBY_SQLTYPE_NBLOB = 405;
    public final static int DERBY_SQLTYPE_CLOB = 408;        // CLOB
    public final static int DERBY_SQLTYPE_NCLOB = 409;

    public final static int DERBY_SQLTYPE_VARCHAR = 448;     // VARCHAR(i) - varying length string
    public final static int DERBY_SQLTYPE_NVARCHAR = 449;
    public final static int DERBY_SQLTYPE_CHAR = 452;        // CHAR(i) - fixed length
    public final static int DERBY_SQLTYPE_NCHAR = 453;
    public final static int DERBY_SQLTYPE_LONG = 456;        // LONG VARCHAR - varying length string
    public final static int DERBY_SQLTYPE_NLONG = 457;
    public final static int DERBY_SQLTYPE_CSTR = 460;        // SBCS - null terminated
    public final static int DERBY_SQLTYPE_NCSTR = 461;

    public final static int DERBY_SQLTYPE_FLOAT = 480;       // FLOAT - 4 or 8 byte floating point
    public final static int DERBY_SQLTYPE_NFLOAT = 481;
    public final static int DERBY_SQLTYPE_DECIMAL = 484;     // DECIMAL (m,n)
    public final static int DERBY_SQLTYPE_NDECIMAL = 485;
    public final static int DERBY_SQLTYPE_BIGINT = 492;      // BIGINT - 8-byte signed integer
    public final static int DERBY_SQLTYPE_NBIGINT = 493;
    public final static int DERBY_SQLTYPE_INTEGER = 496;     // INTEGER
    public final static int DERBY_SQLTYPE_NINTEGER = 497;
    public final static int DERBY_SQLTYPE_SMALL = 500;       // SMALLINT - 2-byte signed integer                                                                    */
    public final static int DERBY_SQLTYPE_NSMALL = 501;

    public final static int DERBY_SQLTYPE_NUMERIC = 504;     // NUMERIC -> DECIMAL (m,n)
    public final static int DERBY_SQLTYPE_NNUMERIC = 505;

    static public int mapDERBYTypeToDriverType(boolean isDescribed, int sqlType, long length, int ccsid) {
        switch (Utils.getNonNullableSqlType(sqlType)) { // mask the isNullable bit
        case DERBY_SQLTYPE_SMALL:
            return SMALLINT;
        case DERBY_SQLTYPE_INTEGER:
            return INTEGER;
        case DERBY_SQLTYPE_BIGINT:
            return BIGINT;
        case DERBY_SQLTYPE_FLOAT:
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
        case DERBY_SQLTYPE_DECIMAL:            // can map to either NUMERIC or DECIMAL
        case DERBY_SQLTYPE_NUMERIC:            // can map to either NUMERIC or DECIMAL
            return DECIMAL;
        case DERBY_SQLTYPE_CHAR:    // mixed and single byte
            if (isDescribed && (ccsid == 0xffff || ccsid == 0)) {
                return BINARY;
            } else {
                return CHAR;
            }
        case DERBY_SQLTYPE_CSTR:    // null terminated SBCS/Mixed
            return CHAR;
            // use ccsid to distinguish between BINARY and CHAR, VARBINARY and VARCHAR, LONG...
        case DERBY_SQLTYPE_VARCHAR:   // variable character SBCS/Mixed
            if (isDescribed && (ccsid == 0xffff || ccsid == 0)) {
                return VARBINARY;
            } else {
                return VARCHAR;
            }
        case DERBY_SQLTYPE_LONG:      // long varchar SBCS/Mixed
            if (isDescribed && (ccsid == 0xffff || ccsid == 0)) {
                return LONGVARBINARY;
            } else {
                return LONGVARCHAR;
            }
        case DERBY_SQLTYPE_DATE:
            return DATE;
        case DERBY_SQLTYPE_TIME:
            return TIME;
        case DERBY_SQLTYPE_TIMESTAMP:
            return TIMESTAMP;
        case DERBY_SQLTYPE_CLOB:    // large object character SBCS/Mixed
            return Types.CLOB;
        case DERBY_SQLTYPE_BLOB:    // large object bytes
            return java.sql.Types.BLOB;
        default:
            return 0;
        }
    }
}
