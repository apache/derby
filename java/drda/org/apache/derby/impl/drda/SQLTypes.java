/*

   Derby - Class org.apache.derby.impl.drda.SQLTypes

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.drda;

import java.sql.Types;
import java.sql.SQLException;
import org.apache.derby.iapi.reference.JDBC30Translation;



public class SQLTypes {

  //----------------------------------------------------------------------------
  protected final static int DB2_SQLTYPE_DATE = 384;        // DATE
  protected final static int DB2_SQLTYPE_NDATE = 385;
  protected final static int DB2_SQLTYPE_TIME = 388;        // TIME
  protected final static int DB2_SQLTYPE_NTIME = 389;
  protected final static int DB2_SQLTYPE_TIMESTAMP = 392;   // TIMESTAMP
  protected final static int DB2_SQLTYPE_NTIMESTAMP = 393;
  protected final static int DB2_SQLTYPE_DATALINK = 396;    // DATALINK
  protected final static int DB2_SQLTYPE_NDATALINK = 397;

  protected final static int DB2_SQLTYPE_BLOB = 404;        // BLOB
  protected final static int DB2_SQLTYPE_NBLOB = 405;
  protected final static int DB2_SQLTYPE_CLOB = 408;        // CLOB
  protected final static int DB2_SQLTYPE_NCLOB = 409;
  protected final static int DB2_SQLTYPE_DBCLOB = 412;      // DBCLOB
  protected final static int DB2_SQLTYPE_NDBCLOB = 413;

  protected final static int DB2_SQLTYPE_VARCHAR = 448;     // VARCHAR(i) - varying length string
  protected final static int DB2_SQLTYPE_NVARCHAR = 449;
  protected final static int DB2_SQLTYPE_CHAR = 452;        // CHAR(i) - fixed length
  protected final static int DB2_SQLTYPE_NCHAR = 453;
  protected final static int DB2_SQLTYPE_LONG = 456;        // LONG VARCHAR - varying length string
  protected final static int DB2_SQLTYPE_NLONG = 457;
  protected final static int DB2_SQLTYPE_CSTR = 460;        // SBCS - null terminated
  protected final static int DB2_SQLTYPE_NCSTR = 461;
  protected final static int DB2_SQLTYPE_VARGRAPH = 464;    // VARGRAPHIC(i) - varying length
                                                  // graphic string (2 byte length)
  protected final static int DB2_SQLTYPE_NVARGRAPH = 465;
  protected final static int DB2_SQLTYPE_GRAPHIC = 468;     // GRAPHIC(i) - fixed length graphic string                                                             */
  protected final static int DB2_SQLTYPE_NGRAPHIC = 469;
  protected final static int DB2_SQLTYPE_LONGRAPH = 472;    // LONG VARGRAPHIC(i) - varying length graphic string                                              */
  protected final static int DB2_SQLTYPE_NLONGRAPH = 473;
  protected final static int DB2_SQLTYPE_LSTR = 476;        // varying length string for Pascal (1-byte length)                                                     */
  protected final static int DB2_SQLTYPE_NLSTR = 477;

  protected final static int DB2_SQLTYPE_FLOAT = 480;       // FLOAT - 4 or 8 byte floating point
  protected final static int DB2_SQLTYPE_NFLOAT = 481;
  protected final static int DB2_SQLTYPE_DECIMAL = 484;     // DECIMAL (m,n)
  protected final static int DB2_SQLTYPE_NDECIMAL = 485;
  protected final static int DB2_SQLTYPE_ZONED = 488;       // Zoned Decimal -> DECIMAL(m,n)
  protected final static int DB2_SQLTYPE_NZONED = 489;

  protected final static int DB2_SQLTYPE_BIGINT = 492;      // BIGINT - 8-byte signed integer
  protected final static int DB2_SQLTYPE_NBIGINT = 493;
  protected final static int DB2_SQLTYPE_INTEGER = 496;     // INTEGER
  protected final static int DB2_SQLTYPE_NINTEGER = 497;
  protected final static int DB2_SQLTYPE_SMALL = 500;       // SMALLINT - 2-byte signed integer                                                                    */
  protected final static int DB2_SQLTYPE_NSMALL = 501;

  protected final static int DB2_SQLTYPE_NUMERIC = 504;     // NUMERIC -> DECIMAL (m,n)
  protected final static int DB2_SQLTYPE_NNUMERIC = 505;

  protected final static int DB2_SQLTYPE_ROWID = 904;           // ROWID
  protected final static int DB2_SQLTYPE_NROWID = 905;
  protected final static int DB2_SQLTYPE_BLOB_LOCATOR = 960;    // BLOB locator
  protected final static int DB2_SQLTYPE_NBLOB_LOCATOR = 961;
  protected final static int DB2_SQLTYPE_CLOB_LOCATOR = 964;    // CLOB locator
  protected final static int DB2_SQLTYPE_NCLOB_LOCATOR = 965;
  protected final static int DB2_SQLTYPE_DBCLOB_LOCATOR = 968;  // DBCLOB locator
  protected final static int DB2_SQLTYPE_NDBCLOB_LOCATOR = 969;

  // define final statics for the fdoca type codes here!!!

  // hide the default constructor
  private SQLTypes() {}


  /**
   * Map DB2 SQL Type to JDBC Type
   * 
   * @param sqlType SQL Type to convert
   * @param length storage length of type
   * @param ccsid ccsid of type
   *
   * @return Corresponding JDBC Type 
   */

  static protected int mapDB2SqlTypeToJdbcType (int sqlType, long length, int ccsid)
  {
    switch (getNonNullableSqlType (sqlType)) { // mask the isNullable bit
    case DB2_SQLTYPE_SMALL:
      return java.sql.Types.SMALLINT;
    case DB2_SQLTYPE_INTEGER:
      return java.sql.Types.INTEGER;
    case DB2_SQLTYPE_BIGINT:
      return java.sql.Types.BIGINT;
    case DB2_SQLTYPE_FLOAT:
      if (length == 16)                  // can map to either NUMERIC or DECIMAL!!! @sxg
        return java.sql.Types.DECIMAL;
      else if (length == 8)              // can map to either DOUBLE or FLOAT!!! @sxg
        return java.sql.Types.DOUBLE;
      else if (length == 4)
        return java.sql.Types.REAL;
      else
        return 0;
        //throw new BugCheckException ("Encountered unexpected float length");
    case DB2_SQLTYPE_DECIMAL:            // can map to either NUMERIC or DECIMAL!!! @sxg
    case DB2_SQLTYPE_ZONED:              // can map to either NUMERIC or DECIMAL!!! @sxg
    case DB2_SQLTYPE_NUMERIC:            // can map to either NUMERIC or DECIMAL!!! @sxg
      return java.sql.Types.DECIMAL;
    case DB2_SQLTYPE_CHAR:    // mixed and single byte
      if (ccsid == 0xffff || ccsid == 0) // we think UW returns 0, and 390 returns 0xffff, doublecheck !!!
        return java.sql.Types.BINARY;
      else
        return java.sql.Types.CHAR;
    case DB2_SQLTYPE_CSTR:    // SBCS null terminated 
    case DB2_SQLTYPE_GRAPHIC: // fixed character DBCS
      return java.sql.Types.CHAR;
    // use ccsid to distinguish between BINARY and CHAR, VARBINARY and VARCHAR, LONG... !!! -j/p/s
    case DB2_SQLTYPE_VARGRAPH:  // variable character DBCS
    case DB2_SQLTYPE_VARCHAR:   // variable character SBCS/Mixed
      if (ccsid == 0xffff || ccsid == 0) // we think UW returns 0, and 390 returns 0xffff, doublecheck !!!
        return java.sql.Types.VARBINARY;
      else
        return java.sql.Types.VARCHAR;
    case DB2_SQLTYPE_LSTR:      // pascal string SBCS/Mixed
      return java.sql.Types.VARCHAR;
    case DB2_SQLTYPE_LONGRAPH:  // long varchar DBCS
    case DB2_SQLTYPE_LONG:      // long varchar SBCS/Mixed
      if (ccsid == 0xffff || ccsid == 0) // we think UW returns 0, and 390 returns 0xffff, doublecheck !!!
        return java.sql.Types.LONGVARBINARY;
      else
        return java.sql.Types.LONGVARCHAR;
    case DB2_SQLTYPE_DATE:
      return java.sql.Types.DATE;
    case DB2_SQLTYPE_TIME:
      return java.sql.Types.TIME;
    case DB2_SQLTYPE_TIMESTAMP:
      return java.sql.Types.TIMESTAMP;
    case DB2_SQLTYPE_CLOB:    // large object character SBCS/Mixed
    case DB2_SQLTYPE_DBCLOB:  // large object character DBCS
      return java.sql.Types.CLOB;
    case DB2_SQLTYPE_BLOB:    // large object bytes
		case DB2_SQLTYPE_BLOB_LOCATOR:
		case DB2_SQLTYPE_CLOB_LOCATOR:
		case DB2_SQLTYPE_DBCLOB_LOCATOR:
      return java.sql.Types.BLOB;
    default:
      //throw new BugCheckException ("Encountered unexpected type code");
      return 0;
    }
  }


	/**
	 * Map jdbc type to the DB2 DRDA SQL Types expected by jcc.
	 *@param jdbctype  - jdbc Type to convert
	 *@param nullable - whether the type is nullable
	 **/

	
 /**  Map JDBC Type to DB2 SqlType
  * @param jdbctype   JDBC Type from java.sql.Types
  * @param nullable   true if this is a nullable type
  * @param outlen     output parameter with type length
  *
  * @return Corresponding DB2 SQL Type (See DRDA Manual FD:OCA Meta 
  *          Data Summary, page 245)
  * 
  * @exception SQLException thrown for unrecognized SQLType
  */

 static protected int mapJdbcTypeToDB2SqlType (int jdbctype, boolean nullable,
											   int[] outlen)
	 throws SQLException
  {
	  int nullAddVal =0;

	  if (nullable) 
		  nullAddVal =1; 
	  
	  // Call FdocaConstants just to get the length
	  FdocaConstants.mapJdbcTypeToDrdaType(jdbctype,nullable,outlen);

	  switch(jdbctype)
	  {
		  case JDBC30Translation.BOOLEAN:
		  case java.sql.Types.BIT:
		  case java.sql.Types.TINYINT:
		  case java.sql.Types.SMALLINT:
			  return DB2_SQLTYPE_SMALL + nullAddVal;
		  case java.sql.Types.INTEGER:
			  return DB2_SQLTYPE_INTEGER + nullAddVal;
		  case java.sql.Types.BIGINT:
			  return DB2_SQLTYPE_BIGINT + nullAddVal;
		  case java.sql.Types.DOUBLE:
		  case java.sql.Types.REAL:
			  return DB2_SQLTYPE_FLOAT + nullAddVal;
		  case java.sql.Types.DECIMAL:
		  case java.sql.Types.NUMERIC:
			  return DB2_SQLTYPE_DECIMAL + nullAddVal;
		  case java.sql.Types.DATE:
			  return DB2_SQLTYPE_DATE + nullAddVal;
		  case java.sql.Types.TIME:
			  return DB2_SQLTYPE_TIME + nullAddVal;
		  case java.sql.Types.TIMESTAMP:
			  return DB2_SQLTYPE_TIMESTAMP + nullAddVal;
		  case java.sql.Types.CHAR:
			  return  DB2_SQLTYPE_CHAR + nullAddVal;    // null terminated SBCS/Mixed
		  case java.sql.Types.BINARY:
			  return DB2_SQLTYPE_CHAR + nullAddVal;

		  case java.sql.Types.VARCHAR:
		  case java.sql.Types.VARBINARY:
			  return  DB2_SQLTYPE_VARCHAR + nullAddVal;			  
		  case java.sql.Types.LONGVARBINARY:
			  return DB2_SQLTYPE_LONG + nullAddVal;
		  case java.sql.Types.JAVA_OBJECT:
			  return DB2_SQLTYPE_LONG + nullAddVal;
		  case java.sql.Types.BLOB:
			  return DB2_SQLTYPE_BLOB + nullAddVal;
		  case java.sql.Types.CLOB:
			  return DB2_SQLTYPE_CLOB + nullAddVal;
		  case java.sql.Types.LONGVARCHAR:
			  return DB2_SQLTYPE_LONG + nullAddVal;
		  case java.sql.Types.ARRAY:
		  case java.sql.Types.DISTINCT:
		  case java.sql.Types.NULL:
		  case java.sql.Types.OTHER:
		  case java.sql.Types.REF:
		  case java.sql.Types.STRUCT:
			  throw new SQLException("Jdbc type" + jdbctype + "not Supported yet");
			default:
				throw new SQLException ("unrecognized sql type: " + jdbctype);
			  //throw new BugCheckException ("Encountered unexpected type code");

	  }
  }

	/**
	 * Translate DB2 SQL Type to the non-nullable type.
	 * @param sqlType DB2 SQL Type
	 *
	 * @return The Non-Nullable DB2 SQL Type.
	 */
   protected  static int getNonNullableSqlType (int sqlType)
  {
    return sqlType & ~1;
  }


}
