/*

   Derby - Class org.apache.derby.impl.drda.FdocaConstants

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
package org.apache.derby.impl.drda;

import java.sql.Types;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.DRDAConstants;
import java.sql.SQLException;
import org.apache.derby.iapi.reference.Limits;

class FdocaConstants
{
  //
  // This is where DRDA FD:OCA constants used to live. They were removed
  // because they were not referenced anywhere.
  //

  static final int CPT_TRIPLET_TYPE = 0x7F;      // CPT triplet type
  static final int MDD_TRIPLET_TYPE = 0x78;      // MDD triplet type
  static final int NGDA_TRIPLET_TYPE = 0x76;     // N-GDA triplet type
  static final int RLO_TRIPLET_TYPE = 0x71;      // RLO triplet type
  static final int SDA_TRIPLET_TYPE = 0x70;      // SDA triplet type

  static final int SDA_MD_TYPE = 1;              // SDA MD type
  static final int GDA_MD_TYPE = 2;              // GDA MD type
  static final int ROW_MD_TYPE = 3;              // Row MD type

  static final int SQLCADTA_LID = 0xE0;
  static final int SQLDTAGRP_LID = 0xD0;         // SQLDTAGRP LID
  static final int NULL_LID = 0x00;

  static final int INDICATOR_NULLABLE = 0x00;
  static final int NULL_DATA = 0xFF;
  static final int TYP_NULLIND = 1;
  static final int MAX_ENV_LID = 0x49;           // Largest possible N-GDA/CPT repeating

  static final int MAX_VARS_IN_NGDA = 84;        // Number of SQLVARs in full SQLDTAGRP
                                                 // N-GDA or CPT
  static final int FULL_NGDA_SIZE = 255;         // Size of full SQLDTAGRP N-GDA or CPT
  static final int MDD_TRIPLET_SIZE = 7;         // Size of MDD triplet
  static final int SDA_TRIPLET_SIZE = 12;        // Size of SDA triplet
  static final int SQLDTA_RLO_SIZE = 6;          // Size of SQLDTA RLO triplet
  static final int RLO_RPT_GRP_SIZE = 3;         // Size of RLO repeating group
  static final int SQLDTAGRP_SIZE = 3;           // Size of SQLDTAGRP descriptor
  static final int CPT_SIZE = 3;                 // Size of CPT descriptor
  static final int FDODSC_FOOTER_SIZE = 6;       // Size of regular FDODSC "footer" (RLO)
  static final int SQLDTAGRP_COL_DSC_SIZE = 3;   // Env. LID & len. bytes
  static final int MAX_OVERRIDES = 250;          // Max nbr of overrides via pairs of MDD + SDA
  static final int MDD_REST_SIZE = 5;            // Size of the MDD group minus length and type

  // Hard-coded SQLCADTA MDD
  static final byte[] SQLCADTA_MDD = {
    (byte)0x07, (byte)0x78, (byte)0x00,
    (byte)0x05, (byte)0x03, (byte)0x01,
    (byte)0xE0
  };

  // Hard-coded SQLDTA MDD
  static final byte[] SQLDTA_MDD = {
    (byte)0x07, (byte)0x78, (byte)0x00,
    (byte)0x05, (byte)0x04, (byte)0x01,
    (byte)0xF0
  };

  // Hard-coded SQLDTA MDD
  static final byte[] SQLDTAGRP_MDD = {
    (byte)0x07, (byte)0x78, (byte)0x00,
    (byte)0x05, (byte)0x02, (byte)0x01,
    (byte)0xD0
  };

  // Hard-coded SQLCADTA+SQLDTARD footer bytes
  static final byte[] SQLCADTA_SQLDTARD_RLO = {
    (byte)0x09, (byte)0x71, (byte)0xE0,   // SQLCADTA
    (byte)0x54, (byte)0x00, (byte)0x01,
    (byte)0xD0, (byte)0x00, (byte)0x01,
    (byte)0x06, (byte)0x71, (byte)0xF0,   // SQLDTARD
    (byte)0xE0, (byte)0x00, (byte)0x00
  };

  // Hard-coded SQLDTA RLO
  static final byte[] SQLDTA_RLO = {
    (byte)0x06, (byte)0x71, (byte)0xE4,
    (byte)0xD0, (byte)0x00, (byte)0x01
  };

  static final int SQLCADTA_SQLDTARD_RLO_SIZE = SQLCADTA_SQLDTARD_RLO.length;

	protected static boolean isNullable(int fdocaType)
	{
		return ( (fdocaType & 1) == 1);
	}

	// The maxumum length for LONG VARCHAR RETURN RESULTS IS
	// 64K, since we send an unsigned short.  We should be
	// able to send the number of bytes in which we encode the
	// length as 4 (or more) , but JCC does not support this yet.
	// JAVA_OBJECTS are returned as LONG VARCHAR values by calling
	// their toString() method and their limit is 64K as well.
	// BUT, that said, we ultimately have to match DB2's limit,
	// so just use that...
	protected static int LONGVARCHAR_MAX_LEN = Limits.DB2_LONGVARCHAR_MAXWIDTH;
	protected static int LONGVARBINARY_MAX_LEN = Limits.DB2_LONGVARCHAR_MAXWIDTH;
	protected static int LONGVARCHAR_LEN_NUMBYTES = 2;

	// JCC  only supports a max precision of 31 like DB2
	protected static int NUMERIC_MAX_PRECISION=31;
	protected static int NUMERIC_DEFAULT_PRECISION=NUMERIC_MAX_PRECISION;
	protected static int NUMERIC_DEFAULT_SCALE=15;

	/***
	 * Map jdbctype to fdoca drda type
	 * @param jdbcType - Jdbc type for mappingy
	 * @param nullable - true if type is nullable
	 * @param outlen - output parameter with length of type.
	 * @return standard drdaTypeLength. -1 if we don't know.
	 **/
	protected static int mapJdbcTypeToDrdaType(int jdbcType, boolean nullable,
											   int[] outlen)
		throws SQLException
	{
		int drdaType = 0;
		switch (jdbcType) {
			case JDBC30Translation.BOOLEAN:
			case java.sql.Types.BIT:
			case java.sql.Types.TINYINT:
			case java.sql.Types.SMALLINT:
				drdaType = DRDAConstants.DRDA_TYPE_NSMALL;
				outlen[0] = 2;
				break;
			case java.sql.Types.INTEGER:
				drdaType = DRDAConstants.DRDA_TYPE_NINTEGER;
				outlen[0] = 4;
				break;
			case java.sql.Types.BIGINT:
				drdaType = DRDAConstants.DRDA_TYPE_NINTEGER8;
				outlen[0] = 8;
				break;
			case java.sql.Types.REAL:
				drdaType = DRDAConstants.DRDA_TYPE_NFLOAT4;
				outlen[0] = 4;
				break;
			case java.sql.Types.DOUBLE:
			case java.sql.Types.FLOAT:
				drdaType = DRDAConstants.DRDA_TYPE_NFLOAT8;
				outlen[0] = 8;
				break;
			case java.sql.Types.NUMERIC:
			case java.sql.Types.DECIMAL:
				drdaType = DRDAConstants.DRDA_TYPE_NDECIMAL;
				//needs to be adjusted for actual value
				outlen[0] = -1;
				break;
			case java.sql.Types.DATE:
				drdaType = DRDAConstants.DRDA_TYPE_NDATE;
				outlen[0] = 10;
				break;
			case java.sql.Types.TIME:
				drdaType = DRDAConstants.DRDA_TYPE_NTIME;
				outlen[0] = 8;
				break;
			case java.sql.Types.TIMESTAMP:
				drdaType = DRDAConstants.DRDA_TYPE_NTIMESTAMP;
				outlen[0] = 26;
				break;
			case java.sql.Types.CHAR:
//				drdaType = DRDAConstants.DRDA_TYPE_NCHAR;
				//making this NVARMIX for now to handle different byte length
				//characters - checking with Paul to see if this is the
				//correct way to handle it.
				drdaType = DRDAConstants.DRDA_TYPE_NVARMIX;
				outlen[0] = -1;
				break;
			case java.sql.Types.VARCHAR:
				drdaType = DRDAConstants.DRDA_TYPE_NVARCHAR;
				outlen[0] = -1;
				break;
				// we will just convert a java object to a string
				// since jcc doesn't support it.
			case java.sql.Types.JAVA_OBJECT:
				drdaType = DRDAConstants.DRDA_TYPE_NLONG;
				outlen[0] = LONGVARCHAR_MAX_LEN;
				break;
			case java.sql.Types.LONGVARCHAR:
					drdaType = DRDAConstants.DRDA_TYPE_NLONG;
					outlen[0] = LONGVARCHAR_MAX_LEN;
				break;
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
				drdaType = DRDAConstants.DRDA_TYPE_NVARBYTE;
				outlen[0] = -1;
				break;
			case java.sql.Types.LONGVARBINARY:
					drdaType = DRDAConstants.DRDA_TYPE_NLONGVARBYTE;
					outlen[0] = LONGVARBINARY_MAX_LEN;
				break;
				// blob begin
				// merge BLOB and BLOB_LOCATOR ????
			case java.sql.Types.BLOB:
				drdaType = DRDAConstants.DRDA_TYPE_NLOBBYTES;
				// indicates fdocadata is a place holder with 4 byte length
				outlen[0] = 0x8004;
				break;
			case java.sql.Types.CLOB:
				drdaType = DRDAConstants.DRDA_TYPE_NLOBCMIXED;
				outlen[0] = 0x8004;
				break;
				// blob end
			case java.sql.Types.ARRAY:
			case java.sql.Types.DISTINCT:
			case java.sql.Types.NULL:
			case java.sql.Types.OTHER:
			case java.sql.Types.REF:
			case java.sql.Types.STRUCT:
				throw new SQLException("Jdbc type" + jdbcType + "not Supported yet");
			default:
				throw new SQLException ("unrecognized sql type: " + jdbcType);
		}

		if (!nullable)
			drdaType--;
		return drdaType;
	}


}
