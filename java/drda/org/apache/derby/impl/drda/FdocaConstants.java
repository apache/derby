/*

   Derby - Class org.apache.derby.impl.drda.FdocaConstants

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
import org.apache.derby.iapi.reference.JDBC30Translation;
import java.sql.SQLException;
import org.apache.derby.iapi.reference.DB2Limit;

class FdocaConstants
{
// DRDA FD:OCA constants.
  static final int FDOCA_TYPE_FIXEDBYTES = 0x01;
  static final int FDOCA_TYPE_NFIXEDBYTES = 0x81;
  static final int FDOCA_TYPE_VARBYTES = 0x02;
  static final int FDOCA_TYPE_NVARBYTES = 0x82;
  static final int FDOCA_TYPE_NTBYTES = 0x03;
  static final int FDOCA_TYPE_NNTBYTES = 0x83;
  static final int FDOCA_TYPE_PSCLBYTE = 0x07;
  static final int FDOCA_TYPE_NPSCLBYTE = 0x87;
  static final int FDOCA_TYPE_FIXEDCHAR = 0x10;
  static final int FDOCA_TYPE_NFIXEDCHAR = 0x90;
  static final int FDOCA_TYPE_VARCHAR = 0x11;
  static final int FDOCA_TYPE_NVARCHAR = 0x91;
  static final int FDOCA_TYPE_NTCHAR = 0x14;
  static final int FDOCA_TYPE_NNTCHAR = 0x94;
  static final int FDOCA_TYPE_PSCLCHAR = 0x19;
  static final int FDOCA_TYPE_NPSCLCHAR = 0x99;
  static final int FDOCA_TYPE_INTEGER_BE = 0x23;
  static final int FDOCA_TYPE_NINTEGER_BE = 0xA3;
  static final int FDOCA_TYPE_INTEGER_LE = 0x24;
  static final int FDOCA_TYPE_NINTEGER_LE = 0xA4;
  static final int FDOCA_TYPE_DECIMAL = 0x30;
  static final int FDOCA_TYPE_NDECIMAL = 0xB0;
  static final int FDOCA_TYPE_NUMERIC_CHAR = 0x32;
  static final int FDOCA_TYPE_NNUMERIC_CHAR = 0xB2;
  static final int FDOCA_TYPE_ZDECIMAL_IBM = 0x33;   //370, 390, zOS and 400
  static final int FDOCA_TYPE_NZDECIMAL_IBM = 0xB3;  //370, 390, zOS and 400
  static final int FDOCA_TYPE_ZDECIMAL = 0x35;
  static final int FDOCA_TYPE_NZDECIMAL = 0xB5;
  static final int FDOCA_TYPE_FLOAT_370 = 0x40;
  static final int FDOCA_TYPE_NFLOAT_370 = 0xC0;
  static final int FDOCA_TYPE_FLOAT_X86 = 0x47;
  static final int FDOCA_TYPE_NFLOAT_X86 = 0xC7;
  static final int FDOCA_TYPE_FLOAT_IEEE = 0x48;   //including 400
  static final int FDOCA_TYPE_NFLOAT_IEEE = 0xC8;  //including 400
  static final int FDOCA_TYPE_FLOAT_VAX = 0x49;
  static final int FDOCA_TYPE_NFLOAT_VAX = 0xC9;
  static final int FDOCA_TYPE_LOBBYTES = 0x50;
  static final int FDOCA_TYPE_NLOBBYTES = 0xD0;
  static final int FDOCA_TYPE_LOBCHAR = 0x51;
  static final int FDOCA_TYPE_NLOBCHAR = 0xD1;

  // DRDA Type constants.
  static final int DRDA_TYPE_INTEGER = 0x02;
  static final int DRDA_TYPE_NINTEGER = 0x03;
  static final int DRDA_TYPE_SMALL = 0x04;
  static final int DRDA_TYPE_NSMALL = 0x05;
  static final int DRDA_TYPE_1BYTE_INT = 0x06;
  static final int DRDA_TYPE_N1BYTE_INT = 0x07;
  static final int DRDA_TYPE_FLOAT16 = 0x08;
  static final int DRDA_TYPE_NFLOAT16 = 0x09;
  static final int DRDA_TYPE_FLOAT8 = 0x0A;
  static final int DRDA_TYPE_NFLOAT8 = 0x0B;
  static final int DRDA_TYPE_FLOAT4 = 0x0C;
  static final int DRDA_TYPE_NFLOAT4 = 0x0D;
  static final int DRDA_TYPE_DECIMAL = 0x0E;
  static final int DRDA_TYPE_NDECIMAL = 0x0F;
  static final int DRDA_TYPE_ZDECIMAL = 0x10;
  static final int DRDA_TYPE_NZDECIMAL = 0x11;
  static final int DRDA_TYPE_NUMERIC_CHAR = 0x12;
  static final int DRDA_TYPE_NNUMERIC_CHAR = 0x13;
  static final int DRDA_TYPE_RSET_LOC = 0x14;
  static final int DRDA_TYPE_NRSET_LOC = 0x15;
  static final int DRDA_TYPE_INTEGER8 = 0x16;
  static final int DRDA_TYPE_NINTEGER8 = 0x17;
  static final int DRDA_TYPE_LOBLOC = 0x18;
  static final int DRDA_TYPE_NLOBLOC = 0x19;
  static final int DRDA_TYPE_CLOBLOC = 0x1A;
  static final int DRDA_TYPE_NCLOBLOC = 0x1B;
  static final int DRDA_TYPE_DBCSCLOBLOC = 0x1C;
  static final int DRDA_TYPE_NDBCSCLOBLOC = 0x1D;
  static final int DRDA_TYPE_ROWID = 0x1E;
  static final int DRDA_TYPE_NROWID = 0x1F;
  static final int DRDA_TYPE_DATE = 0x20;
  static final int DRDA_TYPE_NDATE = 0x21;
  static final int DRDA_TYPE_TIME = 0x22;
  static final int DRDA_TYPE_NTIME = 0x23;
  static final int DRDA_TYPE_TIMESTAMP = 0x24;
  static final int DRDA_TYPE_NTIMESTAMP = 0x25;
  static final int DRDA_TYPE_FIXBYTE = 0x26;
  static final int DRDA_TYPE_NFIXBYTE = 0x27;
  static final int DRDA_TYPE_VARBYTE = 0x28;
  static final int DRDA_TYPE_NVARBYTE = 0x29;
  static final int DRDA_TYPE_LONGVARBYTE = 0x2A;
  static final int DRDA_TYPE_NLONGVARBYTE = 0x2B;
  static final int DRDA_TYPE_NTERMBYTE = 0x2C;
  static final int DRDA_TYPE_NNTERMBYTE = 0x2D;
  static final int DRDA_TYPE_CSTR = 0x2E;
  static final int DRDA_TYPE_NCSTR = 0x2F;
  static final int DRDA_TYPE_CHAR = 0x30;
  static final int DRDA_TYPE_NCHAR = 0x31;
  static final int DRDA_TYPE_VARCHAR = 0x32;
  static final int DRDA_TYPE_NVARCHAR = 0x33;
  static final int DRDA_TYPE_LONG = 0x34;
  static final int DRDA_TYPE_NLONG = 0x35;
  static final int DRDA_TYPE_GRAPHIC = 0x36;
  static final int DRDA_TYPE_NGRAPHIC = 0x37;
  static final int DRDA_TYPE_VARGRAPH = 0x38;
  static final int DRDA_TYPE_NVARGRAPH = 0x39;
  static final int DRDA_TYPE_LONGRAPH = 0x3A;
  static final int DRDA_TYPE_NLONGRAPH = 0x3B;
  static final int DRDA_TYPE_MIX = 0x3C;
  static final int DRDA_TYPE_NMIX = 0x3D;
  static final int DRDA_TYPE_VARMIX = 0x3E;
  static final int DRDA_TYPE_NVARMIX = 0x3F;
  static final int DRDA_TYPE_LONGMIX = 0x40;
  static final int DRDA_TYPE_NLONGMIX = 0x41;
  static final int DRDA_TYPE_CSTRMIX = 0x42;
  static final int DRDA_TYPE_NCSTRMIX = 0x43;
  static final int DRDA_TYPE_PSCLBYTE = 0x44;
  static final int DRDA_TYPE_NPSCLBYTE = 0x45;
  static final int DRDA_TYPE_LSTR = 0x46;
  static final int DRDA_TYPE_NLSTR = 0x47;
  static final int DRDA_TYPE_LSTRMIX = 0x48;
  static final int DRDA_TYPE_NLSTRMIX = 0x49;
  static final int DRDA_TYPE_SDATALINK = 0x4C;
  static final int DRDA_TYPE_NSDATALINK = 0x4D;
  static final int DRDA_TYPE_MDATALINK = 0x4E;
  static final int DRDA_TYPE_NMDATALINK = 0x4F;

  // --- Override LIDs 0x50 - 0xAF
  static final int DRDA_TYPE_LOBBYTES = 0xC8;
  static final int DRDA_TYPE_NLOBBYTES = 0xC9;
  static final int DRDA_TYPE_LOBCSBCS = 0xCA;
  static final int DRDA_TYPE_NLOBCSBCS = 0xCB;
  static final int DRDA_TYPE_LOBCDBCS = 0xCC;
  static final int DRDA_TYPE_NLOBCDBCS = 0xCD;
  static final int DRDA_TYPE_LOBCMIXED = 0xCE;
  static final int DRDA_TYPE_NLOBCMIXED = 0xCF;

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
	protected static int LONGVARCHAR_MAX_LEN = DB2Limit.DB2_LONGVARCHAR_MAXWIDTH;
	protected static int LONGVARBINARY_MAX_LEN = DB2Limit.DB2_LONGVARCHAR_MAXWIDTH;
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
				drdaType = FdocaConstants.DRDA_TYPE_NSMALL;
				outlen[0] = 2;
				break;
			case java.sql.Types.INTEGER:
				drdaType = FdocaConstants.DRDA_TYPE_NINTEGER;
				outlen[0] = 4;
				break;
			case java.sql.Types.BIGINT:
				drdaType = FdocaConstants.DRDA_TYPE_NINTEGER8;
				outlen[0] = 8;
				break;
			case java.sql.Types.REAL:
				drdaType = FdocaConstants.DRDA_TYPE_NFLOAT4;
				outlen[0] = 4;
				break;
			case java.sql.Types.DOUBLE:
			case java.sql.Types.FLOAT:
				drdaType = FdocaConstants.DRDA_TYPE_NFLOAT8;
				outlen[0] = 8;
				break;
			case java.sql.Types.NUMERIC:
			case java.sql.Types.DECIMAL:
				drdaType = FdocaConstants.DRDA_TYPE_NDECIMAL;
				//needs to be adjusted for actual value
				outlen[0] = -1;
				break;
			case java.sql.Types.DATE:
				drdaType = FdocaConstants.DRDA_TYPE_NDATE;
				outlen[0] = 10;
				break;
			case java.sql.Types.TIME:
				drdaType = FdocaConstants.DRDA_TYPE_NTIME;
				outlen[0] = 8;
				break;
			case java.sql.Types.TIMESTAMP:
				drdaType = FdocaConstants.DRDA_TYPE_NTIMESTAMP;
				outlen[0] = 26;
				break;
			case java.sql.Types.CHAR:
//				drdaType = FdocaConstants.DRDA_TYPE_NCHAR;
				//making this NVARMIX for now to handle different byte length
				//characters - checking with Paul to see if this is the
				//correct way to handle it.
				drdaType = FdocaConstants.DRDA_TYPE_NVARMIX;
				outlen[0] = -1;
				break;
			case java.sql.Types.VARCHAR:
				drdaType = FdocaConstants.DRDA_TYPE_NVARCHAR;
				outlen[0] = -1;
				break;
				// we will just convert a java object to a string
				// since jcc doesn't support it.
			case java.sql.Types.JAVA_OBJECT:
				drdaType = FdocaConstants.DRDA_TYPE_NLONG;
				outlen[0] = LONGVARCHAR_MAX_LEN;
				break;
			case java.sql.Types.LONGVARCHAR:
					drdaType = DRDA_TYPE_NLONG;
					outlen[0] = LONGVARCHAR_MAX_LEN;
				break;
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
				drdaType = FdocaConstants.DRDA_TYPE_NVARBYTE;
				outlen[0] = -1;
				break;
			case java.sql.Types.LONGVARBINARY:
					drdaType = FdocaConstants.DRDA_TYPE_NLONGVARBYTE;
					outlen[0] = LONGVARBINARY_MAX_LEN;
				break;
				// blob begin
				// merge BLOB and BLOB_LOCATOR ????
			case java.sql.Types.BLOB:
				drdaType = FdocaConstants.DRDA_TYPE_NLOBBYTES;
				// indicates fdocadata is a place holder with 4 byte length
				outlen[0] = 0x8004;
				break;
			case java.sql.Types.CLOB:
				drdaType = FdocaConstants.DRDA_TYPE_NLOBCMIXED;
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
