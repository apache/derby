/*

   Derby - Class org.apache.derby.iapi.reference.DRDAConstants

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
/**
 * <p>
 * Machinery shared across Derby DRDA clients and server.
 * </p>
 */

package org.apache.derby.iapi.reference;

public	interface	DRDAConstants
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	//
	// Derby Product Identifiers as defined by the Open Group.
	// See http://www.opengroup.org/dbiop/prodid.htm for the
	// list of legal DRDA Product Identifiers.
	//
	public	static	final	String	DERBY_DRDA_SERVER_ID = "CSS";
	public	static	final	String	DERBY_DRDA_CLIENT_ID = "DNC";
	
	///////////////////////
	//
	// DRDA Type constants.
	//
	///////////////////////

	public	static final int DRDA_TYPE_INTEGER = 0x02;
	public	static final int DRDA_TYPE_NINTEGER = 0x03;
	public	static final int DRDA_TYPE_SMALL = 0x04;
	public	static final int DRDA_TYPE_NSMALL = 0x05;
	public	static final int DRDA_TYPE_1BYTE_INT = 0x06;
	public	static final int DRDA_TYPE_N1BYTE_INT = 0x07;
	public	static final int DRDA_TYPE_FLOAT16 = 0x08;
	public	static final int DRDA_TYPE_NFLOAT16 = 0x09;
	public	static final int DRDA_TYPE_FLOAT8 = 0x0A;
	public	static final int DRDA_TYPE_NFLOAT8 = 0x0B;
	public	static final int DRDA_TYPE_FLOAT4 = 0x0C;
	public	static final int DRDA_TYPE_NFLOAT4 = 0x0D;
	public	static final int DRDA_TYPE_DECIMAL = 0x0E;
	public	static final int DRDA_TYPE_NDECIMAL = 0x0F;
	public	static final int DRDA_TYPE_ZDECIMAL = 0x10;
	public	static final int DRDA_TYPE_NZDECIMAL = 0x11;
	public	static final int DRDA_TYPE_NUMERIC_CHAR = 0x12;
	public	static final int DRDA_TYPE_NNUMERIC_CHAR = 0x13;
	public	static final int DRDA_TYPE_RSET_LOC = 0x14;
	public	static final int DRDA_TYPE_NRSET_LOC = 0x15;
	public	static final int DRDA_TYPE_INTEGER8 = 0x16;
	public	static final int DRDA_TYPE_NINTEGER8 = 0x17;
	public	static final int DRDA_TYPE_LOBLOC = 0x18;
	public	static final int DRDA_TYPE_NLOBLOC = 0x19;
	public	static final int DRDA_TYPE_CLOBLOC = 0x1A;
	public	static final int DRDA_TYPE_NCLOBLOC = 0x1B;
	public	static final int DRDA_TYPE_DBCSCLOBLOC = 0x1C;
	public	static final int DRDA_TYPE_NDBCSCLOBLOC = 0x1D;
	public	static final int DRDA_TYPE_ROWID = 0x1E;
	public	static final int DRDA_TYPE_NROWID = 0x1F;
	public	static final int DRDA_TYPE_DATE = 0x20;
	public	static final int DRDA_TYPE_NDATE = 0x21;
	public	static final int DRDA_TYPE_TIME = 0x22;
	public	static final int DRDA_TYPE_NTIME = 0x23;
	public	static final int DRDA_TYPE_TIMESTAMP = 0x24;
	public	static final int DRDA_TYPE_NTIMESTAMP = 0x25;
	public	static final int DRDA_TYPE_FIXBYTE = 0x26;
	public	static final int DRDA_TYPE_NFIXBYTE = 0x27;
	public	static final int DRDA_TYPE_VARBYTE = 0x28;
	public	static final int DRDA_TYPE_NVARBYTE = 0x29;
	public	static final int DRDA_TYPE_LONGVARBYTE = 0x2A;
	public	static final int DRDA_TYPE_NLONGVARBYTE = 0x2B;
	public	static final int DRDA_TYPE_NTERMBYTE = 0x2C;
	public	static final int DRDA_TYPE_NNTERMBYTE = 0x2D;
	public	static final int DRDA_TYPE_CSTR = 0x2E;
	public	static final int DRDA_TYPE_NCSTR = 0x2F;
	public	static final int DRDA_TYPE_CHAR = 0x30;
	public	static final int DRDA_TYPE_NCHAR = 0x31;
	public	static final int DRDA_TYPE_VARCHAR = 0x32;
	public	static final int DRDA_TYPE_NVARCHAR = 0x33;
	public	static final int DRDA_TYPE_LONG = 0x34;
	public	static final int DRDA_TYPE_NLONG = 0x35;
	public	static final int DRDA_TYPE_GRAPHIC = 0x36;
	public	static final int DRDA_TYPE_NGRAPHIC = 0x37;
	public	static final int DRDA_TYPE_VARGRAPH = 0x38;
	public	static final int DRDA_TYPE_NVARGRAPH = 0x39;
	public	static final int DRDA_TYPE_LONGRAPH = 0x3A;
	public	static final int DRDA_TYPE_NLONGRAPH = 0x3B;
	public	static final int DRDA_TYPE_MIX = 0x3C;
	public	static final int DRDA_TYPE_NMIX = 0x3D;
	public	static final int DRDA_TYPE_VARMIX = 0x3E;
	public	static final int DRDA_TYPE_NVARMIX = 0x3F;
	public	static final int DRDA_TYPE_LONGMIX = 0x40;
	public	static final int DRDA_TYPE_NLONGMIX = 0x41;
	public	static final int DRDA_TYPE_CSTRMIX = 0x42;
	public	static final int DRDA_TYPE_NCSTRMIX = 0x43;
	public	static final int DRDA_TYPE_PSCLBYTE = 0x44;
	public	static final int DRDA_TYPE_NPSCLBYTE = 0x45;
	public	static final int DRDA_TYPE_LSTR = 0x46;
	public	static final int DRDA_TYPE_NLSTR = 0x47;
	public	static final int DRDA_TYPE_LSTRMIX = 0x48;
	public	static final int DRDA_TYPE_NLSTRMIX = 0x49;
	public	static final int DRDA_TYPE_SDATALINK = 0x4C;
	public	static final int DRDA_TYPE_NSDATALINK = 0x4D;
	public	static final int DRDA_TYPE_MDATALINK = 0x4E;
	public	static final int DRDA_TYPE_NMDATALINK = 0x4F;

	// --- Override LIDs 0x50 - 0xAF
	public	static final int DRDA_TYPE_LOBBYTES = 0xC8;
	public	static final int DRDA_TYPE_NLOBBYTES = 0xC9;
	public	static final int DRDA_TYPE_LOBCSBCS = 0xCA;
	public	static final int DRDA_TYPE_NLOBCSBCS = 0xCB;
	public	static final int DRDA_TYPE_LOBCDBCS = 0xCC;
	public	static final int DRDA_TYPE_NLOBCDBCS = 0xCD;
	public	static final int DRDA_TYPE_LOBCMIXED = 0xCE;
	public	static final int DRDA_TYPE_NLOBCMIXED = 0xCF;

	// Experimental types. These codes will change when the Open Group
	// publishes an addendum to the DRDA spec covering these
	// datatypes.
	
	// public	static final int DRDA_TYPE_BOOLEAN = 0xBE;
	// public	static final int DRDA_TYPE_NBOOLEAN = 0xBF;
	
	///////////////////////
	//
	// DB2 datatypes
	//
	///////////////////////

	public	static final  int DB2_SQLTYPE_DATE = 384;        // DATE
	public	static final  int DB2_SQLTYPE_NDATE = 385;
	public	static final  int DB2_SQLTYPE_TIME = 388;        // TIME
	public	static final  int DB2_SQLTYPE_NTIME = 389;
	public	static final  int DB2_SQLTYPE_TIMESTAMP = 392;   // TIMESTAMP
	public	static final  int DB2_SQLTYPE_NTIMESTAMP = 393;
	public	static final  int DB2_SQLTYPE_DATALINK = 396;    // DATALINK
	public	static final  int DB2_SQLTYPE_NDATALINK = 397;

	public	static final  int DB2_SQLTYPE_BLOB = 404;        // BLOB
	public	static final  int DB2_SQLTYPE_NBLOB = 405;
	public	static final  int DB2_SQLTYPE_CLOB = 408;        // CLOB
	public	static final  int DB2_SQLTYPE_NCLOB = 409;
	public	static final  int DB2_SQLTYPE_DBCLOB = 412;      // DBCLOB
	public	static final  int DB2_SQLTYPE_NDBCLOB = 413;

	public	static final  int DB2_SQLTYPE_VARCHAR = 448;     // VARCHAR(i) - varying length string
	public	static final  int DB2_SQLTYPE_NVARCHAR = 449;
	public	static final  int DB2_SQLTYPE_CHAR = 452;        // CHAR(i) - fixed length
	public	static final  int DB2_SQLTYPE_NCHAR = 453;
	public	static final  int DB2_SQLTYPE_LONG = 456;        // LONG VARCHAR - varying length string
	public	static final  int DB2_SQLTYPE_NLONG = 457;
	public	static final  int DB2_SQLTYPE_CSTR = 460;        // SBCS - null terminated
	public	static final  int DB2_SQLTYPE_NCSTR = 461;
	public	static final  int DB2_SQLTYPE_VARGRAPH = 464;    // VARGRAPHIC(i) - varying length
                                                  // graphic string (2 byte length)
	public	static final  int DB2_SQLTYPE_NVARGRAPH = 465;
	public	static final  int DB2_SQLTYPE_GRAPHIC = 468;     // GRAPHIC(i) - fixed length graphic string                                                             */
	public	static final  int DB2_SQLTYPE_NGRAPHIC = 469;
	public	static final  int DB2_SQLTYPE_LONGRAPH = 472;    // LONG VARGRAPHIC(i) - varying length graphic string                                              */
	public	static final  int DB2_SQLTYPE_NLONGRAPH = 473;
	public	static final  int DB2_SQLTYPE_LSTR = 476;        // varying length string for Pascal (1-byte length)                                                     */
	public	static final  int DB2_SQLTYPE_NLSTR = 477;

	public	static final  int DB2_SQLTYPE_FLOAT = 480;       // FLOAT - 4 or 8 byte floating point
	public	static final  int DB2_SQLTYPE_NFLOAT = 481;
	public	static final  int DB2_SQLTYPE_DECIMAL = 484;     // DECIMAL (m,n)
	public	static final  int DB2_SQLTYPE_NDECIMAL = 485;
	public	static final  int DB2_SQLTYPE_ZONED = 488;       // Zoned Decimal -> DECIMAL(m,n)
	public	static final  int DB2_SQLTYPE_NZONED = 489;

	public	static final  int DB2_SQLTYPE_BIGINT = 492;      // BIGINT - 8-byte signed integer
	public	static final  int DB2_SQLTYPE_NBIGINT = 493;
	public	static final  int DB2_SQLTYPE_INTEGER = 496;     // INTEGER
	public	static final  int DB2_SQLTYPE_NINTEGER = 497;
	public	static final  int DB2_SQLTYPE_SMALL = 500;       // SMALLINT - 2-byte signed integer                                                                    */
	public	static final  int DB2_SQLTYPE_NSMALL = 501;

	public	static final  int DB2_SQLTYPE_NUMERIC = 504;     // NUMERIC -> DECIMAL (m,n)
	public	static final  int DB2_SQLTYPE_NNUMERIC = 505;

	public	static final  int DB2_SQLTYPE_ROWID = 904;           // ROWID
	public	static final  int DB2_SQLTYPE_NROWID = 905;
	public	static final  int DB2_SQLTYPE_BLOB_LOCATOR = 960;    // BLOB locator
	public	static final  int DB2_SQLTYPE_NBLOB_LOCATOR = 961;
	public	static final  int DB2_SQLTYPE_CLOB_LOCATOR = 964;    // CLOB locator
	public	static final  int DB2_SQLTYPE_NCLOB_LOCATOR = 965;
	public	static final  int DB2_SQLTYPE_DBCLOB_LOCATOR = 968;  // DBCLOB locator
	public	static final  int DB2_SQLTYPE_NDBCLOB_LOCATOR = 969;

	// extensions to the db2 datatypes
    // public	static final  int DB2_SQLTYPE_BOOLEAN = 1000;     // BOOLEAN
    // public	static final  int DB2_SQLTYPE_NBOOLEAN = 1001;

}
