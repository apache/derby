/*

   Derby - Class org.apache.derby.impl.drda.CodePoint

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

// TBD:
//	 organize into separate kinds of code points; impose organizational scheme.
// TBD:
//	 reconsider the various SECCHKCD_xx constants, perhaps we should hardwire.

class CodePoint
{
	// --------------------code points for constant ddm data----------------------

	// Indicates false state.  This 1-byte code point is used by some DDM parameters.
	static final byte FALSE = -16;  // was 0xf0

	// Indicates true state.  This 1-byte code point is used by some DDM parameters.
	static final byte TRUE = -15;  // was 0xf1

	// Zero indicator constant.
	// Indicates data does flow.
	static final int ZEROIND = 0x00;

	static final int NULLDATA = 0xFF;

	// Security check was successful.
	static final int SECCHKCD_00 = 0x00;

	// SECMEC value not supported.
	static final int SECCHKCD_01 = 0x01;

	// DCE informational status
	static final int SECCHKCD_02 = 0x02;

	// DCE retryable error.
	static final int SECCHKCD_03 = 0x03;

	// DCE non-retryable error.
	static final int SECCHKCD_04 = 0x04;

	// GSSAPI informaional status.
	static final int SECCHKCD_05 = 0x05;

	// GSSAPI retryable error.
	static final int SECCHKCD_06 = 0x06;

	// GSSAPI non-retryable error.
	static final int SECCHKCD_07 = 0x07;

	// Local security service info.
	static final int SECCHKCD_08 = 0x08;

	// Local security service retryable error.
	static final int SECCHKCD_09 = 0x09;

	// Local security service non-retryable error.
	static final int SECCHKCD_0A = 0x0A;

	// SECTKN missing or invalid.
	static final int SECCHKCD_0B = 0x0B;

	// Password expired.
	static final int SECCHKCD_0E = 0x0E;

	// Password invalid.
	static final int SECCHKCD_0F = 0x0F;

	// Password missing.
	static final int SECCHKCD_10 = 0x10;

	// Userid missing.
	static final int SECCHKCD_12 = 0x12;

	// Userid invalid.
	static final int SECCHKCD_13 = 0x13;

	// Userid revoked.
	static final int SECCHKCD_14 = 0x14;

	// New password invalid.
	static final int SECCHKCD_15 = 0x15;

	// ----------------------ddm code points--------------------------------------

	final static int EXCSAT = 0x1041;
	final static int SYNCCTL = 0x1055;
	final static int SYNCRSY = 0x1069;
	final static int ACCSEC = 0x106D;
	final static int SECCHK = 0x106E;
	final static int SYNCLOG = 0x106F;
	final static int ACCRDB = 0x2001;
	final static int BGNBND = 0x2002;
	final static int BNDSQLSTT = 0x2004;
	final static int CLSQRY = 0x2005;
	final static int CNTQRY = 0x2006;
	final static int DRPPKG = 0x2007;
	final static int DSCSQLSTT = 0x2008;
	final static int ENDBND = 0x2009;
	final static int EXCSQLIMM = 0x200A;
	final static int EXCSQLSTT = 0x200B;
	final static int EXCSQLSET = 0x2014;
	final static int OPNQRY = 0x200C;
	final static int PRPSQLSTT = 0x200D;
	final static int RDBCMM = 0x200E;
	final static int RDBRLLBCK = 0x200F;
	final static int REBIND = 0x2010;
	final static int DSCRDBTBL = 0x2012;

	final static int SQLDTA = 0x2412;
	final static int SQLDTARD = 0x2413;
	final static int SQLSTT = 0x2414;
	final static int SQLATTR = 0x2450;
	final static int SQLSTTVRB = 0x2419;
	final static int QRYDSC = 0x241A;
	final static int QRYDTA = 0x241B;
	final static int SQLRSLRD = 0x240E;
	final static int SQLCINRD = 0x240B;

	// Access Security Reply Data.
	// Contains the security information from a target server's
	// security manager.  This information is returned in response
	// to an ACCSEC command.
	static final int ACCSECRD = 0x14AC;

	// Agent codepoint constant.
	static final int AGENT = 0x1403;

	// The codepoint for codepoint
	static final int CODPNT = 0x000C;

	// The Codepoint for data representation of dictionary codepoint
	static final int CODPNTDR = 0x0064;

	// Subtypes for CODPNTR
	static final int CSTMBCS = 0x2435;  // Multibyte default

	// CCSID for Double-Byte Characters codepoint constant.
	static final int CCSIDDBC = 0x119D;

	// CCSID for Mixed-Byte Characters codepoint constant.
	static final int CCSIDMBC = 0x119E;

	// CCSID Manager.  Min. level 4.
	// Provides character data conversion of the DDM parameters
	// containing character data.
	static final int CCSIDMGR = 0x14CC;

	// CCSID for Single-Byte Characters codepoint constant.
	static final int CCSIDSBC = 0x119C;

	// LU 6.2 Conversational Communications Manager.
	// Describes the communications manager that supports
	// conversational protocols by using System Network
	// Architecture Logical Unit 6.2 (SNA LU 6.2) local
	// communications facilities.
	static final int CMNAPPC = 0x1444;

	// SNA LU 6.2 Sync Point Conversational Communications
	// Manager.  Min. level 4.
	// Provides an SNA LU 6.2 Conversational Communications
	// Manager with sync point support.
	static final int CMNSYNCPT = 0x147C;

	// TCP/IP Communication Manager codepoint constant.  Min. level 5.
	static final int CMNTCPIP = 0x1474;

	// XA Manager codepoint constant
	static final int XAMGR = 0x1C01;

	// Correlation Token codepoint constant.
	static final int CRRTKN = 0x2135;

	// Target Default Value Return
	static final int TRGDFTRT = 0x213B;

	// It is a manager of a set of named descriptions of object.
	static final int DICTIONARY = 0x1458;

	// Manager dependency error code
	static final int DEPERRCD = 0x119B;

	// Description Error code
	static final int DSCERRCD = 0x2101;

	// Server Attributes Reply Data codepoint constant.
	static final int EXCSATRD = 0x1443;

	// External Name codepoint constant.
	static final int EXTNAM = 0x115E;

	// Fixed Row Query Protocol.
	static final int FIXROWPRC = 0x2418;

	// Force Fixed Row Query Protocol.
	static final int FRCFIXROW = 0x2410;

	// Limited Block Query Protocol.
	static final int LMTBLKPRC = 0x2417;

	// Manager Level List codepoint constant.
	static final int MGRLVLLS = 0x1404;

	// Manager Level Number Attribute constants.
	// Specifies the level of a defined DDM manager.
	static final int MGRLVLN = 0x1473;

	// Monitor Events
	static final int MONITOR = 0x1900;

	// Monitor Reply Data
	static final int MONITORRD = 0x1C00;

	// New Password
	static final int NEWPASSWORD = 0x11DE;

	// Password
	static final int PASSWORD = 0x11A1;

	// Package Default Character Subtype codepoint constant.
	static final int PKGDFTCST = 0x2125;

	// Package Id
	static final int PKGID = 0x2109;

	// Maximum Number of extra Blocks
	static final int MAXBLKEXT = 0x2141;

	// Maximum result set count
	static final int MAXRSLCNT = 0x2140;

	// Result Set Flags
	static final int RSLSETFLG = 0x2142;

	// RDB Commit allowed
	static final int RDBCMTOK = 0x2105;

	// Package name & consistency token
	static final int PKGNAMCT = 0x2112;

	// list of PAKNAMCSN
	static final int PKGSNLST = 0x2139;

	// Conversational Protocol Error Code
	static final int PRCCNVCD = 0x113F;

	// Product Specific Identifier codepoint constant.
	static final int PRDID = 0x112E;

	// Output override
	static final int OUTOVR = 0x2415;

  	//Output override option
	static final int OUTOVROPT = 0x2147;

  	// Package Consistency Token
	static final int PKGCNSTKN = 0x210D;

	// Product Specific Data
	static final int PRDDTA = 0x2104;

	// Query Instance Identifier
	static final int QRYINSID = 0x215B;

	// Query Block Protocol Control
	static final int QRYBLKCTL = 0x2132;

	// Query Block Size
	static final int QRYBLKSZ = 0x2114;

	// Query Protocol Type
	static final int QRYPRCTYP = 0x2102;

	// Query Close Implicit
	static final int QRYCLSIMP = 0x215D;

	// Query Close Lock Release
	static final int QRYCLSRLS = 0x215E;

	// QRYOPTVAL - Query Optimization Value
	static final int QRYOPTVAL = 0x215F;

	// Cursor Allows Read and Delete Operations.
	static final int QRYDEL = 0x2;

	// Cursor is Read-only.
	static final int QRYRDO = 0x1;

	// Insensitive SCROLL
	static final int QRYINS = 0x1;

	// Number of fetch or Insert Rows
	static final int NBRROW = 0x213A;

	// Output expected
	static final int OUTEXP = 0x2111;

	// Procedure name
	static final int PRCNAM = 0x2138; 

	// Query Attribute for Updatability
	static final int QRYATTUPD = 0x2150;

	// Cursor Allows Read, Delete, and Update Operations.
	static final int QRYUPD = 0x4;

	// Relational Database codepoint constant.  Min. level 3.
	static final int RDB = 0x240F;

	// RDB Access Manager Class.
	static final int RDBACCCL = 0x210F;

	// RDB Allow Updates
	static final int RDBALWUPD = 0x211A;

	// Query Relative  Scrolling Action
	static final int QRYRELSCR = 0x213C;

	// Query Scroll Orientation
	static final int QRYSCRORN = 0x2152;

	// Query Row Number
	static final int QRYROWNBR = 0x213D;

	// Query Row Sensitivity
	static final int QRYROWSNS = 0x2153;

	// Query Refresh Answer set table 
	static final int QRYRFRTBL = 0x213E;

	// Query Attribute for Scrollability
	static final int QRYATTSCR = 0x2149;

	// Query Attribute for Sensitivity
	static final int QRYATTSNS = 0x2157;

	// Query Block Reset
	static final int QRYBLKRST = 0x2154;

	// Query Rowset Size
	static final int QRYROWSET = 0x2156;

	// Query Returns Data
	static final int QRYRTNDTA = 0x2155;

	// RDB interrupt token.
	static final int RDBINTTKN = 0x2103;

	// Relational Database Name codepoint constant.
	static final int RDBNAM = 0x2110;

	// RDB Collection Identifier
	static final int RDBCOLID = 0x2108;

	// Resource name information
	static final int RSCNAM = 0x112D;

	// Resource Type Information
	static final int RSCTYP = 0x111F;

	// Reason Code Information
	static final int RSNCOD = 0x1127;

	// Resynchronization Manager.  Min levl 5.
	// It is a manager object of DDM that performs
	// resynchronization for in-doubt units of work after
	// a sync point operation failure.
	static final int RSYNCMGR = 0x14C1;

	// Retuan SQL Descriptor Area
	static final int RTNSQLDA = 0x2116;

	// Type of SQL Descriptor Area
	static final int TYPSQLDA = 0x2146;

	// Security Check Code codepoint constant.
	static final int SECCHKCD = 0x11A4;

	// Security Mechanism codepoint constant.
	static final int SECMEC = 0x11A2;

	// Security Manager codepoint constant.
	static final int SECMGR = 0x1440;

	// Security Manager Name codepoint constant.
	static final int SECMGRNM = 0x1196;

	// Security Token codepoint constant.
	static final int SECTKN = 0x11DC;

	// Return of EXTDTA Option
	static final int RTNEXTDTA = 0x2148;

	// Return of EXTDTA Option - Return EXTDTAs by Row
	static final int RTNEXTROW = 0x1;

	// Return of EXTDTA Option - Return All EXTDTAs for QRYDTA's Sent
	static final int RTNEXTALL = 0x2;

	// Supervisor name codepoint constant.
	static final int SPVNAM = 0x115D;

	// SQL Application Manager codepoint constant.  Min. level 3.
	static final int SQLAM = 0x2407;

	// SQL Communication Area Reply Data codepoint constant.
	static final int SQLCARD = 0x2408;

	// Hold Cursor Position
	static final int SQLCSRHLD = 0x211F;

	// SQLDA Reply Data codepoint constant.
	static final int SQLDARD = 0x2411;

	// Server Class Name codepoint constant.
	static final int SRVCLSNM = 0x1147;

	// Server Diagnostic Information codepoint constant.
	static final int SRVDGN = 0x1153;

	// Server List codepoint constant.
	static final int SRVLST = 0x244E;

	// Server Name codepoint constant.
	static final int SRVNAM = 0x116D;

	// Server Product Release Level codepoint constant.
	static final int SRVRLSLV = 0x115A;

	//Statement Decimal Delimiter
	static final int STTDECDEL = 0x2121;

	//Statement String Delimiter
	static final int STTSTRDEL = 0x2120;

	// Supervisor.
	// Manages a collection of managers in a consistent manner.
	static final int SUPERVISOR = 0x143C;

	// Security Service Error Number codepoint constant.
	static final int SVCERRNO = 0x11B4;

	// Severity Code codepoint constant.
	static final int SVRCOD = 0x1149;

	// Sync Point Manager.  Min. level 4.
	// It is a manager object of DDM that coordinates resource
	// recovery of the units of work associated with recoverable
	// resources in multiple DDM servers.
	static final int SYNCPTMGR = 0x14C0;

	// Syntax Error code
	static final int SYNERRCD = 0x114A;

	// Data Type Definition Name codepoint constant.
	static final int TYPDEFNAM = 0x002F;

	// TYPDEF Overrides codepoint constant.
	static final int TYPDEFOVR = 0x0035;

	// Unit of Word Disposition codepoint constant.
	static final int UOWDSP = 0x2115;

	// Usrid codepoint constant.
	static final int USRID = 0x11A0;

	// Version name
	static final int VRSNAM = 0x1144;

	// Rdb Package Name, Consistency Token, and Section
	// Number codepoint constant.
	static final int PKGNAMCSN = 0x2113;

	//-----------------------DDM reply codepoints---------------------------------

	// Invalid description
	static final int DSCINVRM = 0x220A;

	// Codepoint for Command Authorization for Agent Permanent Error
	static final int CMDATHRM = 0x121C;

	// Command Check codepoint constant.
	static final int CMDCHKRM = 0x1254;

	// Command Not Supported codepoint constant.
	static final int CMDNSPRM = 0x1250;

	// Codepoint for Agent Permanent Error Reply message
	static final int AGNPRMRM = 0x1232;

	static final int BGNBNDRM = 0x2208;

	// Abnormal End of Unit of Work Condition codepoint constant.
	static final int ABNUOWRM = 0x220D;

	// Access to RDB Completed.
	// Specifies that an instance of the SQL application manager
	// has been created and is bound to the specified RDB.
	static final int ACCRDBRM = 0x2201;

	final static int CMDCMPRM = 0x124B;

	final static int MGRLVLRM = 0x1210;

	// Manager dependency Error
	static final int MGRDEPRM = 0x1218;

	// End Unit of Work Condition codepoint constant.
	static final int ENDUOWRM = 0x220C;

	// Object Not Supported codepoint constant.
	static final int OBJNSPRM = 0x1253;

	// Conversational Protocol Error
	static final int PRCCNVRM = 0x1245;

	// Parameter Not Supported codepoint constant.
	static final int PRMNSPRM = 0x1251;

	// RDB Package Binding Process Not active Codepoint
	static final int PKGBNARM = 0x2206;

	// RDB Package Binding Process Active codepoint constant.
	static final int PKGBPARM = 0x2209;

	// Query not open codepoint constant.
	static final int QRYNOPRM = 0x2202;

	// Query previously opened codepoint
	static final int QRYPOPRM = 0x220F;

	// RDB Currently Accessed Codepoint
	static final int RDBACCRM = 0x2207;

	// Security Check.
	// Indicates the acceptability of the security information.
	static final int SECCHKRM = 0x1219;

	// RDB Access Failed Reply Message codepoint
	static final int RDBAFLRM = 0x221A;

	// Not Authorized To RDB reply message codepoint
	static final int RDBATHRM = 0x22CB;

	// RDB Not Accessed codepoint constant.
	static final int RDBNACRM = 0x2204;

	// RDB not found codepoint
	static final int RDBNFNRM = 0x2211;

	// RDB Update Reply Message codepoint constant.
	static final int RDBUPDRM = 0x2218;

	// Resources Limits Reached
	static final int RSCLMTRM = 0x1233;

	// Data Stream Syntax Error
	static final int SYNTAXRM = 0x124C;

	// Target Not Supported
	static final int TRGNSPRM = 0x125F;

	// Parameter Value Not Supported codepoint constant.
	static final int VALNSPRM = 0x1252;

	// SQL Error Condition codepoint constant.
	static final int SQLERRRM = 0x2213;

	final static int OPNQRYRM = 0x2205;
	final static int ENDQRYRM = 0x220B;
	final static int DTAMCHRM = 0x220E;
	final static int OPNQFLRM = 0x2212;
	final static int RSLSETRM = 0x2219;
	final static int CMDVLTRM = 0x221D;
	final static int CMMRQSRM = 0x2225;

	//-----------------------ddm enumerated values-------------------------------

	// TYPSQLDA - Standard Output SQLDA
	static final int TYPSQLDA_STD_OUTPUT = 0;

	// TYPSQLDA - Standard Input SQLDA
	static final int TYPSQLDA_STD_INPUT = 1;

	// TYPSQLDA - Light Output SQLDA
	static final int TYPSQLDA_LIGHT_OUTPUT = 2;

	// TYPSQLDA - Light Input SQLDA
	static final int TYPSQLDA_LIGHT_INPUT = 3;

	// TYPSQLDA - Extended Output SQLDA
	static final int TYPSQLDA_X_OUTPUT = 4;

	// TYPSQLDA - Extended Input SQLDA
	static final int TYPSQLDA_X_INPUT = 5;



	// QRYCLSIMP - Target Server determines whether to implicitly
	// close the cursor or not upon SQLSTATE 02000 based on cursor type.
	static final int QRYCLSIMP_SERVER_CHOICE = 0x00;
	
	// QRYCLSIMP - Target Server must implicitly close the cursor
	// upon SQLSTATE 02000.
	static final int QRYCLSIMP_YES = 0x01;
	
	// QRYCLSIMP - Target Server must not implicitly close the cursor
	// upon SQLSTATE 02000.
	static final int QRYCLSIMP_NO = 0x02;

	// QRYCLSRLS - Do not release read locks when the query is closed
	static final int QRYCLSRLS_NO = 0x00;
	
	// QRYCLSRLS - Release read locks when the query is closed
	static final int QRYCLSRLS_YES = 0x01;

	// QRYBLKFCT - No 'OPTIMIZE for N ROWS' clause associated with
	// the select statement for the cursor.
	static final int QRYBLKFCT_NONE = 0x00;
	
	// QRYBLKEXA - Every query block is exactly the size specified
	// in the QRYBLKSZ parameter, except for possibly the last
	// query block which may be shorter.
	static final int QRYBLKEXA = 0x00;

	// QRYBLKFLX - Every query block is at least the size specified
	// in the QRYBLKSZ parameter, except for possibly the last
	// query block which may be shorter.
	static final int QRYBLKFLX = 0x01;
	
	//----------------------------fdoca code points-------------------------------

	// Externalized FD:OCA Data codepoint constant.
	static final int EXTDTA = 0x146C;

	// FDOCA data descriptor
	static final int FDODSC = 0x0010;

	// FDOCA data
	static final int FDODTA = 0x147A;

	// FDOCA Descriptor offset
	static final int FDODSCOFF = 0x2118;

	// FDOCA Triplet Parameter offset
	static final int FDOPRMOFF = 0x212B;

	// FDOCA Triplet offset
	static final int FDOTRPOFF = 0x212A;

	//--------------------------ddm error code points---------------------------------
	static final int SYNERRCD_DSS_LESS_THAN_6 = 0x01;
	static final int SYNERRCD_DSS_LENGTH_BYTE_NUMBER_MISMATCH = 0x02;
	static final int SYNERRCD_CBYTE_NOT_D0 = 0x03;
	static final int SYNERRCD_FBYTE_NOT_SUPPORTED = 0x04;
	static final int SYNERRCD_OBJ_LEN_LESS_THAN_4 = 0x07;
	static final int SYNERRCD_TOO_BIG = 0x09;
	static final int SYNERRCD_OBJ_LEN_NOT_ALLOWED = 0x0B;
	static final int SYNERRCD_INCORRECT_EXTENDED_LEN = 0x0C;
	static final int SYNERRCD_REQ_OBJ_NOT_FOUND = 0x0E;
	static final int SYNERRCD_TOO_MANY = 0x0F;
	static final int SYNERRCD_DUP_OBJ_PRESENT = 0x12;
	static final int SYNERRCD_INVALID_CORRELATOR = 0x13;
	static final int SYNERRCD_REQ_VAL_NOT_FOUND = 0x14;
	static final int SYNERRCD_DSS_CONT_LESS_OR_EQUAL_2 = 0x16;
	static final int SYNERRCD_CHAIN_OFF_SAME_NEXT_CORRELATOR = 0x18;
	static final int SYNERRCD_CHAIN_OFF_ERROR_CONTINUE = 0x1A;
	static final int SYNERRCD_INVALID_CP_FOR_CMD = 0x1D;

	static final int PRCCNVCD_OBJDSS_SENT_NOT_ALLOWED = 0x03;
	static final int PRCCNVCD_EXCSAT_FIRST_AFTER_CONN = 0x06;
	static final int PRCCNVCD_ACCSEC_SECCHK_WRONG_STATE = 0x11;
	static final int PRCCNVCD_RDBNAM_MISMATCH = 0x12;

	static final int SVRCOD_INFO = 0;      // Information Only Severity Code
	static final int SVRCOD_WARNING = 4;   // Warning Severity Code
	static final int SVRCOD_ERROR = 8;     // Error Severity Code
	static final int SVRCOD_SEVERE = 16;   // Severe Error Severity Code
	static final int SVRCOD_ACCDMG = 32;   // Access Damage Severity Code
	static final int SVRCOD_PRMDMG = 64;   // Permanent Damage Severity Code
	static final int SVRCOD_SESDMG = 128;  // Session Damage Severity Code
	
	//---------------------- Security Mechanisms ---------------------------
	static final int SECMEC_DCESEC = 1;		// Distributed Computing Environment Security
	static final int SECMEC_USRIDPWD = 3;	// Userid and Password
	static final int SECMEC_USRIDONL = 4;	// Userid only
	static final int SECMEC_USRIDNWPWD = 5;  // Userid, Password, and new Password
	static final int SECMEC_USRSBSPWD = 6;  // Userid with substitute password
	static final int SECMEC_USRENCPWD = 7;  // Userid with encrypted password
	static final int SECMEC_EUSRIDPWD = 9;  // Encrpyted userid and password
	static final int SECMEC_EUSRIDNWPWD = 10;  // Encrpyted userid and password

	//---------------------Security Check Codes ---------------------------
	static final int SECCHKCD_OK = 0;		// Security info correct and acceptable
	static final int SECCHKCD_NOTSUPPORTED = 0x01;	// SECMEC value not supported
	static final int SECCHKCD_SECTKNMISSING = 0x0E;	// SECTKN missing or invalid 
	static final int SECCHKCD_PASSWORDMISSING = 0x10;	// Password missing  
	static final int SECCHKCD_USERIDMISSING = 0x12;	// User Id missing  
	static final int SECCHKCD_USERIDINVALID = 0x13;	// Userid invalid

	//----------------------Type Definition Names we care about -----------
	static final String TYPDEFNAM_QTDSQLASC = "QTDSQLASC"; // ASCII
	static final String TYPDEFNAM_QTDSQLJVM = "QTDSQLJVM"; // Java platform
	static final String TYPDEFNAM_QTDSQLX86 = "QTDSQLX86"; //  Intel X86 platform

	//----------------------Max sizes for strings in the protocol ---------
	static final int MAX_NAME = 255;
	static final int RDBNAM_LEN = 18; //dbname fixed length for SQLAM level 6, for level 7,
									  //limit is MAX_NAME (255)
	static final int PRDID_MAX = 8;
	static final int RDBCOLID_LEN = 18;
	static final int PKGID_LEN = 18;
	static final int PKGCNSTKN_LEN = 8;
	static final int PKGNAMCSN_LEN = RDBNAM_LEN + RDBCOLID_LEN + PKGID_LEN +
		PKGCNSTKN_LEN + 2;

	//---------------------QRYBLSZ min and maximum
	static final int QRYBLKSZ_MIN = 512;
	static final int QRYBLKSZ_MAX = 32767;
	static final int QRYROWSET_MAX = 32767;
	static final int QRYROWSET_DEFAULT = -1;

	//--------------------Defaults for QRYBLKCTL and MAXBLKEXT
	static final int QRYBLKCTL_DEFAULT = LMTBLKPRC;
	static final int MAXBLKEXT_NONE = 0;
	static final int MAXBLKEXT_DEFAULT = MAXBLKEXT_NONE;

	//-------------------Default for QRYCLSIMP
	static final int QRYCLSIMP_DEFAULT = QRYCLSIMP_SERVER_CHOICE;

	//-------------- QRYSCRORN - query scroll orientation values
	static final int QRYSCRREL = 1;		// relative fetch
	static final int QRYSCRABS = 2;		// absolute fetch
	static final int QRYSCRAFT = 3;		// after last row
	static final int QRYSCRBEF = 4;		// before first row

	//---------------OUTOVROPT - output override option values
	static final int OUTOVRFRS = 1; 	// Output Override allowed on first CNTQRY
	static final int OUTOVRANY = 2; 	// Output Override allowed on any CNTQRY

	//-----------------------Manager code points --------------------------

	protected static int [] MGR_CODEPOINTS = {
											AGENT,
											CCSIDMGR,	
											CMNAPPC, 
											CMNSYNCPT,	
											CMNTCPIP,	
											DICTIONARY,
											RDB,
											RSYNCMGR,	
											SECMGR,	
											SQLAM,
											SUPERVISOR,	
											SYNCPTMGR,
											XAMGR
											};
	protected static final int UNKNOWN_MANAGER = -1;

	// hide the default constructor
	private CodePoint () {}

	/**
	 * Given a manager codepoint find it's location in the managers array
	 *
	 * @return index into manager array or UNKNOWN_MANAGER if not found
	 */
	protected static int getManagerIndex(int manager)
	{
		for (int i = 0; i < MGR_CODEPOINTS.length; i++)
			if (MGR_CODEPOINTS[i] == manager)
				return i;
		return UNKNOWN_MANAGER;
	}
	/**
	 * Check if a manager codepoint is a known manager
	 * 
	 * @return true if known, false otherwise
	 */
	protected static boolean isKnownManager(int manager)
	{
		for (int i = 0; i < CodePoint.MGR_CODEPOINTS.length; i++)
			if (manager == CodePoint.MGR_CODEPOINTS[i])
				return true;
		return false;
	}
}
